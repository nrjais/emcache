use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::executor::Task;
use crate::types::OplogEvent;
use futures::StreamExt;

use super::database::OplogDatabase;
use futures_batch::ChunksTimeoutStreamExt;

pub struct OplogManager {
    db_ops: OplogDatabase,
    event_receiver: Arc<Mutex<Option<mpsc::Receiver<OplogEvent>>>>,
    ack_sender: broadcast::Sender<OplogEvent>,
}

impl OplogManager {
    pub async fn new(
        db_ops: OplogDatabase,
        ack_sender: broadcast::Sender<OplogEvent>,
    ) -> Result<(Self, mpsc::Sender<OplogEvent>)> {
        let (sender, receiver) = mpsc::channel(2000);

        let manager = Self {
            db_ops,
            event_receiver: Arc::new(Mutex::new(Some(receiver))),
            ack_sender,
        };

        Ok((manager, sender))
    }

    pub async fn flush_batch(&self, oplogs: Vec<OplogEvent>) -> Result<()> {
        if oplogs.is_empty() {
            return Ok(());
        }

        let oplogs = oplogs.into_iter().map(|oplog| oplog.oplog).collect::<Vec<_>>();

        self.db_ops.insert_to_oplog(oplogs).await?;

        Ok(())
    }

    async fn process_oplogs(&self, cancellation_token: CancellationToken) -> Result<()> {
        let receiver = {
            let mut guard = self.event_receiver.lock().await;
            guard.take()
        };

        let event_channel = receiver.expect("receiver is not initialized");
        let mut stream = ReceiverStream::new(event_channel).chunks_timeout(1000, Duration::from_millis(10));
        loop {
            tokio::select! {
                Some(oplogs) = stream.next() => {
                    info!("Flushing batch of {} oplogs", oplogs.len());
                    if oplogs.is_empty() {
                        continue;
                    }
                    let last_oplog = oplogs[oplogs.len() - 1].clone();

                    info!("Oplog from, first: {:?}, last: {:?}", oplogs[0].from, last_oplog.from);

                    self.flush_batch(oplogs).await?;
                    self.ack_sender.send(last_oplog)
                        .inspect_err(|e| error!("Failed to send oplog ack: {}", e))?;
                }
                _ = cancellation_token.cancelled() => {
                    info!("Oplog manager cancelled");
                    return Ok(());
                }
            }
        }
    }
}

impl Task for OplogManager {
    fn name(&self) -> String {
        "oplog-manager".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> Result<()> {
        self.process_oplogs(cancellation_token).await
    }
}
