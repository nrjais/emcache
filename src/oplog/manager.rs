use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::executor::Task;
use crate::storage::PostgresClient;
use crate::types::OplogEvent;
use futures::StreamExt;

use super::processor::OplogProcessor;
use futures_batch::ChunksTimeoutStreamExt;

pub struct OplogManager {
    processor: OplogProcessor,
    event_receiver: Arc<Mutex<Option<mpsc::Receiver<OplogEvent>>>>,
    ack_sender: broadcast::Sender<OplogEvent>,
}

impl OplogManager {
    pub async fn new(
        client: PostgresClient,
        ack_sender: broadcast::Sender<OplogEvent>,
    ) -> Result<(Self, mpsc::Sender<OplogEvent>)> {
        let processor = OplogProcessor::new(client);
        let (sender, receiver) = mpsc::channel(1000);

        let manager = Self {
            processor,
            event_receiver: Arc::new(Mutex::new(Some(receiver))),
            ack_sender,
        };

        Ok((manager, sender))
    }
}

impl Task for OplogManager {
    fn name(&self) -> String {
        "oplog-manager".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> Result<()> {
        let receiver = {
            let mut guard = self.event_receiver.lock().await;
            guard.take()
        };

        let event_channel = receiver.expect("receiver is not initialized");
        let mut stream = ReceiverStream::new(event_channel).chunks_timeout(100, Duration::from_secs(1));
        loop {
            tokio::select! {
                Some(oplogs) = stream.next() => {
                    info!("Flushing batch of {} oplogs", oplogs.len());
                    if oplogs.len() == 0 {
                        continue;
                    }
                    let last_oplog = oplogs[oplogs.len() - 1].clone();

                    info!("Oplog from, first: {:?}, last: {:?}", oplogs[0].from, last_oplog.from);

                    self.processor.flush_batch(oplogs).await?;
                    let _ = self.ack_sender.send(last_oplog);
                }
                _ = cancellation_token.cancelled() => {
                    info!("Oplog manager cancelled");
                    return Ok(());
                }
            }
        }
    }
}
