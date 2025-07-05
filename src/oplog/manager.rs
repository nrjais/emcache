use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::executor::Task;
use crate::storage::PostgresClient;
use crate::types::OplogEvent;
use futures::StreamExt;

use super::processor::OplogProcessor;
use futures_batch::ChunksTimeoutStreamExt;

pub struct OplogManager {
    processor: OplogProcessor,
    event_receiver: Arc<Mutex<Option<mpsc::Receiver<OplogEvent>>>>,
}

impl OplogManager {
    pub async fn new(client: PostgresClient) -> Result<(Self, mpsc::Sender<OplogEvent>)> {
        let processor = OplogProcessor::new(client);
        let (sender, receiver) = mpsc::channel(1000); // Choose appropriate buffer size

        let manager = Self {
            processor,
            event_receiver: Arc::new(Mutex::new(Some(receiver))),
        };

        Ok((manager, sender))
    }
}

impl Task for OplogManager {
    fn name(&self) -> String {
        "oplog-manager".to_string()
    }

    async fn execute(&self, _cancellation_token: CancellationToken) -> Result<()> {
        let receiver = {
            let mut guard = self.event_receiver.lock().await;
            guard.take()
        };

        let event_channel = receiver.expect("receiver is not initialized");
        let mut stream = ReceiverStream::new(event_channel).chunks_timeout(100, Duration::from_secs(1));
        while let Some(oplogs) = stream.next().await {
            self.processor.flush_batch(oplogs).await?;
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> { Ok(()) }
}
