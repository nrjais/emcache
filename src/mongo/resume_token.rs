use mongodb::change_stream::event::ResumeToken;
use tokio::sync::{
    Mutex,
    broadcast::{Receiver, error::RecvError},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{executor::Task, storage::PostgresClient, types::OplogEvent};

#[derive(Debug, sqlx::FromRow)]
pub struct CollectionResumeToken {
    token_data: serde_json::Value,
}

impl CollectionResumeToken {
    pub fn token_data(&self) -> ResumeToken {
        serde_json::from_value(self.token_data.clone()).unwrap()
    }
}

pub struct ResumeTokenManager {
    postgres: PostgresClient,
    receiver: Mutex<Receiver<OplogEvent>>,
}

impl ResumeTokenManager {
    pub fn new(postgres: PostgresClient, receiver: Receiver<OplogEvent>) -> Self {
        Self {
            postgres,
            receiver: Mutex::new(receiver),
        }
    }

    pub async fn fetch(&self, entity: &str) -> anyhow::Result<Option<CollectionResumeToken>> {
        let resume_token = sqlx::query_as!(
            CollectionResumeToken,
            "SELECT token_data FROM mongo_resume_tokens WHERE entity = $1",
            entity
        )
        .fetch_optional(self.postgres.postgres())
        .await?;

        Ok(resume_token)
    }

    pub async fn save(&self, entity: &str, data: &serde_json::Value) -> Result<(), anyhow::Error> {
        let _ = sqlx::query!(
            "INSERT INTO mongo_resume_tokens (entity, token_data) VALUES ($1, $2) ON CONFLICT (entity) DO UPDATE SET token_data = $2",
            entity,
            data
        )
        .execute(self.postgres.postgres())
        .await?;
        Ok(())
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        let mut receiver = self.receiver.lock().await;
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Resume token receiver cancelled, shutting down");
                    return Ok(());
                }
                oplog = receiver.recv() => {
                    match oplog {
                        Ok(oplog) => {
                            self.save(&oplog.oplog.entity, &oplog.data).await?;
                        }
                        Err(e) => {
                            if let RecvError::Closed = e {
                                info!("Resume token receiver closed, shutting down");
                                return Ok(());
                            }
                            return Err(e.into());
                        }
                    }
                }
            }
        }
    }

    pub async fn delete(&self, entity: &str) -> Result<(), anyhow::Error> {
        let _ = sqlx::query!("DELETE FROM mongo_resume_tokens WHERE entity = $1", entity)
            .execute(self.postgres.postgres())
            .await?;
        Ok(())
    }
}

impl Task for ResumeTokenManager {
    fn name(&self) -> String {
        "resume_token".to_string()
    }

    async fn execute(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        self.start(cancellation_token.clone()).await
    }
}
