mod resume_token;
mod shaper;

use std::{future::Future, pin::Pin};

use futures::{Stream, StreamExt};
use mongodb::{
    Client, Collection, Database,
    bson::{self, Document},
    change_stream::event::ResumeToken,
    options::FullDocumentType,
};
use tokio::sync::mpsc;
use tracing::warn;

use crate::{config::AppConfig, executor::Task, storage::PostgresClient, types::OplogEvent};

pub struct MongoClient {
    database: Database,
    postgres: PostgresClient,
    entity: String,
    collection: String,
    event_channel: mpsc::Sender<OplogEvent>,
}

impl MongoClient {
    pub async fn new(
        config: &AppConfig,
        pg: &PostgresClient,
        event_channel: mpsc::Sender<OplogEvent>,
    ) -> anyhow::Result<Self> {
        let client = Client::with_uri_str(&config.sources.main.uri).await?;
        Ok(Self {
            event_channel,
            database: client.database(&config.sources.main.database),
            postgres: pg.clone(),
            entity: config.sources.main.entity.to_string(),
            collection: config.sources.main.collection.to_string(),
        })
    }

    pub async fn checkpoint_oplog(&self, oplog: &OplogEvent) -> anyhow::Result<()> {
        resume_token::save(&self.postgres, &self.entity, &oplog.data).await
    }

    pub async fn start_stream(&self) -> anyhow::Result<()> {
        let mut stream = create_change_stream(&self.postgres, &self.database, &self.entity, &self.collection).await?;

        while let Some(event) = stream.next().await {
            if let Err(e) = self.event_channel.send(event).await {
                warn!("Failed to send event: {}", e);
            }
        }

        Ok(())
    }
}

async fn create_change_stream(
    postgres: &PostgresClient,
    database: &Database,
    entity: &str,
    collection: &str,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = OplogEvent> + Send>>> {
    let resume_token = resume_token::fetch(postgres, entity).await?;
    let resume_token = resume_token.map(|token| token.token_data());
    let has_resume_token = resume_token.is_some();

    let change_stream = start_stream(database.collection(collection), resume_token, entity.to_string()).await?;

    if has_resume_token {
        Ok(change_stream.boxed())
    } else {
        let scan_stream = collection_scan(database, entity.to_string(), collection).await?;
        Ok(scan_stream.chain(change_stream).boxed())
    }
}

async fn collection_scan(
    database: &Database,
    entity: String,
    collection: &str,
) -> anyhow::Result<impl Stream<Item = OplogEvent> + Send + 'static> {
    let cursor = database.collection(collection).find(bson::doc! {}).await?;
    let entity = entity.to_string();
    let stream = cursor
        .map(move |change| shaper::map_oplog_from_document(change.unwrap(), &entity))
        .filter_map(async move |oplog| oplog.ok());
    Ok(stream)
}

async fn start_stream(
    collection: Collection<Document>,
    resume_token: Option<ResumeToken>,
    entity: String,
) -> anyhow::Result<impl Stream<Item = OplogEvent> + Send + 'static> {
    let post_image = FullDocumentType::WhenAvailable;
    let stream = collection
        .watch()
        .full_document(post_image)
        .resume_after(resume_token)
        .await?;

    let stream = stream
        .map(move |change| shaper::map_oplog_from_change(change.unwrap(), &entity))
        .filter_map(async move |oplog| oplog.ok().flatten());

    Ok(stream)
}

impl Task for MongoClient {
    fn name(&self) -> String {
        "change_stream".to_string()
    }

    fn execute(&self) -> impl Future<Output = anyhow::Result<()>> + Send {
        self.start_stream()
    }

    fn shutdown(&self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
