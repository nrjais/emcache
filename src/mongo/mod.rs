mod resume_token;
mod shaper;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use futures::{Stream, StreamExt};
use mongodb::{
    Client, Collection, Database,
    bson::{self, Document},
    change_stream::event::ResumeToken,
    options::FullDocumentType,
};
use tokio::{
    sync::{Mutex, broadcast::error::RecvError, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::Configs,
    entity::EntityManager,
    executor::Task,
    types::{Entity, OplogEvent},
};
pub use resume_token::ResumeTokenManager;

#[derive(Debug)]
struct ActiveStream {
    cancel_token: CancellationToken,
    handle: JoinHandle<anyhow::Result<()>>,
}

pub struct MongoClient {
    sources: HashMap<String, Database>,
    event_channel: mpsc::Sender<OplogEvent>,
    entity_manager: Arc<EntityManager>,
    active_streams: Arc<Mutex<HashMap<String, ActiveStream>>>,
    token_manager: Arc<ResumeTokenManager>,
}

impl MongoClient {
    pub async fn new(
        config: &Configs,
        event_channel: mpsc::Sender<OplogEvent>,
        entity_manager: Arc<EntityManager>,
        token_manager: Arc<ResumeTokenManager>,
    ) -> anyhow::Result<Self> {
        let sources = Self::init_sources(config).await?;
        Ok(Self {
            event_channel,
            sources,
            entity_manager,
            active_streams: Arc::new(Mutex::new(HashMap::new())),
            token_manager,
        })
    }

    async fn init_sources(config: &Configs) -> anyhow::Result<HashMap<String, Database>> {
        let mut sources = HashMap::new();
        for (name, source) in &config.sources {
            let client = Client::with_uri_str(&source.uri).await?;
            sources.insert(name.clone(), client.database(&source.database));
        }
        Ok(sources)
    }

    async fn monitor_entities(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        info!("Starting entity monitor for MongoDB change streams");

        let mut broadcast_rx = self.entity_manager.broadcast();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Entity monitor received shutdown signal");
                    break;
                }
                m = broadcast_rx.recv() => {
                    if let Err(RecvError::Lagged(_)) = m {
                        continue;
                    }
                    if let Err(RecvError::Closed) = m {
                        info!("Entity monitor received closed signal, stopping");
                        break;
                    }

                    if let Err(e) = self.sync_streams().await {
                        error!("Failed to sync streams: {}", e);
                    }
                }
            }
        }

        self.stop_all_streams().await;

        info!("Entity monitor stopped");
        Ok(())
    }

    async fn sync_streams(&self) -> anyhow::Result<()> {
        info!("Syncing live mongo change streams with entities");
        let entities = self.entity_manager.get_all_entities();

        let mut active_streams = self.active_streams.lock().await;
        let current_entity_names: HashSet<String> = entities.iter().map(|e| e.name.clone()).collect();

        let mut to_remove = Vec::new();
        for (entity_name, active_stream) in active_streams.iter() {
            if !current_entity_names.contains(entity_name) {
                info!("Stopping stream for removed entity: {}", entity_name);
                active_stream.cancel_token.cancel();
                to_remove.push(entity_name.clone());
            }
        }

        for entity_name in to_remove {
            if let Some(stream) = active_streams.remove(&entity_name) {
                let _ = timeout(Duration::from_secs(5), stream.handle).await;
            }
        }

        for entity in entities {
            if !active_streams.contains_key(&entity.name) {
                match self.start_entity_stream(&entity).await {
                    Ok(active_stream) => {
                        info!("Started stream for entity: {}", entity.name);
                        active_streams.insert(entity.name.clone(), active_stream);
                    }
                    Err(e) => {
                        error!("Failed to start stream for entity {}: {}", entity.name, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn start_entity_stream(&self, entity: &Entity) -> anyhow::Result<ActiveStream> {
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        let entity_clone = entity.clone();
        let event_channel = self.event_channel.clone();
        let token_manager = self.token_manager.clone();
        let sources = self.sources.clone();

        let source_db = sources
            .get(&entity.client)
            .with_context(|| format!("Database '{}' not found for entity '{}'", entity.client, entity.name))?
            .clone();

        let handle = tokio::spawn(async move {
            Self::stream_entity_changes(
                token_manager,
                source_db,
                event_channel,
                entity_clone,
                cancel_token_clone,
            )
            .await
        });

        Ok(ActiveStream { cancel_token, handle })
    }

    async fn stream_entity_changes(
        token_manager: Arc<ResumeTokenManager>,
        source_db: Database,
        event_channel: mpsc::Sender<OplogEvent>,
        entity: Entity,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        info!(
            "Starting change stream for entity: {} (source: {})",
            entity.name, entity.source
        );

        let mut stream = create_change_stream(token_manager, &source_db, &entity).await?;
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Change stream for entity '{}' received shutdown signal", entity.name);
                    break;
                }
                event = stream.next() => {
                    match event {
                        Some(oplog_event) => {
                            if let Err(e) = event_channel.send(oplog_event).await {
                                warn!("Failed to send event for entity '{}': {}", entity.name, e);
                                break;
                            }
                        }
                        None => {
                            warn!("Change stream for entity '{}' ended unexpectedly", entity.name);
                            break;
                        }
                    }
                }
            }
        }

        info!("Change stream for entity '{}' stopped", entity.name);
        Ok(())
    }

    async fn stop_all_streams(&self) {
        let mut active_streams = self.active_streams.lock().await;

        info!("Stopping {} active streams", active_streams.len());

        for (entity_name, active_stream) in active_streams.iter() {
            info!("Stopping stream for entity: {}", entity_name);
            active_stream.cancel_token.cancel();
        }

        let handles: Vec<_> = active_streams.drain().map(|(_, stream)| stream.handle).collect();

        for handle in handles {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }

        info!("All streams stopped");
    }
}

async fn create_change_stream(
    resume_token_manager: Arc<ResumeTokenManager>,
    database: &Database,
    entity: &Entity,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = OplogEvent> + Send>>> {
    let resume_token = resume_token_manager.fetch(&entity.name).await?;
    let resume_token = resume_token.map(|token| token.token_data());
    let has_resume_token = resume_token.is_some();

    let change_stream = start_stream(database.collection(&entity.source), resume_token, entity.clone()).await?;

    if has_resume_token {
        info!("Starting change stream for entity '{}' with resume token", &entity.name);
        Ok(change_stream.boxed())
    } else {
        info!(
            "Starting change stream for entity '{}' without resume token",
            &entity.name
        );
        let scan_stream = collection_scan(database, entity.clone()).await?;
        Ok(scan_stream.chain(change_stream).boxed())
    }
}

async fn collection_scan(
    database: &Database,
    entity: Entity,
) -> anyhow::Result<impl Stream<Item = OplogEvent> + Send + 'static> {
    let cursor = database.collection(&entity.source).find(bson::doc! {}).await?;
    info!("Starting collection scan for entity '{}'", &entity.name);
    let stream = cursor
        .map(move |change| shaper::map_oplog_from_document(change.unwrap(), &entity))
        .filter_map(async move |oplog| {
            if let Ok(oplog) = oplog {
                Some(oplog)
            } else {
                error!("Failed to map oplog for entity: {:?}", oplog);
                None
            }
        });
    Ok(stream)
}

async fn start_stream(
    collection: Collection<Document>,
    resume_token: Option<ResumeToken>,
    entity: Entity,
) -> anyhow::Result<impl Stream<Item = OplogEvent> + Send + 'static> {
    let post_image = FullDocumentType::UpdateLookup;
    let stream = collection
        .watch()
        .full_document(post_image)
        .resume_after(resume_token)
        .await?;

    info!("Started change stream for entity '{}'", &entity.name);

    let stream = stream
        .map(move |change| shaper::map_oplog_from_change(change.unwrap(), &entity))
        .filter_map(async move |oplog| oplog.ok().flatten());

    Ok(stream)
}

impl Task for MongoClient {
    fn name(&self) -> String {
        "change_stream".to_string()
    }

    fn execute(&self, cancellation_token: CancellationToken) -> impl Future<Output = anyhow::Result<()>> + Send {
        self.monitor_entities(cancellation_token)
    }
}
