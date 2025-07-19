mod resume_token;
mod shaper;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    mem,
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
    error::{Error as MongoError, ErrorKind},
    options::FullDocumentType,
};
use thiserror::Error;
use tokio::{
    sync::{Mutex, broadcast::error::RecvError, mpsc},
    task::{AbortHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    config::Configs,
    entity::EntityManager,
    executor::Task,
    mongo::shaper::OplogError,
    types::{Entity, OplogEvent},
};
pub use resume_token::ResumeTokenManager;

#[derive(Debug, Error)]
enum StreamError {
    #[error("Oplog error: {1:?}")]
    OplogError(Entity, OplogError),
    #[error("Stream error: {1:?}")]
    StreamError(Entity, anyhow::Error),
}

type StreamResult = Result<Entity, StreamError>;

pub struct MongoClient {
    sources: HashMap<String, Database>,
    event_channel: mpsc::Sender<OplogEvent>,
    entity_manager: Arc<EntityManager>,
    active_streams: Arc<Mutex<HashMap<String, AbortHandle>>>,
    token_manager: Arc<ResumeTokenManager>,
    join_set: Mutex<JoinSet<StreamResult>>,
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
            join_set: Mutex::new(JoinSet::new()),
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
        let mut join_set = self.join_set.lock().await;

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

                    if let Err(e) = self.sync_streams(&mut join_set).await {
                        error!("Failed to sync streams: {}", e);
                    }
                }
                res = join_set.join_next() => {
                    match res {
                        Some(Ok(res)) => {
                            self.handle_stream_result(res).await;
                        }
                        Some(Err(e)) => {
                            error!("Failed to join stream: {e:?}");
                        }
                        None => {
                            info!("No stream to join, pausing for 10 seconds");
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        }
                    }
                }
            }
        }

        let join_set = mem::take(&mut *join_set);
        self.stop_all_streams(join_set).await;

        info!("Entity monitor stopped");
        Ok(())
    }

    async fn handle_stream_result(&self, res: StreamResult) {
        match res {
            Ok(entity) => {
                info!("Stream stopped for entity: {}", entity.name);
            }
            Err(StreamError::OplogError(entity, e)) => match e {
                OplogError::DocumentError(e) => {
                    error!("Document error for entity {}: {e}", entity.name);
                }
                OplogError::ExtractDataError(e) => error!("Extract data error for entity {}: {e}", entity.name),
                OplogError::Invalidate => {
                    info!("Invalidate stream event for entity {}: {e}", entity.name);
                    let _ = self.token_manager.delete(&entity.name).await;
                    self.active_streams.lock().await.remove(&entity.name);
                }
            },
            Err(StreamError::StreamError(entity, e)) => {
                error!("Stream error for entity {}: {e}", entity.name);
            }
        }
    }

    async fn sync_streams(&self, join_set: &mut JoinSet<StreamResult>) -> anyhow::Result<()> {
        debug!("Syncing live mongo change streams with entities");
        let entities = self.entity_manager.get_all_entities();

        let mut handles = self.active_streams.lock().await;
        let current_entity_names: HashSet<String> = entities.iter().map(|e| e.name.clone()).collect();

        let mut to_remove = Vec::new();
        for (entity_name, active_stream) in handles.iter() {
            if !current_entity_names.contains(entity_name) {
                info!("Stopping stream for removed entity: {}", entity_name);
                active_stream.abort();
                to_remove.push(entity_name.clone());
            }
        }

        for entity_name in to_remove {
            if let Some(stream) = handles.remove(&entity_name) {
                stream.abort();
            }
        }

        for entity in entities {
            if !handles.contains_key(&entity.name) {
                match self.start_entity_stream(&entity, join_set).await {
                    Ok(handle) => {
                        info!("Started stream for entity: {}", entity.name);
                        handles.insert(entity.name.clone(), handle);
                    }
                    Err(e) => {
                        error!("Failed to start stream for entity {}: {}", entity.name, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn start_entity_stream(
        &self,
        entity: &Entity,
        join_set: &mut JoinSet<StreamResult>,
    ) -> anyhow::Result<AbortHandle> {
        let entity_clone = entity.clone();
        let event_channel = self.event_channel.clone();
        let token_manager = self.token_manager.clone();
        let sources = self.sources.clone();

        let source_db = sources
            .get(&entity.client)
            .with_context(|| format!("Database '{}' not found for entity '{}'", entity.client, entity.name))?
            .clone();

        let handle = join_set.spawn(async move {
            Self::stream_entity_changes(token_manager, source_db, event_channel, entity_clone).await
        });

        Ok(handle)
    }

    async fn stream_entity_changes(
        token_manager: Arc<ResumeTokenManager>,
        source_db: Database,
        event_channel: mpsc::Sender<OplogEvent>,
        entity: Entity,
    ) -> StreamResult {
        let entity_name = &entity.name;
        let source = &entity.source;
        info!("Starting changestream for entity: {entity_name} (source: {source})");

        let mut stream = create_change_stream(token_manager, &source_db, &entity)
            .await
            .map_err(|e| StreamError::StreamError(entity.clone(), e))?;

        while let Some(event) = stream.next().await {
            let event = event.map_err(|e| StreamError::OplogError(entity.clone(), e))?;
            if let Err(e) = event_channel.send(event).await {
                warn!("Failed to send event for entity '{entity_name}': {e}");
                return Err(StreamError::StreamError(entity.clone(), e.into()));
            }
        }

        info!("Change stream for entity '{entity_name}' stopped");
        Ok(entity)
    }

    async fn stop_all_streams(&self, mut join_set: JoinSet<StreamResult>) {
        let mut active_streams = self.active_streams.lock().await;
        info!("Stopping {} active streams", active_streams.len());

        active_streams.clear();
        join_set.abort_all();

        join_set.join_all().await;
        info!("All streams stopped");
    }
}

fn resume_token_error(error: &anyhow::Error) -> bool {
    if let Some(mongo_error) = error.downcast_ref::<MongoError>() {
        match mongo_error.kind.as_ref() {
            ErrorKind::MissingResumeToken => true,
            ErrorKind::Command(command_error) => {
                let message = command_error.message.to_lowercase();
                message.contains("resume") ||
                command_error.code == 260 || // InvalidResumeToken
                command_error.code == 280 || // ChangeStreamFatalError
                command_error.code == 286 || // ChangeStreamHistoryLost
                command_error.code == 222 || // CloseChangeStream
                command_error.code == 234 // RetryChangeStream
            }
            _ => false,
        }
    } else {
        let error_msg = error.to_string().to_lowercase();
        error_msg.contains("resume token")
            || error_msg.contains("resumetoken")
            || error_msg.contains("change stream")
            || error_msg.contains("oplog")
            || error_msg.contains("cursor not found")
    }
}

async fn create_change_stream(
    resume_token_manager: Arc<ResumeTokenManager>,
    database: &Database,
    entity: &Entity,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<OplogEvent, OplogError>> + Send>>> {
    let entity_name = entity.name.clone();
    let resume_token = resume_token_manager.fetch(&entity_name).await?;
    let resume_token = resume_token.map(|token| token.token_data());
    let Some(resume_token) = resume_token else {
        info!("Starting stream for entity '{}' without resume token", entity_name);
        return restart_stream(database, entity.clone()).await;
    };

    let change_stream = start_stream(database.collection(&entity.source), Some(resume_token), entity.clone()).await;

    match change_stream {
        Ok(change_stream) => {
            info!("Started stream for entity '{entity_name}' with resume token");
            Ok(change_stream.boxed())
        }
        Err(e) if resume_token_error(&e) => {
            error!("resume token error {e:?} for entity '{entity_name}', restarting stream");
            restart_stream(database, entity.clone()).await
        }
        Err(e) => Err(e),
    }
}

async fn restart_stream(
    database: &Database,
    entity: Entity,
) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<OplogEvent, OplogError>> + Send>>> {
    let scan_stream = collection_scan(database, entity.clone()).await?;
    let change_stream = start_stream(database.collection(&entity.source), None, entity.clone()).await?;
    Ok(scan_stream.chain(change_stream).boxed())
}

async fn collection_scan(
    database: &Database,
    entity: Entity,
) -> anyhow::Result<impl Stream<Item = Result<OplogEvent, OplogError>> + Send + 'static> {
    let cursor = database.collection(&entity.source).find(bson::doc! {}).await?;
    info!("Starting collection scan for entity '{}'", &entity.name);
    let stream = cursor.map(move |change| shaper::map_oplog_from_document(change.unwrap(), &entity));
    Ok(stream)
}

async fn start_stream(
    collection: Collection<Document>,
    resume_token: Option<ResumeToken>,
    entity: Entity,
) -> anyhow::Result<impl Stream<Item = Result<OplogEvent, OplogError>> + Send + 'static> {
    let post_image = FullDocumentType::UpdateLookup;
    let stream = collection
        .watch()
        .full_document(post_image)
        .resume_after(resume_token)
        .await?;

    info!("Started change stream for entity '{}'", &entity.name);

    let stream = stream
        .map(move |change| shaper::map_oplog_from_change(change.unwrap(), &entity))
        .filter_map(async move |oplog| oplog.transpose());

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
