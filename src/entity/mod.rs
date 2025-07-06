mod database;

use std::collections::HashSet;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::entity::database::EntityDatabase;
use crate::executor::Task;
use crate::storage::PostgresClient;
use crate::types::Entity;

pub struct EntityManager {
    db: EntityDatabase,
    cache: DashMap<String, Entity>,
    broadcast_tx: broadcast::Sender<()>,
}

impl EntityManager {
    pub fn new(db: PostgresClient) -> Self {
        let (tx, _) = broadcast::channel(1);
        Self {
            db: EntityDatabase::new(db),
            cache: DashMap::new(),
            broadcast_tx: tx,
        }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        self.refresh_entities().await?;
        Ok(())
    }

    pub async fn get_all_entities(&self) -> anyhow::Result<Vec<Entity>> {
        let mut entities = vec![];
        for entity in self.cache.iter() {
            entities.push(entity.value().clone());
        }

        Ok(entities)
    }

    pub async fn refresh_entities(&self) -> anyhow::Result<Vec<Entity>> {
        let entities = self.db.get_all_entities().await?;
        let names_set = entities.iter().map(|e| e.name.clone()).collect::<HashSet<_>>();
        self.cache.retain(|name, _| names_set.contains(name));

        for entity in &entities {
            self.cache.insert(entity.name.clone(), entity.clone());
        }
        let _ = self.broadcast_tx.send(());

        Ok(entities)
    }

    pub async fn create_entity(&self, entity: Entity) -> anyhow::Result<Entity> {
        let entity = self.db.create_entity(entity).await?;
        self.refresh_entities().await?;
        Ok(entity)
    }

    pub fn get_entity(&self, name: &str) -> Option<Entity> {
        self.cache.get(name).map(|e| e.value().clone())
    }

    pub async fn delete_entity(&self, name: &str) -> anyhow::Result<()> {
        self.db.delete_entity(name).await?;
        self.cache.remove(name);
        self.refresh_entities().await?;
        Ok(())
    }

    pub fn broadcast(&self) -> broadcast::Receiver<()> {
        self.broadcast_tx.subscribe()
    }

    async fn refresh_loop(&self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let mut interval = interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.refresh_entities().await {
                        error!("Failed to refresh entities: {}", e);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Task for EntityManager {
    fn name(&self) -> String {
        "entity_manager".to_string()
    }

    fn execute(&self, cancellation_token: CancellationToken) -> impl Future<Output = anyhow::Result<()>> + Send {
        self.refresh_loop(cancellation_token)
    }
}
