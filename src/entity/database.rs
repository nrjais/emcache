use tracing::{debug, info};

use crate::{
    storage::PostgresClient,
    types::{Entity, Shape},
};

pub struct EntityDatabase {
    db: PostgresClient,
}

impl EntityDatabase {
    pub fn new(db: PostgresClient) -> Self {
        Self { db }
    }

    pub async fn get_all_entities(&self) -> anyhow::Result<Vec<Entity>> {
        debug!("Getting all entities");

        let rows = sqlx::query!("SELECT id, name, source, shape, created_at FROM entities",)
            .fetch_all(self.db.postgres())
            .await?;

        let mut entities = Vec::new();
        for row in rows {
            let shape: Shape = serde_json::from_value(row.shape)?;
            entities.push(Entity {
                id: row.id as i64,
                name: row.name,
                source: row.source,
                shape,
                created_at: row.created_at,
            });
        }

        debug!("Found {} entities", entities.len());
        Ok(entities)
    }

    /// Create entity with Entity struct
    pub async fn create_entity(&self, entity: Entity) -> anyhow::Result<Entity> {
        info!("Creating entity from struct: {}", entity.name);

        let shape_json = serde_json::to_value(&entity.shape)?;

        let row = sqlx::query_scalar!(
            r#"
            INSERT INTO entities (name, source, shape, created_at)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            &entity.name,
            &entity.source,
            &shape_json,
            &entity.created_at
        )
        .fetch_one(self.db.postgres())
        .await?;

        let mut created_entity = entity;
        created_entity.id = row as i64;

        info!("Entity created successfully: {}", created_entity.name);
        Ok(created_entity)
    }

    pub async fn delete_entity(&self, name: &str) -> anyhow::Result<()> {
        info!("Deleting entity: {}", name);

        let _ = sqlx::query!("DELETE FROM entities WHERE name = $1", name)
            .execute(self.db.postgres())
            .await?;

        Ok(())
    }
}
