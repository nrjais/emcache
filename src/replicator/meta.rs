use sqlx::SqlitePool;

const META_TABLE_NAME: &str = "meta";
const LAST_PROCESSED_ID: &str = "last_processed_id";

pub struct MetadataDb {
    db: SqlitePool,
}

impl MetadataDb {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    pub async fn get_last_processed_id(&self) -> anyhow::Result<i64> {
        let last_processed_id: Option<i64> =
            sqlx::query_scalar(&format!("SELECT value FROM {META_TABLE_NAME} WHERE key = ?"))
                .bind(LAST_PROCESSED_ID)
                .fetch_optional(&self.db)
                .await?;
        Ok(last_processed_id.unwrap_or(0))
    }

    pub async fn set_last_processed_id(&self, last_processed_id: i64) -> anyhow::Result<()> {
        sqlx::query(
            &format!(
                "INSERT INTO {META_TABLE_NAME} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            ),
        )
        .bind(LAST_PROCESSED_ID)
        .bind(last_processed_id)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {META_TABLE_NAME} (key TEXT PRIMARY KEY, value ANY) STRICT"
        ))
        .execute(&self.db)
        .await?;
        Ok(())
    }
}
