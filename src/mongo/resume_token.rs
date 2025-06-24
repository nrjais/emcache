use mongodb::change_stream::event::ResumeToken;

use crate::storage::postgres::PostgresClient;

#[derive(Debug, sqlx::FromRow)]
pub struct CollectionResumeToken {
    token_data: serde_json::Value,
}

impl CollectionResumeToken {
    pub fn token_data(&self) -> ResumeToken {
        serde_json::from_value(self.token_data.clone()).unwrap()
    }
}

pub async fn fetch(
    postgres: &PostgresClient,
    entity: &str,
) -> anyhow::Result<Option<CollectionResumeToken>> {
    let resume_token = sqlx::query_as!(
        CollectionResumeToken,
        "SELECT token_data FROM mongo_resume_tokens WHERE entity = $1",
        entity
    )
    .fetch_optional(postgres.postgres())
    .await?;

    Ok(resume_token)
}

pub(crate) async fn save(
    postgres: &PostgresClient,
    entity: &str,
    data: &serde_json::Value,
) -> Result<(), anyhow::Error> {
    let _ = sqlx::query!(
        "INSERT INTO mongo_resume_tokens (entity, token_data) VALUES ($1, $2) ON CONFLICT (entity) DO UPDATE SET token_data = $2",
        entity,
        data
    )
    .execute(postgres.postgres())
    .await?;
    Ok(())
}
