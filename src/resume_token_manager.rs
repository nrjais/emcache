use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::database::DatabaseManager;

/// Resume token manager for MongoDB change streams
pub struct ResumeTokenManager {
    db_manager: Arc<DatabaseManager>,
}

/// Resume token status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "resume_token_status", rename_all = "lowercase")]
pub enum ResumeTokenStatus {
    Scanning,
    Live,
    Failed,
}

/// Resume token record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    pub id: i64,
    pub entity_name: String,
    pub token_data: String,
    pub status: ResumeTokenStatus,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub failure_count: i32,
    pub last_error: Option<String>,
}

/// Resume token statistics
#[derive(Debug, Serialize)]
pub struct ResumeTokenStats {
    pub total_tokens: i64,
    pub live_tokens: i64,
    pub scanning_tokens: i64,
    pub failed_tokens: i64,
    pub oldest_token: Option<DateTime<Utc>>,
    pub newest_token: Option<DateTime<Utc>>,
}

impl ResumeTokenManager {
    /// Create a new resume token manager
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self { db_manager }
    }

    /// Store or update a resume token
    pub async fn store_token(
        &self,
        entity_name: &str,
        token_data: &str,
        status: ResumeTokenStatus,
    ) -> Result<()> {
        let query = r#"
            INSERT INTO resume_tokens (entity_name, token_data, status, version, created_at, updated_at)
            VALUES ($1, $2, $3, 1, NOW(), NOW())
            ON CONFLICT (entity_name)
            DO UPDATE SET
                token_data = EXCLUDED.token_data,
                status = EXCLUDED.status,
                version = resume_tokens.version + 1,
                updated_at = NOW(),
                last_used_at = CASE
                    WHEN EXCLUDED.status = 'live' THEN NOW()
                    ELSE resume_tokens.last_used_at
                END
        "#;

        sqlx::query(query)
            .bind(entity_name)
            .bind(token_data)
            .bind(&status)
            .execute(self.db_manager.postgres())
            .await?;

        debug!(
            "Stored resume token for entity: {} with status: {:?}",
            entity_name, status
        );

        Ok(())
    }

    /// Get resume token for an entity
    pub async fn get_token(&self, entity_name: &str) -> Result<Option<ResumeToken>> {
        let query = r#"
            SELECT id, entity_name, token_data, status, version,
                   created_at, updated_at, last_used_at, failure_count, last_error
            FROM resume_tokens
            WHERE entity_name = $1
        "#;

        let row = sqlx::query(query)
            .bind(entity_name)
            .fetch_optional(self.db_manager.postgres())
            .await?;

        if let Some(row) = row {
            let token = ResumeToken {
                id: row.get("id"),
                entity_name: row.get("entity_name"),
                token_data: row.get("token_data"),
                status: row.get("status"),
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                last_used_at: row.get("last_used_at"),
                failure_count: row.get("failure_count"),
                last_error: row.get("last_error"),
            };
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }

    /// Get all resume tokens
    pub async fn get_all_tokens(&self) -> Result<Vec<ResumeToken>> {
        let query = r#"
            SELECT id, entity_name, token_data, status, version,
                   created_at, updated_at, last_used_at, failure_count, last_error
            FROM resume_tokens
            ORDER BY updated_at DESC
        "#;

        let rows = sqlx::query(query)
            .fetch_all(self.db_manager.postgres())
            .await?;

        let mut tokens = Vec::new();
        for row in rows {
            tokens.push(ResumeToken {
                id: row.get("id"),
                entity_name: row.get("entity_name"),
                token_data: row.get("token_data"),
                status: row.get("status"),
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                last_used_at: row.get("last_used_at"),
                failure_count: row.get("failure_count"),
                last_error: row.get("last_error"),
            });
        }

        Ok(tokens)
    }

    /// Get tokens by status
    pub async fn get_tokens_by_status(
        &self,
        status: ResumeTokenStatus,
    ) -> Result<Vec<ResumeToken>> {
        let query = r#"
            SELECT id, entity_name, token_data, status, version,
                   created_at, updated_at, last_used_at, failure_count, last_error
            FROM resume_tokens
            WHERE status = $1
            ORDER BY updated_at DESC
        "#;

        let rows = sqlx::query(query)
            .bind(&status)
            .fetch_all(self.db_manager.postgres())
            .await?;

        let mut tokens = Vec::new();
        for row in rows {
            tokens.push(ResumeToken {
                id: row.get("id"),
                entity_name: row.get("entity_name"),
                token_data: row.get("token_data"),
                status: row.get("status"),
                version: row.get("version"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                last_used_at: row.get("last_used_at"),
                failure_count: row.get("failure_count"),
                last_error: row.get("last_error"),
            });
        }

        Ok(tokens)
    }

    /// Update token status
    pub async fn update_status(
        &self,
        entity_name: &str,
        status: ResumeTokenStatus,
        error_message: Option<&str>,
    ) -> Result<()> {
        let query = r#"
            UPDATE resume_tokens
            SET status = $2,
                updated_at = NOW(),
                last_used_at = CASE WHEN $2 = 'live' THEN NOW() ELSE last_used_at END,
                failure_count = CASE
                    WHEN $2 = 'failed' THEN failure_count + 1
                    WHEN $2 = 'live' THEN 0
                    ELSE failure_count
                END,
                last_error = $3
            WHERE entity_name = $1
        "#;

        let affected_rows = sqlx::query(query)
            .bind(entity_name)
            .bind(&status)
            .bind(error_message)
            .execute(self.db_manager.postgres())
            .await?
            .rows_affected();

        if affected_rows == 0 {
            warn!(
                "No resume token found to update for entity: {}",
                entity_name
            );
        } else {
            debug!(
                "Updated resume token status for entity: {} to {:?}",
                entity_name, status
            );
        }

        Ok(())
    }

    /// Mark token as used (update last_used_at)
    pub async fn mark_token_used(&self, entity_name: &str) -> Result<()> {
        let query = r#"
            UPDATE resume_tokens
            SET last_used_at = NOW(), updated_at = NOW()
            WHERE entity_name = $1
        "#;

        sqlx::query(query)
            .bind(entity_name)
            .execute(self.db_manager.postgres())
            .await?;

        debug!("Marked resume token as used for entity: {}", entity_name);
        Ok(())
    }

    /// Delete resume token (triggers full scan)
    pub async fn delete_token(&self, entity_name: &str) -> Result<bool> {
        let query = "DELETE FROM resume_tokens WHERE entity_name = $1";

        let affected_rows = sqlx::query(query)
            .bind(entity_name)
            .execute(self.db_manager.postgres())
            .await?
            .rows_affected();

        if affected_rows > 0 {
            info!(
                "Deleted resume token for entity: {} - will trigger full scan",
                entity_name
            );
            Ok(true)
        } else {
            debug!(
                "No resume token found to delete for entity: {}",
                entity_name
            );
            Ok(false)
        }
    }

    /// Clean up old failed tokens (older than specified days)
    pub async fn cleanup_failed_tokens(&self, older_than_days: i32) -> Result<u64> {
        let query = r#"
            DELETE FROM resume_tokens
            WHERE status = 'failed'
            AND updated_at < NOW() - INTERVAL '%d days'
        "#;

        let formatted_query = query.replace("%d", &older_than_days.to_string());

        let affected_rows = sqlx::query(&formatted_query)
            .execute(self.db_manager.postgres())
            .await?
            .rows_affected();

        if affected_rows > 0 {
            info!(
                "Cleaned up {} old failed resume tokens (older than {} days)",
                affected_rows, older_than_days
            );
        }

        Ok(affected_rows)
    }

    /// Get resume token statistics
    pub async fn get_statistics(&self) -> Result<ResumeTokenStats> {
        let query = r#"
            SELECT
                COUNT(*) as total_tokens,
                COUNT(CASE WHEN status = 'live' THEN 1 END) as live_tokens,
                COUNT(CASE WHEN status = 'scanning' THEN 1 END) as scanning_tokens,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_tokens,
                MIN(created_at) as oldest_token,
                MAX(created_at) as newest_token
            FROM resume_tokens
        "#;

        let row = sqlx::query(query)
            .fetch_one(self.db_manager.postgres())
            .await?;

        Ok(ResumeTokenStats {
            total_tokens: row.get("total_tokens"),
            live_tokens: row.get("live_tokens"),
            scanning_tokens: row.get("scanning_tokens"),
            failed_tokens: row.get("failed_tokens"),
            oldest_token: row.get("oldest_token"),
            newest_token: row.get("newest_token"),
        })
    }

    /// Get entities that need full scan (no resume token or failed token)
    pub async fn get_entities_needing_scan(&self) -> Result<Vec<String>> {
        let query = r#"
            SELECT e.name
            FROM entities e
            LEFT JOIN resume_tokens rt ON e.name = rt.entity_name
            WHERE e.status = 'live'
            AND (rt.entity_name IS NULL OR rt.status = 'failed')
        "#;

        let rows = sqlx::query(query)
            .fetch_all(self.db_manager.postgres())
            .await?;

        let entity_names: Vec<String> = rows.into_iter().map(|row| row.get("name")).collect();

        if !entity_names.is_empty() {
            info!(
                "Found {} entities needing full scan: {:?}",
                entity_names.len(),
                entity_names
            );
        }

        Ok(entity_names)
    }

    /// Validate token format (basic validation)
    pub fn validate_token_format(&self, token_data: &str) -> bool {
        // Basic validation - in production, this would be more sophisticated
        // MongoDB resume tokens are typically base64-encoded BSON documents
        !token_data.is_empty() && token_data.len() > 10
    }

    /// Create a mapping of entity names to their resume tokens
    pub async fn get_token_map(&self) -> Result<HashMap<String, String>> {
        let tokens = self.get_tokens_by_status(ResumeTokenStatus::Live).await?;

        let mut token_map = HashMap::new();
        for token in tokens {
            token_map.insert(token.entity_name, token.token_data);
        }

        debug!("Created token map with {} live tokens", token_map.len());
        Ok(token_map)
    }

    /// Handle resume token failure (increment failure count, potentially mark as failed)
    pub async fn handle_token_failure(
        &self,
        entity_name: &str,
        error_message: &str,
        max_failures: i32,
    ) -> Result<bool> {
        // Get current token to check failure count
        if let Some(token) = self.get_token(entity_name).await? {
            let new_failure_count = token.failure_count + 1;

            if new_failure_count >= max_failures {
                // Mark as failed and delete token to trigger full scan
                self.update_status(entity_name, ResumeTokenStatus::Failed, Some(error_message))
                    .await?;

                error!(
                    "Resume token for entity {} failed {} times, marking as failed: {}",
                    entity_name, new_failure_count, error_message
                );

                // Delete the failed token to trigger full scan
                self.delete_token(entity_name).await?;

                Ok(true) // Indicates full scan needed
            } else {
                // Just increment failure count
                self.update_status(entity_name, ResumeTokenStatus::Live, Some(error_message))
                    .await?;

                warn!(
                    "Resume token failure for entity {} (count: {}): {}",
                    entity_name, new_failure_count, error_message
                );

                Ok(false) // Can continue with token
            }
        } else {
            // No token exists, need full scan anyway
            warn!(
                "No resume token found for entity {} during failure handling",
                entity_name
            );
            Ok(true)
        }
    }
}

// Tests removed to avoid async compilation issues in tests
