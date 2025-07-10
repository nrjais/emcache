use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use chrono::Utc;
use tokio::fs::create_dir_all;
use tracing::error;

pub const SNAPSHOT_DIR: &str = "snapshots";

#[derive(Debug)]
struct DeleteOnDrop {
    file_path: PathBuf,
    _ref_count: Arc<()>,
}

impl DeleteOnDrop {
    fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            _ref_count: Arc::new(()),
        }
    }
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        if Arc::strong_count(&self._ref_count) == 1 {
            if let Err(e) = std::fs::remove_file(&self.file_path) {
                error!("Failed to remove snapshot file {:?}: {}", self.file_path, e);
            }
        }
    }
}

pub struct SnapshotRef {
    file_path: PathBuf,
    guard: DeleteOnDrop,
}

impl SnapshotRef {
    pub async fn new(entity_name: &str, base_dir: &str) -> anyhow::Result<Self> {
        let now = Utc::now();
        let time_str = now.format("%Y%m%d-%H%M%S").to_string();

        let entity_dir = format!("{base_dir}/{SNAPSHOT_DIR}/{entity_name}");
        create_dir_all(&entity_dir).await?;

        let file_path = PathBuf::from(format!("{entity_dir}/{time_str}-snap.db"));
        let guard = DeleteOnDrop::new(file_path.clone());

        Ok(Self { file_path, guard })
    }

    pub fn path(&self) -> &Path {
        &self.guard.file_path
    }

    pub fn open(&self) -> anyhow::Result<File> {
        File::open(&self.file_path).context("Failed to open snapshot file")
    }
}
