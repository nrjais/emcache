use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use chrono::Utc;
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
    timestamp: Instant,
}

impl SnapshotRef {
    pub fn new(entity_name: &str, base_dir: &str) -> anyhow::Result<Self> {
        let now = Utc::now();
        let time_str = now.format("%H%M").to_string();

        let entity_dir = format!("{base_dir}/{SNAPSHOT_DIR}/{entity_name}");
        std::fs::create_dir_all(&entity_dir)?;

        let file_path = PathBuf::from(format!("{entity_dir}/{time_str}-snap.db"));
        let guard = DeleteOnDrop::new(file_path.clone());

        Ok(Self {
            file_path,
            guard,
            timestamp: Instant::now(),
        })
    }

    pub fn from_existing(file_path: PathBuf) -> Self {
        Self {
            guard: DeleteOnDrop::new(file_path.clone()),
            file_path,
            timestamp: Instant::now(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.guard.file_path
    }

    pub fn open(&self) -> anyhow::Result<File> {
        File::open(&self.file_path).context("Failed to open snapshot file")
    }

    pub fn is_stale(&self, threshold: Duration) -> bool {
        self.timestamp.elapsed() > threshold
    }
}
