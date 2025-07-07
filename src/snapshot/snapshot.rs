use std::{
    fs::File,
    path::Path,
    time::{Duration, Instant},
};

use tempfile::{Builder, NamedTempFile};

pub struct Snapshot {
    file: NamedTempFile,
    timestamp: Instant,
}

pub struct SnapshotRef {
    file: File,
}

impl Snapshot {
    pub fn new(entity_name: &str) -> anyhow::Result<Self> {
        let file = Builder::new()
            .prefix(entity_name)
            .suffix("-snap.db")
            .rand_bytes(5)
            .tempfile()?;
        Ok(Self {
            file,
            timestamp: Instant::now(),
        })
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }

    pub fn clone(&self) -> anyhow::Result<SnapshotRef> {
        let file = self.file.reopen()?;
        Ok(SnapshotRef { file })
    }

    pub fn is_stale(&self, threshold: Duration) -> bool {
        self.timestamp.elapsed() > threshold
    }
}
