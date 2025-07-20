use std::fmt::{self, Display};

use crate::replicator::migrator::{DATA_SYNC_TABLE, DATA_TABLE};

const MODE_LIVE: &str = "live";
const MODE_SYNC: &str = "sync";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Live,
    Sync,
}

impl Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode: String = (*self).into();
        write!(f, "{mode}")
    }
}

impl From<Mode> for String {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Live => MODE_LIVE.to_string(),
            Mode::Sync => MODE_SYNC.to_string(),
        }
    }
}

impl From<String> for Mode {
    fn from(s: String) -> Self {
        match s.as_str() {
            MODE_LIVE => Mode::Live,
            MODE_SYNC => Mode::Sync,
            _ => Mode::Live,
        }
    }
}

impl Mode {
    pub fn table_name(&self) -> &str {
        match self {
            Mode::Live => DATA_TABLE,
            Mode::Sync => DATA_SYNC_TABLE,
        }
    }
}
