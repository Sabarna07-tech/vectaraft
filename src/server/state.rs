use std::{env, path::PathBuf};

use crate::catalog::{Catalog, PointWrite};
use crate::storage::wal::{Wal, WalRecord};
use crate::types::Metric;
use tracing::{error, warn};

/// Central database state: catalog and optional write-ahead log.
#[derive(Clone)]
pub struct DbState {
    pub catalog: Catalog,
    pub wal: Option<Wal>,
}

impl DbState {
    pub fn new() -> Self {
        Self::with_config(DbStateConfig::default())
    }

    pub fn with_config(config: DbStateConfig) -> Self {
        let catalog = Catalog::default();
        let wal = if config.enable_wal {
            match &config.wal_path {
                Some(path) => match Wal::open(path.clone()) {
                    Ok(wal) => Some(wal),
                    Err(err) => {
                        warn!(path = %path.display(), ?err, "failed to open WAL; continuing without durability");
                        None
                    }
                },
                None => None,
            }
        } else {
            None
        };

        let state = Self { catalog, wal };
        state.replay_wal();
        state
    }

    fn replay_wal(&self) {
        let Some(wal) = &self.wal else { return; };
        match wal.replay() {
            Ok(records) => {
                for rec in records {
                    match rec {
                        WalRecord::CreateCollection { name, dim, metric, .. } => {
                            let metric = Metric::from_str(&metric);
                            let _ = self.catalog.create_collection(name, dim as usize, metric);
                        }
                        WalRecord::Upsert { collection, id, vector, payload_json, .. } => {
                            if let Some(handle) = self.catalog.get(&collection) {
                                let _ = handle.upsert_points(vec![PointWrite {
                                    id,
                                    vector,
                                    payload_json,
                                }]);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!(?err, "failed to replay WAL; database will start empty");
            }
        }
    }

    pub fn append_wal(&self, record: WalRecord) {
        if let Some(wal) = &self.wal {
            if let Err(err) = wal.append(&record) {
                error!(?err, "failed to append WAL record");
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DbStateConfig {
    pub wal_path: Option<PathBuf>,
    pub enable_wal: bool,
}

impl Default for DbStateConfig {
    fn default() -> Self {
        let enable_wal = env::var("VECTARAFT_ENABLE_WAL")
            .ok()
            .and_then(|v| parse_bool(&v))
            .unwrap_or(true);

        let wal_path = if enable_wal {
            env::var("VECTARAFT_WAL_PATH")
                .ok()
                .map(PathBuf::from)
                .or_else(|| Some(PathBuf::from("data/wal.log")))
        } else {
            None
        };
        Self {
            wal_path,
            enable_wal,
        }
    }
}

fn parse_bool(input: &str) -> Option<bool> {
    match input.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
