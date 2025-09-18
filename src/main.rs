use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

use vectaraft::pb::vectordb::v1::vector_db_server::VectorDbServer;
use vectaraft::server::grpc::VectorDbService;
use vectaraft::server::state::{DbState, DbStateConfig};
use vectaraft::telemetry::Metrics;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut config = RuntimeConfig::default();
    apply_cli_overrides(&mut config);

    let state = Arc::new(DbState::with_config(config.db.clone()));

    let metrics = if config.metrics.enable {
        match Metrics::new() {
            Ok(metrics) => {
                metrics.set_collection_count(state.catalog.len());
                metrics.set_point_count(state.catalog.total_points());
                vectaraft::telemetry::spawn(metrics.clone(), config.metrics.addr);
                Some(metrics)
            }
            Err(err) => {
                tracing::error!(?err, "failed to initialize metrics; running without telemetry");
                None
            }
        }
    } else {
        None
    };

    let svc = VectorDbService { state, metrics: metrics.clone() };

    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    tracing::info!("gRPC listening on {}", addr);

    Server::builder()
        .add_service(VectorDbServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

fn apply_cli_overrides(config: &mut RuntimeConfig) {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--no-wal" => {
                config.db.enable_wal = false;
                config.db.wal_path = None;
                tracing::info!("WAL disabled via CLI flag");
            }
            "--wal-path" => {
                if let Some(path) = args.next() {
                    let path_buf = std::path::PathBuf::from(path);
                    tracing::info!(wal_path = %path_buf.display(), "WAL path overridden via CLI flag");
                    config.db.enable_wal = true;
                    config.db.wal_path = Some(path_buf);
                } else {
                    tracing::warn!("--wal-path flag requires a value; ignoring");
                }
            }
            _ if arg.starts_with("--wal-path=") => {
                let path = &arg["--wal-path=".len()..];
                if path.is_empty() {
                    tracing::warn!("--wal-path flag requires a non-empty value; ignoring");
                    continue;
                }
                let path_buf = std::path::PathBuf::from(path);
                tracing::info!(wal_path = %path_buf.display(), "WAL path overridden via CLI flag");
                config.db.enable_wal = true;
                config.db.wal_path = Some(path_buf);
            }
            "--no-metrics" => {
                config.metrics.enable = false;
                tracing::info!("metrics disabled via CLI flag");
            }
            "--metrics-addr" => {
                if let Some(value) = args.next() {
                    match value.parse::<SocketAddr>() {
                        Ok(addr) => {
                            config.metrics.enable = true;
                            config.metrics.addr = addr;
                            tracing::info!(%addr, "metrics endpoint address overridden");
                        }
                        Err(err) => tracing::warn!(input = %value, ?err, "invalid --metrics-addr value; ignoring"),
                    }
                } else {
                    tracing::warn!("--metrics-addr flag requires a value; ignoring");
                }
            }
            _ if arg.starts_with("--metrics-addr=") => {
                let value = &arg["--metrics-addr=".len()..];
                match value.parse::<SocketAddr>() {
                    Ok(addr) => {
                        config.metrics.enable = true;
                        config.metrics.addr = addr;
                        tracing::info!(%addr, "metrics endpoint address overridden");
                    }
                    Err(err) => tracing::warn!(input = %value, ?err, "invalid --metrics-addr value; ignoring"),
                }
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeConfig {
    db: DbStateConfig,
    metrics: MetricsConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            db: DbStateConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

#[derive(Clone, Debug)]
struct MetricsConfig {
    enable: bool,
    addr: SocketAddr,
}

impl MetricsConfig {
    fn from_env() -> Self {
        let enable = std::env::var("VECTARAFT_ENABLE_METRICS")
            .ok()
            .and_then(|v| parse_bool(&v))
            .unwrap_or(true);
        let addr = std::env::var("VECTARAFT_METRICS_ADDR")
            .ok()
            .and_then(|s| s.parse::<SocketAddr>().ok())
            .unwrap_or_else(|| "127.0.0.1:9100".parse().expect("valid socket address"));
        Self { enable, addr }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

fn parse_bool(input: &str) -> Option<bool> {
    match input.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
