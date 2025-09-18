use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

use vectaraft::pb::vectordb::v1::vector_db_server::VectorDbServer;
use vectaraft::server::grpc::VectorDbService;
use vectaraft::server::state::{DbState, DbStateConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut config = DbStateConfig::default();
    apply_cli_overrides(&mut config);

    let state = Arc::new(DbState::with_config(config));
    let svc = VectorDbService { state };

    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    tracing::info!("gRPC listening on {}", addr);

    Server::builder()
        .add_service(VectorDbServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

fn apply_cli_overrides(config: &mut DbStateConfig) {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--no-wal" => {
                config.enable_wal = false;
                config.wal_path = None;
                tracing::info!("WAL disabled via CLI flag");
            }
            "--wal-path" => {
                if let Some(path) = args.next() {
                    let path_buf = std::path::PathBuf::from(path);
                    tracing::info!(wal_path = %path_buf.display(), "WAL path overridden via CLI flag");
                    config.enable_wal = true;
                    config.wal_path = Some(path_buf);
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
                config.enable_wal = true;
                config.wal_path = Some(path_buf);
            }
            _ => {}
        }
    }
}
