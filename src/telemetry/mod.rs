use std::{net::SocketAddr, sync::Arc};

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{Encoder, Opts, Registry, TextEncoder, CounterVec, Gauge};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    grpc_requests_total: CounterVec,
    collections_total: Gauge,
    points_total: Gauge,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Arc<Self>> {
        let registry = Registry::new();

        let grpc_requests_total = CounterVec::new(
            Opts::new("grpc_requests_total", "Total gRPC requests handled"),
            &["method", "status"],
        )?;
        let collections_total = Gauge::with_opts(Opts::new(
            "collections_total",
            "Number of collections currently registered",
        ))?;
        let points_total = Gauge::with_opts(Opts::new(
            "points_total",
            "Number of points stored across all collections",
        ))?;

        registry.register(Box::new(grpc_requests_total.clone()))?;
        registry.register(Box::new(collections_total.clone()))?;
        registry.register(Box::new(points_total.clone()))?;

        Ok(Arc::new(Self {
            registry,
            grpc_requests_total,
            collections_total,
            points_total,
        }))
    }

    pub fn record_grpc(&self, method: &str, status: &str) {
        self.grpc_requests_total
            .with_label_values(&[method, status])
            .inc();
    }

    pub fn set_collection_count(&self, value: usize) {
        self.collections_total.set(value as f64);
    }

    pub fn set_point_count(&self, value: usize) {
        self.points_total.set(value as f64);
    }

    fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(self)
    }
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(?err, "failed to encode metrics");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match String::from_utf8(buffer) {
        Ok(body) => (StatusCode::OK, body).into_response(),
        Err(err) => {
            tracing::error!(?err, "failed to convert metrics to UTF-8");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn serve(metrics: Arc<Metrics>, addr: SocketAddr) -> anyhow::Result<()> {
    let router = metrics.clone().router();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("metrics server listening on {}", addr);
    axum::serve(listener, router.into_make_service()).await?;
    Ok(())
}

pub fn spawn(metrics: Arc<Metrics>, addr: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = serve(metrics, addr).await {
            tracing::error!(?err, "metrics server stopped");
        }
    })
}
