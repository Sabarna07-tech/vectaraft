use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use crate::catalog::PointWrite;
use crate::pb::vectordb::v1::{
    vector_db_server::VectorDb,
    CreateCollectionRequest, CreateCollectionResponse,
    PingRequest, PingResponse,
    QueryRequest, QueryResponse,
    ScoredPoint,
    UpsertRequest, UpsertResponse,
};
use crate::server::state::DbState;
use crate::storage::wal::WalRecord;
use crate::types::Metric;
use uuid::Uuid;

#[derive(Clone)]
pub struct VectorDbService {
    pub state: Arc<DbState>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_millis() as i64)
        .unwrap_or_default()
}

#[tonic::async_trait]
impl VectorDb for VectorDbService {
    async fn ping(
        &self,
        _req: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {}))
    }

    async fn create_collection(
        &self,
        req: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req = req.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("collection name must be provided"));
        }
        if req.dims == 0 {
            return Err(Status::invalid_argument("dims must be greater than zero"));
        }
        let metric = Metric::from_str(&req.metric);
        let created = self
            .state
            .catalog
            .create_collection(req.name.clone(), req.dims as usize, metric);
        if !created {
            return Err(Status::already_exists("collection already exists"));
        }
        self.state.append_wal(WalRecord::CreateCollection {
            name: req.name,
            dim: req.dims,
            metric: req.metric,
            ts_ms: now_ms(),
        });
        Ok(Response::new(CreateCollectionResponse {}))
    }

    async fn upsert(
        &self,
        req: Request<UpsertRequest>,
    ) -> Result<Response<UpsertResponse>, Status> {
        let req = req.into_inner();
        if req.collection.is_empty() {
            return Err(Status::invalid_argument("collection must be specified"));
        }
        let Some(handle) = self.state.catalog.get(&req.collection) else {
            return Err(Status::not_found("collection not found"));
        };

        if req.points.is_empty() {
            return Ok(Response::new(UpsertResponse { upserted: 0 }));
        }

        let mut prepared = Vec::with_capacity(req.points.len());
        let mut wal_records = Vec::with_capacity(req.points.len());
        let ts = now_ms();
        for point in req.points.into_iter() {
            let id = if point.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                point.id
            };
            if point.vector.is_empty() {
                return Err(Status::invalid_argument("point vector must not be empty"));
            }
            let payload = point.payload_json;
            wal_records.push(WalRecord::Upsert {
                collection: req.collection.clone(),
                id: id.clone(),
                vector: point.vector.clone(),
                payload_json: payload.clone(),
                ts_ms: ts,
            });
            prepared.push(PointWrite {
                id,
                vector: point.vector,
                payload_json: payload,
            });
        }

        let inserted = handle
            .upsert_points(prepared)
            .ok_or_else(|| Status::invalid_argument("vector dimension mismatch"))?;

        for record in wal_records {
            self.state.append_wal(record);
        }

        Ok(Response::new(UpsertResponse {
            upserted: inserted as u32,
        }))
    }

    async fn query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = req.into_inner();
        if req.collection.is_empty() {
            return Err(Status::invalid_argument("collection must be specified"));
        }
        let Some(handle) = self.state.catalog.get(&req.collection) else {
            return Err(Status::not_found("collection not found"));
        };
        if req.vector.is_empty() {
            return Err(Status::invalid_argument("query vector must not be empty"));
        }
        let metric_override = if req.metric_override.is_empty() {
            None
        } else {
            Some(Metric::from_str(&req.metric_override))
        };
        let filters: Vec<(String, String)> = req
            .filters
            .into_iter()
            .map(|f| (f.key, f.equals))
            .collect();
        let hits = handle
            .search(req.vector, req.top_k as usize, metric_override, filters)
            .ok_or_else(|| Status::invalid_argument("query vector dimension mismatch"))?;
        let mut resp = QueryResponse { hits: Vec::with_capacity(hits.len()) };
        for (id, score, payload) in hits {
            resp.hits.push(ScoredPoint {
                id,
                score,
                payload_json: if req.with_payloads { payload } else { String::new() },
            });
        }
        Ok(Response::new(resp))
    }
}
