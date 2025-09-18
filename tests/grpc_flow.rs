use std::sync::Arc;

use serial_test::serial;
use tempfile::tempdir;
use tonic::Request;

use vectaraft::pb::vectordb::v1::{
    vector_db_server::VectorDb,
    CreateCollectionRequest,
    Filter,
    Point,
    QueryRequest,
    UpsertRequest,
};
use vectaraft::server::grpc::VectorDbService;
use vectaraft::server::state::{DbState, DbStateConfig};

fn state_with_temp_wal() -> (Arc<DbState>, std::path::PathBuf, tempfile::TempDir) {
    let tmp = tempdir().expect("tempdir");
    let wal_path = tmp.path().join("wal.log");
    let config = DbStateConfig {
        wal_path: Some(wal_path.clone()),
        enable_wal: true,
    };
    (Arc::new(DbState::with_config(config)), wal_path, tmp)
}

#[tokio::test]
#[serial]
async fn create_upsert_query_roundtrip() {
    let (state, _wal_path, _guard) = state_with_temp_wal();
    let svc = VectorDbService { state: state.clone() };

    svc.create_collection(Request::new(CreateCollectionRequest {
        name: "demo".into(),
        dims: 4,
        metric: "cosine".into(),
    }))
    .await
    .expect("create collection");

    let points = vec![
        Point { id: String::new(), vector: vec![1.0, 0.0, 0.0, 0.0], payload_json: "{\"k\":0}".into() },
        Point { id: "manual".into(), vector: vec![0.0, 1.0, 0.0, 0.0], payload_json: "{\"k\":1}".into() },
    ];

    let upserted = svc
        .upsert(Request::new(UpsertRequest {
            collection: "demo".into(),
            points,
        }))
        .await
        .expect("upsert")
        .into_inner()
        .upserted;
    assert_eq!(upserted, 2);

    let hits = svc
        .query(Request::new(QueryRequest {
            collection: "demo".into(),
            vector: vec![0.9, 0.1, 0.0, 0.0],
            top_k: 2,
            metric_override: String::new(),
            with_payloads: true,
            filters: vec![],
        }))
        .await
        .expect("query")
        .into_inner()
        .hits;

    assert_eq!(hits.len(), 2);
    assert!(hits.iter().any(|h| h.id == "manual"));
    assert!(hits.iter().all(|h| !h.payload_json.is_empty()));
    assert!(hits.iter().all(|h| !h.id.is_empty()));

    let filtered = svc
        .query(Request::new(QueryRequest {
            collection: "demo".into(),
            vector: vec![0.9, 0.1, 0.0, 0.0],
            top_k: 5,
            metric_override: String::new(),
            with_payloads: true,
            filters: vec![Filter { key: "k".into(), equals: "1".into() }],
        }))
        .await
        .expect("filtered query")
        .into_inner()
        .hits;

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].payload_json, "{\"k\":1}");
}

#[tokio::test]
#[serial]
async fn wal_replay_restores_points() {
    let (state, wal_path, guard) = state_with_temp_wal();
    let svc = VectorDbService { state: state.clone() };

    svc.create_collection(Request::new(CreateCollectionRequest {
        name: "demo".into(),
        dims: 3,
        metric: "l2".into(),
    }))
    .await
    .expect("create collection");

    svc.upsert(Request::new(UpsertRequest {
        collection: "demo".into(),
        points: vec![Point {
            id: "persist".into(),
            vector: vec![1.0, 1.0, 1.0],
            payload_json: "{\"hello\":true}".into(),
        }],
    }))
    .await
    .expect("upsert");

    drop(svc);
    drop(state);

    let config = DbStateConfig {
        wal_path: Some(wal_path.clone()),
        enable_wal: true,
    };
    let state = Arc::new(DbState::with_config(config));
    // Keep guard alive until end of test.
    let _guard = guard;
    let svc = VectorDbService { state };

    let hits = svc
        .query(Request::new(QueryRequest {
            collection: "demo".into(),
            vector: vec![1.0, 1.0, 1.0],
            top_k: 1,
            metric_override: String::new(),
            with_payloads: true,
            filters: vec![],
        }))
        .await
        .expect("query after replay")
        .into_inner()
        .hits;

    assert_eq!(hits.len(), 1);
    let hit = &hits[0];
    assert_eq!(hit.id, "persist");
    assert_eq!(hit.payload_json, "{\"hello\":true}");
}

#[tokio::test]
#[serial]
async fn operations_work_without_wal() {
    let config = DbStateConfig {
        wal_path: None,
        enable_wal: false,
    };
    let state = Arc::new(DbState::with_config(config));
    assert!(state.wal.is_none());

    let svc = VectorDbService { state: state.clone() };

    svc.create_collection(Request::new(CreateCollectionRequest {
        name: "no-wal".into(),
        dims: 2,
        metric: "ip".into(),
    }))
    .await
    .expect("create collection");

    let upserted = svc
        .upsert(Request::new(UpsertRequest {
            collection: "no-wal".into(),
            points: vec![Point {
                id: String::new(),
                vector: vec![0.5, 0.5],
                payload_json: String::new(),
            }],
        }))
        .await
        .expect("upsert")
        .into_inner()
        .upserted;
    assert_eq!(upserted, 1);

    let hits = svc
        .query(Request::new(QueryRequest {
            collection: "no-wal".into(),
            vector: vec![0.5, 0.5],
            top_k: 1,
            metric_override: String::new(),
            with_payloads: false,
            filters: vec![],
        }))
        .await
        .expect("query")
        .into_inner()
        .hits;

    assert_eq!(hits.len(), 1);
    assert!(!hits[0].id.is_empty());
}
