# vectaraft (prototype)

An experimental gRPC vector search service designed as a playground for building a Pinecone/Chroma-style engine. It exposes a tonic-based API backed by an in-memory flat index, optional JSON WAL durability, and a thin Python sample client.

> ⚠️ **Status:** prototype. Expect missing features (no replication, no index compaction, limited filtering) and breaking changes.

## Quickstart

Requirements:

- Rust toolchain (1.75+)
- (Optional) Python 3.11+/virtualenv for the demo client

Clone and build:

```powershell
cd E:\vectaraft
cargo build --release
```

### Run the server

With WAL persistence (recommended):

```powershell
mkdir data 2>$null
$env:VECTARAFT_ENABLE_WAL = "1"
$env:VECTARAFT_WAL_PATH = ".\data\wal.jsonl"
cargo run --release
```

Memory-only mode:

```powershell
cargo run --release -- --no-wal
```

The server listens on `127.0.0.1:50051`.

### Quick checks

Rust ping:

```powershell
cargo run --example ping_client
```

Python end-to-end:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install grpcio grpcio-tools
python clients/python/sample_client.py
```

The Python script autogenerates stubs under `clients/python/_gen/` and performs collection create, upsert, unfiltered and filtered queries.

### Tests

```powershell
cargo test --all -- --nocapture
```

## Packaging

A release binary is emitted at `target/release/vectaraft.exe`. Run it directly with the same flags as above, e.g.:

```powershell
.\target\release\vectaraft.exe --wal-path .\data\wal.jsonl
```

### Docker (optional)

```powershell
docker build -t vectaraft:dev .
docker run --rm -p 50051:50051 -v ${PWD}/data:/data vectaraft:dev
```

(See the `Dockerfile` in the repo root.)

## Operational notes

- Environment / flags:
- `VECTARAFT_ENABLE_WAL=0|1`
- `VECTARAFT_WAL_PATH=...`
- `--no-wal`, `--wal-path <file>`
- `VECTARAFT_ENABLE_METRICS=0|1`
- `VECTARAFT_METRICS_ADDR=host:port`
- `--no-metrics`, `--metrics-addr <addr>`
- Persistence check: stop the server, restart with the same WAL path, re-query—data should survive.
- Port conflicts: `netstat -ano | findstr :50051` then `taskkill /PID <pid> /F`.

Metrics are exposed on `/metrics` (Prometheus text format) and default to `127.0.0.1:9100`.

## Roadmap before public release

- **Storage**: WAL compaction, snapshots, fsync strategy.
- **API**: delete/upsert-by-id, pagination, richer filter grammar.
- **Indexes**: fast ANN (e.g., HNSW/IVF) alongside the flat scan.
- **Observability**: Prometheus metrics, health checks, tracing.
- **Security**: authn/z, multi-tenant isolation.
- **Tooling**: CI/CD (fmt/clippy/tests), versioned artifacts, docs.

Contributions welcome while the project is still in flux.
