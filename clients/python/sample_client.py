import importlib
import grpc, json, sys
from pathlib import Path
from grpc_tools import protoc

ROOT = Path(__file__).resolve().parents[2]  # vectaraft/
PROTO = ROOT / "proto" / "vector_db.proto"
OUT = Path(__file__).resolve().parent / "_gen"
OUT.mkdir(parents=True, exist_ok=True)

protoc.main(["", f"-I{ROOT / 'proto'}", f"--python_out={OUT}", f"--grpc_python_out={OUT}", str(PROTO)])
sys.path.insert(0, str(OUT))

vector_db_pb2 = importlib.import_module("vector_db_pb2")
vector_db_pb2_grpc = importlib.import_module("vector_db_pb2_grpc")

VectorDbStub = vector_db_pb2_grpc.VectorDbStub
CreateCollectionRequest = vector_db_pb2.CreateCollectionRequest
UpsertRequest = vector_db_pb2.UpsertRequest
Point = vector_db_pb2.Point
QueryRequest = vector_db_pb2.QueryRequest
Filter = vector_db_pb2.Filter

def main():
    ch = grpc.insecure_channel("127.0.0.1:50051")
    stub = VectorDbStub(ch)

    print("Create collection...")
    print(stub.CreateCollection(CreateCollectionRequest(name="demo", dims=4, metric="cosine")))

    vecs = [[1,0,0,0],[0,1,0,0],[0.7,0.7,0,0],[0.9,0.1,0,0]]
    payloads = [json.dumps({"i": i}) for i in range(len(vecs))]
    points = [Point(id="", vector=v, payload_json=p) for v, p in zip(vecs, payloads)]
    up = UpsertRequest(collection="demo", points=points)
    print("Upserted:", stub.Upsert(up).upserted)

    q = [0.8,0.2,0,0]
    res = stub.Query(QueryRequest(collection="demo", vector=q, top_k=3, with_payloads=True))
    for h in res.hits:
        print(f"id={h.id} score={h.score:.4f} payload={h.payload_json}")

    print("Filtered query (k == 1)...")
    res = stub.Query(QueryRequest(
        collection="demo",
        vector=q,
        top_k=3,
        with_payloads=True,
        filters=[Filter(key="k", equals="1")],
    ))
    for h in res.hits:
        print(f"[filtered] id={h.id} score={h.score:.4f} payload={h.payload_json}")

if __name__ == "__main__":
    main()
