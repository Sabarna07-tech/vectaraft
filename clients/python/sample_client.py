import grpc, json, sys
from pathlib import Path
from grpc_tools import protoc

ROOT = Path(__file__).resolve().parents[2]  # vectaraft/
PROTO = ROOT / "proto" / "vector_db.proto"
OUT = Path(__file__).resolve().parent / "_gen"
OUT.mkdir(parents=True, exist_ok=True)

protoc.main(["", f"-I{ROOT / 'proto'}", f"--python_out={OUT}", f"--grpc_python_out={OUT}", str(PROTO)])
sys.path.append(str(OUT))

from vectordb_dot_v1_dot_vector__db__pb2_grpc import VectorDBStub
from vectordb_dot_v1_dot_vector__db__pb2 import CreateCollectionReq, UpsertReq, FloatVector, SearchReq

def main():
    ch = grpc.insecure_channel("127.0.0.1:50051")
    stub = VectorDBStub(ch)

    print("Create collection...")
    print(stub.CreateCollection(CreateCollectionReq(name="demo", dim=4, metric="cosine")))

    vecs = [[1,0,0,0],[0,1,0,0],[0.7,0.7,0,0],[0.9,0.1,0,0]]
    payloads = [json.dumps({"i": i}) for i in range(len(vecs))]
    up = UpsertReq(collection="demo", ids=[], vectors=[FloatVector(values=v) for v in vecs], payload_json=payloads)
    print("Upserted:", stub.Upsert(up).upserted)

    q = [0.8,0.2,0,0]
    res = stub.Search(SearchReq(collection="demo", query=FloatVector(values=q), top_k=3, metric="cosine"))
    for h in res.hits:
        print(f"id={h.id} score={h.score:.4f} payload={h.payload_json}")

if __name__ == "__main__":
    main()
