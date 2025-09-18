use vectaraft::pb::vectordb::v1::{vector_db_client::VectorDbClient, PingRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Server must be running on 127.0.0.1:50051
    let mut client = VectorDbClient::connect("http://127.0.0.1:50051").await?;
    let resp = client.ping(tonic::Request::new(PingRequest {})).await?;
    println!("Ping OK: {:?}", resp.into_inner());
    Ok(())
}
