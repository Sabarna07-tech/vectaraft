mod pb;

use tonic::{Request, Response, Status};
use tonic::transport::Server;

use crate::pb::vectordb::v1::{
    vector_db_server::{VectorDb, VectorDbServer},
    PingRequest, PingResponse,
};

struct VectorDbService;

#[tonic::async_trait]
impl VectorDb for VectorDbService {
    async fn ping(&self, request: Request<PingRequest>)
        -> Result<Response<PingResponse>, Status>
    {
        let name = request.into_inner().message;
        let reply = PingResponse { message: format!("pong: {}", name) };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:50051".parse()?;
    let svc  = VectorDbService;

    Server::builder()
        .add_service(VectorDbServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
