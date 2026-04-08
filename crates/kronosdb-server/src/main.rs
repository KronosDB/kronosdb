mod proto;
mod eventstore;

use std::path::PathBuf;

use tonic::transport::Server;

use kronosdb_eventstore::store::EventStoreEngine;
use crate::eventstore::service::EventStoreService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from("data/default");
    let addr = "[::1]:50051".parse()?;

    // Create or open the event store.
    let store = if data_dir.exists() {
        EventStoreEngine::open(&data_dir)?
    } else {
        EventStoreEngine::create(&data_dir)?
    };

    let event_store_service = EventStoreService::new(Box::new(store));

    println!("KronosDB listening on {addr}");

    Server::builder()
        .add_service(event_store_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
