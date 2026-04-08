mod proto;
mod eventstore;
mod messaging;

use std::path::PathBuf;
use std::sync::Arc;

use tonic::transport::Server;

use kronosdb_eventstore::store::EventStoreEngine;
use kronosdb_messaging::engine::MessagingEngine;

use crate::eventstore::service::EventStoreService;
use crate::messaging::command_service::CommandServiceImpl;
use crate::messaging::query_service::QueryServiceImpl;

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

    // Create the messaging platform.
    let messaging = Arc::new(MessagingEngine::new());

    // Build gRPC services.
    let event_store_service = EventStoreService::new(Box::new(store));
    let command_service = CommandServiceImpl::new(Arc::clone(&messaging) as Arc<dyn kronosdb_messaging::api::MessagingPlatform>);
    let query_service = QueryServiceImpl::new(Arc::clone(&messaging) as Arc<dyn kronosdb_messaging::api::MessagingPlatform>);

    println!("KronosDB listening on {addr}");

    Server::builder()
        .add_service(event_store_service.into_server())
        .add_service(command_service.into_server())
        .add_service(query_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
