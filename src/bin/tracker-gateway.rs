//! tracker-gateway — serves `tracker.v1.TrackerService` over gRPC.
//!
//! The daemon holds no tracker credential of its own; the caller's identity
//! travels in request metadata (see `tracker::gateway`). Bind address from
//! `$TRACKER_GATEWAY_BIND` (default `0.0.0.0:50068`) — distinct from the
//! `$TRACKER_GATEWAY_ADDR` the *clients* (the agents campaign controller,
//! plugin-tracker) dial.

use std::net::SocketAddr;

use tonic::transport::Server;
use tracker::gateway::TrackerGateway;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let addr: SocketAddr = std::env::var("TRACKER_GATEWAY_BIND")
        .unwrap_or_else(|_| "0.0.0.0:50068".to_string())
        .parse()?;

    tracing::info!(%addr, "starting tracker-gateway (tracker.v1.TrackerService)");

    Server::builder()
        .add_service(TrackerGateway::default().into_server())
        .serve(addr)
        .await?;
    Ok(())
}
