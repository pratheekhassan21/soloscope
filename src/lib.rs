
pub mod api;
pub mod config;
pub mod contention;
pub mod db;
pub mod ingest;
pub mod metrics;
pub mod model;
pub mod ohlcv;
pub mod rpc;

use std::sync::Arc;

use tokio::sync::mpsc;

/// Shared by the HTTP handlers.
pub struct AppState {
    pub cfg: Arc<config::Config>,
    pub metrics: Arc<metrics::Metrics>,
    pub reads: Arc<db::ReadPool>,
    pub writes: mpsc::Sender<db::WriteMsg>,
}
