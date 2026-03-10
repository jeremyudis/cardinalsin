//! Ingestion API modules

mod dispatcher;
pub mod flight_ingest;
pub mod otlp;
pub mod prometheus;

pub use dispatcher::{
    handle_internal_arrow_ingest, IngestDispatcher, RoutingContext, ShardWriteRouter,
};
