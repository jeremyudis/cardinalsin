use crate::api::ApiState;
use crate::cluster::NodeInfo;
use crate::ingester::{decode_record_batch_ipc, Ingester};
use crate::Result;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use std::sync::Arc;

/// Minimal router surface needed by the ingest dispatcher.
#[async_trait]
pub trait ShardWriteRouter: Send + Sync {
    async fn route_write(&self, shard_id: &str) -> Result<Option<NodeInfo>>;
    async fn forward_write(
        &self,
        target_node: &NodeInfo,
        batch: &RecordBatch,
        tenant_id: &str,
    ) -> Result<()>;
}

/// Runtime routing configuration for distributed ingest dispatch.
#[derive(Clone)]
pub struct RoutingContext {
    router: Arc<dyn ShardWriteRouter>,
    local_node_id: String,
    tenant_id: String,
}

impl RoutingContext {
    pub fn new(
        router: Arc<dyn ShardWriteRouter>,
        local_node_id: String,
        tenant_id: String,
    ) -> Self {
        Self {
            router,
            local_node_id,
            tenant_id,
        }
    }
}

/// Shared dispatcher used by HTTP and gRPC ingest protocols.
#[derive(Clone)]
pub struct IngestDispatcher {
    ingester: Arc<Ingester>,
    routing: Option<RoutingContext>,
}

impl IngestDispatcher {
    pub fn new(ingester: Arc<Ingester>, routing: Option<RoutingContext>) -> Self {
        Self { ingester, routing }
    }

    pub async fn dispatch(&self, batch: RecordBatch) -> Result<()> {
        let Some(routing) = &self.routing else {
            return self.ingester.write(batch).await;
        };

        let shard_batches = self.ingester.partition_batch_for_routing(&batch).await?;
        for (shard_id, shard_batch) in shard_batches {
            match routing.router.route_write(&shard_id).await? {
                Some(target) if target.id != routing.local_node_id => {
                    routing
                        .router
                        .forward_write(&target, &shard_batch, &routing.tenant_id)
                        .await?;
                }
                _ => {
                    self.ingester.write(shard_batch).await?;
                }
            }
        }

        Ok(())
    }
}

/// Internal Arrow IPC ingest endpoint used for shard forwarding between ingesters.
pub async fn handle_internal_arrow_ingest(
    State(state): State<ApiState>,
    body: Bytes,
) -> StatusCode {
    let Some(ingester) = state.ingester else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };

    let batch = match decode_record_batch_ipc(&body) {
        Ok(batch) => batch,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    match ingester.write(batch).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
