//! Arrow Flight SQL service
//!
//! High-performance query interface for analytics tools.

use crate::query::QueryNode;
use crate::Result;

use arrow_flight::{FlightData, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::Schema;
use std::sync::Arc;

/// Flight SQL query service
pub struct FlightSqlQueryService {
    query_node: Arc<QueryNode>,
}

impl FlightSqlQueryService {
    /// Create a new Flight SQL service
    pub fn new(query_node: Arc<QueryNode>) -> Self {
        Self { query_node }
    }

    /// Get flight info for a SQL statement
    pub async fn get_flight_info(&self, query: &str) -> Result<FlightInfo> {
        self.get_flight_info_with_ticket(query, Ticket::new(query.as_bytes().to_vec()))
            .await
    }

    /// Get flight info with a caller-provided ticket.
    pub async fn get_flight_info_with_ticket(
        &self,
        query: &str,
        ticket: Ticket,
    ) -> Result<FlightInfo> {
        // Analyze query to get schema without executing
        let plan = self.query_node.engine.analyze(query).await?;
        // Convert DFSchema to Arrow Schema
        let df_schema = plan.schema();
        let schema: Schema = df_schema.as_ref().into();

        let info = FlightInfo::new()
            .try_with_schema(&schema)?
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket));

        Ok(info)
    }

    /// Execute a query and return results as Flight data
    pub async fn do_get(&self, ticket: &Ticket) -> Result<Vec<FlightData>> {
        let query = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let batches = self.query_node.query(&query).await?;

        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let stream = arrow_flight::utils::batches_to_flight_data(schema.as_ref(), batches)
            .map_err(|e| {
                crate::Error::InvalidSchema(format!("Failed to encode Flight stream: {e}"))
            })?;
        Ok(stream)
    }

    /// Create a prepared statement
    pub async fn create_prepared_statement(&self, query: &str) -> Result<PreparedStatement> {
        let handle = self.query_node.engine.prepare(query).await?;

        Ok(PreparedStatement {
            handle,
            query: query.to_string(),
        })
    }
}

/// Prepared statement handle
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub handle: String,
    pub query: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepared_statement() {
        let stmt = PreparedStatement {
            handle: "test-handle".to_string(),
            query: "SELECT * FROM metrics".to_string(),
        };

        assert_eq!(stmt.handle, "test-handle");
        assert_eq!(stmt.query, "SELECT * FROM metrics");
    }
}
