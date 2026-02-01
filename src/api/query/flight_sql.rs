//! Arrow Flight SQL service
//!
//! High-performance query interface for analytics tools.

use crate::query::QueryNode;
use crate::Result;

use arrow_array::RecordBatch;
use arrow_flight::{FlightData, FlightInfo, SchemaAsIpc, Ticket};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
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
        // Analyze query to get schema without executing
        let plan = self.query_node.engine.analyze(query).await?;
        // Convert DFSchema to Arrow Schema
        let df_schema = plan.schema();
        let schema: Schema = df_schema.as_ref().into();

        let ticket = Ticket::new(query.as_bytes().to_vec());

        let info = FlightInfo::new()
            .try_with_schema(&schema)?
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(ticket));

        Ok(info)
    }

    /// Execute a query and return results as Flight data
    pub async fn do_get(&self, ticket: &Ticket) -> Result<Vec<FlightData>> {
        let query = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| crate::Error::Query(e.to_string()))?;

        let batches = self.query_node.query(&query).await?;

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Convert to Flight data
        let schema = batches[0].schema();
        let mut flight_data = Vec::new();

        // Add schema as first message
        let options = IpcWriteOptions::default();
        let schema_data = SchemaAsIpc::new(&schema, &options);
        flight_data.push(FlightData::from(schema_data));

        // Add data batches
        for batch in &batches {
            let data = batch_to_flight_data(batch)?;
            flight_data.extend(data);
        }

        Ok(flight_data)
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

/// Convert a RecordBatch to FlightData
fn batch_to_flight_data(batch: &RecordBatch) -> Result<Vec<FlightData>> {
    use bytes::Bytes;

    let options = IpcWriteOptions::default();
    let mut buffer = Vec::new();

    {
        let mut writer = StreamWriter::try_new_with_options(
            &mut buffer,
            &batch.schema(),
            options.clone(),
        )?;
        writer.write(batch)?;
        writer.finish()?;
    }

    // Create FlightData with the serialized IPC data
    let data = FlightData {
        flight_descriptor: None,
        data_header: Bytes::new(),
        app_metadata: Bytes::new(),
        data_body: Bytes::from(buffer),
    };

    Ok(vec![data])
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
