//! Arrow Flight bulk ingestion
//!
//! High-performance bulk data loading via Arrow Flight DoPut.

use crate::ingester::Ingester;
use crate::Result;

use arrow_array::RecordBatch;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::FlightData;
use std::sync::Arc;

/// Arrow Flight ingestion service
pub struct FlightIngestService {
    ingester: Arc<Ingester>,
}

impl FlightIngestService {
    /// Create a new Flight ingestion service
    pub fn new(ingester: Arc<Ingester>) -> Self {
        Self { ingester }
    }

    /// Process a stream of FlightData messages
    pub async fn process_stream(
        &self,
        data_stream: impl Iterator<Item = FlightData>,
    ) -> Result<u64> {
        let payload: Vec<FlightData> = data_stream
            .filter(|msg| !msg.data_header.is_empty() || !msg.data_body.is_empty())
            .collect();
        if payload.is_empty() {
            return Ok(0);
        }

        let batches = flight_data_to_batches(&payload)
            .map_err(|e| crate::Error::InvalidSchema(format!("Flight IPC decode failed: {e}")))?;

        let mut total_rows = 0u64;
        for batch in batches {
            total_rows += batch.num_rows() as u64;
            self.ingester.write(batch).await?;
        }
        Ok(total_rows)
    }
}

/// Convert a RecordBatch to FlightData for responses
pub fn batch_to_flight_data(batch: &RecordBatch) -> Result<Vec<FlightData>> {
    let stream =
        arrow_flight::utils::batches_to_flight_data(batch.schema().as_ref(), vec![batch.clone()])
            .map_err(|e| crate::Error::InvalidSchema(format!("Flight IPC encode failed: {e}")))?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_batch_to_flight_data() {
        let batch = create_test_batch();
        let flight_data = batch_to_flight_data(&batch).unwrap();
        assert!(flight_data.len() >= 2);
    }

    #[tokio::test]
    async fn test_process_stream_decodes_and_counts_rows() {
        use crate::ingester::{Ingester, IngesterConfig};
        use crate::metadata::LocalMetadataClient;
        use crate::schema::MetricSchema;
        use crate::StorageConfig;
        use object_store::memory::InMemory;

        let object_store = Arc::new(InMemory::new());
        let metadata = Arc::new(LocalMetadataClient::new());
        let ingester = Arc::new(Ingester::new(
            IngesterConfig::default(),
            object_store,
            metadata,
            StorageConfig::default(),
            MetricSchema::default_metrics(),
        ));

        let service = FlightIngestService::new(ingester);
        let batch = create_test_batch();
        let flight_data = batch_to_flight_data(&batch).unwrap();
        let rows = service
            .process_stream(flight_data.into_iter())
            .await
            .unwrap();
        assert_eq!(rows, 3);
    }
}
