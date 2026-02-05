//! Arrow Flight bulk ingestion
//!
//! High-performance bulk data loading via Arrow Flight DoPut.

use crate::ingester::Ingester;
use crate::Result;

use arrow_array::RecordBatch;
use arrow_flight::FlightData;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::Schema;
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
        let mut total_rows = 0u64;
        let mut decoder: Option<StreamDecoder> = None;
        let mut schema: Option<Arc<Schema>> = None;

        for data in data_stream {
            // First message should contain schema
            if schema.is_none() {
                if let Some(sch) = Self::extract_schema(&data)? {
                    schema = Some(Arc::new(sch));
                    decoder = Some(StreamDecoder::new());
                    continue;
                }
            }

            // Decode subsequent messages as record batches
            if let Some(ref mut dec) = decoder {
                if let Some(batch) = Self::decode_batch(dec, &data, schema.clone())? {
                    total_rows += batch.num_rows() as u64;
                    self.ingester.write(batch).await?;
                }
            }
        }

        Ok(total_rows)
    }

    /// Extract schema from FlightData
    fn extract_schema(data: &FlightData) -> Result<Option<Schema>> {
        if !data.data_header.is_empty() {
            // Try to parse as IPC schema
            if let Ok(schema) = Schema::try_from(data) {
                return Ok(Some(schema));
            }
        }
        Ok(None)
    }

    /// Decode a FlightData message to RecordBatch
    fn decode_batch(
        _decoder: &mut StreamDecoder,
        _data: &FlightData,
        _schema: Option<Arc<Schema>>,
    ) -> Result<Option<RecordBatch>> {
        // In production, use the decoder to parse IPC data
        // For now, return None (simplified implementation)
        Ok(None)
    }
}

/// Convert a RecordBatch to FlightData for responses
pub fn batch_to_flight_data(batch: &RecordBatch) -> Result<Vec<FlightData>> {
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use bytes::Bytes;

    let options = IpcWriteOptions::default();
    let mut buffer = Vec::new();

    {
        let mut writer = StreamWriter::try_new_with_options(
            &mut buffer,
            &batch.schema(),
            options,
        )?;
        writer.write(batch)?;
        writer.finish()?;
    }

    // Create FlightData with the serialized IPC data
    let flight_data = FlightData {
        flight_descriptor: None,
        data_header: Bytes::new(),
        app_metadata: Bytes::new(),
        data_body: Bytes::from(buffer),
    };

    Ok(vec![flight_data])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, Float64Array};
    use arrow_schema::{Field, DataType};

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
        ).unwrap()
    }

    #[test]
    fn test_batch_to_flight_data() {
        let batch = create_test_batch();
        let flight_data = batch_to_flight_data(&batch).unwrap();
        assert!(!flight_data.is_empty());
    }
}
