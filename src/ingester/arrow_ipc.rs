use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use std::io::Cursor;

/// Encode a single RecordBatch as Arrow IPC stream bytes.
pub fn encode_record_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(bytes)
}

/// Decode a single RecordBatch from Arrow IPC stream bytes.
pub fn decode_record_batch_ipc(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes.to_vec()), None)?;
    let batch = reader.next().transpose()?.ok_or_else(|| {
        Error::InvalidSchema("Arrow IPC payload contained no record batches".into())
    })?;

    if reader.next().transpose()?.is_some() {
        return Err(Error::InvalidSchema(
            "Arrow IPC payload must contain exactly one record batch".into(),
        ));
    }

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
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
    fn round_trips_single_record_batch() {
        let batch = make_batch();
        let encoded = encode_record_batch_ipc(&batch).unwrap();
        let decoded = decode_record_batch_ipc(&encoded).unwrap();
        assert_eq!(decoded.num_rows(), batch.num_rows());
        assert_eq!(decoded.schema(), batch.schema());
    }
}
