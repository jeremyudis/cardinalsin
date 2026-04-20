//! FST term dictionary builder for the inverted index.
//!
//! Builds FST (Finite State Transducer) term dictionaries from Arrow RecordBatch
//! string columns. Each unique column=value pair maps to a postings list of chunk ordinals.

use crate::{Error, Result};

use super::postings::PostingsList;
use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use std::collections::BTreeMap;

/// FST data for a single column, ready for segment serialization.
pub struct ColumnFstData {
    /// Column name this FST covers.
    pub column_name: String,
    /// Serialized FST bytes (fst::Map).
    pub fst_bytes: Vec<u8>,
    /// Postings lists, one per unique term, in FST output order.
    pub postings: Vec<PostingsList>,
}

/// Builds FST term dictionaries from Arrow RecordBatch columns.
pub struct FstTermBuilder;

impl FstTermBuilder {
    /// Build an FST for one string column in the batch.
    ///
    /// Key format: the raw string value (column name is tracked separately).
    /// Uses BTreeMap for guaranteed lexicographic insertion order into the FST.
    /// Handles both Utf8 and Dictionary-encoded string columns.
    pub fn build_column_fst(
        column_name: &str,
        batch: &RecordBatch,
        chunk_ordinals: &[u32],
    ) -> Result<ColumnFstData> {
        let col_idx = batch
            .schema()
            .index_of(column_name)
            .map_err(|_| Error::Index(format!("Column '{column_name}' not found in batch")))?;
        let col = batch.column(col_idx);

        // Collect unique values -> set of ordinals
        let mut term_ordinals: BTreeMap<String, PostingsList> = BTreeMap::new();

        // Try Utf8 (StringArray)
        if let Some(str_arr) = col.as_string_opt::<i32>() {
            for (row_idx, chunk_ord) in chunk_ordinals.iter().enumerate() {
                if row_idx < str_arr.len() && !str_arr.is_null(row_idx) {
                    let val = str_arr.value(row_idx);
                    term_ordinals
                        .entry(val.to_string())
                        .or_default()
                        .add(*chunk_ord);
                }
            }
        }
        // Try LargeUtf8
        else if let Some(str_arr) = col.as_string_opt::<i64>() {
            for (row_idx, chunk_ord) in chunk_ordinals.iter().enumerate() {
                if row_idx < str_arr.len() && !str_arr.is_null(row_idx) {
                    let val = str_arr.value(row_idx);
                    term_ordinals
                        .entry(val.to_string())
                        .or_default()
                        .add(*chunk_ord);
                }
            }
        }
        // Try Dictionary encoded string columns
        else if let Some(dict_arr) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>()
        {
            if let Some(values) = dict_arr.values().as_string_opt::<i32>() {
                for (row_idx, chunk_ord) in chunk_ordinals.iter().enumerate() {
                    if row_idx < dict_arr.len() && !dict_arr.is_null(row_idx) {
                        let key = dict_arr.keys().value(row_idx) as usize;
                        if key < values.len() {
                            let val = values.value(key);
                            term_ordinals
                                .entry(val.to_string())
                                .or_default()
                                .add(*chunk_ord);
                        }
                    }
                }
            }
        } else if let Some(dict_arr) = col
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt32Type>>()
        {
            if let Some(values) = dict_arr.values().as_string_opt::<i32>() {
                for (row_idx, chunk_ord) in chunk_ordinals.iter().enumerate() {
                    if row_idx < dict_arr.len() && !dict_arr.is_null(row_idx) {
                        let key = dict_arr.keys().value(row_idx) as usize;
                        if key < values.len() {
                            let val = values.value(key);
                            term_ordinals
                                .entry(val.to_string())
                                .or_default()
                                .add(*chunk_ord);
                        }
                    }
                }
            }
        } else {
            return Err(Error::Index(format!(
                "Column '{column_name}' is not a string or dictionary-encoded string type"
            )));
        }

        if term_ordinals.is_empty() {
            // Build a trivially empty FST
            let fst_builder = fst::MapBuilder::memory();
            let fst_bytes = fst_builder
                .into_inner()
                .map_err(|e| Error::Index(format!("FST build failed: {e}")))?;
            return Ok(ColumnFstData {
                column_name: column_name.to_string(),
                fst_bytes,
                postings: Vec::new(),
            });
        }

        // Build FST: keys are sorted (BTreeMap guarantees this), values are postings indices
        let mut fst_builder = fst::MapBuilder::memory();
        let mut postings = Vec::with_capacity(term_ordinals.len());

        for (idx, (term, posting)) in term_ordinals.into_iter().enumerate() {
            fst_builder
                .insert(term.as_bytes(), idx as u64)
                .map_err(|e| Error::Index(format!("FST insert failed: {e}")))?;
            postings.push(posting);
        }

        let fst_bytes = fst_builder
            .into_inner()
            .map_err(|e| Error::Index(format!("FST build failed: {e}")))?;

        Ok(ColumnFstData {
            column_name: column_name.to_string(),
            fst_bytes,
            postings,
        })
    }

    /// Build FSTs for all eligible string columns in the batch.
    ///
    /// Skips columns in `skip_columns` and columns with cardinality above `max_cardinality`.
    pub fn build_all_columns(
        batch: &RecordBatch,
        chunk_ordinals: &[u32],
        skip_columns: &[String],
        max_cardinality: usize,
    ) -> Result<Vec<ColumnFstData>> {
        let schema = batch.schema();
        let mut results = Vec::new();

        for field in schema.fields() {
            let name = field.name();

            // Skip non-indexable columns
            if skip_columns.iter().any(|s| s == name) {
                continue;
            }

            // Only index string-like columns
            if !is_string_type(field.data_type()) {
                continue;
            }

            // Skip columns whose cardinality exceeds the threshold.
            // The estimator short-circuits once it sees more than max_cardinality
            // distinct values, so memory stays O(max_cardinality) even on columns
            // with millions of rows.
            let col_idx = schema.index_of(name).unwrap();
            let col = batch.column(col_idx);
            if exceeds_cardinality(col, max_cardinality) {
                continue;
            }

            match Self::build_column_fst(name, batch, chunk_ordinals) {
                Ok(fst_data) => results.push(fst_data),
                Err(e) => {
                    tracing::debug!(column = %name, error = %e, "Skipping column for indexing");
                }
            }
        }

        Ok(results)
    }
}

/// Check if an Arrow data type is a string-like type we can index.
fn is_string_type(dt: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _)
    )
}

/// Return true if the column contains strictly more than `limit` distinct
/// non-null values. Short-circuits as soon as the limit is exceeded so peak
/// memory is O(limit) regardless of row count.
fn exceeds_cardinality(col: &dyn arrow_array::Array, limit: usize) -> bool {
    use arrow_schema::DataType;
    use std::collections::HashSet;

    let probe_capacity = limit.saturating_add(1);

    if let Some(str_arr) = col.as_string_opt::<i32>() {
        let mut distinct: HashSet<&str> = HashSet::with_capacity(probe_capacity.min(1024));
        for i in 0..str_arr.len() {
            if !str_arr.is_null(i) {
                distinct.insert(str_arr.value(i));
                if distinct.len() > limit {
                    return true;
                }
            }
        }
        return false;
    }
    if let Some(str_arr) = col.as_string_opt::<i64>() {
        let mut distinct: HashSet<&str> = HashSet::with_capacity(probe_capacity.min(1024));
        for i in 0..str_arr.len() {
            if !str_arr.is_null(i) {
                distinct.insert(str_arr.value(i));
                if distinct.len() > limit {
                    return true;
                }
            }
        }
        return false;
    }

    // Dictionary-encoded strings: count distinct referenced keys. The
    // dictionary size is an upper bound but may include unreferenced values
    // after filter pushdown or slicing.
    if matches!(col.data_type(), DataType::Dictionary(_, _)) {
        use arrow_array::types::{
            Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
            UInt8Type,
        };
        use arrow_array::DictionaryArray;

        macro_rules! probe_dict_keys {
            ($kt:ty) => {{
                if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<$kt>>() {
                    let keys = dict.keys();
                    let mut distinct: HashSet<i64> =
                        HashSet::with_capacity(probe_capacity.min(1024));
                    for i in 0..keys.len() {
                        if !keys.is_null(i) {
                            distinct.insert(keys.value(i) as i64);
                            if distinct.len() > limit {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            }};
        }

        probe_dict_keys!(Int8Type);
        probe_dict_keys!(Int16Type);
        probe_dict_keys!(Int32Type);
        probe_dict_keys!(Int64Type);
        probe_dict_keys!(UInt8Type);
        probe_dict_keys!(UInt16Type);
        probe_dict_keys!(UInt32Type);
        probe_dict_keys!(UInt64Type);
    }

    // Conservative fallback for any other type: treat as exceeding the limit
    // so the column is skipped rather than scanned with an unknown shape.
    col.len() > limit
}
