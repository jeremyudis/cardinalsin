//! Inverted index for dimension/tag columns
//! Maps column_name -> value -> sorted chunk paths.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

/// Inverted index: column_name -> label_value -> sorted chunk paths
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndex {
    pub entries: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a chunk to the index for a set of column->value pairs
    pub fn add_chunk(&mut self, chunk_path: &str, columns: &[(String, String)]) {
        for (col, val) in columns {
            let paths = self
                .entries
                .entry(col.clone())
                .or_default()
                .entry(val.clone())
                .or_default();
            if let Err(pos) = paths.binary_search(&chunk_path.to_string()) {
                paths.insert(pos, chunk_path.to_string());
            }
        }
    }

    /// Remove a chunk from all index entries (called during compaction)
    pub fn remove_chunk(&mut self, chunk_path: &str) {
        for col_map in self.entries.values_mut() {
            for paths in col_map.values_mut() {
                paths.retain(|p| p != chunk_path);
            }
            col_map.retain(|_, paths| !paths.is_empty());
        }
        self.entries.retain(|_, col_map| !col_map.is_empty());
    }

    /// Query: returns chunk paths matching ALL equality predicates (AND semantics).
    /// Returns None if a predicate column is not in the index (caller falls back to full scan).
    pub fn query(&self, predicates: &[(String, String)]) -> Option<HashSet<String>> {
        if predicates.is_empty() {
            return None;
        }
        let mut result: Option<HashSet<String>> = None;
        for (col, val) in predicates {
            let col_map = self.entries.get(col)?; // None = column not indexed
            let chunk_set: HashSet<String> = col_map
                .get(val)
                .map(|paths| paths.iter().cloned().collect())
                .unwrap_or_default();
            result = Some(match result {
                None => chunk_set,
                Some(existing) => existing.intersection(&chunk_set).cloned().collect(),
            });
        }
        result
    }

    /// Returns true if a column has reached the cardinality limit
    pub fn at_cardinality_limit(&self, col: &str, limit: usize) -> bool {
        self.entries
            .get(col)
            .map(|m| m.len() >= limit)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_query() {
        let mut idx = InvertedIndex::new();
        idx.add_chunk(
            "chunk_a",
            &[
                ("host".into(), "web-01".into()),
                ("region".into(), "us-east-1".into()),
            ],
        );
        idx.add_chunk(
            "chunk_b",
            &[
                ("host".into(), "web-02".into()),
                ("region".into(), "us-east-1".into()),
            ],
        );
        let result = idx.query(&[("host".into(), "web-01".into())]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("chunk_a"));
    }

    #[test]
    fn test_and_semantics() {
        let mut idx = InvertedIndex::new();
        idx.add_chunk(
            "chunk_a",
            &[
                ("host".into(), "web-01".into()),
                ("region".into(), "us-east-1".into()),
            ],
        );
        idx.add_chunk(
            "chunk_b",
            &[
                ("host".into(), "web-02".into()),
                ("region".into(), "us-east-1".into()),
            ],
        );
        let result = idx
            .query(&[
                ("host".into(), "web-02".into()),
                ("region".into(), "us-east-1".into()),
            ])
            .unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("chunk_b"));
    }

    #[test]
    fn test_remove_chunk() {
        let mut idx = InvertedIndex::new();
        idx.add_chunk("chunk_a", &[("host".into(), "web-01".into())]);
        idx.remove_chunk("chunk_a");
        // After removal, the column is gone entirely since it had only one value
        assert!(idx.query(&[("host".into(), "web-01".into())]).is_none());
    }

    #[test]
    fn test_unindexed_column_returns_none() {
        let idx = InvertedIndex::new();
        assert!(idx.query(&[("no_such_col".into(), "val".into())]).is_none());
    }

    #[test]
    fn test_cardinality_limit() {
        let mut idx = InvertedIndex::new();
        for i in 0..100 {
            idx.add_chunk(
                &format!("chunk_{}", i),
                &[("host".into(), format!("web-{:03}", i))],
            );
        }
        assert!(idx.at_cardinality_limit("host", 100));
        assert!(!idx.at_cardinality_limit("host", 101));
        assert!(!idx.at_cardinality_limit("missing", 1));
    }

    #[test]
    fn test_empty_predicates_returns_none() {
        let idx = InvertedIndex::new();
        assert!(idx.query(&[]).is_none());
    }

    #[test]
    fn test_dedup_chunk_paths() {
        let mut idx = InvertedIndex::new();
        idx.add_chunk("chunk_a", &[("host".into(), "web-01".into())]);
        // Adding the same chunk again should not duplicate
        idx.add_chunk("chunk_a", &[("host".into(), "web-01".into())]);
        let result = idx.query(&[("host".into(), "web-01".into())]).unwrap();
        assert_eq!(result.len(), 1);
    }
}
