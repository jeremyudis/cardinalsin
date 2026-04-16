//! Roaring bitmap postings lists for the inverted index.

use crate::{Error, Result};
use roaring::RoaringBitmap;

/// A postings list backed by a Roaring bitmap.
///
/// Each entry is an ordinal (u32) that maps to a chunk path
/// via the segment's ordinal table.
#[derive(Debug, Clone)]
pub struct PostingsList {
    bitmap: RoaringBitmap,
}

impl PostingsList {
    /// Create an empty postings list.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Add an ordinal to the postings list.
    pub fn add(&mut self, ordinal: u32) {
        self.bitmap.insert(ordinal);
    }

    /// AND intersection of two postings lists.
    pub fn intersect(&self, other: &PostingsList) -> PostingsList {
        PostingsList {
            bitmap: &self.bitmap & &other.bitmap,
        }
    }

    /// OR union of two postings lists.
    pub fn union(&self, other: &PostingsList) -> PostingsList {
        PostingsList {
            bitmap: &self.bitmap | &other.bitmap,
        }
    }

    /// Set difference (self - other).
    pub fn difference(&self, other: &PostingsList) -> PostingsList {
        PostingsList {
            bitmap: &self.bitmap - &other.bitmap,
        }
    }

    /// Return all ordinals in the postings list.
    pub fn ordinals(&self) -> Vec<u32> {
        self.bitmap.iter().collect()
    }

    /// Serialize to Roaring portable format.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.bitmap.serialized_size());
        self.bitmap
            .serialize_into(&mut buf)
            .expect("serialization to Vec should not fail");
        buf
    }

    /// Deserialize from Roaring portable format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let bitmap = RoaringBitmap::deserialize_from(bytes)
            .map_err(|e| Error::IndexCorrupt(format!("Invalid roaring bitmap: {e}")))?;
        Ok(PostingsList { bitmap })
    }

    /// Number of ordinals in the postings list.
    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    /// Whether the postings list is empty.
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

impl Default for PostingsList {
    fn default() -> Self {
        Self::new()
    }
}
