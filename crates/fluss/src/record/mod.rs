use core::fmt;
use std::collections::HashMap;
use crate::metadata::TableBucket;
use crate::row::ColumnarRow;

mod arrow;
mod error;

pub use arrow::*;
use futures::stream::Scan;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeType {
    /// Append-only operation
    AppendOnly,
    /// Insert operation
    Insert,
    /// Update operation containing the previous content of the updated row
    UpdateBefore,
    /// Update operation containing the new content of the updated row
    UpdateAfter,
    /// Delete operation
    Delete,
}

impl ChangeType {
    /// Returns a short string representation of this ChangeType
    pub fn short_string(&self) -> &'static str {
        match self {
            ChangeType::AppendOnly => "+A",
            ChangeType::Insert => "+I",
            ChangeType::UpdateBefore => "-U",
            ChangeType::UpdateAfter => "+U",
            ChangeType::Delete => "-D",
        }
    }

    /// Returns the byte value representation used for serialization
    pub fn to_byte_value(&self) -> u8 {
        match self {
            ChangeType::AppendOnly => 0,
            ChangeType::Insert => 1,
            ChangeType::UpdateBefore => 2,
            ChangeType::UpdateAfter => 3,
            ChangeType::Delete => 4,
        }
    }

    /// Creates a ChangeType from its byte value representation
    ///
    /// # Errors
    /// Returns an error if the byte value doesn't correspond to any ChangeType
    pub fn from_byte_value(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(ChangeType::AppendOnly),
            1 => Ok(ChangeType::Insert),
            2 => Ok(ChangeType::UpdateBefore),
            3 => Ok(ChangeType::UpdateAfter),
            4 => Ok(ChangeType::Delete),
            _ => Err(format!(
                "Unsupported byte value '{value}' for change type"
            )),
        }
    }
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.short_string())
    }
}

pub struct ScanRecord {
    pub row: ColumnarRow,
    offset: i64,
    timestamp: i64,
    change_type: ChangeType,
}

impl ScanRecord {
    const INVALID: i64 = -1;

    pub fn new_default(row: ColumnarRow) -> Self {
        ScanRecord {
            row,
            offset: Self::INVALID,
            timestamp: Self::INVALID,
            change_type: ChangeType::Insert,
        }
    }

    pub fn new(row: ColumnarRow, offset: i64, timestamp: i64, change_type: ChangeType) -> Self {
        ScanRecord {
            row,
            offset,
            timestamp,
            change_type,
        }
    }

    pub fn row(&self) -> &ColumnarRow {
        &self.row
    }

    /// Returns the position in the log
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the change type
    pub fn change_type(&self) -> &ChangeType {
        &self.change_type
    }
}

pub struct ScanRecords {
    records: HashMap<TableBucket, Vec<ScanRecord>>,
}

impl ScanRecords {
    pub fn empty() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    pub fn new(records: HashMap<TableBucket, Vec<ScanRecord>>) -> Self {
        Self { records }
    }

    pub fn into_records(self) -> HashMap<TableBucket, Vec<ScanRecord>> {
        self.records
    }
    
    pub fn records(&self, scan_bucket: &TableBucket) -> &[ScanRecord] {
        self.records.get(scan_bucket).map_or(&[], |records| records)
    }

    pub fn count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl IntoIterator for ScanRecords {
    type Item = ScanRecord;
    type IntoIter = std::vec::IntoIter<ScanRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records
            .into_values()
            .flatten()
            .collect::<Vec<_>>()
            .into_iter()
    }
}