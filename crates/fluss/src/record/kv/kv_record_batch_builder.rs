// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! KV record batch builder implementation.
//!
//! This module provides the KvRecordBatchBuilder for building batches of KV records.

use bytes::{Bytes, BytesMut};
use std::io;

use crate::metadata::KvFormat;
use crate::record::kv::kv_record::KvRecord;
use crate::record::kv::kv_record_batch::{
    ATTRIBUTES_OFFSET, BATCH_SEQUENCE_OFFSET, CRC_OFFSET, LENGTH_LENGTH, LENGTH_OFFSET,
    MAGIC_OFFSET, RECORD_BATCH_HEADER_SIZE, RECORDS_COUNT_OFFSET, SCHEMA_ID_OFFSET,
    WRITER_ID_OFFSET,
};
use crate::record::kv::{CURRENT_KV_MAGIC_VALUE, NO_BATCH_SEQUENCE, NO_WRITER_ID};
use crate::row::compacted::CompactedRowWriter;

/// Builder for KvRecordBatch.
///
/// This builder accumulates KV records and produces a serialized batch with proper
/// header information and checksums.
pub struct KvRecordBatchBuilder {
    schema_id: i32,
    magic: u8,
    write_limit: usize,
    buffer: BytesMut,
    writer_id: i64,
    batch_sequence: i32,
    current_record_number: i32,
    size_in_bytes: usize,
    is_closed: bool,
    kv_format: KvFormat,
    aborted: bool,
    built_buffer: Option<Bytes>,
}

impl KvRecordBatchBuilder {
    /// Create a new KvRecordBatchBuilder.
    ///
    /// # Arguments
    /// * `schema_id` - The schema ID for records in this batch (must fit in i16)
    /// * `write_limit` - Maximum bytes that can be appended
    /// * `kv_format` - The KV format (Compacted, Indexed, or Aligned)
    pub fn new(schema_id: i32, write_limit: usize, kv_format: KvFormat) -> Self {
        assert!(
            schema_id <= i16::MAX as i32,
            "schema_id shouldn't be greater than the max value of i16: {}",
            i16::MAX
        );

        let mut buffer = BytesMut::with_capacity(write_limit.max(RECORD_BATCH_HEADER_SIZE));

        // Reserve space for header (we'll write it at the end)
        buffer.resize(RECORD_BATCH_HEADER_SIZE, 0);

        Self {
            schema_id,
            magic: CURRENT_KV_MAGIC_VALUE,
            write_limit,
            buffer,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            current_record_number: 0,
            size_in_bytes: RECORD_BATCH_HEADER_SIZE,
            is_closed: false,
            kv_format,
            aborted: false,
            built_buffer: None,
        }
    }

    /// Check if there is room for a new record containing the given key and value.
    /// If no records have been appended, this always returns true.
    pub fn has_room_for(&self, key: &[u8], value: Option<&[u8]>) -> bool {
        self.size_in_bytes + KvRecord::size_of(key, value) <= self.write_limit
    }

    /// Check if there is room for a new record containing the given key and CompactedRow.
    /// If no records have been appended, this always returns true.
    pub fn has_room_for_row(&self, key: &[u8], row_writer: Option<&CompactedRowWriter>) -> bool {
        let value = row_writer.map(|w: &CompactedRowWriter| w.buffer());
        self.has_room_for(key, value)
    }

    /// Append a KV record with a CompactedRow value to the batch.
    ///
    /// This is the recommended API for KvFormat::COMPACTED batches.
    ///
    /// # Arguments
    /// * `key` - The key bytes
    /// * `row_writer` - The CompactedRowWriter containing the serialized row data.
    ///   Pass None for deletion records.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The builder has been aborted
    /// - The builder is closed
    /// - Adding this record would exceed the write limit
    /// - The maximum number of records is exceeded
    /// - The KV format is not COMPACTED
    pub fn append_row(
        &mut self,
        key: &[u8],
        row_writer: Option<&CompactedRowWriter>,
    ) -> io::Result<()> {
        if self.kv_format != KvFormat::COMPACTED {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append_row can only be used with KvFormat::COMPACTED",
            ));
        }

        let value = row_writer.map(|w: &CompactedRowWriter| w.buffer());
        self.append(key, value)
    }

    /// Append a KV record to the batch with raw bytes.
    ///
    /// # Arguments
    /// * `key` - The key bytes
    /// * `value` - Optional value bytes. If None, this represents a deletion.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The builder has been aborted
    /// - The builder is closed
    /// - Adding this record would exceed the write limit
    /// - The maximum number of records is exceeded
    /// - The value format doesn't match the configured KV format
    pub fn append(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        if self.aborted {
            return Err(io::Error::other(
                "Tried to append a record, but KvRecordBatchBuilder has already been aborted",
            ));
        }

        if self.is_closed {
            return Err(io::Error::other(
                "Tried to append a record, but KvRecordBatchBuilder is closed for record appends",
            ));
        }

        // Check record count limit before mutation
        if self.current_record_number == i32::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Maximum number of records per batch exceeded, max records: {}",
                    i32::MAX
                ),
            ));
        }

        let record_size = KvRecord::size_of(key, value);
        if self.size_in_bytes + record_size > self.write_limit {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "Adding record would exceed write limit: {} + {} > {}",
                    self.size_in_bytes, record_size, self.write_limit
                ),
            ));
        }

        self.validate_row_format(value)?;

        let record_byte_size = KvRecord::write_to_buf(&mut self.buffer, key, value)?;
        debug_assert_eq!(record_byte_size, record_size, "Record size mismatch");

        self.current_record_number += 1;
        self.size_in_bytes += record_byte_size;

        // Invalidate cached buffer since we modified the batch
        self.built_buffer = None;

        Ok(())
    }

    /// Set the writer state (writer ID and batch base sequence).
    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.writer_id = writer_id;
        self.batch_sequence = batch_base_sequence;
        // Invalidate cached buffer since header fields changed
        self.built_buffer = None;
    }

    /// Reset the writer state.
    /// This triggers a rebuild of the batch header on next build.
    pub fn reset_writer_state(&mut self, writer_id: i64, batch_sequence: i32) {
        self.built_buffer = None;
        self.writer_id = writer_id;
        self.batch_sequence = batch_sequence;
    }

    /// Build the batch and return the serialized bytes.
    /// This can be called multiple times as the batch is cached after the first build.
    pub fn build(&mut self) -> io::Result<Bytes> {
        if self.aborted {
            return Err(io::Error::other(
                "Attempting to build an aborted record batch",
            ));
        }

        if let Some(ref cached) = self.built_buffer {
            return Ok(cached.clone());
        }

        self.write_batch_header()?;
        let bytes = self.buffer.clone().freeze();
        self.built_buffer = Some(bytes.clone());
        Ok(bytes)
    }

    /// Get the writer ID.
    pub fn writer_id(&self) -> i64 {
        self.writer_id
    }

    /// Get the batch sequence.
    pub fn batch_sequence(&self) -> i32 {
        self.batch_sequence
    }

    /// Check if the builder is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Abort the builder.
    /// After aborting, no more records can be appended and the batch cannot be built.
    pub fn abort(&mut self) {
        self.aborted = true;
    }

    /// Close the builder.
    /// After closing, no more records can be appended, but the batch can still be built.
    pub fn close(&mut self) -> io::Result<()> {
        if self.aborted {
            return Err(io::Error::other(
                "Cannot close KvRecordBatchBuilder as it has already been aborted",
            ));
        }
        self.is_closed = true;
        Ok(())
    }

    /// Get the current size in bytes of the batch.
    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    // ----------------------- Internal methods -------------------------------

    /// Write the batch header.
    fn write_batch_header(&mut self) -> io::Result<()> {
        let size_without_length = self.size_in_bytes - LENGTH_LENGTH;
        let total_size = i32::try_from(size_without_length).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Batch size {} exceeds i32::MAX", size_without_length),
            )
        })?;

        // Compute attributes before borrowing buffer mutably
        let attributes = self.compute_attributes();

        // Write to the beginning of the buffer
        let header = &mut self.buffer[0..RECORD_BATCH_HEADER_SIZE];

        // Write length
        header[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH]
            .copy_from_slice(&total_size.to_be_bytes());

        // Write magic
        header[MAGIC_OFFSET] = self.magic;

        // Write empty CRC first (will update later)
        header[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&0u32.to_be_bytes());

        // Write schema ID
        header[SCHEMA_ID_OFFSET..SCHEMA_ID_OFFSET + 2]
            .copy_from_slice(&(self.schema_id as i16).to_be_bytes());

        // Write attributes
        header[ATTRIBUTES_OFFSET] = attributes;

        // Write writer ID
        header[WRITER_ID_OFFSET..WRITER_ID_OFFSET + 8]
            .copy_from_slice(&self.writer_id.to_be_bytes());

        // Write batch sequence
        header[BATCH_SEQUENCE_OFFSET..BATCH_SEQUENCE_OFFSET + 4]
            .copy_from_slice(&self.batch_sequence.to_be_bytes());

        // Write record count
        header[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&self.current_record_number.to_be_bytes());

        // Compute and update CRC
        let crc = crc32c::crc32c(&self.buffer[SCHEMA_ID_OFFSET..self.size_in_bytes]);
        self.buffer[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

        Ok(())
    }

    /// Compute the attributes byte.
    fn compute_attributes(&self) -> u8 {
        // Currently no attributes are used
        0
    }

    /// Validate the row format according to the KV format.
    fn validate_row_format(&self, value: Option<&[u8]>) -> io::Result<()> {
        match self.kv_format {
            KvFormat::COMPACTED => {
                if let Some(bytes) = value {
                    // CompactedRow must have at least a header (null bitmap).
                    // The minimum size is at least 1 byte for any row (even 1 field has 1 byte header).
                    // We can't validate the exact header size without knowing field count (arity),
                    // but we can at least check it's not empty.
                    if bytes.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "CompactedRow value cannot be empty (must have at least a header)",
                        ));
                    }
                    // Further validation could be done if we stored schema/field count,
                    // but for now this basic check prevents obvious errors.
                }
                Ok(())
            }
            KvFormat::INDEXED => {
                // IndexedRow is not yet implemented
                if value.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "KvFormat::INDEXED is not yet implemented",
                    ));
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic_workflow() {
        let schema_id = 42;
        let write_limit = 4096;
        let mut builder = KvRecordBatchBuilder::new(schema_id, write_limit, KvFormat::COMPACTED);

        // Test initial state
        assert!(!builder.is_closed());
        assert_eq!(builder.writer_id(), NO_WRITER_ID);
        assert_eq!(builder.batch_sequence(), NO_BATCH_SEQUENCE);

        // Test writer state
        builder.set_writer_state(100, 5);
        assert_eq!(builder.writer_id(), 100);
        assert_eq!(builder.batch_sequence(), 5);

        // Test appending records
        let key1 = b"key1";
        let value1 = b"value1";
        assert!(builder.has_room_for(key1, Some(value1)));
        builder.append(key1, Some(value1)).unwrap();

        let key2 = b"key2";
        assert!(builder.has_room_for(key2, None));
        builder.append(key2, None).unwrap();

        // Test close and build
        builder.close().unwrap();
        assert!(builder.is_closed());

        let bytes = builder.build().unwrap();
        assert!(bytes.len() > RECORD_BATCH_HEADER_SIZE);

        // Building again should return cached result
        let bytes2 = builder.build().unwrap();
        assert_eq!(bytes.len(), bytes2.len());
    }

    #[test]
    fn test_builder_lifecycle() {
        // Test abort behavior
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        builder.append(b"key", Some(b"value")).unwrap();
        builder.abort();
        assert!(builder.append(b"key2", None).is_err());
        assert!(builder.build().is_err());
        assert!(builder.close().is_err());

        // Test close behavior
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        builder.append(b"key", Some(b"value")).unwrap();
        builder.close().unwrap();
        assert!(builder.append(b"key2", None).is_err()); // Can't append after close
        assert!(builder.build().is_ok()); // But can still build
    }

    #[test]
    fn test_write_limit_enforcement() {
        let write_limit = 100; // Very small limit
        let mut builder = KvRecordBatchBuilder::new(1, write_limit, KvFormat::COMPACTED);

        // Test has_room_for helper
        let large_key = vec![0u8; 1000];
        let large_value = vec![1u8; 1000];
        assert!(!builder.has_room_for(&large_key, Some(&large_value)));
        assert!(builder.has_room_for(b"key", Some(b"value")));

        // Test append enforcement - add small record first
        builder.append(b"key", Some(b"value")).unwrap();

        // Try to add large record that exceeds limit
        let result = builder.append(b"key2", Some(&large_value));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_append_checks_record_count_limit() {
        let mut builder = KvRecordBatchBuilder::new(1, 100000, KvFormat::COMPACTED);
        builder.current_record_number = i32::MAX - 1;

        builder.append(b"key1", Some(b"value1")).unwrap();

        let result = builder.append(b"key2", Some(b"value2"));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_builder_reset_writer_state() {
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);

        builder.set_writer_state(100, 5);
        builder.append(b"key", Some(b"value")).unwrap();
        let bytes1 = builder.build().unwrap();

        // Reset writer state should invalidate cached buffer
        builder.reset_writer_state(200, 10);
        assert_eq!(builder.writer_id(), 200);
        assert_eq!(builder.batch_sequence(), 10);

        let bytes2 = builder.build().unwrap();
        // The bytes should be different because writer state changed
        assert_ne!(bytes1, bytes2);
    }

    #[test]
    #[should_panic(expected = "schema_id shouldn't be greater than")]
    fn test_builder_invalid_schema_id() {
        KvRecordBatchBuilder::new(i16::MAX as i32 + 1, 4096, KvFormat::COMPACTED);
    }

    #[test]
    fn test_cache_invalidation_on_append() {
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);

        builder.append(b"key1", Some(b"value1")).unwrap();
        let bytes1 = builder.build().unwrap();
        let len1 = bytes1.len();

        // Append another record - this should invalidate the cache
        builder.append(b"key2", Some(b"value2")).unwrap();
        let bytes2 = builder.build().unwrap();
        let len2 = bytes2.len();

        // Verify the second build includes both records
        assert!(len2 > len1, "Second build should be larger");

        use crate::record::kv::KvRecordBatch;
        let batch = KvRecordBatch::new(bytes2, 0);
        assert!(batch.is_valid());
        assert_eq!(batch.record_count(), 2, "Should have 2 records");
    }

    #[test]
    fn test_cache_invalidation_on_set_writer_state() {
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);

        builder.set_writer_state(100, 5);
        builder.append(b"key", Some(b"value")).unwrap();
        let bytes1 = builder.build().unwrap();

        // Change writer state - this should invalidate the cache
        builder.set_writer_state(200, 10);
        let bytes2 = builder.build().unwrap();

        assert_ne!(
            bytes1, bytes2,
            "Bytes should differ after writer state change"
        );

        use crate::record::kv::KvRecordBatch;
        let batch1 = KvRecordBatch::new(bytes1, 0);
        let batch2 = KvRecordBatch::new(bytes2, 0);

        assert_eq!(batch1.writer_id(), 100);
        assert_eq!(batch1.batch_sequence(), 5);

        assert_eq!(batch2.writer_id(), 200);
        assert_eq!(batch2.batch_sequence(), 10);
    }

    #[test]
    fn test_builder_with_many_records() {
        let mut builder = KvRecordBatchBuilder::new(1, 100000, KvFormat::COMPACTED);
        builder.set_writer_state(1, 0);

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            builder
                .append(key.as_bytes(), Some(value.as_bytes()))
                .unwrap();
        }

        builder.close().unwrap();
        let bytes = builder.build().unwrap();

        use crate::record::kv::KvRecordBatch;
        let batch = KvRecordBatch::new(bytes, 0);
        assert!(batch.is_valid());
        assert_eq!(batch.record_count(), 100);
        assert_eq!(batch.writer_id(), 1);
        assert_eq!(batch.batch_sequence(), 0);

        let records: Vec<_> = batch.records().unwrap().collect();
        assert_eq!(records.len(), 100);
        for (i, result) in records.iter().enumerate() {
            let record = result.as_ref().unwrap();
            let expected_key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(record.key(), expected_key.as_bytes());
            assert_eq!(record.value().unwrap(), expected_value.as_bytes());
        }
    }

    #[test]
    fn test_builder_with_compacted_row_writer() {
        use crate::metadata::{DataType, IntType, StringType};
        use crate::record::kv::KvRecordBatch;
        use crate::row::InternalRow;
        use crate::row::compacted::CompactedRow;

        let mut builder = KvRecordBatchBuilder::new(1, 100000, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);

        let types = vec![
            DataType::Int(IntType::new()),
            DataType::String(StringType::new()),
        ];

        // Create and append first record with CompactedRowWriter
        let mut row_writer1 = CompactedRowWriter::new(2);
        row_writer1.write_int(42);
        row_writer1.write_string("hello");

        let key1 = b"key1";
        assert!(builder.has_room_for_row(key1, Some(&row_writer1)));
        builder.append_row(key1, Some(&row_writer1)).unwrap();

        // Create and append second record
        let mut row_writer2 = CompactedRowWriter::new(2);
        row_writer2.write_int(100);
        row_writer2.write_string("world");

        let key2 = b"key2";
        builder.append_row(key2, Some(&row_writer2)).unwrap();

        // Append a deletion record
        let key3 = b"key3";
        builder.append_row(key3, None).unwrap();

        // Build and verify
        builder.close().unwrap();
        let bytes = builder.build().unwrap();

        let batch = KvRecordBatch::new(bytes, 0);
        assert!(batch.is_valid());
        assert_eq!(batch.record_count(), 3);
        assert_eq!(batch.writer_id(), 100);
        assert_eq!(batch.batch_sequence(), 5);

        // Read back and verify records
        let records: Vec<_> = batch.records().unwrap().collect();
        assert_eq!(records.len(), 3);

        // Verify first record
        let record1 = records[0].as_ref().unwrap();
        assert_eq!(record1.key().as_ref(), key1);
        let row1 = CompactedRow::from_bytes(&types, record1.value().unwrap());
        assert_eq!(row1.get_int(0), 42);
        assert_eq!(row1.get_string(1), "hello");

        // Verify second record
        let record2 = records[1].as_ref().unwrap();
        assert_eq!(record2.key().as_ref(), key2);
        let row2 = CompactedRow::from_bytes(&types, record2.value().unwrap());
        assert_eq!(row2.get_int(0), 100);
        assert_eq!(row2.get_string(1), "world");

        // Verify deletion record
        let record3 = records[2].as_ref().unwrap();
        assert_eq!(record3.key().as_ref(), key3);
        assert!(record3.value().is_none());
    }

    #[test]
    fn test_append_row_validates_kv_format() {
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::INDEXED);

        let mut row_writer = CompactedRowWriter::new(1);
        row_writer.write_int(42);

        let result = builder.append_row(b"key", Some(&row_writer));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_validate_compacted_row_format() {
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);

        let result = builder.append(b"key", Some(&[]));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);

        let mut row_writer = CompactedRowWriter::new(1);
        row_writer.write_int(42);
        let result = builder.append(b"key", Some(row_writer.buffer()));
        assert!(result.is_ok());
    }
}
