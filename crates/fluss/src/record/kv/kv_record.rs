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

//! Key-Value record implementation.
//!
//! This module provides the KvRecord struct which represents an immutable key-value record.
//! The record format is:
//! - Length => Int32
//! - KeyLength => Unsigned VarInt
//! - Key => bytes
//! - Row => BinaryRow (optional, if null then this is a deletion record)

use bytes::{BufMut, Bytes, BytesMut};
use std::io;

use crate::util::varint::{
    read_unsigned_varint_bytes, size_of_unsigned_varint, write_unsigned_varint_buf,
};

/// Length field size in bytes
pub const LENGTH_LENGTH: usize = 4;

/// A key-value record.
///
/// The schema is:
/// - Length => Int32
/// - KeyLength => Unsigned VarInt
/// - Key => bytes
/// - Value => bytes (BinaryRow, written directly without length prefix)
///
/// When the value is None (deletion), no Value bytes are present.
// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/record/KvRecord.java
#[derive(Debug, Clone)]
pub struct KvRecord {
    key: Bytes,
    value: Option<Bytes>,
    size_in_bytes: usize,
}

impl KvRecord {
    /// Create a new KvRecord with the given key and optional value.
    pub fn new(key: Bytes, value: Option<Bytes>) -> Self {
        let size_in_bytes = Self::size_of(&key, value.as_deref());
        Self {
            key,
            value,
            size_in_bytes,
        }
    }

    /// Get the key bytes.
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Get the value bytes (None indicates a deletion).
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Calculate the total size of the record when serialized (including length prefix).
    pub fn size_of(key: &[u8], value: Option<&[u8]>) -> usize {
        Self::size_without_length(key, value) + LENGTH_LENGTH
    }

    /// Calculate the size without the length prefix.
    fn size_without_length(key: &[u8], value: Option<&[u8]>) -> usize {
        let key_len = key.len();
        let key_len_size = size_of_unsigned_varint(key_len as u32);

        match value {
            Some(v) => key_len_size.saturating_add(key_len).saturating_add(v.len()),
            None => {
                // Deletion: no value bytes
                key_len_size.saturating_add(key_len)
            }
        }
    }

    /// Write a KV record to a buffer.
    ///
    /// Returns the number of bytes written.
    pub fn write_to_buf(buf: &mut BytesMut, key: &[u8], value: Option<&[u8]>) -> io::Result<usize> {
        let size_in_bytes = Self::size_without_length(key, value);

        let size_i32 = i32::try_from(size_in_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Record size {size_in_bytes} exceeds i32::MAX"),
            )
        })?;
        buf.put_i32_le(size_i32);
        let key_len = key.len() as u32;
        write_unsigned_varint_buf(key_len, buf);

        buf.put_slice(key);

        if let Some(v) = value {
            buf.put_slice(v);
        }
        // For None (deletion), don't write any value bytes

        Ok(size_in_bytes + LENGTH_LENGTH)
    }

    /// Read a KV record from bytes at the given position.
    ///
    /// Returns the KvRecord and the number of bytes consumed.
    ///
    /// TODO: Connect KvReadContext and return CompactedRow records.
    pub fn read_from(bytes: &Bytes, position: usize) -> io::Result<(Self, usize)> {
        if bytes.len() < position.saturating_add(LENGTH_LENGTH) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read record length",
            ));
        }

        let size_in_bytes_i32 = i32::from_le_bytes([
            bytes[position],
            bytes[position + 1],
            bytes[position + 2],
            bytes[position + 3],
        ]);

        if size_in_bytes_i32 < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid record length: {size_in_bytes_i32}"),
            ));
        }

        let size_in_bytes = size_in_bytes_i32 as usize;

        let total_size = size_in_bytes.checked_add(LENGTH_LENGTH).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Record size overflow: {size_in_bytes} + {LENGTH_LENGTH}"),
            )
        })?;

        let available = bytes.len().saturating_sub(position);
        if available < total_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Not enough bytes to read record: expected {total_size}, available {available}"
                ),
            ));
        }

        let mut current_offset = position + LENGTH_LENGTH;
        let record_end = position + total_size;

        // Read key length as unsigned varint (bounded by record end)
        let (key_len, varint_size) =
            read_unsigned_varint_bytes(&bytes[current_offset..record_end])?;
        current_offset += varint_size;

        // Read key bytes
        let key_end = current_offset + key_len as usize;
        if key_end > position + total_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key length exceeds record size",
            ));
        }
        let key = bytes.slice(current_offset..key_end);
        current_offset = key_end;

        // Read value bytes directly
        let value = if current_offset < record_end {
            // Value is present: all remaining bytes are the value
            let value_bytes = bytes.slice(current_offset..record_end);
            Some(value_bytes)
        } else {
            // No remaining bytes: this is a deletion record
            None
        };

        Ok((
            Self {
                key,
                value,
                size_in_bytes: total_size,
            },
            total_size,
        ))
    }

    /// Get the total size in bytes of this record.
    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_record_size_calculation() {
        let key = b"test_key";
        let value = b"test_value";

        // With value (no value length varint)
        let size_with_value = KvRecord::size_of(key, Some(value));
        assert_eq!(
            size_with_value,
            LENGTH_LENGTH + size_of_unsigned_varint(key.len() as u32) + key.len() + value.len()
        );

        // Without value
        let size_without_value = KvRecord::size_of(key, None);
        assert_eq!(
            size_without_value,
            LENGTH_LENGTH + size_of_unsigned_varint(key.len() as u32) + key.len()
        );
    }

    #[test]
    fn test_kv_record_write_read_round_trip() {
        let key = b"my_key";
        let value = b"my_value_data";

        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, key, Some(value)).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().as_ref(), key);
        assert_eq!(record.value().unwrap().as_ref(), value);
        assert_eq!(record.get_size_in_bytes(), written);
    }

    #[test]
    fn test_kv_record_deletion() {
        let key = b"delete_me";

        // Write deletion record (no value)
        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, key, None).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().as_ref(), key);
        assert!(record.value().is_none());
    }

    #[test]
    fn test_kv_record_with_large_key() {
        let key = vec![0u8; 1024];
        let value = vec![1u8; 4096];

        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, &key, Some(&value)).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().len(), key.len());
        assert_eq!(record.value().unwrap().len(), value.len());
    }

    #[test]
    fn test_invalid_record_lengths() {
        let mut buf = BytesMut::new();
        buf.put_i32_le(-1); // Negative length
        buf.put_u8(1); // Some dummy data
        buf.put_slice(b"key");
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);

        // Test overflow length
        let mut buf = BytesMut::new();
        buf.put_i32_le(i32::MAX); // Very large length
        buf.put_u8(1); // Some dummy data
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());

        // Test impossibly large but non-negative length
        let mut buf = BytesMut::new();
        buf.put_i32_le(1_000_000);
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_multiple_records_in_buffer() {
        let records = vec![
            (b"key1".as_slice(), Some(b"value1".as_slice())),
            (b"key2".as_slice(), None),
            (b"key3".as_slice(), Some(b"value3".as_slice())),
        ];

        let mut buf = BytesMut::new();
        for (key, value) in &records {
            KvRecord::write_to_buf(&mut buf, key, *value).unwrap();
        }

        let bytes = buf.freeze();
        let mut offset = 0;
        for (expected_key, expected_value) in &records {
            let (record, size) = KvRecord::read_from(&bytes, offset).unwrap();
            assert_eq!(record.key().as_ref(), *expected_key);
            match expected_value {
                Some(v) => assert_eq!(record.value().unwrap().as_ref(), *v),
                None => assert!(record.value().is_none()),
            }
            offset += size;
        }
        assert_eq!(offset, bytes.len());
    }
}
