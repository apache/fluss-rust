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
use std::io::{self, Write};

use crate::util::varint::{
    read_unsigned_varint_bytes, size_of_unsigned_varint, write_unsigned_varint,
    write_unsigned_varint_buf,
};

/// Length field size in bytes
pub const LENGTH_LENGTH: usize = 4;

/// A key-value record.
///
/// The schema is:
/// - Length => Int32
/// - KeyLength => Unsigned VarInt
/// - Key => bytes
/// - ValueLength => Unsigned VarInt (only present if value exists, even for empty values)
/// - Value => bytes (if ValueLength > 0)
///
/// When the value is None (deletion), no ValueLength or Value bytes are present.
/// When the value is Some(&[]) (empty value), ValueLength is present with value 0.
#[derive(Debug, Clone)]
pub struct KvRecord {
    key: Bytes,
    value: Option<Bytes>,
}

impl KvRecord {
    /// Create a new KvRecord with the given key and optional value.
    pub fn new(key: Bytes, value: Option<Bytes>) -> Self {
        Self { key, value }
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
            Some(v) => {
                let value_len_size = size_of_unsigned_varint(v.len() as u32);
                key_len_size + key_len + value_len_size + v.len()
            }
            None => {
                // Deletion: no value length varint or value bytes
                key_len_size + key_len
            }
        }
    }

    /// Write a KV record to a writer.
    ///
    /// Returns the number of bytes written.
    pub fn write_to<W: Write>(
        writer: &mut W,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> io::Result<usize> {
        let size_in_bytes = Self::size_without_length(key, value);

        let size_i32 = i32::try_from(size_in_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Record size {} exceeds i32::MAX", size_in_bytes),
            )
        })?;
        writer.write_all(&size_i32.to_be_bytes())?;

        let key_len = key.len() as u32;
        write_unsigned_varint(key_len, writer)?;

        writer.write_all(key)?;

        if let Some(v) = value {
            let value_len = v.len() as u32;
            write_unsigned_varint(value_len, writer)?;
            writer.write_all(v)?;
        }
        // For None (deletion), don't write value length or value bytes

        Ok(size_in_bytes + LENGTH_LENGTH)
    }

    /// Write a KV record to a buffer.
    ///
    /// Returns the number of bytes written.
    pub fn write_to_buf(buf: &mut BytesMut, key: &[u8], value: Option<&[u8]>) -> io::Result<usize> {
        let size_in_bytes = Self::size_without_length(key, value);

        let size_i32 = i32::try_from(size_in_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Record size {} exceeds i32::MAX", size_in_bytes),
            )
        })?;
        buf.put_i32(size_i32);

        // Write key length as unsigned varint
        let key_len = key.len() as u32;
        write_unsigned_varint_buf(key_len, buf);

        buf.put_slice(key);

        // Write the value length and bytes if present (even for empty values)
        if let Some(v) = value {
            let value_len = v.len() as u32;
            write_unsigned_varint_buf(value_len, buf);
            buf.put_slice(v);
        }
        // For None (deletion), don't write value length or value bytes

        Ok(size_in_bytes + LENGTH_LENGTH)
    }

    /// Read a KV record from bytes at the given position.
    ///
    /// Returns the KvRecord and the number of bytes consumed.
    pub fn read_from(bytes: &Bytes, position: usize) -> io::Result<(Self, usize)> {
        if bytes.len() < position.saturating_add(LENGTH_LENGTH) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read record length",
            ));
        }

        let size_in_bytes_i32 = i32::from_be_bytes([
            bytes[position],
            bytes[position + 1],
            bytes[position + 2],
            bytes[position + 3],
        ]);

        if size_in_bytes_i32 < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid record length: {}", size_in_bytes_i32),
            ));
        }

        let size_in_bytes = size_in_bytes_i32 as usize;

        let total_size = size_in_bytes.checked_add(LENGTH_LENGTH).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Record size overflow: {} + {}",
                    size_in_bytes, LENGTH_LENGTH
                ),
            )
        })?;

        // Check against available bytes with checked arithmetic
        let available = bytes.len().saturating_sub(position);
        if available < total_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Not enough bytes to read record: expected {}, available {}",
                    total_size,
                    bytes.len() - position
                ),
            ));
        }

        // Start reading from after the length field
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

        let remaining_bytes = (position + total_size) - current_offset;

        let value = if remaining_bytes > 0 {
            // Value is present: read value length varint
            let (value_len, varint_size) =
                read_unsigned_varint_bytes(&bytes[current_offset..record_end])?;
            current_offset += varint_size;

            // Read value bytes
            let value_end = current_offset + value_len as usize;
            if value_end > record_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Value length exceeds record size",
                ));
            }

            // Use slice for both empty and non-empty values (zero-copy)
            let value_bytes = bytes.slice(current_offset..value_end);
            current_offset = value_end;

            // Verify no trailing bytes remain
            if current_offset != record_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Record has trailing bytes after value",
                ));
            }

            Some(value_bytes)
        } else {
            // No remaining bytes: this is a deletion
            None
        };

        Ok((Self { key, value }, total_size))
    }

    /// Get the total size in bytes of this record.
    pub fn get_size_in_bytes(&self) -> usize {
        Self::size_of(&self.key, self.value.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_record_size_calculation() {
        let key = b"test_key";
        let value = b"test_value";

        // With value (includes value length varint)
        let size_with_value = KvRecord::size_of(key, Some(value));
        assert_eq!(
            size_with_value,
            LENGTH_LENGTH
                + size_of_unsigned_varint(key.len() as u32)
                + key.len()
                + size_of_unsigned_varint(value.len() as u32)
                + value.len()
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
    fn test_empty_value_vs_deletion() {
        let key = b"test_key";

        // Write empty value (Some(&[]))
        let mut buf1 = BytesMut::new();
        let written1 = KvRecord::write_to_buf(&mut buf1, key, Some(&[])).unwrap();
        let bytes1 = buf1.freeze();
        let (record1, _) = KvRecord::read_from(&bytes1, 0).unwrap();

        // Write deletion (None)
        let mut buf2 = BytesMut::new();
        let written2 = KvRecord::write_to_buf(&mut buf2, key, None).unwrap();
        let bytes2 = buf2.freeze();
        let (record2, _) = KvRecord::read_from(&bytes2, 0).unwrap();

        assert!(record1.value().is_some(), "Empty value should be Some");
        assert_eq!(
            record1.value().unwrap().len(),
            0,
            "Empty value should have length 0"
        );

        assert!(record2.value().is_none(), "Deletion should be None");

        assert_ne!(
            written1, written2,
            "Empty value and deletion should have different sizes"
        );
    }

    #[test]
    fn test_record_with_trailing_bytes() {
        use bytes::BufMut;

        let key = b"key";
        let value = b"val";

        let mut buf = BytesMut::new();

        // Calculate correct size
        let key_len_varint_size = size_of_unsigned_varint(key.len() as u32);
        let value_len_varint_size = size_of_unsigned_varint(value.len() as u32);
        let correct_size = key_len_varint_size + key.len() + value_len_varint_size + value.len();

        // Write INCORRECT length that includes trailing bytes
        let incorrect_size = correct_size + 10; // Add 10 trailing bytes
        buf.put_i32(incorrect_size as i32);

        write_unsigned_varint_buf(key.len() as u32, &mut buf);
        buf.put_slice(key);

        write_unsigned_varint_buf(value.len() as u32, &mut buf);
        buf.put_slice(value);

        // Add 10 bytes of trailing garbage
        buf.put_slice(&[0xFF; 10]);

        let bytes = buf.freeze();

        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err(), "Should reject record with trailing bytes");
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
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
        buf.put_i32(-1); // Negative length
        buf.put_u8(1); // Some dummy data
        buf.put_slice(b"key");
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);

        // Test overflow length
        let mut buf = BytesMut::new();
        buf.put_i32(i32::MAX); // Very large length
        buf.put_u8(1); // Some dummy data
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());

        // Test impossibly large but non-negative length
        let mut buf = BytesMut::new();
        buf.put_i32(1_000_000);
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
