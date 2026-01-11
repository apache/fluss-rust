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

use crate::row::compacted::CompactedRow;
use crate::{
    metadata::DataType,
    row::{Datum, GenericRow, compacted::compacted_row_writer::CompactedRowWriter},
};
use std::cell::Cell;
use std::str::from_utf8;

#[allow(dead_code)]
pub struct CompactedRowDeserializer<'a> {
    schema: &'a [DataType],
}

#[allow(dead_code)]
impl<'a> CompactedRowDeserializer<'a> {
    pub fn new(schema: &'a [DataType]) -> Self {
        Self { schema }
    }

    pub fn deserialize(&self, reader: &CompactedRowReader<'a>) -> GenericRow<'a> {
        let mut row = GenericRow::new();
        for (pos, dtype) in self.schema.iter().enumerate() {
            if reader.is_null_at(pos) {
                row.set_field(pos, Datum::Null);
                continue;
            }
            let datum = match dtype {
                DataType::Boolean(_) => Datum::Bool(reader.read_boolean()),
                DataType::TinyInt(_) => Datum::Int8(reader.read_byte() as i8),
                DataType::SmallInt(_) => Datum::Int16(reader.read_short()),
                DataType::Int(_) => Datum::Int32(reader.read_int()),
                DataType::BigInt(_) => Datum::Int64(reader.read_long()),
                DataType::Float(_) => Datum::Float32(reader.read_float().into()),
                DataType::Double(_) => Datum::Float64(reader.read_double().into()),
                // TODO: use read_char(length) in the future, but need to keep compatibility
                DataType::Char(_) | DataType::String(_) => {
                    Datum::String(reader.read_string().into())
                }
                // TODO: use read_binary(length) in the future, but need to keep compatibility
                DataType::Bytes(_) | DataType::Binary(_) => Datum::Blob(reader.read_bytes().into()),
                _ => panic!("unsupported DataType in CompactedRowDeserializer"),
            };
            row.set_field(pos, datum);
        }
        row
    }
}

// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRowReader.java
#[allow(dead_code)]
pub struct CompactedRowReader<'a> {
    segment: &'a [u8],
    offset: usize,
    position: Cell<usize>,
    limit: usize,
    header_size_in_bytes: usize,
}

#[allow(dead_code)]
impl<'a> CompactedRowReader<'a> {
    pub fn new(field_count: usize, data: &'a [u8], offset: usize, length: usize) -> Self {
        let header_size_in_bytes = CompactedRow::calculate_bit_set_width_in_bytes(field_count);
        let limit = offset + length;
        let position = offset + header_size_in_bytes;
        debug_assert!(limit <= data.len());
        debug_assert!(position <= limit);

        CompactedRowReader {
            segment: data,
            offset,
            position: Cell::new(position),
            limit,
            header_size_in_bytes,
        }
    }

    pub fn is_null_at(&self, pos: usize) -> bool {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    pub fn read_boolean(&self) -> bool {
        self.read_byte() != 0
    }

    pub fn read_byte(&self) -> u8 {
        let pos = self.position.get();
        debug_assert!(pos < self.limit);
        let b = self.segment[pos];
        self.position.set(pos + 1);
        b
    }

    pub fn read_short(&self) -> i16 {
        let pos = self.position.get();
        debug_assert!(pos + 2 <= self.limit);
        let bytes_slice = &self.segment[pos..pos + 2];
        let byte_array: [u8; 2] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 2 bytes long");

        self.position.set(pos + 2);
        i16::from_ne_bytes(byte_array)
    }

    pub fn read_int(&self) -> i32 {
        let mut result: u32 = 0;
        let mut shift = 0;

        for _ in 0..CompactedRowWriter::MAX_INT_SIZE {
            let b = self.read_byte();
            result |= ((b & 0x7F) as u32) << shift;
            if (b & 0x80) == 0 {
                return result as i32;
            }
            shift += 7;
        }

        panic!("Invalid input stream.");
    }

    pub fn read_long(&self) -> i64 {
        let mut result: u64 = 0;
        let mut shift = 0;

        for _ in 0..CompactedRowWriter::MAX_LONG_SIZE {
            let b = self.read_byte();
            result |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return result as i64;
            }
            shift += 7;
        }

        panic!("Invalid input stream.");
    }

    pub fn read_float(&self) -> f32 {
        let pos = self.position.get();
        debug_assert!(pos + 4 <= self.limit);
        let bytes_slice = &self.segment[pos..pos + 4];
        let byte_array: [u8; 4] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 4 bytes long");

        self.position.set(pos + 4);
        f32::from_ne_bytes(byte_array)
    }

    pub fn read_double(&self) -> f64 {
        let pos = self.position.get();
        debug_assert!(pos + 8 <= self.limit);
        let bytes_slice = &self.segment[pos..pos + 8];
        let byte_array: [u8; 8] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 8 bytes long");

        self.position.set(pos + 8);
        f64::from_ne_bytes(byte_array)
    }

    pub fn read_binary(&self, length: usize) -> &[u8] {
        let pos = self.position.get();
        debug_assert!(pos + length <= self.limit);

        let start = pos;
        let end = start + length;
        self.position.set(end);

        &self.segment[start..end]
    }

    pub fn read_bytes(&self) -> &'a [u8] {
        let len = self.read_int();
        debug_assert!(len >= 0);
        let pos = self.position.get();

        let len = len as usize;
        debug_assert!(pos + len <= self.limit);

        let start = pos;
        let end = start + len;
        self.position.set(end);

        &self.segment[start..end]
    }

    pub fn read_string(&self) -> &'a str {
        let bytes = self.read_bytes();
        from_utf8(bytes).expect("Invalid UTF-8 when reading string from compacted row")
    }
}
