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

use bytes::Bytes;

use crate::{
    metadata::DataType,
    row::{
        Datum, GenericRow,
        compacted::{compacted_row::CompactedRow, compacted_row_writer::CompactedRowWriter},
    },
};

#[allow(dead_code)]
pub struct CompactedRowDeserializer {
    schema: Vec<DataType>,
}

#[allow(dead_code)]
impl CompactedRowDeserializer {
    pub fn new(schema: Vec<DataType>) -> Self {
        Self { schema }
    }

    pub fn deserialize(&self, reader: &mut CompactedRowReader) -> GenericRow<'static> {
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
                DataType::Char(_) | DataType::String(_) => Datum::OwnedString(reader.read_string()),
                // TODO: use read_binary(length) in the future, but need to keep compatibility
                DataType::Bytes(_) | DataType::Binary(_) => {
                    Datum::Blob(reader.read_bytes().into_vec().into())
                }
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
pub struct CompactedRowReader {
    segment: Bytes,
    offset: usize,
    position: usize,
    limit: usize,
    header_size_in_bytes: usize,
}

#[allow(dead_code)]
impl CompactedRowReader {
    pub fn new(field_count: usize) -> Self {
        let header = CompactedRow::calculate_bit_set_width_in_bytes(field_count);
        Self {
            header_size_in_bytes: header,
            segment: Bytes::new(),
            offset: 0,
            position: 0,
            limit: 0,
        }
    }

    pub fn point_to(&mut self, data: Bytes, offset: usize, length: usize) {
        let limit = offset + length;
        let position = offset + self.header_size_in_bytes;

        debug_assert!(limit <= data.len());
        debug_assert!(position <= limit);

        self.segment = data;
        self.offset = offset;
        self.position = position;
        self.limit = limit;
    }

    pub fn is_null_at(&self, pos: usize) -> bool {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    pub fn read_boolean(&mut self) -> bool {
        self.read_byte() != 0
    }

    pub fn read_byte(&mut self) -> u8 {
        debug_assert!(self.position < self.limit);
        let b = self.segment[self.position];
        self.position += 1;
        b
    }

    pub fn read_short(&mut self) -> i16 {
        debug_assert!(self.position + 2 <= self.limit);
        let bytes_slice = &self.segment[self.position..self.position + 2];
        let byte_array: [u8; 2] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 2 bytes long");

        self.position += 2;
        i16::from_ne_bytes(byte_array)
    }

    pub fn read_int(&mut self) -> i32 {
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

    pub fn read_long(&mut self) -> i64 {
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

    pub fn read_float(&mut self) -> f32 {
        debug_assert!(self.position + 4 <= self.limit);
        let bytes_slice = &self.segment[self.position..self.position + 4];
        let byte_array: [u8; 4] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 4 bytes long");

        self.position += 4;
        f32::from_ne_bytes(byte_array)
    }

    pub fn read_double(&mut self) -> f64 {
        debug_assert!(self.position + 8 <= self.limit);
        let bytes_slice = &self.segment[self.position..self.position + 8];
        let byte_array: [u8; 8] = bytes_slice
            .try_into()
            .expect("Slice must be exactly 8 bytes long");

        self.position += 8;
        f64::from_ne_bytes(byte_array)
    }

    pub fn read_binary(&mut self, length: usize) -> Bytes {
        debug_assert!(self.position + length <= self.limit);

        let start = self.position;
        let end = start + length;
        self.position = end;

        self.segment.slice(start..end)
    }

    pub fn read_bytes(&mut self) -> Box<[u8]> {
        let len = self.read_int();
        debug_assert!(len >= 0);

        let len = len as usize;
        debug_assert!(self.position + len <= self.limit);

        let start = self.position;
        let end = start + len;
        self.position = end;

        self.segment[start..end].to_vec().into_boxed_slice()
    }

    pub fn read_string(&mut self) -> String {
        let bytes = self.read_bytes();
        String::from_utf8(bytes.into_vec()).unwrap_or_else(|e| panic!("Invalid UTF-8 in string data: {e}"))
    }
}
