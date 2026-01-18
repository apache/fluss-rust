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

use crate::metadata::RowType;
use crate::row::compacted::compacted_row::calculate_bit_set_width_in_bytes;
use crate::{
    metadata::DataType,
    row::{Datum, GenericRow, compacted::compacted_row_writer::CompactedRowWriter},
    util::varint::{read_unsigned_varint_at, read_unsigned_varint_u64_at},
};
use std::borrow::Cow;
use std::str::from_utf8;

#[allow(dead_code)]
#[derive(Clone)]
pub struct CompactedRowDeserializer<'a> {
    row_type: Cow<'a, RowType>,
}

#[allow(dead_code)]
impl<'a> CompactedRowDeserializer<'a> {
    pub fn new(row_type: &'a RowType) -> Self {
        Self {
            row_type: Cow::Borrowed(row_type),
        }
    }

    pub fn new_from_owned(row_type: RowType) -> Self {
        Self {
            row_type: Cow::Owned(row_type),
        }
    }

    pub fn get_row_type(&self) -> &RowType {
        self.row_type.as_ref()
    }

    pub fn deserialize(&self, reader: &CompactedRowReader<'a>) -> GenericRow<'a> {
        let mut row = GenericRow::new();
        let mut cursor = reader.initial_position();
        for (col_pos, data_field) in self.row_type.fields().iter().enumerate() {
            let dtype = &data_field.data_type;
            if dtype.is_nullable() && reader.is_null_at(col_pos) {
                row.set_field(col_pos, Datum::Null);
                continue;
            }
            let (datum, next_cursor) = match dtype {
                DataType::Boolean(_) => {
                    let (val, next) = reader.read_boolean(cursor);
                    (Datum::Bool(val), next)
                }
                DataType::TinyInt(_) => {
                    let (val, next) = reader.read_byte(cursor);
                    (Datum::Int8(val as i8), next)
                }
                DataType::SmallInt(_) => {
                    let (val, next) = reader.read_short(cursor);
                    (Datum::Int16(val), next)
                }
                DataType::Int(_) => {
                    let (val, next) = reader.read_int(cursor);
                    (Datum::Int32(val), next)
                }
                DataType::BigInt(_) => {
                    let (val, next) = reader.read_long(cursor);
                    (Datum::Int64(val), next)
                }
                DataType::Float(_) => {
                    let (val, next) = reader.read_float(cursor);
                    (Datum::Float32(val.into()), next)
                }
                DataType::Double(_) => {
                    let (val, next) = reader.read_double(cursor);
                    (Datum::Float64(val.into()), next)
                }
                // TODO: use read_char(length) in the future, but need to keep compatibility
                DataType::Char(_) | DataType::String(_) => {
                    let (val, next) = reader.read_string(cursor);
                    (Datum::String(val.into()), next)
                }
                // TODO: use read_binary(length) in the future, but need to keep compatibility
                DataType::Bytes(_) | DataType::Binary(_) => {
                    let (val, next) = reader.read_bytes(cursor);
                    (Datum::Blob(val.into()), next)
                }
                _ => panic!("unsupported DataType in CompactedRowDeserializer"),
            };
            cursor = next_cursor;
            row.set_field(col_pos, datum);
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
    limit: usize,
    header_size_in_bytes: usize,
}

#[allow(dead_code)]
impl<'a> CompactedRowReader<'a> {
    pub fn new(field_count: usize, data: &'a [u8], offset: usize, length: usize) -> Self {
        let header_size_in_bytes = calculate_bit_set_width_in_bytes(field_count);
        let limit = offset + length;
        let position = offset + header_size_in_bytes;
        debug_assert!(limit <= data.len());
        debug_assert!(position <= limit);

        CompactedRowReader {
            segment: data,
            offset,
            limit,
            header_size_in_bytes,
        }
    }

    fn initial_position(&self) -> usize {
        self.offset + self.header_size_in_bytes
    }

    pub fn is_null_at(&self, col_pos: usize) -> bool {
        let byte_index = col_pos >> 3;
        let bit = col_pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    pub fn read_boolean(&self, pos: usize) -> (bool, usize) {
        let (val, next) = self.read_byte(pos);
        (val != 0, next)
    }

    pub fn read_byte(&self, pos: usize) -> (u8, usize) {
        debug_assert!(pos < self.limit);
        (self.segment[pos], pos + 1)
    }

    pub fn read_short(&self, pos: usize) -> (i16, usize) {
        let next_pos = pos + 2;
        debug_assert!(next_pos <= self.limit);
        let bytes_slice = &self.segment[pos..pos + 2];
        let val = i16::from_ne_bytes(
            bytes_slice
                .try_into()
                .expect("Slice must be exactly 2 bytes long"),
        );
        (val, next_pos)
    }

    pub fn read_int(&self, pos: usize) -> (i32, usize) {
        match read_unsigned_varint_at(self.segment, pos, CompactedRowWriter::MAX_INT_SIZE) {
            Ok((value, next_pos)) => (value as i32, next_pos),
            Err(_) => panic!("Invalid VarInt32 input stream."),
        }
    }

    pub fn read_long(&self, pos: usize) -> (i64, usize) {
        match read_unsigned_varint_u64_at(self.segment, pos, CompactedRowWriter::MAX_LONG_SIZE) {
            Ok((value, next_pos)) => (value as i64, next_pos),
            Err(_) => panic!("Invalid VarInt64 input stream."),
        }
    }

    pub fn read_float(&self, pos: usize) -> (f32, usize) {
        let next_pos = pos + 4;
        debug_assert!(next_pos <= self.limit);
        let val = f32::from_ne_bytes(
            self.segment[pos..pos + 4]
                .try_into()
                .expect("Slice must be exactly 4 bytes long"),
        );
        (val, next_pos)
    }

    pub fn read_double(&self, pos: usize) -> (f64, usize) {
        let next_pos = pos + 8;
        debug_assert!(next_pos <= self.limit);
        let val = f64::from_ne_bytes(
            self.segment[pos..pos + 8]
                .try_into()
                .expect("Slice must be exactly 8 bytes long"),
        );
        (val, next_pos)
    }

    pub fn read_binary(&self, pos: usize) -> (&'a [u8], usize) {
        self.read_bytes(pos)
    }

    pub fn read_bytes(&self, pos: usize) -> (&'a [u8], usize) {
        let (len, data_pos) = self.read_int(pos);
        let len = len as usize;
        let next_pos = data_pos + len;
        debug_assert!(next_pos <= self.limit);
        (&self.segment[data_pos..next_pos], next_pos)
    }

    pub fn read_string(&self, pos: usize) -> (&'a str, usize) {
        let (bytes, next_pos) = self.read_bytes(pos);
        let s = from_utf8(bytes).expect("Invalid UTF-8 when reading string");
        (s, next_pos)
    }
}
