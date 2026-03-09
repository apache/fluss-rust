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
    error::{Error::IllegalArgument, Result},
    metadata::DataType,
    row::{Datum, Decimal, GenericRow, compacted::compacted_row_writer::CompactedRowWriter},
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

    pub fn deserialize(&self, reader: &CompactedRowReader<'a>) -> Result<GenericRow<'a>> {
        let mut row = GenericRow::new(self.row_type.fields().len());
        let mut cursor = reader.initial_position();
        for (col_pos, data_field) in self.row_type.fields().iter().enumerate() {
            let dtype = &data_field.data_type;
            if dtype.is_nullable() && reader.is_null_at(col_pos) {
                row.set_field(col_pos, Datum::Null);
                continue;
            }
            let (datum, next_cursor) = match dtype {
                DataType::Boolean(_) => {
                    let (val, next) = reader.read_boolean(cursor)?;
                    (Datum::Bool(val), next)
                }
                DataType::TinyInt(_) => {
                    let (val, next) = reader.read_byte(cursor)?;
                    (Datum::Int8(val as i8), next)
                }
                DataType::SmallInt(_) => {
                    let (val, next) = reader.read_short(cursor)?;
                    (Datum::Int16(val), next)
                }
                DataType::Int(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Int32(val), next)
                }
                DataType::BigInt(_) => {
                    let (val, next) = reader.read_long(cursor)?;
                    (Datum::Int64(val), next)
                }
                DataType::Float(_) => {
                    let (val, next) = reader.read_float(cursor)?;
                    (Datum::Float32(val.into()), next)
                }
                DataType::Double(_) => {
                    let (val, next) = reader.read_double(cursor)?;
                    (Datum::Float64(val.into()), next)
                }
                // TODO: use read_char(length) in the future, but need to keep compatibility
                DataType::Char(_) | DataType::String(_) => {
                    let (val, next) = reader.read_string(cursor)?;
                    (Datum::String(val.into()), next)
                }
                // TODO: use read_binary(length) in the future, but need to keep compatibility
                DataType::Bytes(_) | DataType::Binary(_) => {
                    let (val, next) = reader.read_bytes(cursor)?;
                    (Datum::Blob(val.into()), next)
                }
                DataType::Decimal(decimal_type) => {
                    let precision = decimal_type.precision();
                    let scale = decimal_type.scale();
                    if Decimal::is_compact_precision(precision) {
                        // Compact: stored as i64
                        let (val, next) = reader.read_long(cursor)?;
                        let decimal =
                            Decimal::from_unscaled_long(val, precision, scale).map_err(|e| {
                                IllegalArgument {
                                    message: format!(
                                        "Failed to create decimal from unscaled long: {e}"
                                    ),
                                }
                            })?;
                        (Datum::Decimal(decimal), next)
                    } else {
                        // Non-compact: stored as minimal big-endian bytes
                        let (bytes, next) = reader.read_bytes(cursor)?;
                        let decimal = Decimal::from_unscaled_bytes(bytes, precision, scale)
                            .map_err(|e| IllegalArgument {
                                message: format!(
                                    "Failed to create decimal from unscaled bytes: {e}"
                                ),
                            })?;
                        (Datum::Decimal(decimal), next)
                    }
                }
                DataType::Date(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Date(crate::row::datum::Date::new(val)), next)
                }
                DataType::Time(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Time(crate::row::datum::Time::new(val)), next)
                }
                DataType::Timestamp(timestamp_type) => {
                    let precision = timestamp_type.precision();
                    if crate::row::datum::TimestampNtz::is_compact(precision) {
                        // Compact: only milliseconds
                        let (millis, next) = reader.read_long(cursor)?;
                        (
                            Datum::TimestampNtz(crate::row::datum::TimestampNtz::new(millis)),
                            next,
                        )
                    } else {
                        // Non-compact: milliseconds + nanos
                        let (millis, mid) = reader.read_long(cursor)?;
                        let (nanos, next) = reader.read_int(mid)?;
                        let timestamp = crate::row::datum::TimestampNtz::from_millis_nanos(
                            millis, nanos,
                        )
                        .map_err(|e| IllegalArgument {
                            message: format!(
                                "Invalid nano_of_millisecond value in compacted row timestamp: {e}"
                            ),
                        })?;
                        (Datum::TimestampNtz(timestamp), next)
                    }
                }
                DataType::TimestampLTz(timestamp_ltz_type) => {
                    let precision = timestamp_ltz_type.precision();
                    if crate::row::datum::TimestampLtz::is_compact(precision) {
                        // Compact: only epoch milliseconds
                        let (epoch_millis, next) = reader.read_long(cursor)?;
                        (
                            Datum::TimestampLtz(crate::row::datum::TimestampLtz::new(epoch_millis)),
                            next,
                        )
                    } else {
                        // Non-compact: epoch milliseconds + nanos
                        let (epoch_millis, mid) = reader.read_long(cursor)?;
                        let (nanos, next) = reader.read_int(mid)?;
                        let timestamp_ltz = crate::row::datum::TimestampLtz::from_millis_nanos(
                            epoch_millis,
                            nanos,
                        )
                        .map_err(|e| IllegalArgument {
                            message: format!(
                                "Invalid nano_of_millisecond value in compacted row timestamp_ltz: {e}"
                            ),
                        })?;
                        (Datum::TimestampLtz(timestamp_ltz), next)
                    }
                }
                DataType::Array(_) => {
                    let (bytes, next) = reader.read_bytes(cursor)?;
                    let array = crate::row::binary_array::FlussArray::from_bytes(bytes)?;
                    (Datum::Array(array), next)
                }
                _ => {
                    return Err(IllegalArgument {
                        message: format!(
                            "Unsupported DataType in CompactedRowDeserializer: {dtype:?}"
                        ),
                    });
                }
            };
            cursor = next_cursor;
            row.set_field(col_pos, datum);
        }
        Ok(row)
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

    fn checked_pos(&self, pos: usize, width: usize, context: &str) -> Result<usize> {
        let next = pos.checked_add(width).ok_or_else(|| IllegalArgument {
            message: format!("Overflow while reading {context}: pos={pos}, width={width}"),
        })?;
        if next > self.limit {
            return Err(IllegalArgument {
                message: format!(
                    "Out-of-bounds while reading {context}: pos={pos}, width={width}, limit={}",
                    self.limit
                ),
            });
        }
        Ok(next)
    }

    pub fn is_null_at(&self, col_pos: usize) -> bool {
        let byte_index = col_pos >> 3;
        let bit = col_pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    pub fn read_boolean(&self, pos: usize) -> Result<(bool, usize)> {
        let (val, next) = self.read_byte(pos)?;
        Ok((val != 0, next))
    }

    pub fn read_byte(&self, pos: usize) -> Result<(u8, usize)> {
        let next = self.checked_pos(pos, 1, "byte")?;
        Ok((self.segment[pos], next))
    }

    pub fn read_short(&self, pos: usize) -> Result<(i16, usize)> {
        let next_pos = self.checked_pos(pos, 2, "short")?;
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((i16::from_ne_bytes(arr), next_pos))
    }

    pub fn read_int(&self, pos: usize) -> Result<(i32, usize)> {
        match read_unsigned_varint_at(self.segment, pos, CompactedRowWriter::MAX_INT_SIZE) {
            Ok((value, next_pos)) => Ok((value as i32, next_pos)),
            Err(e) => Err(IllegalArgument {
                message: format!("Invalid VarInt32 input stream at pos {pos}: {e}"),
            }),
        }
    }

    pub fn read_long(&self, pos: usize) -> Result<(i64, usize)> {
        match read_unsigned_varint_u64_at(self.segment, pos, CompactedRowWriter::MAX_LONG_SIZE) {
            Ok((value, next_pos)) => Ok((value as i64, next_pos)),
            Err(e) => Err(IllegalArgument {
                message: format!("Invalid VarInt64 input stream at pos {pos}: {e}"),
            }),
        }
    }

    pub fn read_float(&self, pos: usize) -> Result<(f32, usize)> {
        let next_pos = self.checked_pos(pos, 4, "float")?;
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((f32::from_ne_bytes(arr), next_pos))
    }

    pub fn read_double(&self, pos: usize) -> Result<(f64, usize)> {
        let next_pos = self.checked_pos(pos, 8, "double")?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((f64::from_ne_bytes(arr), next_pos))
    }

    pub fn read_binary(&self, pos: usize) -> Result<(&'a [u8], usize)> {
        self.read_bytes(pos)
    }

    pub fn read_bytes(&self, pos: usize) -> Result<(&'a [u8], usize)> {
        let (len, data_pos) = self.read_int(pos)?;
        let len = usize::try_from(len).map_err(|_| IllegalArgument {
            message: format!("Negative length while reading bytes at pos {pos}: {len}"),
        })?;
        let next_pos = self.checked_pos(data_pos, len, "bytes payload")?;
        Ok((&self.segment[data_pos..next_pos], next_pos))
    }

    pub fn read_string(&self, pos: usize) -> Result<(&'a str, usize)> {
        let (bytes, next_pos) = self.read_bytes(pos)?;
        let s = from_utf8(bytes).map_err(|e| IllegalArgument {
            message: format!("Invalid UTF-8 when reading string at pos {pos}: {e}"),
        })?;
        Ok((s, next_pos))
    }
}
