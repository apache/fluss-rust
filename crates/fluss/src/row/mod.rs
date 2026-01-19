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

mod column;

mod datum;
mod decimal;

pub mod binary;
pub mod compacted;
pub mod encode;
pub mod field_getter;
mod row_decoder;

pub use column::*;
pub use compacted::CompactedRow;
pub use datum::*;
pub use decimal::{Decimal, MAX_COMPACT_PRECISION};
pub use encode::KeyEncoder;
pub use row_decoder::{CompactedRowDecoder, RowDecoder, RowDecoderFactory};

pub trait BinaryRow: InternalRow {
    /// Returns the binary representation of this row as a byte slice.
    fn as_bytes(&self) -> &[u8];
}

// TODO make functions return Result<?> for better error handling
pub trait InternalRow {
    /// Returns the number of fields in this row
    fn get_field_count(&self) -> usize;

    /// Returns true if the element is null at the given position
    fn is_null_at(&self, pos: usize) -> bool;

    /// Returns the boolean value at the given position
    fn get_boolean(&self, pos: usize) -> bool;

    /// Returns the byte value at the given position
    fn get_byte(&self, pos: usize) -> i8;

    /// Returns the short value at the given position
    fn get_short(&self, pos: usize) -> i16;

    /// Returns the integer value at the given position
    fn get_int(&self, pos: usize) -> i32;

    /// Returns the long value at the given position
    fn get_long(&self, pos: usize) -> i64;

    /// Returns the float value at the given position
    fn get_float(&self, pos: usize) -> f32;

    /// Returns the double value at the given position
    fn get_double(&self, pos: usize) -> f64;

    /// Returns the string value at the given position with fixed length
    fn get_char(&self, pos: usize, length: usize) -> &str;

    /// Returns the string value at the given position
    fn get_string(&self, pos: usize) -> &str;

    /// Returns the decimal value at the given position
    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Decimal;

    /// Returns the date value at the given position (date as days since epoch)
    fn get_date(&self, pos: usize) -> datum::Date;

    /// Returns the time value at the given position (time as milliseconds since midnight)
    fn get_time(&self, pos: usize) -> datum::Time;

    /// Returns the timestamp value at the given position (timestamp without timezone)
    ///
    /// The precision is required to determine whether the timestamp value was stored
    /// in a compact representation (precision <= 3) or with nanosecond precision.
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> datum::TimestampNtz;

    /// Returns the timestamp value at the given position (timestamp with local timezone)
    ///
    /// The precision is required to determine whether the timestamp value was stored
    /// in a compact representation (precision <= 3) or with nanosecond precision.
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> datum::TimestampLtz;

    /// Returns the binary value at the given position with fixed length
    fn get_binary(&self, pos: usize, length: usize) -> &[u8];

    /// Returns the binary value at the given position
    fn get_bytes(&self, pos: usize) -> &[u8];
}

pub struct GenericRow<'a> {
    pub values: Vec<Datum<'a>>,
}

impl<'a> InternalRow for GenericRow<'a> {
    fn get_field_count(&self) -> usize {
        self.values.len()
    }

    fn is_null_at(&self, pos: usize) -> bool {
        self.values
            .get(pos)
            .expect("position out of bounds")
            .is_null()
    }

    fn get_boolean(&self, pos: usize) -> bool {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_byte(&self, pos: usize) -> i8 {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_short(&self, pos: usize) -> i16 {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_int(&self, pos: usize) -> i32 {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_long(&self, _pos: usize) -> i64 {
        self.values.get(_pos).unwrap().try_into().unwrap()
    }

    fn get_decimal(&self, pos: usize, _precision: usize, _scale: usize) -> Decimal {
        match self.values.get(pos).unwrap() {
            Datum::Decimal(d) => d.clone(),
            other => panic!("Expected Decimal at pos {pos:?}, got {other:?}"),
        }
    }

    fn get_date(&self, pos: usize) -> datum::Date {
        match self.values.get(pos).unwrap() {
            Datum::Date(d) => *d,
            Datum::Int32(i) => datum::Date::new(*i),
            other => panic!("Expected Date or Int32 at pos {pos:?}, got {other:?}"),
        }
    }

    fn get_time(&self, pos: usize) -> datum::Time {
        match self.values.get(pos).unwrap() {
            Datum::Time(t) => *t,
            Datum::Int32(i) => datum::Time::new(*i),
            other => panic!("Expected Time or Int32 at pos {pos:?}, got {other:?}"),
        }
    }

    fn get_timestamp_ntz(&self, pos: usize, _precision: u32) -> datum::TimestampNtz {
        match self.values.get(pos).unwrap() {
            Datum::TimestampNtz(t) => *t,
            other => panic!("Expected TimestampNtz at pos {pos:?}, got {other:?}"),
        }
    }

    fn get_timestamp_ltz(&self, pos: usize, _precision: u32) -> datum::TimestampLtz {
        match self.values.get(pos).unwrap() {
            Datum::TimestampLtz(t) => *t,
            other => panic!("Expected TimestampLtz at pos {pos:?}, got {other:?}"),
        }
    }

    fn get_float(&self, pos: usize) -> f32 {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_double(&self, pos: usize) -> f64 {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_char(&self, pos: usize, _length: usize) -> &str {
        // don't check length, following java client
        self.get_string(pos)
    }

    fn get_string(&self, pos: usize) -> &str {
        self.values.get(pos).unwrap().try_into().unwrap()
    }

    fn get_binary(&self, pos: usize, _length: usize) -> &[u8] {
        self.values.get(pos).unwrap().as_blob()
    }

    fn get_bytes(&self, pos: usize) -> &[u8] {
        self.values.get(pos).unwrap().as_blob()
    }
}

impl<'a> Default for GenericRow<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> GenericRow<'a> {
    pub fn from_data(data: Vec<impl Into<Datum<'a>>>) -> GenericRow<'a> {
        GenericRow {
            values: data.into_iter().map(Into::into).collect(),
        }
    }
    pub fn new() -> GenericRow<'a> {
        GenericRow { values: vec![] }
    }

    pub fn set_field(&mut self, pos: usize, value: impl Into<Datum<'a>>) {
        self.values.insert(pos, value.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_null_at_checks_datum_nullity() {
        let mut row = GenericRow::new();
        row.set_field(0, Datum::Null);
        row.set_field(1, 42_i32);

        assert!(row.is_null_at(0));
        assert!(!row.is_null_at(1));
    }
}
