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

use crate::row::InternalRow;
use arrow::array::{
    Array, AsArray, BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use std::sync::Arc;

#[derive(Clone)]
pub struct ColumnarRow {
    record_batch: Arc<RecordBatch>,
    row_id: usize,
}

impl ColumnarRow {
    pub fn new(batch: Arc<RecordBatch>) -> Self {
        ColumnarRow {
            record_batch: batch,
            row_id: 0,
        }
    }

    pub fn new_with_row_id(bach: Arc<RecordBatch>, row_id: usize) -> Self {
        ColumnarRow {
            record_batch: bach,
            row_id,
        }
    }

    pub fn set_row_id(&mut self, row_id: usize) {
        self.row_id = row_id
    }

    pub fn get_row_id(&self) -> usize {
        self.row_id
    }

    pub fn get_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    /// Generic helper to read timestamp from Arrow, handling all TimeUnit conversions.
    /// Like Java, the precision parameter is ignored - conversion is determined by Arrow TimeUnit.
    fn read_timestamp_from_arrow<T>(
        &self,
        pos: usize,
        _precision: u32,
        construct_compact: impl FnOnce(i64) -> T,
        construct_with_nanos: impl FnOnce(i64, i32) -> crate::error::Result<T>,
    ) -> T {
        let schema = self.record_batch.schema();
        let arrow_field = schema.field(pos);
        let column = self.record_batch.column(pos);

        // Read value based on the actual Arrow timestamp type
        let value = match arrow_field.data_type() {
            ArrowDataType::Timestamp(TimeUnit::Second, _) => column
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("Expected TimestampSecondArray")
                .value(self.row_id),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("Expected TimestampMillisecondArray")
                .value(self.row_id),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("Expected TimestampMicrosecondArray")
                .value(self.row_id),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Expected TimestampNanosecondArray")
                .value(self.row_id),
            other => panic!("Expected Timestamp column at position {pos}, got {other:?}"),
        };

        // Convert based on Arrow TimeUnit
        let (millis, nanos) = match arrow_field.data_type() {
            ArrowDataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => (value * 1000, 0),
                TimeUnit::Millisecond => (value, 0),
                TimeUnit::Microsecond => {
                    let millis = value / 1000;
                    let nanos = ((value % 1000) * 1000) as i32;
                    (millis, nanos)
                }
                TimeUnit::Nanosecond => {
                    let millis = value / 1_000_000;
                    let nanos = (value % 1_000_000) as i32;
                    (millis, nanos)
                }
            },
            _ => unreachable!(),
        };

        if nanos == 0 {
            construct_compact(millis)
        } else {
            // nanos is guaranteed to be in valid range [0, 999_999] by arithmetic
            construct_with_nanos(millis, nanos).expect("nanos in valid range by construction")
        }
    }

    /// Read date value from Arrow Date32Array
    fn read_date_from_arrow(&self, pos: usize) -> i32 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Expected Date32Array")
            .value(self.row_id)
    }

    /// Read time value from Arrow Time32/Time64 arrays, converting to milliseconds
    fn read_time_from_arrow(&self, pos: usize) -> i32 {
        let schema = self.record_batch.schema();
        let arrow_field = schema.field(pos);
        let column = self.record_batch.column(pos);

        match arrow_field.data_type() {
            ArrowDataType::Time32(TimeUnit::Second) => {
                let value = column
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .expect("Expected Time32SecondArray")
                    .value(self.row_id);
                value * 1000 // Convert seconds to milliseconds
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => column
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .expect("Expected Time32MillisecondArray")
                .value(self.row_id),
            ArrowDataType::Time64(TimeUnit::Microsecond) => {
                let value = column
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .expect("Expected Time64MicrosecondArray")
                    .value(self.row_id);
                (value / 1000) as i32 // Convert microseconds to milliseconds
            }
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                let value = column
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .expect("Expected Time64NanosecondArray")
                    .value(self.row_id);
                (value / 1_000_000) as i32 // Convert nanoseconds to milliseconds
            }
            other => panic!("Expected Time column at position {pos}, got {other:?}"),
        }
    }
}

impl InternalRow for ColumnarRow {
    fn get_field_count(&self) -> usize {
        self.record_batch.num_columns()
    }

    fn is_null_at(&self, pos: usize) -> bool {
        self.record_batch.column(pos).is_null(self.row_id)
    }

    fn get_boolean(&self, pos: usize) -> bool {
        self.record_batch
            .column(pos)
            .as_boolean()
            .value(self.row_id)
    }

    fn get_byte(&self, pos: usize) -> i8 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Expect byte array")
            .value(self.row_id)
    }

    fn get_short(&self, pos: usize) -> i16 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Expect short array")
            .value(self.row_id)
    }

    fn get_int(&self, pos: usize) -> i32 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expect int array")
            .value(self.row_id)
    }

    fn get_long(&self, pos: usize) -> i64 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expect long array")
            .value(self.row_id)
    }

    fn get_float(&self, pos: usize) -> f32 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Expect float32 array")
            .value(self.row_id)
    }

    fn get_double(&self, pos: usize) -> f64 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Expect float64 array")
            .value(self.row_id)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> crate::row::Decimal {
        use arrow::datatypes::DataType;

        let column = self.record_batch.column(pos);
        let array = column
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap_or_else(|| {
                panic!(
                    "Expected Decimal128Array at column {}, found: {:?}",
                    pos,
                    column.data_type()
                )
            });

        // Contract: caller must check is_null_at() before calling get_decimal.
        // Calling on null value violates the contract and returns garbage data
        debug_assert!(
            !array.is_null(self.row_id),
            "get_decimal called on null value at pos {} row {}",
            pos,
            self.row_id
        );

        // Read scale from Arrow schema field metadata
        let schema = self.record_batch.schema();
        let field = schema.field(pos);
        let arrow_scale = match field.data_type() {
            DataType::Decimal128(_p, s) => *s as i64,
            dt => panic!("Expected Decimal128 data type at column {pos}, found: {dt:?}"),
        };

        let i128_val = array.value(self.row_id);

        // Convert Arrow Decimal128 to Fluss Decimal (handles rescaling and validation)
        crate::row::Decimal::from_arrow_decimal128(
            i128_val,
            arrow_scale,
            precision as u32,
            scale as u32,
        )
        .unwrap_or_else(|e| {
            panic!(
                "Failed to create Decimal at column {} row {}: {}",
                pos, self.row_id, e
            )
        })
    }

    fn get_date(&self, pos: usize) -> crate::row::datum::Date {
        crate::row::datum::Date::new(self.read_date_from_arrow(pos))
    }

    fn get_time(&self, pos: usize) -> crate::row::datum::Time {
        crate::row::datum::Time::new(self.read_time_from_arrow(pos))
    }

    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> crate::row::datum::TimestampNtz {
        // Like Java's ArrowTimestampNtzColumnVector, we ignore the precision parameter
        // and determine the conversion from the Arrow column's TimeUnit.
        self.read_timestamp_from_arrow(
            pos,
            precision,
            crate::row::datum::TimestampNtz::new,
            crate::row::datum::TimestampNtz::from_millis_nanos,
        )
    }

    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> crate::row::datum::TimestampLtz {
        // Like Java's ArrowTimestampLtzColumnVector, we ignore the precision parameter
        // and determine the conversion from the Arrow column's TimeUnit.
        self.read_timestamp_from_arrow(
            pos,
            precision,
            crate::row::datum::TimestampLtz::new,
            crate::row::datum::TimestampLtz::from_millis_nanos,
        )
    }

    fn get_char(&self, pos: usize, _length: usize) -> &str {
        let column = self.record_batch.column(pos);
        let schema = self.record_batch.schema();
        let arrow_field = schema.field(pos);

        match arrow_field.data_type() {
            ArrowDataType::FixedSizeBinary(_) => {
                // KV table format: char stored as FixedSizeBinary
                let array = column
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("Expected fixed-size binary array for char type");
                let bytes = array.value(self.row_id);
                // don't check length, following java client
                std::str::from_utf8(bytes).expect("Invalid UTF-8 in char field")
            }
            ArrowDataType::Utf8 => {
                // Log table format: char stored as Utf8
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected String array for char type")
                    .value(self.row_id)
            }
            other => panic!(
                "Expected FixedSizeBinary or Utf8 for char type at position {pos}, got {other:?}"
            ),
        }
    }

    fn get_string(&self, pos: usize) -> &str {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected String array.")
            .value(self.row_id)
    }

    fn get_binary(&self, pos: usize, _length: usize) -> &[u8] {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected binary array.")
            .value(self.row_id)
    }

    fn get_bytes(&self, pos: usize) -> &[u8] {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Expected bytes array.")
            .value(self.row_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn columnar_row_reads_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("bin", DataType::Binary, false),
            Field::new("char", DataType::FixedSizeBinary(2), false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int8Array::from(vec![1])),
                Arc::new(Int16Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int64Array::from(vec![4])),
                Arc::new(Float32Array::from(vec![1.25])),
                Arc::new(Float64Array::from(vec![2.5])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(BinaryArray::from(vec![b"data".as_slice()])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![Some(b"ab".as_slice())].into_iter(),
                        2,
                    )
                    .expect("fixed array"),
                ),
            ],
        )
        .expect("record batch");

        let mut row = ColumnarRow::new(Arc::new(batch));
        assert_eq!(row.get_field_count(), 10);
        assert!(row.get_boolean(0));
        assert_eq!(row.get_byte(1), 1);
        assert_eq!(row.get_short(2), 2);
        assert_eq!(row.get_int(3), 3);
        assert_eq!(row.get_long(4), 4);
        assert_eq!(row.get_float(5), 1.25);
        assert_eq!(row.get_double(6), 2.5);
        assert_eq!(row.get_string(7), "hello");
        assert_eq!(row.get_bytes(8), b"data");
        assert_eq!(row.get_char(9, 2), "ab");
        row.set_row_id(0);
        assert_eq!(row.get_row_id(), 0);
    }

    #[test]
    fn columnar_row_reads_decimal() {
        use arrow::datatypes::DataType;
        use bigdecimal::{BigDecimal, num_bigint::BigInt};

        // Test with Decimal128
        let schema = Arc::new(Schema::new(vec![
            Field::new("dec1", DataType::Decimal128(10, 2), false),
            Field::new("dec2", DataType::Decimal128(20, 5), false),
            Field::new("dec3", DataType::Decimal128(38, 10), false),
        ]));

        // Create decimal values: 123.45, 12345.67890, large decimal
        let dec1_val = 12345i128; // 123.45 with scale 2
        let dec2_val = 1234567890i128; // 12345.67890 with scale 5
        let dec3_val = 999999999999999999i128; // Large value (18 nines) with scale 10

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal128Array::from(vec![dec1_val])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![dec2_val])
                        .with_precision_and_scale(20, 5)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![dec3_val])
                        .with_precision_and_scale(38, 10)
                        .unwrap(),
                ),
            ],
        )
        .expect("record batch");

        let row = ColumnarRow::new(Arc::new(batch));
        assert_eq!(row.get_field_count(), 3);

        // Verify decimal values
        assert_eq!(
            row.get_decimal(0, 10, 2),
            crate::row::Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12345), 2), 10, 2)
                .unwrap()
        );
        assert_eq!(
            row.get_decimal(1, 20, 5),
            crate::row::Decimal::from_big_decimal(
                BigDecimal::new(BigInt::from(1234567890), 5),
                20,
                5
            )
            .unwrap()
        );
        assert_eq!(
            row.get_decimal(2, 38, 10),
            crate::row::Decimal::from_big_decimal(
                BigDecimal::new(BigInt::from(999999999999999999i128), 10),
                38,
                10
            )
            .unwrap()
        );
    }
}
