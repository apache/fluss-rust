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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::row::{GenericRow, InternalRow};
use crate::row::datum::{Date, Datum, Time, TimestampLtz, TimestampNtz};
use arrow::array::{Array, AsArray, BinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ColumnarRow {
    record_batch: Arc<RecordBatch>,
    row_id: usize,
    nested_rows: Vec<std::sync::OnceLock<GenericRow<'static>>>,
}

impl ColumnarRow {
    pub fn new(batch: Arc<RecordBatch>) -> Self {
        let num_cols = batch.num_columns();
        ColumnarRow {
            record_batch: batch,
            row_id: 0,
            nested_rows: (0..num_cols).map(|_| std::sync::OnceLock::new()).collect(),
        }
    }

    pub fn new_with_row_id(bach: Arc<RecordBatch>, row_id: usize) -> Self {
        let num_cols = bach.num_columns();
        ColumnarRow {
            record_batch: bach,
            row_id,
            nested_rows: (0..num_cols).map(|_| std::sync::OnceLock::new()).collect(),
        }
    }

    pub fn set_row_id(&mut self, row_id: usize) {
        self.row_id = row_id;
        for lock in &mut self.nested_rows {
            *lock = std::sync::OnceLock::new();
        }
    }

    pub fn get_row_id(&self) -> usize {
        self.row_id
    }

    pub fn get_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    fn column(&self, pos: usize) -> Result<&Arc<dyn Array>> {
        self.record_batch
            .columns()
            .get(pos)
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "column index {pos} out of bounds (batch has {} columns)",
                    self.record_batch.num_columns()
                ),
            })
    }

    /// Generic helper to read timestamp from Arrow, handling all TimeUnit conversions.
    /// Like Java, the precision parameter is ignored - conversion is determined by Arrow TimeUnit.
    fn read_timestamp_from_arrow<T>(
        &self,
        pos: usize,
        _precision: u32,
        construct_compact: impl FnOnce(i64) -> T,
        construct_with_nanos: impl FnOnce(i64, i32) -> Result<T>,
    ) -> Result<T> {
        let column = self.column(pos)?;

        // Read value and time unit based on the actual Arrow timestamp type
        let (value, time_unit) = match column.data_type() {
            ArrowDataType::Timestamp(TimeUnit::Second, _) => (
                column
                    .as_primitive_opt::<TimestampSecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampSecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Second,
            ),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => (
                column
                    .as_primitive_opt::<TimestampMillisecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampMillisecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Millisecond,
            ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => (
                column
                    .as_primitive_opt::<TimestampMicrosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampMicrosecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Microsecond,
            ),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => (
                column
                    .as_primitive_opt::<TimestampNanosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampNanosecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Nanosecond,
            ),
            other => {
                return Err(IllegalArgument {
                    message: format!("expected Timestamp column at position {pos}, got {other:?}"),
                });
            }
        };

        // Convert based on Arrow TimeUnit
        let (millis, nanos) = match time_unit {
            TimeUnit::Second => (value * 1000, 0),
            TimeUnit::Millisecond => (value, 0),
            TimeUnit::Microsecond => {
                // Use Euclidean division so that nanos is always non-negative,
                // even for timestamps before the Unix epoch.
                let millis = value.div_euclid(1000);
                let nanos = (value.rem_euclid(1000) * 1000) as i32;
                (millis, nanos)
            }
            TimeUnit::Nanosecond => {
                // Use Euclidean division so that nanos is always in [0, 999_999].
                let millis = value.div_euclid(1_000_000);
                let nanos = value.rem_euclid(1_000_000) as i32;
                (millis, nanos)
            }
        };

        if nanos == 0 {
            Ok(construct_compact(millis))
        } else {
            construct_with_nanos(millis, nanos)
        }
    }

    /// Read date value from Arrow Date32Array
    fn read_date_from_arrow(&self, pos: usize) -> Result<i32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Date32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected Date32Array at position {pos}"),
            })?
            .value(self.row_id))
    }

    /// Read time value from Arrow Time32/Time64 arrays, converting to milliseconds
    fn read_time_from_arrow(&self, pos: usize) -> Result<i32> {
        let column = self.column(pos)?;

        match column.data_type() {
            ArrowDataType::Time32(TimeUnit::Second) => {
                let value = column
                    .as_primitive_opt::<Time32SecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time32SecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok(value * 1000) // Convert seconds to milliseconds
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(column
                .as_primitive_opt::<Time32MillisecondType>()
                .ok_or_else(|| IllegalArgument {
                    message: format!("expected Time32MillisecondArray at position {pos}"),
                })?
                .value(self.row_id)),
            ArrowDataType::Time64(TimeUnit::Microsecond) => {
                let value = column
                    .as_primitive_opt::<Time64MicrosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time64MicrosecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok((value / 1000) as i32) // Convert microseconds to milliseconds
            }
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                let value = column
                    .as_primitive_opt::<Time64NanosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time64NanosecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok((value / 1_000_000) as i32) // Convert nanoseconds to milliseconds
            }
            other => Err(IllegalArgument {
                message: format!("expected Time column at position {pos}, got {other:?}"),
            }),
        }
    }

    /// Extract a `GenericRow<'static>` from a column in the RecordBatch at the given row_id.
    fn extract_struct_at(
        batch: &RecordBatch,
        pos: usize,
        row_id: usize,
    ) -> Result<GenericRow<'static>> {
        let col = batch.column(pos);
        Self::extract_struct_from_array(col.as_ref(), row_id)
    }

    /// Recursively extract a `GenericRow<'static>` from a `StructArray` at row_id.
    fn extract_struct_from_array(array: &dyn Array, row_id: usize) -> Result<GenericRow<'static>> {
        use arrow::array::StructArray;
        let sa = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected StructArray, got {:?}", array.data_type()),
            })?;
        let mut values = Vec::with_capacity(sa.num_columns());
        for i in 0..sa.num_columns() {
            let child = sa.column(i);
            values.push(Self::arrow_value_to_datum(child.as_ref(), row_id)?);
        }
        Ok(GenericRow { values })
    }

    /// Convert a single element at `row_id` in an Arrow array to a `Datum<'static>`.
    fn arrow_value_to_datum(array: &dyn Array, row_id: usize) -> Result<Datum<'static>> {
        use arrow::array::{
            BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int64Array, Time32MillisecondArray, Time32SecondArray,
            Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        use crate::row::Decimal;

        if array.is_null(row_id) {
            return Ok(Datum::Null);
        }

        match array.data_type() {
            ArrowDataType::Boolean => {
                let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Datum::Bool(a.value(row_id)))
            }
            ArrowDataType::Int8 => {
                let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(Datum::Int8(a.value(row_id)))
            }
            ArrowDataType::Int16 => {
                let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(Datum::Int16(a.value(row_id)))
            }
            ArrowDataType::Int32 => {
                let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(Datum::Int32(a.value(row_id)))
            }
            ArrowDataType::Int64 => {
                let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(Datum::Int64(a.value(row_id)))
            }
            ArrowDataType::Float32 => {
                let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(Datum::Float32(a.value(row_id).into()))
            }
            ArrowDataType::Float64 => {
                let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(Datum::Float64(a.value(row_id).into()))
            }
            ArrowDataType::Utf8 => {
                let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(Datum::String(std::borrow::Cow::Owned(a.value(row_id).to_owned())))
            }
            ArrowDataType::Binary => {
                let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(Datum::Blob(std::borrow::Cow::Owned(a.value(row_id).to_vec())))
            }
            ArrowDataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let i128_val = a.value(row_id);
                Ok(Datum::Decimal(Decimal::from_arrow_decimal128(
                    i128_val,
                    s as i64,
                    p as u32,
                    s as u32,
                )?))
            }
            ArrowDataType::Date32 => {
                let a = array.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
                Ok(Datum::Date(Date::new(a.value(row_id))))
            }
            ArrowDataType::Time32(TimeUnit::Second) => {
                let a = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
                Ok(Datum::Time(Time::new(a.value(row_id) * 1000)))
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => {
                let a = array.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
                Ok(Datum::Time(Time::new(a.value(row_id))))
            }
            ArrowDataType::Time64(TimeUnit::Microsecond) => {
                let a = array.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
                Ok(Datum::Time(Time::new((a.value(row_id) / 1000) as i32)))
            }
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                let a = array.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                Ok(Datum::Time(Time::new((a.value(row_id) / 1_000_000) as i32)))
            }
            ArrowDataType::Timestamp(time_unit, tz) => {
                let value: i64 = match time_unit {
                    TimeUnit::Second => {
                        array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(row_id)
                    }
                    TimeUnit::Millisecond => {
                        array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row_id)
                    }
                    TimeUnit::Microsecond => {
                        array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row_id)
                    }
                    TimeUnit::Nanosecond => {
                        array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row_id)
                    }
                };
                let (millis, nanos) = match time_unit {
                    TimeUnit::Second => (value * 1000, 0i32),
                    TimeUnit::Millisecond => (value, 0i32),
                    TimeUnit::Microsecond => {
                        let millis = value.div_euclid(1000);
                        let nanos = (value.rem_euclid(1000) * 1000) as i32;
                        (millis, nanos)
                    }
                    TimeUnit::Nanosecond => {
                        let millis = value.div_euclid(1_000_000);
                        let nanos = value.rem_euclid(1_000_000) as i32;
                        (millis, nanos)
                    }
                };
                if tz.is_some() {
                    if nanos == 0 {
                        Ok(Datum::TimestampLtz(TimestampLtz::new(millis)))
                    } else {
                        Ok(Datum::TimestampLtz(TimestampLtz::from_millis_nanos(millis, nanos)?))
                    }
                } else if nanos == 0 {
                    Ok(Datum::TimestampNtz(TimestampNtz::new(millis)))
                } else {
                    Ok(Datum::TimestampNtz(TimestampNtz::from_millis_nanos(millis, nanos)?))
                }
            }
            ArrowDataType::Struct(_) => {
                let nested = Self::extract_struct_from_array(array, row_id)?;
                Ok(Datum::Row(Box::new(nested)))
            }
            other => Err(IllegalArgument {
                message: format!(
                    "unsupported Arrow data type for nested row extraction: {other:?}"
                ),
            }),
        }
    }
}

impl InternalRow for ColumnarRow {
    fn get_field_count(&self) -> usize {
        self.record_batch.num_columns()
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        Ok(self.column(pos)?.is_null(self.row_id))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        Ok(self
            .column(pos)?
            .as_boolean_opt()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected boolean array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int8Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected byte array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int16Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected short array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected int array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected long array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Float32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected float32 array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected float64 array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected String array for char type at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected String array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_decimal(
        &self,
        pos: usize,
        precision: usize,
        scale: usize,
    ) -> Result<crate::row::Decimal> {
        use arrow::datatypes::DataType;

        let column = self.column(pos)?;
        let array = column
            .as_primitive_opt::<Decimal128Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "expected Decimal128Array at column {pos}, found: {:?}",
                    column.data_type()
                ),
            })?;

        // Contract: caller must check is_null_at() before calling get_decimal.
        debug_assert!(
            !array.is_null(self.row_id),
            "get_decimal called on null value at pos {} row {}",
            pos,
            self.row_id
        );

        // Read scale from Arrow column data type
        let arrow_scale = match column.data_type() {
            DataType::Decimal128(_p, s) => *s as i64,
            dt => {
                return Err(IllegalArgument {
                    message: format!(
                        "expected Decimal128 data type at column {pos}, found: {dt:?}"
                    ),
                });
            }
        };

        let i128_val = array.value(self.row_id);

        // Convert Arrow Decimal128 to Fluss Decimal (handles rescaling and validation)
        crate::row::Decimal::from_arrow_decimal128(
            i128_val,
            arrow_scale,
            precision as u32,
            scale as u32,
        )
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        Ok(Date::new(self.read_date_from_arrow(pos)?))
    }

    fn get_time(&self, pos: usize) -> Result<Time> {
        Ok(Time::new(self.read_time_from_arrow(pos)?))
    }

    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        self.read_timestamp_from_arrow(
            pos,
            precision,
            TimestampNtz::new,
            TimestampNtz::from_millis_nanos,
        )
    }

    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        self.read_timestamp_from_arrow(
            pos,
            precision,
            TimestampLtz::new,
            TimestampLtz::from_millis_nanos,
        )
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        Ok(self
            .column(pos)?
            .as_fixed_size_binary_opt()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected binary array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected bytes array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_row(&self, pos: usize) -> Result<&GenericRow<'_>> {
        let lock = self.nested_rows.get(pos).ok_or_else(|| IllegalArgument {
            message: format!("column index {pos} out of bounds for get_row"),
        })?;
        let batch = Arc::clone(&self.record_batch);
        let row_id = self.row_id;
        Ok(lock.get_or_init(|| {
            Self::extract_struct_at(&batch, pos, row_id)
                .expect("failed to extract nested row from StructArray")
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, StringArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};

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
            Field::new("char", DataType::Utf8, false),
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
                Arc::new(StringArray::from(vec!["ab"])),
            ],
        )
        .expect("record batch");

        let mut row = ColumnarRow::new(Arc::new(batch));
        assert_eq!(row.get_field_count(), 10);
        assert!(row.get_boolean(0).unwrap());
        assert_eq!(row.get_byte(1).unwrap(), 1);
        assert_eq!(row.get_short(2).unwrap(), 2);
        assert_eq!(row.get_int(3).unwrap(), 3);
        assert_eq!(row.get_long(4).unwrap(), 4);
        assert_eq!(row.get_float(5).unwrap(), 1.25);
        assert_eq!(row.get_double(6).unwrap(), 2.5);
        assert_eq!(row.get_string(7).unwrap(), "hello");
        assert_eq!(row.get_bytes(8).unwrap(), b"data");
        assert_eq!(row.get_char(9, 2).unwrap(), "ab");
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
            row.get_decimal(0, 10, 2).unwrap(),
            crate::row::Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12345), 2), 10, 2)
                .unwrap()
        );
        assert_eq!(
            row.get_decimal(1, 20, 5).unwrap(),
            crate::row::Decimal::from_big_decimal(
                BigDecimal::new(BigInt::from(1234567890), 5),
                20,
                5
            )
            .unwrap()
        );
        assert_eq!(
            row.get_decimal(2, 38, 10).unwrap(),
            crate::row::Decimal::from_big_decimal(
                BigDecimal::new(BigInt::from(999999999999999999i128), 10),
                38,
                10
            )
            .unwrap()
        );
    }

    fn make_struct_batch(
        field_name: &str,
        child_fields: Fields,
        child_arrays: Vec<Arc<dyn Array>>,
        _num_rows: usize,
    ) -> Arc<RecordBatch> {
        let struct_array = StructArray::new(child_fields.clone(), child_arrays, None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Struct(child_fields),
            false,
        )]));
        Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(struct_array)])
                .expect("record batch"),
        )
    }

    #[test]
    fn columnar_row_reads_nested_row() {
        // Build a RecordBatch with a Struct column: {i32, string}
        let child_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]);
        let child_arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int32Array::from(vec![42, 99])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ];
        let batch = make_struct_batch("nested", child_fields, child_arrays, 2);

        let mut row = ColumnarRow::new(batch);

        // row_id = 0
        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_field_count(), 2);
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");

        // row_id = 1
        row.set_row_id(1);
        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 99);
        assert_eq!(nested.get_string(1).unwrap(), "world");
    }

    #[test]
    fn columnar_row_reads_deeply_nested_row() {
        // Build: outer struct { i32, inner struct { string } }
        let inner_fields = Fields::from(vec![Field::new("s", DataType::Utf8, false)]);
        let inner_array = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["deep", "deeper"])) as Arc<dyn Array>],
            None,
        ));

        let outer_fields = Fields::from(vec![
            Field::new("n", DataType::Int32, false),
            Field::new("inner", DataType::Struct(inner_fields), false),
        ]);
        let outer_array = Arc::new(StructArray::new(
            outer_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>,
                inner_array as Arc<dyn Array>,
            ],
            None,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(outer_fields),
            false,
        )]));
        let batch = Arc::new(
            RecordBatch::try_new(schema, vec![outer_array]).expect("record batch"),
        );

        let row = ColumnarRow::new(batch);

        // Access outer struct at column 0, row 0
        let outer = row.get_row(0).unwrap();
        assert_eq!(outer.get_int(0).unwrap(), 1);

        // Access inner struct (column 1 of outer)
        let inner = outer.get_row(1).unwrap();
        assert_eq!(inner.get_string(0).unwrap(), "deep");
    }

    #[test]
    fn columnar_row_get_row_cache_invalidated_on_set_row_id() {
        let child_fields = Fields::from(vec![Field::new("x", DataType::Int32, false)]);
        let child_arrays: Vec<Arc<dyn Array>> =
            vec![Arc::new(Int32Array::from(vec![10, 20]))];
        let batch = make_struct_batch("s", child_fields, child_arrays, 2);

        let mut row = ColumnarRow::new(batch);

        // row_id = 0: nested x = 10
        let nested_0 = row.get_row(0).unwrap();
        assert_eq!(nested_0.get_int(0).unwrap(), 10);

        // After set_row_id(1), cache is cleared → nested x = 20
        row.set_row_id(1);
        let nested_1 = row.get_row(0).unwrap();
        assert_eq!(nested_1.get_int(0).unwrap(), 20);
    }
}
