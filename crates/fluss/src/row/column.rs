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
use crate::row::InternalRow;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
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

    fn get_array(&self, pos: usize) -> Result<crate::row::FlussArray> {
        use crate::record::from_arrow_type;
        use crate::row::binary_array::FlussArrayWriter;
        use arrow::array::ListArray;

        let column = self.column(pos)?;
        let list_array =
            column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| IllegalArgument {
                    message: format!("expected List array at position {pos}"),
                })?;

        let values = list_array.value(self.row_id);
        let num_elements = values.len();
        let element_arrow_type = values.data_type();
        let element_fluss_type = from_arrow_type(element_arrow_type)?;

        let mut writer = FlussArrayWriter::new(num_elements, &element_fluss_type);
        let element_row = ColumnarRow::new(std::sync::Arc::new(
            arrow::array::RecordBatch::try_from_iter(vec![("v", values)]).map_err(|e| {
                IllegalArgument {
                    message: format!("Failed to create RecordBatch from list values: {e}"),
                }
            })?,
        ));

        for i in 0..num_elements {
            let mut row = element_row.clone();
            row.set_row_id(i);
            if row.is_null_at(0)? {
                writer.set_null_at(i);
            } else {
                write_arrow_value_to_fluss_array(&row, 0, &element_fluss_type, &mut writer, i)?;
            }
        }

        writer.complete()
    }
}

fn write_arrow_value_to_fluss_array(
    row: &ColumnarRow,
    col: usize,
    element_type: &crate::metadata::DataType,
    writer: &mut crate::row::binary_array::FlussArrayWriter,
    pos: usize,
) -> Result<()> {
    use crate::metadata::DataType;

    match element_type {
        DataType::Boolean(_) => writer.write_boolean(pos, row.get_boolean(col)?),
        DataType::TinyInt(_) => writer.write_byte(pos, row.get_byte(col)?),
        DataType::SmallInt(_) => writer.write_short(pos, row.get_short(col)?),
        DataType::Int(_) => writer.write_int(pos, row.get_int(col)?),
        DataType::BigInt(_) => writer.write_long(pos, row.get_long(col)?),
        DataType::Float(_) => writer.write_float(pos, row.get_float(col)?),
        DataType::Double(_) => writer.write_double(pos, row.get_double(col)?),
        DataType::Char(t) => writer.write_string(pos, row.get_char(col, t.length() as usize)?),
        DataType::String(_) => writer.write_string(pos, row.get_string(col)?),
        DataType::Binary(t) => writer.write_binary_bytes(pos, row.get_binary(col, t.length())?),
        DataType::Bytes(_) => writer.write_binary_bytes(pos, row.get_bytes(col)?),
        DataType::Decimal(dt) => {
            let d = row.get_decimal(col, dt.precision() as usize, dt.scale() as usize)?;
            writer.write_decimal(pos, &d, dt.precision());
        }
        DataType::Date(_) => writer.write_date(pos, row.get_date(col)?),
        DataType::Time(_) => writer.write_time(pos, row.get_time(col)?),
        DataType::Timestamp(t) => {
            let ts = row.get_timestamp_ntz(col, t.precision())?;
            writer.write_timestamp_ntz(pos, &ts, t.precision());
        }
        DataType::TimestampLTz(t) => {
            let ts = row.get_timestamp_ltz(col, t.precision())?;
            writer.write_timestamp_ltz(pos, &ts, t.precision());
        }
        DataType::Array(_) => {
            let nested = row.get_array(col)?;
            writer.write_array(pos, &nested);
        }
        _ => {
            return Err(IllegalArgument {
                message: format!(
                    "Unsupported element type for Arrow → FlussArray conversion: {element_type:?}"
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array, ListBuilder, StringArray,
        UInt32Builder,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    fn single_column_row(array: ArrayRef) -> ColumnarRow {
        let batch =
            RecordBatch::try_from_iter(vec![("arr", array)]).expect("record batch with one column");
        ColumnarRow::new(Arc::new(batch))
    }

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

    #[test]
    fn columnar_row_get_array_int_roundtrip() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 3);
        assert_eq!(arr.get_int(0), 1);
        assert_eq!(arr.get_int(1), 2);
        assert_eq!(arr.get_int(2), 3);
    }

    #[test]
    fn columnar_row_get_array_with_nulls() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_null();
        builder.values().append_value(3);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 3);
        assert_eq!(arr.get_int(0), 1);
        assert!(arr.is_null_at(1));
        assert_eq!(arr.get_int(2), 3);
    }

    #[test]
    fn columnar_row_get_array_nested_array() {
        let mut outer = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        // first nested array: [1, 2]
        outer.values().values().append_value(1);
        outer.values().values().append_value(2);
        outer.values().append(true);

        // second nested array: [99]
        outer.values().values().append_value(99);
        outer.values().append(true);

        // one row containing two nested arrays
        outer.append(true);
        let array = Arc::new(outer.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 2);

        let nested0 = arr.get_array(0).unwrap();
        assert_eq!(nested0.size(), 2);
        assert_eq!(nested0.get_int(0), 1);
        assert_eq!(nested0.get_int(1), 2);

        let nested1 = arr.get_array(1).unwrap();
        assert_eq!(nested1.size(), 1);
        assert_eq!(nested1.get_int(0), 99);
    }

    #[test]
    fn columnar_row_get_array_non_list_column_returns_error() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let row = single_column_row(array);
        let err = row.get_array(0).unwrap_err();
        assert!(
            err.to_string().contains("expected List array"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn columnar_row_get_array_unsupported_element_type_returns_error() {
        let mut builder = ListBuilder::new(UInt32Builder::new());
        builder.values().append_value(7);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let row = single_column_row(array);
        let err = row.get_array(0).unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot convert Arrow type to Fluss type"),
            "unexpected error: {err}"
        );
    }
}
