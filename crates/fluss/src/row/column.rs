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
    AsArray, BinaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
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

    fn get_char(&self, pos: usize, _length: usize) -> &str {
        let array = self
            .record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected fixed-size binary array for char type");

        let bytes = array.value(self.row_id);
        // don't check length, following java client
        std::str::from_utf8(bytes).expect("Invalid UTF-8 in char field")
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
        assert_eq!(row.get_boolean(0), true);
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
}
