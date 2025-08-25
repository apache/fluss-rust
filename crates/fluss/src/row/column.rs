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

    fn get_char(&self, pos: usize, length: usize) -> String {
        let array = self
            .record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected fixed-size binary array for char type");

        let bytes = array.value(self.row_id);
        if bytes.len() != length {
            panic!(
                "Length mismatch for fixed-size char: expected {}, got {}",
                length,
                bytes.len()
            );
        }

        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
    }

    fn get_string(&self, pos: usize) -> &str {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected String array.")
            .value(self.row_id)
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Vec<u8> {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected binary array.")
            .value(self.row_id)
            .to_vec()
    }

    fn get_bytes(&self, pos: usize) -> Vec<u8> {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Expected bytes array.")
            .value(self.row_id)
            .to_vec()
    }
}
