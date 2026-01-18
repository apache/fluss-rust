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
use crate::row::compacted::compacted_row_reader::{CompactedRowDeserializer, CompactedRowReader};
use crate::row::{BinaryRow, GenericRow, InternalRow};
use std::sync::{Arc, OnceLock};

// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRow.java
#[allow(dead_code)]
pub struct CompactedRow<'a> {
    arity: usize,
    size_in_bytes: usize,
    decoded_row: OnceLock<GenericRow<'a>>,
    deserializer: Arc<CompactedRowDeserializer<'a>>,
    reader: CompactedRowReader<'a>,
    data: &'a [u8],
}

pub fn calculate_bit_set_width_in_bytes(arity: usize) -> usize {
    arity.div_ceil(8)
}

#[allow(dead_code)]
impl<'a> CompactedRow<'a> {
    pub fn from_bytes(row_type: &'a RowType, data: &'a [u8]) -> Self {
        Self::deserialize(
            Arc::new(CompactedRowDeserializer::new(row_type)),
            row_type.fields().len(),
            data,
        )
    }

    pub fn deserialize(
        deserializer: Arc<CompactedRowDeserializer<'a>>,
        arity: usize,
        data: &'a [u8],
    ) -> Self {
        Self {
            arity,
            size_in_bytes: data.len(),
            decoded_row: OnceLock::new(),
            deserializer: Arc::clone(&deserializer),
            reader: CompactedRowReader::new(arity, data, 0, data.len()),
            data,
        }
    }

    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn decoded_row(&self) -> &GenericRow<'_> {
        self.decoded_row
            .get_or_init(|| self.deserializer.deserialize(&self.reader))
    }
}

impl BinaryRow for CompactedRow<'_> {
    fn as_bytes(&self) -> &[u8] {
        self.data
    }
}

#[allow(dead_code)]
impl<'a> InternalRow for CompactedRow<'a> {
    fn get_field_count(&self) -> usize {
        self.arity
    }

    fn is_null_at(&self, pos: usize) -> bool {
        self.deserializer.get_row_type().fields().as_slice()[pos]
            .data_type
            .is_nullable()
            && self.reader.is_null_at(pos)
    }

    fn get_boolean(&self, pos: usize) -> bool {
        self.decoded_row().get_boolean(pos)
    }

    fn get_byte(&self, pos: usize) -> i8 {
        self.decoded_row().get_byte(pos)
    }

    fn get_short(&self, pos: usize) -> i16 {
        self.decoded_row().get_short(pos)
    }

    fn get_int(&self, pos: usize) -> i32 {
        self.decoded_row().get_int(pos)
    }

    fn get_long(&self, pos: usize) -> i64 {
        self.decoded_row().get_long(pos)
    }

    fn get_float(&self, pos: usize) -> f32 {
        self.decoded_row().get_float(pos)
    }

    fn get_double(&self, pos: usize) -> f64 {
        self.decoded_row().get_double(pos)
    }

    fn get_char(&self, pos: usize, length: usize) -> &str {
        self.decoded_row().get_char(pos, length)
    }

    fn get_string(&self, pos: usize) -> &str {
        self.decoded_row().get_string(pos)
    }

    fn get_binary(&self, pos: usize, length: usize) -> &[u8] {
        self.decoded_row().get_binary(pos, length)
    }

    fn get_bytes(&self, pos: usize) -> &[u8] {
        self.decoded_row().get_bytes(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::binary::BinaryWriter;

    use crate::metadata::{
        BigIntType, BooleanType, BytesType, DataType, DoubleType, FloatType, IntType, SmallIntType,
        StringType, TinyIntType,
    };
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;

    #[test]
    fn test_compacted_row() {
        // Test all primitive types
        let row_type = RowType::with_data_types(vec![
            DataType::Boolean(BooleanType::new()),
            DataType::TinyInt(TinyIntType::new()),
            DataType::SmallInt(SmallIntType::new()),
            DataType::Int(IntType::new()),
            DataType::BigInt(BigIntType::new()),
            DataType::Float(FloatType::new()),
            DataType::Double(DoubleType::new()),
            DataType::String(StringType::new()),
            DataType::Bytes(BytesType::new()),
        ]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        writer.write_boolean(true);
        writer.write_byte(1);
        writer.write_short(100);
        writer.write_int(1000);
        writer.write_long(10000);
        writer.write_float(1.5);
        writer.write_double(2.5);
        writer.write_string("Hello World");
        writer.write_bytes(&[1, 2, 3, 4, 5]);

        let bytes = writer.to_bytes();
        let mut row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert_eq!(row.get_field_count(), 9);
        assert!(row.get_boolean(0));
        assert_eq!(row.get_byte(1), 1);
        assert_eq!(row.get_short(2), 100);
        assert_eq!(row.get_int(3), 1000);
        assert_eq!(row.get_long(4), 10000);
        assert_eq!(row.get_float(5), 1.5);
        assert_eq!(row.get_double(6), 2.5);
        assert_eq!(row.get_string(7), "Hello World");
        assert_eq!(row.get_bytes(8), &[1, 2, 3, 4, 5]);

        // Test with nulls
        let row_type = RowType::with_data_types(
            [
                DataType::Int(IntType::new()),
                DataType::String(StringType::new()),
                DataType::Double(DoubleType::new()),
            ]
            .to_vec(),
        );

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        writer.write_int(100);
        writer.set_null_at(1);
        writer.write_double(2.71);

        let bytes = writer.to_bytes();
        row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert!(!row.is_null_at(0));
        assert!(row.is_null_at(1));
        assert!(!row.is_null_at(2));
        assert_eq!(row.get_int(0), 100);
        assert_eq!(row.get_double(2), 2.71);

        // Test multiple reads (caching)
        assert_eq!(row.get_int(0), 100);
        assert_eq!(row.get_int(0), 100);

        // Test from_bytes
        let row_type = RowType::with_data_types(vec![
            DataType::Int(IntType::new()),
            DataType::String(StringType::new()),
        ]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());
        writer.write_int(-1);
        writer.write_string("test");

        let bytes = writer.to_bytes();
        let mut row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert_eq!(row.get_int(0), -1);
        assert_eq!(row.get_string(1), "test");

        // Test large row
        let num_fields = 100;
        let row_type = RowType::with_data_types(
            (0..num_fields)
                .map(|_| DataType::Int(IntType::new()))
                .collect(),
        );

        let mut writer = CompactedRowWriter::new(num_fields);

        for i in 0..num_fields {
            writer.write_int((i * 10) as i32);
        }

        let bytes = writer.to_bytes();
        row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        for i in 0..num_fields {
            assert_eq!(row.get_int(i), (i * 10) as i32);
        }
    }
}
