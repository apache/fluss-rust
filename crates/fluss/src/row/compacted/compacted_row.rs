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

use crate::metadata::DataType;
use crate::row::compacted::compacted_row_reader::{CompactedRowDeserializer, CompactedRowReader};
use crate::row::{GenericRow, InternalRow};

// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRow.java
#[allow(dead_code)]
pub struct CompactedRow {
    arity: usize,
    segment: Bytes,
    offset: usize,
    size_in_bytes: usize,
    decoded: bool,
    decoded_row: GenericRow<'static>,
    reader: CompactedRowReader,
    deserializer: CompactedRowDeserializer,
}

#[allow(dead_code)]
impl CompactedRow {
    pub fn calculate_bit_set_width_in_bytes(arity: usize) -> usize {
        arity.div_ceil(8)
    }

    pub fn new(types: Vec<DataType>) -> Self {
        let arity = types.len();
        Self {
            arity,
            segment: Bytes::new(),
            offset: 0,
            size_in_bytes: 0,
            decoded: false,
            decoded_row: GenericRow::new(),
            reader: CompactedRowReader::new(arity),
            deserializer: CompactedRowDeserializer::new(types),
        }
    }

    pub fn from_bytes(types: Vec<DataType>, data: Bytes) -> Self {
        let arity = types.len();
        let size = data.len();
        Self {
            arity,
            segment: data,
            offset: 0,
            size_in_bytes: size,
            decoded: false,
            decoded_row: GenericRow::new(),
            reader: CompactedRowReader::new(arity),
            deserializer: CompactedRowDeserializer::new(types),
        }
    }

    pub fn point_to(&mut self, segment: Bytes, offset: usize, size_in_bytes: usize) {
        self.segment = segment;
        self.offset = offset;
        self.size_in_bytes = size_in_bytes;
        self.decoded = false;
    }

    pub fn get_segment(&self) -> &Bytes {
        &self.segment
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    pub fn get_field_count(&self) -> usize {
        self.arity
    }

    pub fn is_null_at(&self, pos: usize) -> bool {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    fn decoded_row(&mut self) -> &GenericRow<'static> {
        if !self.decoded {
            self.reader
                .point_to(self.segment.clone(), self.offset, self.size_in_bytes);
            self.decoded_row = self.deserializer.deserialize(&mut self.reader);
            self.decoded = true;
        }
        &self.decoded_row
    }

    pub fn get_boolean(&mut self, pos: usize) -> bool {
        self.decoded_row().get_boolean(pos)
    }

    pub fn get_byte(&mut self, pos: usize) -> i8 {
        self.decoded_row().get_byte(pos)
    }

    pub fn get_short(&mut self, pos: usize) -> i16 {
        self.decoded_row().get_short(pos)
    }

    pub fn get_int(&mut self, pos: usize) -> i32 {
        self.decoded_row().get_int(pos)
    }

    pub fn get_long(&mut self, pos: usize) -> i64 {
        self.decoded_row().get_long(pos)
    }

    pub fn get_float(&mut self, pos: usize) -> f32 {
        self.decoded_row().get_float(pos)
    }

    pub fn get_double(&mut self, pos: usize) -> f64 {
        self.decoded_row().get_double(pos)
    }

    pub fn get_string(&mut self, pos: usize) -> &str {
        self.decoded_row().get_string(pos)
    }

    pub fn get_bytes(&mut self, pos: usize) -> &[u8] {
        self.decoded_row().get_bytes(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        BigIntType, BooleanType, BytesType, DoubleType, FloatType, IntType, SmallIntType,
        StringType, TinyIntType,
    };
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;

    #[test]
    fn test_compacted_row() {
        // Test all primitive types
        let types = vec![
            DataType::Boolean(BooleanType::new()),
            DataType::TinyInt(TinyIntType::new()),
            DataType::SmallInt(SmallIntType::new()),
            DataType::Int(IntType::new()),
            DataType::BigInt(BigIntType::new()),
            DataType::Float(FloatType::new()),
            DataType::Double(DoubleType::new()),
            DataType::String(StringType::new()),
            DataType::Bytes(BytesType::new()),
        ];

        let mut row = CompactedRow::new(types.clone());
        let mut writer = CompactedRowWriter::new(types.len());

        writer.write_boolean(true);
        writer.write_byte(1);
        writer.write_short(100);
        writer.write_int(1000);
        writer.write_long(10000);
        writer.write_float(1.5);
        writer.write_double(2.5);
        writer.write_string("Hello World");
        writer.write_bytes(&[1, 2, 3, 4, 5]);

        row.point_to(writer.to_bytes(), 0, writer.position());

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
        let types = vec![
            DataType::Int(IntType::new()),
            DataType::String(StringType::new()),
            DataType::Double(DoubleType::new()),
        ];

        let mut row = CompactedRow::new(types.clone());
        let mut writer = CompactedRowWriter::new(types.len());

        writer.write_int(100);
        writer.set_null_at(1);
        writer.write_double(2.71);

        row.point_to(writer.to_bytes(), 0, writer.position());

        assert!(!row.is_null_at(0));
        assert!(row.is_null_at(1));
        assert!(!row.is_null_at(2));
        assert_eq!(row.get_int(0), 100);
        assert_eq!(row.get_double(2), 2.71);

        // Test multiple reads (caching)
        assert_eq!(row.get_int(0), 100);
        assert_eq!(row.get_int(0), 100);

        // Test from_bytes
        let types = vec![
            DataType::Int(IntType::new()),
            DataType::String(StringType::new()),
        ];

        let mut writer = CompactedRowWriter::new(types.len());
        writer.write_int(42);
        writer.write_string("test");

        let mut row = CompactedRow::from_bytes(types, writer.to_bytes());

        assert_eq!(row.get_int(0), 42);
        assert_eq!(row.get_string(1), "test");

        // Test large row
        let num_fields = 100;
        let types: Vec<DataType> = (0..num_fields)
            .map(|_| DataType::Int(IntType::new()))
            .collect();

        let mut row = CompactedRow::new(types.clone());
        let mut writer = CompactedRowWriter::new(num_fields);

        for i in 0..num_fields {
            writer.write_int((i * 10) as i32);
        }

        row.point_to(writer.to_bytes(), 0, writer.position());

        for i in 0..num_fields {
            assert_eq!(row.get_int(i), (i * 10) as i32);
        }
    }
}
