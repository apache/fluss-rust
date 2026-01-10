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
use crate::metadata::RowType;
use crate::row::binary::ValueWriter;
use crate::row::compacted::CompactedKeyWriter;
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

#[allow(dead_code)]
pub struct CompactedKeyEncoder {
    field_getters: Vec<FieldGetter>,
    field_encoders: Vec<ValueWriter>,
    compacted_encoder: CompactedKeyWriter,
}

impl CompactedKeyEncoder {
    /// Create a key encoder to encode the key of the input row.
    ///
    /// # Arguments
    /// * `row_type` - the row type of the input row
    /// * `keys` - the key fields to encode
    ///
    /// # Returns
    /// * key_encoder - the [`KeyEncoder`]
    pub fn create_key_encoder(row_type: &RowType, keys: &[String]) -> Result<CompactedKeyEncoder> {
        let mut encode_col_indexes = Vec::with_capacity(keys.len());

        for key in keys {
            match row_type.get_field_index(key) {
                Some(idx) => encode_col_indexes.push(idx),
                None => {
                    return Err(IllegalArgument {
                        message: format!(
                            "Field {:?} not found in input row type {:?}",
                            key, row_type
                        ),
                    });
                }
            }
        }

        Self::new(row_type, encode_col_indexes)
    }

    pub fn new(row_type: &RowType, encode_field_pos: Vec<usize>) -> Result<CompactedKeyEncoder> {
        let mut field_getters: Vec<FieldGetter> = Vec::with_capacity(encode_field_pos.len());
        let mut field_encoders: Vec<ValueWriter> = Vec::with_capacity(encode_field_pos.len());

        for pos in &encode_field_pos {
            let data_type = row_type.fields().get(*pos).unwrap().data_type();
            field_getters.push(FieldGetter::create(data_type, *pos));
            field_encoders.push(CompactedKeyWriter::create_value_writer(data_type)?);
        }

        Ok(CompactedKeyEncoder {
            field_encoders,
            field_getters,
            compacted_encoder: CompactedKeyWriter::new(),
        })
    }
}

#[allow(dead_code)]
impl KeyEncoder for CompactedKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        self.compacted_encoder.reset();

        // iterate all the fields of the row, and encode each field
        for (pos, field_getter) in self.field_getters.iter().enumerate() {
            match &field_getter.get_field(row) {
                Datum::Null => {
                    return Err(IllegalArgument {
                        message: format!(
                            "Cannot encode key with null value at position: {:?}",
                            pos
                        ),
                    });
                }
                value => self.field_encoders.get(pos).unwrap().write_value(
                    &mut self.compacted_encoder,
                    pos,
                    value,
                )?,
            }
        }

        Ok(self.compacted_encoder.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::{Datum, GenericRow};

    pub fn for_test_row_type(row_type: &RowType) -> CompactedKeyEncoder {
        CompactedKeyEncoder::new(row_type, (0..row_type.fields().len()).collect())
            .expect("CompactedKeyEncoder initialization failed")
    }

    #[test]
    fn test_encode_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::int(),
        ]);
        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::from(2i32),
        ]);

        let mut encoder = for_test_row_type(&row_type);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [1u8, 3u8, 2u8]
        );

        let row = GenericRow::from_data(vec![
            Datum::from(2i32),
            Datum::from(5i64),
            Datum::from(6i32),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [2u8, 5u8, 6u8]
        );
    }

    #[test]
    fn test_encode_key_with_key_names() {
        let data_types = vec![
            DataTypes::string(),
            DataTypes::bigint(),
            DataTypes::string(),
        ];
        let field_names = vec!["partition", "f1", "f2"];

        let row_type = RowType::with_data_types_and_field_names(data_types, field_names);

        let primary_keys = &["f2".to_string()];

        let mut encoder = CompactedKeyEncoder::create_key_encoder(&row_type, primary_keys).unwrap();

        let row = GenericRow::from_data(vec![
            Datum::from("p1"),
            Datum::from(1i64),
            Datum::from("a2"),
        ]);

        // should only get "a2" 's ASCII representation
        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            //  2 (start of text), 97 (the letter a), 50 (the number 2)
            [2u8, 97u8, 50u8]
        );
    }

    #[test]
    #[should_panic(expected = "Cannot encode key with null value at position: 2")]
    fn test_null_primary_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::int(),
            DataTypes::string(),
        ]);

        let primary_key_indices = vec![0, 1, 2];

        let mut encoder = CompactedKeyEncoder::new(&row_type, primary_key_indices)
            .expect("CompactedKeyEncoder initialization failed");

        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::from(2i32),
            Datum::from("a2"),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [1u8, 3u8, 2u8]
        );

        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::Null,
            Datum::from("a2"),
        ]);

        encoder.encode_key(&row).unwrap();
    }

    #[test]
    fn test_int_string_as_primary_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::string(),
            DataTypes::int(),
            DataTypes::string(),
            DataTypes::string(),
        ]);

        let primary_key_indices = vec![1, 2];
        let mut encoder = CompactedKeyEncoder::new(&row_type, primary_key_indices)
            .expect("CompactedKeyEncoder initialization failed");

        let row = GenericRow::from_data(vec![
            Datum::from("a1"),
            Datum::from(1i32),
            Datum::from("a2"),
            Datum::from("a3"),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            // 1 (1i32), 2 (start of text), 97 (the letter a), 50 (the number 2)
            [1u8, 2u8, 97u8, 50u8]
        );
    }

    #[test]
    fn test_all_data_types() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::boolean(),
            DataTypes::tinyint(),
            DataTypes::smallint(),
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::float(),
            DataTypes::double(),
            // TODO Date
            // TODO Time
            DataTypes::binary(20),
            DataTypes::bytes(),
            DataTypes::char(2),
            DataTypes::string(),
            // TODO Decimal
            // TODO Timestamp
            // TODO Timestamp LTZ
            // TODO Array of Int
            // TODO Array of Float
            // TODO Array of String
            // TODO: Add Map and Row fields in Issue #1973
        ]);

        let row = GenericRow::from_data(vec![
            Datum::from(true),
            Datum::from(2i8),
            Datum::from(10i16),
            Datum::from(100i32),
            Datum::from(-6101065172474983726i64), // from Java test case: new BigInteger("12345678901234567890").longValue()
            Datum::from(13.2f32),
            Datum::from(15.21f64),
            // TODO Date
            // TODO Time
            Datum::from("1234567890".as_bytes()),
            Datum::from("20".as_bytes()),
            Datum::from("1"),
            Datum::from("hello"),
            // TODO Decimal
            // TODO Timestamp
            // TODO Timestamp LTZ
            // TODO Array of Int
            // TODO Array of Float
            // TODO Array of String
            // TODO: Add Map and Row fields in Issue #1973
        ]);

        let mut encoder = for_test_row_type(&row_type);

        let mut expected: Vec<u8> = Vec::new();
        // BOOLEAN: true
        expected.extend(vec![0x01]);
        // TINYINT: 2
        expected.extend(vec![0x02]);
        // SMALLINT: 10
        expected.extend(vec![0x0A]);
        // INT: 100
        expected.extend(vec![0x00, 0x64]);
        // BIGINT: -6101065172474983726
        expected.extend(vec![
            0xD2, 0x95, 0xFC, 0xD8, 0xCE, 0xB1, 0xAA, 0xAA, 0xAB, 0x01,
        ]);
        // FLOAT: 13.2
        expected.extend(vec![0x33, 0x33, 0x53, 0x41]);
        // DOUBLE: 15.21
        expected.extend(vec![0xEC, 0x51, 0xB8, 0x1E, 0x85, 0x6B, 0x2E, 0x40]);
        // BINARY(20): "1234567890".getBytes()
        expected.extend(vec![
            0x0A, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30,
        ]);

        // BYTES: "20".getBytes()
        expected.extend(vec![0x02, 0x32, 0x30]);
        // CHAR(2): "1"
        expected.extend(vec![0x01, 0x31]);
        // STRING: String: "hello"
        expected.extend(vec![0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);
        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            expected.as_slice()
        );
    }
}
