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
use crate::row::InternalRow;
use crate::row::binary::ValueWriter;
use crate::row::compacted::CompactedKeyWriter;
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use bytes::Bytes;

#[allow(dead_code)]
pub struct CompactedKeyEncoder {
    field_getters: Vec<Box<dyn FieldGetter>>,
    field_encoders: Vec<Box<dyn ValueWriter>>,
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
                            "Field {} not found in input row type {:?}",
                            key, row_type
                        ),
                    });
                }
            }
        }

        Ok(Self::new(row_type, encode_col_indexes))
    }

    pub fn new(row_type: &RowType, encode_field_pos: Vec<usize>) -> CompactedKeyEncoder {
        let mut field_getters: Vec<Box<dyn FieldGetter>> =
            Vec::with_capacity(encode_field_pos.len());
        let mut field_encoders: Vec<Box<dyn ValueWriter>> =
            Vec::with_capacity(encode_field_pos.len());

        for pos in &encode_field_pos {
            let data_type = row_type.fields().get(*pos).unwrap().data_type();
            field_getters.push(<dyn FieldGetter>::create_field_getter(data_type, *pos));
            field_encoders.push(CompactedKeyWriter::create_value_writer(data_type));
        }

        CompactedKeyEncoder {
            field_encoders,
            field_getters,
            compacted_encoder: CompactedKeyWriter::new(),
        }
    }
}

#[allow(dead_code)]
impl KeyEncoder for CompactedKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Bytes {
        self.compacted_encoder.reset();

        // iterate all the fields of the row, and encode each field
        self.field_getters
            .iter()
            .enumerate()
            .for_each(|(pos, field_getter)| {
                self.field_encoders.get(pos).unwrap().write_value(
                    &mut self.compacted_encoder,
                    pos,
                    &field_getter.get_field(row),
                );
            });

        self.compacted_encoder.to_bytes()
    }
}
