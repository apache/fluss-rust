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

mod compacted_key_encoder;

use crate::error::Result;
use crate::metadata::{DataLakeFormat, RowType};
use crate::row::InternalRow;
use crate::row::encode::compacted_key_encoder::CompactedKeyEncoder;
use bytes::Bytes;

/// An interface for encoding key of row into bytes.
#[allow(dead_code)]
pub trait KeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes>;
}

#[allow(dead_code)]
impl dyn KeyEncoder {
    /// Create a key encoder to encode the key bytes of the input row.
    /// # Arguments
    /// * `row_type` - the row type of the input row
    /// * `key_fields` - the key fields to encode
    /// * `lake_format` - the data lake format
    ///
    /// # Returns
    /// key encoder
    pub fn of(
        row_type: &RowType,
        key_fields: Vec<String>,
        data_lake_format: Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Paimon) => {
                unimplemented!("KeyEncoder for Paimon format is currently unimplemented")
            }
            Some(DataLakeFormat::Lance) => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type,
                key_fields.as_slice(),
            )?)),
            Some(DataLakeFormat::Iceberg) => {
                unimplemented!("KeyEncoder for Iceberg format is currently unimplemented")
            }
            None => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type,
                key_fields.as_slice(),
            )?)),
        }
    }
}
