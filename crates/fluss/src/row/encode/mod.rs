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
mod compacted_row_encoder;

use crate::error::Result;
use crate::metadata::{DataLakeFormat, KvFormat, RowType};
use crate::row::encode::compacted_key_encoder::CompactedKeyEncoder;
use crate::row::encode::compacted_row_encoder::CompactedRowEncoder;
use crate::row::{BinaryRow, Datum, InternalRow};
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

/// An encoder to write [`BinaryRow`]. It's used to write row
/// multi-times one by one. When writing a new row:
///
/// 1. call method [`RowEncoder::start_new_row()`] to start the writing.
/// 2. call method [`RowEncoder::encode_field()`] to write the row's field.
/// 3. call method [`RowEncoder::finishRow()`] to finish the writing and get the written row.
#[allow(dead_code)]
pub trait RowEncoder {
    /// Start to write a new row.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn start_new_row(&mut self) -> Result<()>;

    /// Write the row's field in given pos with given value.
    ///
    /// # Arguments
    /// * pos - the position of the field to write.
    /// * value - the value of the field to write.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn encode_field(&mut self, pos: usize, value: Datum) -> Result<()>;

    /// Finish write the row, returns the written row.
    ///
    /// Note that returned row borrows from [`RowEncoder`]'s internal buffer which is reused for subsequent rows
    /// [`RowEncoder::start_new_row()`] should only be called after the returned row goes out of scope.
    ///
    /// # Returns
    /// * the written row
    fn finish_row(&mut self) -> Result<impl BinaryRow>;

    /// Closes the row encoder
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn close(&mut self) -> Result<()>;
}

#[allow(dead_code)]
pub struct RowEncoderFactory {}

#[allow(dead_code)]
impl RowEncoderFactory {
    pub fn create(kv_format: KvFormat, row_type: &RowType) -> Result<impl RowEncoder> {
        Self::create_for_field_types(kv_format, row_type.clone())
    }

    pub fn create_for_field_types(
        kv_format: KvFormat,
        row_type: RowType,
    ) -> Result<impl RowEncoder> {
        match kv_format {
            KvFormat::INDEXED => {
                todo!()
            }
            KvFormat::COMPACTED => CompactedRowEncoder::new(row_type),
        }
    }
}
