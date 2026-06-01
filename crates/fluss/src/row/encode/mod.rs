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
mod iceberg_key_encoder;

use crate::error::{Error, Result};
use crate::metadata::{DataLakeFormat, KvFormat, KvFormatVersion, RowType, TableConfig};
use crate::row::encode::compacted_key_encoder::CompactedKeyEncoder;
use crate::row::encode::compacted_row_encoder::CompactedRowEncoder;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

pub use iceberg_key_encoder::IcebergKeyEncoder;

/// An interface for encoding key of row into bytes.
#[allow(dead_code)]
pub trait KeyEncoder: Send + Sync {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes>;
}

pub struct KeyEncoderFactory;

impl KeyEncoderFactory {
    fn of(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Paimon) => Err(Error::UnsupportedOperation {
                message: "KeyEncoder for Paimon format is not yet implemented".to_string(),
            }),
            Some(DataLakeFormat::Lance) => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            Some(DataLakeFormat::Iceberg) => Ok(Box::new(IcebergKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            None => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
        }
    }

    pub fn of_bucket_key(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        Self::of(row_type, key_fields, data_lake_format)
    }

    /// When `table.kv.format-version` is **2** and the table uses a **custom** bucket key
    /// (`is_default_bucket_key == false`), the primary key is encoded with
    /// [`CompactedKeyEncoder`] so prefix lookups work, even for Iceberg-tiered tables.
    /// Otherwise encoding follows [`Self::of`] (v1 tables, or v2 with default bucket key).
    pub fn of_primary_key(
        row_type: &RowType,
        key_fields: &[String],
        table_config: &TableConfig,
        is_default_bucket_key: bool,
    ) -> Result<Box<dyn KeyEncoder>> {
        let kv_version = table_config.get_kv_format_version();
        let data_lake_format = &table_config.get_datalake_format()?;

        match (kv_version, is_default_bucket_key) {
            (KvFormatVersion::V1, _) | (KvFormatVersion::V2, true) => {
                Self::of(row_type, key_fields, data_lake_format)
            }
            (KvFormatVersion::V2, false) => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
        }
    }
}

/// An encoder to write binary row data. It's used to write rows
/// one by one. When writing a new row:
///
/// 1. call method [`RowEncoder::start_new_row()`] to start the writing.
/// 2. call method [`RowEncoder::encode_field()`] to write the row's field.
/// 3. call method [`RowEncoder::finish_row()`] to finish the writing and get the written row.
#[allow(dead_code)]
pub trait RowEncoder: Send + Sync {
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
    fn finish_row(&mut self) -> Result<Bytes>;

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
    pub fn create(kv_format: KvFormat, row_type: RowType) -> Result<impl RowEncoder> {
        Self::create_for_field_types(kv_format, row_type)
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

#[cfg(test)]
mod key_encoder_factory_tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::{Datum, GenericRow};
    use std::collections::HashMap;

    fn table_config(pairs: &[(&str, &str)]) -> TableConfig {
        TableConfig::from_properties(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn primary_key_kv2_default_bucket_iceberg_uses_iceberg_encoding() {
        let cfg = table_config(&[
            ("table.kv.format-version", "2"),
            ("table.datalake.format", "iceberg"),
        ]);
        let row_type = RowType::with_data_types(vec![DataTypes::int()]);
        let mut enc =
            KeyEncoderFactory::of_primary_key(&row_type, &["f0".to_string()], &cfg, true).unwrap();
        let row = GenericRow::from_data(vec![Datum::from(7i32)]);
        let bytes = enc.encode_key(&row).unwrap();
        assert_eq!(bytes.as_ref(), (7i64).to_le_bytes());
    }

    #[test]
    fn primary_key_kv2_custom_bucket_uses_compacted_even_with_iceberg() {
        let cfg = table_config(&[
            ("table.kv.format-version", "2"),
            ("table.datalake.format", "iceberg"),
        ]);
        let row_type = RowType::with_data_types(vec![DataTypes::int(), DataTypes::string()]);
        let mut enc = KeyEncoderFactory::of_primary_key(
            &row_type,
            &["f0".to_string(), "f1".to_string()],
            &cfg,
            false,
        )
        .unwrap();
        let row = GenericRow::from_data(vec![Datum::from(1i32), Datum::from("x")]);
        let bytes = enc.encode_key(&row).unwrap();
        assert_ne!(bytes.as_ref(), (1i64).to_le_bytes());
    }

    #[test]
    fn primary_key_invalid_kv_format_version_defaults_to_v1() {
        let cfg = table_config(&[
            ("table.kv.format-version", "99"),
            ("table.datalake.format", "iceberg"),
        ]);
        let row_type = RowType::with_data_types(vec![DataTypes::int()]);
        let mut enc =
            KeyEncoderFactory::of_primary_key(&row_type, &["f0".to_string()], &cfg, false).unwrap();
        let row = GenericRow::from_data(vec![Datum::from(7i32)]);
        assert_eq!(enc.encode_key(&row).unwrap().as_ref(), (7i64).to_le_bytes());
    }
}
