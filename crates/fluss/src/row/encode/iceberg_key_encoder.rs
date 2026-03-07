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

//! Iceberg key encoding.
//!
//! This encoder follows Fluss Java's `IcebergKeyEncoder` / `IcebergBinaryRowWriter`:
//! - **Exactly one** key field is supported for Iceberg format.
//! - INT / DATE are encoded as **8-byte little-endian long** (int promoted to long).
//! - TIME is encoded as **microseconds since midnight** in 8-byte little-endian long.
//! - TIMESTAMP (without time zone) is encoded as **microseconds since epoch** in 8-byte
//!   little-endian long: `millis * 1000 + (nanos_of_millis / 1000)`.
//! - BIGINT / FLOAT / DOUBLE are encoded as little-endian primitives.
//! - DECIMAL is encoded as **unscaled value bytes** (two's complement big-endian) with **no**
//!   length prefix.
//! - STRING / CHAR are encoded as UTF-8 bytes with **no** length prefix.
//! - BYTES / BINARY are encoded as raw bytes with **no** length prefix.

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataType, RowType};
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

#[allow(dead_code)]
pub struct IcebergKeyEncoder {
    field_getter: FieldGetter,
    field_type: DataType,
}

impl IcebergKeyEncoder {
    pub fn create_key_encoder(row_type: &RowType, keys: &[String]) -> Result<IcebergKeyEncoder> {
        if keys.len() != 1 {
            return Err(IllegalArgument {
                message: format!(
                    "Key fields must have exactly one field for iceberg format, but got: {keys:?}"
                ),
            });
        }

        let key = &keys[0];
        let key_index = row_type
            .get_field_index(key)
            .ok_or_else(|| IllegalArgument {
                message: format!("Field {key:?} not found in input row type {row_type:?}"),
            })?;

        let field_type = row_type
            .fields()
            .get(key_index)
            .ok_or_else(|| IllegalArgument {
                message: format!("Invalid key field index {key_index} for row type {row_type:?}"),
            })?
            .data_type()
            .clone();

        // Fail fast on unsupported types to match Java behavior.
        Self::validate_supported_type(&field_type)?;

        Ok(IcebergKeyEncoder {
            field_getter: FieldGetter::create(&field_type, key_index),
            field_type,
        })
    }

    fn validate_supported_type(field_type: &DataType) -> Result<()> {
        match field_type {
            DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::Decimal(_)
            | DataType::String(_)
            | DataType::Char(_)
            | DataType::Binary(_)
            | DataType::Bytes(_) => Ok(()),

            DataType::Array(_) => Err(IllegalArgument {
                message:
                    "Array types cannot be used as bucket keys. Bucket keys must be scalar types."
                        .to_string(),
            }),
            DataType::Map(_) => Err(IllegalArgument {
                message:
                    "Map types cannot be used as bucket keys. Bucket keys must be scalar types."
                        .to_string(),
            }),
            other => Err(IllegalArgument {
                message: format!("Unsupported type for Iceberg key encoder: {other}"),
            }),
        }
    }
}

#[allow(dead_code)]
impl KeyEncoder for IcebergKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        let value = self.field_getter.get_field(row);
        if value.is_null() {
            return Err(IllegalArgument {
                message: "Cannot encode Iceberg key with null value".to_string(),
            });
        }

        let bytes: Vec<u8> = match (&self.field_type, value) {
            (DataType::Int(_), Datum::Int32(v)) => (v as i64).to_le_bytes().to_vec(),
            (DataType::Date(_), Datum::Date(v)) => (v.get_inner() as i64).to_le_bytes().to_vec(),

            (DataType::Time(_), Datum::Time(v)) => {
                let micros = v.get_inner() as i64 * 1000;
                micros.to_le_bytes().to_vec()
            }

            (DataType::BigInt(_), Datum::Int64(v)) => v.to_le_bytes().to_vec(),
            (DataType::Float(_), Datum::Float32(v)) => v.0.to_le_bytes().to_vec(),
            (DataType::Double(_), Datum::Float64(v)) => v.0.to_le_bytes().to_vec(),

            (DataType::Timestamp(_), Datum::TimestampNtz(ts)) => {
                let micros =
                    ts.get_millisecond() * 1000 + (ts.get_nano_of_millisecond() as i64 / 1000);
                micros.to_le_bytes().to_vec()
            }

            (DataType::Decimal(_), Datum::Decimal(d)) => d.to_unscaled_bytes(),
            (DataType::String(_), Datum::String(s)) => s.as_bytes().to_vec(),
            (DataType::Char(_), Datum::String(s)) => s.as_bytes().to_vec(),
            (DataType::Binary(_), Datum::Blob(b)) => b.as_ref().to_vec(),
            (DataType::Bytes(_), Datum::Blob(b)) => b.as_ref().to_vec(),

            // FieldGetter uses Datum::String for CHAR, Datum::Blob for BINARY/BYTES.
            (expected_type, actual) => {
                return Err(IllegalArgument {
                    message: format!(
                        "IcebergKeyEncoder type mismatch: expected {expected_type}, got {actual:?}"
                    ),
                });
            }
        };

        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::datum::{Date, Time, TimestampNtz};
    use crate::row::{Decimal, GenericRow};
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[test]
    fn test_single_key_field_requirement() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "name"],
        );

        // ok with single key
        let _ = IcebergKeyEncoder::create_key_encoder(&row_type, &["id".to_string()]).unwrap();

        // error with multiple keys
        let err = IcebergKeyEncoder::create_key_encoder(
            &row_type,
            &["id".to_string(), "name".to_string()],
        )
        .err()
        .unwrap();
        assert!(matches!(err, crate::error::Error::IllegalArgument { .. }));
        assert!(
            err.to_string()
                .contains("Key fields must have exactly one field for iceberg format")
        );
    }

    #[test]
    fn test_integer_encoding() {
        let row_type = RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["id"]);
        let row = GenericRow::from_data(vec![Datum::from(42i32)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["id".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), (42i64).to_le_bytes().as_slice());
    }

    #[test]
    fn test_long_encoding() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::bigint()], vec!["id"]);
        let v = 1234567890123456789i64;
        let row = GenericRow::from_data(vec![Datum::from(v)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["id".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), v.to_le_bytes().as_slice());
    }

    #[test]
    fn test_string_encoding() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::string()], vec!["name"]);
        let s = "Hello Iceberg, Fluss this side!";
        let row = GenericRow::from_data(vec![Datum::from(s)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["name".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), s.as_bytes());
    }

    #[test]
    fn test_decimal_encoding() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::decimal(10, 2)],
            vec!["amount"],
        );

        let bd = BigDecimal::from_str("123.45").unwrap();
        let decimal = Decimal::from_big_decimal(bd.clone(), 10, 2).unwrap();
        let row = GenericRow::from_data(vec![Datum::Decimal(decimal.clone())]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["amount".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), decimal.to_unscaled_bytes().as_slice());
    }

    #[test]
    fn test_timestamp_encoding() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::timestamp_with_precision(6)],
            vec!["event_time"],
        );

        let millis = 1698235273182i64;
        let nanos = 123_456i32;
        let ts = TimestampNtz::from_millis_nanos(millis, nanos).unwrap();
        let micros = millis * 1000 + (nanos as i64 / 1000);

        let row = GenericRow::from_data(vec![Datum::TimestampNtz(ts)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["event_time".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), micros.to_le_bytes().as_slice());
    }

    #[test]
    fn test_date_encoding() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::date()], vec!["date"]);

        let days = 19655i32; // 2023-10-25
        let row = GenericRow::from_data(vec![Datum::Date(Date::new(days))]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["date".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), (days as i64).to_le_bytes().as_slice());
    }

    #[test]
    fn test_time_encoding() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::time()], vec!["time"]);

        let millis_since_midnight = 34_200_000i32;
        let micros = millis_since_midnight as i64 * 1000;
        let row = GenericRow::from_data(vec![Datum::Time(Time::new(millis_since_midnight))]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["time".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), micros.to_le_bytes().as_slice());
    }

    #[test]
    fn test_binary_encoding() {
        let row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::bytes()], vec!["data"]);

        let bytes = b"Hello i only understand binary data".as_slice();
        let row = GenericRow::from_data(vec![Datum::from(bytes)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["data".to_string()]).unwrap();

        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), bytes);
    }
}
