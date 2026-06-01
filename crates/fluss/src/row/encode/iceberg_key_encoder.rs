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

//! Primary key encoder for Iceberg lake tiering (single key column).
//!
//! Reference: `org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder`

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::RowType;
use crate::row::binary::{BinaryWriter, IcebergBinaryRowWriter, ValueWriter};
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

pub struct IcebergKeyEncoder {
    field_getter: FieldGetter,
    field_encoder: ValueWriter,
    writer: IcebergBinaryRowWriter,
}

impl IcebergKeyEncoder {
    /// Create an Iceberg-format key encoder for the given primary key field names.
    ///
    /// Iceberg tiering requires exactly one primary key column (FIP / Java `checkArgument`).
    pub fn create_key_encoder(row_type: &RowType, keys: &[String]) -> Result<Self> {
        if keys.len() != 1 {
            return Err(IllegalArgument {
                message: format!(
                    "Key fields must have exactly one field for iceberg format, but got: {keys:?}"
                ),
            });
        }

        let key_name = &keys[0];
        let key_index = match row_type.get_field_index(key_name) {
            Some(idx) => idx,
            None => {
                return Err(IllegalArgument {
                    message: format!("Field {key_name:?} not found in input row type {row_type:?}"),
                });
            }
        };

        let data_type = row_type.fields().get(key_index).unwrap().data_type();
        let field_encoder = IcebergBinaryRowWriter::create_value_writer(data_type)?;
        let field_getter = FieldGetter::create(data_type, key_index);

        Ok(IcebergKeyEncoder {
            field_getter,
            field_encoder,
            writer: IcebergBinaryRowWriter::new(),
        })
    }
}

impl KeyEncoder for IcebergKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        self.writer.reset();
        match self.field_getter.get_field(row)? {
            Datum::Null => Err(IllegalArgument {
                message: "Cannot encode Iceberg key with null value".to_string(),
            }),
            value => {
                self.field_encoder
                    .write_value(&mut self.writer, 0, &value)?;
                self.writer.complete();
                Ok(self.writer.to_bytes())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataType, DataTypes, TimestampType};
    use crate::row::datum::{Time, TimestampNtz};
    use crate::row::{Decimal, GenericRow};
    use bigdecimal::{BigDecimal, num_bigint::BigInt};

    #[test]
    fn single_key_field_requirement() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "name"],
        );

        assert!(IcebergKeyEncoder::create_key_encoder(&row_type, &["id".to_string()]).is_ok());

        let err = IcebergKeyEncoder::create_key_encoder(
            &row_type,
            &["id".to_string(), "name".to_string()],
        )
        .err()
        .expect("expected error for multiple keys");
        assert!(
            err.to_string().contains("exactly one field"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_integer_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::int()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let row = GenericRow::from_data(vec![Datum::from(42i32)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), (42i64).to_le_bytes());
    }

    #[test]
    fn test_long_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::bigint()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let v: i64 = 1234567890123456789;
        let row = GenericRow::from_data(vec![Datum::from(v)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), v.to_le_bytes());
    }

    #[test]
    fn test_string_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::string()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let s = "Hello Iceberg, Fluss this side!";
        let row = GenericRow::from_data(vec![Datum::from(s)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), s.as_bytes());
    }

    #[test]
    fn test_decimal_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::decimal(10, 2)]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let dec =
            Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12345), 2), 10, 2).unwrap();
        let row = GenericRow::from_data(vec![Datum::Decimal(dec)]);
        let encoded = encoder.encode_key(&row).unwrap();
        let expected = BigDecimal::new(BigInt::from(12345), 2)
            .into_bigint_and_exponent()
            .0
            .to_signed_bytes_be();
        assert_eq!(encoded.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_timestamp_encoding() {
        let row_type = RowType::with_data_types(vec![DataType::Timestamp(
            TimestampType::with_nullable(false, 6).unwrap(),
        )]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let millis = 1698235273182i64;
        let nanos = 123000;
        let micros = millis * 1000 + (nanos as i64 / 1000);
        let ts = TimestampNtz::from_millis_nanos(millis, nanos).unwrap();
        let row = GenericRow::from_data(vec![Datum::TimestampNtz(ts)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), micros.to_le_bytes());
    }

    #[test]
    fn test_date_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::date()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let date_value = 19655i32;
        let row = GenericRow::from_data(vec![Datum::from(date_value)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), (date_value as i64).to_le_bytes());
    }

    #[test]
    fn test_time_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::time()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let time_millis = 34200000i32;
        let row = GenericRow::from_data(vec![Datum::Time(Time::new(time_millis))]);
        let encoded = encoder.encode_key(&row).unwrap();
        let micros = (time_millis as i64) * 1000;
        assert_eq!(encoded.as_ref(), micros.to_le_bytes());
    }

    #[test]
    fn test_binary_encoding() {
        let row_type = RowType::with_data_types(vec![DataTypes::bytes()]);
        let mut encoder =
            IcebergKeyEncoder::create_key_encoder(&row_type, &["f0".to_string()]).unwrap();
        let data: &[u8] = b"Hello i only understand binary data";
        let row = GenericRow::from_data(vec![Datum::from(data)]);
        let encoded = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded.as_ref(), data);
    }
}
