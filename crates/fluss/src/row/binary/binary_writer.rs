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
use crate::metadata::DataType;
use crate::row::Datum;
use crate::row::binary::BinaryRowFormat;

/// Writer to write a composite data format, like row, array,
#[allow(dead_code)]
pub trait BinaryWriter {
    /// Reset writer to prepare next write
    fn reset(&mut self);

    /// Set null to this field
    fn set_null_at(&mut self, pos: usize);

    fn write_boolean(&mut self, value: bool) -> Result<()>;

    fn write_byte(&mut self, value: u8) -> Result<()>;

    fn write_bytes(&mut self, value: &[u8]) -> Result<()>;

    fn write_char(&mut self, value: &str, length: usize) -> Result<()>;

    fn write_string(&mut self, value: &str) -> Result<()>;

    fn write_short(&mut self, value: i16) -> Result<()>;

    fn write_int(&mut self, value: i32) -> Result<()>;

    fn write_long(&mut self, value: i64) -> Result<()>;

    fn write_float(&mut self, value: f32) -> Result<()>;

    fn write_double(&mut self, value: f64) -> Result<()>;

    fn write_binary(&mut self, bytes: &[u8], length: usize) -> Result<()>;

    fn write_decimal(
        &mut self,
        value: &bigdecimal::BigDecimal,
        precision: u32,
        scale: u32,
    ) -> Result<()>;

    fn write_time(&mut self, value: i32, precision: u32) -> Result<()>;

    fn write_timestamp_ntz(
        &mut self,
        value: &crate::row::datum::Timestamp,
        precision: u32,
    ) -> Result<()>;

    fn write_timestamp_ltz(
        &mut self,
        value: &crate::row::datum::TimestampLtz,
        precision: u32,
    ) -> Result<()>;

    // TODO InternalArray, ArraySerializer
    // fn write_array(&mut self, pos: i32, value: i64);

    // TODO Row serializer
    // fn write_row(&mut self, pos: i32, value: &InternalRow);

    /// Finally, complete write to set real size to binary.
    fn complete(&mut self);
}

pub enum ValueWriter {
    Nullable(InnerValueWriter),
    NonNullable(InnerValueWriter),
}

impl ValueWriter {
    pub fn create_value_writer(
        element_type: &DataType,
        binary_row_format: Option<&BinaryRowFormat>,
    ) -> Result<ValueWriter> {
        let value_writer =
            InnerValueWriter::create_inner_value_writer(element_type, binary_row_format)?;
        if element_type.is_nullable() {
            Ok(Self::Nullable(value_writer))
        } else {
            Ok(Self::NonNullable(value_writer))
        }
    }

    pub fn write_value<W: BinaryWriter>(
        &self,
        writer: &mut W,
        pos: usize,
        value: &Datum,
    ) -> Result<()> {
        match self {
            Self::Nullable(inner_value_writer) => {
                if let Datum::Null = value {
                    writer.set_null_at(pos);
                    Ok(())
                } else {
                    inner_value_writer.write_value(writer, pos, value)
                }
            }
            Self::NonNullable(inner_value_writer) => {
                inner_value_writer.write_value(writer, pos, value)
            }
        }
    }
}

#[derive(Debug)]
pub enum InnerValueWriter {
    Char,
    String,
    Boolean,
    Binary,
    Bytes,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal(u32, u32), // precision, scale
    Date,
    Time(u32),         // precision (not used in wire format, but kept for consistency)
    TimestampNtz(u32), // precision
    TimestampLtz(u32), // precision
                       // TODO Array, Row
}

/// Accessor for writing the fields/elements of a binary writer during runtime, the
/// fields/elements must be written in the order.
impl InnerValueWriter {
    pub fn create_inner_value_writer(
        data_type: &DataType,
        _: Option<&BinaryRowFormat>,
    ) -> Result<InnerValueWriter> {
        match data_type {
            DataType::Char(_) => Ok(InnerValueWriter::Char),
            DataType::String(_) => Ok(InnerValueWriter::String),
            DataType::Boolean(_) => Ok(InnerValueWriter::Boolean),
            DataType::Binary(_) => Ok(InnerValueWriter::Binary),
            DataType::Bytes(_) => Ok(InnerValueWriter::Bytes),
            DataType::TinyInt(_) => Ok(InnerValueWriter::TinyInt),
            DataType::SmallInt(_) => Ok(InnerValueWriter::SmallInt),
            DataType::Int(_) => Ok(InnerValueWriter::Int),
            DataType::BigInt(_) => Ok(InnerValueWriter::BigInt),
            DataType::Float(_) => Ok(InnerValueWriter::Float),
            DataType::Double(_) => Ok(InnerValueWriter::Double),
            DataType::Decimal(d) => {
                let precision = d.precision();
                let scale = d.scale();
                if !(1..=38).contains(&precision) {
                    return Err(IllegalArgument {
                        message: format!(
                            "Decimal precision must be between 1 and 38 (both inclusive), got: {}",
                            precision
                        ),
                    });
                }
                if scale > precision {
                    return Err(IllegalArgument {
                        message: format!(
                            "Decimal scale must be between 0 and the precision {} (both inclusive), got: {}",
                            precision, scale
                        ),
                    });
                }
                Ok(InnerValueWriter::Decimal(precision, scale))
            }
            DataType::Date(_) => Ok(InnerValueWriter::Date),
            DataType::Time(t) => {
                let precision = t.precision();
                if precision > 9 {
                    return Err(IllegalArgument {
                        message: format!(
                            "Time precision must be between 0 and 9 (both inclusive), got: {}",
                            precision
                        ),
                    });
                }
                Ok(InnerValueWriter::Time(precision))
            }
            DataType::Timestamp(t) => {
                let precision = t.precision();
                if precision > 9 {
                    return Err(IllegalArgument {
                        message: format!(
                            "Timestamp precision must be between 0 and 9 (both inclusive), got: {}",
                            precision
                        ),
                    });
                }
                Ok(InnerValueWriter::TimestampNtz(precision))
            }
            DataType::TimestampLTz(t) => {
                let precision = t.precision();
                if precision > 9 {
                    return Err(IllegalArgument {
                        message: format!(
                            "Timestamp with local time zone precision must be between 0 and 9 (both inclusive), got: {}",
                            precision
                        ),
                    });
                }
                Ok(InnerValueWriter::TimestampLtz(precision))
            }
            _ => unimplemented!(
                "ValueWriter for DataType {:?} is currently not implemented",
                data_type
            ),
        }
    }
    pub fn write_value<W: BinaryWriter>(
        &self,
        writer: &mut W,
        _pos: usize,
        value: &Datum,
    ) -> Result<()> {
        match (self, value) {
            (InnerValueWriter::Char, Datum::String(v)) => {
                writer.write_char(v, v.len())?;
            }
            (InnerValueWriter::String, Datum::String(v)) => {
                writer.write_string(v)?;
            }
            (InnerValueWriter::Boolean, Datum::Bool(v)) => {
                writer.write_boolean(*v)?;
            }
            (InnerValueWriter::Binary, Datum::Blob(v)) => {
                let b = v.as_ref();
                writer.write_binary(b, b.len())?;
            }
            (InnerValueWriter::Bytes, Datum::Blob(v)) => {
                writer.write_bytes(v.as_ref())?;
            }
            (InnerValueWriter::TinyInt, Datum::Int8(v)) => {
                writer.write_byte(*v as u8)?;
            }
            (InnerValueWriter::SmallInt, Datum::Int16(v)) => {
                writer.write_short(*v)?;
            }
            (InnerValueWriter::Int, Datum::Int32(v)) => {
                writer.write_int(*v)?;
            }
            (InnerValueWriter::BigInt, Datum::Int64(v)) => {
                writer.write_long(*v)?;
            }
            (InnerValueWriter::Float, Datum::Float32(v)) => {
                writer.write_float(v.into_inner())?;
            }
            (InnerValueWriter::Double, Datum::Float64(v)) => {
                writer.write_double(v.into_inner())?;
            }
            (InnerValueWriter::Decimal(p, s), Datum::Decimal(v)) => {
                writer.write_decimal(v, *p, *s)?;
            }
            (InnerValueWriter::Date, Datum::Date(d)) => {
                writer.write_int(d.get_inner())?;
            }
            (InnerValueWriter::Time(p), Datum::Time(t)) => {
                writer.write_time(t.get_inner(), *p)?;
            }
            (InnerValueWriter::TimestampNtz(p), Datum::Timestamp(ts)) => {
                writer.write_timestamp_ntz(ts, *p)?;
            }
            (InnerValueWriter::TimestampLtz(p), Datum::TimestampTz(ts)) => {
                writer.write_timestamp_ltz(ts, *p)?;
            }
            _ => {
                return Err(IllegalArgument {
                    message: format!("{self:?} used to write value {value:?}"),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, DecimalType, TimeType, TimestampLTzType, TimestampType};

    #[test]
    fn test_decimal_precision_validation() {
        // Valid cases
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::decimal(1, 0),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::decimal(38, 10),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );

        // Invalid: precision too low
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::Decimal(DecimalType::with_nullable(true, 0, 0)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Decimal precision must be between 1 and 38")
        );

        // Invalid: precision too high
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::Decimal(DecimalType::with_nullable(true, 39, 0)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Decimal precision must be between 1 and 38")
        );

        // Invalid: scale > precision
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::Decimal(DecimalType::with_nullable(true, 10, 11)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Decimal scale must be between 0 and the precision")
        );
    }

    #[test]
    fn test_time_precision_validation() {
        // Valid cases
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::time_with_precision(0),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::time_with_precision(9),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );

        // Invalid: precision too high
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::Time(TimeType::with_nullable(true, 10)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Time precision must be between 0 and 9")
        );
    }

    #[test]
    fn test_timestamp_precision_validation() {
        // Valid cases
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::timestamp_with_precision(0),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::timestamp_with_precision(9),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );

        // Invalid: precision too high
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::Timestamp(TimestampType::with_nullable(true, 10)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Timestamp precision must be between 0 and 9")
        );
    }

    #[test]
    fn test_timestamp_ltz_precision_validation() {
        // Valid cases
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::timestamp_ltz_with_precision(0),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );
        assert!(
            InnerValueWriter::create_inner_value_writer(
                &DataTypes::timestamp_ltz_with_precision(9),
                Some(&BinaryRowFormat::Compacted)
            )
            .is_ok()
        );

        // Invalid: precision too high
        let result = InnerValueWriter::create_inner_value_writer(
            &DataType::TimestampLTz(TimestampLTzType::with_nullable(true, 10)),
            Some(&BinaryRowFormat::Compacted),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Timestamp with local time zone precision must be between 0 and 9")
        );
    }
}
