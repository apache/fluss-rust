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

    fn write_boolean(&mut self, value: bool);

    fn write_byte(&mut self, value: u8);

    fn write_bytes(&mut self, value: &[u8]);

    fn write_char(&mut self, value: &str, length: usize);

    fn write_string(&mut self, value: &str);

    fn write_short(&mut self, value: i16);

    fn write_int(&mut self, value: i32);

    fn write_long(&mut self, value: i64);

    fn write_float(&mut self, value: f32);

    fn write_double(&mut self, value: f64);

    fn write_binary(&mut self, bytes: &[u8], length: usize);

    // TODO Decimal type
    // fn write_decimal(&mut self, pos: i32, value: f64);

    // TODO Timestamp type
    // fn write_timestamp_ntz(&mut self, pos: i32, value: i64);

    // TODO Timestamp type
    // fn write_timestamp_ltz(&mut self, pos: i32, value: i64);

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
    // TODO Decimal, Date, TimeWithoutTimeZone, TimestampWithoutTimeZone, TimestampWithLocalTimeZone, Array, Row
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
                writer.write_char(v, v.len());
            }
            (InnerValueWriter::String, Datum::String(v)) => {
                writer.write_string(v);
            }
            (InnerValueWriter::Boolean, Datum::Bool(v)) => {
                writer.write_boolean(*v);
            }
            (InnerValueWriter::Binary, Datum::Blob(v)) => {
                writer.write_binary(v.as_ref(), v.len());
            }
            (InnerValueWriter::Binary, Datum::BorrowedBlob(v)) => {
                writer.write_binary(v.as_ref(), v.len());
            }
            (InnerValueWriter::Bytes, Datum::Blob(v)) => {
                writer.write_bytes(v.as_ref());
            }
            (InnerValueWriter::Bytes, Datum::BorrowedBlob(v)) => {
                writer.write_bytes(v.as_ref());
            }
            (InnerValueWriter::TinyInt, Datum::Int8(v)) => {
                writer.write_byte(*v as u8);
            }
            (InnerValueWriter::SmallInt, Datum::Int16(v)) => {
                writer.write_short(*v);
            }
            (InnerValueWriter::Int, Datum::Int32(v)) => {
                writer.write_int(*v);
            }
            (InnerValueWriter::BigInt, Datum::Int64(v)) => {
                writer.write_long(*v);
            }
            (InnerValueWriter::Float, Datum::Float32(v)) => {
                writer.write_float(v.into_inner());
            }
            (InnerValueWriter::Double, Datum::Float64(v)) => {
                writer.write_double(v.into_inner());
            }
            _ => {
                return Err(IllegalArgument {
                    message: format!("{:?} used to write value {:?}", self, value),
                });
            }
        }
        Ok(())
    }
}
