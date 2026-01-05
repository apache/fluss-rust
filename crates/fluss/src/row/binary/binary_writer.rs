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

use crate::error::Error::{IllegalArgument, IoUnsupported};
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

/// Accessor for writing the fields/elements of a binary writer during runtime, the
/// fields/elements must be written in the order.
pub trait ValueWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, pos: usize, value: &Datum) -> Result<()>;
}

#[allow(dead_code)]
impl dyn ValueWriter {
    /// Creates an accessor for setting the elements of a binary writer during runtime.
    pub fn create_value_writer(
        element_type: &DataType,
        binary_row_format: &BinaryRowFormat,
    ) -> Result<Box<dyn ValueWriter>> {
        let value_writer =
            Self::create_not_null_value_writer(element_type, Some(binary_row_format))?;
        if !element_type.is_nullable() {
            Ok(value_writer)
        } else {
            Ok(Box::new(NullWriter {
                delegate: value_writer,
            }))
        }
    }

    /// Creates an accessor for setting the elements of a binary writer during runtime.
    pub fn create_not_null_value_writer(
        element_type: &DataType,
        _: Option<&BinaryRowFormat>,
    ) -> Result<Box<dyn ValueWriter>> {
        match element_type {
            DataType::Char(_) => Ok(Box::new(CharWriter)),
            DataType::String(_) => Ok(Box::new(StringWriter)),
            DataType::Boolean(_) => Ok(Box::new(BoolWriter)),
            DataType::Binary(_) => Ok(Box::new(BinaryValueWriter)),
            DataType::Bytes(_) => Ok(Box::new(BytesWriter)),
            // TODO DECIMAL
            DataType::TinyInt(_) => Ok(Box::new(TinyIntWriter)),
            DataType::SmallInt(_) => Ok(Box::new(SmallIntWriter)),
            DataType::Int(_) => Ok(Box::new(IntWriter)),
            // TODO DATE
            // TODO TIME_WITHOUT_TIME_ZONE
            DataType::BigInt(_) => Ok(Box::new(LongWriter)),
            DataType::Float(_) => Ok(Box::new(FloatWriter)),
            DataType::Double(_) => Ok(Box::new(DoubleWriter)),
            // TODO TIMESTAMP_WITHOUT_TIME_ZONE
            // TODO TIMESTAMP_WITH_LOCAL_TIME_ZONE
            // TODO ARRAY
            DataType::Map(_) => Err(IoUnsupported {
                message: "Map type is not supported yet. Will be added in Issue #1973.".to_string(),
            }),
            // TODO ROW
            _ => Err(IllegalArgument {
                message: format!("Type {} is not supported yet", element_type),
            }),
        }
    }
}

#[derive(Default)]
struct CharWriter;
impl ValueWriter for CharWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::String(v) = value {
            writer.write_char(v, v.len());
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("CharWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct StringWriter;
impl ValueWriter for StringWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::String(v) = value {
            writer.write_string(v);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("StringWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct BoolWriter;
impl ValueWriter for BoolWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Bool(v) = value {
            writer.write_boolean(*v);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("BoolWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct BinaryValueWriter;
impl ValueWriter for BinaryValueWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        match value {
            Datum::Blob(v) => {
                writer.write_binary(v.as_ref(), v.len());
                Ok(())
            }
            Datum::BorrowedBlob(v) => {
                writer.write_binary(v.as_ref(), v.len());
                Ok(())
            }
            _ => Err(IllegalArgument {
                message: format!("BinaryValueWriter used to write value: {:?}", value),
            }),
        }
    }
}

#[derive(Default)]
struct BytesWriter;
impl ValueWriter for BytesWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        match value {
            Datum::Blob(v) => {
                writer.write_bytes(v.as_ref());
                Ok(())
            }
            Datum::BorrowedBlob(v) => {
                writer.write_bytes(v.as_ref());
                Ok(())
            }
            value => Err(IllegalArgument {
                message: format!("BytesWriter used to write value: {:?}", value),
            }),
        }
    }
}

// TODO DecimalWriter

#[derive(Default)]
struct TinyIntWriter;
impl ValueWriter for TinyIntWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Int8(v) = value {
            writer.write_byte(*v as u8);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("TinyIntWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct SmallIntWriter;
impl ValueWriter for SmallIntWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Int16(v) = value {
            writer.write_short(*v);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("SmallIntWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct IntWriter;
impl ValueWriter for IntWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Int32(v) = value {
            writer.write_int(*v);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("IntWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct LongWriter;
impl ValueWriter for LongWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Int64(v) = value {
            writer.write_long(*v);
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("LongWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct FloatWriter;
impl ValueWriter for FloatWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Float32(v) = value {
            writer.write_float(v.into_inner());
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("FloatWriter used to write value: {:?}", value),
        })
    }
}

#[derive(Default)]
struct DoubleWriter;
impl ValueWriter for DoubleWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, _pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Float64(v) = value {
            writer.write_double(v.into_inner());
            return Ok(());
        }

        Err(IllegalArgument {
            message: format!("DoubleWriter used to write value: {:?}", value),
        })
    }
}

// TODO TIMESTAMP_NTZ writer
// TODO TIMESTAMP_LTZ writer
// TODO ARRAY writer
// TODO ROW writer

struct NullWriter {
    delegate: Box<dyn ValueWriter>,
}
impl ValueWriter for NullWriter {
    fn write_value(&self, writer: &mut dyn BinaryWriter, pos: usize, value: &Datum) -> Result<()> {
        if let Datum::Null = value {
            writer.set_null_at(pos);
            Ok(())
        } else {
            self.delegate.write_value(writer, pos, value)
        }
    }
}
