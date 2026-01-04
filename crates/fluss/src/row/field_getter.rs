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

use crate::metadata::DataType;
use crate::row::{Datum, InternalRow};

pub trait FieldGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a>;
}

struct CharGetter {
    field_position: usize,
    length: usize,
}
impl FieldGetter for CharGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_char(self.field_position, self.length))
    }
}

struct StringGetter {
    field_position: usize,
}

impl FieldGetter for StringGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_string(self.field_position))
    }
}

struct BooleanGetter {
    field_position: usize,
}

impl FieldGetter for BooleanGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_boolean(self.field_position))
    }
}

struct BinaryGetter {
    field_position: usize,
    length: usize,
}
impl FieldGetter for BinaryGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_binary(self.field_position, self.length))
    }
}

struct BytesGetter {
    field_position: usize,
}
impl FieldGetter for BytesGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_bytes(self.field_position))
    }
}

//TODO Decimal Getter

struct TinyIntGetter {
    field_position: usize,
}
impl FieldGetter for TinyIntGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_byte(self.field_position))
    }
}

struct SmallIntGetter {
    field_position: usize,
}
impl FieldGetter for SmallIntGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_short(self.field_position))
    }
}

struct IntGetter {
    field_position: usize,
}
impl FieldGetter for IntGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_int(self.field_position))
    }
}

struct BigIntGetter {
    field_position: usize,
}
impl FieldGetter for BigIntGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_long(self.field_position))
    }
}

struct FloatGetter {
    field_position: usize,
}
impl FieldGetter for FloatGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_float(self.field_position))
    }
}

struct DoubleGetter {
    field_position: usize,
}
impl FieldGetter for DoubleGetter {
    fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        Datum::from(row.get_double(self.field_position))
    }
}

#[allow(dead_code)]
impl dyn FieldGetter {
    pub fn create_field_getter(
        data_type: &DataType,
        field_position: usize,
    ) -> Box<dyn FieldGetter> {
        match data_type {
            DataType::Char(t) => Box::new(CharGetter {
                field_position,
                length: t.length() as usize,
            }),
            DataType::String(_) => Box::new(StringGetter { field_position }),
            DataType::Boolean(_) => Box::new(BooleanGetter { field_position }),
            DataType::Binary(t) => Box::new(BinaryGetter {
                field_position,
                length: t.length(),
            }),
            DataType::Bytes(_) => Box::new(BytesGetter { field_position }),
            DataType::TinyInt(_) => Box::new(TinyIntGetter { field_position }),
            DataType::SmallInt(_) => Box::new(SmallIntGetter { field_position }),
            DataType::Int(_) => Box::new(IntGetter { field_position }),
            DataType::BigInt(_) => Box::new(BigIntGetter { field_position }),
            DataType::Float(_) => Box::new(FloatGetter { field_position }),
            DataType::Double(_) => Box::new(DoubleGetter { field_position }),
            _ => unimplemented!(),
            // // TODO
            // DataType::Decimal(_) => {}
            // DataType::Date(_) => {}
            // DataType::Time(_) => {}
            // DataType::Timestamp(_) => {}
            // DataType::TimestampLTz(_) => {}
            // DataType::Array(_) => {}
            // DataType::Map(_) => {}
            // DataType::Row(_) => {}
        }
    }
}
