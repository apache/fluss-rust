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

pub enum FieldGetter {
    Nullable(InnerFieldGetter),
    NonNullable(InnerFieldGetter),
}
impl FieldGetter {
    pub fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        match self {
            FieldGetter::Nullable(getter) => {
                if row.is_null_at(getter.pos()) {
                    Datum::Null
                } else {
                    getter.get_field(row)
                }
            }
            FieldGetter::NonNullable(getter) => getter.get_field(row),
        }
    }

    pub fn create(data_type: &DataType, pos: usize) -> FieldGetter {
        let inner_field_getter = match data_type {
            DataType::Char(t) => InnerFieldGetter::Char {
                pos,
                len: t.length() as usize,
            },
            DataType::String(_) => InnerFieldGetter::String { pos },
            DataType::Boolean(_) => InnerFieldGetter::Bool { pos },
            DataType::Binary(t) => InnerFieldGetter::Binary {
                pos,
                len: t.length(),
            },
            DataType::Bytes(_) => InnerFieldGetter::Bytes { pos },
            DataType::TinyInt(_) => InnerFieldGetter::TinyInt { pos },
            DataType::SmallInt(_) => InnerFieldGetter::SmallInt { pos },
            DataType::Int(_) => InnerFieldGetter::Int { pos },
            DataType::BigInt(_) => InnerFieldGetter::BigInt { pos },
            DataType::Float(_) => InnerFieldGetter::Float { pos },
            DataType::Double(_) => InnerFieldGetter::Double { pos },
            _ => unimplemented!("DataType {:?} is currently unimplemented", data_type),
        };

        if data_type.is_nullable() {
            Self::Nullable(inner_field_getter)
        } else {
            Self::NonNullable(inner_field_getter)
        }
    }
}

pub enum InnerFieldGetter {
    Char { pos: usize, len: usize },
    String { pos: usize },
    Bool { pos: usize },
    Binary { pos: usize, len: usize },
    Bytes { pos: usize },
    TinyInt { pos: usize },
    SmallInt { pos: usize },
    Int { pos: usize },
    BigInt { pos: usize },
    Float { pos: usize },
    Double { pos: usize },
}

impl InnerFieldGetter {
    pub fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Datum<'a> {
        match self {
            InnerFieldGetter::Char { pos, len } => Datum::String(row.get_char(*pos, *len)),
            InnerFieldGetter::String { pos } => Datum::from(row.get_string(*pos)),
            InnerFieldGetter::Bool { pos } => Datum::from(row.get_boolean(*pos)),
            InnerFieldGetter::Binary { pos, len } => Datum::from(row.get_binary(*pos, *len)),
            InnerFieldGetter::Bytes { pos } => Datum::from(row.get_bytes(*pos)),
            InnerFieldGetter::TinyInt { pos } => Datum::from(row.get_byte(*pos)),
            InnerFieldGetter::SmallInt { pos } => Datum::from(row.get_short(*pos)),
            InnerFieldGetter::Int { pos } => Datum::from(row.get_int(*pos)),
            InnerFieldGetter::BigInt { pos } => Datum::from(row.get_long(*pos)),
            InnerFieldGetter::Float { pos } => Datum::from(row.get_float(*pos)),
            InnerFieldGetter::Double { pos } => Datum::from(row.get_double(*pos)),
            //TODO Decimal, Date, Time, Timestamp, TimestampLTZ, Array, Map, Row
        }
    }

    pub fn pos(&self) -> usize {
        match self {
            Self::Char { pos, .. }
            | Self::String { pos }
            | Self::Bool { pos }
            | Self::Binary { pos, .. }
            | Self::Bytes { pos }
            | Self::TinyInt { pos }
            | Self::SmallInt { pos, .. }
            | Self::Int { pos }
            | Self::BigInt { pos }
            | Self::Float { pos, .. }
            | Self::Double { pos } => *pos,
        }
    }
}
