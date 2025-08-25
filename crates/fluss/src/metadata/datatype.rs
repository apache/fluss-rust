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

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Data type for Fluss table.
/// Impl reference: <todo: link>
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean(BooleanType),
    TinyInt(TinyIntType),
    SmallInt(SmallIntType),
    Int(IntType),
    BigInt(BigIntType),
    Float(FloatType),
    Double(DoubleType),
    Char(CharType),
    String(StringType),
    Decimal(DecimalType),
    Date(DateType),
    Time(TimeType),
    Timestamp(TimestampType),
    TimestampLTz(TimestampLTzType),
    Bytes(BytesType),
    Binary(BinaryType),
    Array(ArrayType),
    Map(MapType),
    Row(RowType),
}

impl DataType {
    pub fn is_nullable(&self) -> bool {
        match self {
            DataType::Boolean(v) => v.nullable,
            DataType::TinyInt(v) => v.nullable,
            DataType::SmallInt(v) => v.nullable,
            DataType::Int(v) => v.nullable,
            DataType::BigInt(v) => v.nullable,
            DataType::Decimal(v) => v.nullable,
            DataType::Double(v) => v.nullable,
            DataType::Float(v) => v.nullable,
            DataType::Binary(v) => v.nullable,
            DataType::Char(v) => v.nullable,
            DataType::String(v) => v.nullable,
            DataType::Date(v) => v.nullable,
            DataType::TimestampLTz(v) => v.nullable,
            DataType::Time(v) => v.nullable,
            DataType::Timestamp(v) => v.nullable,
            DataType::Array(v) => v.nullable,
            DataType::Map(v) => v.nullable,
            DataType::Row(v) => v.nullable,
            DataType::Bytes(v) => v.nullable,
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        match self {
            DataType::Boolean(v) => DataType::Boolean(v.as_non_nullable()),
            DataType::TinyInt(v) => DataType::TinyInt(v.as_non_nullable()),
            DataType::SmallInt(v) => DataType::SmallInt(v.as_non_nullable()),
            DataType::Int(v) => DataType::Int(v.as_non_nullable()),
            DataType::BigInt(v) => DataType::BigInt(v.as_non_nullable()),
            DataType::Decimal(v) => DataType::Decimal(v.as_non_nullable()),
            DataType::Double(v) => DataType::Double(v.as_non_nullable()),
            DataType::Float(v) => DataType::Float(v.as_non_nullable()),
            DataType::Binary(v) => DataType::Binary(v.as_non_nullable()),
            DataType::Char(v) => DataType::Char(v.as_non_nullable()),
            DataType::String(v) => DataType::String(v.as_non_nullable()),
            DataType::Date(v) => DataType::Date(v.as_non_nullable()),
            DataType::TimestampLTz(v) => DataType::TimestampLTz(v.as_non_nullable()),
            DataType::Time(v) => DataType::Time(v.as_non_nullable()),
            DataType::Timestamp(v) => DataType::Timestamp(v.as_non_nullable()),
            DataType::Array(v) => DataType::Array(v.as_non_nullable()),
            DataType::Map(v) => DataType::Map(v.as_non_nullable()),
            DataType::Row(v) => DataType::Row(v.as_non_nullable()),
            DataType::Bytes(v) => DataType::Bytes(v.as_non_nullable()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BooleanType {
    nullable: bool,
}

impl Default for BooleanType {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TinyIntType {
    nullable: bool,
}

impl Default for TinyIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl TinyIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SmallIntType {
    nullable: bool,
}

impl Default for SmallIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl SmallIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IntType {
    nullable: bool,
}

impl Default for IntType {
    fn default() -> Self {
        Self::new()
    }
}

impl IntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BigIntType {
    nullable: bool,
}

impl Default for BigIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl BigIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FloatType {
    nullable: bool,
}

impl Default for FloatType {
    fn default() -> Self {
        Self::new()
    }
}

impl FloatType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DoubleType {
    nullable: bool,
}

impl Default for DoubleType {
    fn default() -> Self {
        Self::new()
    }
}

impl DoubleType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CharType {
    nullable: bool,
    length: u32,
}

impl CharType {
    pub fn new(length: u32) -> Self {
        Self::with_nullable(length, true)
    }

    pub fn with_nullable(length: u32, nullable: bool) -> Self {
        Self { nullable, length }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(self.length, false)
    }

    pub fn length(&self) -> u32 {
        self.length
    }
}

impl Display for CharType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CHAR({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StringType {
    nullable: bool,
}

impl Default for StringType {
    fn default() -> Self {
        Self::new()
    }
}

impl StringType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DecimalType {
    nullable: bool,
    precision: u32,
    scale: u32,
}

impl DecimalType {
    pub const MIN_PRECISION: u32 = 1;

    pub const MAX_PRECISION: u32 = 38;

    pub const DEFAULT_PRECISION: u32 = 10;

    pub const MIN_SCALE: u32 = 0;

    pub const DEFAULT_SCALE: u32 = 0;

    pub fn new(precision: u32, scale: u32) -> Self {
        Self::with_nullable(true, precision, scale)
    }

    pub fn with_nullable(nullable: bool, precision: u32, scale: u32) -> Self {
        DecimalType {
            nullable,
            precision,
            scale,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision, self.scale)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateType {
    nullable: bool,
}

impl Default for DateType {
    fn default() -> Self {
        Self::new()
    }
}

impl DateType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimeType {
    nullable: bool,
    precision: u32,
}

impl TimeType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }
}

impl TimeType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 0;

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimeType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampType {
    nullable: bool,
    precision: u32,
}

impl Default for TimestampType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }
}

impl TimestampType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimestampType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampLTzType {
    nullable: bool,
    precision: u32,
}

impl Default for TimestampLTzType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }
}

impl TimestampLTzType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimestampLTzType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BytesType {
    nullable: bool,
}

impl Default for BytesType {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BinaryType {
    nullable: bool,
    length: usize,
}

impl BinaryType {
    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = usize::MAX;

    pub const DEFAULT_LENGTH: usize = 1;

    pub fn new(length: usize) -> Self {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: usize) -> Self {
        Self { nullable, length }
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.length)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrayType {
    nullable: bool,
    element_type: Box<DataType>,
}

impl ArrayType {
    pub fn new(element_type: DataType) -> Self {
        Self::with_nullable(true, element_type)
    }

    pub fn with_nullable(nullable: bool, element_type: DataType) -> Self {
        Self {
            nullable,
            element_type: Box::new(element_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            element_type: self.element_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MapType {
    nullable: bool,
    key_type: Box<DataType>,
    value_type: Box<DataType>,
}

impl MapType {
    pub fn new(key_type: DataType, value_type: DataType) -> Self {
        Self::with_nullable(true, key_type, value_type)
    }

    pub fn with_nullable(nullable: bool, key_type: DataType, value_type: DataType) -> Self {
        Self {
            nullable,
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            key_type: self.key_type.clone(),
            value_type: self.value_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct RowType {
    nullable: bool,
    fields: Vec<DataField>,
}

impl RowType {
    pub const fn new(fields: Vec<DataField>) -> Self {
        Self::with_nullable(true, fields)
    }

    pub const fn with_nullable(nullable: bool, fields: Vec<DataField>) -> Self {
        Self { nullable, fields }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.fields.clone())
    }

    pub fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }
}

pub struct DataTypes;

impl DataTypes {
    pub fn binary(length: usize) -> DataType {
        DataType::Binary(BinaryType::new(length))
    }

    pub fn bytes() -> DataType {
        DataType::Bytes(BytesType::new())
    }

    pub fn boolean() -> DataType {
        DataType::Boolean(BooleanType::new())
    }

    pub fn int() -> DataType {
        DataType::Int(IntType::new())
    }

    /// Data type of a 1-byte signed integer with values from -128 to 127.
    pub fn tinyint() -> DataType {
        DataType::TinyInt(TinyIntType::new())
    }

    /// Data type of a 2-byte signed integer with values from -32,768 to 32,767.
    pub fn smallint() -> DataType {
        DataType::SmallInt(SmallIntType::new())
    }

    pub fn bigint() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    /// Data type of a 4-byte single precision floating point number.
    pub fn float() -> DataType {
        DataType::Float(FloatType::new())
    }

    /// Data type of an 8-byte double precision floating point number.
    pub fn double() -> DataType {
        DataType::Double(DoubleType::new())
    }

    pub fn char(length: u32) -> DataType {
        DataType::Char(CharType::new(length))
    }

    /// Data type of a variable-length character string.
    pub fn string() -> DataType {
        DataType::String(StringType::new())
    }

    /// Data type of a decimal number with fixed precision and scale `DECIMAL(p, s)` where
    /// `p` is the number of digits in a number (=precision) and `s` is the number of
    /// digits to the right of the decimal point in a number (=scale). `p` must have a value
    /// between 1 and 38 (both inclusive). `s` must have a value between 0 and `p` (both inclusive).
    pub fn decimal(precision: u32, scale: u32) -> DataType {
        DataType::Decimal(DecimalType::new(precision, scale))
    }

    pub fn date() -> DataType {
        DataType::Date(DateType::new())
    }

    /// Data type of a time WITHOUT time zone `TIME` with no fractional seconds by default.
    pub fn time() -> DataType {
        DataType::Time(TimeType::default())
    }

    /// Data type of a time WITHOUT time zone `TIME(p)` where `p` is the number of digits
    /// of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn time_with_precision(precision: u32) -> DataType {
        DataType::Time(TimeType::new(precision))
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP` with 6 digits of fractional
    /// seconds by default.
    pub fn timestamp() -> DataType {
        DataType::Timestamp(TimestampType::default())
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9
    /// (both inclusive).
    pub fn timestamp_with_precision(precision: u32) -> DataType {
        DataType::Timestamp(TimestampType::new(precision))
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE` with 6 digits of
    /// fractional seconds by default.
    pub fn timestamp_ltz() -> DataType {
        DataType::TimestampLTz(TimestampLTzType::default())
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn timestamp_ltz_with_precision(precision: u32) -> DataType {
        DataType::TimestampLTz(TimestampLTzType::new(precision))
    }

    /// Data type of an array of elements with same subtype.
    pub fn array(element: DataType) -> DataType {
        DataType::Array(ArrayType::new(element))
    }

    /// Data type of an associative array that maps keys to values.
    pub fn map(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Map(MapType::new(key_type, value_type))
    }

    /// Field definition with field name and data type.
    pub fn field(name: String, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
    }

    /// Field definition with field name, data type, and a description.
    pub fn field_with_description(
        name: String,
        data_type: DataType,
        description: String,
    ) -> DataField {
        DataField::new(name, data_type, Some(description))
    }

    /// Data type of a sequence of fields.
    pub fn row(fields: Vec<DataField>) -> DataType {
        DataType::Row(RowType::new(fields))
    }

    /// Data type of a sequence of fields with generated field names (f0, f1, f2, ...).
    pub fn row_from_types(field_types: Vec<DataType>) -> DataType {
        let fields = field_types
            .into_iter()
            .enumerate()
            .map(|(i, dt)| DataField::new(format!("f{i}"), dt, None))
            .collect();
        DataType::Row(RowType::new(fields))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataField {
    pub name: String,
    pub data_type: DataType,
    pub description: Option<String>,
}

impl DataField {
    pub fn new(name: String, data_type: DataType, description: Option<String>) -> DataField {
        DataField {
            name,
            data_type,
            description,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

// todo: implement display for datatype
