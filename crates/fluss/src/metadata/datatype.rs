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

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean(v) => write!(f, "{v}"),
            DataType::TinyInt(v) => write!(f, "{v}"),
            DataType::SmallInt(v) => write!(f, "{v}"),
            DataType::Int(v) => write!(f, "{v}"),
            DataType::BigInt(v) => write!(f, "{v}"),
            DataType::Float(v) => write!(f, "{v}"),
            DataType::Double(v) => write!(f, "{v}"),
            DataType::Char(v) => write!(f, "{v}"),
            DataType::String(v) => write!(f, "{v}"),
            DataType::Decimal(v) => write!(f, "{v}"),
            DataType::Date(v) => write!(f, "{v}"),
            DataType::Time(v) => write!(f, "{v}"),
            DataType::Timestamp(v) => write!(f, "{v}"),
            DataType::TimestampLTz(v) => write!(f, "{v}"),
            DataType::Bytes(v) => write!(f, "{v}"),
            DataType::Binary(v) => write!(f, "{v}"),
            DataType::Array(v) => write!(f, "{v}"),
            DataType::Map(v) => write!(f, "{v}"),
            DataType::Row(v) => write!(f, "{v}"),
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

impl Display for BooleanType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BOOLEAN")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for TinyIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TINYINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for SmallIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMALLINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for IntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for BigIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BIGINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for FloatType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FLOAT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for DoubleType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DOUBLE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for StringType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "STRING")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for DecimalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECIMAL({}, {})", self.precision, self.scale)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for DateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DATE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for TimeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIME({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for TimestampType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for TimestampLTzType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP_LTZ({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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
    pub const fn new() -> Self {
        Self::with_nullable(true)
    }

    pub const fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for BytesType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BYTES")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

impl Display for BinaryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BINARY({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

    pub fn get_element_type(&self) -> &DataType {
        &self.element_type
    }
}

impl Display for ArrayType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ARRAY<{}>", self.element_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

    pub fn key_type(&self) -> &DataType {
        &self.key_type
    }

    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }
}

impl Display for MapType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAP<{}, {}>", self.key_type, self.value_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
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

    pub fn get_field_index(&self, field_name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field_name)
    }

    pub fn field_types(&self) -> impl Iterator<Item = &DataType> + '_ {
        self.fields.iter().map(|f| &f.data_type)
    }

    pub fn get_field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    pub fn project(&self, project_field_positions: &[usize]) -> Result<RowType> {
        Ok(RowType::with_nullable(
            self.nullable,
            project_field_positions
                .iter()
                .map(|pos| {
                    self.fields
                        .get(*pos)
                        .cloned()
                        .ok_or_else(|| IllegalArgument {
                            message: format!("invalid field position: {}", *pos),
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    #[cfg(test)]
    pub fn with_data_types(data_types: Vec<DataType>) -> Self {
        let mut fields: Vec<DataField> = Vec::new();
        data_types.iter().enumerate().for_each(|(idx, data_type)| {
            fields.push(DataField::new(format!("f{idx}"), data_type.clone(), None));
        });

        Self::with_nullable(true, fields)
    }

    #[cfg(test)]
    pub fn with_data_types_and_field_names(
        data_types: Vec<DataType>,
        field_names: Vec<&str>,
    ) -> Self {
        let fields = data_types
            .into_iter()
            .zip(field_names)
            .map(|(data_type, field_name)| {
                DataField::new(field_name.to_string(), data_type.clone(), None)
            })
            .collect::<Vec<_>>();

        Self::with_nullable(true, fields)
    }
}

impl Display for RowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROW<")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}")?;
        }
        write!(f, ">")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

pub struct DataTypes;

impl DataTypes {
    pub fn binary(length: usize) -> DataType {
        DataType::Binary(BinaryType::new(length))
    }

    pub const fn bytes() -> DataType {
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

impl Display for DataField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

#[test]
fn test_boolean_display() {
    assert_eq!(BooleanType::new().to_string(), "BOOLEAN");
    assert_eq!(
        BooleanType::with_nullable(false).to_string(),
        "BOOLEAN NOT NULL"
    );
}

#[test]
fn test_tinyint_display() {
    assert_eq!(TinyIntType::new().to_string(), "TINYINT");
    assert_eq!(
        TinyIntType::with_nullable(false).to_string(),
        "TINYINT NOT NULL"
    );
}

#[test]
fn test_smallint_display() {
    assert_eq!(SmallIntType::new().to_string(), "SMALLINT");
    assert_eq!(
        SmallIntType::with_nullable(false).to_string(),
        "SMALLINT NOT NULL"
    );
}

#[test]
fn test_int_display() {
    assert_eq!(IntType::new().to_string(), "INT");
    assert_eq!(IntType::with_nullable(false).to_string(), "INT NOT NULL");
}

#[test]
fn test_bigint_display() {
    assert_eq!(BigIntType::new().to_string(), "BIGINT");
    assert_eq!(
        BigIntType::with_nullable(false).to_string(),
        "BIGINT NOT NULL"
    );
}

#[test]
fn test_float_display() {
    assert_eq!(FloatType::new().to_string(), "FLOAT");
    assert_eq!(
        FloatType::with_nullable(false).to_string(),
        "FLOAT NOT NULL"
    );
}

#[test]
fn test_double_display() {
    assert_eq!(DoubleType::new().to_string(), "DOUBLE");
    assert_eq!(
        DoubleType::with_nullable(false).to_string(),
        "DOUBLE NOT NULL"
    );
}

#[test]
fn test_string_display() {
    assert_eq!(StringType::new().to_string(), "STRING");
    assert_eq!(
        StringType::with_nullable(false).to_string(),
        "STRING NOT NULL"
    );
}

#[test]
fn test_date_display() {
    assert_eq!(DateType::new().to_string(), "DATE");
    assert_eq!(DateType::with_nullable(false).to_string(), "DATE NOT NULL");
}

#[test]
fn test_bytes_display() {
    assert_eq!(BytesType::new().to_string(), "BYTES");
    assert_eq!(
        BytesType::with_nullable(false).to_string(),
        "BYTES NOT NULL"
    );
}

#[test]
fn test_char_display() {
    assert_eq!(CharType::new(10).to_string(), "CHAR(10)");
    assert_eq!(
        CharType::with_nullable(20, false).to_string(),
        "CHAR(20) NOT NULL"
    );
}

#[test]
fn test_decimal_display() {
    assert_eq!(DecimalType::new(10, 2).to_string(), "DECIMAL(10, 2)");
    assert_eq!(
        DecimalType::with_nullable(false, 38, 10).to_string(),
        "DECIMAL(38, 10) NOT NULL"
    );
}

#[test]
fn test_time_display() {
    assert_eq!(TimeType::new(0).to_string(), "TIME(0)");
    assert_eq!(TimeType::new(3).to_string(), "TIME(3)");
    assert_eq!(
        TimeType::with_nullable(false, 9).to_string(),
        "TIME(9) NOT NULL"
    );
}

#[test]
fn test_timestamp_display() {
    assert_eq!(TimestampType::new(6).to_string(), "TIMESTAMP(6)");
    assert_eq!(TimestampType::new(0).to_string(), "TIMESTAMP(0)");
    assert_eq!(
        TimestampType::with_nullable(false, 9).to_string(),
        "TIMESTAMP(9) NOT NULL"
    );
}

#[test]
fn test_timestamp_ltz_display() {
    assert_eq!(TimestampLTzType::new(6).to_string(), "TIMESTAMP_LTZ(6)");
    assert_eq!(TimestampLTzType::new(3).to_string(), "TIMESTAMP_LTZ(3)");
    assert_eq!(
        TimestampLTzType::with_nullable(false, 9).to_string(),
        "TIMESTAMP_LTZ(9) NOT NULL"
    );
}

#[test]
fn test_binary_display() {
    assert_eq!(BinaryType::new(100).to_string(), "BINARY(100)");
    assert_eq!(
        BinaryType::with_nullable(false, 256).to_string(),
        "BINARY(256) NOT NULL"
    );
}

#[test]
fn test_array_display() {
    let array_type = ArrayType::new(DataTypes::int());
    assert_eq!(array_type.to_string(), "ARRAY<INT>");

    let array_type_non_null = ArrayType::with_nullable(false, DataTypes::string());
    assert_eq!(array_type_non_null.to_string(), "ARRAY<STRING> NOT NULL");

    let nested_array = ArrayType::new(DataTypes::array(DataTypes::int()));
    assert_eq!(nested_array.to_string(), "ARRAY<ARRAY<INT>>");
}

#[test]
fn test_map_display() {
    let map_type = MapType::new(DataTypes::string(), DataTypes::int());
    assert_eq!(map_type.to_string(), "MAP<STRING, INT>");

    let map_type_non_null = MapType::with_nullable(false, DataTypes::int(), DataTypes::string());
    assert_eq!(map_type_non_null.to_string(), "MAP<INT, STRING> NOT NULL");

    let nested_map = MapType::new(
        DataTypes::string(),
        DataTypes::map(DataTypes::int(), DataTypes::boolean()),
    );
    assert_eq!(nested_map.to_string(), "MAP<STRING, MAP<INT, BOOLEAN>>");
}

#[test]
fn test_row_display() {
    let fields = vec![
        DataTypes::field("id".to_string(), DataTypes::int()),
        DataTypes::field("name".to_string(), DataTypes::string()),
    ];
    let row_type = RowType::new(fields);
    assert_eq!(row_type.to_string(), "ROW<id INT, name STRING>");

    let fields_non_null = vec![DataTypes::field("age".to_string(), DataTypes::bigint())];
    let row_type_non_null = RowType::with_nullable(false, fields_non_null);
    assert_eq!(row_type_non_null.to_string(), "ROW<age BIGINT> NOT NULL");
}

#[test]
fn test_datatype_display() {
    assert_eq!(DataTypes::boolean().to_string(), "BOOLEAN");
    assert_eq!(DataTypes::int().to_string(), "INT");
    assert_eq!(DataTypes::string().to_string(), "STRING");
    assert_eq!(DataTypes::char(50).to_string(), "CHAR(50)");
    assert_eq!(DataTypes::decimal(10, 2).to_string(), "DECIMAL(10, 2)");
    assert_eq!(DataTypes::time_with_precision(3).to_string(), "TIME(3)");
    assert_eq!(
        DataTypes::timestamp_with_precision(6).to_string(),
        "TIMESTAMP(6)"
    );
    assert_eq!(
        DataTypes::timestamp_ltz_with_precision(9).to_string(),
        "TIMESTAMP_LTZ(9)"
    );
    assert_eq!(DataTypes::array(DataTypes::int()).to_string(), "ARRAY<INT>");
    assert_eq!(
        DataTypes::map(DataTypes::string(), DataTypes::int()).to_string(),
        "MAP<STRING, INT>"
    );
}

#[test]
fn test_datafield_display() {
    let field = DataTypes::field("user_id".to_string(), DataTypes::bigint());
    assert_eq!(field.to_string(), "user_id BIGINT");

    let field2 = DataTypes::field("email".to_string(), DataTypes::string());
    assert_eq!(field2.to_string(), "email STRING");

    let field3 = DataTypes::field("score".to_string(), DataTypes::decimal(10, 2));
    assert_eq!(field3.to_string(), "score DECIMAL(10, 2)");
}

#[test]
fn test_complex_nested_display() {
    let row_type = DataTypes::row(vec![
        DataTypes::field("id".to_string(), DataTypes::int()),
        DataTypes::field("tags".to_string(), DataTypes::array(DataTypes::string())),
        DataTypes::field(
            "metadata".to_string(),
            DataTypes::map(DataTypes::string(), DataTypes::string()),
        ),
    ]);
    assert_eq!(
        row_type.to_string(),
        "ROW<id INT, tags ARRAY<STRING>, metadata MAP<STRING, STRING>>"
    );
}

#[test]
fn test_non_nullable_datatype() {
    let nullable_int = DataTypes::int();
    assert_eq!(nullable_int.to_string(), "INT");

    let non_nullable_int = nullable_int.as_non_nullable();
    assert_eq!(non_nullable_int.to_string(), "INT NOT NULL");
}

#[test]
fn test_deeply_nested_types() {
    let nested = DataTypes::array(DataTypes::map(
        DataTypes::string(),
        DataTypes::row(vec![
            DataTypes::field("x".to_string(), DataTypes::int()),
            DataTypes::field("y".to_string(), DataTypes::int()),
        ]),
    ));
    assert_eq!(nested.to_string(), "ARRAY<MAP<STRING, ROW<x INT, y INT>>>");
}
