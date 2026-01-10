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

use crate::error::Error::RowConvertError;
use crate::error::Result;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder,
};
use jiff::ToSpan;
use ordered_float::OrderedFloat;
use parse_display::Display;
use ref_cast::RefCast;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[allow(dead_code)]
const THIRTY_YEARS_MICROSECONDS: i64 = 946_684_800_000_000;

#[derive(Debug, Clone, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Datum<'a> {
    #[display("null")]
    Null,
    #[display("{0}")]
    Bool(bool),
    #[display("{0}")]
    Int8(i8),
    #[display("{0}")]
    Int16(i16),
    #[display("{0}")]
    Int32(i32),
    #[display("{0}")]
    Int64(i64),
    #[display("{0}")]
    Float32(F32),
    #[display("{0}")]
    Float64(F64),
    #[display("'{0}'")]
    String(&'a str),
    #[display("{0}")]
    Blob(Blob),
    #[display("{:?}")]
    BorrowedBlob(&'a [u8]),
    #[display("{0}")]
    Decimal(Decimal),
    #[display("{0}")]
    Date(Date),
    #[display("{0}")]
    Timestamp(Timestamp),
    #[display("{0}")]
    TimestampTz(TimestampLtz),
}

impl Datum<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s,
            _ => panic!("not a string: {self:?}"),
        }
    }

    pub fn as_blob(&self) -> &[u8] {
        match self {
            Self::Blob(blob) => blob.as_ref(),
            Self::BorrowedBlob(blob) => blob,
            _ => panic!("not a blob: {self:?}"),
        }
    }
}

// ----------- implement from
impl<'a> From<i32> for Datum<'a> {
    #[inline]
    fn from(i: i32) -> Datum<'a> {
        Datum::Int32(i)
    }
}

impl<'a> From<i64> for Datum<'a> {
    #[inline]
    fn from(i: i64) -> Datum<'a> {
        Datum::Int64(i)
    }
}

impl<'a> From<i8> for Datum<'a> {
    #[inline]
    fn from(i: i8) -> Datum<'a> {
        Datum::Int8(i)
    }
}

impl<'a> From<i16> for Datum<'a> {
    #[inline]
    fn from(i: i16) -> Datum<'a> {
        Datum::Int16(i)
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    #[inline]
    fn from(s: &'a str) -> Datum<'a> {
        Datum::String(s)
    }
}

impl From<Option<&()>> for Datum<'_> {
    fn from(_: Option<&()>) -> Self {
        Self::Null
    }
}

impl<'a> From<f32> for Datum<'a> {
    #[inline]
    fn from(f: f32) -> Datum<'a> {
        Datum::Float32(F32::from(f))
    }
}

impl<'a> From<f64> for Datum<'a> {
    #[inline]
    fn from(f: f64) -> Datum<'a> {
        Datum::Float64(F64::from(f))
    }
}

impl TryFrom<&Datum<'_>> for i32 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int32(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i16 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int16(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i64 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int64(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for f32 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Float32(f) => Ok(f.into_inner()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for f64 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Float64(f) => Ok(f.into_inner()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for bool {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Bool(b) => Ok(*b),
            _ => Err(()),
        }
    }
}

impl<'a> TryFrom<&Datum<'a>> for &'a str {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum<'a>) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::String(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i8 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int8(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl<'a> From<bool> for Datum<'a> {
    #[inline]
    fn from(b: bool) -> Datum<'a> {
        Datum::Bool(b)
    }
}

pub trait ToArrow {
    fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

impl Datum<'_> {
    pub fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        macro_rules! append_null_to_arrow {
            ($builder_type:ty) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    b.append_null();
                    return Ok(());
                }
            };
        }

        macro_rules! append_value_to_arrow {
            ($builder_type:ty, $value:expr) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    b.append_value($value);
                    return Ok(());
                }
            };
        }

        match self {
            Datum::Null => {
                append_null_to_arrow!(Int8Builder);
                append_null_to_arrow!(BooleanBuilder);
                append_null_to_arrow!(Int16Builder);
                append_null_to_arrow!(Int32Builder);
                append_null_to_arrow!(Int64Builder);
                append_null_to_arrow!(Float32Builder);
                append_null_to_arrow!(Float64Builder);
                append_null_to_arrow!(StringBuilder);
                append_null_to_arrow!(BinaryBuilder);
            }
            Datum::Bool(v) => append_value_to_arrow!(BooleanBuilder, *v),
            Datum::Int8(v) => append_value_to_arrow!(Int8Builder, *v),
            Datum::Int16(v) => append_value_to_arrow!(Int16Builder, *v),
            Datum::Int32(v) => append_value_to_arrow!(Int32Builder, *v),
            Datum::Int64(v) => append_value_to_arrow!(Int64Builder, *v),
            Datum::Float32(v) => append_value_to_arrow!(Float32Builder, v.into_inner()),
            Datum::Float64(v) => append_value_to_arrow!(Float64Builder, v.into_inner()),
            Datum::String(v) => append_value_to_arrow!(StringBuilder, *v),
            Datum::Blob(v) => append_value_to_arrow!(BinaryBuilder, v.as_ref()),
            Datum::BorrowedBlob(v) => append_value_to_arrow!(BinaryBuilder, *v),
            Datum::Decimal(_) | Datum::Date(_) | Datum::Timestamp(_) | Datum::TimestampTz(_) => {
                return Err(RowConvertError {
                    message: format!(
                        "Type {:?} is not yet supported for Arrow conversion",
                        std::mem::discriminant(self)
                    ),
                });
            }
        }

        Err(RowConvertError {
            message: format!(
                "Cannot append {:?} to builder of type {}",
                self,
                std::any::type_name_of_val(builder)
            ),
        })
    }
}

macro_rules! impl_to_arrow {
    ($ty:ty, $variant:ident) => {
        impl ToArrow for $ty {
            fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$variant>() {
                    b.append_value(*self);
                    Ok(())
                } else {
                    Err(RowConvertError {
                        message: format!(
                            "Cannot cast {} to {} builder",
                            stringify!($ty),
                            stringify!($variant)
                        ),
                    })
                }
            }
        }
    };
}

impl_to_arrow!(i8, Int8Builder);
impl_to_arrow!(i16, Int16Builder);
impl_to_arrow!(i32, Int32Builder);
impl_to_arrow!(f32, Float32Builder);
impl_to_arrow!(f64, Float64Builder);
impl_to_arrow!(&str, StringBuilder);

pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;
#[allow(dead_code)]
pub type Str = Box<str>;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, Default)]
pub struct Blob(Box<[u8]>);

impl Deref for Blob {
    type Target = BlobRef;

    fn deref(&self) -> &Self::Target {
        BlobRef::new(&self.0)
    }
}

impl BlobRef {
    pub fn new(bytes: &[u8]) -> &Self {
        // SAFETY: `&BlobRef` and `&[u8]` have the same layout.
        BlobRef::ref_cast(bytes)
    }
}

/// A slice of a blob.
#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, RefCast, Hash)]
pub struct BlobRef([u8]);

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl AsRef<[u8]> for BlobRef {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BlobRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Date(i32);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Timestamp(i64);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct TimestampLtz(i64);

impl From<Vec<u8>> for Blob {
    fn from(vec: Vec<u8>) -> Self {
        Blob(vec.into())
    }
}

impl<'a> From<&'a [u8]> for Datum<'a> {
    fn from(bytes: &'a [u8]) -> Datum<'a> {
        Datum::BorrowedBlob(bytes)
    }
}

const UNIX_EPOCH_DAY: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

impl Date {
    pub const fn new(inner: i32) -> Self {
        Date(inner)
    }

    /// Get the inner value of date type
    pub fn get_inner(&self) -> i32 {
        self.0
    }

    pub fn year(&self) -> i16 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.year()
    }
    pub fn month(&self) -> i8 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.month()
    }

    pub fn day(&self) -> i8 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.day()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Builder, StringBuilder};

    #[test]
    fn datum_accessors_and_conversions() {
        let datum = Datum::String("value");
        assert_eq!(datum.as_str(), "value");
        assert!(!datum.is_null());

        let blob = Blob::from(vec![1, 2, 3]);
        let datum = Datum::Blob(blob);
        assert_eq!(datum.as_blob(), &[1, 2, 3]);

        assert!(Datum::Null.is_null());

        let datum = Datum::Int32(42);
        let value: i32 = (&datum).try_into().unwrap();
        assert_eq!(value, 42);
        let value: std::result::Result<i16, _> = (&datum).try_into();
        assert!(value.is_err());
    }

    #[test]
    fn datum_append_to_builder() {
        let mut builder = Int32Builder::new();
        Datum::Null.append_to(&mut builder).unwrap();
        Datum::Int32(5).append_to(&mut builder).unwrap();
        let array = builder.finish();
        assert!(array.is_null(0));
        assert_eq!(array.value(1), 5);

        let mut builder = StringBuilder::new();
        let err = Datum::Int32(1).append_to(&mut builder).unwrap_err();
        assert!(matches!(err, crate::error::Error::RowConvertError { .. }));

        let mut builder = Int32Builder::new();
        let err = Datum::Date(Date::new(0))
            .append_to(&mut builder)
            .unwrap_err();
        assert!(matches!(err, crate::error::Error::RowConvertError { .. }));
    }

    #[test]
    #[should_panic]
    fn datum_as_str_panics_on_non_string() {
        let _ = Datum::Int32(1).as_str();
    }

    #[test]
    #[should_panic]
    fn datum_as_blob_panics_on_non_blob() {
        let _ = Datum::Int16(1).as_blob();
    }

    #[test]
    fn date_components() {
        let date = Date::new(0);
        assert_eq!(date.get_inner(), 0);
        assert_eq!(date.year(), 1970);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 1);
    }
}
