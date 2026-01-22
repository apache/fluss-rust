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
use crate::row::Decimal;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes as arrow_schema;
use jiff::ToSpan;
use ordered_float::OrderedFloat;
use parse_display::Display;
use serde::Serialize;
use std::borrow::Cow;

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
    String(Str<'a>),
    #[display("{:?}")]
    Blob(Blob<'a>),
    #[display("{0}")]
    Decimal(Decimal),
    #[display("{0}")]
    Date(Date),
    #[display("{0}")]
    Time(Time),
    #[display("{0}")]
    TimestampNtz(TimestampNtz),
    #[display("{0}")]
    TimestampLtz(TimestampLtz),
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
            _ => panic!("not a blob: {self:?}"),
        }
    }

    pub fn as_decimal(&self) -> &Decimal {
        match self {
            Self::Decimal(d) => d,
            _ => panic!("not a decimal: {self:?}"),
        }
    }

    pub fn as_date(&self) -> Date {
        match self {
            Self::Date(d) => *d,
            _ => panic!("not a date: {self:?}"),
        }
    }

    pub fn as_time(&self) -> Time {
        match self {
            Self::Time(t) => *t,
            _ => panic!("not a time: {self:?}"),
        }
    }

    pub fn as_timestamp_ntz(&self) -> TimestampNtz {
        match self {
            Self::TimestampNtz(ts) => *ts,
            _ => panic!("not a timestamp ntz: {self:?}"),
        }
    }

    pub fn as_timestamp_ltz(&self) -> TimestampLtz {
        match self {
            Self::TimestampLtz(ts) => *ts,
            _ => panic!("not a timestamp ltz: {self:?}"),
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

pub type Str<'a> = Cow<'a, str>;

impl<'a> From<String> for Datum<'a> {
    #[inline]
    fn from(s: String) -> Self {
        Datum::String(Cow::Owned(s))
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    #[inline]
    fn from(s: &'a str) -> Datum<'a> {
        Datum::String(Cow::Borrowed(s))
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

impl<'b, 'a: 'b> TryFrom<&'b Datum<'a>> for &'b str {
    type Error = ();

    #[inline]
    fn try_from(from: &'b Datum<'a>) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::String(s) => Ok(s.as_ref()),
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

impl TryFrom<&Datum<'_>> for Decimal {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Decimal(d) => Ok(d.clone()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for Date {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Date(d) => Ok(*d),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for Time {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Time(t) => Ok(*t),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for TimestampNtz {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::TimestampNtz(ts) => Ok(*ts),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for TimestampLtz {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::TimestampLtz(ts) => Ok(*ts),
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

impl<'a> From<Decimal> for Datum<'a> {
    #[inline]
    fn from(d: Decimal) -> Datum<'a> {
        Datum::Decimal(d)
    }
}

impl<'a> From<Date> for Datum<'a> {
    #[inline]
    fn from(d: Date) -> Datum<'a> {
        Datum::Date(d)
    }
}

impl<'a> From<Time> for Datum<'a> {
    #[inline]
    fn from(t: Time) -> Datum<'a> {
        Datum::Time(t)
    }
}

impl<'a> From<TimestampNtz> for Datum<'a> {
    #[inline]
    fn from(ts: TimestampNtz) -> Datum<'a> {
        Datum::TimestampNtz(ts)
    }
}

impl<'a> From<TimestampLtz> for Datum<'a> {
    #[inline]
    fn from(ts: TimestampLtz) -> Datum<'a> {
        Datum::TimestampLtz(ts)
    }
}

pub trait ToArrow {
    fn append_to(
        &self,
        builder: &mut dyn ArrayBuilder,
        data_type: &arrow_schema::DataType,
    ) -> Result<()>;
}

impl Datum<'_> {
    pub fn append_to(
        &self,
        builder: &mut dyn ArrayBuilder,
        data_type: &arrow_schema::DataType,
    ) -> Result<()> {
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
                append_null_to_arrow!(Decimal128Builder);
                append_null_to_arrow!(Date32Builder);
                append_null_to_arrow!(Time32SecondBuilder);
                append_null_to_arrow!(Time32MillisecondBuilder);
                append_null_to_arrow!(Time64MicrosecondBuilder);
                append_null_to_arrow!(Time64NanosecondBuilder);
                append_null_to_arrow!(TimestampSecondBuilder);
                append_null_to_arrow!(TimestampMillisecondBuilder);
                append_null_to_arrow!(TimestampMicrosecondBuilder);
                append_null_to_arrow!(TimestampNanosecondBuilder);
            }
            Datum::Bool(v) => append_value_to_arrow!(BooleanBuilder, *v),
            Datum::Int8(v) => append_value_to_arrow!(Int8Builder, *v),
            Datum::Int16(v) => append_value_to_arrow!(Int16Builder, *v),
            Datum::Int32(v) => append_value_to_arrow!(Int32Builder, *v),
            Datum::Int64(v) => append_value_to_arrow!(Int64Builder, *v),
            Datum::Float32(v) => append_value_to_arrow!(Float32Builder, v.into_inner()),
            Datum::Float64(v) => append_value_to_arrow!(Float64Builder, v.into_inner()),
            Datum::String(v) => append_value_to_arrow!(StringBuilder, v.as_ref()),
            Datum::Blob(v) => append_value_to_arrow!(BinaryBuilder, v.as_ref()),
            Datum::Decimal(decimal) => {
                // Extract target precision and scale from Arrow schema
                let (p, s) = match data_type {
                    arrow_schema::DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(RowConvertError {
                            message: format!(
                                "Expected Decimal128 Arrow type, got: {:?}",
                                data_type
                            ),
                        });
                    }
                };

                // Validate scale is non-negative (Fluss doesn't support negative scales)
                if s < 0 {
                    return Err(RowConvertError {
                        message: format!("Negative decimal scale {} is not supported", s),
                    });
                }

                let target_precision = p as u32;
                let target_scale = s as i64; // Safe now: 0..127 → 0i64..127i64

                if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    use bigdecimal::RoundingMode;

                    // Rescale the decimal to match Arrow's target scale
                    let bd = decimal.to_big_decimal();
                    let rescaled = bd.with_scale_round(target_scale, RoundingMode::HalfUp);
                    let (unscaled, _) = rescaled.as_bigint_and_exponent();

                    // Validate precision
                    let actual_precision = Decimal::compute_precision(&unscaled);
                    if actual_precision > target_precision as usize {
                        return Err(RowConvertError {
                            message: format!(
                                "Decimal precision overflow: value has {} digits but Arrow expects {} (value: {})",
                                actual_precision, target_precision, rescaled
                            ),
                        });
                    }

                    // Convert to i128 for Arrow
                    let i128_val: i128 = match unscaled.try_into() {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(RowConvertError {
                                message: format!("Decimal value exceeds i128 range: {}", rescaled),
                            });
                        }
                    };

                    b.append_value(i128_val);
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for Decimal128".to_string(),
                });
            }
            Datum::Date(date) => {
                append_value_to_arrow!(Date32Builder, date.get_inner());
            }
            Datum::Time(time) => {
                // Time is stored as milliseconds since midnight in Fluss
                // Convert to Arrow's time unit based on schema
                let millis = time.get_inner();

                match data_type {
                    arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Second) => {
                        if let Some(b) = builder.as_any_mut().downcast_mut::<Time32SecondBuilder>()
                        {
                            // Validate no sub-second precision is lost
                            if millis % MILLIS_PER_SECOND as i32 != 0 {
                                return Err(RowConvertError {
                                    message: format!(
                                        "Time value {} ms has sub-second precision but schema expects seconds only",
                                        millis
                                    ),
                                });
                            }
                            b.append_value(millis / MILLIS_PER_SECOND as i32);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time32MillisecondBuilder>()
                        {
                            b.append_value(millis);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time64MicrosecondBuilder>()
                        {
                            let micros = (millis as i64)
                                .checked_mul(MICROS_PER_MILLI)
                                .ok_or_else(|| RowConvertError {
                                    message: format!(
                                        "Time value {} ms overflows when converting to microseconds",
                                        millis
                                    ),
                                })?;
                            b.append_value(micros);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time64NanosecondBuilder>()
                        {
                            let nanos = (millis as i64).checked_mul(NANOS_PER_MILLI).ok_or_else(
                                || RowConvertError {
                                    message: format!(
                                        "Time value {} ms overflows when converting to nanoseconds",
                                        millis
                                    ),
                                },
                            )?;
                            b.append_value(nanos);
                            return Ok(());
                        }
                    }
                    _ => {
                        return Err(RowConvertError {
                            message: format!(
                                "Expected Time32/Time64 Arrow type, got: {:?}",
                                data_type
                            ),
                        });
                    }
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for Time".to_string(),
                });
            }
            Datum::TimestampNtz(ts) => {
                let millis = ts.get_millisecond();
                let nanos = ts.get_nano_of_millisecond();

                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    // Convert milliseconds to seconds
                    b.append_value(millis / MILLIS_PER_SECOND);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_value(millis);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    // Convert milliseconds + nanos to microseconds
                    let millis_micros = millis
                        .checked_mul(MICROS_PER_MILLI)
                        .ok_or_else(|| RowConvertError {
                            message: format!(
                                "Timestamp milliseconds {} overflows when converting to microseconds",
                                millis
                            ),
                        })?;
                    let nanos_micros = (nanos as i64) / MICROS_PER_MILLI;
                    let micros =
                        millis_micros
                            .checked_add(nanos_micros)
                            .ok_or_else(|| RowConvertError {
                                message: format!(
                                    "Timestamp overflow when adding microseconds: {} + {}",
                                    millis_micros, nanos_micros
                                ),
                            })?;
                    b.append_value(micros);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    // Convert milliseconds + nanos to nanoseconds
                    let millis_nanos = millis
                        .checked_mul(NANOS_PER_MILLI)
                        .ok_or_else(|| RowConvertError {
                            message: format!(
                                "Timestamp milliseconds {} overflows when converting to nanoseconds",
                                millis
                            ),
                        })?;
                    let nanos_total =
                        millis_nanos
                            .checked_add(nanos as i64)
                            .ok_or_else(|| RowConvertError {
                                message: format!(
                                    "Timestamp overflow when adding nanoseconds: {} + {}",
                                    millis_nanos, nanos
                                ),
                            })?;
                    b.append_value(nanos_total);
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for TimestampNtz".to_string(),
                });
            }
            Datum::TimestampLtz(ts) => {
                let millis = ts.get_epoch_millisecond();
                let nanos = ts.get_nano_of_millisecond();

                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    // Convert milliseconds to seconds
                    b.append_value(millis / MILLIS_PER_SECOND);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_value(millis);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    // Convert milliseconds + nanos to microseconds
                    let millis_micros = millis
                        .checked_mul(MICROS_PER_MILLI)
                        .ok_or_else(|| RowConvertError {
                            message: format!(
                                "Timestamp milliseconds {} overflows when converting to microseconds",
                                millis
                            ),
                        })?;
                    let nanos_micros = (nanos as i64) / MICROS_PER_MILLI;
                    let micros =
                        millis_micros
                            .checked_add(nanos_micros)
                            .ok_or_else(|| RowConvertError {
                                message: format!(
                                    "Timestamp overflow when adding microseconds: {} + {}",
                                    millis_micros, nanos_micros
                                ),
                            })?;
                    b.append_value(micros);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    // Convert milliseconds + nanos to nanoseconds
                    let millis_nanos = millis
                        .checked_mul(NANOS_PER_MILLI)
                        .ok_or_else(|| RowConvertError {
                            message: format!(
                                "Timestamp milliseconds {} overflows when converting to nanoseconds",
                                millis
                            ),
                        })?;
                    let nanos_total =
                        millis_nanos
                            .checked_add(nanos as i64)
                            .ok_or_else(|| RowConvertError {
                                message: format!(
                                    "Timestamp overflow when adding nanoseconds: {} + {}",
                                    millis_nanos, nanos
                                ),
                            })?;
                    b.append_value(nanos_total);
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for TimestampLtz".to_string(),
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
            fn append_to(
                &self,
                builder: &mut dyn ArrayBuilder,
                _data_type: &arrow_schema::DataType,
            ) -> Result<()> {
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
#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Date(i32);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Time(i32);

impl Time {
    pub const fn new(inner: i32) -> Self {
        Time(inner)
    }

    /// Get the inner value of time type (milliseconds since midnight)
    pub fn get_inner(&self) -> i32 {
        self.0
    }
}

/// Maximum timestamp precision that can be stored compactly (milliseconds only).
/// Values with precision > MAX_COMPACT_TIMESTAMP_PRECISION require additional nanosecond storage.
pub const MAX_COMPACT_TIMESTAMP_PRECISION: u32 = 3;

/// Maximum valid value for nanoseconds within a millisecond (0 to 999,999 inclusive).
/// A millisecond contains 1,000,000 nanoseconds, so the fractional part ranges from 0 to 999,999.
pub const MAX_NANO_OF_MILLISECOND: i32 = 999_999;

// Time unit conversion constants
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
#[display("{millisecond}")]
pub struct TimestampNtz {
    millisecond: i64,
    nano_of_millisecond: i32,
}

impl TimestampNtz {
    pub const fn new(millisecond: i64) -> Self {
        TimestampNtz {
            millisecond,
            nano_of_millisecond: 0,
        }
    }

    pub fn from_millis_nanos(
        millisecond: i64,
        nano_of_millisecond: i32,
    ) -> crate::error::Result<Self> {
        if !(0..=MAX_NANO_OF_MILLISECOND).contains(&nano_of_millisecond) {
            return Err(crate::error::Error::IllegalArgument {
                message: format!(
                    "nanoOfMillisecond must be in range [0, {}], got: {}",
                    MAX_NANO_OF_MILLISECOND, nano_of_millisecond
                ),
            });
        }
        Ok(TimestampNtz {
            millisecond,
            nano_of_millisecond,
        })
    }

    pub fn get_millisecond(&self) -> i64 {
        self.millisecond
    }

    pub fn get_nano_of_millisecond(&self) -> i32 {
        self.nano_of_millisecond
    }

    /// Check if the timestamp is compact based on precision.
    /// Precision <= MAX_COMPACT_TIMESTAMP_PRECISION means millisecond precision, no need for nanos.
    pub fn is_compact(precision: u32) -> bool {
        precision <= MAX_COMPACT_TIMESTAMP_PRECISION
    }
}

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
#[display("{epoch_millisecond}")]
pub struct TimestampLtz {
    epoch_millisecond: i64,
    nano_of_millisecond: i32,
}

impl TimestampLtz {
    pub const fn new(epoch_millisecond: i64) -> Self {
        TimestampLtz {
            epoch_millisecond,
            nano_of_millisecond: 0,
        }
    }

    pub fn from_millis_nanos(
        epoch_millisecond: i64,
        nano_of_millisecond: i32,
    ) -> crate::error::Result<Self> {
        if !(0..=MAX_NANO_OF_MILLISECOND).contains(&nano_of_millisecond) {
            return Err(crate::error::Error::IllegalArgument {
                message: format!(
                    "nanoOfMillisecond must be in range [0, {}], got: {}",
                    MAX_NANO_OF_MILLISECOND, nano_of_millisecond
                ),
            });
        }
        Ok(TimestampLtz {
            epoch_millisecond,
            nano_of_millisecond,
        })
    }

    pub fn get_epoch_millisecond(&self) -> i64 {
        self.epoch_millisecond
    }

    pub fn get_nano_of_millisecond(&self) -> i32 {
        self.nano_of_millisecond
    }

    /// Check if the timestamp is compact based on precision.
    /// Precision <= MAX_COMPACT_TIMESTAMP_PRECISION means millisecond precision, no need for nanos.
    pub fn is_compact(precision: u32) -> bool {
        precision <= MAX_COMPACT_TIMESTAMP_PRECISION
    }
}

pub type Blob<'a> = Cow<'a, [u8]>;

impl<'a> From<Vec<u8>> for Datum<'a> {
    fn from(vec: Vec<u8>) -> Self {
        Datum::Blob(Blob::from(vec))
    }
}

impl<'a> From<&'a [u8]> for Datum<'a> {
    fn from(bytes: &'a [u8]) -> Datum<'a> {
        Datum::Blob(Blob::from(bytes))
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
        let datum = Datum::String("value".into());
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

        // Test temporal types
        let decimal = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        let datum: Datum = decimal.clone().into();
        assert_eq!(datum.as_decimal(), &decimal);
        let extracted: Decimal = (&datum).try_into().unwrap();
        assert_eq!(extracted, decimal);

        let date = Date::new(19000);
        let datum: Datum = date.into();
        assert_eq!(datum.as_date(), date);

        let ts_ltz = TimestampLtz::new(1672531200000);
        let datum: Datum = ts_ltz.into();
        assert_eq!(datum.as_timestamp_ltz(), ts_ltz);
    }

    #[test]
    fn datum_append_to_builder() {
        let mut builder = Int32Builder::new();
        Datum::Null
            .append_to(&mut builder, &arrow_schema::DataType::Int32)
            .unwrap();
        Datum::Int32(5)
            .append_to(&mut builder, &arrow_schema::DataType::Int32)
            .unwrap();
        let array = builder.finish();
        assert!(array.is_null(0));
        assert_eq!(array.value(1), 5);

        let mut builder = StringBuilder::new();
        let err = Datum::Int32(1)
            .append_to(&mut builder, &arrow_schema::DataType::Utf8)
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

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    #[test]
    fn test_timestamp_valid_nanos() {
        // Valid range: 0 to MAX_NANO_OF_MILLISECOND for both TimestampNtz and TimestampLtz
        let ntz1 = TimestampNtz::from_millis_nanos(1000, 0).unwrap();
        assert_eq!(ntz1.get_nano_of_millisecond(), 0);

        let ntz2 = TimestampNtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND).unwrap();
        assert_eq!(ntz2.get_nano_of_millisecond(), MAX_NANO_OF_MILLISECOND);

        let ntz3 = TimestampNtz::from_millis_nanos(1000, 500_000).unwrap();
        assert_eq!(ntz3.get_nano_of_millisecond(), 500_000);

        let ltz1 = TimestampLtz::from_millis_nanos(1000, 0).unwrap();
        assert_eq!(ltz1.get_nano_of_millisecond(), 0);

        let ltz2 = TimestampLtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND).unwrap();
        assert_eq!(ltz2.get_nano_of_millisecond(), MAX_NANO_OF_MILLISECOND);
    }

    #[test]
    fn test_timestamp_nanos_out_of_range() {
        // Test that both TimestampNtz and TimestampLtz reject invalid nanos
        let expected_msg = format!(
            "nanoOfMillisecond must be in range [0, {}]",
            MAX_NANO_OF_MILLISECOND
        );

        // Too large (1,000,000 is just beyond the valid range)
        let result_ntz = TimestampNtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND + 1);
        assert!(result_ntz.is_err());
        assert!(result_ntz.unwrap_err().to_string().contains(&expected_msg));

        let result_ltz = TimestampLtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND + 1);
        assert!(result_ltz.is_err());
        assert!(result_ltz.unwrap_err().to_string().contains(&expected_msg));

        // Negative
        let result_ntz = TimestampNtz::from_millis_nanos(1000, -1);
        assert!(result_ntz.is_err());
        assert!(result_ntz.unwrap_err().to_string().contains(&expected_msg));

        let result_ltz = TimestampLtz::from_millis_nanos(1000, -1);
        assert!(result_ltz.is_err());
        assert!(result_ltz.unwrap_err().to_string().contains(&expected_msg));
    }
}
