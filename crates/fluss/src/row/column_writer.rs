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

//! Typed column writers that write directly from [`InternalRow`] to concrete
//! Arrow builders, bypassing the intermediate [`Datum`] enum and runtime
//! `downcast_mut` dispatch.

use crate::error::Error::RowConvertError;
use crate::error::{Error, Result};
use crate::metadata::DataType;
use crate::row::datum::{
    MICROS_PER_MILLI, MILLIS_PER_SECOND, NANOS_PER_MILLI, append_decimal_to_builder,
    millis_nanos_to_micros, millis_nanos_to_nanos,
};
use crate::row::{FlussArray, InternalRow};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, ListBuilder, StringBuilder, Time32MillisecondBuilder,
    Time32SecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder,
};
use arrow_schema::DataType as ArrowDataType;

/// Estimated average byte size for variable-width columns (Utf8, Binary).
/// Used to pre-allocate data buffers and avoid reallocations during batch building.
const VARIABLE_WIDTH_AVG_BYTES: usize = 64;

/// A typed column writer that reads one column from an [`InternalRow`] and
/// appends directly to a concrete Arrow builder — no intermediate [`Datum`],
/// no `as_any_mut().downcast_mut()`.
pub struct ColumnWriter {
    pos: usize,
    nullable: bool,
    inner: TypedWriter,
}

enum TypedWriter {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Char {
        len: usize,
        builder: StringBuilder,
    },
    String(StringBuilder),
    Bytes(BinaryBuilder),
    Binary {
        len: usize,
        builder: FixedSizeBinaryBuilder,
    },
    Decimal128 {
        src_precision: usize,
        src_scale: usize,
        target_precision: u32,
        target_scale: i64,
        builder: Decimal128Builder,
    },
    Date32(Date32Builder),
    Time32Second(Time32SecondBuilder),
    Time32Millisecond(Time32MillisecondBuilder),
    Time64Microsecond(Time64MicrosecondBuilder),
    Time64Nanosecond(Time64NanosecondBuilder),
    TimestampNtzSecond {
        precision: u32,
        builder: TimestampSecondBuilder,
    },
    TimestampNtzMillisecond {
        precision: u32,
        builder: TimestampMillisecondBuilder,
    },
    TimestampNtzMicrosecond {
        precision: u32,
        builder: TimestampMicrosecondBuilder,
    },
    TimestampNtzNanosecond {
        precision: u32,
        builder: TimestampNanosecondBuilder,
    },
    TimestampLtzSecond {
        precision: u32,
        builder: TimestampSecondBuilder,
    },
    TimestampLtzMillisecond {
        precision: u32,
        builder: TimestampMillisecondBuilder,
    },
    TimestampLtzMicrosecond {
        precision: u32,
        builder: TimestampMicrosecondBuilder,
    },
    TimestampLtzNanosecond {
        precision: u32,
        builder: TimestampNanosecondBuilder,
    },
    List {
        element_type: DataType,
        builder: ListBuilder<Box<dyn ArrayBuilder>>,
    },
}

/// Dispatch to the inner builder across all `TypedWriter` variants.
/// Exhaustive matching ensures new variants won't compile without an arm.
macro_rules! with_builder {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            TypedWriter::Bool($b) => $body,
            TypedWriter::Int8($b) => $body,
            TypedWriter::Int16($b) => $body,
            TypedWriter::Int32($b) => $body,
            TypedWriter::Int64($b) => $body,
            TypedWriter::Float32($b) => $body,
            TypedWriter::Float64($b) => $body,
            TypedWriter::Char { builder: $b, .. } => $body,
            TypedWriter::String($b) => $body,
            TypedWriter::Bytes($b) => $body,
            TypedWriter::Binary { builder: $b, .. } => $body,
            TypedWriter::Decimal128 { builder: $b, .. } => $body,
            TypedWriter::Date32($b) => $body,
            TypedWriter::Time32Second($b) => $body,
            TypedWriter::Time32Millisecond($b) => $body,
            TypedWriter::Time64Microsecond($b) => $body,
            TypedWriter::Time64Nanosecond($b) => $body,
            TypedWriter::TimestampNtzSecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzMillisecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzMicrosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzNanosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzSecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzMillisecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzMicrosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzNanosecond { builder: $b, .. } => $body,
            TypedWriter::List { builder: $b, .. } => $body,
        }
    };
}

impl ColumnWriter {
    /// Create a column writer for the given Fluss `DataType` and Arrow
    /// `ArrowDataType` at position `pos` with the given pre-allocation
    /// `capacity`.
    pub fn create(
        fluss_type: &DataType,
        arrow_type: &ArrowDataType,
        pos: usize,
        capacity: usize,
    ) -> Result<Self> {
        let nullable = fluss_type.is_nullable();

        let inner = match fluss_type {
            DataType::Boolean(_) => TypedWriter::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::TinyInt(_) => TypedWriter::Int8(Int8Builder::with_capacity(capacity)),
            DataType::SmallInt(_) => TypedWriter::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int(_) => TypedWriter::Int32(Int32Builder::with_capacity(capacity)),
            DataType::BigInt(_) => TypedWriter::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float(_) => TypedWriter::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Double(_) => TypedWriter::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Char(t) => TypedWriter::Char {
                len: t.length() as usize,
                builder: StringBuilder::with_capacity(
                    capacity,
                    capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
                ),
            },
            DataType::String(_) => TypedWriter::String(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
            )),
            DataType::Bytes(_) => TypedWriter::Bytes(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
            )),
            DataType::Binary(t) => {
                let arrow_len: i32 = t.length().try_into().map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Binary length {} exceeds Arrow's maximum (i32::MAX)",
                        t.length()
                    ),
                })?;
                TypedWriter::Binary {
                    len: t.length(),
                    builder: FixedSizeBinaryBuilder::with_capacity(capacity, arrow_len),
                }
            }
            DataType::Decimal(dt) => {
                let (target_p, target_s) = match arrow_type {
                    ArrowDataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected Decimal128 Arrow type for Decimal, got: {arrow_type:?}"
                            ),
                        });
                    }
                };
                if target_s < 0 {
                    return Err(Error::IllegalArgument {
                        message: format!("Negative decimal scale {target_s} is not supported"),
                    });
                }
                let builder = Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(target_p, target_s)
                    .map_err(|e| Error::IllegalArgument {
                        message: format!(
                            "Invalid decimal precision {target_p} or scale {target_s}: {e}"
                        ),
                    })?;
                TypedWriter::Decimal128 {
                    src_precision: dt.precision() as usize,
                    src_scale: dt.scale() as usize,
                    target_precision: target_p as u32,
                    target_scale: target_s as i64,
                    builder,
                }
            }
            DataType::Date(_) => TypedWriter::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Time(_) => match arrow_type {
                ArrowDataType::Time32(arrow_schema::TimeUnit::Second) => {
                    TypedWriter::Time32Second(Time32SecondBuilder::with_capacity(capacity))
                }
                ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                    TypedWriter::Time32Millisecond(Time32MillisecondBuilder::with_capacity(
                        capacity,
                    ))
                }
                ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                    TypedWriter::Time64Microsecond(Time64MicrosecondBuilder::with_capacity(
                        capacity,
                    ))
                }
                ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                    TypedWriter::Time64Nanosecond(Time64NanosecondBuilder::with_capacity(capacity))
                }
                _ => {
                    return Err(Error::IllegalArgument {
                        message: format!("Unsupported Arrow type for Time: {arrow_type:?}"),
                    });
                }
            },
            DataType::Timestamp(t) => {
                let precision = t.precision();
                match arrow_type {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                        TypedWriter::TimestampNtzSecond {
                            precision,
                            builder: TimestampSecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        TypedWriter::TimestampNtzMillisecond {
                            precision,
                            builder: TimestampMillisecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        TypedWriter::TimestampNtzMicrosecond {
                            precision,
                            builder: TimestampMicrosecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                        TypedWriter::TimestampNtzNanosecond {
                            precision,
                            builder: TimestampNanosecondBuilder::with_capacity(capacity),
                        }
                    }
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Unsupported Arrow type for Timestamp: {arrow_type:?}"
                            ),
                        });
                    }
                }
            }
            DataType::TimestampLTz(t) => {
                let precision = t.precision();
                match arrow_type {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                        TypedWriter::TimestampLtzSecond {
                            precision,
                            builder: TimestampSecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        TypedWriter::TimestampLtzMillisecond {
                            precision,
                            builder: TimestampMillisecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        TypedWriter::TimestampLtzMicrosecond {
                            precision,
                            builder: TimestampMicrosecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                        TypedWriter::TimestampLtzNanosecond {
                            precision,
                            builder: TimestampNanosecondBuilder::with_capacity(capacity),
                        }
                    }
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Unsupported Arrow type for TimestampLTz: {arrow_type:?}"
                            ),
                        });
                    }
                }
            }
            DataType::Array(array_type) => {
                let element_type = array_type.get_element_type();
                let arrow_element_type = match arrow_type {
                    ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
                        field.data_type()
                    }
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected List Arrow type for Array, got: {arrow_type:?}"
                            ),
                        });
                    }
                };
                let element_builder = create_builder(element_type, arrow_element_type, capacity)?;
                TypedWriter::List {
                    element_type: element_type.clone(),
                    builder: ListBuilder::with_capacity(element_builder, capacity),
                }
            }
            _ => {
                return Err(Error::IllegalArgument {
                    message: format!("Unsupported Fluss DataType: {fluss_type:?}"),
                });
            }
        };

        Ok(Self {
            pos,
            nullable,
            inner,
        })
    }

    /// Read one value from `row` at this writer's column position and append it
    /// directly to the concrete Arrow builder.
    #[inline]
    pub fn write_field(&mut self, row: &dyn InternalRow) -> Result<()> {
        if self.nullable && row.is_null_at(self.pos)? {
            self.append_null();
            return Ok(());
        }
        self.write_non_null(row)
    }

    /// Finish the builder, producing the final Arrow array.
    pub fn finish(&mut self) -> ArrayRef {
        self.as_builder_mut().finish()
    }

    /// Clone-finish the builder for size estimation (does not reset the builder).
    pub fn finish_cloned(&self) -> ArrayRef {
        self.as_builder_ref().finish_cloned()
    }

    fn append_null(&mut self) {
        with_builder!(&mut self.inner, b => b.append_null());
    }

    /// Returns a trait-object reference to the inner builder.
    /// Used for type-agnostic operations (`finish`, `finish_cloned`).
    fn as_builder_mut(&mut self) -> &mut dyn ArrayBuilder {
        with_builder!(&mut self.inner, b => b)
    }

    fn as_builder_ref(&self) -> &dyn ArrayBuilder {
        with_builder!(&self.inner, b => b)
    }

    #[inline]
    fn write_non_null(&mut self, row: &dyn InternalRow) -> Result<()> {
        let pos = self.pos;

        match &mut self.inner {
            TypedWriter::Bool(b) => {
                b.append_value(row.get_boolean(pos)?);
                Ok(())
            }
            TypedWriter::Int8(b) => {
                b.append_value(row.get_byte(pos)?);
                Ok(())
            }
            TypedWriter::Int16(b) => {
                b.append_value(row.get_short(pos)?);
                Ok(())
            }
            TypedWriter::Int32(b) => {
                b.append_value(row.get_int(pos)?);
                Ok(())
            }
            TypedWriter::Int64(b) => {
                b.append_value(row.get_long(pos)?);
                Ok(())
            }
            TypedWriter::Float32(b) => {
                b.append_value(row.get_float(pos)?);
                Ok(())
            }
            TypedWriter::Float64(b) => {
                b.append_value(row.get_double(pos)?);
                Ok(())
            }
            TypedWriter::Char { len, builder } => {
                let v = row.get_char(pos, *len)?;
                builder.append_value(v);
                Ok(())
            }
            TypedWriter::String(b) => {
                let v = row.get_string(pos)?;
                b.append_value(v);
                Ok(())
            }
            TypedWriter::Bytes(b) => {
                let v = row.get_bytes(pos)?;
                b.append_value(v);
                Ok(())
            }
            TypedWriter::Binary { len, builder } => {
                let v = row.get_binary(pos, *len)?;
                builder.append_value(v).map_err(|e| RowConvertError {
                    message: format!("Failed to append binary value: {e}"),
                })?;
                Ok(())
            }
            TypedWriter::Decimal128 {
                src_precision,
                src_scale,
                target_precision,
                target_scale,
                builder,
            } => {
                let decimal = row.get_decimal(pos, *src_precision, *src_scale)?;
                append_decimal_to_builder(&decimal, *target_precision, *target_scale, builder)
            }
            TypedWriter::Date32(b) => {
                let date = row.get_date(pos)?;
                b.append_value(date.get_inner());
                Ok(())
            }
            TypedWriter::Time32Second(b) => {
                let millis = row.get_time(pos)?.get_inner();
                if millis % MILLIS_PER_SECOND as i32 != 0 {
                    return Err(RowConvertError {
                        message: format!(
                            "Time value {millis} ms has sub-second precision but schema expects seconds only"
                        ),
                    });
                }
                b.append_value(millis / MILLIS_PER_SECOND as i32);
                Ok(())
            }
            TypedWriter::Time32Millisecond(b) => {
                b.append_value(row.get_time(pos)?.get_inner());
                Ok(())
            }
            TypedWriter::Time64Microsecond(b) => {
                let millis = row.get_time(pos)?.get_inner();
                let micros = (millis as i64)
                    .checked_mul(MICROS_PER_MILLI)
                    .ok_or_else(|| RowConvertError {
                        message: format!(
                            "Time value {millis} ms overflows when converting to microseconds"
                        ),
                    })?;
                b.append_value(micros);
                Ok(())
            }
            TypedWriter::Time64Nanosecond(b) => {
                let millis = row.get_time(pos)?.get_inner();
                let nanos = (millis as i64)
                    .checked_mul(NANOS_PER_MILLI)
                    .ok_or_else(|| RowConvertError {
                        message: format!(
                            "Time value {millis} ms overflows when converting to nanoseconds"
                        ),
                    })?;
                b.append_value(nanos);
                Ok(())
            }
            // --- TimestampNtz variants ---
            TypedWriter::TimestampNtzSecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(ts.get_millisecond() / MILLIS_PER_SECOND);
                Ok(())
            }
            TypedWriter::TimestampNtzMillisecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(ts.get_millisecond());
                Ok(())
            }
            TypedWriter::TimestampNtzMicrosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(millis_nanos_to_micros(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::TimestampNtzNanosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(millis_nanos_to_nanos(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            // --- TimestampLtz variants ---
            TypedWriter::TimestampLtzSecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(ts.get_epoch_millisecond() / MILLIS_PER_SECOND);
                Ok(())
            }
            TypedWriter::TimestampLtzMillisecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(ts.get_epoch_millisecond());
                Ok(())
            }
            TypedWriter::TimestampLtzMicrosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(millis_nanos_to_micros(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::TimestampLtzNanosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(millis_nanos_to_nanos(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::List {
                element_type,
                builder,
            } => {
                let array = row.get_array(pos)?;
                let values_builder = builder.values();
                for i in 0..array.size() {
                    append_element_to_builder(values_builder, &array, i, element_type)?;
                }
                builder.append(true);
                Ok(())
            }
        }
    }
}

fn create_builder(
    fluss_type: &DataType,
    arrow_type: &ArrowDataType,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>> {
    match fluss_type {
        DataType::Boolean(_) => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
        DataType::TinyInt(_) => Ok(Box::new(Int8Builder::with_capacity(capacity))),
        DataType::SmallInt(_) => Ok(Box::new(Int16Builder::with_capacity(capacity))),
        DataType::Int(_) => Ok(Box::new(Int32Builder::with_capacity(capacity))),
        DataType::BigInt(_) => Ok(Box::new(Int64Builder::with_capacity(capacity))),
        DataType::Float(_) => Ok(Box::new(Float32Builder::with_capacity(capacity))),
        DataType::Double(_) => Ok(Box::new(Float64Builder::with_capacity(capacity))),
        DataType::Char(_) | DataType::String(_) => Ok(Box::new(StringBuilder::with_capacity(
            capacity,
            capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
        ))),
        DataType::Bytes(_) => Ok(Box::new(BinaryBuilder::with_capacity(
            capacity,
            capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
        ))),
        DataType::Binary(t) => {
            let arrow_len: i32 = t.length().try_into().map_err(|_| Error::IllegalArgument {
                message: format!(
                    "Binary length {} exceeds Arrow's maximum (i32::MAX)",
                    t.length()
                ),
            })?;
            Ok(Box::new(FixedSizeBinaryBuilder::with_capacity(
                capacity, arrow_len,
            )))
        }
        DataType::Decimal(_) => {
            let (p, s) = match arrow_type {
                ArrowDataType::Decimal128(p, s) => (*p, *s),
                _ => {
                    return Err(Error::IllegalArgument {
                        message: format!(
                            "Expected Decimal128 Arrow type for Decimal, got: {arrow_type:?}"
                        ),
                    });
                }
            };
            let builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(p, s)
                .map_err(|e| Error::IllegalArgument {
                    message: format!("Invalid decimal precision {p} or scale {s}: {e}"),
                })?;
            Ok(Box::new(builder))
        }
        DataType::Date(_) => Ok(Box::new(Date32Builder::with_capacity(capacity))),
        DataType::Time(_) => match arrow_type {
            ArrowDataType::Time32(arrow_schema::TimeUnit::Second) => {
                Ok(Box::new(Time32SecondBuilder::with_capacity(capacity)))
            }
            ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                Ok(Box::new(Time32MillisecondBuilder::with_capacity(capacity)))
            }
            ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                Ok(Box::new(Time64MicrosecondBuilder::with_capacity(capacity)))
            }
            ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                Ok(Box::new(Time64NanosecondBuilder::with_capacity(capacity)))
            }
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported Arrow type for Time: {arrow_type:?}"),
            }),
        },
        DataType::Timestamp(_) => match arrow_type {
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                Ok(Box::new(TimestampSecondBuilder::with_capacity(capacity)))
            }
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => Ok(Box::new(
                TimestampMillisecondBuilder::with_capacity(capacity),
            )),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => Ok(Box::new(
                TimestampMicrosecondBuilder::with_capacity(capacity),
            )),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => Ok(Box::new(
                TimestampNanosecondBuilder::with_capacity(capacity),
            )),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported Arrow type for Timestamp: {arrow_type:?}"),
            }),
        },
        DataType::TimestampLTz(_) => match arrow_type {
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                Ok(Box::new(TimestampSecondBuilder::with_capacity(capacity)))
            }
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => Ok(Box::new(
                TimestampMillisecondBuilder::with_capacity(capacity),
            )),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => Ok(Box::new(
                TimestampMicrosecondBuilder::with_capacity(capacity),
            )),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => Ok(Box::new(
                TimestampNanosecondBuilder::with_capacity(capacity),
            )),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported Arrow type for TimestampLTz: {arrow_type:?}"),
            }),
        },
        DataType::Array(array_type) => {
            let element_type = array_type.get_element_type();
            let arrow_element_type = match arrow_type {
                ArrowDataType::List(field) | ArrowDataType::LargeList(field) => field.data_type(),
                _ => {
                    return Err(Error::IllegalArgument {
                        message: format!("Expected List Arrow type for Array, got: {arrow_type:?}"),
                    });
                }
            };
            let element_builder = create_builder(element_type, arrow_element_type, capacity)?;
            Ok(Box::new(ListBuilder::with_capacity(
                element_builder,
                capacity,
            )))
        }
        _ => Err(Error::IllegalArgument {
            message: format!("Unsupported Fluss DataType for builder: {fluss_type:?}"),
        }),
    }
}

fn append_element_to_builder(
    builder: &mut dyn ArrayBuilder,
    array: &FlussArray,
    pos: usize,
    element_type: &DataType,
) -> Result<()> {
    match element_type {
        DataType::Boolean(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .expect("Failed to downcast to BooleanBuilder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_boolean(pos)?);
            }
        }
        DataType::TinyInt(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .expect("Failed to downcast to Int8Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_byte(pos)?);
            }
        }
        DataType::SmallInt(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .expect("Failed to downcast to Int16Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_short(pos)?);
            }
        }
        DataType::Int(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("Failed to downcast to Int32Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_int(pos)?);
            }
        }
        DataType::BigInt(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("Failed to downcast to Int64Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_long(pos)?);
            }
        }
        DataType::Float(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .expect("Failed to downcast to Float32Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_float(pos)?);
            }
        }
        DataType::Double(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .expect("Failed to downcast to Float64Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_double(pos)?);
            }
        }
        DataType::Char(_) | DataType::String(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Failed to downcast to StringBuilder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_string(pos)?);
            }
        }
        DataType::Bytes(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .expect("Failed to downcast to BinaryBuilder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_binary(pos)?);
            }
        }
        DataType::Binary(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeBinaryBuilder>()
                .expect("Failed to downcast to FixedSizeBinaryBuilder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_binary(pos)?)
                    .map_err(|e| RowConvertError {
                        message: format!("Failed to append binary value: {e}"),
                    })?;
            }
        }
        DataType::Decimal(dt) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Decimal128Builder>()
                .expect("Failed to downcast to Decimal128Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                let decimal = array.get_decimal(pos, dt.precision(), dt.scale())?;
                append_decimal_to_builder(&decimal, dt.precision(), dt.scale() as i64, b)?;
            }
        }
        DataType::Date(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .expect("Failed to downcast to Date32Builder");
            if array.is_null_at(pos) {
                b.append_null();
            } else {
                b.append_value(array.get_date(pos)?.get_inner());
            }
        }
        DataType::Time(_) => {
            if array.is_null_at(pos) {
                // We don't know the exact time unit builder here without re-matching type,
                // but we can try to downcast to any of them just for append_null.
                if let Some(b) = builder.as_any_mut().downcast_mut::<Time32SecondBuilder>() {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<Time32MillisecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64MicrosecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64NanosecondBuilder>()
                {
                    b.append_null();
                } else {
                    return Err(RowConvertError {
                        message: "Failed to downcast to any TimeBuilder for null".to_string(),
                    });
                }
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Time32SecondBuilder>() {
                let millis = array.get_time(pos)?.get_inner();
                b.append_value(millis / MILLIS_PER_SECOND as i32);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<Time32MillisecondBuilder>()
            {
                b.append_value(array.get_time(pos)?.get_inner());
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<Time64MicrosecondBuilder>()
            {
                let millis = array.get_time(pos)?.get_inner();
                b.append_value(millis as i64 * MICROS_PER_MILLI);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<Time64NanosecondBuilder>()
            {
                let millis = array.get_time(pos)?.get_inner();
                b.append_value(millis as i64 * NANOS_PER_MILLI);
            } else {
                return Err(RowConvertError {
                    message: "Failed to downcast to any TimeBuilder".to_string(),
                });
            }
        }
        DataType::Timestamp(dt) => {
            let precision = dt.precision();
            if array.is_null_at(pos) {
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_null();
                } else {
                    return Err(RowConvertError {
                        message: "Failed to downcast to any TimestampBuilder for null".to_string(),
                    });
                }
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
            {
                let ts = array.get_timestamp_ntz(pos, precision)?;
                b.append_value(ts.get_millisecond() / MILLIS_PER_SECOND);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
            {
                let ts = array.get_timestamp_ntz(pos, precision)?;
                b.append_value(ts.get_millisecond());
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
            {
                let ts = array.get_timestamp_ntz(pos, precision)?;
                b.append_value(millis_nanos_to_micros(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
            {
                let ts = array.get_timestamp_ntz(pos, precision)?;
                b.append_value(millis_nanos_to_nanos(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
            } else {
                return Err(RowConvertError {
                    message: "Failed to downcast to any TimestampBuilder".to_string(),
                });
            }
        }
        DataType::TimestampLTz(dt) => {
            let precision = dt.precision();
            if array.is_null_at(pos) {
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    b.append_null();
                } else if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_null();
                } else {
                    return Err(RowConvertError {
                        message: "Failed to downcast to any TimestampLTzBuilder for null"
                            .to_string(),
                    });
                }
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
            {
                let ts = array.get_timestamp_ltz(pos, precision)?;
                b.append_value(ts.get_epoch_millisecond() / MILLIS_PER_SECOND);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
            {
                let ts = array.get_timestamp_ltz(pos, precision)?;
                b.append_value(ts.get_epoch_millisecond());
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
            {
                let ts = array.get_timestamp_ltz(pos, precision)?;
                b.append_value(millis_nanos_to_micros(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
            } else if let Some(b) = builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
            {
                let ts = array.get_timestamp_ltz(pos, precision)?;
                b.append_value(millis_nanos_to_nanos(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
            } else {
                return Err(RowConvertError {
                    message: "Failed to downcast to any TimestampLTzBuilder".to_string(),
                });
            }
        }
        DataType::Array(array_type) => {
            let element_type = array_type.get_element_type();
            let b = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .expect("Failed to downcast to ListBuilder");
            if array.is_null_at(pos) {
                b.append(false);
            } else {
                let nested_array = array.get_array(pos)?;
                let nested_values_builder = b.values();
                for i in 0..nested_array.size() {
                    append_element_to_builder(
                        nested_values_builder,
                        &nested_array,
                        i,
                        element_type,
                    )?;
                }
                b.append(true);
            }
        }
        _ => {
            return Err(Error::IllegalArgument {
                message: format!("Unsupported element type for Array datum: {element_type:?}"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::record::to_arrow_type;
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::{Date, Datum, GenericRow, Time, TimestampLtz, TimestampNtz};
    use arrow::array::*;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    /// Helper: create a ColumnWriter from a Fluss DataType, deriving the Arrow type automatically.
    fn writer_for(fluss_type: &DataType, capacity: usize) -> ColumnWriter {
        let arrow_type = to_arrow_type(fluss_type).unwrap();
        ColumnWriter::create(fluss_type, &arrow_type, 0, capacity).unwrap()
    }

    /// Helper: write a single datum and return the finished array.
    fn write_one(fluss_type: &DataType, datum: Datum) -> ArrayRef {
        let mut w = writer_for(fluss_type, 4);
        w.write_field(&GenericRow::from_data(vec![datum])).unwrap();
        w.finish()
    }

    #[test]
    fn write_all_scalar_types() {
        // Boolean
        let arr = write_one(&DataTypes::boolean(), Datum::Bool(true));
        assert!(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );

        // Integer types
        let arr = write_one(&DataTypes::tinyint(), Datum::Int8(42));
        assert_eq!(
            arr.as_any().downcast_ref::<Int8Array>().unwrap().value(0),
            42
        );

        let arr = write_one(&DataTypes::smallint(), Datum::Int16(1000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int16Array>().unwrap().value(0),
            1000
        );

        let arr = write_one(&DataTypes::int(), Datum::Int32(100_000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            100_000
        );

        let arr = write_one(&DataTypes::bigint(), Datum::Int64(9_000_000_000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            9_000_000_000
        );

        // Float types
        let arr = write_one(&DataTypes::float(), Datum::Float32(1.5.into()));
        assert!(
            (arr.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                - 1.5)
                .abs()
                < 0.001
        );

        let arr = write_one(&DataTypes::double(), Datum::Float64(1.125.into()));
        assert!(
            (arr.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 1.125)
                .abs()
                < 0.001
        );

        // String / Char
        let arr = write_one(&DataTypes::string(), Datum::String("hello".into()));
        assert_eq!(
            arr.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "hello"
        );

        let arr = write_one(&DataTypes::char(10), Datum::String("world".into()));
        assert_eq!(
            arr.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "world"
        );

        // Bytes / Binary
        let arr = write_one(&DataTypes::bytes(), Datum::Blob(vec![1, 2, 3].into()));
        assert_eq!(
            arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(0),
            &[1, 2, 3]
        );

        let arr = write_one(
            &DataTypes::binary(4),
            Datum::Blob(vec![10, 20, 30, 40].into()),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &[10, 20, 30, 40]
        );

        // Date
        let arr = write_one(&DataTypes::date(), Datum::Date(Date::new(19000)));
        assert_eq!(
            arr.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
            19000
        );

        // Time (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::time_with_precision(3),
            Datum::Time(Time::new(45_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            45_000
        );

        // Decimal
        let decimal =
            crate::row::Decimal::from_big_decimal(BigDecimal::from_str("123.45").unwrap(), 10, 2)
                .unwrap();
        let arr = write_one(&DataTypes::decimal(10, 2), Datum::Decimal(decimal));
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12345
        );

        // Timestamp NTZ (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::timestamp_with_precision(3),
            Datum::TimestampNtz(TimestampNtz::new(1_700_000_000_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000
        );

        // Timestamp LTZ (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::timestamp_ltz_with_precision(3),
            Datum::TimestampLtz(TimestampLtz::new(1_700_000_000_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000
        );
    }

    #[test]
    fn write_null_and_multiple_rows() {
        // Null
        let arr = write_one(&DataTypes::int(), Datum::Null);
        assert!(arr.is_null(0));

        // Multiple rows
        let mut w = writer_for(&DataTypes::int(), 8);
        for val in [10, 20, 30] {
            w.write_field(&GenericRow::from_data(vec![val])).unwrap();
        }
        let arr = w.finish();
        let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.len(), 3);
        assert_eq!(int_arr.value(0), 10);
        assert_eq!(int_arr.value(1), 20);
        assert_eq!(int_arr.value(2), 30);

        // finish_cloned does not reset
        let mut w = writer_for(&DataTypes::int(), 4);
        w.write_field(&GenericRow::from_data(vec![42_i32])).unwrap();
        assert_eq!(w.finish_cloned().len(), 1);
        w.write_field(&GenericRow::from_data(vec![99_i32])).unwrap();
        let int_arr = w
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!((int_arr.value(0), int_arr.value(1)), (42, 99));
    }

    #[test]
    fn write_array_type() {
        let element_type = DataTypes::int();
        let mut array_writer = FlussArrayWriter::new(3, &element_type);
        array_writer.write_int(0, 10);
        array_writer.set_null_at(1);
        array_writer.write_int(2, 30);
        let fluss_array = array_writer.complete().unwrap();

        let fluss_type = DataTypes::array(element_type);

        let arr = write_one(&fluss_type, Datum::Array(fluss_array));
        let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);
        let values = list_arr.value(0);
        let int_values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_values.len(), 3);
        assert_eq!(int_values.value(0), 10);
        assert!(int_values.is_null(1));
        assert_eq!(int_values.value(2), 30);
    }

    #[test]
    fn unsupported_type_returns_error() {
        // Map is currently unsupported in ColumnWriter
        let fluss_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let arrow_type = ArrowDataType::Boolean; // Any arrow type
        assert!(ColumnWriter::create(&fluss_type, &arrow_type, 0, 4).is_err());
    }
}
