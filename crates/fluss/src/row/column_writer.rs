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
use crate::row::InternalRow;
use crate::row::datum::{
    MICROS_PER_MILLI, MILLIS_PER_SECOND, NANOS_PER_MILLI, append_decimal_to_builder,
    millis_nanos_to_micros, millis_nanos_to_nanos,
};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
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
    /// List/Array type with nested element writer.
    List {
        element_type: DataType,
        arrow_element_type: ArrowDataType,
        element_writer: Box<ColumnWriter>,
        builder: ListBuilder<Box<dyn ArrayBuilder>>,
    },
    /// Struct/Row type with struct builder.
    Struct {
        fields: Vec<DataType>,
        arrow_fields: arrow_schema::Fields,
        builder: StructBuilder,
    },
}

/// Dispatch to the inner builder across all `TypedWriter` variants.
/// Exhaustive matching ensures new variants won't compile without an arm.
/// Note: List variant is handled separately due to its nested structure.
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
            TypedWriter::Struct { builder: $b, .. } => $body,
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
                let element_fluss_type = array_type.get_element_type().clone();
                let element_arrow_type = match arrow_type {
                    ArrowDataType::List(field) => field.data_type().clone(),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected List Arrow type for Array, got: {arrow_type:?}"
                            ),
                        });
                    }
                };

                // Create the element writer
                let element_writer =
                    ColumnWriter::create(&element_fluss_type, &element_arrow_type, 0, capacity)?;

                // Create the list builder - we use a boxed builder to allow dynamic dispatch
                let list_builder = create_list_builder(&element_arrow_type, capacity)?;

                TypedWriter::List {
                    element_type: element_fluss_type,
                    arrow_element_type: element_arrow_type,
                    element_writer: Box::new(element_writer),
                    builder: list_builder,
                }
            }
            DataType::Row(row_type) => {
                let arrow_fields = match arrow_type {
                    ArrowDataType::Struct(fields) => fields.clone(),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected Struct Arrow type for Row, got: {arrow_type:?}"
                            ),
                        });
                    }
                };

                // Create field builders for each row field
                // row_type.fields() and arrow_fields are in the same order
                let field_builders: Vec<Box<dyn ArrayBuilder>> = row_type
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(idx, _field)| create_boxed_builder(arrow_fields[idx].data_type(), capacity))
                    .collect::<Result<Vec<_>>>()?;

                let struct_builder = StructBuilder::new(arrow_fields.clone(), field_builders);

                TypedWriter::Struct {
                    fields: row_type
                        .fields()
                        .iter()
                        .map(|f| f.data_type().clone())
                        .collect(),
                    arrow_fields,
                    builder: struct_builder,
                }
            }
            DataType::Map(_) => {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Unsupported Fluss DataType (not yet implemented): {fluss_type:?}"
                    ),
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
                element_writer,
                builder,
                ..
            } => {
                let array = row.get_array(pos)?;
                write_fluss_array_to_list_builder(
                    &array,
                    element_type,
                    element_writer.as_mut(),
                    builder,
                )?;
                builder.append(true);
                Ok(())
            }
            TypedWriter::Struct {
                fields,
                arrow_fields,
                builder,
            } => {
                // Row is stored as FlussArray in CompactedRow format
                let row_array = row.get_array(pos)?;
                write_fluss_array_to_struct_builder(
                    &row_array,
                    fields,
                    arrow_fields,
                    builder,
                )?;
                Ok(())
            }
        }
    }
}

/// Helper function to create a ListBuilder for a given element Arrow type.
/// This uses boxed builders to allow dynamic dispatch for nested types.
fn create_list_builder(
    element_arrow_type: &ArrowDataType,
    capacity: usize,
) -> Result<ListBuilder<Box<dyn ArrayBuilder>>> {
    use arrow::array::*;

    let values_builder: Box<dyn ArrayBuilder> = match element_arrow_type {
        ArrowDataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        ArrowDataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        ArrowDataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        ArrowDataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        ArrowDataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        ArrowDataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        ArrowDataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Box::new(StringBuilder::with_capacity(
            capacity,
            capacity * 64,
        )),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Box::new(
            BinaryBuilder::with_capacity(capacity, capacity * 64),
        ),
        ArrowDataType::FixedSizeBinary(len) => Box::new(
            FixedSizeBinaryBuilder::with_capacity(capacity, *len),
        ),
        ArrowDataType::Decimal128(p, s) => Box::new(
            Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::IllegalArgument {
                    message: format!("Invalid decimal precision {p} or scale {s}: {e}"),
                })?,
        ),
        ArrowDataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        ArrowDataType::Time32(arrow_schema::TimeUnit::Second) => {
            Box::new(Time32SecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            Box::new(Time32MillisecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            Box::new(Time64MicrosecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            Box::new(Time64NanosecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            Box::new(TimestampSecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::with_capacity(capacity))
        }
        ArrowDataType::List(field) => {
            // Recursive case: nested list
            let inner_builder = create_list_builder(field.data_type(), capacity)?;
            Box::new(inner_builder)
        }
        ArrowDataType::Struct(fields) => {
            // Create struct builder with all field builders
            let field_builders: Vec<Box<dyn ArrayBuilder>> = fields
                .iter()
                .map(|f| create_builder_for_type(f.data_type(), capacity))
                .collect::<Result<Vec<_>>>()?;
            Box::new(StructBuilder::new(fields.clone(), field_builders))
        }
        other => {
            return Err(Error::IllegalArgument {
                message: format!("Unsupported Arrow type for List element: {other:?}"),
            });
        }
    };

    Ok(ListBuilder::with_capacity(values_builder, capacity))
}

/// Create an ArrayBuilder for a given Arrow type (helper for struct fields).
fn create_builder_for_type(
    arrow_type: &ArrowDataType,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>> {
    use arrow::array::*;

    match arrow_type {
        ArrowDataType::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
        ArrowDataType::Int8 => Ok(Box::new(Int8Builder::with_capacity(capacity))),
        ArrowDataType::Int16 => Ok(Box::new(Int16Builder::with_capacity(capacity))),
        ArrowDataType::Int32 => Ok(Box::new(Int32Builder::with_capacity(capacity))),
        ArrowDataType::Int64 => Ok(Box::new(Int64Builder::with_capacity(capacity))),
        ArrowDataType::Float32 => Ok(Box::new(Float32Builder::with_capacity(capacity))),
        ArrowDataType::Float64 => Ok(Box::new(Float64Builder::with_capacity(capacity))),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(Box::new(
            StringBuilder::with_capacity(capacity, capacity * 64),
        )),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(Box::new(
            BinaryBuilder::with_capacity(capacity, capacity * 64),
        )),
        ArrowDataType::FixedSizeBinary(len) => Ok(Box::new(
            FixedSizeBinaryBuilder::with_capacity(capacity, *len),
        )),
        ArrowDataType::Date32 => Ok(Box::new(Date32Builder::with_capacity(capacity))),
        _ => Err(Error::IllegalArgument {
            message: format!("Unsupported Arrow type for struct field: {arrow_type:?}"),
        }),
    }
}

/// Create a boxed ArrayBuilder for a given Arrow type (for use in ListBuilder and StructBuilder).
fn create_boxed_builder(
    arrow_type: &ArrowDataType,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>> {
    use arrow::array::*;

    match arrow_type {
        ArrowDataType::Boolean => Ok(Box::new(BooleanBuilder::with_capacity(capacity))),
        ArrowDataType::Int8 => Ok(Box::new(Int8Builder::with_capacity(capacity))),
        ArrowDataType::Int16 => Ok(Box::new(Int16Builder::with_capacity(capacity))),
        ArrowDataType::Int32 => Ok(Box::new(Int32Builder::with_capacity(capacity))),
        ArrowDataType::Int64 => Ok(Box::new(Int64Builder::with_capacity(capacity))),
        ArrowDataType::Float32 => Ok(Box::new(Float32Builder::with_capacity(capacity))),
        ArrowDataType::Float64 => Ok(Box::new(Float64Builder::with_capacity(capacity))),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(Box::new(
            StringBuilder::with_capacity(capacity, capacity * 64),
        )),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(Box::new(
            BinaryBuilder::with_capacity(capacity, capacity * 64),
        )),
        ArrowDataType::FixedSizeBinary(len) => Ok(Box::new(
            FixedSizeBinaryBuilder::with_capacity(capacity, *len),
        )),
        ArrowDataType::Decimal128(p, s) => Ok(Box::new(
            Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::IllegalArgument {
                    message: format!("Invalid decimal precision {p} or scale {s}: {e}"),
                })?,
        )),
        ArrowDataType::Date32 => Ok(Box::new(Date32Builder::with_capacity(capacity))),
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
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            Ok(Box::new(TimestampSecondBuilder::with_capacity(capacity)))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            Ok(Box::new(TimestampMillisecondBuilder::with_capacity(capacity)))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            Ok(Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            Ok(Box::new(TimestampNanosecondBuilder::with_capacity(capacity)))
        }
        ArrowDataType::List(_) => Err(Error::IllegalArgument {
            message: "Nested List type should use create_list_builder".to_string(),
        }),
        ArrowDataType::Struct(_) => Err(Error::IllegalArgument {
            message: "Nested Struct type should be handled separately".to_string(),
        }),
        other => Err(Error::IllegalArgument {
            message: format!("Unsupported Arrow type for boxed builder: {other:?}"),
        }),
    }
}

/// Write a FlussArray to a ListBuilder.
/// This recursively writes each element using the appropriate ColumnWriter.
fn write_fluss_array_to_list_builder(
    array: &crate::row::FlussArray,
    element_type: &DataType,
    element_writer: &mut ColumnWriter,
    list_builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    // Write each element of the FlussArray
    for i in 0..array.size() {
        if array.is_null_at(i) {
            // Append null to the element builder
            append_null_to_builder(list_builder.values());
        } else {
            // Create a temporary row that wraps the array element and write it
            let element_row = ArrayElementRow::new(array.clone(), i);
            element_writer.write_field(&element_row)?;
        }
    }
    Ok(())
}

/// Helper function to append null to a boxed ArrayBuilder.
fn append_null_to_builder(builder: &mut dyn ArrayBuilder) {
    // This is a workaround since we can't directly call append_null on dyn ArrayBuilder
    // We use Any to try to downcast to known builder types
    use arrow::array::*;
    use std::any::Any;

    if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int8Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int16Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
        b.append_null();
    } else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        b.append_null();
    } else {
        // For complex types, try to use the builder's finish_cloned and ignore result
        // This is not ideal but works for our use case
        let _ = builder.finish_cloned();
    }
}

/// Write a FlussArray (representing a Row) to a StructBuilder.
fn write_fluss_array_to_struct_builder(
    row_array: &crate::row::FlussArray,
    fields: &[DataType],
    _arrow_fields: &arrow_schema::Fields,
    struct_builder: &mut StructBuilder,
) -> Result<()> {
    use arrow::array::*;

    // We need to write each field of the row to the corresponding field builder
    // For each field, we downcast to the specific builder type based on the DataType
    for (idx, field_type) in fields.iter().enumerate() {
        if row_array.is_null_at(idx) {
            // Append null using a helper that tries all common types
            append_null_to_struct_field(struct_builder, idx, field_type)?;
        } else {
            // Write the field value based on type
            write_field_to_struct_builder(row_array, idx, field_type, struct_builder)?;
        }
    }

    // Append this struct row (marking it as non-null)
    struct_builder.append(true);
    Ok(())
}

/// Append null to a specific field of a StructBuilder based on the field type.
fn append_null_to_struct_field(
    struct_builder: &mut StructBuilder,
    idx: usize,
    field_type: &DataType,
) -> Result<()> {
    use arrow::array::*;

    // Based on the field type, downcast to the specific builder and append null
    macro_rules! try_append_null {
        ($builder_type:ty) => {
            if let Some(b) = struct_builder.field_builder::<$builder_type>(idx) {
                b.append_null();
                return Ok(());
            }
        };
    }

    match field_type {
        DataType::Boolean(_) => try_append_null!(BooleanBuilder),
        DataType::TinyInt(_) => try_append_null!(Int8Builder),
        DataType::SmallInt(_) => try_append_null!(Int16Builder),
        DataType::Int(_) => try_append_null!(Int32Builder),
        DataType::BigInt(_) => try_append_null!(Int64Builder),
        DataType::Float(_) => try_append_null!(Float32Builder),
        DataType::Double(_) => try_append_null!(Float64Builder),
        DataType::String(_) | DataType::Char(_) => try_append_null!(StringBuilder),
        DataType::Bytes(_) | DataType::Binary(_) => try_append_null!(BinaryBuilder),
        DataType::Date(_) => try_append_null!(Date32Builder),
        DataType::Decimal(_) => try_append_null!(Decimal128Builder),
        DataType::Time(_) => try_append_null!(Int32Builder), // Time stored as millis
        DataType::Timestamp(_) => try_append_null!(TimestampMillisecondBuilder),
        DataType::TimestampLTz(_) => try_append_null!(TimestampMillisecondBuilder),
        DataType::Array(_) | DataType::Row(_) => try_append_null!(ListBuilder<Box<dyn ArrayBuilder>>),
        DataType::Map(_) => {
            return Err(Error::IllegalArgument {
                message: "Map type in struct not supported".to_string(),
            });
        }
    }

    // If we reach here, the downcast failed
    Err(Error::IllegalArgument {
        message: format!("Failed to append null to field {idx} with type {field_type:?}"),
    })
}

/// Write a single field value from FlussArray to a StructBuilder field.
fn write_field_to_struct_builder(
    row_array: &crate::row::FlussArray,
    idx: usize,
    field_type: &DataType,
    struct_builder: &mut StructBuilder,
) -> Result<()> {
    use arrow::array::*;

    macro_rules! write_primitive {
        ($builder_type:ty, $get_method:ident, $value_type:ty) => {
            if let Some(b) = struct_builder.field_builder::<$builder_type>(idx) {
                let value: $value_type = row_array.$get_method(idx)?;
                b.append_value(value);
                return Ok(());
            }
        };
    }

    macro_rules! write_decimal {
        () => {
            if let Some(b) = struct_builder.field_builder::<Decimal128Builder>(idx) {
                let decimal_type = match field_type {
                    DataType::Decimal(dt) => dt,
                    _ => return Err(Error::IllegalArgument {
                        message: "Expected Decimal type".to_string(),
                    }),
                };
                let decimal = row_array.get_decimal(idx, decimal_type.precision(), decimal_type.scale())?;
                append_decimal_to_builder(&decimal, decimal_type.precision() as u32, decimal_type.scale() as i64, b)?;
                return Ok(());
            }
        };
    }

    // Helper macro to write primitive types
    macro_rules! try_write_primitive {
        ($builder_type:ty, $get_method:ident) => {
            if let Some(b) = struct_builder.field_builder::<$builder_type>(idx) {
                b.append_value(row_array.$get_method(idx)?);
                return Ok(());
            }
        };
    }

    // Try each type in order
    try_write_primitive!(BooleanBuilder, get_boolean);
    try_write_primitive!(Int8Builder, get_byte);
    try_write_primitive!(Int16Builder, get_short);
    try_write_primitive!(Int32Builder, get_int);
    try_write_primitive!(Int64Builder, get_long);
    try_write_primitive!(Float32Builder, get_float);
    try_write_primitive!(Float64Builder, get_double);

    // String/Char
    if let Some(b) = struct_builder.field_builder::<StringBuilder>(idx) {
        b.append_value(row_array.get_string(idx)?);
        return Ok(());
    }

    // Binary/Bytes
    if let Some(b) = struct_builder.field_builder::<BinaryBuilder>(idx) {
        b.append_value(row_array.get_binary(idx)?);
        return Ok(());
    }

    // Date
    if let Some(b) = struct_builder.field_builder::<Date32Builder>(idx) {
        b.append_value(row_array.get_date(idx)?.get_inner());
        return Ok(());
    }

    // Decimal
    if let Some(b) = struct_builder.field_builder::<Decimal128Builder>(idx) {
        let decimal_type = match field_type {
            DataType::Decimal(dt) => dt,
            _ => return Err(Error::IllegalArgument {
                message: "Expected Decimal type".to_string(),
            }),
        };
        let decimal = row_array.get_decimal(idx, decimal_type.precision(), decimal_type.scale())?;
        append_decimal_to_builder(&decimal, decimal_type.precision() as u32, decimal_type.scale() as i64, b)?;
        return Ok(());
    }

    // Time (stored as Int32 milliseconds)
    if let Some(b) = struct_builder.field_builder::<Int32Builder>(idx) {
        b.append_value(row_array.get_time(idx)?.get_inner());
        return Ok(());
    }

    // Timestamp - try different precision levels
    if let Some(b) = struct_builder.field_builder::<TimestampMillisecondBuilder>(idx) {
        let ts = row_array.get_timestamp_ntz(idx, 3)?;
        b.append_value(ts.get_millisecond());
        return Ok(());
    }
    if let Some(b) = struct_builder.field_builder::<TimestampMicrosecondBuilder>(idx) {
        let ts = row_array.get_timestamp_ntz(idx, 6)?;
        b.append_value(millis_nanos_to_micros(ts.get_millisecond(), ts.get_nano_of_millisecond())?);
        return Ok(());
    }

    // TimestampLTz
    if let Some(b) = struct_builder.field_builder::<TimestampMillisecondBuilder>(idx) {
        let ts = row_array.get_timestamp_ltz(idx, 3)?;
        b.append_value(ts.get_epoch_millisecond());
        return Ok(());
    }

    // Nested types - not fully supported yet
    match field_type {
        DataType::Array(_) => {
            return Err(Error::IllegalArgument {
                message: "Nested arrays in structs not yet fully supported".to_string(),
            });
        }
        DataType::Row(_) => {
            return Err(Error::IllegalArgument {
                message: "Nested structs not yet fully supported".to_string(),
            });
        }
        DataType::Map(_) => {
            return Err(Error::IllegalArgument {
                message: "Map type in struct not yet supported".to_string(),
            });
        }
        _ => {
            // Other types should have been handled above
            return Err(Error::IllegalArgument {
                message: format!("Failed to write field {idx} with type {field_type:?} - no matching builder found"),
            });
        }
    }
}

/// A wrapper that treats a FlussArray element as an InternalRow for writing.
/// This allows us to reuse the ColumnWriter infrastructure for nested elements.
struct ArrayElementRow {
    array: crate::row::FlussArray,
    index: usize,
}

impl ArrayElementRow {
    fn new(array: crate::row::FlussArray, index: usize) -> Self {
        Self { array, index }
    }
}

impl crate::row::InternalRow for ArrayElementRow {
    fn get_field_count(&self) -> usize {
        1
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        if pos == 0 {
            Ok(self.array.is_null_at(self.index))
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        if pos == 0 {
            self.array.get_boolean(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        if pos == 0 {
            self.array.get_byte(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        if pos == 0 {
            self.array.get_short(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        if pos == 0 {
            self.array.get_int(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        if pos == 0 {
            self.array.get_long(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        if pos == 0 {
            self.array.get_float(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        if pos == 0 {
            self.array.get_double(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        if pos == 0 {
            self.array.get_string(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_char(&self, pos: usize, _len: usize) -> Result<&str> {
        self.get_string(pos)
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        if pos == 0 {
            self.array.get_binary(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_binary(&self, pos: usize, _len: usize) -> Result<&[u8]> {
        self.get_bytes(pos)
    }

    fn get_decimal(
        &self,
        pos: usize,
        precision: usize,
        scale: usize,
    ) -> Result<crate::row::Decimal> {
        if pos == 0 {
            self.array.get_decimal(
                self.index,
                precision as u32,
                scale as u32,
            )
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_date(&self, pos: usize) -> Result<crate::row::datum::Date> {
        if pos == 0 {
            self.array.get_date(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_time(&self, pos: usize) -> Result<crate::row::datum::Time> {
        if pos == 0 {
            self.array.get_time(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_timestamp_ntz(
        &self,
        pos: usize,
        precision: u32,
    ) -> Result<crate::row::datum::TimestampNtz> {
        if pos == 0 {
            self.array.get_timestamp_ntz(self.index, precision)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_timestamp_ltz(
        &self,
        pos: usize,
        precision: u32,
    ) -> Result<crate::row::datum::TimestampLtz> {
        if pos == 0 {
            self.array.get_timestamp_ltz(self.index, precision)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }

    fn get_array(&self, pos: usize) -> Result<crate::row::FlussArray> {
        if pos == 0 {
            self.array.get_array(self.index)
        } else {
            Err(Error::IllegalArgument {
                message: format!("Array element row has only one field, got position {pos}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::record::to_arrow_type;
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
    fn array_type_supported() {
        // Array type is now supported
        let fluss_type = DataTypes::array(DataTypes::int());
        let arrow_type = ArrowDataType::List(arrow_schema::FieldRef::new(
            arrow_schema::Field::new("item", ArrowDataType::Int32, true),
        ));
        assert!(
            ColumnWriter::create(&fluss_type, &arrow_type, 0, 4).is_ok(),
            "Array type should be supported now"
        );
    }

    #[test]
    fn nested_array_type_supported() {
        // Nested array type: ARRAY<ARRAY<INT>>
        let inner_array = DataTypes::array(DataTypes::int());
        let fluss_type = DataTypes::array(inner_array);
        let arrow_type = ArrowDataType::List(arrow_schema::FieldRef::new(
            arrow_schema::Field::new(
                "item",
                ArrowDataType::List(arrow_schema::FieldRef::new(
                    arrow_schema::Field::new("inner", ArrowDataType::Int32, true),
                )),
                true,
            ),
        ));
        assert!(
            ColumnWriter::create(&fluss_type, &arrow_type, 0, 4).is_ok(),
            "Nested array type should be supported"
        );
    }

    #[test]
    fn row_type_supported() {
        // Row type is now supported
        let row_type = DataTypes::row(vec![
            DataTypes::field("id", DataTypes::int()),
            DataTypes::field("name", DataTypes::string()),
        ]);
        let row_arrow = ArrowDataType::Struct(arrow_schema::Fields::from(vec![
            arrow_schema::Field::new("id", ArrowDataType::Int32, true),
            arrow_schema::Field::new("name", ArrowDataType::Utf8, true),
        ]));
        assert!(
            ColumnWriter::create(&row_type, &row_arrow, 0, 4).is_ok(),
            "Row type should now be supported"
        );
    }

    #[test]
    fn map_type_not_yet_supported() {
        // Map type is still not supported
        let map_type = DataTypes::map(DataTypes::string(), DataTypes::int());
        let map_arrow = ArrowDataType::Map(
            arrow_schema::FieldRef::new(
                arrow_schema::Field::new(
                    "entries",
                    ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                        arrow_schema::Field::new("key", ArrowDataType::Utf8, true),
                        arrow_schema::Field::new("value", ArrowDataType::Int32, true),
                    ])),
                    true,
                ),
            ),
            false,
        );
        assert!(
            ColumnWriter::create(&map_type, &map_arrow, 0, 4).is_err(),
            "Map type should not yet be supported"
        );
    }
}
