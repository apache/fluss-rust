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

use crate::ffi;
use anyhow::{Result, anyhow};
use arrow::array::{
    Date32Array, Decimal128Array, LargeBinaryArray, LargeStringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use fcore::row::InternalRow;
use fluss as fcore;
use std::borrow::Cow;
use std::str::FromStr;

use arrow::array::Array;

pub const DATA_TYPE_BOOLEAN: i32 = 1;
pub const DATA_TYPE_TINYINT: i32 = 2;
pub const DATA_TYPE_SMALLINT: i32 = 3;
pub const DATA_TYPE_INT: i32 = 4;
pub const DATA_TYPE_BIGINT: i32 = 5;
pub const DATA_TYPE_FLOAT: i32 = 6;
pub const DATA_TYPE_DOUBLE: i32 = 7;
pub const DATA_TYPE_STRING: i32 = 8;
pub const DATA_TYPE_BYTES: i32 = 9;
pub const DATA_TYPE_DATE: i32 = 10;
pub const DATA_TYPE_TIME: i32 = 11;
pub const DATA_TYPE_TIMESTAMP: i32 = 12;
pub const DATA_TYPE_TIMESTAMP_LTZ: i32 = 13;
pub const DATA_TYPE_DECIMAL: i32 = 14;

pub const DATUM_TYPE_NULL: i32 = 0;
pub const DATUM_TYPE_BOOL: i32 = 1;
pub const DATUM_TYPE_INT32: i32 = 2;
pub const DATUM_TYPE_INT64: i32 = 3;
pub const DATUM_TYPE_FLOAT32: i32 = 4;
pub const DATUM_TYPE_FLOAT64: i32 = 5;
pub const DATUM_TYPE_STRING: i32 = 6;
pub const DATUM_TYPE_BYTES: i32 = 7;
pub const DATUM_TYPE_DECIMAL_I64: i32 = 8;
pub const DATUM_TYPE_DECIMAL_I128: i32 = 9;
pub const DATUM_TYPE_DECIMAL_STRING: i32 = 10;
pub const DATUM_TYPE_DATE: i32 = 11;
pub const DATUM_TYPE_TIME: i32 = 12;
pub const DATUM_TYPE_TIMESTAMP_NTZ: i32 = 13;
pub const DATUM_TYPE_TIMESTAMP_LTZ: i32 = 14;

const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

fn ffi_data_type_to_core(dt: i32, precision: u32, scale: u32) -> Result<fcore::metadata::DataType> {
    match dt {
        DATA_TYPE_BOOLEAN => Ok(fcore::metadata::DataTypes::boolean()),
        DATA_TYPE_TINYINT => Ok(fcore::metadata::DataTypes::tinyint()),
        DATA_TYPE_SMALLINT => Ok(fcore::metadata::DataTypes::smallint()),
        DATA_TYPE_INT => Ok(fcore::metadata::DataTypes::int()),
        DATA_TYPE_BIGINT => Ok(fcore::metadata::DataTypes::bigint()),
        DATA_TYPE_FLOAT => Ok(fcore::metadata::DataTypes::float()),
        DATA_TYPE_DOUBLE => Ok(fcore::metadata::DataTypes::double()),
        DATA_TYPE_STRING => Ok(fcore::metadata::DataTypes::string()),
        DATA_TYPE_BYTES => Ok(fcore::metadata::DataTypes::bytes()),
        DATA_TYPE_DATE => Ok(fcore::metadata::DataTypes::date()),
        DATA_TYPE_TIME => Ok(fcore::metadata::DataTypes::time()),
        DATA_TYPE_TIMESTAMP => Ok(fcore::metadata::DataTypes::timestamp_with_precision(
            precision,
        )),
        DATA_TYPE_TIMESTAMP_LTZ => Ok(fcore::metadata::DataTypes::timestamp_ltz_with_precision(
            precision,
        )),
        DATA_TYPE_DECIMAL => {
            let dt = fcore::metadata::DecimalType::new(precision, scale)?;
            Ok(fcore::metadata::DataType::Decimal(dt))
        }
        _ => Err(anyhow!("Unknown data type: {dt}")),
    }
}

fn core_data_type_to_ffi(dt: &fcore::metadata::DataType) -> i32 {
    match dt {
        fcore::metadata::DataType::Boolean(_) => DATA_TYPE_BOOLEAN,
        fcore::metadata::DataType::TinyInt(_) => DATA_TYPE_TINYINT,
        fcore::metadata::DataType::SmallInt(_) => DATA_TYPE_SMALLINT,
        fcore::metadata::DataType::Int(_) => DATA_TYPE_INT,
        fcore::metadata::DataType::BigInt(_) => DATA_TYPE_BIGINT,
        fcore::metadata::DataType::Float(_) => DATA_TYPE_FLOAT,
        fcore::metadata::DataType::Double(_) => DATA_TYPE_DOUBLE,
        fcore::metadata::DataType::String(_) => DATA_TYPE_STRING,
        fcore::metadata::DataType::Bytes(_) => DATA_TYPE_BYTES,
        fcore::metadata::DataType::Date(_) => DATA_TYPE_DATE,
        fcore::metadata::DataType::Time(_) => DATA_TYPE_TIME,
        fcore::metadata::DataType::Timestamp(_) => DATA_TYPE_TIMESTAMP,
        fcore::metadata::DataType::TimestampLTz(_) => DATA_TYPE_TIMESTAMP_LTZ,
        fcore::metadata::DataType::Decimal(_) => DATA_TYPE_DECIMAL,
        _ => 0,
    }
}

pub fn ffi_descriptor_to_core(
    descriptor: &ffi::FfiTableDescriptor,
) -> Result<fcore::metadata::TableDescriptor> {
    let mut schema_builder = fcore::metadata::Schema::builder();

    for col in &descriptor.schema.columns {
        if col.precision < 0 || col.scale < 0 {
            return Err(anyhow!(
                "Column '{}': precision and scale must be non-negative",
                col.name
            ));
        }
        let dt = ffi_data_type_to_core(col.data_type, col.precision as u32, col.scale as u32)?;
        schema_builder = schema_builder.column(&col.name, dt);
        if !col.comment.is_empty() {
            schema_builder = schema_builder.with_comment(&col.comment);
        }
    }

    if !descriptor.schema.primary_keys.is_empty() {
        schema_builder = schema_builder.primary_key(descriptor.schema.primary_keys.clone());
    }

    let schema = schema_builder.build()?;

    let mut builder = fcore::metadata::TableDescriptor::builder()
        .schema(schema)
        .partitioned_by(descriptor.partition_keys.clone());

    if descriptor.bucket_count > 0 {
        builder = builder.distributed_by(
            Some(descriptor.bucket_count),
            descriptor.bucket_keys.clone(),
        );
    } else {
        builder = builder.distributed_by(None, descriptor.bucket_keys.clone());
    }

    for prop in &descriptor.properties {
        builder = builder.property(&prop.key, &prop.value);
    }

    if !descriptor.comment.is_empty() {
        builder = builder.comment(&descriptor.comment);
    }

    Ok(builder.build()?)
}

pub fn core_table_info_to_ffi(info: &fcore::metadata::TableInfo) -> ffi::FfiTableInfo {
    let schema = info.get_schema();
    let columns: Vec<ffi::FfiColumn> = schema
        .columns()
        .iter()
        .map(|col| {
            let (precision, scale) = match col.data_type() {
                fcore::metadata::DataType::Decimal(dt) => {
                    (dt.precision() as i32, dt.scale() as i32)
                }
                fcore::metadata::DataType::Timestamp(dt) => (dt.precision() as i32, 0),
                fcore::metadata::DataType::TimestampLTz(dt) => (dt.precision() as i32, 0),
                _ => (0, 0),
            };
            ffi::FfiColumn {
                name: col.name().to_string(),
                data_type: core_data_type_to_ffi(col.data_type()),
                comment: col.comment().unwrap_or("").to_string(),
                precision,
                scale,
            }
        })
        .collect();

    let primary_keys: Vec<String> = schema
        .primary_key()
        .map(|pk| pk.column_names().to_vec())
        .unwrap_or_default();

    let properties: Vec<ffi::HashMapValue> = info
        .get_properties()
        .iter()
        .map(|(k, v)| ffi::HashMapValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();

    ffi::FfiTableInfo {
        table_id: info.get_table_id(),
        schema_id: info.get_schema_id(),
        table_path: ffi::FfiTablePath {
            database_name: info.get_table_path().database().to_string(),
            table_name: info.get_table_path().table().to_string(),
        },
        created_time: info.get_created_time(),
        modified_time: info.get_modified_time(),
        primary_keys: info.get_primary_keys().clone(),
        bucket_keys: info.get_bucket_keys().to_vec(),
        partition_keys: info.get_partition_keys().to_vec(),
        num_buckets: info.get_num_buckets(),
        has_primary_key: info.has_primary_key(),
        is_partitioned: info.is_partitioned(),
        properties,
        comment: info.get_comment().unwrap_or("").to_string(),
        schema: ffi::FfiSchema {
            columns,
            primary_keys,
        },
    }
}

pub fn empty_table_info() -> ffi::FfiTableInfo {
    ffi::FfiTableInfo {
        table_id: 0,
        schema_id: 0,
        table_path: ffi::FfiTablePath {
            database_name: String::new(),
            table_name: String::new(),
        },
        created_time: 0,
        modified_time: 0,
        primary_keys: vec![],
        bucket_keys: vec![],
        partition_keys: vec![],
        num_buckets: 0,
        has_primary_key: false,
        is_partitioned: false,
        properties: vec![],
        comment: String::new(),
        schema: ffi::FfiSchema {
            columns: vec![],
            primary_keys: vec![],
        },
    }
}

/// Convert FFI database descriptor to core. Returns None if descriptor is effectively empty
/// (no comment and no properties), so create_database can pass Option::None to core.
pub fn ffi_database_descriptor_to_core(
    d: &ffi::FfiDatabaseDescriptor,
) -> Option<fcore::metadata::DatabaseDescriptor> {
    if d.comment.is_empty() && d.properties.is_empty() {
        return None;
    }
    let mut builder = fcore::metadata::DatabaseDescriptor::builder();
    if !d.comment.is_empty() {
        builder = builder.comment(&d.comment);
    }
    if !d.properties.is_empty() {
        let props: std::collections::HashMap<String, String> = d
            .properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        builder = builder.custom_properties(props);
    }
    Some(builder.build())
}

/// Convert core DatabaseInfo to FFI.
pub fn core_database_info_to_ffi(info: &fcore::metadata::DatabaseInfo) -> ffi::FfiDatabaseInfo {
    let desc = info.database_descriptor();
    let properties: Vec<ffi::HashMapValue> = desc
        .custom_properties()
        .iter()
        .map(|(k, v)| ffi::HashMapValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();
    ffi::FfiDatabaseInfo {
        database_name: info.database_name().to_string(),
        comment: desc.comment().unwrap_or("").to_string(),
        properties,
        created_time: info.created_time(),
        modified_time: info.modified_time(),
    }
}

/// Look up decimal (precision, scale) from schema for column `idx`.
fn get_decimal_type(idx: usize, schema: Option<&fcore::metadata::Schema>) -> Result<(u32, u32)> {
    let col = schema
        .and_then(|s| s.columns().get(idx))
        .ok_or_else(|| anyhow!("Schema not available for decimal column {idx}"))?;
    match col.data_type() {
        fcore::metadata::DataType::Decimal(dt) => Ok((dt.precision(), dt.scale())),
        other => Err(anyhow!("Column {idx} is {other:?}, not Decimal")),
    }
}

pub fn ffi_row_to_core<'a>(
    row: &'a ffi::FfiGenericRow,
    schema: Option<&fcore::metadata::Schema>,
) -> Result<fcore::row::GenericRow<'a>> {
    use fcore::row::Datum;

    let mut generic_row = fcore::row::GenericRow::new(row.fields.len());

    for (idx, field) in row.fields.iter().enumerate() {
        let datum = match field.datum_type {
            DATUM_TYPE_NULL => Datum::Null,
            DATUM_TYPE_BOOL => Datum::Bool(field.bool_val),
            DATUM_TYPE_INT32 => Datum::Int32(field.i32_val),
            DATUM_TYPE_INT64 => Datum::Int64(field.i64_val),
            DATUM_TYPE_FLOAT32 => Datum::Float32(field.f32_val.into()),
            DATUM_TYPE_FLOAT64 => Datum::Float64(field.f64_val.into()),
            DATUM_TYPE_STRING => Datum::String(Cow::Borrowed(field.string_val.as_str())),
            DATUM_TYPE_BYTES => Datum::Blob(Cow::Borrowed(field.bytes_val.as_slice())),
            DATUM_TYPE_DECIMAL_STRING => {
                let (precision, scale) = get_decimal_type(idx, schema)?;
                let bd =
                    bigdecimal::BigDecimal::from_str(field.string_val.as_str()).map_err(|e| {
                        anyhow!(
                            "Column {idx}: invalid decimal string '{}': {e}",
                            field.string_val
                        )
                    })?;
                let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                    .map_err(|e| anyhow!("Column {idx}: {e}"))?;
                Datum::Decimal(decimal)
            }
            DATUM_TYPE_DECIMAL_I64 => {
                let precision = field.decimal_precision as u32;
                let scale = field.decimal_scale as u32;
                let decimal =
                    fcore::row::Decimal::from_unscaled_long(field.i64_val, precision, scale)
                        .map_err(|e| anyhow!("Column {idx}: {e}"))?;
                Datum::Decimal(decimal)
            }
            DATUM_TYPE_DECIMAL_I128 => {
                let precision = field.decimal_precision as u32;
                let scale = field.decimal_scale as u32;
                let i128_val = ((field.i128_hi as i128) << 64) | (field.i128_lo as u64 as i128);
                let decimal = fcore::row::Decimal::from_arrow_decimal128(
                    i128_val,
                    scale as i64,
                    precision,
                    scale,
                )
                .map_err(|e| anyhow!("Column {idx}: {e}"))?;
                Datum::Decimal(decimal)
            }
            DATUM_TYPE_DATE => Datum::Date(fcore::row::Date::new(field.i32_val)),
            DATUM_TYPE_TIME => Datum::Time(fcore::row::Time::new(field.i32_val)),
            DATUM_TYPE_TIMESTAMP_NTZ => Datum::TimestampNtz(
                fcore::row::TimestampNtz::from_millis_nanos(field.i64_val, field.i32_val)
                    .unwrap_or_else(|_| fcore::row::TimestampNtz::new(field.i64_val)),
            ),
            DATUM_TYPE_TIMESTAMP_LTZ => Datum::TimestampLtz(
                fcore::row::TimestampLtz::from_millis_nanos(field.i64_val, field.i32_val)
                    .unwrap_or_else(|_| fcore::row::TimestampLtz::new(field.i64_val)),
            ),
            other => return Err(anyhow!("Column {idx}: unknown datum type {other}")),
        };
        generic_row.set_field(idx, datum);
    }

    Ok(generic_row)
}

pub fn core_scan_records_to_ffi(
    records: &fcore::record::ScanRecords,
    columns: &[fcore::metadata::Column],
) -> ffi::FfiScanRecords {
    let mut ffi_records = Vec::new();

    // Iterate over all buckets and their records
    for (table_bucket, bucket_records) in records.records_by_buckets() {
        let bucket_id = table_bucket.bucket_id();
        for record in bucket_records {
            let row = record.row();
            let fields = core_row_to_ffi_fields(row, columns);

            ffi_records.push(ffi::FfiScanRecord {
                bucket_id,
                offset: record.offset(),
                timestamp: record.timestamp(),
                row: ffi::FfiGenericRow { fields },
            });
        }
    }

    ffi::FfiScanRecords {
        records: ffi_records,
    }
}

fn core_row_to_ffi_fields(
    row: &fcore::row::ColumnarRow,
    columns: &[fcore::metadata::Column],
) -> Vec<ffi::FfiDatum> {
    let record_batch = row.get_record_batch();
    let schema = record_batch.schema();
    let row_id = row.get_row_id();

    let mut fields = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        if row.is_null_at(i) {
            fields.push(ffi::FfiDatum::default());
            continue;
        }

        let datum = match field.data_type() {
            ArrowDataType::Boolean => ffi::FfiDatum {
                datum_type: DATUM_TYPE_BOOL,
                bool_val: row.get_boolean(i),
                ..Default::default()
            },
            ArrowDataType::Int8 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_byte(i) as i32,
                ..Default::default()
            },
            ArrowDataType::Int16 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_short(i) as i32,
                ..Default::default()
            },
            ArrowDataType::Int32 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_int(i),
                ..Default::default()
            },
            ArrowDataType::Int64 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT64,
                i64_val: row.get_long(i),
                ..Default::default()
            },
            ArrowDataType::Float32 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_FLOAT32,
                f32_val: row.get_float(i),
                ..Default::default()
            },
            ArrowDataType::Float64 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_FLOAT64,
                f64_val: row.get_double(i),
                ..Default::default()
            },
            ArrowDataType::Utf8 => ffi::FfiDatum {
                datum_type: DATUM_TYPE_STRING,
                string_val: row.get_string(i).to_string(),
                ..Default::default()
            },
            ArrowDataType::LargeUtf8 => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("LargeUtf8 column expected");
                ffi::FfiDatum {
                    datum_type: DATUM_TYPE_STRING,
                    string_val: array.value(row_id).to_string(),
                    ..Default::default()
                }
            }
            ArrowDataType::Binary => ffi::FfiDatum {
                datum_type: DATUM_TYPE_BYTES,
                bytes_val: row.get_bytes(i).to_vec(),
                ..Default::default()
            },
            ArrowDataType::FixedSizeBinary(len) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_BYTES,
                bytes_val: row.get_binary(i, *len as usize).to_vec(),
                ..Default::default()
            },
            ArrowDataType::LargeBinary => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("LargeBinary column expected");
                ffi::FfiDatum {
                    datum_type: DATUM_TYPE_BYTES,
                    bytes_val: array.value(row_id).to_vec(),
                    ..Default::default()
                }
            }
            ArrowDataType::Date32 => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("Date32 column expected");
                ffi::FfiDatum {
                    datum_type: DATUM_TYPE_DATE,
                    i32_val: array.value(row_id),
                    ..Default::default()
                }
            }
            ArrowDataType::Timestamp(unit, _tz) => {
                let datum_type = match columns.get(i).map(|c| c.data_type()) {
                    Some(fcore::metadata::DataType::TimestampLTz(_)) => DATUM_TYPE_TIMESTAMP_LTZ,
                    _ => DATUM_TYPE_TIMESTAMP_NTZ,
                };
                let mut datum = ffi::FfiDatum {
                    datum_type,
                    ..Default::default()
                };
                match unit {
                    TimeUnit::Second => {
                        let array = record_batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .expect("Timestamp(second) column expected");
                        datum.i64_val = array.value(row_id) * MILLIS_PER_SECOND;
                    }
                    TimeUnit::Millisecond => {
                        let array = record_batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .expect("Timestamp(millisecond) column expected");
                        datum.i64_val = array.value(row_id);
                    }
                    TimeUnit::Microsecond => {
                        let array = record_batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .expect("Timestamp(microsecond) column expected");
                        let micros = array.value(row_id);
                        datum.i64_val = micros.div_euclid(MICROS_PER_MILLI);
                        datum.i32_val =
                            (micros.rem_euclid(MICROS_PER_MILLI) * NANOS_PER_MICRO) as i32;
                    }
                    TimeUnit::Nanosecond => {
                        let array = record_batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .expect("Timestamp(nanosecond) column expected");
                        let nanos = array.value(row_id);
                        datum.i64_val = nanos.div_euclid(NANOS_PER_MILLI);
                        datum.i32_val = nanos.rem_euclid(NANOS_PER_MILLI) as i32;
                    }
                }
                datum
            }
            ArrowDataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .expect("Time32(second) column expected");
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_TIME,
                        i32_val: array.value(row_id) * MILLIS_PER_SECOND as i32,
                        ..Default::default()
                    }
                }
                TimeUnit::Millisecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .expect("Time32(millisecond) column expected");
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_TIME,
                        i32_val: array.value(row_id),
                        ..Default::default()
                    }
                }
                _ => panic!("Will never come here. Unsupported Time32 unit for column {i}"),
            },
            ArrowDataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .expect("Time64(microsecond) column expected");
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_TIME,
                        i32_val: (array.value(row_id) / MICROS_PER_MILLI) as i32,
                        ..Default::default()
                    }
                }
                TimeUnit::Nanosecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .expect("Time64(nanosecond) column expected");
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_TIME,
                        i32_val: (array.value(row_id) / NANOS_PER_MILLI) as i32,
                        ..Default::default()
                    }
                }
                _ => panic!("Will never come here. Unsupported Time64 unit for column {i}"),
            },
            ArrowDataType::Decimal128(precision, scale) => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Decimal128 column expected");
                let i128_val = array.value(row_id);

                if fcore::row::Decimal::is_compact_precision(*precision as u32) {
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_DECIMAL_I64,
                        i64_val: i128_val as i64,
                        decimal_precision: *precision as i32,
                        decimal_scale: *scale as i32,
                        ..Default::default()
                    }
                } else {
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_DECIMAL_I128,
                        i128_hi: (i128_val >> 64) as i64,
                        i128_lo: i128_val as i64,
                        decimal_precision: *precision as i32,
                        decimal_scale: *scale as i32,
                        ..Default::default()
                    }
                }
            }
            other => panic!(
                "Will never come here. Unsupported Arrow data type for column {i}: {other:?}"
            ),
        };

        fields.push(datum);
    }

    fields
}

impl Default for ffi::FfiDatum {
    fn default() -> Self {
        Self {
            datum_type: DATUM_TYPE_NULL,
            bool_val: false,
            i32_val: 0,
            i64_val: 0,
            f32_val: 0.0,
            f64_val: 0.0,
            string_val: String::new(),
            bytes_val: vec![],
            decimal_precision: 0,
            decimal_scale: 0,
            i128_hi: 0,
            i128_lo: 0,
        }
    }
}

/// Convert any InternalRow to FfiGenericRow using Fluss schema metadata.
/// Used for lookup results (CompactedRow) where Arrow schema is unavailable.
pub fn internal_row_to_ffi_row(
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> Result<ffi::FfiGenericRow> {
    let schema = table_info.get_schema();
    let columns = schema.columns();
    let mut fields = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        if row.is_null_at(i) {
            fields.push(ffi::FfiDatum::default());
            continue;
        }

        let datum = match col.data_type() {
            fcore::metadata::DataType::Boolean(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_BOOL,
                bool_val: row.get_boolean(i),
                ..Default::default()
            },
            fcore::metadata::DataType::TinyInt(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_byte(i) as i32,
                ..Default::default()
            },
            fcore::metadata::DataType::SmallInt(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_short(i) as i32,
                ..Default::default()
            },
            fcore::metadata::DataType::Int(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT32,
                i32_val: row.get_int(i),
                ..Default::default()
            },
            fcore::metadata::DataType::BigInt(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_INT64,
                i64_val: row.get_long(i),
                ..Default::default()
            },
            fcore::metadata::DataType::Float(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_FLOAT32,
                f32_val: row.get_float(i),
                ..Default::default()
            },
            fcore::metadata::DataType::Double(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_FLOAT64,
                f64_val: row.get_double(i),
                ..Default::default()
            },
            fcore::metadata::DataType::String(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_STRING,
                string_val: row.get_string(i).to_string(),
                ..Default::default()
            },
            fcore::metadata::DataType::Bytes(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_BYTES,
                bytes_val: row.get_bytes(i).to_vec(),
                ..Default::default()
            },
            fcore::metadata::DataType::Date(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_DATE,
                i32_val: row.get_date(i).get_inner(),
                ..Default::default()
            },
            fcore::metadata::DataType::Time(_) => ffi::FfiDatum {
                datum_type: DATUM_TYPE_TIME,
                i32_val: row.get_time(i).get_inner(),
                ..Default::default()
            },
            fcore::metadata::DataType::Timestamp(dt) => {
                let ts = row.get_timestamp_ntz(i, dt.precision());
                ffi::FfiDatum {
                    datum_type: DATUM_TYPE_TIMESTAMP_NTZ,
                    i64_val: ts.get_millisecond(),
                    i32_val: ts.get_nano_of_millisecond(),
                    ..Default::default()
                }
            }
            fcore::metadata::DataType::TimestampLTz(dt) => {
                let ts = row.get_timestamp_ltz(i, dt.precision());
                ffi::FfiDatum {
                    datum_type: DATUM_TYPE_TIMESTAMP_LTZ,
                    i64_val: ts.get_epoch_millisecond(),
                    i32_val: ts.get_nano_of_millisecond(),
                    ..Default::default()
                }
            }
            fcore::metadata::DataType::Decimal(dt) => {
                let precision = dt.precision();
                let scale = dt.scale();
                let decimal = row.get_decimal(i, precision as usize, scale as usize);
                if fcore::row::Decimal::is_compact_precision(precision) {
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_DECIMAL_I64,
                        i64_val: decimal.to_unscaled_long().unwrap_or(0),
                        decimal_precision: precision as i32,
                        decimal_scale: scale as i32,
                        ..Default::default()
                    }
                } else {
                    let bd = decimal.to_big_decimal();
                    let (unscaled, _) = bd.into_bigint_and_exponent();
                    use bigdecimal::ToPrimitive;
                    let i128_val = unscaled.to_i128().unwrap_or(0);
                    ffi::FfiDatum {
                        datum_type: DATUM_TYPE_DECIMAL_I128,
                        i128_hi: (i128_val >> 64) as i64,
                        i128_lo: i128_val as i64,
                        decimal_precision: precision as i32,
                        decimal_scale: scale as i32,
                        ..Default::default()
                    }
                }
            }
            other => return Err(anyhow!("Unsupported data type for column {i}: {other:?}")),
        };

        fields.push(datum);
    }

    Ok(ffi::FfiGenericRow { fields })
}

pub fn core_lake_snapshot_to_ffi(snapshot: &fcore::metadata::LakeSnapshot) -> ffi::FfiLakeSnapshot {
    let bucket_offsets: Vec<ffi::FfiBucketOffset> = snapshot
        .table_buckets_offset
        .iter()
        .map(|(bucket, offset)| ffi::FfiBucketOffset {
            table_id: bucket.table_id(),
            partition_id: bucket.partition_id().unwrap_or(-1),
            bucket_id: bucket.bucket_id(),
            offset: *offset,
        })
        .collect();

    ffi::FfiLakeSnapshot {
        snapshot_id: snapshot.snapshot_id,
        bucket_offsets,
    }
}

pub fn core_scan_batches_to_ffi(
    batches: &[fcore::record::ScanBatch],
) -> Result<ffi::FfiArrowRecordBatches, String> {
    let mut ffi_batches = Vec::new();
    for batch in batches {
        let record_batch = batch.batch();
        // Convert RecordBatch to StructArray first, then get the data
        let struct_array = arrow::array::StructArray::from(record_batch.clone());
        let ffi_array = Box::new(FFI_ArrowArray::new(&struct_array.into_data()));
        let ffi_schema = Box::new(
            FFI_ArrowSchema::try_from(record_batch.schema().as_ref()).map_err(|e| e.to_string())?,
        );
        // Export as raw pointers
        ffi_batches.push(ffi::FfiArrowRecordBatch {
            array_ptr: Box::into_raw(ffi_array) as usize,
            schema_ptr: Box::into_raw(ffi_schema) as usize,
            table_id: batch.bucket().table_id(),
            partition_id: batch.bucket().partition_id().unwrap_or(-1),
            bucket_id: batch.bucket().bucket_id(),
            base_offset: batch.base_offset(),
        });
    }

    Ok(ffi::FfiArrowRecordBatches {
        batches: ffi_batches,
    })
}
