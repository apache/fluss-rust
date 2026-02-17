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
use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use fluss as fcore;
use std::borrow::Cow;
use std::str::FromStr;

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
pub const DATA_TYPE_CHAR: i32 = 15;
pub const DATA_TYPE_BINARY: i32 = 16;

// DATUM_TYPE_* constants removed — no longer needed with opaque types.

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
        DATA_TYPE_CHAR => Ok(fcore::metadata::DataTypes::char(precision)),
        DATA_TYPE_BINARY => Ok(fcore::metadata::DataTypes::binary(precision as usize)),
        _ => Err(anyhow!("Unknown data type: {dt}")),
    }
}

pub fn core_data_type_to_ffi(dt: &fcore::metadata::DataType) -> i32 {
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
        fcore::metadata::DataType::Char(_) => DATA_TYPE_CHAR,
        fcore::metadata::DataType::Binary(_) => DATA_TYPE_BINARY,
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

    if !descriptor.custom_properties.is_empty() {
        let custom: std::collections::HashMap<String, String> = descriptor
            .custom_properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        builder = builder.custom_properties(custom);
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
                fcore::metadata::DataType::Char(dt) => (dt.length() as i32, 0),
                fcore::metadata::DataType::Binary(dt) => (dt.length() as i32, 0),
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

    let custom_properties: Vec<ffi::HashMapValue> = info
        .get_custom_properties()
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
        custom_properties,
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
        custom_properties: vec![],
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

/// Resolve types in a GenericRow using schema metadata.
/// Narrows Int32 → Int8/Int16, parses decimal strings, etc.
/// Used by both AppendWriter and UpsertWriter.
pub fn resolve_row_types(
    row: &fcore::row::GenericRow<'_>,
    schema: Option<&fcore::metadata::Schema>,
) -> Result<fcore::row::GenericRow<'static>> {
    use fcore::row::Datum;

    let mut out = fcore::row::GenericRow::new(row.values.len());

    for (idx, datum) in row.values.iter().enumerate() {
        let resolved = match datum {
            Datum::Null => Datum::Null,
            Datum::Bool(v) => Datum::Bool(*v),
            Datum::Int32(v) => match schema
                .and_then(|s| s.columns().get(idx))
                .map(|c| c.data_type())
            {
                Some(fcore::metadata::DataType::TinyInt(_)) => Datum::Int8(
                    i8::try_from(*v).map_err(|_| anyhow!("Column {idx}: {v} overflows TinyInt"))?,
                ),
                Some(fcore::metadata::DataType::SmallInt(_)) => Datum::Int16(
                    i16::try_from(*v)
                        .map_err(|_| anyhow!("Column {idx}: {v} overflows SmallInt"))?,
                ),
                _ => Datum::Int32(*v),
            },
            Datum::Int64(v) => Datum::Int64(*v),
            Datum::Float32(v) => Datum::Float32(*v),
            Datum::Float64(v) => Datum::Float64(*v),
            Datum::Int8(v) => Datum::Int8(*v),
            Datum::Int16(v) => Datum::Int16(*v),
            Datum::String(cow) => {
                // Check if the schema column is Decimal — if so, parse the string as decimal
                match schema
                    .and_then(|s| s.columns().get(idx))
                    .map(|c| c.data_type())
                {
                    Some(fcore::metadata::DataType::Decimal(dt)) => {
                        let (precision, scale) = (dt.precision(), dt.scale());
                        let bd = bigdecimal::BigDecimal::from_str(cow.as_ref()).map_err(|e| {
                            anyhow!("Column {idx}: invalid decimal string '{}': {e}", cow)
                        })?;
                        let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                            .map_err(|e| anyhow!("Column {idx}: {e}"))?;
                        Datum::Decimal(decimal)
                    }
                    _ => Datum::String(Cow::Owned(cow.to_string())),
                }
            }
            Datum::Blob(cow) => Datum::Blob(Cow::Owned(cow.to_vec())),
            Datum::Decimal(d) => Datum::Decimal(d.clone()),
            Datum::Date(d) => Datum::Date(*d),
            Datum::Time(t) => Datum::Time(*t),
            Datum::TimestampNtz(ts) => Datum::TimestampNtz(*ts),
            Datum::TimestampLtz(ts) => Datum::TimestampLtz(*ts),
        };
        out.set_field(idx, resolved);
    }

    Ok(out)
}

/// Convert a CompactedRow (lookup result) to an owned GenericRow<'static>.
/// One copy for strings/bytes (Cow::Owned), but no second copy into FfiDatum.
pub fn compacted_row_to_owned(
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> Result<fcore::row::GenericRow<'static>> {
    use fcore::row::Datum;

    let schema = table_info.get_schema();
    let columns = schema.columns();
    let mut out = fcore::row::GenericRow::new(columns.len());

    for (i, col) in columns.iter().enumerate() {
        if row.is_null_at(i) {
            out.set_field(i, Datum::Null);
            continue;
        }

        let datum = match col.data_type() {
            fcore::metadata::DataType::Boolean(_) => Datum::Bool(row.get_boolean(i)),
            fcore::metadata::DataType::TinyInt(_) => Datum::Int8(row.get_byte(i)),
            fcore::metadata::DataType::SmallInt(_) => Datum::Int16(row.get_short(i)),
            fcore::metadata::DataType::Int(_) => Datum::Int32(row.get_int(i)),
            fcore::metadata::DataType::BigInt(_) => Datum::Int64(row.get_long(i)),
            fcore::metadata::DataType::Float(_) => Datum::Float32(row.get_float(i).into()),
            fcore::metadata::DataType::Double(_) => Datum::Float64(row.get_double(i).into()),
            fcore::metadata::DataType::String(_) => {
                Datum::String(Cow::Owned(row.get_string(i).to_string()))
            }
            fcore::metadata::DataType::Bytes(_) => {
                Datum::Blob(Cow::Owned(row.get_bytes(i).to_vec()))
            }
            fcore::metadata::DataType::Date(_) => Datum::Date(row.get_date(i)),
            fcore::metadata::DataType::Time(_) => Datum::Time(row.get_time(i)),
            fcore::metadata::DataType::Timestamp(dt) => {
                Datum::TimestampNtz(row.get_timestamp_ntz(i, dt.precision()))
            }
            fcore::metadata::DataType::TimestampLTz(dt) => {
                Datum::TimestampLtz(row.get_timestamp_ltz(i, dt.precision()))
            }
            fcore::metadata::DataType::Decimal(dt) => {
                let decimal = row.get_decimal(i, dt.precision() as usize, dt.scale() as usize);
                Datum::Decimal(decimal)
            }
            fcore::metadata::DataType::Char(dt) => Datum::String(Cow::Owned(
                row.get_char(i, dt.length() as usize).to_string(),
            )),
            fcore::metadata::DataType::Binary(dt) => {
                Datum::Blob(Cow::Owned(row.get_binary(i, dt.length()).to_vec()))
            }
            other => return Err(anyhow!("Unsupported data type for column {i}: {other:?}")),
        };

        out.set_field(i, datum);
    }

    Ok(out)
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
