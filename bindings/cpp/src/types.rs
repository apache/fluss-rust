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
use anyhow::{anyhow, Result};
use arrow::array::{
    Date32Array, LargeBinaryArray, LargeStringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use fluss as fcore;
use fcore::row::InternalRow;

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

pub const DATUM_TYPE_NULL: i32 = 0;
pub const DATUM_TYPE_BOOL: i32 = 1;
pub const DATUM_TYPE_INT32: i32 = 2;
pub const DATUM_TYPE_INT64: i32 = 3;
pub const DATUM_TYPE_FLOAT32: i32 = 4;
pub const DATUM_TYPE_FLOAT64: i32 = 5;
pub const DATUM_TYPE_STRING: i32 = 6;
pub const DATUM_TYPE_BYTES: i32 = 7;

fn ffi_data_type_to_core(dt: i32) -> Result<fcore::metadata::DataType> {
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
        DATA_TYPE_TIMESTAMP => Ok(fcore::metadata::DataTypes::timestamp()),
        DATA_TYPE_TIMESTAMP_LTZ => Ok(fcore::metadata::DataTypes::timestamp_ltz()),
        _ => Err(anyhow!("Unknown data type: {}", dt)),
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
        _ => 0,
    }
}

pub fn ffi_descriptor_to_core(
    descriptor: &ffi::FfiTableDescriptor,
) -> Result<fcore::metadata::TableDescriptor> {
    let mut schema_builder = fcore::metadata::Schema::builder();

    for col in &descriptor.schema.columns {
        let dt = ffi_data_type_to_core(col.data_type)?;
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
        builder = builder.distributed_by(Some(descriptor.bucket_count), descriptor.bucket_keys.clone());
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
        .map(|col| ffi::FfiColumn {
            name: col.name().to_string(),
            data_type: core_data_type_to_ffi(col.data_type()),
            comment: col.comment().unwrap_or("").to_string(),
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

pub struct OwnedRowData {
    strings: Vec<String>,
}

impl OwnedRowData {
    pub fn new() -> Self {
        Self { strings: Vec::new() }
    }

    pub fn collect_strings(&mut self, row: &ffi::FfiGenericRow) {
        for field in &row.fields {
            if field.datum_type == DATUM_TYPE_STRING {
                self.strings.push(field.string_val.to_string());
            }
        }
    }

    pub fn get_strings(&self) -> &[String] {
        &self.strings
    }
}

pub fn ffi_row_to_core<'a>(
    row: &ffi::FfiGenericRow,
    owner: &'a OwnedRowData,
) -> fcore::row::GenericRow<'a> {
    use fcore::row::{Blob, Datum, F32, F64};

    let mut generic_row = fcore::row::GenericRow::new();
    let mut string_idx = 0;

    for (idx, field) in row.fields.iter().enumerate() {
        let datum = match field.datum_type {
            DATUM_TYPE_NULL => Datum::Null,
            DATUM_TYPE_BOOL => Datum::Bool(field.bool_val),
            DATUM_TYPE_INT32 => Datum::Int32(field.i32_val),
            DATUM_TYPE_INT64 => Datum::Int64(field.i64_val),
            DATUM_TYPE_FLOAT32 => Datum::Float32(F32::from(field.f32_val)),
            DATUM_TYPE_FLOAT64 => Datum::Float64(F64::from(field.f64_val)),
            DATUM_TYPE_STRING => {
                let str_ref = owner.get_strings()[string_idx].as_str();
                string_idx += 1;
                Datum::String(str_ref)
            }
            DATUM_TYPE_BYTES => Datum::Blob(Blob::from(field.bytes_val.clone())),
            _ => Datum::Null,
        };
        generic_row.set_field(idx, datum);
    }

    generic_row
}

pub fn core_scan_records_to_ffi(records: &fcore::record::ScanRecords) -> ffi::FfiScanRecords {
    let mut ffi_records = Vec::new();
    
    // Iterate over all buckets and their records
    for bucket_records in records.records_by_buckets().values() {
        for record in bucket_records {
            let row = record.row();
            let fields = core_row_to_ffi_fields(row);

            ffi_records.push(ffi::FfiScanRecord {
                offset: record.offset(),
                timestamp: record.timestamp(),
                row: ffi::FfiGenericRow { fields },
            });
        }
    }

    ffi::FfiScanRecords { records: ffi_records }
}

fn core_row_to_ffi_fields(row: &fcore::row::ColumnarRow) -> Vec<ffi::FfiDatum> {
    fn new_datum(datum_type: i32) -> ffi::FfiDatum {
        ffi::FfiDatum {
            datum_type,
            bool_val: false,
            i32_val: 0,
            i64_val: 0,
            f32_val: 0.0,
            f64_val: 0.0,
            string_val: String::new(),
            bytes_val: vec![],
        }
    }

    let record_batch = row.get_record_batch();
    let schema = record_batch.schema();
    let row_id = row.get_row_id();

    let mut fields = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        if row.is_null_at(i) {
            fields.push(new_datum(DATUM_TYPE_NULL));
            continue;
        }

        let datum = match field.data_type() {
            ArrowDataType::Boolean => {
                let mut datum = new_datum(DATUM_TYPE_BOOL);
                datum.bool_val = row.get_boolean(i);
                datum
            }
            ArrowDataType::Int8 => {
                let mut datum = new_datum(DATUM_TYPE_INT32);
                datum.i32_val = row.get_byte(i) as i32;
                datum
            }
            ArrowDataType::Int16 => {
                let mut datum = new_datum(DATUM_TYPE_INT32);
                datum.i32_val = row.get_short(i) as i32;
                datum
            }
            ArrowDataType::Int32 => {
                let mut datum = new_datum(DATUM_TYPE_INT32);
                datum.i32_val = row.get_int(i);
                datum
            }
            ArrowDataType::Int64 => {
                let mut datum = new_datum(DATUM_TYPE_INT64);
                datum.i64_val = row.get_long(i);
                datum
            }
            ArrowDataType::Float32 => {
                let mut datum = new_datum(DATUM_TYPE_FLOAT32);
                datum.f32_val = row.get_float(i);
                datum
            }
            ArrowDataType::Float64 => {
                let mut datum = new_datum(DATUM_TYPE_FLOAT64);
                datum.f64_val = row.get_double(i);
                datum
            }
            ArrowDataType::Utf8 => {
                let mut datum = new_datum(DATUM_TYPE_STRING);
                datum.string_val = row.get_string(i).to_string();
                datum
            }
            ArrowDataType::LargeUtf8 => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("LargeUtf8 column expected");
                let mut datum = new_datum(DATUM_TYPE_STRING);
                datum.string_val = array.value(row_id).to_string();
                datum
            }
            ArrowDataType::Binary => {
                let mut datum = new_datum(DATUM_TYPE_BYTES);
                datum.bytes_val = row.get_bytes(i);
                datum
            }
            ArrowDataType::FixedSizeBinary(len) => {
                let mut datum = new_datum(DATUM_TYPE_BYTES);
                datum.bytes_val = row.get_binary(i, *len as usize);
                datum
            }
            ArrowDataType::LargeBinary => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("LargeBinary column expected");
                let mut datum = new_datum(DATUM_TYPE_BYTES);
                datum.bytes_val = array.value(row_id).to_vec();
                datum
            }
            ArrowDataType::Date32 => {
                let array = record_batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("Date32 column expected");
                let mut datum = new_datum(DATUM_TYPE_INT32);
                datum.i32_val = array.value(row_id);
                datum
            }
            ArrowDataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .expect("Timestamp(second) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
                TimeUnit::Millisecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .expect("Timestamp(millisecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
                TimeUnit::Microsecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .expect("Timestamp(microsecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
                TimeUnit::Nanosecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .expect("Timestamp(nanosecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
            },
            ArrowDataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .expect("Time32(second) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT32);
                    datum.i32_val = array.value(row_id);
                    datum
                }
                TimeUnit::Millisecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .expect("Time32(millisecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT32);
                    datum.i32_val = array.value(row_id);
                    datum
                }
                _ => panic!("Unsupported Time32 unit for column {}", i),
            },
            ArrowDataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .expect("Time64(microsecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
                TimeUnit::Nanosecond => {
                    let array = record_batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .expect("Time64(nanosecond) column expected");
                    let mut datum = new_datum(DATUM_TYPE_INT64);
                    datum.i64_val = array.value(row_id);
                    datum
                }
                _ => panic!("Unsupported Time64 unit for column {}", i),
            },
            other => panic!("Unsupported Arrow data type for column {}: {:?}", i, other),
        };

        fields.push(datum);
    }

    fields
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
