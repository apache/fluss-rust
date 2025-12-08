/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "fluss.hpp"
#include "lib.rs.h"

namespace fluss {
namespace utils {

inline Result make_error(int32_t code, std::string msg) {
    return Result{code, std::move(msg)};
}

inline Result make_ok() {
    return Result{0, {}};
}

inline Result from_ffi_result(const ffi::FfiResult& ffi_result) {
    return Result{ffi_result.error_code, std::string(ffi_result.error_message)};
}

inline ffi::FfiTablePath to_ffi_table_path(const TablePath& path) {
    ffi::FfiTablePath ffi_path;
    ffi_path.database_name = rust::String(path.database_name);
    ffi_path.table_name = rust::String(path.table_name);
    return ffi_path;
}

inline ffi::FfiColumn to_ffi_column(const Column& col) {
    ffi::FfiColumn ffi_col;
    ffi_col.name = rust::String(col.name);
    ffi_col.data_type = static_cast<int32_t>(col.data_type);
    ffi_col.comment = rust::String(col.comment);
    return ffi_col;
}

inline ffi::FfiSchema to_ffi_schema(const Schema& schema) {
    ffi::FfiSchema ffi_schema;

    rust::Vec<ffi::FfiColumn> cols;
    for (const auto& col : schema.columns) {
        cols.push_back(to_ffi_column(col));
    }
    ffi_schema.columns = std::move(cols);

    rust::Vec<rust::String> pks;
    for (const auto& pk : schema.primary_keys) {
        pks.push_back(rust::String(pk));
    }
    ffi_schema.primary_keys = std::move(pks);

    return ffi_schema;
}

inline ffi::FfiTableDescriptor to_ffi_table_descriptor(const TableDescriptor& desc) {
    ffi::FfiTableDescriptor ffi_desc;

    ffi_desc.schema = to_ffi_schema(desc.schema);

    rust::Vec<rust::String> partition_keys;
    for (const auto& pk : desc.partition_keys) {
        partition_keys.push_back(rust::String(pk));
    }
    ffi_desc.partition_keys = std::move(partition_keys);

    ffi_desc.bucket_count = desc.bucket_count;

    rust::Vec<rust::String> bucket_keys;
    for (const auto& bk : desc.bucket_keys) {
        bucket_keys.push_back(rust::String(bk));
    }
    ffi_desc.bucket_keys = std::move(bucket_keys);

    rust::Vec<ffi::HashMapValue> props;
    for (const auto& [k, v] : desc.properties) {
        ffi::HashMapValue prop;
        prop.key = rust::String(k);
        prop.value = rust::String(v);
        props.push_back(prop);
    }
    ffi_desc.properties = std::move(props);

    ffi_desc.comment = rust::String(desc.comment);

    return ffi_desc;
}

inline ffi::FfiDatum to_ffi_datum(const Datum& datum) {
    ffi::FfiDatum ffi_datum;
    ffi_datum.datum_type = static_cast<int32_t>(datum.type);
    ffi_datum.bool_val = datum.bool_val;
    ffi_datum.i32_val = datum.i32_val;
    ffi_datum.i64_val = datum.i64_val;
    ffi_datum.f32_val = datum.f32_val;
    ffi_datum.f64_val = datum.f64_val;
    ffi_datum.string_val = rust::String(datum.string_val);

    rust::Vec<uint8_t> bytes;
    for (auto b : datum.bytes_val) {
        bytes.push_back(b);
    }
    ffi_datum.bytes_val = std::move(bytes);

    return ffi_datum;
}

inline ffi::FfiGenericRow to_ffi_generic_row(const GenericRow& row) {
    ffi::FfiGenericRow ffi_row;

    rust::Vec<ffi::FfiDatum> fields;
    for (const auto& field : row.fields) {
        fields.push_back(to_ffi_datum(field));
    }
    ffi_row.fields = std::move(fields);

    return ffi_row;
}

inline Column from_ffi_column(const ffi::FfiColumn& ffi_col) {
    return Column{
        std::string(ffi_col.name),
        static_cast<DataType>(ffi_col.data_type),
        std::string(ffi_col.comment)};
}

inline Schema from_ffi_schema(const ffi::FfiSchema& ffi_schema) {
    Schema schema;

    for (const auto& col : ffi_schema.columns) {
        schema.columns.push_back(from_ffi_column(col));
    }

    for (const auto& pk : ffi_schema.primary_keys) {
        schema.primary_keys.push_back(std::string(pk));
    }

    return schema;
}

inline TableInfo from_ffi_table_info(const ffi::FfiTableInfo& ffi_info) {
    TableInfo info;

    info.table_id = ffi_info.table_id;
    info.schema_id = ffi_info.schema_id;
    info.table_path = TablePath{
        std::string(ffi_info.table_path.database_name),
        std::string(ffi_info.table_path.table_name)};
    info.created_time = ffi_info.created_time;
    info.modified_time = ffi_info.modified_time;

    for (const auto& pk : ffi_info.primary_keys) {
        info.primary_keys.push_back(std::string(pk));
    }

    for (const auto& bk : ffi_info.bucket_keys) {
        info.bucket_keys.push_back(std::string(bk));
    }

    for (const auto& pk : ffi_info.partition_keys) {
        info.partition_keys.push_back(std::string(pk));
    }

    info.num_buckets = ffi_info.num_buckets;
    info.has_primary_key = ffi_info.has_primary_key;
    info.is_partitioned = ffi_info.is_partitioned;

    for (const auto& prop : ffi_info.properties) {
        info.properties[std::string(prop.key)] = std::string(prop.value);
    }

    info.comment = std::string(ffi_info.comment);
    info.schema = from_ffi_schema(ffi_info.schema);

    return info;
}

inline Datum from_ffi_datum(const ffi::FfiDatum& ffi_datum) {
    Datum datum;
    datum.type = static_cast<DatumType>(ffi_datum.datum_type);
    datum.bool_val = ffi_datum.bool_val;
    datum.i32_val = ffi_datum.i32_val;
    datum.i64_val = ffi_datum.i64_val;
    datum.f32_val = ffi_datum.f32_val;
    datum.f64_val = ffi_datum.f64_val;
    datum.string_val = std::string(ffi_datum.string_val);

    for (auto b : ffi_datum.bytes_val) {
        datum.bytes_val.push_back(b);
    }

    return datum;
}

inline GenericRow from_ffi_generic_row(const ffi::FfiGenericRow& ffi_row) {
    GenericRow row;

    for (const auto& field : ffi_row.fields) {
        row.fields.push_back(from_ffi_datum(field));
    }

    return row;
}

inline ScanRecord from_ffi_scan_record(const ffi::FfiScanRecord& ffi_record) {
    return ScanRecord{
        ffi_record.offset,
        ffi_record.timestamp,
        from_ffi_generic_row(ffi_record.row)};
}

inline ScanRecords from_ffi_scan_records(const ffi::FfiScanRecords& ffi_records) {
    ScanRecords records;

    for (const auto& record : ffi_records.records) {
        records.records.push_back(from_ffi_scan_record(record));
    }

    return records;
}

inline LakeSnapshot from_ffi_lake_snapshot(const ffi::FfiLakeSnapshot& ffi_snapshot) {
    LakeSnapshot snapshot;
    snapshot.snapshot_id = ffi_snapshot.snapshot_id;

    for (const auto& offset : ffi_snapshot.bucket_offsets) {
        snapshot.bucket_offsets.push_back(BucketOffset{
            offset.table_id,
            offset.partition_id,
            offset.bucket_id,
            offset.offset});
    }

    return snapshot;
}

}  // namespace utils
}  // namespace fluss
