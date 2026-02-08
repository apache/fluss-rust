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

namespace detail {
struct FfiAccess {
    static const std::vector<Datum>& fields(const GenericRow& row) { return row.fields; }
    static std::vector<Datum>& fields(GenericRow& row) { return row.fields; }

    static int32_t decimal_precision(const Datum& d) { return d.decimal_precision; }
    static int32_t decimal_scale(const Datum& d) { return d.decimal_scale; }
    static int64_t i128_hi(const Datum& d) { return d.i128_hi; }
    static int64_t i128_lo(const Datum& d) { return d.i128_lo; }

    static Datum from_ffi_decimal(const ffi::FfiDatum& ffi_datum, DatumType dtype) {
        Datum d;
        d.type = dtype;
        d.i64_val = ffi_datum.i64_val;
        d.decimal_precision = ffi_datum.decimal_precision;
        d.decimal_scale = ffi_datum.decimal_scale;
        d.i128_hi = ffi_datum.i128_hi;
        d.i128_lo = ffi_datum.i128_lo;
        if (dtype == DatumType::DecimalString) {
            d.string_val = std::string(ffi_datum.string_val);
        }
        return d;
    }
};
}  // namespace detail

namespace utils {

inline Result make_error(int32_t code, std::string msg) { return Result{code, std::move(msg)}; }

inline Result make_ok() { return Result{0, {}}; }

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
    ffi_col.data_type = static_cast<int32_t>(col.data_type.id());
    ffi_col.comment = rust::String(col.comment);
    ffi_col.precision = col.data_type.precision();
    ffi_col.scale = col.data_type.scale();
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
    ffi_datum.datum_type = static_cast<int32_t>(datum.GetType());
    ffi_datum.bool_val = datum.GetBool();
    ffi_datum.i32_val = datum.GetInt32();
    ffi_datum.i64_val = datum.GetInt64();
    ffi_datum.f32_val = datum.GetFloat32();
    ffi_datum.f64_val = datum.GetFloat64();
    ffi_datum.string_val = rust::String(datum.GetString());
    ffi_datum.decimal_precision = detail::FfiAccess::decimal_precision(datum);
    ffi_datum.decimal_scale = detail::FfiAccess::decimal_scale(datum);
    ffi_datum.i128_hi = detail::FfiAccess::i128_hi(datum);
    ffi_datum.i128_lo = detail::FfiAccess::i128_lo(datum);

    rust::Vec<uint8_t> bytes;
    for (auto b : datum.GetBytes()) {
        bytes.push_back(b);
    }
    ffi_datum.bytes_val = std::move(bytes);

    return ffi_datum;
}

inline ffi::FfiGenericRow to_ffi_generic_row(const GenericRow& row) {
    ffi::FfiGenericRow ffi_row;

    rust::Vec<ffi::FfiDatum> ffi_fields;
    for (const auto& field : detail::FfiAccess::fields(row)) {
        ffi_fields.push_back(to_ffi_datum(field));
    }
    ffi_row.fields = std::move(ffi_fields);

    return ffi_row;
}

inline Column from_ffi_column(const ffi::FfiColumn& ffi_col) {
    return Column{
        std::string(ffi_col.name),
        DataType(static_cast<TypeId>(ffi_col.data_type), ffi_col.precision, ffi_col.scale),
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
    info.table_path = TablePath{std::string(ffi_info.table_path.database_name),
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
    auto dtype = static_cast<DatumType>(ffi_datum.datum_type);
    switch (dtype) {
        case DatumType::Null:
            return Datum::Null();
        case DatumType::Bool:
            return Datum::Bool(ffi_datum.bool_val);
        case DatumType::Int32:
            return Datum::Int32(ffi_datum.i32_val);
        case DatumType::Int64:
            return Datum::Int64(ffi_datum.i64_val);
        case DatumType::Float32:
            return Datum::Float32(ffi_datum.f32_val);
        case DatumType::Float64:
            return Datum::Float64(ffi_datum.f64_val);
        case DatumType::String:
            return Datum::String(std::string(ffi_datum.string_val));
        case DatumType::Bytes: {
            std::vector<uint8_t> bytes;
            for (auto b : ffi_datum.bytes_val) {
                bytes.push_back(b);
            }
            return Datum::Bytes(std::move(bytes));
        }
        case DatumType::Date:
            return Datum::Date(fluss::Date{ffi_datum.i32_val});
        case DatumType::Time:
            return Datum::Time(fluss::Time{ffi_datum.i32_val});
        case DatumType::TimestampNtz:
            return Datum::TimestampNtz(fluss::Timestamp{ffi_datum.i64_val, ffi_datum.i32_val});
        case DatumType::TimestampLtz:
            return Datum::TimestampLtz(fluss::Timestamp{ffi_datum.i64_val, ffi_datum.i32_val});
        case DatumType::DecimalI64:
        case DatumType::DecimalI128:
        case DatumType::DecimalString:
            return detail::FfiAccess::from_ffi_decimal(ffi_datum, dtype);
        default:
            return Datum::Null();
    }
}

inline GenericRow from_ffi_generic_row(const ffi::FfiGenericRow& ffi_row) {
    GenericRow row;

    for (const auto& field : ffi_row.fields) {
        detail::FfiAccess::fields(row).push_back(from_ffi_datum(field));
    }

    return row;
}

inline ScanRecord from_ffi_scan_record(const ffi::FfiScanRecord& ffi_record) {
    return ScanRecord{ffi_record.bucket_id, ffi_record.offset, ffi_record.timestamp,
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
        snapshot.bucket_offsets.push_back(
            BucketOffset{offset.table_id, offset.partition_id, offset.bucket_id, offset.offset});
    }

    return snapshot;
}

}  // namespace utils
}  // namespace fluss
