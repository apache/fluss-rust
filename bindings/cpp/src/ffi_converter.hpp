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

inline Result make_error(int32_t code, std::string msg) { return Result{code, std::move(msg)}; }

inline Result make_client_error(std::string msg) {
    return Result{ErrorCode::CLIENT_ERROR, std::move(msg)};
}

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

inline ffi::FfiConfig to_ffi_config(const Configuration& config) {
    ffi::FfiConfig ffi_config;
    ffi_config.bootstrap_servers = rust::String(config.bootstrap_servers);
    ffi_config.writer_request_max_size = config.writer_request_max_size;
    ffi_config.writer_acks = rust::String(config.writer_acks);
    ffi_config.writer_retries = config.writer_retries;
    ffi_config.writer_batch_size = config.writer_batch_size;
    ffi_config.writer_bucket_no_key_assigner = rust::String(config.writer_bucket_no_key_assigner);
    ffi_config.scanner_remote_log_prefetch_num = config.scanner_remote_log_prefetch_num;
    ffi_config.remote_file_download_thread_num = config.remote_file_download_thread_num;
    ffi_config.scanner_remote_log_read_concurrency = config.scanner_remote_log_read_concurrency;
    ffi_config.scanner_log_max_poll_records = config.scanner_log_max_poll_records;
    ffi_config.writer_batch_timeout_ms = config.writer_batch_timeout_ms;
    ffi_config.connect_timeout_ms = config.connect_timeout_ms;
    ffi_config.request_timeout_ms = config.request_timeout_ms;
    ffi_config.security_protocol = rust::String(config.security_protocol);
    ffi_config.security_sasl_mechanism = rust::String(config.security_sasl_mechanism);
    ffi_config.security_sasl_username = rust::String(config.security_sasl_username);
    ffi_config.security_sasl_password = rust::String(config.security_sasl_password);
    return ffi_config;
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

    rust::Vec<ffi::HashMapValue> custom_props;
    for (const auto& [k, v] : desc.custom_properties) {
        ffi::HashMapValue prop;
        prop.key = rust::String(k);
        prop.value = rust::String(v);
        custom_props.push_back(prop);
    }
    ffi_desc.custom_properties = std::move(custom_props);

    ffi_desc.comment = rust::String(desc.comment);

    return ffi_desc;
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

    for (const auto& prop : ffi_info.custom_properties) {
        info.custom_properties[std::string(prop.key)] = std::string(prop.value);
    }

    info.comment = std::string(ffi_info.comment);
    info.schema = from_ffi_schema(ffi_info.schema);

    return info;
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

inline ffi::FfiDatabaseDescriptor to_ffi_database_descriptor(const DatabaseDescriptor& desc) {
    ffi::FfiDatabaseDescriptor ffi_desc;
    ffi_desc.comment = rust::String(desc.comment);
    for (const auto& [k, v] : desc.properties) {
        ffi::HashMapValue kv;
        kv.key = rust::String(k);
        kv.value = rust::String(v);
        ffi_desc.properties.push_back(std::move(kv));
    }
    return ffi_desc;
}

inline DatabaseInfo from_ffi_database_info(const ffi::FfiDatabaseInfo& ffi_info) {
    DatabaseInfo info;
    info.database_name = std::string(ffi_info.database_name);
    info.comment = std::string(ffi_info.comment);
    info.created_time = ffi_info.created_time;
    info.modified_time = ffi_info.modified_time;
    for (const auto& prop : ffi_info.properties) {
        info.properties[std::string(prop.key)] = std::string(prop.value);
    }
    return info;
}

}  // namespace utils
}  // namespace fluss
