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

#include "fluss.hpp"
#include "lib.rs.h"
#include "ffi_converter.hpp"
#include "rust/cxx.h"
#include <arrow/c/bridge.h>
// todo:  bindings/cpp/BUILD.bazel still doesn’t declare Arrow include/link dependencies.
// In environments where Bazel does not already have Arrow available, this will fail at compile/link time.
#include <arrow/record_batch.h>

namespace fluss {

Table::Table() noexcept = default;

Table::Table(ffi::Table* table) noexcept : table_(table) {}

Table::~Table() noexcept { Destroy(); }

void Table::Destroy() noexcept {
    if (table_) {
        ffi::delete_table(table_);
        table_ = nullptr;
    }
}

Table::Table(Table&& other) noexcept : table_(other.table_) {
    other.table_ = nullptr;
}

Table& Table::operator=(Table&& other) noexcept {
    if (this != &other) {
        Destroy();
        table_ = other.table_;
        other.table_ = nullptr;
    }
    return *this;
}

bool Table::Available() const { return table_ != nullptr; }

Result Table::NewAppendWriter(AppendWriter& out) {
    if (!Available()) {
        return utils::make_error(1, "Table not available");
    }

    try {
        out.writer_ = table_->new_append_writer();
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

TableScan Table::NewScan() {
    return TableScan(table_);
}

// TableScan implementation
TableScan::TableScan(ffi::Table* table) noexcept : table_(table) {}

TableScan& TableScan::Project(std::vector<size_t> column_indices) {
    projection_ = std::move(column_indices);
    return *this;
}

Result TableScan::CreateLogScanner(LogScanner& out) {
    if (table_ == nullptr) {
        return utils::make_error(1, "Table not available");
    }

    try {
        if (projection_.empty()) {
            out.scanner_ = table_->new_log_scanner();
        } else {
            rust::Vec<size_t> rust_indices;
            for (size_t idx : projection_) {
                rust_indices.push_back(idx);
            }
            out.scanner_ = table_->new_log_scanner_with_projection(std::move(rust_indices));
        }
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

Result TableScan::CreateRecordBatchScanner(LogScanner& out) {
    if (table_ == nullptr) {
        return utils::make_error(1, "Table not available");
    }

    try {
        if (projection_.empty()) {
            out.scanner_ = table_->new_record_batch_log_scanner();
        } else {
            rust::Vec<size_t> rust_indices;
            for (size_t idx : projection_) {
                rust_indices.push_back(idx);
            }
            out.scanner_ = table_->new_record_batch_log_scanner_with_projection(std::move(rust_indices));
        }
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

TableInfo Table::GetTableInfo() const {
    if (!Available()) {
        return TableInfo{};
    }
    auto ffi_info = table_->get_table_info_from_table();
    return utils::from_ffi_table_info(ffi_info);
}

TablePath Table::GetTablePath() const {
    if (!Available()) {
        return TablePath{};
    }
    auto ffi_path = table_->get_table_path();
    return TablePath{std::string(ffi_path.database_name), std::string(ffi_path.table_name)};
}

bool Table::HasPrimaryKey() const {
    if (!Available()) {
        return false;
    }
    return table_->has_primary_key();
}

// AppendWriter implementation
AppendWriter::AppendWriter() noexcept = default;

AppendWriter::AppendWriter(ffi::AppendWriter* writer) noexcept : writer_(writer) {}

AppendWriter::~AppendWriter() noexcept { Destroy(); }

void AppendWriter::Destroy() noexcept {
    if (writer_) {
        ffi::delete_append_writer(writer_);
        writer_ = nullptr;
    }
}

AppendWriter::AppendWriter(AppendWriter&& other) noexcept : writer_(other.writer_) {
    other.writer_ = nullptr;
}

AppendWriter& AppendWriter::operator=(AppendWriter&& other) noexcept {
    if (this != &other) {
        Destroy();
        writer_ = other.writer_;
        other.writer_ = nullptr;
    }
    return *this;
}

bool AppendWriter::Available() const { return writer_ != nullptr; }

Result AppendWriter::Append(const GenericRow& row) {
    if (!Available()) {
        return utils::make_error(1, "AppendWriter not available");
    }

    auto ffi_row = utils::to_ffi_generic_row(row);
    auto ffi_result = writer_->append(ffi_row);
    return utils::from_ffi_result(ffi_result);
}

Result AppendWriter::Flush() {
    if (!Available()) {
        return utils::make_error(1, "AppendWriter not available");
    }

    auto ffi_result = writer_->flush();
    return utils::from_ffi_result(ffi_result);
}

// LogScanner implementation
LogScanner::LogScanner() noexcept = default;

LogScanner::LogScanner(ffi::LogScanner* scanner) noexcept : scanner_(scanner) {}

LogScanner::~LogScanner() noexcept { Destroy(); }

void LogScanner::Destroy() noexcept {
    if (scanner_) {
        ffi::delete_log_scanner(scanner_);
        scanner_ = nullptr;
    }
}

LogScanner::LogScanner(LogScanner&& other) noexcept : scanner_(other.scanner_) {
    other.scanner_ = nullptr;
}

LogScanner& LogScanner::operator=(LogScanner&& other) noexcept {
    if (this != &other) {
        Destroy();
        scanner_ = other.scanner_;
        other.scanner_ = nullptr;
    }
    return *this;
}

bool LogScanner::Available() const { return scanner_ != nullptr; }

Result LogScanner::Subscribe(int32_t bucket_id, int64_t start_offset) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe(bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Subscribe(const std::vector<BucketSubscription>& bucket_offsets) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    rust::Vec<ffi::FfiBucketSubscription> rust_subs;
    for (const auto& sub : bucket_offsets) {
        ffi::FfiBucketSubscription ffi_sub;
        ffi_sub.bucket_id = sub.bucket_id;
        ffi_sub.offset = sub.offset;
        rust_subs.push_back(ffi_sub);
    }

    auto ffi_result = scanner_->subscribe_buckets(std::move(rust_subs));
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::SubscribePartition(int64_t partition_id, int32_t bucket_id, int64_t start_offset) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe_partition(partition_id, bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::UnsubscribePartition(int64_t partition_id, int32_t bucket_id) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    auto ffi_result = scanner_->unsubscribe_partition(partition_id, bucket_id);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Poll(int64_t timeout_ms, ScanRecords& out) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    auto ffi_result = scanner_->poll(timeout_ms);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (!result.Ok()) {
        return result;
    }

    out = utils::from_ffi_scan_records(ffi_result.scan_records);
    return utils::make_ok();
}

ArrowRecordBatch::ArrowRecordBatch(
    std::shared_ptr<arrow::RecordBatch> batch,
    int64_t table_id,
    int64_t partition_id,
    int32_t bucket_id,
    int64_t base_offset) noexcept
    : batch_(std::move(batch)),
      table_id_(table_id),
      partition_id_(partition_id),
      bucket_id_(bucket_id),
      base_offset_(base_offset) {}

bool ArrowRecordBatch::Available() const { return batch_ != nullptr; }

int64_t ArrowRecordBatch::NumRows() const {
    if (!Available()) return 0;
    return batch_->num_rows();
}


int64_t ArrowRecordBatch::GetTableId() const {
    if (!Available()) return 0;
    return this->table_id_;
}

int64_t ArrowRecordBatch::GetPartitionId() const {
    if (!Available()) return -1;
    return this->partition_id_;
}

int32_t ArrowRecordBatch::GetBucketId() const {
    if (!Available()) return -1;
    return this->bucket_id_;
}

int64_t ArrowRecordBatch::GetBaseOffset() const {
    if (!Available()) return -1;
    return this->base_offset_;
}

int64_t ArrowRecordBatch::GetLastOffset() const {
    if (!Available()) return -1;
    return this->base_offset_ + this->NumRows() - 1;
}

Result LogScanner::PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out) {
    if (!Available()) {
        return utils::make_error(1, "LogScanner not available");
    }

    auto ffi_result = scanner_->poll_record_batch(timeout_ms);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (!result.Ok()) {
        return result;
    }

    // Convert the FFI Arrow record batches to C++ ArrowRecordBatch objects
    out.batches.clear();
    for (const auto& ffi_batch : ffi_result.arrow_batches.batches) {
        auto* c_array = reinterpret_cast<struct ArrowArray*>(ffi_batch.array_ptr);
        auto* c_schema = reinterpret_cast<struct ArrowSchema*>(ffi_batch.schema_ptr);

        auto import_result = arrow::ImportRecordBatch(c_array, c_schema);
        if (import_result.ok()) {
            auto batch_ptr = import_result.ValueOrDie();
            auto batch_wrapper = std::unique_ptr<ArrowRecordBatch>(new ArrowRecordBatch(
                std::move(batch_ptr),
                ffi_batch.table_id,
                ffi_batch.partition_id,
                ffi_batch.bucket_id,
                ffi_batch.base_offset
            ));
            out.batches.push_back(std::move(batch_wrapper));
            
            // Free the container structures that were allocated in Rust after successful import
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);
        } else {
            // Import failed, free the container structures to avoid leaks and return error
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);
            
            // Return an error indicating that the import failed
            std::string error_msg = "Failed to import Arrow record batch: " + import_result.status().ToString();
            return utils::make_error(1, error_msg);
        }
    }
    
    return utils::make_ok();
}

}  // namespace fluss
