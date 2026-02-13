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

#include <arrow/c/bridge.h>

#include <ctime>

#include "ffi_converter.hpp"
#include "fluss.hpp"
#include "lib.rs.h"
#include "rust/cxx.h"
// todo:  bindings/cpp/BUILD.bazel still doesn’t declare Arrow include/link dependencies.
// In environments where Bazel does not already have Arrow available, this will fail at compile/link
// time.
#include <arrow/record_batch.h>

namespace fluss {

static constexpr int kSecondsPerDay = 24 * 60 * 60;

static std::time_t timegm_utc(std::tm* tm) {
#if defined(_WIN32)
    return _mkgmtime(tm);
#else
    return ::timegm(tm);
#endif
}

static std::tm gmtime_utc(std::time_t epoch_seconds) {
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &epoch_seconds);
#else
    ::gmtime_r(&epoch_seconds, &tm);
#endif
    return tm;
}

Date Date::FromYMD(int year, int month, int day) {
    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    std::time_t epoch_seconds = timegm_utc(&tm);
    return {static_cast<int32_t>(epoch_seconds / kSecondsPerDay)};
}

int Date::Year() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_year + 1900;
}

int Date::Month() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_mon + 1;
}

int Date::Day() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_mday;
}

Table::Table() noexcept = default;

Table::Table(ffi::Table* table) noexcept : table_(table) {}

Table::~Table() noexcept { Destroy(); }

void Table::Destroy() noexcept {
    if (table_) {
        ffi::delete_table(table_);
        table_ = nullptr;
    }
}

Table::Table(Table&& other) noexcept
    : table_(other.table_), column_map_(std::move(other.column_map_)) {
    other.table_ = nullptr;
}

Table& Table::operator=(Table&& other) noexcept {
    if (this != &other) {
        Destroy();
        table_ = other.table_;
        column_map_ = std::move(other.column_map_);
        other.table_ = nullptr;
    }
    return *this;
}

bool Table::Available() const { return table_ != nullptr; }

TableAppend Table::NewAppend() { return TableAppend(table_); }

TableUpsert Table::NewUpsert() { return TableUpsert(table_); }

TableLookup Table::NewLookup() { return TableLookup(table_); }

TableScan Table::NewScan() { return TableScan(table_); }

// TableAppend implementation
TableAppend::TableAppend(ffi::Table* table) noexcept : table_(table) {}

Result TableAppend::CreateWriter(AppendWriter& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        out = AppendWriter(table_->new_append_writer());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

// TableUpsert implementation
TableUpsert::TableUpsert(ffi::Table* table) noexcept : table_(table) {}

TableUpsert& TableUpsert::PartialUpdateByIndex(std::vector<size_t> column_indices) {
    if (column_indices.empty()) {
        throw std::invalid_argument("PartialUpdateByIndex requires at least one column");
    }
    column_indices_ = std::move(column_indices);
    column_names_.clear();
    return *this;
}

TableUpsert& TableUpsert::PartialUpdateByName(std::vector<std::string> column_names) {
    if (column_names.empty()) {
        throw std::invalid_argument("PartialUpdateByName requires at least one column");
    }
    column_names_ = std::move(column_names);
    column_indices_.clear();
    return *this;
}

std::vector<size_t> TableUpsert::ResolveNameProjection() const {
    auto ffi_info = table_->get_table_info_from_table();
    const auto& columns = ffi_info.schema.columns;

    std::vector<size_t> indices;
    for (const auto& name : column_names_) {
        bool found = false;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (std::string(columns[i].name) == name) {
                indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column '" + name + "' not found");
        }
    }
    return indices;
}

Result TableUpsert::CreateWriter(UpsertWriter& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        auto resolved_indices = !column_names_.empty() ? ResolveNameProjection() : column_indices_;

        rust::Vec<size_t> rust_indices;
        for (size_t idx : resolved_indices) {
            rust_indices.push_back(idx);
        }
        out = UpsertWriter(table_->create_upsert_writer(std::move(rust_indices)));
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

// TableLookup implementation
TableLookup::TableLookup(ffi::Table* table) noexcept : table_(table) {}

Result TableLookup::CreateLookuper(Lookuper& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        out = Lookuper(table_->new_lookuper());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

// TableScan implementation
TableScan::TableScan(ffi::Table* table) noexcept : table_(table) {}

TableScan& TableScan::ProjectByIndex(std::vector<size_t> column_indices) {
    projection_ = std::move(column_indices);
    name_projection_.clear();
    return *this;
}

TableScan& TableScan::ProjectByName(std::vector<std::string> column_names) {
    name_projection_ = std::move(column_names);
    projection_.clear();
    return *this;
}

std::vector<size_t> TableScan::ResolveNameProjection() const {
    auto ffi_info = table_->get_table_info_from_table();
    const auto& columns = ffi_info.schema.columns;

    std::vector<size_t> indices;
    for (const auto& name : name_projection_) {
        bool found = false;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (std::string(columns[i].name) == name) {
                indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column '" + name + "' not found");
        }
    }
    return indices;
}

Result TableScan::CreateLogScanner(LogScanner& out) { return DoCreateScanner(out, false); }

Result TableScan::CreateRecordBatchLogScanner(LogScanner& out) {
    return DoCreateScanner(out, true);
}

Result TableScan::DoCreateScanner(LogScanner& out, bool is_record_batch) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        auto resolved_indices = !name_projection_.empty() ? ResolveNameProjection() : projection_;
        rust::Vec<size_t> rust_indices;
        for (size_t idx : resolved_indices) {
            rust_indices.push_back(idx);
        }
        out.scanner_ = table_->create_scanner(std::move(rust_indices), is_record_batch);
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

const std::shared_ptr<GenericRow::ColumnMap>& Table::GetColumnMap() const {
    if (!column_map_ && Available()) {
        auto info = GetTableInfo();
        column_map_ = std::make_shared<GenericRow::ColumnMap>();
        for (size_t i = 0; i < info.schema.columns.size(); ++i) {
            (*column_map_)[info.schema.columns[i].name] = {i,
                                                           info.schema.columns[i].data_type.id()};
        }
    }
    return column_map_;
}

GenericRow Table::NewRow() const {
    GenericRow row;
    row.column_map_ = GetColumnMap();
    return row;
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

// WriteResult implementation
WriteResult::WriteResult() noexcept = default;

WriteResult::WriteResult(ffi::WriteResult* inner) noexcept : inner_(inner) {}

WriteResult::~WriteResult() noexcept { Destroy(); }

void WriteResult::Destroy() noexcept {
    if (inner_) {
        // Reconstruct the rust::Box to let Rust drop the value
        rust::Box<ffi::WriteResult>::from_raw(inner_);
        inner_ = nullptr;
    }
}

WriteResult::WriteResult(WriteResult&& other) noexcept : inner_(other.inner_) {
    other.inner_ = nullptr;
}

WriteResult& WriteResult::operator=(WriteResult&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        other.inner_ = nullptr;
    }
    return *this;
}

bool WriteResult::Available() const { return inner_ != nullptr; }

Result WriteResult::Wait() {
    if (!Available()) {
        return utils::make_ok();
    }

    auto ffi_result = inner_->wait();
    return utils::from_ffi_result(ffi_result);
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
    WriteResult wr;
    return Append(row, wr);
}

Result AppendWriter::Append(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }

    try {
        auto ffi_row = utils::to_ffi_generic_row(row);
        auto rust_box = writer_->append(ffi_row);
        out = WriteResult(rust_box.into_raw());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

Result AppendWriter::Flush() {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }

    auto ffi_result = writer_->flush();
    return utils::from_ffi_result(ffi_result);
}

// UpsertWriter implementation
UpsertWriter::UpsertWriter() noexcept = default;

UpsertWriter::UpsertWriter(ffi::UpsertWriter* writer) noexcept : writer_(writer) {}

UpsertWriter::~UpsertWriter() noexcept { Destroy(); }

void UpsertWriter::Destroy() noexcept {
    if (writer_) {
        ffi::delete_upsert_writer(writer_);
        writer_ = nullptr;
    }
}

UpsertWriter::UpsertWriter(UpsertWriter&& other) noexcept : writer_(other.writer_) {
    other.writer_ = nullptr;
}

UpsertWriter& UpsertWriter::operator=(UpsertWriter&& other) noexcept {
    if (this != &other) {
        Destroy();
        writer_ = other.writer_;
        other.writer_ = nullptr;
    }
    return *this;
}

bool UpsertWriter::Available() const { return writer_ != nullptr; }

Result UpsertWriter::Upsert(const GenericRow& row) {
    WriteResult wr;
    return Upsert(row, wr);
}

Result UpsertWriter::Upsert(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }

    try {
        auto ffi_row = utils::to_ffi_generic_row(row);
        auto rust_box = writer_->upsert(ffi_row);
        out = WriteResult(rust_box.into_raw());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

Result UpsertWriter::Delete(const GenericRow& row) {
    WriteResult wr;
    return Delete(row, wr);
}

Result UpsertWriter::Delete(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }

    try {
        auto ffi_row = utils::to_ffi_generic_row(row);
        auto rust_box = writer_->delete_row(ffi_row);
        out = WriteResult(rust_box.into_raw());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

Result UpsertWriter::Flush() {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }

    auto ffi_result = writer_->upsert_flush();
    return utils::from_ffi_result(ffi_result);
}

// Lookuper implementation
Lookuper::Lookuper() noexcept = default;

Lookuper::Lookuper(ffi::Lookuper* lookuper) noexcept : lookuper_(lookuper) {}

Lookuper::~Lookuper() noexcept { Destroy(); }

void Lookuper::Destroy() noexcept {
    if (lookuper_) {
        ffi::delete_lookuper(lookuper_);
        lookuper_ = nullptr;
    }
}

Lookuper::Lookuper(Lookuper&& other) noexcept : lookuper_(other.lookuper_) {
    other.lookuper_ = nullptr;
}

Lookuper& Lookuper::operator=(Lookuper&& other) noexcept {
    if (this != &other) {
        Destroy();
        lookuper_ = other.lookuper_;
        other.lookuper_ = nullptr;
    }
    return *this;
}

bool Lookuper::Available() const { return lookuper_ != nullptr; }

Result Lookuper::Lookup(const GenericRow& pk_row, bool& found, GenericRow& out) {
    if (!Available()) {
        return utils::make_client_error("Lookuper not available");
    }

    try {
        auto ffi_row = utils::to_ffi_generic_row(pk_row);
        auto ffi_result = lookuper_->lookup(ffi_row);
        auto result = utils::from_ffi_result(ffi_result.result);
        if (!result.Ok()) {
            found = false;
            return result;
        }
        found = ffi_result.found;
        if (found) {
            out = utils::from_ffi_generic_row(ffi_result.row);
        }
        return utils::make_ok();
    } catch (const rust::Error& e) {
        found = false;
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        found = false;
        return utils::make_client_error(e.what());
    }
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
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe(bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Subscribe(const std::vector<BucketSubscription>& bucket_offsets) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
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

Result LogScanner::SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id,
                                             int64_t start_offset) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe_partition(partition_id, bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::SubscribePartitionBuckets(
    const std::vector<PartitionBucketSubscription>& subscriptions) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    rust::Vec<ffi::FfiPartitionBucketSubscription> rust_subs;
    for (const auto& sub : subscriptions) {
        ffi::FfiPartitionBucketSubscription ffi_sub;
        ffi_sub.partition_id = sub.partition_id;
        ffi_sub.bucket_id = sub.bucket_id;
        ffi_sub.offset = sub.offset;
        rust_subs.push_back(ffi_sub);
    }

    auto ffi_result = scanner_->subscribe_partition_buckets(std::move(rust_subs));
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Unsubscribe(int32_t bucket_id) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->unsubscribe(bucket_id);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::UnsubscribePartition(int64_t partition_id, int32_t bucket_id) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->unsubscribe_partition(partition_id, bucket_id);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Poll(int64_t timeout_ms, ScanRecords& out) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->poll(timeout_ms);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (!result.Ok()) {
        return result;
    }

    out = utils::from_ffi_scan_records(ffi_result.scan_records);
    return utils::make_ok();
}

ArrowRecordBatch::ArrowRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, int64_t table_id,
                                   int64_t partition_id, int32_t bucket_id,
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
        return utils::make_client_error("LogScanner not available");
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
                std::move(batch_ptr), ffi_batch.table_id, ffi_batch.partition_id,
                ffi_batch.bucket_id, ffi_batch.base_offset));
            out.batches.push_back(std::move(batch_wrapper));

            // Free the container structures that were allocated in Rust after successful import
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);
        } else {
            // Import failed, free the container structures to avoid leaks and return error
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);

            // Return an error indicating that the import failed
            std::string error_msg =
                "Failed to import Arrow record batch: " + import_result.status().ToString();
            return utils::make_client_error(error_msg);
        }
    }

    return utils::make_ok();
}

}  // namespace fluss
