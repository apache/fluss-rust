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
// todo:  bindings/cpp/BUILD.bazel still doesn't declare Arrow include/link dependencies.
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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK_INNER(name)                                                                 \
    do {                                                                                  \
        if (!inner_) throw std::logic_error(name ": not available (moved-from or null)"); \
    } while (0)

// ============================================================================
// GenericRow — write-only row backed by opaque Rust GenericRowInner
// ============================================================================

GenericRow::GenericRow() {
    auto box = ffi::new_generic_row(0);
    inner_ = box.into_raw();
}

GenericRow::GenericRow(size_t field_count) {
    auto box = ffi::new_generic_row(field_count);
    inner_ = box.into_raw();
}

GenericRow::~GenericRow() noexcept { Destroy(); }

void GenericRow::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::GenericRowInner>::from_raw(inner_);
        inner_ = nullptr;
    }
    column_map_.reset();
}

GenericRow::GenericRow(GenericRow&& other) noexcept
    : inner_(other.inner_), column_map_(std::move(other.column_map_)) {
    other.inner_ = nullptr;
}

GenericRow& GenericRow::operator=(GenericRow&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        column_map_ = std::move(other.column_map_);
        other.inner_ = nullptr;
    }
    return *this;
}

bool GenericRow::Available() const { return inner_ != nullptr; }

void GenericRow::Reset() {
    CHECK_INNER("GenericRow");
    inner_->gr_reset();
}

void GenericRow::SetNull(size_t idx) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_null(idx);
}
void GenericRow::SetBool(size_t idx, bool v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_bool(idx, v);
}
void GenericRow::SetInt32(size_t idx, int32_t v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_i32(idx, v);
}
void GenericRow::SetInt64(size_t idx, int64_t v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_i64(idx, v);
}
void GenericRow::SetFloat32(size_t idx, float v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_f32(idx, v);
}
void GenericRow::SetFloat64(size_t idx, double v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_f64(idx, v);
}

void GenericRow::SetString(size_t idx, std::string v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_str(idx, v);
}

void GenericRow::SetBytes(size_t idx, std::vector<uint8_t> v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_bytes(idx, rust::Slice<const uint8_t>(v.data(), v.size()));
}

void GenericRow::SetDate(size_t idx, fluss::Date d) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_date(idx, d.days_since_epoch);
}

void GenericRow::SetTime(size_t idx, fluss::Time t) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_time(idx, t.millis_since_midnight);
}

void GenericRow::SetTimestampNtz(size_t idx, fluss::Timestamp ts) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_ts_ntz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void GenericRow::SetTimestampLtz(size_t idx, fluss::Timestamp ts) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_ts_ltz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void GenericRow::SetDecimal(size_t idx, const std::string& value) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_decimal_str(idx, value);
}

// ============================================================================
// RowView — zero-copy read-only row view for scan results
// ============================================================================

size_t RowView::FieldCount() const { return inner_ ? inner_->sv_field_count() : 0; }

TypeId RowView::GetType(size_t idx) const {
    CHECK_INNER("RowView");
    return static_cast<TypeId>(inner_->sv_column_type(idx));
}

bool RowView::IsNull(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_is_null(record_idx_, idx);
}
bool RowView::GetBool(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_get_bool(record_idx_, idx);
}
int32_t RowView::GetInt32(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_get_i32(record_idx_, idx);
}
int64_t RowView::GetInt64(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_get_i64(record_idx_, idx);
}
float RowView::GetFloat32(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_get_f32(record_idx_, idx);
}
double RowView::GetFloat64(size_t idx) const {
    CHECK_INNER("RowView");
    return inner_->sv_get_f64(record_idx_, idx);
}

std::string_view RowView::GetString(size_t idx) const {
    CHECK_INNER("RowView");
    auto s = inner_->sv_get_str(record_idx_, idx);
    return std::string_view(s.data(), s.size());
}

std::pair<const uint8_t*, size_t> RowView::GetBytes(size_t idx) const {
    CHECK_INNER("RowView");
    auto bytes = inner_->sv_get_bytes(record_idx_, idx);
    return {bytes.data(), bytes.size()};
}

Date RowView::GetDate(size_t idx) const {
    CHECK_INNER("RowView");
    return Date{inner_->sv_get_date_days(record_idx_, idx)};
}

Time RowView::GetTime(size_t idx) const {
    CHECK_INNER("RowView");
    return Time{inner_->sv_get_time_millis(record_idx_, idx)};
}

Timestamp RowView::GetTimestamp(size_t idx) const {
    CHECK_INNER("RowView");
    return Timestamp{inner_->sv_get_ts_millis(record_idx_, idx),
                     inner_->sv_get_ts_nanos(record_idx_, idx)};
}

bool RowView::IsDecimal(size_t idx) const { return GetType(idx) == TypeId::Decimal; }

std::string RowView::GetDecimalString(size_t idx) const {
    CHECK_INNER("RowView");
    return std::string(inner_->sv_get_decimal_str(record_idx_, idx));
}

// ============================================================================
// ScanRecords — backed by opaque Rust ScanResultInner
// ============================================================================

// ScanRecords constructor, destructor, move operations are all defaulted in the header.

size_t ScanRecords::Size() const { return inner_ ? inner_->sv_record_count() : 0; }

bool ScanRecords::Empty() const { return Size() == 0; }

void ScanRecords::BuildColumnMap() const {
    if (!inner_) return;
    auto map = std::make_shared<detail::ColumnMap>();
    auto count = inner_->sv_column_count();
    for (size_t i = 0; i < count; ++i) {
        auto name = inner_->sv_column_name(i);
        (*map)[std::string(name.data(), name.size())] = {
            i, static_cast<TypeId>(inner_->sv_column_type(i))};
    }
    column_map_ = std::move(map);
}

const std::shared_ptr<detail::ColumnMap>& ScanRecords::GetColumnMap() const {
    if (!column_map_) {
        BuildColumnMap();
    }
    return column_map_;
}

ScanRecord ScanRecords::operator[](size_t idx) const {
    if (!inner_) {
        throw std::logic_error("ScanRecords: not available (moved-from or null)");
    }
    if (idx >= inner_->sv_record_count()) {
        throw std::out_of_range("ScanRecords: index " + std::to_string(idx) + " out of range (" +
                                std::to_string(inner_->sv_record_count()) + " records)");
    }
    return ScanRecord{inner_->sv_bucket_id(idx),
                      inner_->sv_has_partition_id(idx)
                          ? std::optional<int64_t>(inner_->sv_partition_id(idx))
                          : std::nullopt,
                      inner_->sv_offset(idx),
                      inner_->sv_timestamp(idx),
                      static_cast<ChangeType>(inner_->sv_change_type(idx)),
                      RowView(inner_, idx, GetColumnMap())};
}

ScanRecord ScanRecords::Iterator::operator*() const { return owner_->operator[](idx_); }

// ============================================================================
// LookupResult — backed by opaque Rust LookupResultInner
// ============================================================================

LookupResult::LookupResult() noexcept = default;

LookupResult::~LookupResult() noexcept { Destroy(); }

void LookupResult::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::LookupResultInner>::from_raw(inner_);
        inner_ = nullptr;
        column_map_.reset();
    }
}

LookupResult::LookupResult(LookupResult&& other) noexcept
    : inner_(other.inner_), column_map_(std::move(other.column_map_)) {
    other.inner_ = nullptr;
}

LookupResult& LookupResult::operator=(LookupResult&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        column_map_ = std::move(other.column_map_);
        other.inner_ = nullptr;
    }
    return *this;
}

void LookupResult::BuildColumnMap() const {
    if (!inner_) return;
    auto map = std::make_shared<detail::ColumnMap>();
    auto count = inner_->lv_field_count();
    for (size_t i = 0; i < count; ++i) {
        auto name = inner_->lv_column_name(i);
        (*map)[std::string(name.data(), name.size())] = {
            i, static_cast<TypeId>(inner_->lv_column_type(i))};
    }
    column_map_ = std::move(map);
}

bool LookupResult::Found() const { return inner_ && inner_->lv_found(); }

size_t LookupResult::FieldCount() const { return inner_ ? inner_->lv_field_count() : 0; }

TypeId LookupResult::GetType(size_t idx) const {
    CHECK_INNER("LookupResult");
    return static_cast<TypeId>(inner_->lv_column_type(idx));
}

bool LookupResult::IsNull(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_is_null(idx);
}
bool LookupResult::GetBool(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_bool(idx);
}
int32_t LookupResult::GetInt32(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_i32(idx);
}
int64_t LookupResult::GetInt64(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_i64(idx);
}
float LookupResult::GetFloat32(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_f32(idx);
}
double LookupResult::GetFloat64(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_f64(idx);
}

std::string_view LookupResult::GetString(size_t idx) const {
    CHECK_INNER("LookupResult");
    auto s = inner_->lv_get_str(idx);
    return std::string_view(s.data(), s.size());
}

std::pair<const uint8_t*, size_t> LookupResult::GetBytes(size_t idx) const {
    CHECK_INNER("LookupResult");
    auto bytes = inner_->lv_get_bytes(idx);
    return {bytes.data(), bytes.size()};
}

Date LookupResult::GetDate(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Date{inner_->lv_get_date_days(idx)};
}

Time LookupResult::GetTime(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Time{inner_->lv_get_time_millis(idx)};
}

Timestamp LookupResult::GetTimestamp(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Timestamp{inner_->lv_get_ts_millis(idx), inner_->lv_get_ts_nanos(idx)};
}

bool LookupResult::IsDecimal(size_t idx) const { return GetType(idx) == TypeId::Decimal; }

std::string LookupResult::GetDecimalString(size_t idx) const {
    CHECK_INNER("LookupResult");
    return std::string(inner_->lv_get_decimal_str(idx));
}

// ============================================================================
// Table
// ============================================================================

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

// ============================================================================
// TableAppend
// ============================================================================

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

// ============================================================================
// TableUpsert
// ============================================================================

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

// ============================================================================
// TableLookup
// ============================================================================

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

// ============================================================================
// TableScan
// ============================================================================

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

// ============================================================================
// WriteResult
// ============================================================================

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

// ============================================================================
// AppendWriter
// ============================================================================

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
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    try {
        auto rust_box = writer_->append(*row.inner_);
        out = WriteResult(rust_box.into_raw());
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(e.what());
    } catch (const std::exception& e) {
        return utils::make_client_error(e.what());
    }
}

Result AppendWriter::AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    WriteResult wr;
    return AppendArrowBatch(batch, wr);
}

Result AppendWriter::AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }
    if (!batch) {
        return utils::make_client_error("Arrow RecordBatch is null");
    }

    // Export via Arrow C Data Interface
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    auto status = arrow::ExportRecordBatch(*batch, &c_array, &c_schema);
    if (!status.ok()) {
        return utils::make_client_error("Failed to export Arrow batch: " + status.ToString());
    }

    // Heap-allocate for Rust ownership transfer
    auto* array_heap = new ArrowArray(std::move(c_array));
    auto* schema_heap = new ArrowSchema(std::move(c_schema));

    try {
        // Rust takes ownership of both pointers immediately via Box::from_raw(),
        // so after this call (success or exception) C++ must NOT free them.
        auto result_box = writer_->append_arrow_batch(reinterpret_cast<size_t>(array_heap),
                                                      reinterpret_cast<size_t>(schema_heap));
        out.Destroy();
        out.inner_ = result_box.into_raw();
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_client_error(std::string(e.what()));
    } catch (const std::exception& e) {
        return utils::make_client_error(std::string(e.what()));
    }
}

Result AppendWriter::Flush() {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }

    auto ffi_result = writer_->flush();
    return utils::from_ffi_result(ffi_result);
}

// ============================================================================
// UpsertWriter
// ============================================================================

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
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    try {
        auto rust_box = writer_->upsert(*row.inner_);
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
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    try {
        auto rust_box = writer_->delete_row(*row.inner_);
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

// ============================================================================
// Lookuper
// ============================================================================

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

Result Lookuper::Lookup(const GenericRow& pk_row, LookupResult& out) {
    if (!Available()) {
        return utils::make_client_error("Lookuper not available");
    }
    if (!pk_row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    auto result_box = lookuper_->lookup(*pk_row.inner_);
    if (result_box->lv_has_error()) {
        return utils::make_error(result_box->lv_error_code(),
                                 std::string(result_box->lv_error_message()));
    }

    out.Destroy();
    out.inner_ = result_box.into_raw();
    return utils::make_ok();
}

// ============================================================================
// LogScanner
// ============================================================================

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

    auto result_box = scanner_->poll(timeout_ms);
    if (result_box->sv_has_error()) {
        return utils::make_error(result_box->sv_error_code(),
                                 std::string(result_box->sv_error_message()));
    }

    out.column_map_.reset();
    out.inner_ = std::shared_ptr<ffi::ScanResultInner>(
        result_box.into_raw(),
        [](ffi::ScanResultInner* p) { rust::Box<ffi::ScanResultInner>::from_raw(p); });
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
