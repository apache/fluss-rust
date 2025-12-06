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

namespace fluss {

// Table implementation
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

ErrorCode Table::NewAppendWriter(AppendWriter& out) {
    if (!Available()) {
        return ErrorCode::TableNotAvailable;
    }

    try {
        out.writer_ = table_->new_append_writer();
        return ErrorCode::Ok;
    } catch (const rust::Error& e) {
        return ErrorCode::OperationFailed;
    }
}

ErrorCode Table::NewLogScanner(LogScanner& out) {
    if (!Available()) {
        return ErrorCode::TableNotAvailable;
    }

    try {
        out.scanner_ = table_->new_log_scanner();
        return ErrorCode::Ok;
    } catch (const rust::Error& e) {
        return ErrorCode::OperationFailed;
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

ErrorCode AppendWriter::Append(const GenericRow& row) {
    if (!Available()) {
        return ErrorCode::AppendWriterNotAvailable;
    }

    auto ffi_row = utils::to_ffi_generic_row(row);
    auto ffi_result = writer_->append(ffi_row);
    if (ffi_result.success) {
        return ErrorCode::Ok;
    }
    return ErrorCode::OperationFailed;
}

ErrorCode AppendWriter::Flush() {
    if (!Available()) {
        return ErrorCode::AppendWriterNotAvailable;
    }

    auto ffi_result = writer_->flush();
    if (ffi_result.success) {
        return ErrorCode::Ok;
    }
    return ErrorCode::OperationFailed;
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

ErrorCode LogScanner::Subscribe(int32_t bucket_id, int64_t start_offset) {
    if (!Available()) {
        return ErrorCode::LogScannerNotAvailable;
    }

    auto ffi_result = scanner_->subscribe(bucket_id, start_offset);
    if (ffi_result.success) {
        return ErrorCode::Ok;
    }
    return ErrorCode::OperationFailed;
}

ErrorCode LogScanner::Poll(int64_t timeout_ms, ScanRecords& out) {
    if (!Available()) {
        return ErrorCode::LogScannerNotAvailable;
    }

    auto ffi_result = scanner_->poll(timeout_ms);
    if (!ffi_result.result.success) {
        return ErrorCode::OperationFailed;
    }

    out = utils::from_ffi_scan_records(ffi_result.scan_records);
    return ErrorCode::Ok;
}

}  // namespace fluss
