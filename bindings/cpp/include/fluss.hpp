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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declare Arrow classes to avoid including heavy Arrow headers in header
namespace arrow {
    class RecordBatch;
}

namespace fluss {

namespace ffi {
    struct Connection;
    struct Admin;
    struct Table;
    struct AppendWriter;
    struct WriteResult;
    struct LogScanner;
}  // namespace ffi

enum class DataType {
    Boolean = 1,
    TinyInt = 2,
    SmallInt = 3,
    Int = 4,
    BigInt = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Bytes = 9,
    Date = 10,
    Time = 11,
    Timestamp = 12,
    TimestampLtz = 13,
};

enum class DatumType {
    Null = 0,
    Bool = 1,
    Int32 = 2,
    Int64 = 3,
    Float32 = 4,
    Float64 = 5,
    String = 6,
    Bytes = 7,
};

constexpr int64_t EARLIEST_OFFSET = -2;
constexpr int64_t LATEST_OFFSET = -1;

enum class OffsetSpec {
    Earliest = 0,
    Latest = 1,
    Timestamp = 2,
};

struct OffsetQuery {
    OffsetSpec spec;
    int64_t timestamp{0};

    static OffsetQuery Earliest() { return {OffsetSpec::Earliest, 0}; }
    static OffsetQuery Latest() { return {OffsetSpec::Latest, 0}; }
    static OffsetQuery FromTimestamp(int64_t ts) { return {OffsetSpec::Timestamp, ts}; }
};

struct Result {
    int32_t error_code{0};
    std::string error_message;

    bool Ok() const { return error_code == 0; }
};

struct TablePath {
    std::string database_name;
    std::string table_name;

    TablePath() = default;
    TablePath(std::string db, std::string tbl)
        : database_name(std::move(db)), table_name(std::move(tbl)) {}

    std::string ToString() const { return database_name + "." + table_name; }
};

struct Column {
    std::string name;
    DataType data_type;
    std::string comment;
};

struct Schema {
    std::vector<Column> columns;
    std::vector<std::string> primary_keys;

    class Builder {
    public:
        Builder& AddColumn(std::string name, DataType type,
                           std::string comment = "") {
            columns_.push_back({std::move(name), type, std::move(comment)});
            return *this;
        }

        Builder& SetPrimaryKeys(std::vector<std::string> keys) {
            primary_keys_ = std::move(keys);
            return *this;
        }

        Schema Build() {
            return Schema{std::move(columns_), std::move(primary_keys_)};
        }

    private:
        std::vector<Column> columns_;
        std::vector<std::string> primary_keys_;
    };

    static Builder NewBuilder() { return Builder(); }
};

struct TableDescriptor {
    Schema schema;
    std::vector<std::string> partition_keys;
    int32_t bucket_count{0};
    std::vector<std::string> bucket_keys;
    std::unordered_map<std::string, std::string> properties;
    std::string comment;

    class Builder {
    public:
        Builder& SetSchema(Schema s) {
            schema_ = std::move(s);
            return *this;
        }

        Builder& SetPartitionKeys(std::vector<std::string> keys) {
            partition_keys_ = std::move(keys);
            return *this;
        }

        Builder& SetBucketCount(int32_t count) {
            bucket_count_ = count;
            return *this;
        }

        Builder& SetBucketKeys(std::vector<std::string> keys) {
            bucket_keys_ = std::move(keys);
            return *this;
        }

        Builder& SetProperty(std::string key, std::string value) {
            properties_[std::move(key)] = std::move(value);
            return *this;
        }

        Builder& SetComment(std::string comment) {
            comment_ = std::move(comment);
            return *this;
        }

        TableDescriptor Build() {
            return TableDescriptor{std::move(schema_),
                                   std::move(partition_keys_),
                                   bucket_count_,
                                   std::move(bucket_keys_),
                                   std::move(properties_),
                                   std::move(comment_)};
        }

    private:
        Schema schema_;
        std::vector<std::string> partition_keys_;
        int32_t bucket_count_{0};
        std::vector<std::string> bucket_keys_;
        std::unordered_map<std::string, std::string> properties_;
        std::string comment_;
    };

    static Builder NewBuilder() { return Builder(); }
};

struct TableInfo {
    int64_t table_id;
    int32_t schema_id;
    TablePath table_path;
    int64_t created_time;
    int64_t modified_time;
    std::vector<std::string> primary_keys;
    std::vector<std::string> bucket_keys;
    std::vector<std::string> partition_keys;
    int32_t num_buckets;
    bool has_primary_key;
    bool is_partitioned;
    std::unordered_map<std::string, std::string> properties;
    std::string comment;
    Schema schema;
};

struct Datum {
    DatumType type{DatumType::Null};
    bool bool_val{false};
    int32_t i32_val{0};
    int64_t i64_val{0};
    float f32_val{0.0F};
    double f64_val{0.0};
    std::string string_val;
    std::vector<uint8_t> bytes_val;

    static Datum Null() { return {}; }
    static Datum Bool(bool v) {
        Datum d;
        d.type = DatumType::Bool;
        d.bool_val = v;
        return d;
    }
    static Datum Int32(int32_t v) {
        Datum d;
        d.type = DatumType::Int32;
        d.i32_val = v;
        return d;
    }
    static Datum Int64(int64_t v) {
        Datum d;
        d.type = DatumType::Int64;
        d.i64_val = v;
        return d;
    }
    static Datum Float32(float v) {
        Datum d;
        d.type = DatumType::Float32;
        d.f32_val = v;
        return d;
    }
    static Datum Float64(double v) {
        Datum d;
        d.type = DatumType::Float64;
        d.f64_val = v;
        return d;
    }
    static Datum String(std::string v) {
        Datum d;
        d.type = DatumType::String;
        d.string_val = std::move(v);
        return d;
    }
    static Datum Bytes(std::vector<uint8_t> v) {
        Datum d;
        d.type = DatumType::Bytes;
        d.bytes_val = std::move(v);
        return d;
    }
};

struct GenericRow {
    std::vector<Datum> fields;

    void SetNull(size_t idx) {
        EnsureSize(idx);
        fields[idx] = Datum::Null();
    }

    void SetBool(size_t idx, bool v) {
        EnsureSize(idx);
        fields[idx] = Datum::Bool(v);
    }

    void SetInt32(size_t idx, int32_t v) {
        EnsureSize(idx);
        fields[idx] = Datum::Int32(v);
    }

    void SetInt64(size_t idx, int64_t v) {
        EnsureSize(idx);
        fields[idx] = Datum::Int64(v);
    }

    void SetFloat32(size_t idx, float v) {
        EnsureSize(idx);
        fields[idx] = Datum::Float32(v);
    }

    void SetFloat64(size_t idx, double v) {
        EnsureSize(idx);
        fields[idx] = Datum::Float64(v);
    }

    void SetString(size_t idx, std::string v) {
        EnsureSize(idx);
        fields[idx] = Datum::String(std::move(v));
    }

    void SetBytes(size_t idx, std::vector<uint8_t> v) {
        EnsureSize(idx);
        fields[idx] = Datum::Bytes(std::move(v));
    }

private:
    void EnsureSize(size_t idx) {
        if (fields.size() <= idx) {
            fields.resize(idx + 1);
        }
    }
};

struct ScanRecord {
    int32_t bucket_id;
    int64_t offset;
    int64_t timestamp;
    GenericRow row;
};

struct ScanRecords {
    std::vector<ScanRecord> records;

    size_t Size() const { return records.size(); }
    bool Empty() const { return records.empty(); }
    const ScanRecord& operator[](size_t idx) const { return records[idx]; }

    auto begin() const { return records.begin(); }
    auto end() const { return records.end(); }
};

class ArrowRecordBatch {
public:

    std::shared_ptr<arrow::RecordBatch> GetArrowRecordBatch() const { return batch_; }

    bool Available() const;

    // Get number of rows in the batch
    int64_t NumRows() const;
    
    // Get ScanBatch metadata
    int64_t GetTableId() const;
    int64_t GetPartitionId() const;
    int32_t GetBucketId() const;
    int64_t GetBaseOffset() const;
    int64_t GetLastOffset() const;

private:
    friend class LogScanner;
    explicit ArrowRecordBatch(
        std::shared_ptr<arrow::RecordBatch> batch,
        int64_t table_id,
        int64_t partition_id,
        int32_t bucket_id,
        int64_t base_offset) noexcept;

    std::shared_ptr<arrow::RecordBatch> batch_{nullptr};

    int64_t table_id_;
    int64_t partition_id_;
    int32_t bucket_id_;
    int64_t base_offset_;
};


struct ArrowRecordBatches {
    std::vector<std::unique_ptr<ArrowRecordBatch>> batches;

    size_t Size() const { return batches.size(); }
    bool Empty() const { return batches.empty(); }
    const std::unique_ptr<ArrowRecordBatch>& operator[](size_t idx) const { return batches[idx]; }

    auto begin() const { return batches.begin(); }
    auto end() const { return batches.end(); }
};

struct BucketOffset {
    int64_t table_id;
    int64_t partition_id;
    int32_t bucket_id;
    int64_t offset;
};

struct BucketSubscription {
    int32_t bucket_id;
    int64_t offset;
};

struct LakeSnapshot {
    int64_t snapshot_id;
    std::vector<BucketOffset> bucket_offsets;
};

struct PartitionInfo {
    int64_t partition_id;
    std::string partition_name;
};

class AppendWriter;
class WriteResult;
class LogScanner;
class Admin;
class Table;
class TableScan;

class Connection {
public:
    Connection() noexcept;
    ~Connection() noexcept;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    static Result Connect(const std::string& bootstrap_server, Connection& out);

    bool Available() const;

    Result GetAdmin(Admin& out);
    Result GetTable(const TablePath& table_path, Table& out);

private:
    void Destroy() noexcept;
    ffi::Connection* conn_{nullptr};
};

class Admin {
public:
    Admin() noexcept;
    ~Admin() noexcept;

    Admin(const Admin&) = delete;
    Admin& operator=(const Admin&) = delete;
    Admin(Admin&& other) noexcept;
    Admin& operator=(Admin&& other) noexcept;

    bool Available() const;

    Result CreateTable(const TablePath& table_path,
                       const TableDescriptor& descriptor,
                       bool ignore_if_exists = false);

    Result DropTable(const TablePath& table_path, bool ignore_if_not_exists = false);

    Result GetTable(const TablePath& table_path, TableInfo& out);

    Result GetLatestLakeSnapshot(const TablePath& table_path, LakeSnapshot& out);

    Result ListOffsets(const TablePath& table_path,
                       const std::vector<int32_t>& bucket_ids,
                       const OffsetQuery& offset_query,
                       std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionOffsets(const TablePath& table_path,
                              const std::string& partition_name,
                              const std::vector<int32_t>& bucket_ids,
                              const OffsetQuery& offset_query,
                              std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionInfos(const TablePath& table_path,
                             std::vector<PartitionInfo>& out);

private:
    Result DoListOffsets(const TablePath& table_path,
                       const std::vector<int32_t>& bucket_ids,
                       const OffsetQuery& offset_query,
                       std::unordered_map<int32_t, int64_t>& out,
                       const std::string* partition_name = nullptr);

    friend class Connection;
    Admin(ffi::Admin* admin) noexcept;

    void Destroy() noexcept;
    ffi::Admin* admin_{nullptr};
};

class Table {
public:
    Table() noexcept;
    ~Table() noexcept;

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;
    Table(Table&& other) noexcept;
    Table& operator=(Table&& other) noexcept;

    bool Available() const;

    Result NewAppendWriter(AppendWriter& out);
    TableScan NewScan();

    TableInfo GetTableInfo() const;
    TablePath GetTablePath() const;
    bool HasPrimaryKey() const;

private:
    friend class Connection;
    friend class TableScan;
    Table(ffi::Table* table) noexcept;

    void Destroy() noexcept;
    ffi::Table* table_{nullptr};
};

class TableScan {
public:
    TableScan(const TableScan&) = delete;
    TableScan& operator=(const TableScan&) = delete;
    TableScan(TableScan&&) noexcept = default;
    TableScan& operator=(TableScan&&) noexcept = default;

    TableScan& Project(std::vector<size_t> column_indices);

    Result CreateLogScanner(LogScanner& out);
    Result CreateRecordBatchScanner(LogScanner& out);

private:
    friend class Table;
    explicit TableScan(ffi::Table* table) noexcept;

    ffi::Table* table_{nullptr};
    std::vector<size_t> projection_;
};

class WriteResult {
public:
    WriteResult() noexcept;
    ~WriteResult() noexcept;

    WriteResult(const WriteResult&) = delete;
    WriteResult& operator=(const WriteResult&) = delete;
    WriteResult(WriteResult&& other) noexcept;
    WriteResult& operator=(WriteResult&& other) noexcept;

    bool Available() const;

    /// Wait for server acknowledgment of the write.
    /// For fire-and-forget, simply let the WriteResult go out of scope.
    Result Wait();

private:
    friend class AppendWriter;
    WriteResult(ffi::WriteResult* inner) noexcept;

    void Destroy() noexcept;
    ffi::WriteResult* inner_{nullptr};
};

class AppendWriter {
public:
    AppendWriter() noexcept;
    ~AppendWriter() noexcept;

    AppendWriter(const AppendWriter&) = delete;
    AppendWriter& operator=(const AppendWriter&) = delete;
    AppendWriter(AppendWriter&& other) noexcept;
    AppendWriter& operator=(AppendWriter&& other) noexcept;

    bool Available() const;

    Result Append(const GenericRow& row);
    Result Append(const GenericRow& row, WriteResult& out);
    Result Flush();

private:
    friend class Table;
    AppendWriter(ffi::AppendWriter* writer) noexcept;

    void Destroy() noexcept;
    ffi::AppendWriter* writer_{nullptr};
};

class LogScanner {
public:
    LogScanner() noexcept;
    ~LogScanner() noexcept;

    LogScanner(const LogScanner&) = delete;
    LogScanner& operator=(const LogScanner&) = delete;
    LogScanner(LogScanner&& other) noexcept;
    LogScanner& operator=(LogScanner&& other) noexcept;

    bool Available() const;

    Result Subscribe(int32_t bucket_id, int64_t start_offset);
    Result Subscribe(const std::vector<BucketSubscription>& bucket_offsets);
    Result SubscribePartition(int64_t partition_id, int32_t bucket_id, int64_t start_offset);
    Result UnsubscribePartition(int64_t partition_id, int32_t bucket_id);
    Result Poll(int64_t timeout_ms, ScanRecords& out);
    Result PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out);

private:
    friend class Table;
    friend class TableScan;
    LogScanner(ffi::LogScanner* scanner) noexcept;

    void Destroy() noexcept;
    ffi::LogScanner* scanner_{nullptr};
};

}  // namespace fluss
