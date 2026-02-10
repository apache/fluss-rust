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

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
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
struct UpsertWriter;
struct Lookuper;
}  // namespace ffi

struct Date {
    int32_t days_since_epoch{0};

    static Date FromDays(int32_t days) { return {days}; }
    static Date FromYMD(int year, int month, int day);

    int Year() const;
    int Month() const;
    int Day() const;
};

struct Time {
    static constexpr int32_t kMillisPerSecond = 1000;
    static constexpr int32_t kMillisPerMinute = 60 * kMillisPerSecond;
    static constexpr int32_t kMillisPerHour = 60 * kMillisPerMinute;

    int32_t millis_since_midnight{0};

    static Time FromMillis(int32_t ms) { return {ms}; }
    static Time FromHMS(int hour, int minute, int second, int millis = 0) {
        return {hour * kMillisPerHour + minute * kMillisPerMinute + second * kMillisPerSecond +
                millis};
    }

    int Hour() const { return millis_since_midnight / kMillisPerHour; }
    int Minute() const { return (millis_since_midnight % kMillisPerHour) / kMillisPerMinute; }
    int Second() const { return (millis_since_midnight % kMillisPerMinute) / kMillisPerSecond; }
    int Millis() const { return millis_since_midnight % kMillisPerSecond; }
};

struct Timestamp {
    static constexpr int32_t kMaxNanoOfMillisecond = 999999;
    static constexpr int64_t kNanosPerMilli = 1000000;

    int64_t epoch_millis{0};
    int32_t nano_of_millisecond{0};

    static Timestamp FromMillis(int64_t ms) { return {ms, 0}; }
    static Timestamp FromMillisNanos(int64_t ms, int32_t nanos) {
        if (nanos < 0) nanos = 0;
        if (nanos > kMaxNanoOfMillisecond) nanos = kMaxNanoOfMillisecond;
        return {ms, nanos};
    }
    static Timestamp FromTimePoint(std::chrono::system_clock::time_point tp) {
        auto duration = tp.time_since_epoch();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        auto ms = ns / kNanosPerMilli;
        auto nano_of_ms = static_cast<int32_t>(ns % kNanosPerMilli);
        if (nano_of_ms < 0) {
            nano_of_ms += kNanosPerMilli;
            ms -= 1;
        }
        return {ms, nano_of_ms};
    }
};

enum class TypeId {
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
    Decimal = 14,
};

class DataType {
   public:
    explicit DataType(TypeId id, int32_t p = 0, int32_t s = 0)
        : id_(id), precision_(p), scale_(s) {}

    static DataType Boolean() { return DataType(TypeId::Boolean); }
    static DataType TinyInt() { return DataType(TypeId::TinyInt); }
    static DataType SmallInt() { return DataType(TypeId::SmallInt); }
    static DataType Int() { return DataType(TypeId::Int); }
    static DataType BigInt() { return DataType(TypeId::BigInt); }
    static DataType Float() { return DataType(TypeId::Float); }
    static DataType Double() { return DataType(TypeId::Double); }
    static DataType String() { return DataType(TypeId::String); }
    static DataType Bytes() { return DataType(TypeId::Bytes); }
    static DataType Date() { return DataType(TypeId::Date); }
    static DataType Time() { return DataType(TypeId::Time); }
    static DataType Timestamp(int32_t precision = 6) {
        return DataType(TypeId::Timestamp, precision, 0);
    }
    static DataType TimestampLtz(int32_t precision = 6) {
        return DataType(TypeId::TimestampLtz, precision, 0);
    }
    static DataType Decimal(int32_t precision, int32_t scale) {
        return DataType(TypeId::Decimal, precision, scale);
    }

    TypeId id() const { return id_; }
    int32_t precision() const { return precision_; }
    int32_t scale() const { return scale_; }

   private:
    TypeId id_;
    int32_t precision_{0};
    int32_t scale_{0};
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
    DecimalI64 = 8,
    DecimalI128 = 9,
    DecimalString = 10,
    Date = 11,
    Time = 12,
    TimestampNtz = 13,
    TimestampLtz = 14,
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
        Builder& AddColumn(std::string name, DataType type, std::string comment = "") {
            columns_.push_back({std::move(name), std::move(type), std::move(comment)});
            return *this;
        }

        Builder& SetPrimaryKeys(std::vector<std::string> keys) {
            primary_keys_ = std::move(keys);
            return *this;
        }

        Schema Build() { return Schema{std::move(columns_), std::move(primary_keys_)}; }

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
            return TableDescriptor{std::move(schema_),     std::move(partition_keys_),
                                   bucket_count_,          std::move(bucket_keys_),
                                   std::move(properties_), std::move(comment_)};
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

namespace detail {
struct FfiAccess;
}

struct Datum {
    friend struct GenericRow;
    friend struct detail::FfiAccess;

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
    static Datum Date(fluss::Date d) {
        Datum dat;
        dat.type = DatumType::Date;
        dat.i32_val = d.days_since_epoch;
        return dat;
    }
    static Datum Time(fluss::Time t) {
        Datum dat;
        dat.type = DatumType::Time;
        dat.i32_val = t.millis_since_midnight;
        return dat;
    }
    static Datum TimestampNtz(fluss::Timestamp ts) {
        Datum dat;
        dat.type = DatumType::TimestampNtz;
        dat.i64_val = ts.epoch_millis;
        dat.i32_val = ts.nano_of_millisecond;
        return dat;
    }
    static Datum TimestampLtz(fluss::Timestamp ts) {
        Datum dat;
        dat.type = DatumType::TimestampLtz;
        dat.i64_val = ts.epoch_millis;
        dat.i32_val = ts.nano_of_millisecond;
        return dat;
    }
    // Stores the decimal string as-is. Rust side will parse via BigDecimal,
    // look up (p,s) from the schema, validate, and create the Decimal.
    static Datum DecimalString(std::string str) {
        Datum d;
        d.type = DatumType::DecimalString;
        d.string_val = std::move(str);
        return d;
    }

   private:
    DatumType type{DatumType::Null};
    bool bool_val{false};
    int32_t i32_val{0};
    int64_t i64_val{0};
    float f32_val{0.0F};
    double f64_val{0.0};
    std::string string_val;
    std::vector<uint8_t> bytes_val;
    int32_t decimal_precision{0};  // Decimal: precision (total digits)
    int32_t decimal_scale{0};      // Decimal: scale (digits after decimal point)
    int64_t i128_hi{0};            // Decimal (i128): high 64 bits of unscaled value
    int64_t i128_lo{0};            // Decimal (i128): low 64 bits of unscaled value

    DatumType GetType() const { return type; }
    bool IsNull() const { return type == DatumType::Null; }
    bool GetBool() const { return bool_val; }
    int32_t GetInt32() const { return i32_val; }
    int64_t GetInt64() const { return i64_val; }
    float GetFloat32() const { return f32_val; }
    double GetFloat64() const { return f64_val; }
    const std::string& GetString() const { return string_val; }
    const std::vector<uint8_t>& GetBytes() const { return bytes_val; }
    fluss::Date GetDate() const { return {i32_val}; }
    fluss::Time GetTime() const { return {i32_val}; }
    fluss::Timestamp GetTimestamp() const { return {i64_val, i32_val}; }

    bool IsDecimal() const {
        return type == DatumType::DecimalI64 || type == DatumType::DecimalI128 ||
               type == DatumType::DecimalString;
    }

    std::string DecimalToString() const {
        if (type == DatumType::DecimalI64) {
            return FormatUnscaled64(i64_val, decimal_scale);
        } else if (type == DatumType::DecimalI128) {
            unsigned __int128 uval =
                (static_cast<unsigned __int128>(static_cast<uint64_t>(i128_hi)) << 64) |
                static_cast<unsigned __int128>(static_cast<uint64_t>(i128_lo));
            __int128 val = static_cast<__int128>(uval);
            return FormatUnscaled128(val, decimal_scale);
        } else if (type == DatumType::DecimalString) {
            return string_val;
        }
        return "";
    }

    static std::string FormatUnscaled64(int64_t unscaled, int32_t scale) {
        bool negative = unscaled < 0;
        uint64_t abs_val =
            negative ? -static_cast<uint64_t>(unscaled) : static_cast<uint64_t>(unscaled);
        std::string digits = std::to_string(abs_val);
        if (scale <= 0) {
            return (negative ? "-" : "") + digits;
        }
        while (static_cast<int32_t>(digits.size()) <= scale) {
            digits = "0" + digits;
        }
        auto pos = digits.size() - static_cast<size_t>(scale);
        return (negative ? "-" : "") + digits.substr(0, pos) + "." + digits.substr(pos);
    }

    static std::string FormatUnscaled128(__int128 val, int32_t scale) {
        bool negative = val < 0;
        unsigned __int128 abs_val =
            negative ? -static_cast<unsigned __int128>(val) : static_cast<unsigned __int128>(val);
        std::string digits;
        if (abs_val == 0) {
            digits = "0";
        } else {
            while (abs_val > 0) {
                digits = static_cast<char>('0' + static_cast<int>(abs_val % 10)) + digits;
                abs_val /= 10;
            }
        }
        if (scale <= 0) {
            return (negative ? "-" : "") + digits;
        }
        while (static_cast<int32_t>(digits.size()) <= scale) {
            digits = "0" + digits;
        }
        auto pos = digits.size() - static_cast<size_t>(scale);
        return (negative ? "-" : "") + digits.substr(0, pos) + "." + digits.substr(pos);
    }
};

struct GenericRow {
    friend struct detail::FfiAccess;

    size_t FieldCount() const { return fields.size(); }

    // ── Index-based getters ──────────────────────────────────────────
    DatumType GetType(size_t idx) const { return GetField(idx).GetType(); }
    bool IsNull(size_t idx) const { return GetField(idx).IsNull(); }
    bool GetBool(size_t idx) const { return GetTypedField(idx, DatumType::Bool).GetBool(); }
    int32_t GetInt32(size_t idx) const { return GetTypedField(idx, DatumType::Int32).GetInt32(); }
    int64_t GetInt64(size_t idx) const { return GetTypedField(idx, DatumType::Int64).GetInt64(); }
    float GetFloat32(size_t idx) const {
        return GetTypedField(idx, DatumType::Float32).GetFloat32();
    }
    double GetFloat64(size_t idx) const {
        return GetTypedField(idx, DatumType::Float64).GetFloat64();
    }
    const std::string& GetString(size_t idx) const {
        return GetTypedField(idx, DatumType::String).GetString();
    }
    const std::vector<uint8_t>& GetBytes(size_t idx) const {
        return GetTypedField(idx, DatumType::Bytes).GetBytes();
    }
    fluss::Date GetDate(size_t idx) const { return GetTypedField(idx, DatumType::Date).GetDate(); }
    fluss::Time GetTime(size_t idx) const { return GetTypedField(idx, DatumType::Time).GetTime(); }
    fluss::Timestamp GetTimestamp(size_t idx) const {
        const auto& d = GetField(idx);
        auto t = d.GetType();
        if (t != DatumType::TimestampNtz && t != DatumType::TimestampLtz) {
            throw std::runtime_error("GenericRow: field " + std::to_string(idx) +
                                     " is not a Timestamp type");
        }
        return d.GetTimestamp();
    }
    bool IsDecimal(size_t idx) const { return GetField(idx).IsDecimal(); }
    std::string DecimalToString(size_t idx) const {
        const auto& d = GetField(idx);
        if (!d.IsDecimal()) {
            throw std::runtime_error("GenericRow: field " + std::to_string(idx) +
                                     " is not a Decimal type");
        }
        return d.DecimalToString();
    }

    // ── Index-based setters ──────────────────────────────────────────
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

    void SetDate(size_t idx, fluss::Date d) {
        EnsureSize(idx);
        fields[idx] = Datum::Date(d);
    }

    void SetTime(size_t idx, fluss::Time t) {
        EnsureSize(idx);
        fields[idx] = Datum::Time(t);
    }

    void SetTimestampNtz(size_t idx, fluss::Timestamp ts) {
        EnsureSize(idx);
        fields[idx] = Datum::TimestampNtz(ts);
    }

    void SetTimestampLtz(size_t idx, fluss::Timestamp ts) {
        EnsureSize(idx);
        fields[idx] = Datum::TimestampLtz(ts);
    }

    void SetDecimal(size_t idx, const std::string& value) {
        EnsureSize(idx);
        fields[idx] = Datum::DecimalString(value);
    }

    // ── Name-based setters (require schema — see Table::NewRow()) ───
    void Set(const std::string& name, std::nullptr_t) { SetNull(Resolve(name)); }
    void Set(const std::string& name, bool v) { SetBool(Resolve(name), v); }
    void Set(const std::string& name, int32_t v) { SetInt32(Resolve(name), v); }
    void Set(const std::string& name, int64_t v) { SetInt64(Resolve(name), v); }
    void Set(const std::string& name, float v) { SetFloat32(Resolve(name), v); }
    void Set(const std::string& name, double v) { SetFloat64(Resolve(name), v); }
    // const char* overload to prevent "string literal" → bool conversion
    void Set(const std::string& name, const char* v) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::Decimal) {
            SetDecimal(idx, v);
        } else if (type == TypeId::String) {
            SetString(idx, v);
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a string or decimal column");
        }
    }
    void Set(const std::string& name, std::string v) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::Decimal) {
            SetDecimal(idx, v);
        } else if (type == TypeId::String) {
            SetString(idx, std::move(v));
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a string or decimal column");
        }
    }
    void Set(const std::string& name, std::vector<uint8_t> v) {
        SetBytes(Resolve(name), std::move(v));
    }
    void Set(const std::string& name, fluss::Date d) { SetDate(Resolve(name), d); }
    void Set(const std::string& name, fluss::Time t) { SetTime(Resolve(name), t); }
    void Set(const std::string& name, fluss::Timestamp ts) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::TimestampLtz) {
            SetTimestampLtz(idx, ts);
        } else if (type == TypeId::Timestamp) {
            SetTimestampNtz(idx, ts);
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a timestamp column");
        }
    }

   private:
    friend class Table;
    struct ColumnInfo {
        size_t index;
        TypeId type_id;
    };
    using ColumnMap = std::unordered_map<std::string, ColumnInfo>;
    std::vector<Datum> fields;
    std::shared_ptr<ColumnMap> column_map_;

    size_t Resolve(const std::string& name) const { return ResolveColumn(name).index; }

    const ColumnInfo& ResolveColumn(const std::string& name) const {
        if (!column_map_) {
            throw std::runtime_error(
                "GenericRow: name-based Set() requires a schema. "
                "Use Table::NewRow() to create a schema-aware row.");
        }
        auto it = column_map_->find(name);
        if (it == column_map_->end()) {
            throw std::runtime_error("GenericRow: unknown column '" + name + "'");
        }
        return it->second;
    }

    const Datum& GetField(size_t idx) const {
        if (idx >= fields.size()) {
            throw std::runtime_error("GenericRow: index " + std::to_string(idx) +
                                     " out of bounds (size=" + std::to_string(fields.size()) + ")");
        }
        return fields[idx];
    }

    const Datum& GetTypedField(size_t idx, DatumType expected) const {
        const auto& d = GetField(idx);
        if (d.GetType() != expected) {
            throw std::runtime_error("GenericRow: field " + std::to_string(idx) +
                                     " type mismatch: expected " +
                                     std::to_string(static_cast<int>(expected)) + ", got " +
                                     std::to_string(static_cast<int>(d.GetType())));
        }
        return d;
    }

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
    explicit ArrowRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, int64_t table_id,
                              int64_t partition_id, int32_t bucket_id,
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

struct PartitionBucketSubscription {
    int64_t partition_id;
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

/// Descriptor for create_database (optional). Leave comment and properties empty for default.
struct DatabaseDescriptor {
    std::string comment;
    std::unordered_map<std::string, std::string> properties;
};

/// Metadata returned by GetDatabaseInfo.
struct DatabaseInfo {
    std::string database_name;
    std::string comment;
    std::unordered_map<std::string, std::string> properties;
    int64_t created_time{0};
    int64_t modified_time{0};
};

class AppendWriter;
class UpsertWriter;
class Lookuper;
class WriteResult;
class LogScanner;
class Admin;
class Table;
class TableScan;

struct Configuration {
    // Coordinator server address
    std::string bootstrap_server{"127.0.0.1:9123"};
    // Max request size in bytes (10 MB)
    int32_t request_max_size{10 * 1024 * 1024};
    // Writer acknowledgment mode: "all", "0", "1", or "-1"
    std::string writer_acks{"all"};
    // Max number of writer retries
    int32_t writer_retries{std::numeric_limits<int32_t>::max()};
    // Writer batch size in bytes (2 MB)
    int32_t writer_batch_size{2 * 1024 * 1024};
    // Number of remote log batches to prefetch during scanning
    size_t scanner_remote_log_prefetch_num{4};
    // Number of threads for downloading remote log data
    size_t scanner_remote_log_download_threads{3};
};

class Connection {
   public:
    Connection() noexcept;
    ~Connection() noexcept;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    static Result Create(const Configuration& config, Connection& out);

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

    Result CreateTable(const TablePath& table_path, const TableDescriptor& descriptor,
                       bool ignore_if_exists = false);

    Result DropTable(const TablePath& table_path, bool ignore_if_not_exists = false);

    Result GetTable(const TablePath& table_path, TableInfo& out);

    Result GetLatestLakeSnapshot(const TablePath& table_path, LakeSnapshot& out);

    Result ListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                       const OffsetQuery& offset_query, std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionOffsets(const TablePath& table_path, const std::string& partition_name,
                                const std::vector<int32_t>& bucket_ids,
                                const OffsetQuery& offset_query,
                                std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionInfos(const TablePath& table_path, std::vector<PartitionInfo>& out);

    Result CreatePartition(const TablePath& table_path,
                           const std::unordered_map<std::string, std::string>& partition_spec,
                           bool ignore_if_exists = false);

    Result DropPartition(const TablePath& table_path,
                         const std::unordered_map<std::string, std::string>& partition_spec,
                         bool ignore_if_not_exists = false);

    Result CreateDatabase(const std::string& database_name, const DatabaseDescriptor& descriptor,
                          bool ignore_if_exists = false);

    Result DropDatabase(const std::string& database_name, bool ignore_if_not_exists = false,
                        bool cascade = true);

    Result ListDatabases(std::vector<std::string>& out);

    Result DatabaseExists(const std::string& database_name, bool& out);

    Result GetDatabaseInfo(const std::string& database_name, DatabaseInfo& out);

    Result ListTables(const std::string& database_name, std::vector<std::string>& out);

    Result TableExists(const TablePath& table_path, bool& out);

   private:
    Result DoListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                         const OffsetQuery& offset_query, std::unordered_map<int32_t, int64_t>& out,
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

    GenericRow NewRow() const;

    Result NewAppendWriter(AppendWriter& out);
    Result NewUpsertWriter(UpsertWriter& out);
    Result NewUpsertWriter(UpsertWriter& out, const std::vector<std::string>& column_names);
    Result NewUpsertWriter(UpsertWriter& out, const std::vector<size_t>& column_indices);
    Result NewLookuper(Lookuper& out);
    TableScan NewScan();

    TableInfo GetTableInfo() const;
    TablePath GetTablePath() const;
    bool HasPrimaryKey() const;

   private:
    friend class Connection;
    friend class TableScan;
    Table(ffi::Table* table) noexcept;

    void Destroy() noexcept;
    const std::shared_ptr<GenericRow::ColumnMap>& GetColumnMap() const;

    ffi::Table* table_{nullptr};
    mutable std::shared_ptr<GenericRow::ColumnMap> column_map_;
};

class TableScan {
   public:
    TableScan(const TableScan&) = delete;
    TableScan& operator=(const TableScan&) = delete;
    TableScan(TableScan&&) noexcept = default;
    TableScan& operator=(TableScan&&) noexcept = default;

    TableScan& ProjectByIndex(std::vector<size_t> column_indices);
    TableScan& ProjectByName(std::vector<std::string> column_names);

    Result CreateLogScanner(LogScanner& out);
    Result CreateRecordBatchScanner(LogScanner& out);

   private:
    friend class Table;
    explicit TableScan(ffi::Table* table) noexcept;

    std::vector<size_t> ResolveNameProjection() const;

    ffi::Table* table_{nullptr};
    std::vector<size_t> projection_;
    std::vector<std::string> name_projection_;
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
    friend class UpsertWriter;
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

class UpsertWriter {
   public:
    UpsertWriter() noexcept;
    ~UpsertWriter() noexcept;

    UpsertWriter(const UpsertWriter&) = delete;
    UpsertWriter& operator=(const UpsertWriter&) = delete;
    UpsertWriter(UpsertWriter&& other) noexcept;
    UpsertWriter& operator=(UpsertWriter&& other) noexcept;

    bool Available() const;

    Result Upsert(const GenericRow& row);
    Result Upsert(const GenericRow& row, WriteResult& out);
    Result Delete(const GenericRow& row);
    Result Delete(const GenericRow& row, WriteResult& out);
    Result Flush();

   private:
    friend class Table;
    UpsertWriter(ffi::UpsertWriter* writer) noexcept;
    void Destroy() noexcept;
    ffi::UpsertWriter* writer_{nullptr};
};

class Lookuper {
   public:
    Lookuper() noexcept;
    ~Lookuper() noexcept;

    Lookuper(const Lookuper&) = delete;
    Lookuper& operator=(const Lookuper&) = delete;
    Lookuper(Lookuper&& other) noexcept;
    Lookuper& operator=(Lookuper&& other) noexcept;

    bool Available() const;

    Result Lookup(const GenericRow& pk_row, bool& found, GenericRow& out);

   private:
    friend class Table;
    Lookuper(ffi::Lookuper* lookuper) noexcept;
    void Destroy() noexcept;
    ffi::Lookuper* lookuper_{nullptr};
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
    Result SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id, int64_t start_offset);
    Result SubscribePartitionBuckets(const std::vector<PartitionBucketSubscription>& subscriptions);
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
