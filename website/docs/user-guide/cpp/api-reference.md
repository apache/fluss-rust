---
sidebar_position: 2
---
# API Reference

Complete API reference for the Fluss C++ client.

## `Result`

| Field / Method | Type | Description |
|---|---|---|
| `error_code` | `int32_t` | 0 for success, non-zero for errors |
| `error_message` | `std::string` | Human-readable error description |
| `Ok()` | `bool` | Returns `true` if operation succeeded (`error_code == 0`) |

## `Configuration`

| Field | Type | Default | Description |
|---|---|---|---|
| `bootstrap_servers` | `std::string` | `"127.0.0.1:9123"` | Coordinator server address |
| `writer_request_max_size` | `int32_t` | `10485760` (10 MB) | Maximum request size in bytes |
| `writer_acks` | `std::string` | `"all"` | Acknowledgment setting (`"all"`, `"0"`, `"1"`, or `"-1"`) |
| `writer_retries` | `int32_t` | `INT32_MAX` | Number of retries on failure |
| `writer_batch_size` | `int32_t` | `2097152` (2 MB) | Batch size for writes in bytes |
| `scanner_remote_log_prefetch_num` | `size_t` | `4` | Number of remote log segments to prefetch |
| `remote_file_download_thread_num` | `size_t` | `3` | Number of threads for remote log downloads |

## `Connection`

| Method | Description |
|---|---|
| `static Create(const Configuration& config, Connection& out) -> Result` | Create a connection to a Fluss cluster |
| `GetAdmin(Admin& out) -> Result` | Get the admin interface |
| `GetTable(const TablePath& table_path, Table& out) -> Result` | Get a table for read/write operations |
| `Available() -> bool` | Check if the connection is valid and initialized |

## `Admin`

### Database Operations

| Method | Description |
|---|---|
| `CreateDatabase(const std::string& database_name, const DatabaseDescriptor& descriptor, bool ignore_if_exists) -> Result` | Create a database |
| `DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) -> Result` | Drop a database |
| `ListDatabases(std::vector<std::string>& out) -> Result` | List all databases |
| `DatabaseExists(const std::string& name, bool& out) -> Result` | Check if a database exists |
| `GetDatabaseInfo(const std::string& name, DatabaseInfo& out) -> Result` | Get database metadata |

### Table Operations

| Method | Description |
|---|---|
| `CreateTable(const TablePath& path, const TableDescriptor& descriptor, bool ignore_if_exists) -> Result` | Create a table |
| `DropTable(const TablePath& path, bool ignore_if_not_exists) -> Result` | Drop a table |
| `GetTableInfo(const TablePath& path, TableInfo& out) -> Result` | Get table metadata |
| `ListTables(const std::string& database_name, std::vector<std::string>& out) -> Result` | List tables in a database |
| `TableExists(const TablePath& path, bool& out) -> Result` | Check if a table exists |

### Partition Operations

| Method | Description |
|---|---|
| `CreatePartition(const TablePath& path, const std::unordered_map<std::string, std::string>& partition_spec, bool ignore_if_exists) -> Result` | Create a partition |
| `DropPartition(const TablePath& path, const std::unordered_map<std::string, std::string>& partition_spec, bool ignore_if_not_exists) -> Result` | Drop a partition |
| `ListPartitionInfos(const TablePath& path, std::vector<PartitionInfo>& out) -> Result` | List partition metadata |

### Offset Operations

| Method | Description |
|---|---|
| `ListOffsets(const TablePath& path, const std::vector<int32_t>& bucket_ids, const OffsetQuery& query, std::unordered_map<int32_t, int64_t>& out) -> Result` | Get offsets for buckets |
| `ListPartitionOffsets(const TablePath& path, const std::string& partition_name, const std::vector<int32_t>& bucket_ids, const OffsetQuery& query, std::unordered_map<int32_t, int64_t>& out) -> Result` | Get offsets for a partition's buckets |

### Lake Operations

| Method | Description |
|---|---|
| `GetLatestLakeSnapshot(const TablePath& path, LakeSnapshot& out) -> Result` | Get the latest lake snapshot |

## `Table`

| Method | Description |
|---|---|
| `NewRow() -> GenericRow` | Create a schema-aware row for this table |
| `NewAppend() -> TableAppend` | Create an append builder for log tables |
| `NewUpsert() -> TableUpsert` | Create an upsert builder for PK tables |
| `NewLookup() -> TableLookup` | Create a lookup builder for PK tables |
| `NewScan() -> TableScan` | Create a scan builder |
| `GetTableInfo() -> TableInfo` | Get table metadata |
| `GetTablePath() -> TablePath` | Get the table path |
| `HasPrimaryKey() -> bool` | Check if the table has a primary key |

## `TableAppend`

| Method | Description |
|---|---|
| `CreateWriter(AppendWriter& out) -> Result` | Create an append writer |

## `TableUpsert`

| Method | Description |
|---|---|
| `PartialUpdateByIndex(std::vector<size_t> column_indices) -> TableUpsert&` | Configure partial update by column indices |
| `PartialUpdateByName(std::vector<std::string> column_names) -> TableUpsert&` | Configure partial update by column names |
| `CreateWriter(UpsertWriter& out) -> Result` | Create an upsert writer |

## `TableLookup`

| Method | Description |
|---|---|
| `CreateLookuper(Lookuper& out) -> Result` | Create a lookuper for point lookups |

## `TableScan`

| Method | Description |
|---|---|
| `ProjectByIndex(std::vector<size_t> column_indices) -> TableScan&` | Project columns by index |
| `ProjectByName(std::vector<std::string> column_names) -> TableScan&` | Project columns by name |
| `CreateLogScanner(LogScanner& out) -> Result` | Create a record-based log scanner |
| `CreateRecordBatchLogScanner(LogScanner& out) -> Result` | Create an Arrow RecordBatch-based log scanner |

## `AppendWriter`

| Method | Description |
|---|---|
| `Append(const GenericRow& row) -> Result` | Append a row (fire-and-forget) |
| `Append(const GenericRow& row, WriteResult& out) -> Result` | Append a row with write acknowledgment |
| `Flush() -> Result` | Flush all pending writes |

## `UpsertWriter`

| Method | Description |
|---|---|
| `Upsert(const GenericRow& row) -> Result` | Upsert a row (fire-and-forget) |
| `Upsert(const GenericRow& row, WriteResult& out) -> Result` | Upsert a row with write acknowledgment |
| `Delete(const GenericRow& row) -> Result` | Delete a row by primary key (fire-and-forget) |
| `Delete(const GenericRow& row, WriteResult& out) -> Result` | Delete a row with write acknowledgment |
| `Flush() -> Result` | Flush all pending operations |

## `WriteResult`

| Method | Description |
|---|---|
| `Wait() -> Result` | Wait for server acknowledgment of the write |

## `Lookuper`

| Method | Description |
|---|---|
| `Lookup(const GenericRow& pk_row, bool& found, GenericRow& out) -> Result` | Lookup a row by primary key |

## `LogScanner`

| Method | Description |
|---|---|
| `Subscribe(int32_t bucket_id, int64_t offset) -> Result` | Subscribe to a single bucket at an offset |
| `Subscribe(const std::vector<BucketSubscription>& bucket_offsets) -> Result` | Subscribe to multiple buckets |
| `SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id, int64_t start_offset) -> Result` | Subscribe to a single partition bucket |
| `SubscribePartitionBuckets(const std::vector<PartitionBucketSubscription>& subscriptions) -> Result` | Subscribe to multiple partition buckets |
| `UnsubscribePartition(int64_t partition_id, int32_t bucket_id) -> Result` | Unsubscribe from a partition bucket |
| `Poll(int64_t timeout_ms, ScanRecords& out) -> Result` | Poll individual records |
| `PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out) -> Result` | Poll Arrow RecordBatches |

## `GenericRow`

### Index-Based Getters

| Method | Description |
|---|---|
| `GetBool(size_t idx) -> bool` | Get boolean value at index |
| `GetInt32(size_t idx) -> int32_t` | Get 32-bit integer at index |
| `GetInt64(size_t idx) -> int64_t` | Get 64-bit integer at index |
| `GetFloat32(size_t idx) -> float` | Get 32-bit float at index |
| `GetFloat64(size_t idx) -> double` | Get 64-bit float at index |
| `GetString(size_t idx) -> std::string` | Get string at index |
| `GetBytes(size_t idx) -> std::vector<uint8_t>` | Get binary data at index |
| `GetDate(size_t idx) -> Date` | Get date at index |
| `GetTime(size_t idx) -> Time` | Get time at index |
| `GetTimestamp(size_t idx) -> Timestamp` | Get timestamp at index |
| `DecimalToString(size_t idx) -> std::string` | Get decimal as string at index |

### Index-Based Setters

| Method | Description |
|---|---|
| `SetNull(size_t idx)` | Set field to null |
| `SetBool(size_t idx, bool value)` | Set boolean value |
| `SetInt32(size_t idx, int32_t value)` | Set 32-bit integer |
| `SetInt64(size_t idx, int64_t value)` | Set 64-bit integer |
| `SetFloat32(size_t idx, float value)` | Set 32-bit float |
| `SetFloat64(size_t idx, double value)` | Set 64-bit float |
| `SetString(size_t idx, const std::string& value)` | Set string value |
| `SetBytes(size_t idx, const std::vector<uint8_t>& value)` | Set binary data |
| `SetDate(size_t idx, const Date& value)` | Set date value |
| `SetTime(size_t idx, const Time& value)` | Set time value |
| `SetTimestampNtz(size_t idx, const Timestamp& value)` | Set timestamp without timezone |
| `SetTimestampLtz(size_t idx, const Timestamp& value)` | Set timestamp with timezone |
| `SetDecimal(size_t idx, const std::string& value)` | Set decimal from string |

### Name-Based Setters

When using `table.NewRow()`, the `Set()` method auto-routes to the correct type based on the schema:

| Method | Description |
|---|---|
| `Set(const std::string& name, bool value)` | Set boolean by column name |
| `Set(const std::string& name, int32_t value)` | Set integer by column name |
| `Set(const std::string& name, int64_t value)` | Set big integer by column name |
| `Set(const std::string& name, float value)` | Set float by column name |
| `Set(const std::string& name, double value)` | Set double by column name |
| `Set(const std::string& name, const std::string& value)` | Set string/decimal by column name |
| `Set(const std::string& name, const Date& value)` | Set date by column name |
| `Set(const std::string& name, const Time& value)` | Set time by column name |
| `Set(const std::string& name, const Timestamp& value)` | Set timestamp by column name |

### Row Inspection

| Method | Description |
|---|---|
| `FieldCount() -> size_t` | Get the number of fields |
| `GetType(size_t idx) -> DatumType` | Get the datum type at index |
| `IsNull(size_t idx) -> bool` | Check if field is null |
| `IsDecimal(size_t idx) -> bool` | Check if field is a decimal type |

## `ScanRecord`

| Field | Type | Description |
|---|---|---|
| `bucket_id` | `int32_t` | Bucket this record belongs to |
| `offset` | `int64_t` | Record offset in the log |
| `timestamp` | `int64_t` | Record timestamp |
| `row` | `GenericRow` | Row data |

## `ScanRecords`

| Method | Description |
|---|---|
| `Size() -> size_t` | Number of records |
| `Empty() -> bool` | Check if empty |
| `operator[](size_t idx) -> const ScanRecord&` | Access record by index |
| `begin() / end()` | Iterator support for range-based for loops |

## `ArrowRecordBatch`

| Method | Description |
|---|---|
| `GetArrowRecordBatch() -> std::shared_ptr<arrow::RecordBatch>` | Get the underlying Arrow RecordBatch |
| `NumRows() -> int64_t` | Number of rows in the batch |
| `GetTableId() -> int64_t` | Table ID |
| `GetPartitionId() -> int64_t` | Partition ID |
| `GetBucketId() -> int32_t` | Bucket ID |
| `GetBaseOffset() -> int64_t` | First record offset |
| `GetLastOffset() -> int64_t` | Last record offset |

## `ArrowRecordBatches`

| Method | Description |
|---|---|
| `Size() -> size_t` | Number of batches |
| `Empty() -> bool` | Check if empty |
| `operator[](size_t idx)` | Access batch by index |
| `begin() / end()` | Iterator support for range-based for loops |

## `Schema`

| Method | Description |
|---|---|
| `NewBuilder() -> Schema::Builder` | Create a new schema builder |

## `Schema::Builder`

| Method | Description |
|---|---|
| `AddColumn(const std::string& name, const DataType& type) -> Builder&` | Add a column |
| `SetPrimaryKeys(const std::vector<std::string>& keys) -> Builder&` | Set primary key columns |
| `Build() -> Schema` | Build the schema |

## `TableDescriptor`

| Method | Description |
|---|---|
| `NewBuilder() -> TableDescriptor::Builder` | Create a new table descriptor builder |

## `TableDescriptor::Builder`

| Method | Description |
|---|---|
| `SetSchema(const Schema& schema) -> Builder&` | Set the table schema |
| `SetPartitionKeys(const std::vector<std::string>& keys) -> Builder&` | Set partition key columns |
| `SetBucketCount(int32_t count) -> Builder&` | Set the number of buckets |
| `SetBucketKeys(const std::vector<std::string>& keys) -> Builder&` | Set bucket key columns |
| `SetProperty(const std::string& key, const std::string& value) -> Builder&` | Set a table property |
| `SetComment(const std::string& comment) -> Builder&` | Set a table comment |
| `Build() -> TableDescriptor` | Build the table descriptor |

## `DataType`

### Factory Methods

| Method | Description |
|---|---|
| `DataType::Boolean()` | Boolean type |
| `DataType::TinyInt()` | 8-bit signed integer |
| `DataType::SmallInt()` | 16-bit signed integer |
| `DataType::Int()` | 32-bit signed integer |
| `DataType::BigInt()` | 64-bit signed integer |
| `DataType::Float()` | 32-bit floating point |
| `DataType::Double()` | 64-bit floating point |
| `DataType::String()` | UTF-8 string |
| `DataType::Bytes()` | Binary data |
| `DataType::Date()` | Date (days since epoch) |
| `DataType::Time()` | Time (milliseconds since midnight) |
| `DataType::Timestamp(int precision)` | Timestamp without timezone |
| `DataType::TimestampLtz(int precision)` | Timestamp with timezone |
| `DataType::Decimal(int precision, int scale)` | Decimal with precision and scale |

### Accessors

| Method | Description |
|---|---|
| `id() -> TypeId` | Get the type ID |
| `precision() -> int` | Get precision (for Decimal/Timestamp types) |
| `scale() -> int` | Get scale (for Decimal type) |

## `TablePath`

| Method / Field | Description |
|---|---|
| `TablePath(const std::string& database, const std::string& table)` | Create a table path |
| `database_name -> std::string` | Database name |
| `table_name -> std::string` | Table name |
| `ToString() -> std::string` | String representation |

## `TableInfo`

| Field | Type | Description |
|---|---|---|
| `table_id` | `int64_t` | Table ID |
| `schema_id` | `int32_t` | Schema ID |
| `table_path` | `TablePath` | Table path |
| `created_time` | `int64_t` | Creation timestamp |
| `modified_time` | `int64_t` | Last modification timestamp |
| `primary_keys` | `std::vector<std::string>` | Primary key columns |
| `bucket_keys` | `std::vector<std::string>` | Bucket key columns |
| `partition_keys` | `std::vector<std::string>` | Partition key columns |
| `num_buckets` | `int32_t` | Number of buckets |
| `has_primary_key` | `bool` | Whether the table has a primary key |
| `is_partitioned` | `bool` | Whether the table is partitioned |
| `properties` | `std::unordered_map<std::string, std::string>` | Table properties |
| `comment` | `std::string` | Table comment |
| `schema` | `Schema` | Table schema |

## Temporal Types

### `Date`

| Method | Description |
|---|---|
| `Date::FromDays(int32_t days)` | Create from days since epoch |
| `Date::FromYMD(int year, int month, int day)` | Create from year, month, day |
| `Year() -> int` | Get year |
| `Month() -> int` | Get month |
| `Day() -> int` | Get day |

### `Time`

| Method | Description |
|---|---|
| `Time::FromMillis(int64_t millis)` | Create from milliseconds since midnight |
| `Time::FromHMS(int hour, int minute, int second)` | Create from hour, minute, second |
| `Hour() -> int` | Get hour |
| `Minute() -> int` | Get minute |
| `Second() -> int` | Get second |
| `Millis() -> int64_t` | Get milliseconds |

### `Timestamp`

| Method | Description |
|---|---|
| `Timestamp::FromMillis(int64_t millis)` | Create from milliseconds since epoch |
| `Timestamp::FromMillisNanos(int64_t millis, int32_t nanos)` | Create from milliseconds and nanoseconds |
| `Timestamp::FromTimePoint(std::chrono::system_clock::time_point tp)` | Create from a time point |

## `PartitionInfo`

| Field | Type | Description |
|---|---|---|
| `partition_id` | `int64_t` | Partition ID |
| `partition_name` | `std::string` | Partition name |

## `DatabaseDescriptor`

| Field | Type | Description |
|---|---|---|
| `comment` | `std::string` | Database comment |
| `properties` | `std::unordered_map<std::string, std::string>` | Custom properties |

## `DatabaseInfo`

| Field | Type | Description |
|---|---|---|
| `database_name` | `std::string` | Database name |
| `comment` | `std::string` | Database comment |
| `properties` | `std::unordered_map<std::string, std::string>` | Custom properties |
| `created_time` | `int64_t` | Creation timestamp |
| `modified_time` | `int64_t` | Last modification timestamp |

## `LakeSnapshot`

| Field | Type | Description |
|---|---|---|
| `snapshot_id` | `int64_t` | Snapshot ID |
| `bucket_offsets` | `std::vector<BucketOffset>` | All bucket offsets |

## `BucketOffset`

| Field | Type | Description |
|---|---|---|
| `table_id` | `int64_t` | Table ID |
| `partition_id` | `int64_t` | Partition ID |
| `bucket_id` | `int32_t` | Bucket ID |
| `offset` | `int64_t` | Offset value |

## `OffsetQuery`

| Method | Description |
|---|---|
| `OffsetQuery::Earliest()` | Query for the earliest available offset |
| `OffsetQuery::Latest()` | Query for the latest offset |
| `OffsetQuery::FromTimestamp(int64_t timestamp_ms)` | Query offset at a specific timestamp |

## Constants

| Constant | Value | Description |
|---|---|---|
| `fluss::EARLIEST_OFFSET` | `-2` | Start reading from the earliest available offset |
| `fluss::LATEST_OFFSET` | `-1` | Start reading from the latest offset (only new records) |

## Enums

### `TypeId`

| Value | Description |
|---|---|
| `Boolean` | Boolean type |
| `TinyInt` | 8-bit signed integer |
| `SmallInt` | 16-bit signed integer |
| `Int` | 32-bit signed integer |
| `BigInt` | 64-bit signed integer |
| `Float` | 32-bit floating point |
| `Double` | 64-bit floating point |
| `String` | UTF-8 string |
| `Bytes` | Binary data |
| `Date` | Date |
| `Time` | Time |
| `Timestamp` | Timestamp without timezone |
| `TimestampLtz` | Timestamp with timezone |
| `Decimal` | Decimal |

### `DatumType`

| Value | C++ Type | Description |
|---|---|---|
| `Null` | -- | Null value |
| `Bool` | `bool` | Boolean |
| `Int32` | `int32_t` | 32-bit integer |
| `Int64` | `int64_t` | 64-bit integer |
| `Float32` | `float` | 32-bit float |
| `Float64` | `double` | 64-bit float |
| `String` | `std::string` | String |
| `Bytes` | `std::vector<uint8_t>` | Binary data |
| `DecimalI64` | `int64_t` | Decimal (64-bit internal) |
| `DecimalI128` | `__int128` | Decimal (128-bit internal) |
| `DecimalString` | `std::string` | Decimal (string representation) |
| `Date` | `Date` | Date |
| `Time` | `Time` | Time |
| `TimestampNtz` | `Timestamp` | Timestamp without timezone |
| `TimestampLtz` | `Timestamp` | Timestamp with timezone |

### `OffsetSpec`

| Value | Description |
|---|---|
| `Earliest` | Earliest available offset |
| `Latest` | Latest offset |
| `Timestamp` | Offset at a specific timestamp |
