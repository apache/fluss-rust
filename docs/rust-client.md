<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Fluss Rust Client Guide

This guide covers how to use the Fluss Rust client for reading and writing data to log tables and primary key tables.

## Adding to Your Project

The Fluss Rust client is published to [crates.io](https://crates.io/crates/fluss-rs) as `fluss-rs`. The crate's library name is `fluss`, so you import it with `use fluss::...`.

```toml
[dependencies]
fluss-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Feature Flags

The Fluss crate supports optional storage backends:

```toml
[dependencies]
# Default: memory and filesystem storage
fluss-rs = "0.1"

# With S3 storage support
fluss-rs = { version = "0.1", features = ["storage-s3"] }

# With OSS storage support
fluss-rs = { version = "0.1", features = ["storage-oss"] }

# All storage backends
fluss-rs = { version = "0.1", features = ["storage-all"] }
```

Available features:
- `storage-memory` (default) - In-memory storage
- `storage-fs` (default) - Local filesystem storage
- `storage-s3` - Amazon S3 storage
- `storage-oss` - Alibaba OSS storage
- `storage-all` - All storage backends

### Alternative: Git or Path Dependency

For development against unreleased changes, you can depend on the Git repository or a local checkout:

```toml
[dependencies]
# From Git
fluss = { git = "https://github.com/apache/fluss-rust.git", package = "fluss-rs" }

# From local path
fluss = { path = "/path/to/fluss-rust/crates/fluss", package = "fluss-rs" }
```

> **Note:** When using `git` or `path` dependencies, the `package = "fluss-rs"` field is required so that Cargo resolves the correct package while still allowing `use fluss::...` imports.

## Building from Source

### Prerequisites

- Rust 1.85+
- Protobuf compiler (`protoc`) - only required when [building from source](#building-from-source)


### 1. Clone the Repository

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust
```

### 2. Install Dependencies

The Protobuf compiler (`protoc`) is required to build from source.

#### macOS

```bash
brew install protobuf
```

#### Ubuntu/Debian

```bash
sudo apt-get install protobuf-compiler
```

### 3. Build the Library

```bash
cargo build --workspace --all-targets
```

## Connection Setup

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;

    // Use the connection...

    Ok(())
}
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `bootstrap_servers` | Coordinator server address | `127.0.0.1:9123` |
| `writer_request_max_size` | Maximum request size in bytes | 10 MB |
| `writer_acks` | Acknowledgment setting (`all` waits for all replicas) | `all` |
| `writer_retries` | Number of retries on failure | `i32::MAX` |
| `writer_batch_size` | Batch size for writes | 2 MB |

## Admin Operations

### Get Admin Interface

```rust
let admin = conn.get_admin().await?;
```

### Database Operations

```rust
// Create database
admin.create_database("my_database", None, true).await?;

// List all databases
let databases = admin.list_databases().await?;
println!("Databases: {:?}", databases);

// Check if database exists
let exists = admin.database_exists("my_database").await?;

// Get database information
let db_info = admin.get_database_info("my_database").await?;

// Drop database
admin.drop_database("my_database", true, false).await?;
```

### Table Operations

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

// Define table schema
let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("amount", DataTypes::bigint())
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("my_database", "my_table");

// Create table
admin.create_table(&table_path, &table_descriptor, true).await?;

// Get table information
let table_info = admin.get_table_info(&table_path).await?;
println!("Table: {}", table_info);

// List tables in database
let tables = admin.list_tables("my_database").await?;

// Check if table exists
let exists = admin.table_exists(&table_path).await?;

// Drop table
admin.drop_table(&table_path, true).await?;
```

### Partition Operations

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

// List all partitions
let partitions = admin.list_partition_infos(&table_path).await?;

// List partitions matching a spec
let mut filter = HashMap::new();
filter.insert("year", "2024");
let spec = PartitionSpec::new(filter);
let partitions = admin.list_partition_infos_with_spec(&table_path, Some(&spec)).await?;

// Create partition
admin.create_partition(&table_path, &spec, true).await?;

// Drop partition
admin.drop_partition(&table_path, &spec, true).await?;
```

### Offset Operations

```rust
use fluss::rpc::message::OffsetSpec;

let bucket_ids = vec![0, 1, 2];

// Get earliest offsets
let earliest = admin.list_offsets(&table_path, &bucket_ids, OffsetSpec::Earliest).await?;

// Get latest offsets
let latest = admin.list_offsets(&table_path, &bucket_ids, OffsetSpec::Latest).await?;

// Get offsets for a specific timestamp
let timestamp_ms = 1704067200000; // 2024-01-01 00:00:00 UTC
let offsets = admin.list_offsets(&table_path, &bucket_ids, OffsetSpec::Timestamp(timestamp_ms)).await?;

// Get offsets for a specific partition
let partition_offsets = admin.list_partition_offsets(
    &table_path,
    "partition_name",
    &bucket_ids,
    OffsetSpec::Latest,
).await?;
```

### Lake Snapshot

```rust
// Get latest lake snapshot for lakehouse integration
let snapshot = admin.get_latest_lake_snapshot(&table_path).await?;
println!("Snapshot ID: {}", snapshot.snapshot_id);
```

## Log Table Operations

Log tables are append-only tables without primary keys, suitable for event streaming.

### Creating a Log Table

```rust
let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("event_id", DataTypes::int())
            .column("event_type", DataTypes::string())
            .column("timestamp", DataTypes::bigint())
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("fluss", "events");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Writing to Log Tables

```rust
use fluss::row::{GenericRow, InternalRow};

let table = conn.get_table(&table_path).await?;
let append_writer = table.new_append()?.create_writer()?;

// Write a single row
let mut row = GenericRow::new(3);
row.set_field(0, 1);                    // event_id (int)
row.set_field(1, "user_login");         // event_type (string)
row.set_field(2, 1704067200000i64);     // timestamp (bigint)

append_writer.append(&row)?;

// Write multiple rows
let mut row2 = GenericRow::new(3);
row2.set_field(0, 2);
row2.set_field(1, "page_view");
row2.set_field(2, 1704067201000i64);

append_writer.append(&row2)?;

// Flush to ensure data is persisted
append_writer.flush().await?;
```

Write operations (`append`, `upsert`, `delete`) use a **fire-and-forget** pattern for efficient batching. Each call queues the write and returns a `WriteResultFuture` immediately. Call `flush()` to ensure all queued writes are sent to the server.

If you need per-record acknowledgment, you can await the returned future:

```rust
// Per-record acknowledgment (blocks until server confirms)
append_writer.append(&row)?.await?;
```

### Reading from Log Tables

```rust
use std::time::Duration;

let table = conn.get_table(&table_path).await?;
let log_scanner = table.new_scan().create_log_scanner()?;

// Subscribe to bucket 0 starting from offset 0
log_scanner.subscribe(0, 0).await?;

// Poll for records
let records = log_scanner.poll(Duration::from_secs(10)).await?;

for record in records {
    let row = record.row();
    println!(
        "event_id={}, event_type={}, timestamp={} @ offset={}",
        row.get_int(0),
        row.get_string(1),
        row.get_long(2),
        record.offset()
    );
}
```

### Column Projection

```rust
// Project specific columns by index
let scanner = table.new_scan().project(&[0, 2])?.create_log_scanner()?;

// Or project by column names
let scanner = table.new_scan().project_by_name(&["event_id", "timestamp"])?.create_log_scanner()?;
```

### Subscribe from Specific Offsets

```rust
use fluss::client::{EARLIEST_OFFSET, LATEST_OFFSET};

// Subscribe from earliest available offset
log_scanner.subscribe(0, EARLIEST_OFFSET).await?;

// Subscribe from latest offset (only new records)
log_scanner.subscribe(0, LATEST_OFFSET).await?;

// Subscribe from a specific offset
log_scanner.subscribe(0, 42).await?;

// Subscribe to all buckets
let num_buckets = table.table_info().get_num_buckets();
for bucket_id in 0..num_buckets {
    log_scanner.subscribe(bucket_id, 0).await?;
}
```

### Subscribe to Multiple Buckets

```rust
use std::collections::HashMap;

// Subscribe to multiple buckets at once with specific offsets
let mut bucket_offsets = HashMap::new();
bucket_offsets.insert(0, 0i64);    // bucket 0 from offset 0
bucket_offsets.insert(1, 100i64);  // bucket 1 from offset 100
log_scanner.subscribe_buckets(&bucket_offsets).await?;
```

### Unsubscribe from a Partition

```rust
// Unsubscribe from a specific partition bucket
log_scanner.unsubscribe_partition(partition_id, bucket_id).await?;
```

## Partitioned Log Tables

Partitioned tables distribute data across partitions based on partition column values, enabling efficient data organization and querying.

### Creating a Partitioned Log Table

```rust
use fluss::metadata::{DataTypes, LogFormat, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("event_id", DataTypes::int())
            .column("event_type", DataTypes::string())
            .column("dt", DataTypes::string())       // partition column
            .column("region", DataTypes::string())   // partition column
            .build()?,
    )
    .partitioned_by(vec!["dt", "region"])  // Define partition columns
    .log_format(LogFormat::ARROW)
    .build()?;

let table_path = TablePath::new("fluss", "partitioned_events");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Writing to Partitioned Log Tables

Writing works the same as non-partitioned tables. Include partition column values in each row:

```rust
let table = conn.get_table(&table_path).await?;
let append_writer = table.new_append()?.create_writer()?;

// Partition column values determine which partition the record goes to
let mut row = GenericRow::new(4);
row.set_field(0, 1);                  // event_id
row.set_field(1, "user_login");       // event_type
row.set_field(2, "2024-01-15");       // dt (partition column)
row.set_field(3, "US");               // region (partition column)

append_writer.append(&row)?;
append_writer.flush().await?;
```

### Reading from Partitioned Log Tables

For partitioned tables, use `subscribe_partition()` instead of `subscribe()`:

```rust
use std::time::Duration;

let table = conn.get_table(&table_path).await?;
let admin = conn.get_admin().await?;

// Get partition information
let partitions = admin.list_partition_infos(&table_path).await?;

let log_scanner = table.new_scan().create_log_scanner()?;

// Subscribe to each partition's buckets
for partition_info in &partitions {
    let partition_id = partition_info.get_partition_id();
    let num_buckets = table.table_info().get_num_buckets();

    for bucket_id in 0..num_buckets {
        log_scanner.subscribe_partition(partition_id, bucket_id, 0).await?;
    }
}

// Poll for records
let records = log_scanner.poll(Duration::from_secs(10)).await?;
for record in records {
    println!("Record from partition: {:?}", record.row());
}
```

You can also subscribe to multiple partition-buckets at once:

```rust
use std::collections::HashMap;

let mut partition_bucket_offsets = HashMap::new();
partition_bucket_offsets.insert((partition_id, 0), 0i64);  // partition, bucket 0, offset 0
partition_bucket_offsets.insert((partition_id, 1), 0i64);  // partition, bucket 1, offset 0
log_scanner.subscribe_partition_buckets(&partition_bucket_offsets).await?;
```

### Managing Partitions

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

// Create a partition
let mut partition_values = HashMap::new();
partition_values.insert("dt", "2024-01-15");
partition_values.insert("region", "EMEA");
let spec = PartitionSpec::new(partition_values);
admin.create_partition(&table_path, &spec, true).await?;

// List all partitions
let partitions = admin.list_partition_infos(&table_path).await?;
for partition in &partitions {
    println!(
        "Partition: id={}, name={}",
        partition.get_partition_id(),
        partition.get_partition_name()  // Format: "value1$value2"
    );
}

// List partitions with filter (partial spec)
let mut partial_values = HashMap::new();
partial_values.insert("dt", "2024-01-15");
let partial_spec = PartitionSpec::new(partial_values);
let filtered = admin.list_partition_infos_with_spec(&table_path, Some(&partial_spec)).await?;

// Drop a partition
admin.drop_partition(&table_path, &spec, true).await?;
```

## Primary Key Table Operations

Primary key tables (KV tables) support upsert, delete, and lookup operations.

### Creating a Primary Key Table

```rust
let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("age", DataTypes::bigint())
            .primary_key(vec!["id"])  // Define primary key
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("fluss", "users");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Upserting Records

```rust
let table = conn.get_table(&table_path).await?;
let table_upsert = table.new_upsert()?;
let upsert_writer = table_upsert.create_writer()?;

// Insert or update records
for (id, name, age) in [(1, "Alice", 25i64), (2, "Bob", 30), (3, "Charlie", 35)] {
    let mut row = GenericRow::new(3);
    row.set_field(0, id);
    row.set_field(1, name);
    row.set_field(2, age);
    upsert_writer.upsert(&row)?;
}
upsert_writer.flush().await?;
```

### Updating Records

```rust
// Update existing record (same primary key)
let mut row = GenericRow::new(3);
row.set_field(0, 1);        // id (primary key)
row.set_field(1, "Alice");  // name
row.set_field(2, 26i64);    // Updated age

upsert_writer.upsert(&row)?;
upsert_writer.flush().await?;
```

### Deleting Records

```rust
// Delete by primary key (only primary key field needs to be set)
let mut row = GenericRow::new(3);
row.set_field(0, 2);  // id of record to delete

upsert_writer.delete(&row)?;
upsert_writer.flush().await?;
```

### Partial Updates

Update only specific columns while preserving others:

```rust
// By column indices
let partial_upsert = table_upsert.partial_update(Some(vec![0, 2]))?;
let partial_writer = partial_upsert.create_writer()?;

let mut row = GenericRow::new(3);
row.set_field(0, 1);       // id (primary key, required)
row.set_field(2, 27i64);   // age (will be updated)
// name will remain unchanged

partial_writer.upsert(&row)?;
partial_writer.flush().await?;

// By column names
let partial_upsert = table_upsert.partial_update_with_column_names(&["id", "age"])?;
let partial_writer = partial_upsert.create_writer()?;
```

### Looking Up Records

```rust
let mut lookuper = table.new_lookup()?.create_lookuper()?;

// Create a key row (only primary key fields)
let mut key = GenericRow::new(1);
key.set_field(0, 1);  // id to lookup

let result = lookuper.lookup(&key).await?;

if let Some(row) = result.get_single_row()? {
    println!(
        "Found: id={}, name={}, age={}",
        row.get_int(0),
        row.get_string(1),
        row.get_long(2)
    );
} else {
    println!("Record not found");
}
```

## Partitioned Primary Key Tables

Partitioned KV tables combine partitioning with primary key operations. Partition columns must be part of the primary key.

### Creating a Partitioned Primary Key Table

```rust
use fluss::metadata::{DataTypes, KvFormat, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("user_id", DataTypes::int())
            .column("region", DataTypes::string())   // partition column
            .column("zone", DataTypes::bigint())     // partition column
            .column("score", DataTypes::bigint())
            // Primary key must include partition columns
            .primary_key(vec!["user_id", "region", "zone"])
            .build()?,
    )
    .partitioned_by(vec!["region", "zone"])  // Define partition columns
    .kv_format(KvFormat::COMPACTED)
    .build()?;

let table_path = TablePath::new("fluss", "partitioned_users");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Writing to Partitioned Primary Key Tables

Upsert and delete operations work the same as non-partitioned KV tables. **Partitions must be created before upserting data.**

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

let table = conn.get_table(&table_path).await?;

// Ensure partitions exist before upserting
for (region, zone) in [("APAC", "1"), ("EMEA", "2"), ("US", "3")] {
    let mut partition_values = HashMap::new();
    partition_values.insert("region", region);
    partition_values.insert("zone", zone);
    let spec = PartitionSpec::new(partition_values);
    admin.create_partition(&table_path, &spec, true).await?;
}

let table_upsert = table.new_upsert()?;
let upsert_writer = table_upsert.create_writer()?;

// Upsert records - partition is determined by partition column values
for (user_id, region, zone, score) in [
    (1001, "APAC", 1i64, 1234i64),
    (1002, "EMEA", 2, 2234),
    (1003, "US", 3, 3234),
] {
    let mut row = GenericRow::new(4);
    row.set_field(0, user_id);
    row.set_field(1, region);
    row.set_field(2, zone);
    row.set_field(3, score);
    upsert_writer.upsert(&row)?;
}
upsert_writer.flush().await?;

// Update a record
let mut row = GenericRow::new(4);
row.set_field(0, 1001);
row.set_field(1, "APAC");
row.set_field(2, 1i64);
row.set_field(3, 5000i64);  // Updated score
upsert_writer.upsert(&row)?;
upsert_writer.flush().await?;

// Delete a record (primary key includes partition columns)
let mut row = GenericRow::new(4);
row.set_field(0, 1002);
row.set_field(1, "EMEA");
row.set_field(2, 2i64);
upsert_writer.delete(&row)?;
upsert_writer.flush().await?;
```

### Looking Up Records in Partitioned Tables

Lookup requires all primary key columns including partition columns:

```rust
let mut lookuper = table.new_lookup()?.create_lookuper()?;

// Key must include all primary key columns (including partition columns)
let mut key = GenericRow::new(3);
key.set_field(0, 1001);    // user_id
key.set_field(1, "APAC");  // region (partition column)
key.set_field(2, 1i64);    // zone (partition column)

let result = lookuper.lookup(&key).await?;
if let Some(row) = result.get_single_row()? {
    println!("Found: score={}", row.get_long(3));
}
```

> **Note:** Scanning partitioned primary key tables is not supported. Use lookup operations instead.

## Data Types

| Fluss Type      | Rust Type      | Method                                                              |
|-----------------|----------------|---------------------------------------------------------------------|
| `BOOLEAN`       | `bool`         | `get_boolean()`, `set_field(idx, bool)`                             |
| `TINYINT`       | `i8`           | `get_byte()`, `set_field(idx, i8)`                                  |
| `SMALLINT`      | `i16`          | `get_short()`, `set_field(idx, i16)`                                |
| `INT`           | `i32`          | `get_int()`, `set_field(idx, i32)`                                  |
| `BIGINT`        | `i64`          | `get_long()`, `set_field(idx, i64)`                                 |
| `FLOAT`         | `f32`          | `get_float()`, `set_field(idx, f32)`                                |
| `DOUBLE`        | `f64`          | `get_double()`, `set_field(idx, f64)`                               |
| `CHAR`          | `&str`         | `get_char(idx, length)`, `set_field(idx, &str)`                     |
| `STRING`        | `&str`         | `get_string()`, `set_field(idx, &str)`                              |
| `DECIMAL`       | `Decimal`      | `get_decimal(idx, precision, scale)`, `set_field(idx, Decimal)`     |
| `DATE`          | `Date`         | `get_date()`, `set_field(idx, Date)`                                |
| `TIME`          | `Time`         | `get_time()`, `set_field(idx, Time)`                                |
| `TIMESTAMP`     | `TimestampNtz` | `get_timestamp_ntz(idx, precision)`, `set_field(idx, TimestampNtz)` |
| `TIMESTAMP_LTZ` | `TimestampLtz` | `get_timestamp_ltz(idx, precision)`, `set_field(idx, TimestampLtz)` |
| `BYTES`         | `&[u8]`        | `get_bytes()`, `set_field(idx, &[u8])`                              |
| `BINARY(n)`     | `&[u8]`        | `get_binary(idx, length)`, `set_field(idx, &[u8])`                  |

