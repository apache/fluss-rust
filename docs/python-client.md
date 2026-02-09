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

# Python Client Guide

This guide covers how to use the Fluss Python client for reading and writing data to log tables and primary key tables.

The Python client is async-first, built on top of the Rust core via [PyO3](https://pyo3.rs/), and uses [PyArrow](https://arrow.apache.org/docs/python/) for schema definitions and data interchange.

## Key Concepts

- **Log table** — an append-only table (no primary key). Records are immutable once written. Use for event streams, logs, and audit trails.
- **Primary key (PK) table** — a table with a primary key. Supports upsert, delete, and point lookups.
- **Bucket** — the unit of parallelism within a table (similar to Kafka partitions). Each table has one or more buckets. Readers subscribe to individual buckets.
- **Partition** — a way to organize data by column values (e.g. by date or region). Each partition contains its own set of buckets. Partitions must be created explicitly before writing.
- **Offset** — the position of a record within a bucket. Used to track reading progress. Start from `EARLIEST_OFFSET` to read all data, or `LATEST_OFFSET` to only read new records.

## Prerequisites

You need a running Fluss cluster to use the Python client. See the [Quick-Start guide](../README.md#quick-start) for how to start a local cluster.

## Installation

```bash
pip install pyfluss
```

To build from source instead, see the [Python bindings README](../bindings/python/README.md).

## Quick Start

A minimal end-to-end example: connect, create a table, write data, and read it back. Assumes a Fluss cluster is running on `localhost:9123`.

```python
import asyncio
import pyarrow as pa
import fluss

async def main():
    # Connect
    config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
    conn = await fluss.FlussConnection.connect(config)
    admin = await conn.get_admin()

    # Create a log table
    schema = fluss.Schema(pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
    ]))
    table_path = fluss.TablePath("fluss", "quick_start")
    await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=True)

    # Write
    table = await conn.get_table(table_path)
    writer = await table.new_append_writer()
    writer.append({"id": 1, "name": "Alice", "score": 95.5})
    writer.append({"id": 2, "name": "Bob", "score": 87.0})
    await writer.flush()

    # Read
    num_buckets = (await admin.get_table(table_path)).num_buckets
    scanner = await table.new_scan().create_batch_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    print(scanner.to_pandas())

    # Cleanup
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    conn.close()

asyncio.run(main())
```

## Connection Setup

```python
config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
conn = await fluss.FlussConnection.connect(config)
```

The connection also supports context managers:

```python
with await fluss.FlussConnection.connect(config) as conn:
    ...
```

### Configuration Options

| Key | Description | Default |
|-----|-------------|---------|
| `bootstrap.servers` | Coordinator server address | `127.0.0.1:9123` |
| `request.max.size` | Maximum request size in bytes | `10485760` (10 MB) |
| `writer.acks` | Acknowledgment setting (`all` waits for all replicas) | `all` |
| `writer.retries` | Number of retries on failure | `2147483647` |
| `writer.batch.size` | Batch size for writes in bytes | `2097152` (2 MB) |

## Admin Operations

```python
admin = await conn.get_admin()
```

### Databases

```python
await admin.create_database("my_database", ignore_if_exists=True)
databases = await admin.list_databases()
exists = await admin.database_exists("my_database")
await admin.drop_database("my_database", ignore_if_not_exists=True, cascade=True)
```

### Tables

Schemas are defined using PyArrow and wrapped in `fluss.Schema`:

```python
import pyarrow as pa

schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("name", pa.string()),
    pa.field("amount", pa.int64()),
]))

table_path = fluss.TablePath("my_database", "my_table")
await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=True)

table_info = await admin.get_table(table_path)
tables = await admin.list_tables("my_database")
await admin.drop_table(table_path, ignore_if_not_exists=True)
```

`TableDescriptor` accepts these optional parameters:

| Parameter | Description |
|---|---|
| `partition_keys` | Column names to partition by (e.g. `["region"]`) |
| `bucket_count` | Number of buckets (parallelism units) for the table |
| `bucket_keys` | Columns used to determine bucket assignment |
| `comment` | Table comment / description |
| `log_format` | Log storage format: `"ARROW"` or `"INDEXED"` |
| `kv_format` | KV storage format for primary key tables: `"INDEXED"` or `"COMPACTED"` |
| `properties` | Table configuration properties as a dict (e.g. `{"table.replication.factor": "1"}`) |
| `custom_properties` | User-defined properties as a dict |

### Offsets

```python
# Latest offsets for buckets
offsets = await admin.list_offsets(table_path, bucket_ids=[0, 1], offset_type="latest")

# By timestamp
offsets = await admin.list_offsets(table_path, bucket_ids=[0], offset_type="timestamp", timestamp=1704067200000)

# Per-partition offsets
offsets = await admin.list_partition_offsets(table_path, partition_name="US", bucket_ids=[0], offset_type="latest")
```

## Log Tables

Log tables are append-only tables without primary keys, suitable for event streaming.

### Writing

Rows can be appended as dicts, lists, or tuples. For bulk writes, use `write_arrow()`, `write_arrow_batch()`, or `write_pandas()`.

Write methods like `append()` and `write_arrow_batch()` return a `WriteResultHandle`. You can ignore it for fire-and-forget semantics (flush at the end), or `await handle.wait()` to block until the server acknowledges that specific write.

```python
table = await conn.get_table(table_path)
writer = await table.new_append_writer()

# Fire-and-forget: queue writes, flush at the end
writer.append({"id": 1, "name": "Alice", "score": 95.5})
writer.append([2, "Bob", 87.0])
await writer.flush()

# Per-record acknowledgment
handle = writer.append({"id": 3, "name": "Charlie", "score": 91.0})
await handle.wait()

# Bulk writes
writer.write_arrow(pa_table)          # PyArrow Table
writer.write_arrow_batch(record_batch) # PyArrow RecordBatch
writer.write_pandas(df)                # Pandas DataFrame
await writer.flush()
```

### Reading

There are two scanner types:
- **Batch scanner** (`create_batch_scanner()`) — returns Arrow Tables or DataFrames, best for analytics
- **Record scanner** (`create_log_scanner()`) — returns individual records with metadata (offset, timestamp, change type), best for streaming

And two reading modes:
- **`to_arrow()` / `to_pandas()`** — reads all data from subscribed buckets up to the current latest offset, then returns. Best for one-shot batch reads.
- **`poll_arrow()` / `poll()` / `poll_batches()`** — returns whatever data is available within the timeout, then returns. Call in a loop for continuous streaming.

#### Batch Read (One-Shot)

```python
num_buckets = (await admin.get_table(table_path)).num_buckets

scanner = await table.new_scan().create_batch_scanner()
scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

# Reads everything up to current latest offset, then returns
arrow_table = scanner.to_arrow()
df = scanner.to_pandas()
```

#### Continuous Polling

Use `poll_arrow()` or `poll()` in a loop for streaming consumption:

```python
# Batch scanner: poll as Arrow Tables
scanner = await table.new_scan().create_batch_scanner()
scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

while True:
    result = scanner.poll_arrow(timeout_ms=5000)
    if result.num_rows > 0:
        print(result.to_pandas())

# Record scanner: poll individual records with metadata
scanner = await table.new_scan().create_log_scanner()
scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

while True:
    for record in scanner.poll(timeout_ms=5000):
        print(f"offset={record.offset}, change={record.change_type.short_string()}, row={record.row}")
```

#### Subscribe from Latest Offset

To only consume new records (skip existing data), use `LATEST_OFFSET`:

```python
scanner = await table.new_scan().create_batch_scanner()
scanner.subscribe(bucket_id=0, start_offset=fluss.LATEST_OFFSET)
```

### Column Projection

```python
scanner = await table.new_scan().project([0, 2]).create_batch_scanner()
# or by name
scanner = await table.new_scan().project_by_name(["id", "score"]).create_batch_scanner()
```

## Primary Key Tables

Primary key tables support upsert, delete, and point lookup operations.

### Creating

Pass `primary_keys` to `fluss.Schema`:

```python
schema = fluss.Schema(
    pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
    ]),
    primary_keys=["id"],
)
table_path = fluss.TablePath("fluss", "users")
await admin.create_table(table_path, fluss.TableDescriptor(schema, bucket_count=3), ignore_if_exists=True)
```

### Upsert, Delete, Lookup

```python
table = await conn.get_table(table_path)

# Upsert (fire-and-forget, flush at the end)
writer = table.new_upsert()
writer.upsert({"id": 1, "name": "Alice", "age": 25})
writer.upsert({"id": 2, "name": "Bob", "age": 30})
await writer.flush()

# Per-record acknowledgment (for read-after-write)
handle = writer.upsert({"id": 3, "name": "Charlie", "age": 35})
await handle.wait()

# Delete by primary key
handle = writer.delete({"id": 2})
await handle.wait()

# Lookup
lookuper = table.new_lookup()
result = await lookuper.lookup({"id": 1})
if result:
    print(f"Found: name={result['name']}, age={result['age']}")
```

### Partial Updates

Update specific columns while preserving others:

```python
partial_writer = table.new_upsert(columns=["id", "age"])
partial_writer.upsert({"id": 1, "age": 27})  # only updates age
await partial_writer.flush()
```

## Partitioned Tables

Partitioned tables distribute data across partitions based on column values. Partitions must be created before writing.

### Creating and Managing Partitions

```python
schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("region", pa.string()),
    pa.field("value", pa.int64()),
]))

table_path = fluss.TablePath("fluss", "partitioned_events")
await admin.create_table(
    table_path,
    fluss.TableDescriptor(schema, partition_keys=["region"], bucket_count=1),
    ignore_if_exists=True,
)

# Create partitions
await admin.create_partition(table_path, {"region": "US"}, ignore_if_exists=True)
await admin.create_partition(table_path, {"region": "EU"}, ignore_if_exists=True)

# List partitions
partition_infos = await admin.list_partition_infos(table_path)
```

### Writing

Same as non-partitioned tables — include partition column values in each row:

```python
table = await conn.get_table(table_path)
writer = await table.new_append_writer()
writer.append({"id": 1, "region": "US", "value": 100})
writer.append({"id": 2, "region": "EU", "value": 200})
await writer.flush()
```

### Reading

Use `subscribe_partition()` or `subscribe_partition_buckets()` instead of `subscribe()`:

```python
scanner = await table.new_scan().create_batch_scanner()

# Subscribe to individual partitions
for p in partition_infos:
    scanner.subscribe_partition(partition_id=p.partition_id, bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

# Or batch-subscribe
scanner.subscribe_partition_buckets({
    (p.partition_id, 0): fluss.EARLIEST_OFFSET for p in partition_infos
})

print(scanner.to_pandas())
```

### Partitioned Primary Key Tables

Partition columns must be part of the primary key. Partitions must be created before upserting.

```python
schema = fluss.Schema(
    pa.schema([
        pa.field("user_id", pa.int32()),
        pa.field("region", pa.string()),
        pa.field("score", pa.int64()),
    ]),
    primary_keys=["user_id", "region"],
)

table_path = fluss.TablePath("fluss", "partitioned_users")
await admin.create_table(
    table_path,
    fluss.TableDescriptor(schema, partition_keys=["region"]),
    ignore_if_exists=True,
)

await admin.create_partition(table_path, {"region": "US"}, ignore_if_exists=True)

table = await conn.get_table(table_path)
writer = table.new_upsert()
writer.upsert({"user_id": 1, "region": "US", "score": 1234})
await writer.flush()

# Lookup includes partition columns
lookuper = table.new_lookup()
result = await lookuper.lookup({"user_id": 1, "region": "US"})
```

## Error Handling

The client raises `fluss.FlussError` for Fluss-specific errors (connection failures, table not found, invalid operations, etc.):

```python
try:
    await admin.create_table(table_path, table_descriptor)
except fluss.FlussError as e:
    print(f"Fluss error: {e.message}")
```

Common error scenarios:
- **Connection refused** — Fluss cluster is not running or wrong address in `bootstrap.servers`
- **Table not found** — table doesn't exist or wrong database/table name
- **Partition not found** — writing to a partitioned table before creating partitions
- **Schema mismatch** — row data doesn't match the table schema

## Data Types

The Python client uses PyArrow types for schema definitions:

| PyArrow Type | Fluss Type | Python Type |
|---|---|---|
| `pa.boolean()` | Boolean | `bool` |
| `pa.int8()` / `int16()` / `int32()` / `int64()` | TinyInt / SmallInt / Int / BigInt | `int` |
| `pa.float32()` / `float64()` | Float / Double | `float` |
| `pa.string()` | String | `str` |
| `pa.binary()` | Bytes | `bytes` |
| `pa.date32()` | Date | `datetime.date` |
| `pa.time32("ms")` | Time | `datetime.time` |
| `pa.timestamp("us")` | Timestamp (NTZ) | `datetime.datetime` |
| `pa.timestamp("us", tz="UTC")` | TimestampLTZ | `datetime.datetime` |
| `pa.decimal128(precision, scale)` | Decimal | `decimal.Decimal` |

All Python native types (`date`, `time`, `datetime`, `Decimal`) work when appending rows via dicts.

For a complete list of classes, methods, and properties, see the [Python API Reference](python-api-reference.md).
