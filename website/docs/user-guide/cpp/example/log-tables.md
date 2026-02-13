---
sidebar_position: 4
---
# Log Tables

Log tables are append-only tables without primary keys, suitable for event streaming.

## Creating a Log Table

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("event_id", fluss::DataType::Int())
    .AddColumn("event_type", fluss::DataType::String())
    .AddColumn("timestamp", fluss::DataType::BigInt())
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .Build();

fluss::TablePath table_path("fluss", "events");
check("create_table", admin.CreateTable(table_path, descriptor, true));
```

## Writing to Log Tables

```cpp
fluss::Table table;
check("get_table", conn.GetTable(table_path, table));

fluss::AppendWriter writer;
check("new_writer", table.NewAppend().CreateWriter(writer));

fluss::GenericRow row;
row.SetInt32(0, 1);           // event_id
row.SetString(1, "user_login");  // event_type
row.SetInt64(2, 1704067200000L); // timestamp
check("append", writer.Append(row));

check("flush", writer.Flush());
```

## Reading from Log Tables

```cpp
fluss::LogScanner scanner;
check("new_scanner", table.NewScan().CreateLogScanner(scanner));

auto info = table.GetTableInfo();
for (int b = 0; b < info.num_buckets; ++b) {
    check("subscribe", scanner.Subscribe(b, 0));
}

fluss::ScanRecords records;
check("poll", scanner.Poll(5000, records));  // timeout in ms

for (const auto& rec : records) {
    std::cout << "event_id=" << rec.row.GetInt32(0)
              << " event_type=" << rec.row.GetString(1)
              << " timestamp=" << rec.row.GetInt64(2)
              << " @ offset=" << rec.offset << std::endl;
}
```

**Batch subscribe:**

```cpp
std::vector<fluss::BucketSubscription> subscriptions;
subscriptions.push_back({0, 0});    // bucket 0, offset 0
subscriptions.push_back({1, 100});  // bucket 1, offset 100
check("subscribe_batch", scanner.Subscribe(subscriptions));
```

**Arrow RecordBatch polling (high performance):**

```cpp
#include <arrow/record_batch.h>

fluss::LogScanner arrow_scanner;
check("new_scanner", table.NewScan().CreateRecordBatchScanner(arrow_scanner));

for (int b = 0; b < info.num_buckets; ++b) {
    check("subscribe", arrow_scanner.Subscribe(b, 0));
}

fluss::ArrowRecordBatches batches;
check("poll", arrow_scanner.PollRecordBatch(5000, batches));

for (size_t i = 0; i < batches.Size(); ++i) {
    const auto& batch = batches[i];
    if (batch->Available()) {
        auto arrow_batch = batch->GetArrowRecordBatch();
        std::cout << "Batch " << i << ": " << arrow_batch->num_rows() << " rows"
                  << ", partition_id=" << batch->GetPartitionId()
                  << ", bucket_id=" << batch->GetBucketId() << std::endl;
    }
}
```

## Column Projection

```cpp
// Project by column index
fluss::LogScanner projected_scanner;
check("new_scanner",
      table.NewScan().ProjectByIndex({0, 2}).CreateLogScanner(projected_scanner));

// Project by column name
fluss::LogScanner name_projected_scanner;
check("new_scanner",
      table.NewScan().ProjectByName({"event_id", "timestamp"}).CreateLogScanner(name_projected_scanner));

// Arrow RecordBatch with projection
fluss::LogScanner projected_arrow_scanner;
check("new_scanner",
      table.NewScan().ProjectByIndex({0, 2}).CreateRecordBatchScanner(projected_arrow_scanner));
```
