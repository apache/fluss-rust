// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/record_batch.h>

#include <chrono>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "fluss.hpp"

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code << " msg=" << r.error_message
                  << std::endl;
        std::exit(1);
    }
}

int main() {
    // 1) Connect
    fluss::Connection conn;
    check("connect", fluss::Connection::Connect("127.0.0.1:9123", conn));

    // 2) Admin
    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath table_path("fluss", "sample_table_cpp_v1");

    // 2.1) Drop table if exists
    std::cout << "Dropping table if exists..." << std::endl;
    auto drop_result = admin.DropTable(table_path, true);
    if (drop_result.Ok()) {
        std::cout << "Table dropped successfully" << std::endl;
    } else {
        std::cout << "Table drop result: " << drop_result.error_message << std::endl;
    }

    // 3) Schema with scalar and temporal columns
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("score", fluss::DataType::Float())
                      .AddColumn("age", fluss::DataType::Int())
                      .AddColumn("event_date", fluss::DataType::Date())
                      .AddColumn("event_time", fluss::DataType::Time())
                      .AddColumn("created_at", fluss::DataType::Timestamp())
                      .AddColumn("updated_at", fluss::DataType::TimestampLtz())
                      .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(3)
                          .SetComment("cpp example table with 3 buckets")
                          .Build();

    std::cout << "Creating table with 3 buckets..." << std::endl;
    check("create_table", admin.CreateTable(table_path, descriptor, false));

    // 4) Get table
    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    // 5) Write rows with scalar and temporal values
    fluss::AppendWriter writer;
    check("new_append_writer", table.NewAppend().CreateWriter(writer));

    struct RowData {
        int id;
        const char* name;
        float score;
        int age;
        fluss::Date date;
        fluss::Time time;
        fluss::Timestamp ts_ntz;
        fluss::Timestamp ts_ltz;
    };

    auto tp_now = std::chrono::system_clock::now();
    std::vector<RowData> rows = {
        {1, "Alice", 95.2f, 25, fluss::Date::FromYMD(2024, 6, 15), fluss::Time::FromHMS(14, 30, 45),
         fluss::Timestamp::FromTimePoint(tp_now), fluss::Timestamp::FromMillis(1718467200000)},
        {2, "Bob", 87.2f, 30, fluss::Date::FromYMD(2025, 1, 1), fluss::Time::FromHMS(0, 0, 0),
         fluss::Timestamp::FromMillis(1735689600000),
         fluss::Timestamp::FromMillisNanos(1735689600000, 500000)},
        {3, "Charlie", 92.1f, 35, fluss::Date::FromYMD(1999, 12, 31),
         fluss::Time::FromHMS(23, 59, 59), fluss::Timestamp::FromMillis(946684799999),
         fluss::Timestamp::FromMillis(946684799999)},
    };

    // Fire-and-forget: queue rows, flush at end
    for (const auto& r : rows) {
        fluss::GenericRow row;
        row.SetInt32(0, r.id);
        row.SetString(1, r.name);
        row.SetFloat32(2, r.score);
        row.SetInt32(3, r.age);
        row.SetDate(4, r.date);
        row.SetTime(5, r.time);
        row.SetTimestampNtz(6, r.ts_ntz);
        row.SetTimestampLtz(7, r.ts_ltz);
        check("append", writer.Append(row));
    }
    check("flush", writer.Flush());
    std::cout << "Wrote " << rows.size() << " rows (fire-and-forget + flush)" << std::endl;

    // Per-record acknowledgment
    {
        fluss::GenericRow row;
        row.SetInt32(0, 100);
        row.SetString(1, "AckTest");
        row.SetFloat32(2, 99.9f);
        row.SetInt32(3, 42);
        row.SetDate(4, fluss::Date::FromYMD(2025, 3, 1));
        row.SetTime(5, fluss::Time::FromHMS(12, 0, 0));
        row.SetTimestampNtz(6, fluss::Timestamp::FromMillis(1740787200000));
        row.SetTimestampLtz(7, fluss::Timestamp::FromMillis(1740787200000));
        fluss::WriteResult wr;
        check("append", writer.Append(row, wr));
        check("wait", wr.Wait());
        std::cout << "Row acknowledged by server" << std::endl;
    }

    // 6) Full scan — verify all column types including temporal
    fluss::LogScanner scanner;
    check("new_log_scanner", table.NewScan().CreateLogScanner(scanner));

    auto info = table.GetTableInfo();
    int buckets = info.num_buckets;
    for (int b = 0; b < buckets; ++b) {
        check("subscribe", scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords records;
    check("poll", scanner.Poll(5000, records));

    std::cout << "Scanned records: " << records.Size() << std::endl;
    bool scan_ok = true;
    for (const auto& rec : records.records) {
        if (rec.row.GetType(4) != fluss::DatumType::Date) {
            std::cerr << "ERROR: field 4 expected Date, got "
                      << static_cast<int>(rec.row.GetType(4)) << std::endl;
            scan_ok = false;
        }
        if (rec.row.GetType(5) != fluss::DatumType::Time) {
            std::cerr << "ERROR: field 5 expected Time, got "
                      << static_cast<int>(rec.row.GetType(5)) << std::endl;
            scan_ok = false;
        }
        if (rec.row.GetType(6) != fluss::DatumType::TimestampNtz) {
            std::cerr << "ERROR: field 6 expected TimestampNtz, got "
                      << static_cast<int>(rec.row.GetType(6)) << std::endl;
            scan_ok = false;
        }
        if (rec.row.GetType(7) != fluss::DatumType::TimestampLtz) {
            std::cerr << "ERROR: field 7 expected TimestampLtz, got "
                      << static_cast<int>(rec.row.GetType(7)) << std::endl;
            scan_ok = false;
        }

        auto date = rec.row.GetDate(4);
        auto time = rec.row.GetTime(5);
        auto ts_ntz = rec.row.GetTimestamp(6);
        auto ts_ltz = rec.row.GetTimestamp(7);

        std::cout << "  id=" << rec.row.GetInt32(0) << " name=" << rec.row.GetString(1)
                  << " score=" << rec.row.GetFloat32(2) << " age=" << rec.row.GetInt32(3)
                  << " date=" << date.Year() << "-" << date.Month() << "-" << date.Day()
                  << " time=" << time.Hour() << ":" << time.Minute() << ":" << time.Second()
                  << " ts_ntz=" << ts_ntz.epoch_millis << " ts_ltz=" << ts_ltz.epoch_millis << "+"
                  << ts_ltz.nano_of_millisecond << "ns" << std::endl;
    }

    if (!scan_ok) {
        std::cerr << "Full scan type verification FAILED!" << std::endl;
        std::exit(1);
    }

    // 7a) Projected scan by index — project [id, updated_at(TimestampLtz)] to verify
    //     NTZ/LTZ disambiguation works with column index remapping
    std::vector<size_t> projected_columns = {0, 7};
    fluss::LogScanner projected_scanner;
    check("new_log_scanner_with_projection",
          table.NewScan().ProjectByIndex(projected_columns).CreateLogScanner(projected_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected", projected_scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords projected_records;
    check("poll_projected", projected_scanner.Poll(5000, projected_records));

    std::cout << "Projected records: " << projected_records.Size() << std::endl;
    for (const auto& rec : projected_records.records) {
        if (rec.row.FieldCount() != 2) {
            std::cerr << "ERROR: expected 2 fields, got " << rec.row.FieldCount() << std::endl;
            scan_ok = false;
            continue;
        }
        if (rec.row.GetType(0) != fluss::DatumType::Int32) {
            std::cerr << "ERROR: projected field 0 expected Int32, got "
                      << static_cast<int>(rec.row.GetType(0)) << std::endl;
            scan_ok = false;
        }
        if (rec.row.GetType(1) != fluss::DatumType::TimestampLtz) {
            std::cerr << "ERROR: projected field 1 expected TimestampLtz, got "
                      << static_cast<int>(rec.row.GetType(1)) << std::endl;
            scan_ok = false;
        }

        auto ts = rec.row.GetTimestamp(1);
        std::cout << "  id=" << rec.row.GetInt32(0) << " updated_at=" << ts.epoch_millis << "+"
                  << ts.nano_of_millisecond << "ns" << std::endl;
    }

    // 7b) Projected scan by column names — same columns as above but using names
    fluss::LogScanner name_projected_scanner;
    check("project_by_name_scanner", table.NewScan()
                                         .ProjectByName({"id", "updated_at"})
                                         .CreateLogScanner(name_projected_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_name_projected", name_projected_scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords name_projected_records;
    check("poll_name_projected", name_projected_scanner.Poll(5000, name_projected_records));

    std::cout << "Name-projected records: " << name_projected_records.Size() << std::endl;
    for (const auto& rec : name_projected_records.records) {
        if (rec.row.FieldCount() != 2) {
            std::cerr << "ERROR: expected 2 fields, got " << rec.row.FieldCount() << std::endl;
            scan_ok = false;
            continue;
        }
        if (rec.row.GetType(0) != fluss::DatumType::Int32) {
            std::cerr << "ERROR: name-projected field 0 expected Int32, got "
                      << static_cast<int>(rec.row.GetType(0)) << std::endl;
            scan_ok = false;
        }
        if (rec.row.GetType(1) != fluss::DatumType::TimestampLtz) {
            std::cerr << "ERROR: name-projected field 1 expected TimestampLtz, got "
                      << static_cast<int>(rec.row.GetType(1)) << std::endl;
            scan_ok = false;
        }

        auto ts = rec.row.GetTimestamp(1);
        std::cout << "  id=" << rec.row.GetInt32(0) << " updated_at=" << ts.epoch_millis << "+"
                  << ts.nano_of_millisecond << "ns" << std::endl;
    }

    if (scan_ok) {
        std::cout << "Scan verification passed!" << std::endl;
    } else {
        std::cerr << "Scan verification FAILED!" << std::endl;
        std::exit(1);
    }

    // 8) List offsets examples
    std::cout << "\n=== List Offsets Examples ===" << std::endl;

    std::vector<int32_t> all_bucket_ids;
    all_bucket_ids.reserve(buckets);
    for (int b = 0; b < buckets; ++b) {
        all_bucket_ids.push_back(b);
    }

    std::unordered_map<int32_t, int64_t> earliest_offsets;
    check("list_earliest_offsets",
          admin.ListOffsets(table_path, all_bucket_ids, fluss::OffsetQuery::Earliest(),
                            earliest_offsets));
    std::cout << "Earliest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    std::unordered_map<int32_t, int64_t> latest_offsets;
    check("list_latest_offsets", admin.ListOffsets(table_path, all_bucket_ids,
                                                   fluss::OffsetQuery::Latest(), latest_offsets));
    std::cout << "Latest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : latest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    auto now = std::chrono::system_clock::now();
    auto one_hour_ago = now - std::chrono::hours(1);
    auto timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(one_hour_ago.time_since_epoch())
            .count();

    std::unordered_map<int32_t, int64_t> timestamp_offsets;
    check("list_timestamp_offsets",
          admin.ListOffsets(table_path, all_bucket_ids,
                            fluss::OffsetQuery::FromTimestamp(timestamp_ms), timestamp_offsets));
    std::cout << "Offsets for timestamp " << timestamp_ms << " (1 hour ago):" << std::endl;
    for (const auto& [bucket_id, offset] : timestamp_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    // 9) Batch subscribe
    std::cout << "\n=== Batch Subscribe Example ===" << std::endl;
    fluss::LogScanner batch_scanner;
    check("new_log_scanner_for_batch", table.NewScan().CreateLogScanner(batch_scanner));

    std::vector<fluss::BucketSubscription> subscriptions;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        subscriptions.push_back({bucket_id, offset});
        std::cout << "Preparing subscription: bucket=" << bucket_id << ", offset=" << offset
                  << std::endl;
    }

    check("subscribe_buckets", batch_scanner.Subscribe(subscriptions));
    std::cout << "Batch subscribed to " << subscriptions.size() << " buckets" << std::endl;

    fluss::ScanRecords batch_records;
    check("poll_batch", batch_scanner.Poll(5000, batch_records));

    std::cout << "Scanned " << batch_records.Size() << " records from batch subscription"
              << std::endl;
    for (size_t i = 0; i < batch_records.Size() && i < 5; ++i) {
        const auto& rec = batch_records[i];
        std::cout << "  Record " << i << ": bucket_id=" << rec.bucket_id
                  << ", offset=" << rec.offset << ", timestamp=" << rec.timestamp << std::endl;
    }
    if (batch_records.Size() > 5) {
        std::cout << "  ... and " << (batch_records.Size() - 5) << " more records" << std::endl;
    }

    // 10) Arrow record batch polling
    std::cout << "\n=== Testing Arrow Record Batch Polling ===" << std::endl;

    fluss::LogScanner arrow_scanner;
    check("new_record_batch_log_scanner", table.NewScan().CreateRecordBatchScanner(arrow_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_arrow", arrow_scanner.Subscribe(b, 0));
    }

    fluss::ArrowRecordBatches arrow_batches;
    check("poll_record_batch", arrow_scanner.PollRecordBatch(5000, arrow_batches));

    std::cout << "Polled " << arrow_batches.Size() << " Arrow record batches" << std::endl;
    for (size_t i = 0; i < arrow_batches.Size(); ++i) {
        const auto& batch = arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows()
                      << " rows" << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }

    // 11) Arrow record batch polling with projection
    std::cout << "\n=== Testing Arrow Record Batch Polling with Projection ===" << std::endl;

    fluss::LogScanner projected_arrow_scanner;
    check("new_record_batch_log_scanner_with_projection",
          table.NewScan()
              .ProjectByIndex(projected_columns)
              .CreateRecordBatchScanner(projected_arrow_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected_arrow", projected_arrow_scanner.Subscribe(b, 0));
    }

    fluss::ArrowRecordBatches projected_arrow_batches;
    check("poll_projected_record_batch",
          projected_arrow_scanner.PollRecordBatch(5000, projected_arrow_batches));

    std::cout << "Polled " << projected_arrow_batches.Size() << " projected Arrow record batches"
              << std::endl;
    for (size_t i = 0; i < projected_arrow_batches.Size(); ++i) {
        const auto& batch = projected_arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows()
                      << " rows" << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }

    // 12) Decimal support example
    std::cout << "\n=== Decimal Support Example ===" << std::endl;

    fluss::TablePath decimal_table_path("fluss", "decimal_table_cpp_v1");

    // Drop table if exists
    admin.DropTable(decimal_table_path, true);

    // Create schema with decimal columns
    auto decimal_schema = fluss::Schema::NewBuilder()
                              .AddColumn("id", fluss::DataType::Int())
                              .AddColumn("price", fluss::DataType::Decimal(10, 2))   // compact
                              .AddColumn("amount", fluss::DataType::Decimal(28, 8))  // i128
                              .Build();

    auto decimal_descriptor = fluss::TableDescriptor::NewBuilder()
                                  .SetSchema(decimal_schema)
                                  .SetBucketCount(1)
                                  .SetComment("cpp decimal example table")
                                  .Build();

    check("create_decimal_table", admin.CreateTable(decimal_table_path, decimal_descriptor, false));

    // Get table and writer
    fluss::Table decimal_table;
    check("get_decimal_table", conn.GetTable(decimal_table_path, decimal_table));

    fluss::AppendWriter decimal_writer;
    check("new_decimal_writer", decimal_table.NewAppend().CreateWriter(decimal_writer));

    // Just provide the value — Rust resolves (p,s) from schema
    {
        fluss::GenericRow row;
        row.SetInt32(0, 1);
        row.SetDecimal(1, "123.45");      // Rust knows DECIMAL(10,2)
        row.SetDecimal(2, "1.00000000");  // Rust knows DECIMAL(28,8)
        check("append_decimal", decimal_writer.Append(row));
    }
    {
        fluss::GenericRow row;
        row.SetInt32(0, 2);
        row.SetDecimal(1, "-999.99");
        row.SetDecimal(2, "3.14159265");
        check("append_decimal", decimal_writer.Append(row));
    }
    {
        fluss::GenericRow row;
        row.SetInt32(0, 3);
        row.SetDecimal(1, "500.00");
        row.SetDecimal(2, "2.71828182");
        check("append_decimal", decimal_writer.Append(row));
    }
    check("flush_decimal", decimal_writer.Flush());
    std::cout << "Wrote 3 decimal rows" << std::endl;

    // Scan and read back
    fluss::LogScanner decimal_scanner;
    check("new_decimal_scanner", decimal_table.NewScan().CreateLogScanner(decimal_scanner));
    check("subscribe_decimal", decimal_scanner.Subscribe(0, 0));

    fluss::ScanRecords decimal_records;
    check("poll_decimal", decimal_scanner.Poll(5000, decimal_records));

    std::cout << "Scanned decimal records: " << decimal_records.Size() << std::endl;
    for (const auto& rec : decimal_records) {
        std::cout << "  id=" << rec.row.GetInt32(0) << " price=" << rec.row.DecimalToString(1)
                  << " amount=" << rec.row.DecimalToString(2)
                  << " is_decimal=" << rec.row.IsDecimal(1) << std::endl;
    }

    // 13) Partitioned table example
    std::cout << "\n=== Partitioned Table Example ===" << std::endl;

    fluss::TablePath partitioned_table_path("fluss", "partitioned_table_cpp_v1");

    // Drop if exists
    check("drop_partitioned_table_if_exists", admin.DropTable(partitioned_table_path, true));

    // Create a partitioned table with a "region" partition key
    auto partitioned_schema = fluss::Schema::NewBuilder()
                                  .AddColumn("id", fluss::DataType::Int())
                                  .AddColumn("region", fluss::DataType::String())
                                  .AddColumn("value", fluss::DataType::BigInt())
                                  .Build();

    auto partitioned_descriptor = fluss::TableDescriptor::NewBuilder()
                                      .SetSchema(partitioned_schema)
                                      .SetPartitionKeys({"region"})
                                      .SetBucketCount(1)
                                      .SetComment("cpp partitioned table example")
                                      .Build();

    check("create_partitioned_table",
          admin.CreateTable(partitioned_table_path, partitioned_descriptor, false));
    std::cout << "Created partitioned table" << std::endl;

    // Create partitions
    check("create_partition_US",
          admin.CreatePartition(partitioned_table_path, {{"region", "US"}}, true));
    check("create_partition_EU",
          admin.CreatePartition(partitioned_table_path, {{"region", "EU"}}, true));
    std::cout << "Created partitions: US, EU" << std::endl;

    // List partitions
    std::vector<fluss::PartitionInfo> partition_infos;
    check("list_partition_infos",
          admin.ListPartitionInfos(partitioned_table_path, partition_infos));
    for (const auto& pi : partition_infos) {
        std::cout << "  Partition: " << pi.partition_name << " (id=" << pi.partition_id << ")"
                  << std::endl;
    }

    // Write data to partitioned table
    fluss::Table partitioned_table;
    check("get_partitioned_table", conn.GetTable(partitioned_table_path, partitioned_table));

    fluss::AppendWriter partitioned_writer;
    check("new_partitioned_writer", partitioned_table.NewAppend().CreateWriter(partitioned_writer));

    struct PartitionedRow {
        int id;
        const char* region;
        int64_t value;
    };

    std::vector<PartitionedRow> partitioned_rows = {
        {1, "US", 100},
        {2, "US", 200},
        {3, "EU", 300},
        {4, "EU", 400},
    };

    for (const auto& r : partitioned_rows) {
        fluss::GenericRow row;
        row.SetInt32(0, r.id);
        row.SetString(1, r.region);
        row.SetInt64(2, r.value);
        check("append_partitioned", partitioned_writer.Append(row));
    }
    check("flush_partitioned", partitioned_writer.Flush());
    std::cout << "Wrote " << partitioned_rows.size() << " rows to partitioned table" << std::endl;

    // 13.1) subscribe_partition_buckets: subscribe to each partition individually
    std::cout << "\n--- Testing SubscribePartitionBuckets ---" << std::endl;
    fluss::LogScanner partition_scanner;
    check("new_partition_scanner", partitioned_table.NewScan().CreateLogScanner(partition_scanner));

    for (const auto& pi : partition_infos) {
        check("subscribe_partition_buckets",
              partition_scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0));
        std::cout << "Subscribed to partition " << pi.partition_name << std::endl;
    }

    fluss::ScanRecords partition_records;
    check("poll_partitioned", partition_scanner.Poll(5000, partition_records));
    std::cout << "Scanned " << partition_records.Size() << " records from partitioned table"
              << std::endl;
    for (size_t i = 0; i < partition_records.Size(); ++i) {
        const auto& rec = partition_records[i];
        std::cout << "  Record " << i << ": id=" << rec.row.GetInt32(0)
                  << ", region=" << rec.row.GetString(1) << ", value=" << rec.row.GetInt64(2)
                  << std::endl;
    }

    // 13.2) subscribe_partition_buckets: batch subscribe to all partitions at once
    std::cout << "\n--- Testing SubscribePartitionBuckets (batch) ---" << std::endl;
    fluss::LogScanner partition_batch_scanner;
    check("new_partition_batch_scanner",
          partitioned_table.NewScan().CreateLogScanner(partition_batch_scanner));

    std::vector<fluss::PartitionBucketSubscription> partition_subs;
    for (const auto& pi : partition_infos) {
        partition_subs.push_back({pi.partition_id, 0, 0});
    }
    check("subscribe_partition_buckets",
          partition_batch_scanner.SubscribePartitionBuckets(partition_subs));
    std::cout << "Batch subscribed to " << partition_subs.size() << " partition+bucket combinations"
              << std::endl;

    fluss::ScanRecords partition_batch_records;
    check("poll_partition_batch", partition_batch_scanner.Poll(5000, partition_batch_records));
    std::cout << "Scanned " << partition_batch_records.Size()
              << " records from batch partition subscription" << std::endl;
    for (size_t i = 0; i < partition_batch_records.Size(); ++i) {
        const auto& rec = partition_batch_records[i];
        std::cout << "  Record " << i << ": id=" << rec.row.GetInt32(0)
                  << ", region=" << rec.row.GetString(1) << ", value=" << rec.row.GetInt64(2)
                  << std::endl;
    }

    // Cleanup
    check("drop_partitioned_table", admin.DropTable(partitioned_table_path, true));
    std::cout << "Dropped partitioned table" << std::endl;
    return 0;
}
