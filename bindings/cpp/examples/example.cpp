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
        std::cerr << step << " failed: code=" << r.error_code
                  << " msg=" << r.error_message << std::endl;
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

    // 3) Schema & descriptor
    auto schema = fluss::Schema::NewBuilder()
                        .AddColumn("id", fluss::DataType::Int)
                        .AddColumn("name", fluss::DataType::String)
                        .AddColumn("score", fluss::DataType::Float)
                        .AddColumn("age", fluss::DataType::Int)
                        .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(3)
                          .SetComment("cpp example table with 3 buckets")
                          .Build();

    // 3.1) Create table with 3 buckets
    std::cout << "Creating table with 3 buckets..." << std::endl;
    check("create_table", admin.CreateTable(table_path, descriptor, false));

    // 4) Get table
    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    // 5) Writer
    fluss::AppendWriter writer;
    check("new_append_writer", table.NewAppendWriter(writer));

    struct RowData {
        int id;
        const char* name;
        float score;
        int age;
    };

    std::vector<RowData> rows = {
        {1, "Alice", 95.2f, 25},
        {2, "Bob", 87.2f, 30},
        {3, "Charlie", 92.1f, 35},
    };

    for (const auto& r : rows) {
        fluss::GenericRow row;
        row.SetInt32(0, r.id);
        row.SetString(1, r.name);
        row.SetFloat32(2, r.score);
        row.SetInt32(3, r.age);
        check("append", writer.Append(row));
    }
    check("flush", writer.Flush());
    std::cout << "Wrote " << rows.size() << " rows" << std::endl;

    // 6) Scan
    fluss::LogScanner scanner;
    check("new_log_scanner", table.NewScan().CreateLogScanner(scanner));

    auto info = table.GetTableInfo();
    int buckets = info.num_buckets;
    for (int b = 0; b < buckets; ++b) {
        check("subscribe", scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords records;
    check("poll", scanner.Poll(5000, records));

    std::cout << "Scanned records: " << records.records.size() << std::endl;
    for (const auto& rec : records.records) {
        std::cout << " offset=" << rec.offset << " id=" << rec.row.fields[0].i32_val
                  << " name=" << rec.row.fields[1].string_val
                  << " score=" << rec.row.fields[2].f32_val << " age=" << rec.row.fields[3].i32_val
                  << " ts=" << rec.timestamp << std::endl;
    }
    
    // 7) Project only id (0) and name (1) columns
    std::vector<size_t> projected_columns = {0, 1};
    fluss::LogScanner projected_scanner;
    check("new_log_scanner_with_projection",
          table.NewScan().Project(projected_columns).CreateLogScanner(projected_scanner));
    
    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected", projected_scanner.Subscribe(b, 0));
    }
    
    fluss::ScanRecords projected_records;
    check("poll_projected", projected_scanner.Poll(5000, projected_records));
    
    std::cout << "Projected records: " << projected_records.records.size() << std::endl;
    
    bool projection_verified = true;
    for (size_t i = 0; i < projected_records.records.size(); ++i) {
        const auto& rec = projected_records.records[i];
        const auto& row = rec.row;
        
        if (row.fields.size() != projected_columns.size()) {
            std::cerr << "ERROR: Record " << i << " has " << row.fields.size() 
                      << " fields, expected " << projected_columns.size() << std::endl;
            projection_verified = false;
            continue;
        }
        
        // Verify field types match expected columns
        // Column 0 (id) should be Int32, Column 1 (name) should be String
        if (row.fields[0].type != fluss::DatumType::Int32) {
            std::cerr << "ERROR: Record " << i << " field 0 type mismatch, expected Int32" << std::endl;
            projection_verified = false;
        }
        if (row.fields[1].type != fluss::DatumType::String) {
            std::cerr << "ERROR: Record " << i << " field 1 type mismatch, expected String" << std::endl;
            projection_verified = false;
        }
        
        // Print projected data
        if (row.fields[0].type == fluss::DatumType::Int32 && 
            row.fields[1].type == fluss::DatumType::String) {
            std::cout << "  Record " << i << ": id=" << row.fields[0].i32_val 
                      << ", name=" << row.fields[1].string_val << std::endl;
        }
    }
    
    if (projection_verified) {
        std::cout << "Column pruning verification passed!" << std::endl;
    } else {
        std::cerr << "Column pruning verification failed!" << std::endl;
        std::exit(1);
    }

    // 8) List offsets examples
    std::cout << "\n=== List Offsets Examples ===" << std::endl;
    
    // 8.1) Query earliest offsets for all buckets
    std::vector<int32_t> all_bucket_ids;
    all_bucket_ids.reserve(buckets);
    for (int b = 0; b < buckets; ++b) {
        all_bucket_ids.push_back(b);
    }
    
    std::unordered_map<int32_t, int64_t> earliest_offsets;
    check("list_earliest_offsets", 
          admin.ListOffsets(table_path, all_bucket_ids, 
                           fluss::OffsetQuery::Earliest(), 
                           earliest_offsets));
    std::cout << "Earliest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }
    
    // 8.2) Query latest offsets for all buckets
    std::unordered_map<int32_t, int64_t> latest_offsets;
    check("list_latest_offsets", 
          admin.ListOffsets(table_path, all_bucket_ids, 
                           fluss::OffsetQuery::Latest(), 
                           latest_offsets));
    std::cout << "Latest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : latest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }
    
    // 8.3) Query offsets for a specific timestamp (current time - 1 hour)
    auto now = std::chrono::system_clock::now();
    auto one_hour_ago = now - std::chrono::hours(1);
    auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        one_hour_ago.time_since_epoch()).count();
    
    std::unordered_map<int32_t, int64_t> timestamp_offsets;
    check("list_timestamp_offsets", 
          admin.ListOffsets(table_path, all_bucket_ids, 
                           fluss::OffsetQuery::FromTimestamp(timestamp_ms), 
                           timestamp_offsets));
    std::cout << "Offsets for timestamp " << timestamp_ms << " (1 hour ago):" << std::endl;
    for (const auto& [bucket_id, offset] : timestamp_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }
    
    // 8.4) Use batch subscribe with offsets from list_offsets
    std::cout << "\n=== Batch Subscribe Example ===" << std::endl;
    fluss::LogScanner batch_scanner;
    check("new_log_scanner_for_batch", table.NewScan().CreateLogScanner(batch_scanner));
    
    std::vector<fluss::BucketSubscription> subscriptions;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        subscriptions.push_back({bucket_id, offset});
        std::cout << "Preparing subscription: bucket=" << bucket_id 
                  << ", offset=" << offset << std::endl;
    }
    
    check("subscribe_batch", batch_scanner.Subscribe(subscriptions));
    std::cout << "Batch subscribed to " << subscriptions.size() << " buckets" << std::endl;
    
    // 8.5) Poll and verify bucket_id in records
    fluss::ScanRecords batch_records;
    check("poll_batch", batch_scanner.Poll(5000, batch_records));
    
    std::cout << "Scanned " << batch_records.Size() << " records from batch subscription" << std::endl;
    for (size_t i = 0; i < batch_records.Size() && i < 5; ++i) {
        const auto& rec = batch_records[i];
        std::cout << "  Record " << i << ": bucket_id=" << rec.bucket_id 
                  << ", offset=" << rec.offset 
                  << ", timestamp=" << rec.timestamp << std::endl;
    }
    if (batch_records.Size() > 5) {
        std::cout << "  ... and " << (batch_records.Size() - 5) << " more records" << std::endl;
    }

    // 9) Test the new Arrow record batch polling functionality
    std::cout << "\n=== Testing Arrow Record Batch Polling ===" << std::endl;

    fluss::LogScanner arrow_scanner;
    check("new_record_batch_log_scanner", table.NewScan().CreateRecordBatchScanner(arrow_scanner));
    
    // Subscribe to all buckets starting from offset 0
    for (int b = 0; b < buckets; ++b) {
        check("subscribe_arrow", arrow_scanner.Subscribe(b, 0));
    }
    
    fluss::ArrowRecordBatches arrow_batches;
    check("poll_record_batch", arrow_scanner.PollRecordBatch(5000, arrow_batches));
    
    std::cout << "Polled " << arrow_batches.Size() << " Arrow record batches" << std::endl;
    for (size_t i = 0; i < arrow_batches.Size(); ++i) {
        const auto& batch = arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows() << " rows. " << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }
    
    // 10) Test the new Arrow record batch polling with projection
    std::cout << "\n=== Testing Arrow Record Batch Polling with Projection ===" << std::endl;

    fluss::LogScanner projected_arrow_scanner;
    check("new_record_batch_log_scanner_with_projection",
          table.NewScan().Project(projected_columns).CreateRecordBatchScanner(projected_arrow_scanner));
    
    // Subscribe to all buckets starting from offset 0
    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected_arrow", projected_arrow_scanner.Subscribe(b, 0));
    }
    
    fluss::ArrowRecordBatches projected_arrow_batches;
    check("poll_projected_record_batch", projected_arrow_scanner.PollRecordBatch(5000, projected_arrow_batches));
    
    std::cout << "Polled " << projected_arrow_batches.Size() << " projected Arrow record batches" << std::endl;
    for (size_t i = 0; i < projected_arrow_batches.Size(); ++i) {
        const auto& batch = projected_arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows() << " rows " << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }

    return 0;
}
