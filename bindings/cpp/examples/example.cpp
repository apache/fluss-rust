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

#include "fluss.hpp"

#include <iostream>
#include <vector>

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

    // 3) Schema & descriptor
    auto schema = fluss::Schema::NewBuilder()
                        .AddColumn("id", fluss::DataType::Int)
                        .AddColumn("name", fluss::DataType::String)
                        .AddColumn("score", fluss::DataType::Float)
                        .AddColumn("age", fluss::DataType::Int)
                        .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .SetProperty("table.log.arrow.compression.type", "NONE")
                          .SetComment("cpp example table")
                          .Build();

    fluss::TablePath table_path("fluss", "sample_table_cpp_v1");
    // ignore_if_exists=true to allow re-run
    check("create_table", admin.CreateTable(table_path, descriptor, true));

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
    check("new_log_scanner", table.NewLogScanner(scanner));

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
          table.NewLogScannerWithProjection(projected_columns, projected_scanner));
    
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

    return 0;
}
