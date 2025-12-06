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
                          .SetComment("cpp example table")
                          .Build();

    fluss::TablePath table_path("fluss", "sample_table_cpp");
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
        std::cout << "  offset=" << rec.offset << " ts=" << rec.timestamp << std::endl;
    }

    return 0;
}
