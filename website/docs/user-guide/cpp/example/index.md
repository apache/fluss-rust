---
sidebar_position: 1
---
# Example

Minimal working examples: connect to Fluss, create a table, write data, and read it back.

```cpp
#include <iostream>
#include "fluss.hpp"

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: " << r.error_message << std::endl;
        std::exit(1);
    }
}

int main() {
    // Connect
    fluss::Configuration config;
    config.bootstrap_server = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    // Create a log table
    fluss::TablePath table_path("fluss", "quickstart_cpp");
    auto schema = fluss::Schema::NewBuilder()
        .AddColumn("id", fluss::DataType::Int())
        .AddColumn("name", fluss::DataType::String())
        .Build();
    auto descriptor = fluss::TableDescriptor::NewBuilder()
        .SetSchema(schema)
        .Build();
    check("create_table", admin.CreateTable(table_path, descriptor, true));

    // Write
    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    fluss::AppendWriter writer;
    check("new_writer", table.NewAppend().CreateWriter(writer));

    fluss::GenericRow row;
    row.SetInt32(0, 1);
    row.SetString(1, "hello");
    check("append", writer.Append(row));
    check("flush", writer.Flush());

    // Read
    fluss::LogScanner scanner;
    check("new_scanner", table.NewScan().CreateLogScanner(scanner));
    auto info = table.GetTableInfo();
    for (int b = 0; b < info.num_buckets; ++b) {
        check("subscribe", scanner.Subscribe(b, 0));
    }
    fluss::ScanRecords records;
    check("poll", scanner.Poll(5000, records));
    for (const auto& rec : records) {
        std::cout << "id=" << rec.row.GetInt32(0)
                  << ", name=" << rec.row.GetString(1) << std::endl;
    }

    return 0;
}
```
