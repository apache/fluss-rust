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

#include <iostream>
#include <string>
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
    const std::string bootstrap = "127.0.0.1:9123";

    // 1) Connect and get Admin
    fluss::Connection conn;
    check("connect", fluss::Connection::Connect(bootstrap, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath kv_table_path("fluss", "kv_table_cpp_v1");

    // Drop if exists
    admin.DropTable(kv_table_path, true);

    // 2) Create a KV table with primary key, including decimal and temporal types
    auto kv_schema = fluss::Schema::NewBuilder()
                         .AddColumn("user_id", fluss::DataType::Int())
                         .AddColumn("name", fluss::DataType::String())
                         .AddColumn("email", fluss::DataType::String())
                         .AddColumn("score", fluss::DataType::Float())
                         .AddColumn("balance", fluss::DataType::Decimal(10, 2))
                         .AddColumn("birth_date", fluss::DataType::Date())
                         .AddColumn("login_time", fluss::DataType::Time())
                         .AddColumn("created_at", fluss::DataType::Timestamp())
                         .AddColumn("last_seen", fluss::DataType::TimestampLtz())
                         .SetPrimaryKeys({"user_id"})
                         .Build();

    auto kv_descriptor = fluss::TableDescriptor::NewBuilder()
                             .SetSchema(kv_schema)
                             .SetBucketCount(3)
                             .SetComment("cpp kv table example")
                             .Build();

    check("create_kv_table", admin.CreateTable(kv_table_path, kv_descriptor, false));
    std::cout << "Created KV table with primary key" << std::endl;

    fluss::Table kv_table;
    check("get_kv_table", conn.GetTable(kv_table_path, kv_table));

    // 3) Upsert rows using name-based Set()
    //    - Set("balance", "1234.56") auto-routes to SetDecimal (schema-aware)
    //    - Set("created_at", ts) auto-routes to SetTimestampNtz (schema-aware)
    //    - Set("last_seen", ts) auto-routes to SetTimestampLtz (schema-aware)
    std::cout << "\n--- Upsert Rows ---" << std::endl;
    fluss::UpsertWriter upsert_writer;
    check("new_upsert_writer", kv_table.NewUpsertWriter(upsert_writer));

    // Fire-and-forget upserts
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 1);
        row.Set("name", "Alice");
        row.Set("email", "alice@example.com");
        row.Set("score", 95.5f);
        row.Set("balance", "1234.56");
        row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
        row.Set("login_time", fluss::Time::FromHMS(9, 30, 0));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000060000));
        check("upsert_1", upsert_writer.Upsert(row));
    }
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 2);
        row.Set("name", "Bob");
        row.Set("email", "bob@example.com");
        row.Set("score", 87.3f);
        row.Set("balance", "567.89");
        row.Set("birth_date", fluss::Date::FromYMD(1985, 7, 22));
        row.Set("login_time", fluss::Time::FromHMS(14, 15, 30));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000100000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000200000));
        check("upsert_2", upsert_writer.Upsert(row));
    }

    // Per-record acknowledgment
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 3);
        row.Set("name", "Charlie");
        row.Set("email", "charlie@example.com");
        row.Set("score", 92.0f);
        row.Set("balance", "99999.99");
        row.Set("birth_date", fluss::Date::FromYMD(2000, 1, 1));
        row.Set("login_time", fluss::Time::FromHMS(23, 59, 59));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000300000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000400000));
        fluss::WriteResult wr;
        check("upsert_3", upsert_writer.Upsert(row, wr));
        check("upsert_3_wait", wr.Wait());
        std::cout << "Upsert acknowledged by server" << std::endl;
    }

    check("upsert_flush", upsert_writer.Flush());
    std::cout << "Upserted 3 rows" << std::endl;

    // 4) Lookup by primary key — verify all types round-trip
    std::cout << "\n--- Lookup by Primary Key ---" << std::endl;
    fluss::Lookuper lookuper;
    check("new_lookuper", kv_table.NewLookuper(lookuper));

    // Lookup existing key
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 1);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_1", lookuper.Lookup(pk_row, found, result_row));
        if (found) {
            auto date = result_row.GetDate(5);
            auto time = result_row.GetTime(6);
            auto created = result_row.GetTimestamp(7);
            auto seen = result_row.GetTimestamp(8);
            std::cout << "Found user_id=1:"
                      << "\n  name=" << result_row.GetString(1)
                      << "\n  email=" << result_row.GetString(2)
                      << "\n  score=" << result_row.GetFloat32(3)
                      << "\n  balance=" << result_row.DecimalToString(4)
                      << "\n  birth_date=" << date.Year() << "-" << date.Month() << "-"
                      << date.Day() << "\n  login_time=" << time.Hour() << ":" << time.Minute()
                      << ":" << time.Second() << "\n  created_at(ms)=" << created.epoch_millis
                      << "\n  last_seen(ms)=" << seen.epoch_millis << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=1" << std::endl;
            std::exit(1);
        }
    }

    // Lookup non-existing key
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 999);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_999", lookuper.Lookup(pk_row, found, result_row));
        if (!found) {
            std::cout << "user_id=999 not found (expected)" << std::endl;
        } else {
            std::cerr << "ERROR: Expected user_id=999 to not be found" << std::endl;
            std::exit(1);
        }
    }

    // 5) Update via upsert (overwrite existing key)
    std::cout << "\n--- Update via Upsert ---" << std::endl;
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 1);
        row.Set("name", "Alice Updated");
        row.Set("email", "alice.new@example.com");
        row.Set("score", 99.0f);
        row.Set("balance", "9999.00");
        row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
        row.Set("login_time", fluss::Time::FromHMS(10, 0, 0));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000500000));
        fluss::WriteResult wr;
        check("upsert_update", upsert_writer.Upsert(row, wr));
        check("upsert_update_wait", wr.Wait());
    }

    // Verify update
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 1);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_updated", lookuper.Lookup(pk_row, found, result_row));
        if (found && result_row.GetString(1) == "Alice Updated") {
            std::cout << "Update verified: name=" << result_row.GetString(1)
                      << " balance=" << result_row.DecimalToString(4)
                      << " last_seen(ms)=" << result_row.GetTimestamp(8).epoch_millis << std::endl;
        } else {
            std::cerr << "ERROR: Update verification failed" << std::endl;
            std::exit(1);
        }
    }

    // 6) Delete by primary key
    std::cout << "\n--- Delete by Primary Key ---" << std::endl;
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 2);
        fluss::WriteResult wr;
        check("delete_2", upsert_writer.Delete(pk_row, wr));
        check("delete_2_wait", wr.Wait());
        std::cout << "Deleted user_id=2" << std::endl;
    }

    // Verify deletion
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 2);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_deleted", lookuper.Lookup(pk_row, found, result_row));
        if (!found) {
            std::cout << "Delete verified: user_id=2 not found" << std::endl;
        } else {
            std::cerr << "ERROR: Expected user_id=2 to be deleted" << std::endl;
            std::exit(1);
        }
    }

    // 7) Partial update by column names
    std::cout << "\n--- Partial Update by Column Names ---" << std::endl;
    fluss::UpsertWriter partial_writer;
    check("new_partial_upsert_writer",
          kv_table.NewUpsertWriter(partial_writer,
                                   std::vector<std::string>{"user_id", "balance", "last_seen"}));

    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 3);
        row.Set("balance", "50000.00");
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000999000));
        fluss::WriteResult wr;
        check("partial_upsert", partial_writer.Upsert(row, wr));
        check("partial_upsert_wait", wr.Wait());
        std::cout << "Partial update: set balance=50000.00, last_seen for user_id=3" << std::endl;
    }

    // Verify partial update (other fields unchanged)
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 3);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_partial", lookuper.Lookup(pk_row, found, result_row));
        if (found) {
            std::cout << "Partial update verified:"
                      << "\n  name=" << result_row.GetString(1) << " (unchanged)"
                      << "\n  balance=" << result_row.DecimalToString(4) << " (updated)"
                      << "\n  last_seen(ms)=" << result_row.GetTimestamp(8).epoch_millis
                      << " (updated)" << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=3" << std::endl;
            std::exit(1);
        }
    }

    // 8) Partial update by column indices (using index-based setters for lower overhead)
    std::cout << "\n--- Partial Update by Column Indices ---" << std::endl;
    fluss::UpsertWriter partial_writer_idx;
    // Columns: 0=user_id (PK), 1=name — update name only
    check("new_partial_upsert_writer_idx",
          kv_table.NewUpsertWriter(partial_writer_idx, std::vector<size_t>{0, 1}));

    {
        // Index-based setters: lighter than name-based, useful for hot paths
        fluss::GenericRow row;
        row.SetInt32(0, 3);                   // user_id (PK)
        row.SetString(1, "Charlie Updated");  // name
        fluss::WriteResult wr;
        check("partial_upsert_idx", partial_writer_idx.Upsert(row, wr));
        check("partial_upsert_idx_wait", wr.Wait());
        std::cout << "Partial update by indices: set name='Charlie Updated' for user_id=3"
                  << std::endl;
    }

    // Verify: name changed, balance/last_seen unchanged from previous partial update
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 3);

        bool found = false;
        fluss::GenericRow result_row;
        check("lookup_partial_idx", lookuper.Lookup(pk_row, found, result_row));
        if (found) {
            std::cout << "Partial update by indices verified:"
                      << "\n  name=" << result_row.GetString(1) << " (updated)"
                      << "\n  balance=" << result_row.DecimalToString(4) << " (unchanged)"
                      << "\n  last_seen(ms)=" << result_row.GetTimestamp(8).epoch_millis
                      << " (unchanged)" << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=3" << std::endl;
            std::exit(1);
        }
    }

    // Cleanup
    check("drop_kv_table", admin.DropTable(kv_table_path, true));

    // 9) Partitioned KV table
    std::cout << "\n--- Partitioned KV Table ---" << std::endl;
    fluss::TablePath part_kv_path("fluss", "partitioned_kv_cpp_v1");
    admin.DropTable(part_kv_path, true);

    auto part_kv_schema = fluss::Schema::NewBuilder()
                              .AddColumn("region", fluss::DataType::String())
                              .AddColumn("user_id", fluss::DataType::Int())
                              .AddColumn("name", fluss::DataType::String())
                              .AddColumn("score", fluss::DataType::BigInt())
                              .SetPrimaryKeys({"region", "user_id"})
                              .Build();

    auto part_kv_descriptor = fluss::TableDescriptor::NewBuilder()
                                  .SetSchema(part_kv_schema)
                                  .SetPartitionKeys({"region"})
                                  .SetComment("partitioned kv table example")
                                  .Build();

    check("create_part_kv", admin.CreateTable(part_kv_path, part_kv_descriptor, false));
    std::cout << "Created partitioned KV table" << std::endl;

    // Create partitions
    check("create_US", admin.CreatePartition(part_kv_path, {{"region", "US"}}));
    check("create_EU", admin.CreatePartition(part_kv_path, {{"region", "EU"}}));
    check("create_APAC", admin.CreatePartition(part_kv_path, {{"region", "APAC"}}));
    std::cout << "Created partitions: US, EU, APAC" << std::endl;

    fluss::Table part_kv_table;
    check("get_part_kv_table", conn.GetTable(part_kv_path, part_kv_table));

    fluss::UpsertWriter part_writer;
    check("new_part_writer", part_kv_table.NewUpsertWriter(part_writer));

    // Upsert rows across partitions
    struct TestRow {
        const char* region;
        int32_t user_id;
        const char* name;
        int64_t score;
    };
    TestRow test_data[] = {
        {"US", 1, "Gustave", 100}, {"US", 2, "Lune", 200},   {"EU", 1, "Sciel", 150},
        {"EU", 2, "Maelle", 250},  {"APAC", 1, "Noco", 300},
    };

    for (const auto& td : test_data) {
        auto row = part_kv_table.NewRow();
        row.Set("region", td.region);
        row.Set("user_id", td.user_id);
        row.Set("name", td.name);
        row.Set("score", td.score);
        check("part_upsert", part_writer.Upsert(row));
    }
    check("part_flush", part_writer.Flush());
    std::cout << "Upserted 5 rows across 3 partitions" << std::endl;

    // Lookup all rows
    fluss::Lookuper part_lookuper;
    check("new_part_lookuper", part_kv_table.NewLookuper(part_lookuper));

    for (const auto& td : test_data) {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", td.region);
        pk.Set("user_id", td.user_id);

        bool found = false;
        fluss::GenericRow result;
        check("part_lookup", part_lookuper.Lookup(pk, found, result));
        if (!found) {
            std::cerr << "ERROR: Expected to find region=" << td.region << " user_id=" << td.user_id
                      << std::endl;
            std::exit(1);
        }
        if (result.GetString(2) != td.name || result.GetInt64(3) != td.score) {
            std::cerr << "ERROR: Data mismatch for region=" << td.region
                      << " user_id=" << td.user_id << std::endl;
            std::exit(1);
        }
    }
    std::cout << "All 5 rows verified across partitions" << std::endl;

    // Update within a partition
    {
        auto row = part_kv_table.NewRow();
        row.Set("region", "US");
        row.Set("user_id", 1);
        row.Set("name", "Gustave Updated");
        row.Set("score", static_cast<int64_t>(999));
        fluss::WriteResult wr;
        check("part_update", part_writer.Upsert(row, wr));
        check("part_update_wait", wr.Wait());
    }
    {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", "US");
        pk.Set("user_id", 1);
        bool found = false;
        fluss::GenericRow result;
        check("part_lookup_updated", part_lookuper.Lookup(pk, found, result));
        if (!found || result.GetString(2) != "Gustave Updated" || result.GetInt64(3) != 999) {
            std::cerr << "ERROR: Partition update verification failed" << std::endl;
            std::exit(1);
        }
        std::cout << "Update verified: US/1 name=" << result.GetString(2)
                  << " score=" << result.GetInt64(3) << std::endl;
    }

    // Lookup in non-existent partition
    {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", "UNKNOWN");
        pk.Set("user_id", 1);
        bool found = false;
        fluss::GenericRow result;
        check("part_lookup_unknown", part_lookuper.Lookup(pk, found, result));
        if (found) {
            std::cerr << "ERROR: Expected UNKNOWN partition lookup to return not found"
                      << std::endl;
            std::exit(1);
        }
        std::cout << "UNKNOWN partition lookup: not found (expected)" << std::endl;
    }

    // Delete within a partition
    {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 1);
        fluss::WriteResult wr;
        check("part_delete", part_writer.Delete(pk, wr));
        check("part_delete_wait", wr.Wait());
    }
    {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 1);
        bool found = false;
        fluss::GenericRow result;
        check("part_lookup_deleted", part_lookuper.Lookup(pk, found, result));
        if (found) {
            std::cerr << "ERROR: Expected EU/1 to be deleted" << std::endl;
            std::exit(1);
        }
        std::cout << "Delete verified: EU/1 not found" << std::endl;
    }

    // Verify other record in same partition still exists
    {
        auto pk = part_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 2);
        bool found = false;
        fluss::GenericRow result;
        check("part_lookup_eu2", part_lookuper.Lookup(pk, found, result));
        if (!found || result.GetString(2) != "Maelle") {
            std::cerr << "ERROR: Expected EU/2 (Maelle) to still exist" << std::endl;
            std::exit(1);
        }
        std::cout << "EU/2 still exists: name=" << result.GetString(2) << std::endl;
    }

    check("drop_part_kv", admin.DropTable(part_kv_path, true));
    std::cout << "\nKV table example completed successfully!" << std::endl;

    return 0;
}
