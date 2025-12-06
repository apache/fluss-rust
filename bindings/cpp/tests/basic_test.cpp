/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>
#include "fluss.hpp"

TEST(FlussTest, TablePathCreation) {
    fluss::TablePath path("fluss", "test_table");
    EXPECT_EQ(path.database_name, "fluss");
    EXPECT_EQ(path.table_name, "test_table");
    EXPECT_EQ(path.ToString(), "fluss.test_table");
}

TEST(FlussTest, SchemaBuilder) {
    auto schema = fluss::Schema::NewBuilder()
        .AddColumn("id", fluss::DataType::Int, "Primary key")
        .AddColumn("name", fluss::DataType::String)
        .AddColumn("value", fluss::DataType::Double)
        .SetPrimaryKeys({"id"})
        .Build();

    EXPECT_EQ(schema.columns.size(), 3);
    EXPECT_EQ(schema.columns[0].name, "id");
    EXPECT_EQ(schema.columns[0].data_type, fluss::DataType::Int);
    EXPECT_EQ(schema.columns[0].comment, "Primary key");
    EXPECT_EQ(schema.primary_keys.size(), 1);
    EXPECT_EQ(schema.primary_keys[0], "id");
}

TEST(FlussTest, TableDescriptorBuilder) {
    auto schema = fluss::Schema::NewBuilder()
        .AddColumn("id", fluss::DataType::Int)
        .AddColumn("name", fluss::DataType::String)
        .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
        .SetSchema(schema)
        .SetBucketCount(3)
        .SetProperty("table.log.arrow.compression.type", "NONE")
        .SetComment("Test table")
        .Build();

    EXPECT_EQ(descriptor.schema.columns.size(), 2);
    EXPECT_EQ(descriptor.bucket_count, 3);
    EXPECT_EQ(descriptor.properties.at("table.log.arrow.compression.type"), "NONE");
    EXPECT_EQ(descriptor.comment, "Test table");
}

TEST(FlussTest, GenericRowOperations) {
    fluss::GenericRow row;

    row.SetInt32(0, 42);
    row.SetString(1, "hello");
    row.SetFloat64(2, 3.14);
    row.SetNull(3);

    EXPECT_EQ(row.fields.size(), 4);
    EXPECT_EQ(row.fields[0].type, fluss::DatumType::Int32);
    EXPECT_EQ(row.fields[0].i32_val, 42);
    EXPECT_EQ(row.fields[1].type, fluss::DatumType::String);
    EXPECT_EQ(row.fields[1].string_val, "hello");
    EXPECT_EQ(row.fields[2].type, fluss::DatumType::Float64);
    EXPECT_DOUBLE_EQ(row.fields[2].f64_val, 3.14);
    EXPECT_EQ(row.fields[3].type, fluss::DatumType::Null);
}

TEST(FlussTest, DatumFactory) {
    auto null_datum = fluss::Datum::Null();
    EXPECT_EQ(null_datum.type, fluss::DatumType::Null);

    auto bool_datum = fluss::Datum::Bool(true);
    EXPECT_EQ(bool_datum.type, fluss::DatumType::Bool);
    EXPECT_TRUE(bool_datum.bool_val);

    auto int_datum = fluss::Datum::Int32(123);
    EXPECT_EQ(int_datum.type, fluss::DatumType::Int32);
    EXPECT_EQ(int_datum.i32_val, 123);

    auto str_datum = fluss::Datum::String("test");
    EXPECT_EQ(str_datum.type, fluss::DatumType::String);
    EXPECT_EQ(str_datum.string_val, "test");
}

TEST(FlussTest, ConnectionNotAvailable) {
    fluss::Connection conn;
    EXPECT_FALSE(conn.Available());

    fluss::Admin admin;
    auto result = conn.GetAdmin(admin);
    EXPECT_FALSE(result.Ok());
    EXPECT_NE(result.error_code, 0);
}

TEST(FlussTest, AdminNotAvailable) {
    fluss::Admin admin;
    EXPECT_FALSE(admin.Available());

    fluss::TablePath path("fluss", "test");
    fluss::TableInfo info;
    auto result = admin.GetTable(path, info);
    EXPECT_FALSE(result.Ok());
    EXPECT_NE(result.error_code, 0);
}

TEST(FlussTest, TableNotAvailable) {
    fluss::Table table;
    EXPECT_FALSE(table.Available());

    fluss::AppendWriter writer;
    auto result = table.NewAppendWriter(writer);
    EXPECT_FALSE(result.Ok());
    EXPECT_NE(result.error_code, 0);
}
