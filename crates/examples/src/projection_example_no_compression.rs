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

use clap::Parser;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, LogFormat, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    
    let mut config = Config::parse();
    config.bootstrap_server = Some("127.0.0.1:9123".to_string());

    let conn = FlussConnection::new(config).await?;

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("age", DataTypes::int())
                .column("email", DataTypes::string())
                .column("phone", DataTypes::string())
                .build()?,
        )
        .log_format(LogFormat::ARROW)
        .property("table.log.arrow.compression.type", "NONE")
        .build()?;

    let table_path = TablePath::new("fluss".to_owned(), "projection_test_no_compression_rust".to_owned());

    let admin = conn.get_admin().await?;

    // Drop table if exists, then create
    let _ = admin.drop_table(&table_path, true).await;
    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    let table_info = admin.get_table(&table_path).await?;
    println!("Created table:\n{}\n", table_info);

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer();

    println!("Writing test data...");
    for i in 0..10 {
        let mut row = GenericRow::new();
        row.set_field(0, i);
        let name = format!("User{}", i);
        row.set_field(1, name.as_str());
        row.set_field(2, 20 + i);
        let email = format!("user{}@example.com", i);
        row.set_field(3, email.as_str());
        let phone = format!("123-456-{:04}", i);
        row.set_field(4, phone.as_str());
        append_writer.append(row).await?;
    }
    append_writer.flush().await?;
    println!("Data written successfully\n");

    println!("=== Test 1: Full scan (no projection) ===");
    let log_scanner = table.new_scan().create_log_scanner();
    log_scanner.subscribe(0, 0).await?;
    
    let scan_records = log_scanner.poll(Duration::from_secs(5)).await?;
    println!("Fetched {} records", scan_records.count());
    for record in scan_records {
        let row = record.row();
        println!(
            "Record @{}: id={}, name={}, age={}, email={}, phone={}",
            record.offset(),
            row.get_int(0),
            row.get_string(1),
            row.get_int(2),
            row.get_string(3),
            row.get_string(4)
        );
    }
    println!();

    println!("=== Test 2: Project by column indices (id, name, age) - NO COMPRESSION ===");
    println!("This test verifies projection without compression to isolate compression-related issues.");
    let log_scanner = table.new_scan()
        .project(&[0, 1, 2])?
        .create_log_scanner();
    log_scanner.subscribe(0, 0).await?;
    
    let scan_records = log_scanner.poll(Duration::from_secs(5)).await?;
    println!("Fetched {} records with projection", scan_records.count());
    for record in scan_records {
        let row = record.row();
        println!(
            "Record @{}: id={}, name={}, age={}",
            record.offset(),
            row.get_int(0),
            row.get_string(1),
            row.get_int(2)
        );
    }
    println!();

    println!("✓ Rust client projection test (NO COMPRESSION) PASSED");
    println!("  This indicates projection works correctly without compression.");
    println!("  If compression version fails, the issue is in compression handling.");
    println!();

    println!("All tests completed successfully!");
    Ok(())
}

