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
use tokio::try_join;

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
        .build()?;

    let table_path = TablePath::new("fluss".to_owned(), "projection_test".to_owned());

    let admin = conn.get_admin().await?;

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    let table_info = admin.get_table(&table_path).await?;
    println!("Created table:\n{}\n", table_info);

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer();

    println!("Writing test data...");
    let mut row = GenericRow::new();
    row.set_field(0, 1);
    row.set_field(1, "Alice");
    row.set_field(2, 25);
    row.set_field(3, "alice@example.com");
    row.set_field(4, "123-456-7890");
    let f1 = append_writer.append(row);

    row = GenericRow::new();
    row.set_field(0, 2);
    row.set_field(1, "Bob");
    row.set_field(2, 30);
    row.set_field(3, "bob@example.com");
    row.set_field(4, "098-765-4321");
    let f2 = append_writer.append(row);

    try_join!(f1, f2, append_writer.flush())?;
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

    println!("=== Test 2: Project by column indices (id, name, age) ===");
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

    println!("=== Test 3: Project by column names (name, phone) ===");
    let log_scanner = table.new_scan()
        .project_by_name(&["name", "phone"])?
        .create_log_scanner();
    log_scanner.subscribe(0, 0).await?;
    
    let scan_records = log_scanner.poll(Duration::from_secs(5)).await?;
    println!("Fetched {} records with projection", scan_records.count());
    for record in scan_records {
        let row = record.row();
        println!(
            "Record @{}: name={}, phone={}",
            record.offset(),
            row.get_string(0),
            row.get_string(1)
        );
    }
    println!();

    println!("All tests completed successfully!");
    Ok(())
}

