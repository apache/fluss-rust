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
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;
use tokio::try_join;

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::parse();
    config.bootstrap_server = Some("127.0.0.1:9123".to_string());

    let conn = FlussConnection::new(config).await?;

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::string())
                .build()?,
        )
        .build()?;

    let table_path = TablePath::new("fluss".to_owned(), "rust_test".to_owned());

    let admin = conn.get_admin().await?;

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await?;

    // 2: get the table
    let table_info = admin.get_table(&table_path).await?;
    print!("Get created table:\n {table_info}\n");

    // write row
    let mut row = GenericRow::new();
    row.set_field(0, 22222);
    row.set_field(1, "t2t");

    let table = conn.get_table(&table_path).await?;
    let append_writer = table.new_append()?.create_writer();
    let f1 = append_writer.append(row);
    row = GenericRow::new();
    row.set_field(0, 233333);
    row.set_field(1, "tt44");
    let f2 = append_writer.append(row);
    try_join!(f1, f2, append_writer.flush())?;

    // scan rows
    let log_scanner = table.new_scan().create_log_scanner();
    log_scanner.subscribe(0, 0).await?;

    loop {
        let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;
        println!("Start to poll records......");
        for record in scan_records {
            let row = record.row();
            println!(
                "{{{}, {}}}@{}",
                row.get_int(0),
                row.get_string(1),
                record.offset()
            );
        }
    }
}
