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

use fluss::client::{FlussConnection, EARLIEST_OFFSET};
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::TablePath;
use fluss::row::InternalRow;
use std::time::Duration;
use fluss::rpc::message::OffsetSpec;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[tokio::main]
pub async fn main() -> Result<()> {
    println!("Starting Rust scan from earliest example...");
    println!("Using EARLIEST_OFFSET = {}", EARLIEST_OFFSET);

    let mut config = Config::default();
    config.bootstrap_server = Some("10.147.136.86:9123".to_string());

    println!("1) Connecting to Fluss...");
    let conn = FlussConnection::new(config).await?;
    println!("   Connected successfully!");

    let table_path = TablePath::new("fluss".to_owned(), "mahong_log_table_cpp_test_1212".to_owned());
    println!("2) Getting admin...");
    let admin = conn.get_admin().await?;
    println!("   Admin obtained successfully!");
    // Step 1: 通过 admin API 查询时间戳对应的 offset



    println!("3) Getting table: {}", table_path);
    let table = conn.get_table(&table_path).await?;
    println!("   Table obtained successfully!");

    let table_info = table.table_info();
    let num_buckets = table_info.num_buckets;
    println!("   Table has {} buckets", num_buckets);
    let now = SystemTime::now();
    let twenty_minutes_ago = now - Duration::from_secs(20 * 60);
    
    let timestamp_ms = twenty_minutes_ago
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    let offsets = admin.list_offsets(
        &table_path, 
        &(0..num_buckets).collect::<Vec<i32>>(), 
        OffsetSpec::Timestamp(timestamp_ms as i64)
    ).await?;

    // println!("4) Creating log scanner...");
    // let log_scanner = table.new_scan().create_log_scanner()?;
    // println!("   Log scanner created successfully!");

    // println!("5) Subscribing to all buckets from timestamp {}...", timestamp_ms);
    // for (bucket_id, offset) in offsets {
    //     log_scanner.subscribe(bucket_id, offset).await?;
    //     println!("   Subscribed to bucket {} from timestamp {} offset {}", bucket_id, timestamp_ms, offset);
    // }

    // println!("6) Polling records (timeout: 10 seconds)...");
    // let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;

    // let record_count = scan_records.count();
    // println!("Scanned records: {}", record_count);
    // for (i, record) in scan_records.into_iter().enumerate() {
    //     let row = record.row();
    //     if i < 10 {
    //         println!(
    //             " offset={} id={} number={} value={} ts={}",
    //             record.offset(),
    //             row.get_long(0),
    //             row.get_int(1),
    //             row.get_string(2),
    //             record.timestamp()
    //         );
    //     }
    // }

    // if record_count > 10 {
    //     println!(" ... and {} more records", record_count - 10);
    // }

    println!("\n7) Creating log scanner with projection (columns 0, 1)...");
    let projected_scanner = table.new_scan()
        .project(&[0, 2])?
        .create_log_scanner()?;
    println!("   Projected scanner created successfully!");

    println!("8) Subscribing projected scanner to all buckets from EARLIEST_OFFSET...");
    for bucket_id in 0..num_buckets {
        // projected_scanner.subscribe(bucket_id, EARLIEST_OFFSET).await?;
        projected_scanner.subscribe(bucket_id, 278807821).await?;
        println!("   Subscribed to bucket {} from earliest", bucket_id);
    }

    loop {
        println!("9) Polling projected records (timeout: 10 seconds)...");
        let projected_records = projected_scanner.poll(Duration::from_secs(10)).await?;

        let projected_count = projected_records.count();
        println!("Projected records: {}", projected_count);

        let mut projection_verified = true;
        for (i, record) in projected_records.into_iter().enumerate() {
            let row = record.row();
            let field_count = row.get_field_count();

            if field_count != 2 {
                eprintln!("ERROR: Record {} has {} fields, expected 2", i, field_count);
                projection_verified = false;
                continue;
            }

            if i < 10 {
                println!("  Record {}: id={}, name={}", i, row.get_long(0), row.get_string(1));
            }
        }

        if projected_count > 10 {
            println!("  ... and {} more records", projected_count - 10);
        }

        if projection_verified {
            println!("\nColumn pruning verification passed!");
        } else {
            eprintln!("\nColumn pruning verification failed!");
            std::process::exit(1);
        }

    }



}
