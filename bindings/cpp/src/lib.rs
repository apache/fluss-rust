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

mod types;

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use fluss as fcore;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "fluss::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    struct FfiResult {
        error_code: i32,
        error_message: String,
    }

    struct FfiTablePath {
        database_name: String,
        table_name: String,
    }

    struct FfiColumn {
        name: String,
        data_type: i32,
        comment: String,
    }

    struct FfiSchema {
        columns: Vec<FfiColumn>,
        primary_keys: Vec<String>,
    }

    struct FfiTableDescriptor {
        schema: FfiSchema,
        partition_keys: Vec<String>,
        bucket_count: i32,
        bucket_keys: Vec<String>,
        properties: Vec<HashMapValue>,
        comment: String,
    }

    struct FfiTableInfo {
        table_id: i64,
        schema_id: i32,
        table_path: FfiTablePath,
        created_time: i64,
        modified_time: i64,
        primary_keys: Vec<String>,
        bucket_keys: Vec<String>,
        partition_keys: Vec<String>,
        num_buckets: i32,
        has_primary_key: bool,
        is_partitioned: bool,
        properties: Vec<HashMapValue>,
        comment: String,
        schema: FfiSchema,
    }

    struct FfiTableInfoResult {
        result: FfiResult,
        table_info: FfiTableInfo,
    }

    struct FfiDatum {
        datum_type: i32,
        bool_val: bool,
        i32_val: i32,
        i64_val: i64,
        f32_val: f32,
        f64_val: f64,
        string_val: String,
        bytes_val: Vec<u8>,
    }

    struct FfiGenericRow {
        fields: Vec<FfiDatum>,
    }

    struct FfiScanRecord {
        offset: i64,
        timestamp: i64,
        row: FfiGenericRow,
    }

    struct FfiScanRecords {
        records: Vec<FfiScanRecord>,
    }

    struct FfiScanRecordsResult {
        result: FfiResult,
        scan_records: FfiScanRecords,
    }

    struct FfiLakeSnapshot {
        snapshot_id: i64,
        bucket_offsets: Vec<FfiBucketOffset>,
    }

    struct FfiBucketOffset {
        table_id: i64,
        partition_id: i64,
        bucket_id: i32,
        offset: i64,
    }

    struct FfiLakeSnapshotResult {
        result: FfiResult,
        lake_snapshot: FfiLakeSnapshot,
    }

    extern "Rust" {
        type Connection;
        type Admin;
        type Table;
        type AppendWriter;
        type LogScanner;

        // Connection
        fn new_connection(bootstrap_server: &str) -> Result<*mut Connection>;
        unsafe fn delete_connection(conn: *mut Connection);
        fn get_admin(self: &Connection) -> Result<*mut Admin>;
        fn get_table(self: &Connection, table_path: &FfiTablePath) -> Result<*mut Table>;

        // Admin
        unsafe fn delete_admin(admin: *mut Admin);
        fn create_table(
            self: &Admin,
            table_path: &FfiTablePath,
            descriptor: &FfiTableDescriptor,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn get_table_info(self: &Admin, table_path: &FfiTablePath) -> FfiTableInfoResult;
        fn get_latest_lake_snapshot(self: &Admin, table_path: &FfiTablePath)
            -> FfiLakeSnapshotResult;

        // Table
        unsafe fn delete_table(table: *mut Table);
        fn new_append_writer(self: &Table) -> Result<*mut AppendWriter>;
        fn new_log_scanner(self: &Table) -> Result<*mut LogScanner>;
        fn new_log_scanner_with_projection(self: &Table, column_indices: Vec<usize>) -> Result<*mut LogScanner>;
        fn get_table_info_from_table(self: &Table) -> FfiTableInfo;
        fn get_table_path(self: &Table) -> FfiTablePath;
        fn has_primary_key(self: &Table) -> bool;

        // AppendWriter
        unsafe fn delete_append_writer(writer: *mut AppendWriter);
        fn append(self: &mut AppendWriter, row: &FfiGenericRow) -> FfiResult;
        fn flush(self: &mut AppendWriter) -> FfiResult;

        // LogScanner
        unsafe fn delete_log_scanner(scanner: *mut LogScanner);
        fn subscribe(self: &LogScanner, bucket_id: i32, start_offset: i64) -> FfiResult;
        fn poll(self: &LogScanner, timeout_ms: i64) -> FfiScanRecordsResult;
    }
}

pub struct Connection {
    inner: Arc<fcore::client::FlussConnection>,
    #[allow(dead_code)]
    metadata: Option<Arc<fcore::client::Metadata>>,
}

pub struct Admin {
    inner: fcore::client::FlussAdmin,
}

pub struct Table {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_pk: bool,
}

pub struct AppendWriter {
    inner: fcore::client::AppendWriter,
}

pub struct LogScanner {
    inner: fcore::client::LogScanner,
}

fn ok_result() -> ffi::FfiResult {
    ffi::FfiResult {
        error_code: 0,
        error_message: String::new(),
    }
}

fn err_result(code: i32, msg: String) -> ffi::FfiResult {
    ffi::FfiResult {
        error_code: code,
        error_message: msg,
    }
}

// Connection implementation
fn new_connection(bootstrap_server: &str) -> Result<*mut Connection, String> {
    let mut config = fcore::config::Config::default();
    config.bootstrap_server = Some(bootstrap_server.to_string());

    let conn = RUNTIME.block_on(async { fcore::client::FlussConnection::new(config).await });

    match conn {
        Ok(c) => {
            let conn = Box::into_raw(Box::new(Connection {
                inner: Arc::new(c),
                metadata: None,
            }));
            Ok(conn)
        }
        Err(e) => Err(format!("Failed to connect: {}", e)),
    }
}

unsafe fn delete_connection(conn: *mut Connection) {
    if !conn.is_null() {
        unsafe {
            drop(Box::from_raw(conn));
        }
    }
}

impl Connection {
    fn get_admin(&self) -> Result<*mut Admin, String> {
        let admin_result =
            RUNTIME.block_on(async { self.inner.get_admin().await });

        match admin_result {
            Ok(admin) => {
                let admin = Box::into_raw(Box::new(Admin { inner: admin }));
                Ok(admin)
            }
            Err(e) => Err(format!("Failed to get admin: {}", e)),
        }
    }

    fn get_table(&self, table_path: &ffi::FfiTablePath) -> Result<*mut Table, String> {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let table_result = RUNTIME.block_on(async { self.inner.get_table(&path).await });

        match table_result {
            Ok(t) => {
                let table = Box::into_raw(Box::new(Table {
                    connection: self.inner.clone(),
                    metadata: t.metadata().clone(),
                    table_info: t.table_info().clone(),
                    table_path: t.table_path().clone(),
                    has_pk: t.has_primary_key(),
                }));
                Ok(table)
            }
            Err(e) => Err(format!("Failed to get table: {}", e)),
        }
    }
}

// Admin implementation
unsafe fn delete_admin(admin: *mut Admin) {
    if !admin.is_null() {
        unsafe {
            drop(Box::from_raw(admin));
        }
    }
}

impl Admin {
    fn create_table(
        &self,
        table_path: &ffi::FfiTablePath,
        descriptor: &ffi::FfiTableDescriptor,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let core_descriptor = match types::ffi_descriptor_to_core(descriptor) {
            Ok(d) => d,
            Err(e) => return err_result(1, e.to_string()),
        };

        let result = RUNTIME.block_on(async {
            self.inner
                .create_table(&path, &core_descriptor, ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(2, e.to_string()),
        }
    }

    fn get_table_info(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiTableInfoResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.get_table(&path).await });

        match result {
            Ok(info) => ffi::FfiTableInfoResult {
                result: ok_result(),
                table_info: types::core_table_info_to_ffi(&info),
            },
            Err(e) => ffi::FfiTableInfoResult {
                result: err_result(1, e.to_string()),
                table_info: types::empty_table_info(),
            },
        }
    }

    fn get_latest_lake_snapshot(
        &self,
        table_path: &ffi::FfiTablePath,
    ) -> ffi::FfiLakeSnapshotResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.get_latest_lake_snapshot(&path).await });

        match result {
            Ok(snapshot) => ffi::FfiLakeSnapshotResult {
                result: ok_result(),
                lake_snapshot: types::core_lake_snapshot_to_ffi(&snapshot),
            },
            Err(e) => ffi::FfiLakeSnapshotResult {
                result: err_result(1, e.to_string()),
                lake_snapshot: ffi::FfiLakeSnapshot {
                    snapshot_id: -1,
                    bucket_offsets: vec![],
                },
            },
        }
    }
}

// Table implementation
unsafe fn delete_table(table: *mut Table) {
    if !table.is_null() {
        unsafe {
            drop(Box::from_raw(table));
        }
    }
}

impl Table {
    fn new_append_writer(&self) -> Result<*mut AppendWriter, String> {
        let _enter = RUNTIME.enter();
        
        let fluss_table =
            fcore::client::FlussTable::new(&self.connection, self.metadata.clone(), self.table_info.clone());

        let table_append = match fluss_table.new_append() {
            Ok(a) => a,
            Err(e) => {
                return Err(format!("Failed to create append: {}", e))
            }
        };

        let writer = table_append.create_writer();
        let writer = Box::into_raw(Box::new(AppendWriter { inner: writer }));
        Ok(writer)
    }

    fn new_log_scanner(&self) -> Result<*mut LogScanner, String> {
        let fluss_table =
            fcore::client::FlussTable::new(&self.connection, self.metadata.clone(), self.table_info.clone());

        let scanner = fluss_table.new_scan().create_log_scanner();
        let scanner = Box::into_raw(Box::new(LogScanner { inner: scanner }));
        Ok(scanner)
    }

    fn new_log_scanner_with_projection(&self, column_indices: Vec<usize>) -> Result<*mut LogScanner, String> {
        let fluss_table =
            fcore::client::FlussTable::new(&self.connection, self.metadata.clone(), self.table_info.clone());

        let scan = fluss_table.new_scan();
        let scan = match scan.project(&column_indices) {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to project columns: {}", e)),
        };
        let scanner = scan.create_log_scanner();
        let scanner = Box::into_raw(Box::new(LogScanner { inner: scanner }));
        Ok(scanner)
    }

    fn get_table_info_from_table(&self) -> ffi::FfiTableInfo {
        types::core_table_info_to_ffi(&self.table_info)
    }

    fn get_table_path(&self) -> ffi::FfiTablePath {
        ffi::FfiTablePath {
            database_name: self.table_path.database().to_string(),
            table_name: self.table_path.table().to_string(),
        }
    }

    fn has_primary_key(&self) -> bool {
        self.has_pk
    }
}

// AppendWriter implementation
unsafe fn delete_append_writer(writer: *mut AppendWriter) {
    if !writer.is_null() {
        unsafe {
            drop(Box::from_raw(writer));
        }
    }
}

impl AppendWriter {
    fn append(&mut self, row: &ffi::FfiGenericRow) -> ffi::FfiResult {
        let mut owner = types::OwnedRowData::new();
        owner.collect_strings(row);
        let generic_row = types::ffi_row_to_core(row, &owner);

        let result = RUNTIME.block_on(async { self.inner.append(generic_row).await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(1, e.to_string()),
        }
    }

    fn flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(1, e.to_string()),
        }
    }
}

// LogScanner implementation
unsafe fn delete_log_scanner(scanner: *mut LogScanner) {
    if !scanner.is_null() {
        unsafe {
            drop(Box::from_raw(scanner));
        }
    }
}

impl LogScanner {
    fn subscribe(&self, bucket_id: i32, start_offset: i64) -> ffi::FfiResult {
        let result =
            RUNTIME.block_on(async { self.inner.subscribe(bucket_id, start_offset).await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(1, e.to_string()),
        }
    }

    fn poll(&self, timeout_ms: i64) -> ffi::FfiScanRecordsResult {
        let timeout = Duration::from_millis(timeout_ms as u64);
        let result = RUNTIME.block_on(async { self.inner.poll(timeout).await });

        match result {
            Ok(records) => ffi::FfiScanRecordsResult {
                result: ok_result(),
                scan_records: types::core_scan_records_to_ffi(&records),
            },
            Err(e) => ffi::FfiScanRecordsResult {
                result: err_result(1, e.to_string()),
                scan_records: ffi::FfiScanRecords { records: vec![] },
            },
        }
    }
}
