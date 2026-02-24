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
use fluss::PartitionId;

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

    struct FfiConfig {
        bootstrap_servers: String,
        writer_request_max_size: i32,
        writer_acks: String,
        writer_retries: i32,
        writer_batch_size: i32,
        scanner_remote_log_prefetch_num: usize,
        remote_file_download_thread_num: usize,
        scanner_log_max_poll_records: usize,
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
        precision: i32,
        scale: i32,
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
        custom_properties: Vec<HashMapValue>,
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
        custom_properties: Vec<HashMapValue>,
        comment: String,
        schema: FfiSchema,
    }

    struct FfiTableInfoResult {
        result: FfiResult,
        table_info: FfiTableInfo,
    }

    // NOTE: FfiDatum, FfiGenericRow, FfiScanRecord, FfiScanRecords, FfiScanRecordsResult
    // have been replaced by opaque types below (ScanResultInner, GenericRowInner, LookupResultInner).

    struct FfiArrowRecordBatch {
        array_ptr: usize,
        schema_ptr: usize,
        table_id: i64,
        partition_id: i64,
        bucket_id: i32,
        base_offset: i64,
    }

    struct FfiArrowRecordBatches {
        batches: Vec<FfiArrowRecordBatch>,
    }

    struct FfiArrowRecordBatchesResult {
        result: FfiResult,
        arrow_batches: FfiArrowRecordBatches,
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

    struct FfiOffsetQuery {
        offset_type: i32,
        timestamp: i64,
    }

    struct FfiBucketInfo {
        table_id: i64,
        bucket_id: i32,
        has_partition_id: bool,
        partition_id: i64,
        record_count: usize,
    }

    struct FfiBucketSubscription {
        bucket_id: i32,
        offset: i64,
    }

    struct FfiPartitionBucketSubscription {
        partition_id: i64,
        bucket_id: i32,
        offset: i64,
    }

    struct FfiBucketOffsetPair {
        bucket_id: i32,
        offset: i64,
    }

    struct FfiListOffsetsResult {
        result: FfiResult,
        bucket_offsets: Vec<FfiBucketOffsetPair>,
    }

    // NOTE: FfiLookupResult replaced by opaque LookupResultInner below.

    struct FfiLakeSnapshotResult {
        result: FfiResult,
        lake_snapshot: FfiLakeSnapshot,
    }

    struct FfiPartitionKeyValue {
        key: String,
        value: String,
    }

    struct FfiPartitionInfo {
        partition_id: i64,
        partition_name: String,
    }

    struct FfiListPartitionInfosResult {
        result: FfiResult,
        partition_infos: Vec<FfiPartitionInfo>,
    }

    struct FfiDatabaseDescriptor {
        comment: String,
        properties: Vec<HashMapValue>,
    }

    struct FfiDatabaseInfo {
        database_name: String,
        comment: String,
        properties: Vec<HashMapValue>,
        created_time: i64,
        modified_time: i64,
    }

    struct FfiDatabaseInfoResult {
        result: FfiResult,
        database_info: FfiDatabaseInfo,
    }

    struct FfiListDatabasesResult {
        result: FfiResult,
        database_names: Vec<String>,
    }

    struct FfiListTablesResult {
        result: FfiResult,
        table_names: Vec<String>,
    }

    struct FfiBoolResult {
        result: FfiResult,
        value: bool,
    }

    extern "Rust" {
        type Connection;
        type Admin;
        type Table;
        type AppendWriter;
        type WriteResult;
        type LogScanner;
        type UpsertWriter;
        type Lookuper;

        // Opaque types for optimized FFI
        type ScanResultInner;
        type GenericRowInner;
        type LookupResultInner;

        // Connection
        fn new_connection(config: &FfiConfig) -> Result<*mut Connection>;
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
        fn drop_table(
            self: &Admin,
            table_path: &FfiTablePath,
            ignore_if_not_exists: bool,
        ) -> FfiResult;
        fn get_table_info(self: &Admin, table_path: &FfiTablePath) -> FfiTableInfoResult;
        fn get_latest_lake_snapshot(
            self: &Admin,
            table_path: &FfiTablePath,
        ) -> FfiLakeSnapshotResult;
        fn list_offsets(
            self: &Admin,
            table_path: &FfiTablePath,
            bucket_ids: Vec<i32>,
            offset_query: &FfiOffsetQuery,
        ) -> FfiListOffsetsResult;
        fn list_partition_offsets(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_name: String,
            bucket_ids: Vec<i32>,
            offset_query: &FfiOffsetQuery,
        ) -> FfiListOffsetsResult;
        fn list_partition_infos(
            self: &Admin,
            table_path: &FfiTablePath,
        ) -> FfiListPartitionInfosResult;
        fn list_partition_infos_with_spec(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
        ) -> FfiListPartitionInfosResult;
        fn create_partition(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn drop_partition(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
            ignore_if_not_exists: bool,
        ) -> FfiResult;
        fn create_database(
            self: &Admin,
            database_name: &str,
            descriptor: &FfiDatabaseDescriptor,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn drop_database(
            self: &Admin,
            database_name: &str,
            ignore_if_not_exists: bool,
            cascade: bool,
        ) -> FfiResult;
        fn list_databases(self: &Admin) -> FfiListDatabasesResult;
        fn database_exists(self: &Admin, database_name: &str) -> FfiBoolResult;
        fn get_database_info(self: &Admin, database_name: &str) -> FfiDatabaseInfoResult;
        fn list_tables(self: &Admin, database_name: &str) -> FfiListTablesResult;
        fn table_exists(self: &Admin, table_path: &FfiTablePath) -> FfiBoolResult;

        // Table
        unsafe fn delete_table(table: *mut Table);
        fn new_append_writer(self: &Table) -> Result<*mut AppendWriter>;
        fn create_scanner(
            self: &Table,
            column_indices: Vec<usize>,
            batch: bool,
        ) -> Result<*mut LogScanner>;
        fn get_table_info_from_table(self: &Table) -> FfiTableInfo;
        fn get_table_path(self: &Table) -> FfiTablePath;
        fn has_primary_key(self: &Table) -> bool;
        fn create_upsert_writer(
            self: &Table,
            column_indices: Vec<usize>,
        ) -> Result<*mut UpsertWriter>;
        fn new_lookuper(self: &Table) -> Result<*mut Lookuper>;

        // GenericRowInner — opaque row for writes
        fn new_generic_row(field_count: usize) -> Box<GenericRowInner>;
        fn gr_reset(self: &mut GenericRowInner);
        fn gr_set_null(self: &mut GenericRowInner, idx: usize);
        fn gr_set_bool(self: &mut GenericRowInner, idx: usize, val: bool);
        fn gr_set_i32(self: &mut GenericRowInner, idx: usize, val: i32);
        fn gr_set_i64(self: &mut GenericRowInner, idx: usize, val: i64);
        fn gr_set_f32(self: &mut GenericRowInner, idx: usize, val: f32);
        fn gr_set_f64(self: &mut GenericRowInner, idx: usize, val: f64);
        fn gr_set_str(self: &mut GenericRowInner, idx: usize, val: &str);
        fn gr_set_bytes(self: &mut GenericRowInner, idx: usize, val: &[u8]);
        fn gr_set_date(self: &mut GenericRowInner, idx: usize, days: i32);
        fn gr_set_time(self: &mut GenericRowInner, idx: usize, millis: i32);
        fn gr_set_ts_ntz(self: &mut GenericRowInner, idx: usize, millis: i64, nanos: i32);
        fn gr_set_ts_ltz(self: &mut GenericRowInner, idx: usize, millis: i64, nanos: i32);
        fn gr_set_decimal_str(self: &mut GenericRowInner, idx: usize, val: &str);

        // AppendWriter
        unsafe fn delete_append_writer(writer: *mut AppendWriter);
        fn append(self: &mut AppendWriter, row: &GenericRowInner) -> Result<Box<WriteResult>>;
        fn append_arrow_batch(
            self: &mut AppendWriter,
            array_ptr: usize,
            schema_ptr: usize,
        ) -> Result<Box<WriteResult>>;
        fn flush(self: &mut AppendWriter) -> FfiResult;

        // WriteResult — dropped automatically via rust::Box, or call wait() for ack
        fn wait(self: &mut WriteResult) -> FfiResult;

        // UpsertWriter
        unsafe fn delete_upsert_writer(writer: *mut UpsertWriter);
        fn upsert(self: &mut UpsertWriter, row: &GenericRowInner) -> Result<Box<WriteResult>>;
        fn delete_row(self: &mut UpsertWriter, row: &GenericRowInner) -> Result<Box<WriteResult>>;
        fn upsert_flush(self: &mut UpsertWriter) -> FfiResult;

        // Lookuper
        unsafe fn delete_lookuper(lookuper: *mut Lookuper);
        fn lookup(self: &mut Lookuper, pk_row: &GenericRowInner) -> Box<LookupResultInner>;

        // LookupResultInner accessors
        fn lv_has_error(self: &LookupResultInner) -> bool;
        fn lv_error_code(self: &LookupResultInner) -> i32;
        fn lv_error_message(self: &LookupResultInner) -> &str;
        fn lv_found(self: &LookupResultInner) -> bool;
        fn lv_field_count(self: &LookupResultInner) -> usize;
        fn lv_column_name(self: &LookupResultInner, field: usize) -> Result<&str>;
        fn lv_column_type(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_is_null(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_bool(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_i32(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_i64(self: &LookupResultInner, field: usize) -> Result<i64>;
        fn lv_get_f32(self: &LookupResultInner, field: usize) -> Result<f32>;
        fn lv_get_f64(self: &LookupResultInner, field: usize) -> Result<f64>;
        fn lv_get_str(self: &LookupResultInner, field: usize) -> Result<&str>;
        fn lv_get_bytes(self: &LookupResultInner, field: usize) -> Result<&[u8]>;
        fn lv_get_date_days(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_time_millis(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_ts_millis(self: &LookupResultInner, field: usize) -> Result<i64>;
        fn lv_get_ts_nanos(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_is_ts_ltz(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_decimal_str(self: &LookupResultInner, field: usize) -> Result<String>;

        // LogScanner
        unsafe fn delete_log_scanner(scanner: *mut LogScanner);
        fn subscribe(self: &LogScanner, bucket_id: i32, start_offset: i64) -> FfiResult;
        fn subscribe_buckets(
            self: &LogScanner,
            subscriptions: Vec<FfiBucketSubscription>,
        ) -> FfiResult;
        fn subscribe_partition(
            self: &LogScanner,
            partition_id: i64,
            bucket_id: i32,
            start_offset: i64,
        ) -> FfiResult;
        fn subscribe_partition_buckets(
            self: &LogScanner,
            subscriptions: Vec<FfiPartitionBucketSubscription>,
        ) -> FfiResult;
        fn unsubscribe(self: &LogScanner, bucket_id: i32) -> FfiResult;
        fn unsubscribe_partition(self: &LogScanner, partition_id: i64, bucket_id: i32)
        -> FfiResult;
        fn poll(self: &LogScanner, timeout_ms: i64) -> Box<ScanResultInner>;
        fn poll_record_batch(self: &LogScanner, timeout_ms: i64) -> FfiArrowRecordBatchesResult;
        fn free_arrow_ffi_structures(array_ptr: usize, schema_ptr: usize);

        // ScanResultInner accessors
        fn sv_has_error(self: &ScanResultInner) -> bool;
        fn sv_error_code(self: &ScanResultInner) -> i32;
        fn sv_error_message(self: &ScanResultInner) -> &str;
        fn sv_record_count(self: &ScanResultInner) -> usize;
        fn sv_column_count(self: &ScanResultInner) -> usize;
        fn sv_column_name(self: &ScanResultInner, field: usize) -> Result<&str>;
        fn sv_column_type(self: &ScanResultInner, field: usize) -> Result<i32>;
        fn sv_offset(self: &ScanResultInner, bucket: usize, rec: usize) -> i64;
        fn sv_timestamp(self: &ScanResultInner, bucket: usize, rec: usize) -> i64;
        fn sv_change_type(self: &ScanResultInner, bucket: usize, rec: usize) -> i32;
        fn sv_field_count(self: &ScanResultInner) -> usize;
        fn sv_is_null(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_bool(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_i32(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_i64(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i64>;
        fn sv_get_f32(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<f32>;
        fn sv_get_f64(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<f64>;
        fn sv_get_str(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<&str>;
        fn sv_get_bytes(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<&[u8]>;
        fn sv_get_date_days(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_time_millis(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_ts_millis(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i64>;
        fn sv_get_ts_nanos(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_is_ts_ltz(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_decimal_str(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<String>;

        fn sv_bucket_infos(self: &ScanResultInner) -> &Vec<FfiBucketInfo>;
    }
}

pub struct Connection {
    inner: Arc<fcore::client::FlussConnection>,
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
    table_info: fcore::metadata::TableInfo,
}

pub struct WriteResult {
    inner: Option<fcore::client::WriteResultFuture>,
}

enum ScannerKind {
    Record(fcore::client::LogScanner),
    Batch(fcore::client::RecordBatchLogScanner),
}

pub struct LogScanner {
    scanner: ScannerKind,
    /// Fluss columns matching the projected Arrow fields (1:1 by index).
    /// For non-projected scanners this is the full table schema columns.
    projected_columns: Vec<fcore::metadata::Column>,
}

pub struct UpsertWriter {
    inner: fcore::client::UpsertWriter,
    table_info: fcore::metadata::TableInfo,
}

pub struct Lookuper {
    inner: fcore::client::Lookuper,
    table_info: fcore::metadata::TableInfo,
}

/// Error code for client-side errors that did not originate from the server API protocol.
/// Must be non-zero so that CPP `Result::Ok()` (which checks `error_code == 0`) correctly
/// detects client-side errors as failures. The value -2 is outside the server API error
/// code range (-1 .. 57+), so it will never collide with current or future API codes.
const CLIENT_ERROR_CODE: i32 = -2;

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

/// Create a client-side error result (not from server API).
fn client_err(msg: String) -> ffi::FfiResult {
    err_result(CLIENT_ERROR_CODE, msg)
}

/// Convert a core Error to FfiResult.
/// `FlussAPIError` variants carry the server protocol error code directly.
/// All other error kinds are client-side and use CLIENT_ERROR_CODE.
fn err_from_core_error(e: &fcore::error::Error) -> ffi::FfiResult {
    use fcore::error::Error;
    match e {
        Error::FlussAPIError { api_error } => err_result(api_error.code, api_error.message.clone()),
        _ => client_err(e.to_string()),
    }
}

// Connection implementation
fn new_connection(config: &ffi::FfiConfig) -> Result<*mut Connection, String> {
    let config = fluss::config::Config {
        bootstrap_servers: config.bootstrap_servers.to_string(),
        writer_request_max_size: config.writer_request_max_size,
        writer_acks: config.writer_acks.to_string(),
        writer_retries: config.writer_retries,
        writer_batch_size: config.writer_batch_size,
        writer_batch_timeout_ms: 100,
        scanner_remote_log_prefetch_num: config.scanner_remote_log_prefetch_num,
        remote_file_download_thread_num: config.remote_file_download_thread_num,
        scanner_log_max_poll_records: config.scanner_log_max_poll_records,
    };

    let conn = RUNTIME.block_on(async { fcore::client::FlussConnection::new(config).await });

    match conn {
        Ok(c) => {
            let conn = Box::into_raw(Box::new(Connection { inner: Arc::new(c) }));
            Ok(conn)
        }
        Err(e) => Err(format!("Failed to connect: {e}")),
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
        let admin_result = RUNTIME.block_on(async { self.inner.get_admin().await });

        match admin_result {
            Ok(admin) => {
                let admin = Box::into_raw(Box::new(Admin { inner: admin }));
                Ok(admin)
            }
            Err(e) => Err(format!("Failed to get admin: {e}")),
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
                    table_info: t.get_table_info().clone(),
                    table_path: t.table_path().clone(),
                    has_pk: t.has_primary_key(),
                }));
                Ok(table)
            }
            Err(e) => Err(format!("Failed to get table: {e}")),
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
            Err(e) => return client_err(e.to_string()),
        };

        let result = RUNTIME.block_on(async {
            self.inner
                .create_table(&path, &core_descriptor, ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_table(
        &self,
        table_path: &ffi::FfiTablePath,
        ignore_if_not_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result =
            RUNTIME.block_on(async { self.inner.drop_table(&path, ignore_if_not_exists).await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn get_table_info(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiTableInfoResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.get_table_info(&path).await });

        match result {
            Ok(info) => ffi::FfiTableInfoResult {
                result: ok_result(),
                table_info: types::core_table_info_to_ffi(&info),
            },
            Err(e) => ffi::FfiTableInfoResult {
                result: err_from_core_error(&e),
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
                result: err_from_core_error(&e),
                lake_snapshot: ffi::FfiLakeSnapshot {
                    snapshot_id: -1,
                    bucket_offsets: vec![],
                },
            },
        }
    }

    // Helper function for common list offsets functionality
    fn do_list_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_name: Option<&str>,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        use fcore::rpc::message::OffsetSpec;

        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let offset_spec = match offset_query.offset_type {
            0 => OffsetSpec::Earliest,
            1 => OffsetSpec::Latest,
            2 => OffsetSpec::Timestamp(offset_query.timestamp),
            _ => {
                return ffi::FfiListOffsetsResult {
                    result: client_err(format!(
                        "Invalid offset_type: {}",
                        offset_query.offset_type
                    )),
                    bucket_offsets: vec![],
                };
            }
        };

        let result = RUNTIME.block_on(async {
            if let Some(part_name) = partition_name {
                self.inner
                    .list_partition_offsets(&path, part_name, &bucket_ids, offset_spec)
                    .await
            } else {
                self.inner
                    .list_offsets(&path, &bucket_ids, offset_spec)
                    .await
            }
        });

        match result {
            Ok(offsets) => {
                let bucket_offsets: Vec<ffi::FfiBucketOffsetPair> = offsets
                    .into_iter()
                    .map(|(bucket_id, offset)| ffi::FfiBucketOffsetPair { bucket_id, offset })
                    .collect();
                ffi::FfiListOffsetsResult {
                    result: ok_result(),
                    bucket_offsets,
                }
            }
            Err(e) => ffi::FfiListOffsetsResult {
                result: err_from_core_error(&e),
                bucket_offsets: vec![],
            },
        }
    }

    fn list_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        self.do_list_offsets(table_path, None, bucket_ids, offset_query)
    }

    fn list_partition_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_name: String,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        self.do_list_offsets(table_path, Some(&partition_name), bucket_ids, offset_query)
    }

    fn list_partition_infos(
        &self,
        table_path: &ffi::FfiTablePath,
    ) -> ffi::FfiListPartitionInfosResult {
        self.do_list_partition_infos(table_path, None)
    }

    fn list_partition_infos_with_spec(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
    ) -> ffi::FfiListPartitionInfosResult {
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let spec = fcore::metadata::PartitionSpec::new(spec_map);
        self.do_list_partition_infos(table_path, Some(&spec))
    }
    fn create_partition(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let partition_spec = fcore::metadata::PartitionSpec::new(spec_map);

        let result = RUNTIME.block_on(async {
            self.inner
                .create_partition(&path, &partition_spec, ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_partition(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
        ignore_if_not_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let partition_spec = fcore::metadata::PartitionSpec::new(spec_map);

        let result = RUNTIME.block_on(async {
            self.inner
                .drop_partition(&path, &partition_spec, ignore_if_not_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn create_database(
        &self,
        database_name: &str,
        descriptor: &ffi::FfiDatabaseDescriptor,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let descriptor_opt = types::ffi_database_descriptor_to_core(descriptor);

        let result = RUNTIME.block_on(async {
            self.inner
                .create_database(database_name, descriptor_opt.as_ref(), ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_database(
        &self,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async {
            self.inner
                .drop_database(database_name, ignore_if_not_exists, cascade)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn list_databases(&self) -> ffi::FfiListDatabasesResult {
        let result = RUNTIME.block_on(async { self.inner.list_databases().await });

        match result {
            Ok(names) => ffi::FfiListDatabasesResult {
                result: ok_result(),
                database_names: names,
            },
            Err(e) => ffi::FfiListDatabasesResult {
                result: err_from_core_error(&e),
                database_names: vec![],
            },
        }
    }

    fn database_exists(&self, database_name: &str) -> ffi::FfiBoolResult {
        let result = RUNTIME.block_on(async { self.inner.database_exists(database_name).await });

        match result {
            Ok(exists) => ffi::FfiBoolResult {
                result: ok_result(),
                value: exists,
            },
            Err(e) => ffi::FfiBoolResult {
                result: err_from_core_error(&e),
                value: false,
            },
        }
    }

    fn get_database_info(&self, database_name: &str) -> ffi::FfiDatabaseInfoResult {
        let result = RUNTIME.block_on(async { self.inner.get_database_info(database_name).await });

        match result {
            Ok(info) => ffi::FfiDatabaseInfoResult {
                result: ok_result(),
                database_info: types::core_database_info_to_ffi(&info),
            },
            Err(e) => ffi::FfiDatabaseInfoResult {
                result: err_from_core_error(&e),
                database_info: ffi::FfiDatabaseInfo {
                    database_name: String::new(),
                    comment: String::new(),
                    properties: vec![],
                    created_time: 0,
                    modified_time: 0,
                },
            },
        }
    }

    fn list_tables(&self, database_name: &str) -> ffi::FfiListTablesResult {
        let result = RUNTIME.block_on(async { self.inner.list_tables(database_name).await });

        match result {
            Ok(names) => ffi::FfiListTablesResult {
                result: ok_result(),
                table_names: names,
            },
            Err(e) => ffi::FfiListTablesResult {
                result: err_from_core_error(&e),
                table_names: vec![],
            },
        }
    }

    fn table_exists(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiBoolResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.table_exists(&path).await });

        match result {
            Ok(exists) => ffi::FfiBoolResult {
                result: ok_result(),
                value: exists,
            },
            Err(e) => ffi::FfiBoolResult {
                result: err_from_core_error(&e),
                value: false,
            },
        }
    }

    fn do_list_partition_infos(
        &self,
        table_path: &ffi::FfiTablePath,
        partial_partition_spec: Option<&fcore::metadata::PartitionSpec>,
    ) -> ffi::FfiListPartitionInfosResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let result = RUNTIME.block_on(async {
            self.inner
                .list_partition_infos_with_spec(&path, partial_partition_spec)
                .await
        });
        match result {
            Ok(infos) => {
                let partition_infos: Vec<ffi::FfiPartitionInfo> = infos
                    .into_iter()
                    .map(|info| ffi::FfiPartitionInfo {
                        partition_id: info.get_partition_id(),
                        partition_name: info.get_partition_name(),
                    })
                    .collect();
                ffi::FfiListPartitionInfosResult {
                    result: ok_result(),
                    partition_infos,
                }
            }
            Err(e) => ffi::FfiListPartitionInfosResult {
                result: err_from_core_error(&e),
                partition_infos: vec![],
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
    fn fluss_table(&self) -> fcore::client::FlussTable<'_> {
        fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        )
    }

    fn resolve_projected_columns(
        &self,
        indices: &[usize],
    ) -> Result<Vec<fcore::metadata::Column>, String> {
        let all_columns = self.table_info.get_schema().columns();
        indices
            .iter()
            .map(|&i| {
                all_columns.get(i).cloned().ok_or_else(|| {
                    format!(
                        "Invalid column index {i}: schema has {} columns",
                        all_columns.len()
                    )
                })
            })
            .collect()
    }

    fn new_append_writer(&self) -> Result<*mut AppendWriter, String> {
        let _enter = RUNTIME.enter();

        let table_append = self
            .fluss_table()
            .new_append()
            .map_err(|e| format!("Failed to create append: {e}"))?;

        let writer = table_append
            .create_writer()
            .map_err(|e| format!("Failed to create writer: {e}"))?;

        Ok(Box::into_raw(Box::new(AppendWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        })))
    }

    fn create_scanner(
        &self,
        column_indices: Vec<usize>,
        batch: bool,
    ) -> Result<*mut LogScanner, String> {
        RUNTIME.block_on(async {
            let fluss_table = self.fluss_table();
            let scan = fluss_table.new_scan();

            let (projected_columns, scan) = if column_indices.is_empty() {
                (self.table_info.get_schema().columns().to_vec(), scan)
            } else {
                let cols = self.resolve_projected_columns(&column_indices)?;
                let scan = scan
                    .project(&column_indices)
                    .map_err(|e| format!("Failed to project columns: {e}"))?;
                (cols, scan)
            };

            let scanner = if batch {
                let s = scan
                    .create_record_batch_log_scanner()
                    .map_err(|e| format!("Failed to create record batch log scanner: {e}"))?;
                ScannerKind::Batch(s)
            } else {
                let s = scan
                    .create_log_scanner()
                    .map_err(|e| format!("Failed to create log scanner: {e}"))?;
                ScannerKind::Record(s)
            };

            Ok(Box::into_raw(Box::new(LogScanner {
                scanner,
                projected_columns,
            })))
        })
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

    fn create_upsert_writer(
        &self,
        column_indices: Vec<usize>,
    ) -> Result<*mut UpsertWriter, String> {
        let _enter = RUNTIME.enter();

        let table_upsert = self
            .fluss_table()
            .new_upsert()
            .map_err(|e| format!("Failed to create upsert: {e}"))?;

        let table_upsert = if column_indices.is_empty() {
            table_upsert
        } else {
            table_upsert
                .partial_update(Some(column_indices))
                .map_err(|e| format!("Failed to set partial update columns: {e}"))?
        };

        let writer = table_upsert
            .create_writer()
            .map_err(|e| format!("Failed to create upsert writer: {e}"))?;

        Ok(Box::into_raw(Box::new(UpsertWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        })))
    }

    fn new_lookuper(&self) -> Result<*mut Lookuper, String> {
        let _enter = RUNTIME.enter();

        let table_lookup = self
            .fluss_table()
            .new_lookup()
            .map_err(|e| format!("Failed to create lookup: {e}"))?;

        let lookuper = table_lookup
            .create_lookuper()
            .map_err(|e| format!("Failed to create lookuper: {e}"))?;

        Ok(Box::into_raw(Box::new(Lookuper {
            inner: lookuper,
            table_info: self.table_info.clone(),
        })))
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
    fn append(&mut self, row: &GenericRowInner) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row =
            types::resolve_row_types(&row.row, Some(schema)).map_err(|e| e.to_string())?;

        let result_future = self
            .inner
            .append(&generic_row)
            .map_err(|e| format!("Failed to append: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn append_arrow_batch(
        &mut self,
        array_ptr: usize,
        schema_ptr: usize,
    ) -> Result<Box<WriteResult>, String> {
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

        // Safety: C++ allocates these via `new ArrowArray/ArrowSchema` after a
        // successful `ExportRecordBatch`, so both pointers are valid heap
        // allocations that we take ownership of here.
        let ffi_array = unsafe { *Box::from_raw(array_ptr as *mut FFI_ArrowArray) };
        let ffi_schema = unsafe { Box::from_raw(schema_ptr as *mut FFI_ArrowSchema) };

        // Safety: `from_ffi` requires that the array and schema conform to the
        // Arrow C Data Interface, which is guaranteed by C++'s ExportRecordBatch.
        let array_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }
            .map_err(|e| format!("Failed to import Arrow batch: {e}"))?;
        // ffi_array is consumed by from_ffi; ffi_schema is dropped here (Box goes out of scope)

        // Reconstruct RecordBatch from the imported StructArray data
        let struct_array = arrow::array::StructArray::from(array_data);
        let batch = arrow::record_batch::RecordBatch::from(struct_array);

        let result_future = self
            .inner
            .append_arrow_batch(batch)
            .map_err(|e| format!("Failed to append Arrow batch: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }
}

impl WriteResult {
    fn wait(&mut self) -> ffi::FfiResult {
        if let Some(future) = self.inner.take() {
            let result = RUNTIME.block_on(future);
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_from_core_error(&e),
            }
        } else {
            client_err("WriteResult already consumed".to_string())
        }
    }
}

// UpsertWriter implementation
unsafe fn delete_upsert_writer(writer: *mut UpsertWriter) {
    if !writer.is_null() {
        unsafe {
            drop(Box::from_raw(writer));
        }
    }
}

impl UpsertWriter {
    /// Pad row with Null to full schema width.
    /// This allows callers to only set the fields they care about.
    fn pad_row<'a>(&self, mut row: fcore::row::GenericRow<'a>) -> fcore::row::GenericRow<'a> {
        let num_columns = self.table_info.get_schema().columns().len();
        if row.values.len() < num_columns {
            row.values.resize(num_columns, fcore::row::Datum::Null);
        }
        row
    }

    fn upsert(&mut self, row: &GenericRowInner) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row =
            types::resolve_row_types(&row.row, Some(schema)).map_err(|e| e.to_string())?;
        let generic_row = self.pad_row(generic_row);

        let result_future = self
            .inner
            .upsert(&generic_row)
            .map_err(|e| format!("Failed to upsert: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn delete_row(&mut self, row: &GenericRowInner) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row =
            types::resolve_row_types(&row.row, Some(schema)).map_err(|e| e.to_string())?;
        let generic_row = self.pad_row(generic_row);

        let result_future = self
            .inner
            .delete(&generic_row)
            .map_err(|e| format!("Failed to delete: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn upsert_flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }
}

// Lookuper implementation
unsafe fn delete_lookuper(lookuper: *mut Lookuper) {
    if !lookuper.is_null() {
        unsafe {
            drop(Box::from_raw(lookuper));
        }
    }
}

impl Lookuper {
    /// Build a dense PK-only row from a (possibly sparse) input row.
    /// The user may set PK values at their full schema positions (e.g. [0, 2])
    /// via name-based Set(). We compact them into [0, 1, …] to match
    /// the lookup_row_type the core KeyEncoder expects.
    fn dense_pk_row<'a>(&self, mut row: fcore::row::GenericRow<'a>) -> fcore::row::GenericRow<'a> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let mut dense = fcore::row::GenericRow::new(pk_indices.len());
        for (dense_idx, &schema_idx) in pk_indices.iter().enumerate() {
            if schema_idx < row.values.len() {
                dense.values[dense_idx] =
                    std::mem::replace(&mut row.values[schema_idx], fcore::row::Datum::Null);
            }
        }
        dense
    }

    fn lookup(&mut self, pk_row: &GenericRowInner) -> Box<LookupResultInner> {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&pk_row.row, Some(schema)) {
            Ok(r) => self.dense_pk_row(r),
            Err(e) => {
                return Box::new(LookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                ));
            }
        };

        let lookup_result = match RUNTIME.block_on(self.inner.lookup(&generic_row)) {
            Ok(r) => r,
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                return Box::new(LookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ));
            }
        };

        let columns = self.table_info.get_schema().columns().to_vec();
        match lookup_result.get_single_row() {
            Ok(Some(row)) => match types::compacted_row_to_owned(&row, &self.table_info) {
                Ok(owned_row) => Box::new(LookupResultInner {
                    error: None,
                    found: true,
                    row: Some(owned_row),
                    columns,
                }),
                Err(e) => Box::new(LookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                )),
            },
            Ok(None) => Box::new(LookupResultInner {
                error: None,
                found: false,
                row: None,
                columns,
            }),
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                Box::new(LookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ))
            }
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

// Helper function to free the Arrow FFI structures separately (for use after ImportRecordBatch)
pub extern "C" fn free_arrow_ffi_structures(array_ptr: usize, schema_ptr: usize) {
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    if array_ptr != 0 {
        let _array = unsafe { Box::from_raw(array_ptr as *mut FFI_ArrowArray) };
    }
    if schema_ptr != 0 {
        let _schema = unsafe { Box::from_raw(schema_ptr as *mut FFI_ArrowSchema) };
    }
}

/// Dispatch a method call to whichever scanner variant is active.
/// Both LogScanner and RecordBatchLogScanner share the same subscribe/unsubscribe interface.
macro_rules! dispatch_scanner {
    ($self:expr, $method:ident($($arg:expr),*)) => {
        match RUNTIME.block_on(async {
            match &$self.scanner {
                ScannerKind::Record(s) => s.$method($($arg),*).await,
                ScannerKind::Batch(s) => s.$method($($arg),*).await,
            }
        }) {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    };
}

impl LogScanner {
    fn subscribe(&self, bucket_id: i32, start_offset: i64) -> ffi::FfiResult {
        dispatch_scanner!(self, subscribe(bucket_id, start_offset))
    }

    fn subscribe_buckets(&self, subscriptions: Vec<ffi::FfiBucketSubscription>) -> ffi::FfiResult {
        use std::collections::HashMap;
        let bucket_offsets: HashMap<i32, i64> = subscriptions
            .into_iter()
            .map(|s| (s.bucket_id, s.offset))
            .collect();
        dispatch_scanner!(self, subscribe_buckets(&bucket_offsets))
    }

    fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket_id: i32,
        start_offset: i64,
    ) -> ffi::FfiResult {
        dispatch_scanner!(
            self,
            subscribe_partition(partition_id, bucket_id, start_offset)
        )
    }

    fn subscribe_partition_buckets(
        &self,
        subscriptions: Vec<ffi::FfiPartitionBucketSubscription>,
    ) -> ffi::FfiResult {
        use std::collections::HashMap;
        let offsets: HashMap<(PartitionId, i32), i64> = subscriptions
            .into_iter()
            .map(|s| ((s.partition_id, s.bucket_id), s.offset))
            .collect();
        dispatch_scanner!(self, subscribe_partition_buckets(&offsets))
    }

    fn unsubscribe(&self, bucket_id: i32) -> ffi::FfiResult {
        dispatch_scanner!(self, unsubscribe(bucket_id))
    }

    fn unsubscribe_partition(&self, partition_id: PartitionId, bucket_id: i32) -> ffi::FfiResult {
        dispatch_scanner!(self, unsubscribe_partition(partition_id, bucket_id))
    }

    fn poll(&self, timeout_ms: i64) -> Box<ScanResultInner> {
        let ScannerKind::Record(ref inner) = self.scanner else {
            return Box::new(ScanResultInner::from_error(
                CLIENT_ERROR_CODE,
                "Record-based scanner not available".to_string(),
            ));
        };

        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        let result = RUNTIME.block_on(async { inner.poll(timeout).await });

        match result {
            Ok(records) => {
                let columns = self.projected_columns.clone();
                let mut total_count = 0usize;
                let mut buckets = Vec::new();
                let mut bucket_infos = Vec::new();
                for (table_bucket, bucket_records) in records.into_records_by_buckets() {
                    let count = bucket_records.len();
                    total_count += count;
                    bucket_infos.push(ffi::FfiBucketInfo {
                        table_id: table_bucket.table_id(),
                        bucket_id: table_bucket.bucket_id(),
                        has_partition_id: table_bucket.partition_id().is_some(),
                        partition_id: table_bucket.partition_id().unwrap_or(0),
                        record_count: count,
                    });
                    buckets.push((table_bucket, bucket_records));
                }
                Box::new(ScanResultInner {
                    error: None,
                    buckets,
                    columns,
                    bucket_infos,
                    total_count,
                })
            }
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                Box::new(ScanResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ))
            }
        }
    }

    fn poll_record_batch(&self, timeout_ms: i64) -> ffi::FfiArrowRecordBatchesResult {
        let ScannerKind::Batch(ref inner_batch) = self.scanner else {
            return ffi::FfiArrowRecordBatchesResult {
                result: client_err("Batch-based scanner not available".to_string()),
                arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
            };
        };

        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        let result = RUNTIME.block_on(async { inner_batch.poll(timeout).await });

        match result {
            Ok(batches) => match types::core_scan_batches_to_ffi(&batches) {
                Ok(arrow_batches) => ffi::FfiArrowRecordBatchesResult {
                    result: ok_result(),
                    arrow_batches,
                },
                Err(e) => ffi::FfiArrowRecordBatchesResult {
                    result: client_err(e),
                    arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
                },
            },
            Err(e) => ffi::FfiArrowRecordBatchesResult {
                result: err_from_core_error(&e),
                arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
            },
        }
    }
}

// ============================================================================
// Opaque types: GenericRowInner (write path)
// ============================================================================

pub struct GenericRowInner {
    row: fcore::row::GenericRow<'static>,
}

fn new_generic_row(field_count: usize) -> Box<GenericRowInner> {
    Box::new(GenericRowInner {
        row: fcore::row::GenericRow::new(field_count),
    })
}

impl GenericRowInner {
    fn gr_reset(&mut self) {
        let len = self.row.values.len();
        self.row = fcore::row::GenericRow::new(len);
    }

    fn gr_set_null(&mut self, idx: usize) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Null);
    }

    fn gr_set_bool(&mut self, idx: usize, val: bool) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Bool(val));
    }

    fn gr_set_i32(&mut self, idx: usize, val: i32) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Int32(val));
    }

    fn gr_set_i64(&mut self, idx: usize, val: i64) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Int64(val));
    }

    fn gr_set_f32(&mut self, idx: usize, val: f32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Float32(val.into()));
    }

    fn gr_set_f64(&mut self, idx: usize, val: f64) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Float64(val.into()));
    }

    fn gr_set_str(&mut self, idx: usize, val: &str) {
        self.ensure_size(idx);
        self.row.set_field(
            idx,
            fcore::row::Datum::String(std::borrow::Cow::Owned(val.to_string())),
        );
    }

    fn gr_set_bytes(&mut self, idx: usize, val: &[u8]) {
        self.ensure_size(idx);
        self.row.set_field(
            idx,
            fcore::row::Datum::Blob(std::borrow::Cow::Owned(val.to_vec())),
        );
    }

    fn gr_set_date(&mut self, idx: usize, days: i32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Date(fcore::row::Date::new(days)));
    }

    fn gr_set_time(&mut self, idx: usize, millis: i32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Time(fcore::row::Time::new(millis)));
    }

    fn gr_set_ts_ntz(&mut self, idx: usize, millis: i64, nanos: i32) {
        self.ensure_size(idx);
        // Use from_millis_nanos, falling back to millis-only on error
        let ts = fcore::row::TimestampNtz::from_millis_nanos(millis, nanos)
            .unwrap_or_else(|_| fcore::row::TimestampNtz::new(millis));
        self.row.set_field(idx, fcore::row::Datum::TimestampNtz(ts));
    }

    fn gr_set_ts_ltz(&mut self, idx: usize, millis: i64, nanos: i32) {
        self.ensure_size(idx);
        let ts = fcore::row::TimestampLtz::from_millis_nanos(millis, nanos)
            .unwrap_or_else(|_| fcore::row::TimestampLtz::new(millis));
        self.row.set_field(idx, fcore::row::Datum::TimestampLtz(ts));
    }

    fn gr_set_decimal_str(&mut self, idx: usize, val: &str) {
        self.ensure_size(idx);
        // Store as string; resolve_row_types() will parse and validate against schema
        self.row.set_field(
            idx,
            fcore::row::Datum::String(std::borrow::Cow::Owned(val.to_string())),
        );
    }

    fn ensure_size(&mut self, idx: usize) {
        if self.row.values.len() <= idx {
            self.row.values.resize(idx + 1, fcore::row::Datum::Null);
        }
    }
}

// ============================================================================
// Shared row-reading helpers (used by both ScanResultInner and LookupResultInner)
// ============================================================================

mod row_reader {
    use fcore::row::InternalRow;
    use fluss as fcore;

    use crate::types;

    /// Get column at `field`, or error if out of bounds.
    fn get_column(
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<&fcore::metadata::Column, String> {
        columns.get(field).ok_or_else(|| {
            format!(
                "field index {field} out of range ({} columns)",
                columns.len()
            )
        })
    }

    /// Validate bounds, null, and type compatibility in a single pass.
    /// Returns the data type on success for callers that need to dispatch on it.
    fn validate<'a>(
        row: &dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
        getter: &str,
        allowed: impl FnOnce(&fcore::metadata::DataType) -> bool,
    ) -> Result<&'a fcore::metadata::DataType, String> {
        let col = get_column(columns, field)?;
        if row.is_null_at(field) {
            return Err(format!("field {field} is null"));
        }
        let dt = col.data_type();
        if !allowed(dt) {
            return Err(format!(
                "{getter}: column {field} has incompatible type {dt}"
            ));
        }
        Ok(dt)
    }

    pub fn column_type(columns: &[fcore::metadata::Column], field: usize) -> Result<i32, String> {
        Ok(types::core_data_type_to_ffi(
            get_column(columns, field)?.data_type(),
        ))
    }

    pub fn column_name(columns: &[fcore::metadata::Column], field: usize) -> Result<&str, String> {
        Ok(get_column(columns, field)?.name())
    }

    pub fn is_null(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<bool, String> {
        get_column(columns, field)?;
        Ok(row.is_null_at(field))
    }

    pub fn get_bool(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<bool, String> {
        validate(row, columns, field, "get_bool", |dt| {
            matches!(dt, fcore::metadata::DataType::Boolean(_))
        })?;
        Ok(row.get_boolean(field))
    }

    pub fn get_i32(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        let dt = validate(row, columns, field, "get_i32", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::TinyInt(_)
                    | fcore::metadata::DataType::SmallInt(_)
                    | fcore::metadata::DataType::Int(_)
            )
        })?;
        Ok(match dt {
            fcore::metadata::DataType::TinyInt(_) => row.get_byte(field) as i32,
            fcore::metadata::DataType::SmallInt(_) => row.get_short(field) as i32,
            _ => row.get_int(field),
        })
    }

    pub fn get_i64(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i64, String> {
        validate(row, columns, field, "get_i64", |dt| {
            matches!(dt, fcore::metadata::DataType::BigInt(_))
        })?;
        Ok(row.get_long(field))
    }

    pub fn get_f32(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<f32, String> {
        validate(row, columns, field, "get_f32", |dt| {
            matches!(dt, fcore::metadata::DataType::Float(_))
        })?;
        Ok(row.get_float(field))
    }

    pub fn get_f64(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<f64, String> {
        validate(row, columns, field, "get_f64", |dt| {
            matches!(dt, fcore::metadata::DataType::Double(_))
        })?;
        Ok(row.get_double(field))
    }

    pub fn get_str<'a>(
        row: &'a dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
    ) -> Result<&'a str, String> {
        let dt = validate(row, columns, field, "get_str", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Char(_) | fcore::metadata::DataType::String(_)
            )
        })?;
        Ok(match dt {
            fcore::metadata::DataType::Char(ct) => row.get_char(field, ct.length() as usize),
            _ => row.get_string(field),
        })
    }

    pub fn get_bytes<'a>(
        row: &'a dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
    ) -> Result<&'a [u8], String> {
        let dt = validate(row, columns, field, "get_bytes", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Binary(_) | fcore::metadata::DataType::Bytes(_)
            )
        })?;
        Ok(match dt {
            fcore::metadata::DataType::Binary(bt) => row.get_binary(field, bt.length()),
            _ => row.get_bytes(field),
        })
    }

    pub fn get_date_days(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        validate(row, columns, field, "get_date_days", |dt| {
            matches!(dt, fcore::metadata::DataType::Date(_))
        })?;
        Ok(row.get_date(field).get_inner())
    }

    pub fn get_time_millis(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        validate(row, columns, field, "get_time_millis", |dt| {
            matches!(dt, fcore::metadata::DataType::Time(_))
        })?;
        Ok(row.get_time(field).get_inner())
    }

    pub fn get_ts_millis(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i64, String> {
        let dt = validate(row, columns, field, "get_ts_millis", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Timestamp(_)
                    | fcore::metadata::DataType::TimestampLTz(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::TimestampLTz(ts) => Ok(row
                .get_timestamp_ltz(field, ts.precision())
                .get_epoch_millisecond()),
            fcore::metadata::DataType::Timestamp(ts) => Ok(row
                .get_timestamp_ntz(field, ts.precision())
                .get_millisecond()),
            dt => Err(format!("get_ts_millis: unexpected type {dt}")),
        }
    }

    pub fn get_ts_nanos(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        let dt = validate(row, columns, field, "get_ts_nanos", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Timestamp(_)
                    | fcore::metadata::DataType::TimestampLTz(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::TimestampLTz(ts) => Ok(row
                .get_timestamp_ltz(field, ts.precision())
                .get_nano_of_millisecond()),
            fcore::metadata::DataType::Timestamp(ts) => Ok(row
                .get_timestamp_ntz(field, ts.precision())
                .get_nano_of_millisecond()),
            dt => Err(format!("get_ts_nanos: unexpected type {dt}")),
        }
    }

    pub fn is_ts_ltz(columns: &[fcore::metadata::Column], field: usize) -> Result<bool, String> {
        Ok(matches!(
            get_column(columns, field)?.data_type(),
            fcore::metadata::DataType::TimestampLTz(_)
        ))
    }

    pub fn get_decimal_str(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<String, String> {
        let dt = validate(row, columns, field, "get_decimal_str", |dt| {
            matches!(dt, fcore::metadata::DataType::Decimal(_))
        })?;
        match dt {
            fcore::metadata::DataType::Decimal(dd) => {
                let decimal = row.get_decimal(field, dd.precision() as usize, dd.scale() as usize);
                Ok(decimal.to_big_decimal().to_string())
            }
            dt => Err(format!("get_decimal_str: unexpected type {dt}")),
        }
    }
}

// ============================================================================
// Opaque types: ScanResultInner (scan read path)
// ============================================================================

pub struct ScanResultInner {
    error: Option<(i32, String)>,
    buckets: Vec<(fcore::metadata::TableBucket, Vec<fcore::record::ScanRecord>)>,
    columns: Vec<fcore::metadata::Column>,
    bucket_infos: Vec<ffi::FfiBucketInfo>,
    total_count: usize,
}

impl ScanResultInner {
    fn from_error(code: i32, msg: String) -> Self {
        Self {
            error: Some((code, msg)),
            buckets: Vec::new(),
            columns: Vec::new(),
            bucket_infos: Vec::new(),
            total_count: 0,
        }
    }

    fn resolve(&self, bucket: usize, rec: usize) -> &fcore::record::ScanRecord {
        &self.buckets[bucket].1[rec]
    }

    fn sv_has_error(&self) -> bool {
        self.error.is_some()
    }

    fn sv_error_code(&self) -> i32 {
        self.error.as_ref().map_or(0, |e| e.0)
    }

    fn sv_error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.1.as_str())
    }

    fn sv_record_count(&self) -> usize {
        self.total_count
    }

    fn sv_column_count(&self) -> usize {
        self.columns.len()
    }
    fn sv_column_name(&self, field: usize) -> Result<&str, String> {
        row_reader::column_name(&self.columns, field)
    }
    fn sv_column_type(&self, field: usize) -> Result<i32, String> {
        row_reader::column_type(&self.columns, field)
    }

    fn sv_offset(&self, bucket: usize, rec: usize) -> i64 {
        self.resolve(bucket, rec).offset()
    }
    fn sv_timestamp(&self, bucket: usize, rec: usize) -> i64 {
        self.resolve(bucket, rec).timestamp()
    }
    fn sv_change_type(&self, bucket: usize, rec: usize) -> i32 {
        self.resolve(bucket, rec).change_type().to_byte_value() as i32
    }
    fn sv_field_count(&self) -> usize {
        self.columns.len()
    }

    // Field accessors — C++ validates bounds in BucketRecords/RecordAt, validate() checks field.
    fn sv_is_null(&self, bucket: usize, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_null(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_bool(&self, bucket: usize, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::get_bool(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_i32(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_i32(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_i64(&self, bucket: usize, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_i64(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_f32(&self, bucket: usize, rec: usize, field: usize) -> Result<f32, String> {
        row_reader::get_f32(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_f64(&self, bucket: usize, rec: usize, field: usize) -> Result<f64, String> {
        row_reader::get_f64(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_str(&self, bucket: usize, rec: usize, field: usize) -> Result<&str, String> {
        row_reader::get_str(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_bytes(&self, bucket: usize, rec: usize, field: usize) -> Result<&[u8], String> {
        row_reader::get_bytes(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_date_days(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_date_days(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_time_millis(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_time_millis(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_ts_millis(&self, bucket: usize, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_ts_millis(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_ts_nanos(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_ts_nanos(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_is_ts_ltz(&self, _bucket: usize, _rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_ts_ltz(&self.columns, field)
    }
    fn sv_get_decimal_str(
        &self,
        bucket: usize,
        rec: usize,
        field: usize,
    ) -> Result<String, String> {
        row_reader::get_decimal_str(self.resolve(bucket, rec).row(), &self.columns, field)
    }

    fn sv_bucket_infos(&self) -> &Vec<ffi::FfiBucketInfo> {
        &self.bucket_infos
    }
}

// ============================================================================
// Opaque types: LookupResultInner (lookup read path)
// ============================================================================

pub struct LookupResultInner {
    error: Option<(i32, String)>,
    found: bool,
    row: Option<fcore::row::GenericRow<'static>>,
    columns: Vec<fcore::metadata::Column>,
}

impl LookupResultInner {
    fn from_error(code: i32, msg: String) -> Self {
        Self {
            error: Some((code, msg)),
            found: false,
            row: None,
            columns: Vec::new(),
        }
    }

    fn lv_has_error(&self) -> bool {
        self.error.is_some()
    }

    fn lv_error_code(&self) -> i32 {
        self.error.as_ref().map_or(0, |e| e.0)
    }

    fn lv_error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.1.as_str())
    }

    fn lv_found(&self) -> bool {
        self.found
    }

    fn lv_field_count(&self) -> usize {
        self.columns.len()
    }

    fn lv_column_type(&self, field: usize) -> Result<i32, String> {
        row_reader::column_type(&self.columns, field)
    }

    fn lv_column_name(&self, field: usize) -> Result<&str, String> {
        row_reader::column_name(&self.columns, field)
    }

    fn lv_row(&self) -> Result<&fcore::row::GenericRow<'static>, String> {
        self.row
            .as_ref()
            .ok_or_else(|| "no row available (not found or error)".to_string())
    }

    // Field accessors — delegate to shared row_reader helpers.
    fn lv_is_null(&self, field: usize) -> Result<bool, String> {
        let r = self.lv_row()?;
        row_reader::is_null(r, &self.columns, field)
    }
    fn lv_get_bool(&self, field: usize) -> Result<bool, String> {
        let r = self.lv_row()?;
        row_reader::get_bool(r, &self.columns, field)
    }
    fn lv_get_i32(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_i32(r, &self.columns, field)
    }
    fn lv_get_i64(&self, field: usize) -> Result<i64, String> {
        let r = self.lv_row()?;
        row_reader::get_i64(r, &self.columns, field)
    }
    fn lv_get_f32(&self, field: usize) -> Result<f32, String> {
        let r = self.lv_row()?;
        row_reader::get_f32(r, &self.columns, field)
    }
    fn lv_get_f64(&self, field: usize) -> Result<f64, String> {
        let r = self.lv_row()?;
        row_reader::get_f64(r, &self.columns, field)
    }
    fn lv_get_str(&self, field: usize) -> Result<&str, String> {
        let r = self.lv_row()?;
        row_reader::get_str(r, &self.columns, field)
    }
    fn lv_get_bytes(&self, field: usize) -> Result<&[u8], String> {
        let r = self.lv_row()?;
        row_reader::get_bytes(r, &self.columns, field)
    }
    fn lv_get_date_days(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_date_days(r, &self.columns, field)
    }
    fn lv_get_time_millis(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_time_millis(r, &self.columns, field)
    }
    fn lv_get_ts_millis(&self, field: usize) -> Result<i64, String> {
        let r = self.lv_row()?;
        row_reader::get_ts_millis(r, &self.columns, field)
    }
    fn lv_get_ts_nanos(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_ts_nanos(r, &self.columns, field)
    }
    fn lv_is_ts_ltz(&self, field: usize) -> Result<bool, String> {
        row_reader::is_ts_ltz(&self.columns, field)
    }
    fn lv_get_decimal_str(&self, field: usize) -> Result<String, String> {
        let r = self.lv_row()?;
        row_reader::get_decimal_str(r, &self.columns, field)
    }
}
