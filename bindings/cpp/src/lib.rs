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
        bootstrap_server: String,
        request_max_size: i32,
        writer_acks: String,
        writer_retries: i32,
        writer_batch_size: i32,
        scanner_remote_log_prefetch_num: usize,
        scanner_remote_log_download_threads: usize,
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
        decimal_precision: i32,
        decimal_scale: i32,
        i128_hi: i64,
        i128_lo: i64,
    }

    struct FfiGenericRow {
        fields: Vec<FfiDatum>,
    }

    struct FfiScanRecord {
        bucket_id: i32,
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

    struct FfiLookupResult {
        result: FfiResult,
        found: bool,
        row: FfiGenericRow,
    }

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
        fn new_log_scanner(self: &Table) -> Result<*mut LogScanner>;
        fn new_log_scanner_with_projection(
            self: &Table,
            column_indices: Vec<usize>,
        ) -> Result<*mut LogScanner>;
        fn new_record_batch_log_scanner(self: &Table) -> Result<*mut LogScanner>;
        fn new_record_batch_log_scanner_with_projection(
            self: &Table,
            column_indices: Vec<usize>,
        ) -> Result<*mut LogScanner>;
        fn get_table_info_from_table(self: &Table) -> FfiTableInfo;
        fn get_table_path(self: &Table) -> FfiTablePath;
        fn has_primary_key(self: &Table) -> bool;
        fn new_upsert_writer(self: &Table) -> Result<*mut UpsertWriter>;
        fn new_upsert_writer_with_column_names(
            self: &Table,
            column_names: Vec<String>,
        ) -> Result<*mut UpsertWriter>;
        fn new_upsert_writer_with_column_indices(
            self: &Table,
            column_indices: Vec<usize>,
        ) -> Result<*mut UpsertWriter>;
        fn new_lookuper(self: &Table) -> Result<*mut Lookuper>;

        // AppendWriter
        unsafe fn delete_append_writer(writer: *mut AppendWriter);
        fn append(self: &mut AppendWriter, row: &FfiGenericRow) -> Result<Box<WriteResult>>;
        fn flush(self: &mut AppendWriter) -> FfiResult;

        // WriteResult — dropped automatically via rust::Box, or call wait() for ack
        fn wait(self: &mut WriteResult) -> FfiResult;

        // UpsertWriter
        unsafe fn delete_upsert_writer(writer: *mut UpsertWriter);
        fn upsert(self: &mut UpsertWriter, row: &FfiGenericRow) -> Result<Box<WriteResult>>;
        fn delete_row(self: &mut UpsertWriter, row: &FfiGenericRow) -> Result<Box<WriteResult>>;
        fn upsert_flush(self: &mut UpsertWriter) -> FfiResult;

        // Lookuper
        unsafe fn delete_lookuper(lookuper: *mut Lookuper);
        fn lookup(self: &mut Lookuper, pk_row: &FfiGenericRow) -> FfiLookupResult;

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
        fn unsubscribe_partition(self: &LogScanner, partition_id: i64, bucket_id: i32)
        -> FfiResult;
        fn poll(self: &LogScanner, timeout_ms: i64) -> FfiScanRecordsResult;
        fn poll_record_batch(self: &LogScanner, timeout_ms: i64) -> FfiArrowRecordBatchesResult;
        fn free_arrow_ffi_structures(array_ptr: usize, schema_ptr: usize);
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
    table_info: fcore::metadata::TableInfo,
}

pub struct WriteResult {
    inner: Option<fcore::client::WriteResultFuture>,
}

pub struct LogScanner {
    inner: Option<fcore::client::LogScanner>,
    inner_batch: Option<fcore::client::RecordBatchLogScanner>,
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
fn new_connection(config: &ffi::FfiConfig) -> Result<*mut Connection, String> {
    let config = fluss::config::Config {
        bootstrap_server: config.bootstrap_server.to_string(),
        request_max_size: config.request_max_size,
        writer_acks: config.writer_acks.to_string(),
        writer_retries: config.writer_retries,
        writer_batch_size: config.writer_batch_size,
        scanner_remote_log_prefetch_num: config.scanner_remote_log_prefetch_num,
        scanner_remote_log_download_threads: config.scanner_remote_log_download_threads,
    };

    let conn = RUNTIME.block_on(async { fcore::client::FlussConnection::new(config).await });

    match conn {
        Ok(c) => {
            let conn = Box::into_raw(Box::new(Connection {
                inner: Arc::new(c),
                metadata: None,
            }));
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
                    table_info: t.table_info().clone(),
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
            Err(e) => err_result(1, e.to_string()),
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
                    result: err_result(
                        1,
                        format!("Invalid offset_type: {}", offset_query.offset_type),
                    ),
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
                result: err_result(1, e.to_string()),
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
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let result = RUNTIME.block_on(async { self.inner.list_partition_infos(&path).await });
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
                result: err_result(1, e.to_string()),
                partition_infos: vec![],
            },
        }
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
            Err(e) => err_result(1, e.to_string()),
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
            Err(e) => err_result(1, e.to_string()),
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
                .create_database(database_name, ignore_if_exists, descriptor_opt.as_ref())
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(1, e.to_string()),
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
            Err(e) => err_result(1, e.to_string()),
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
                result: err_result(1, e.to_string()),
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
                result: err_result(1, e.to_string()),
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
                result: err_result(1, e.to_string()),
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
                result: err_result(1, e.to_string()),
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
                result: err_result(1, e.to_string()),
                value: false,
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

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_append = match fluss_table.new_append() {
            Ok(a) => a,
            Err(e) => return Err(format!("Failed to create append: {e}")),
        };

        let writer = match table_append.create_writer() {
            Ok(w) => w,
            Err(e) => return Err(format!("Failed to create writer: {e}")),
        };
        let writer = Box::into_raw(Box::new(AppendWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        }));
        Ok(writer)
    }

    fn new_log_scanner(&self) -> Result<*mut LogScanner, String> {
        RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &self.connection,
                self.metadata.clone(),
                self.table_info.clone(),
            );

            let scanner = fluss_table
                .new_scan()
                .create_log_scanner()
                .map_err(|e| format!("Failed to create log scanner: {e}"))?;

            let scanner_ptr = Box::into_raw(Box::new(LogScanner {
                inner: Some(scanner),
                inner_batch: None,
                projected_columns: self.table_info.get_schema().columns().to_vec(),
            }));

            Ok(scanner_ptr)
        })
    }

    fn new_log_scanner_with_projection(
        &self,
        column_indices: Vec<usize>,
    ) -> Result<*mut LogScanner, String> {
        RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &self.connection,
                self.metadata.clone(),
                self.table_info.clone(),
            );

            let all_columns = self.table_info.get_schema().columns();
            let projected_columns: Vec<_> = column_indices
                .iter()
                .map(|&i| {
                    all_columns.get(i).cloned().ok_or_else(|| {
                        format!(
                            "Invalid column index {i}: schema has {} columns",
                            all_columns.len()
                        )
                    })
                })
                .collect::<Result<_, String>>()?;

            let log_scanner = fluss_table
                .new_scan()
                .project(&column_indices)
                .map_err(|e| format!("Failed to project columns: {e}"))?
                .create_log_scanner()
                .map_err(|e| format!("Failed to create log scanner: {e}"))?;

            let scanner = Box::into_raw(Box::new(LogScanner {
                inner: Some(log_scanner),
                inner_batch: None,
                projected_columns,
            }));
            Ok(scanner)
        })
    }

    fn new_record_batch_log_scanner(&self) -> Result<*mut LogScanner, String> {
        RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &self.connection,
                self.metadata.clone(),
                self.table_info.clone(),
            );

            let batch_scanner = fluss_table
                .new_scan()
                .create_record_batch_log_scanner()
                .map_err(|e| format!("Failed to create record batch log scanner: {e}"))?;

            let scanner = Box::into_raw(Box::new(LogScanner {
                inner: None,
                inner_batch: Some(batch_scanner),
                projected_columns: self.table_info.get_schema().columns().to_vec(),
            }));
            Ok(scanner)
        })
    }

    fn new_record_batch_log_scanner_with_projection(
        &self,
        column_indices: Vec<usize>,
    ) -> Result<*mut LogScanner, String> {
        RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &self.connection,
                self.metadata.clone(),
                self.table_info.clone(),
            );

            let all_columns = self.table_info.get_schema().columns();
            let projected_columns: Vec<_> = column_indices
                .iter()
                .map(|&i| {
                    all_columns.get(i).cloned().ok_or_else(|| {
                        format!(
                            "Invalid column index {i}: schema has {} columns",
                            all_columns.len()
                        )
                    })
                })
                .collect::<Result<_, String>>()?;

            let batch_scanner = fluss_table
                .new_scan()
                .project(&column_indices)
                .map_err(|e| format!("Failed to project columns: {e}"))?
                .create_record_batch_log_scanner()
                .map_err(|e| format!("Failed to create record batch log scanner: {e}"))?;

            let scanner = Box::into_raw(Box::new(LogScanner {
                inner: None,
                inner_batch: Some(batch_scanner),
                projected_columns,
            }));
            Ok(scanner)
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

    fn new_upsert_writer(&self) -> Result<*mut UpsertWriter, String> {
        let _enter = RUNTIME.enter();

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| format!("Failed to create upsert: {e}"))?;

        let writer = table_upsert
            .create_writer()
            .map_err(|e| format!("Failed to create upsert writer: {e}"))?;

        Ok(Box::into_raw(Box::new(UpsertWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        })))
    }

    fn new_upsert_writer_with_column_names(
        &self,
        column_names: Vec<String>,
    ) -> Result<*mut UpsertWriter, String> {
        let _enter = RUNTIME.enter();

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| format!("Failed to create upsert: {e}"))?;

        let col_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let table_upsert = table_upsert
            .partial_update_with_column_names(&col_refs)
            .map_err(|e| format!("Failed to set partial update columns: {e}"))?;

        let writer = table_upsert
            .create_writer()
            .map_err(|e| format!("Failed to create upsert writer: {e}"))?;

        Ok(Box::into_raw(Box::new(UpsertWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        })))
    }

    fn new_upsert_writer_with_column_indices(
        &self,
        column_indices: Vec<usize>,
    ) -> Result<*mut UpsertWriter, String> {
        let _enter = RUNTIME.enter();

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| format!("Failed to create upsert: {e}"))?;

        let table_upsert = table_upsert
            .partial_update(Some(column_indices))
            .map_err(|e| format!("Failed to set partial update columns: {e}"))?;

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

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_lookup = fluss_table
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
    fn append(&mut self, row: &ffi::FfiGenericRow) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row = types::ffi_row_to_core(row, Some(schema)).map_err(|e| e.to_string())?;

        let result_future = self
            .inner
            .append(&generic_row)
            .map_err(|e| format!("Failed to append: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_result(1, e.to_string()),
        }
    }
}

impl WriteResult {
    fn wait(&mut self) -> ffi::FfiResult {
        if let Some(future) = self.inner.take() {
            let result = RUNTIME.block_on(future);
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else {
            err_result(1, "WriteResult already consumed".to_string())
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

    fn upsert(&mut self, row: &ffi::FfiGenericRow) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row = types::ffi_row_to_core(row, Some(schema)).map_err(|e| e.to_string())?;
        let generic_row = self.pad_row(generic_row);

        let result_future = self
            .inner
            .upsert(&generic_row)
            .map_err(|e| format!("Failed to upsert: {e}"))?;

        Ok(Box::new(WriteResult {
            inner: Some(result_future),
        }))
    }

    fn delete_row(&mut self, row: &ffi::FfiGenericRow) -> Result<Box<WriteResult>, String> {
        let schema = self.table_info.get_schema();
        let generic_row = types::ffi_row_to_core(row, Some(schema)).map_err(|e| e.to_string())?;
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
            Err(e) => err_result(1, e.to_string()),
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
    /// Pad row with Null to full schema width (same as UpsertWriter::pad_row).
    /// Ensures the PK row is always full-width, matching Python's behavior.
    fn pad_row<'a>(&self, mut row: fcore::row::GenericRow<'a>) -> fcore::row::GenericRow<'a> {
        let num_columns = self.table_info.get_schema().columns().len();
        if row.values.len() < num_columns {
            row.values.resize(num_columns, fcore::row::Datum::Null);
        }
        row
    }

    fn lookup(&mut self, pk_row: &ffi::FfiGenericRow) -> ffi::FfiLookupResult {
        let schema = self.table_info.get_schema();
        let generic_row = match types::ffi_row_to_core(pk_row, Some(schema)) {
            Ok(r) => self.pad_row(r),
            Err(e) => {
                return ffi::FfiLookupResult {
                    result: err_result(1, e.to_string()),
                    found: false,
                    row: ffi::FfiGenericRow { fields: vec![] },
                };
            }
        };

        let lookup_result = match RUNTIME.block_on(self.inner.lookup(&generic_row)) {
            Ok(r) => r,
            Err(e) => {
                return ffi::FfiLookupResult {
                    result: err_result(1, e.to_string()),
                    found: false,
                    row: ffi::FfiGenericRow { fields: vec![] },
                };
            }
        };

        match lookup_result.get_single_row() {
            Ok(Some(row)) => match types::internal_row_to_ffi_row(&row, &self.table_info) {
                Ok(ffi_row) => ffi::FfiLookupResult {
                    result: ok_result(),
                    found: true,
                    row: ffi_row,
                },
                Err(e) => ffi::FfiLookupResult {
                    result: err_result(1, e.to_string()),
                    found: false,
                    row: ffi::FfiGenericRow { fields: vec![] },
                },
            },
            Ok(None) => ffi::FfiLookupResult {
                result: ok_result(),
                found: false,
                row: ffi::FfiGenericRow { fields: vec![] },
            },
            Err(e) => ffi::FfiLookupResult {
                result: err_result(1, e.to_string()),
                found: false,
                row: ffi::FfiGenericRow { fields: vec![] },
            },
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

impl LogScanner {
    fn subscribe(&self, bucket_id: i32, start_offset: i64) -> ffi::FfiResult {
        self.do_subscribe(None, bucket_id, start_offset)
    }

    fn do_subscribe(
        &self,
        partition_id: Option<PartitionId>,
        bucket_id: i32,
        start_offset: i64,
    ) -> ffi::FfiResult {
        if let Some(ref inner) = self.inner {
            let result = RUNTIME.block_on(async {
                if let Some(partition_id) = partition_id {
                    inner
                        .subscribe_partition(partition_id, bucket_id, start_offset)
                        .await
                } else {
                    inner.subscribe(bucket_id, start_offset).await
                }
            });
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else if let Some(ref inner_batch) = self.inner_batch {
            let result = RUNTIME.block_on(async {
                if let Some(partition_id) = partition_id {
                    inner_batch
                        .subscribe_partition(partition_id, bucket_id, start_offset)
                        .await
                } else {
                    inner_batch.subscribe(bucket_id, start_offset).await
                }
            });
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else {
            err_result(1, "LogScanner not initialized".to_string())
        }
    }

    fn subscribe_buckets(&self, subscriptions: Vec<ffi::FfiBucketSubscription>) -> ffi::FfiResult {
        use std::collections::HashMap;
        let mut bucket_offsets = HashMap::new();
        for sub in subscriptions {
            bucket_offsets.insert(sub.bucket_id, sub.offset);
        }

        if let Some(ref inner) = self.inner {
            let result = RUNTIME.block_on(async { inner.subscribe_buckets(&bucket_offsets).await });

            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else if let Some(ref inner_batch) = self.inner_batch {
            let result =
                RUNTIME.block_on(async { inner_batch.subscribe_buckets(&bucket_offsets).await });

            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else {
            err_result(1, "LogScanner not initialized".to_string())
        }
    }

    fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket_id: i32,
        start_offset: i64,
    ) -> ffi::FfiResult {
        self.do_subscribe(Some(partition_id), bucket_id, start_offset)
    }

    fn subscribe_partition_buckets(
        &self,
        subscriptions: Vec<ffi::FfiPartitionBucketSubscription>,
    ) -> ffi::FfiResult {
        use std::collections::HashMap;
        let mut partition_bucket_offsets: HashMap<(PartitionId, i32), i64> = HashMap::new();
        for sub in subscriptions {
            partition_bucket_offsets.insert((sub.partition_id, sub.bucket_id), sub.offset);
        }

        if let Some(ref inner) = self.inner {
            let result = RUNTIME.block_on(async {
                inner
                    .subscribe_partition_buckets(&partition_bucket_offsets)
                    .await
            });
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else if let Some(ref inner_batch) = self.inner_batch {
            let result = RUNTIME.block_on(async {
                inner_batch
                    .subscribe_partition_buckets(&partition_bucket_offsets)
                    .await
            });
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else {
            err_result(1, "LogScanner not initialized".to_string())
        }
    }

    fn unsubscribe_partition(&self, partition_id: PartitionId, bucket_id: i32) -> ffi::FfiResult {
        if let Some(ref inner) = self.inner {
            match RUNTIME
                .block_on(async { inner.unsubscribe_partition(partition_id, bucket_id).await })
            {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else if let Some(ref inner_batch) = self.inner_batch {
            match RUNTIME.block_on(async {
                inner_batch
                    .unsubscribe_partition(partition_id, bucket_id)
                    .await
            }) {
                Ok(_) => ok_result(),
                Err(e) => err_result(1, e.to_string()),
            }
        } else {
            err_result(1, "LogScanner not initialized".to_string())
        }
    }

    fn poll(&self, timeout_ms: i64) -> ffi::FfiScanRecordsResult {
        if let Some(ref inner) = self.inner {
            let timeout = Duration::from_millis(timeout_ms as u64);
            let result = RUNTIME.block_on(async { inner.poll(timeout).await });

            match result {
                Ok(records) => {
                    match types::core_scan_records_to_ffi(&records, &self.projected_columns) {
                        Ok(scan_records) => ffi::FfiScanRecordsResult {
                            result: ok_result(),
                            scan_records,
                        },
                        Err(e) => ffi::FfiScanRecordsResult {
                            result: err_result(1, e.to_string()),
                            scan_records: ffi::FfiScanRecords { records: vec![] },
                        },
                    }
                }
                Err(e) => ffi::FfiScanRecordsResult {
                    result: err_result(1, e.to_string()),
                    scan_records: ffi::FfiScanRecords { records: vec![] },
                },
            }
        } else {
            ffi::FfiScanRecordsResult {
                result: err_result(1, "Record-based scanner not available".to_string()),
                scan_records: ffi::FfiScanRecords { records: vec![] },
            }
        }
    }

    fn poll_record_batch(&self, timeout_ms: i64) -> ffi::FfiArrowRecordBatchesResult {
        if let Some(ref inner_batch) = self.inner_batch {
            let timeout = Duration::from_millis(timeout_ms as u64);
            let result = RUNTIME.block_on(async { inner_batch.poll(timeout).await });

            match result {
                Ok(batches) => match types::core_scan_batches_to_ffi(&batches) {
                    Ok(arrow_batches) => ffi::FfiArrowRecordBatchesResult {
                        result: ok_result(),
                        arrow_batches,
                    },
                    Err(e) => ffi::FfiArrowRecordBatchesResult {
                        result: err_result(1, e),
                        arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
                    },
                },
                Err(e) => ffi::FfiArrowRecordBatchesResult {
                    result: err_result(1, e.to_string()),
                    arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
                },
            }
        } else {
            ffi::FfiArrowRecordBatchesResult {
                result: err_result(1, "Batch-based scanner not available".to_string()),
                arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
            }
        }
    }
}
