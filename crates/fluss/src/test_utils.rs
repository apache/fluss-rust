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

use crate::client::{CompletedFetch, FetchErrorContext, WriteRecord};
use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
use crate::compression::{
    ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
};
use crate::error::{ApiError, Error, Result};
use crate::metadata::{
    DataField, DataTypes, Schema, TableBucket, TableDescriptor, TableInfo, TablePath,
};
use crate::record::{MemoryLogRecordsArrowBuilder, ReadContext, ScanRecord, to_arrow_schema};
use crate::row::{ColumnarRow, Datum, GenericRow};
use crate::rpc::{ServerConnection, ServerConnectionInner, Transport, spawn_mock_server};
use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::BufStream;
use tokio::task::JoinHandle;

pub(crate) async fn build_mock_connection<F>(handler: F) -> (ServerConnection, JoinHandle<()>)
where
    F: FnMut(crate::rpc::ApiKey, i32, Vec<u8>) -> Vec<u8> + Send + 'static,
{
    let (client, server) = tokio::io::duplex(1024);
    let handle = spawn_mock_server(server, handler).await;
    let transport = Transport::Test { inner: client };
    let connection = Arc::new(ServerConnectionInner::new(
        BufStream::new(transport),
        usize::MAX,
        Arc::from(""),
    ));
    (connection, handle)
}

pub(crate) fn build_table_info(table_path: TablePath, table_id: i64, buckets: i32) -> TableInfo {
    let row_type = DataTypes::row(vec![DataField::new(
        "id".to_string(),
        DataTypes::int(),
        None,
    )]);
    let mut schema_builder = Schema::builder().with_row_type(&row_type);
    let schema = schema_builder.build().expect("schema build");
    let table_descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(buckets), vec![])
        .build()
        .expect("descriptor build");
    TableInfo::of(table_path, table_id, 1, table_descriptor, 0, 0)
}

pub(crate) fn build_cluster(table_path: &TablePath, table_id: i64, buckets: i32) -> Cluster {
    let server = ServerNode::new(1, "127.0.0.1".to_string(), 9092, ServerType::TabletServer);

    let mut servers = HashMap::new();
    servers.insert(server.id(), server.clone());

    let mut locations_by_path = HashMap::new();
    let mut locations_by_bucket = HashMap::new();
    let mut bucket_locations = Vec::new();

    for bucket_id in 0..buckets {
        let table_bucket = TableBucket::new(table_id, bucket_id);
        let bucket_location = BucketLocation::new(
            table_bucket.clone(),
            Some(server.clone()),
            table_path.clone(),
        );
        bucket_locations.push(bucket_location.clone());
        locations_by_bucket.insert(table_bucket, bucket_location);
    }
    locations_by_path.insert(table_path.clone(), bucket_locations);

    let mut table_id_by_path = HashMap::new();
    table_id_by_path.insert(table_path.clone(), table_id);

    let mut table_info_by_path = HashMap::new();
    table_info_by_path.insert(
        table_path.clone(),
        build_table_info(table_path.clone(), table_id, buckets),
    );

    Cluster::new(
        None,
        servers,
        locations_by_path,
        locations_by_bucket,
        table_id_by_path,
        table_info_by_path,
    )
}

pub(crate) fn build_cluster_arc(
    table_path: &TablePath,
    table_id: i64,
    buckets: i32,
) -> Arc<Cluster> {
    Arc::new(build_cluster(table_path, table_id, buckets))
}

pub(crate) fn build_cluster_with_coordinator(
    table_path: &TablePath,
    table_id: i64,
    coordinator: ServerNode,
    tablet: ServerNode,
) -> Cluster {
    let table_bucket = TableBucket::new(table_id, 0);
    let bucket_location = BucketLocation::new(
        table_bucket.clone(),
        Some(tablet.clone()),
        table_path.clone(),
    );

    let mut servers = HashMap::new();
    servers.insert(tablet.id(), tablet);

    let mut locations_by_path = HashMap::new();
    locations_by_path.insert(table_path.clone(), vec![bucket_location.clone()]);

    let mut locations_by_bucket = HashMap::new();
    locations_by_bucket.insert(table_bucket, bucket_location);

    let mut table_id_by_path = HashMap::new();
    table_id_by_path.insert(table_path.clone(), table_id);

    let mut table_info_by_path = HashMap::new();
    table_info_by_path.insert(
        table_path.clone(),
        build_table_info(table_path.clone(), table_id, 1),
    );

    Cluster::new(
        Some(coordinator),
        servers,
        locations_by_path,
        locations_by_bucket,
        table_id_by_path,
        table_info_by_path,
    )
}

pub(crate) fn build_cluster_with_coordinator_arc(
    table_path: &TablePath,
    table_id: i64,
    coordinator: ServerNode,
    tablet: ServerNode,
) -> Arc<Cluster> {
    Arc::new(build_cluster_with_coordinator(
        table_path,
        table_id,
        coordinator,
        tablet,
    ))
}

pub(crate) fn build_read_context_for_int32() -> ReadContext {
    let row_type = DataTypes::row(vec![DataField::new(
        "id".to_string(),
        DataTypes::int(),
        None,
    )]);
    ReadContext::new(to_arrow_schema(&row_type), false)
}

fn build_single_int_record_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).expect("record batch")
}

pub(crate) fn build_single_int_scan_record() -> ScanRecord {
    let batch = build_single_int_record_batch();
    let row = ColumnarRow::new(Arc::new(batch));
    ScanRecord::new_default(row)
}

pub(crate) fn build_log_record_bytes(values: Vec<i32>) -> Result<Vec<u8>> {
    let fields = values
        .iter()
        .enumerate()
        .map(|(idx, _)| DataField::new(format!("c{idx}"), DataTypes::int(), None))
        .collect::<Vec<_>>();
    let row_type = DataTypes::row(fields);
    let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
    let mut builder = MemoryLogRecordsArrowBuilder::new(
        1,
        &row_type,
        false,
        ArrowCompressionInfo {
            compression_type: ArrowCompressionType::None,
            compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        },
    );
    let record = WriteRecord::for_append(
        table_path,
        1,
        GenericRow {
            values: values.into_iter().map(Datum::Int32).collect(),
        },
    );
    builder.append(&record)?;
    builder.build()
}

pub(crate) struct TestCompletedFetch {
    table_bucket: TableBucket,
    records: Vec<ScanRecord>,
    batches: Vec<RecordBatch>,
    error_on_records: bool,
    error_on_batches: bool,
    consumed: AtomicBool,
    initialized: AtomicBool,
    next_fetch_offset: i64,
    records_read: usize,
}

impl TestCompletedFetch {
    pub(crate) fn new(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            records: Vec::new(),
            batches: Vec::new(),
            error_on_records: false,
            error_on_batches: false,
            consumed: AtomicBool::new(false),
            initialized: AtomicBool::new(true),
            next_fetch_offset: 0,
            records_read: 0,
        }
    }

    pub(crate) fn record_ok(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            records: vec![build_single_int_scan_record()],
            batches: Vec::new(),
            error_on_records: false,
            error_on_batches: false,
            consumed: AtomicBool::new(false),
            initialized: AtomicBool::new(true),
            next_fetch_offset: 0,
            records_read: 0,
        }
    }

    pub(crate) fn record_err(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            records: Vec::new(),
            batches: Vec::new(),
            error_on_records: true,
            error_on_batches: false,
            consumed: AtomicBool::new(false),
            initialized: AtomicBool::new(true),
            next_fetch_offset: 0,
            records_read: 0,
        }
    }

    pub(crate) fn batch_ok(table_bucket: TableBucket) -> Self {
        let batch = build_single_int_record_batch();
        Self {
            table_bucket,
            records: Vec::new(),
            batches: vec![batch],
            error_on_records: false,
            error_on_batches: false,
            consumed: AtomicBool::new(false),
            initialized: AtomicBool::new(true),
            next_fetch_offset: 0,
            records_read: 0,
        }
    }

    pub(crate) fn batch_err(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            records: Vec::new(),
            batches: Vec::new(),
            error_on_records: false,
            error_on_batches: true,
            consumed: AtomicBool::new(false),
            initialized: AtomicBool::new(true),
            next_fetch_offset: 0,
            records_read: 0,
        }
    }
}

impl CompletedFetch for TestCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn api_error(&self) -> Option<&ApiError> {
        None
    }

    fn fetch_error_context(&self) -> Option<&FetchErrorContext> {
        None
    }

    fn take_error(&mut self) -> Option<Error> {
        None
    }

    fn fetch_records(&mut self, _max_records: usize) -> Result<Vec<ScanRecord>> {
        if self.error_on_records {
            self.consumed.store(true, Ordering::Release);
            return Err(Error::UnexpectedError {
                message: "fetch error".to_string(),
                source: None,
            });
        }
        if self.consumed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let records = std::mem::take(&mut self.records);
        if !records.is_empty() {
            self.records_read += records.len();
            self.next_fetch_offset += records.len() as i64;
            self.consumed.store(true, Ordering::Release);
        }
        Ok(records)
    }

    fn fetch_batches(&mut self, _max_batches: usize) -> Result<Vec<RecordBatch>> {
        if self.error_on_batches {
            self.consumed.store(true, Ordering::Release);
            return Err(Error::UnexpectedError {
                message: "fetch error".to_string(),
                source: None,
            });
        }
        if self.consumed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let batches = std::mem::take(&mut self.batches);
        if !batches.is_empty() {
            self.records_read += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            self.next_fetch_offset += 1;
            self.consumed.store(true, Ordering::Release);
        }
        Ok(batches)
    }

    fn is_consumed(&self) -> bool {
        self.consumed.load(Ordering::Acquire)
    }

    fn records_read(&self) -> usize {
        self.records_read
    }

    fn drain(&mut self) {
        self.consumed.store(true, Ordering::Release);
    }

    fn size_in_bytes(&self) -> usize {
        0
    }

    fn high_watermark(&self) -> i64 {
        0
    }

    fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    fn set_initialized(&mut self) {
        self.initialized.store(true, Ordering::Release);
    }

    fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }
}
