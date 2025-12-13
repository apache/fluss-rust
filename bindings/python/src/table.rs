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

use crate::TOKIO_RUNTIME;
use crate::*;
use fluss::client::EARLIEST_OFFSET;
use fluss::rpc::message::OffsetSpec;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Represents a Fluss table for data operations
#[pyclass]
pub struct FlussTable {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_primary_key: bool,
}

#[pymethods]
impl FlussTable {
    /// Create a new append writer for the table
    fn new_append_writer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(&conn, metadata, table_info);

            let table_append = fluss_table
                .new_append()
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let rust_writer = table_append.create_writer();

            let py_writer = AppendWriter::from_core(rust_writer);

            Python::attach(|py| Py::new(py, py_writer))
        })
    }

    /// Create a new log scanner for the table
    fn new_log_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table =
                fcore::client::FlussTable::new(&conn, metadata.clone(), table_info.clone());

            let table_scan = fluss_table.new_scan();

            let rust_scanner = table_scan.create_log_scanner().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to create log scanner: {e:?}"
                ))
            })?;

            let admin = conn
                .get_admin()
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let py_scanner = LogScanner::from_core(rust_scanner, admin, table_info.clone());
            Python::attach(|py| Py::new(py, py_scanner))
        })
    }

    /// Get table information
    pub fn get_table_info(&self) -> TableInfo {
        TableInfo::from_core(self.table_info.clone())
    }

    /// Get table path
    pub fn get_table_path(&self) -> TablePath {
        TablePath::from_core(self.table_path.clone())
    }

    /// Check if table has primary key
    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    fn __repr__(&self) -> String {
        format!(
            "FlussTable(path={}.{})",
            self.table_path.database(),
            self.table_path.table()
        )
    }
}

impl FlussTable {
    /// Create a FlussTable
    pub fn new_table(
        connection: Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
        table_path: fcore::metadata::TablePath,
        has_primary_key: bool,
    ) -> Self {
        Self {
            connection,
            metadata,
            table_info,
            table_path,
            has_primary_key,
        }
    }
}

/// Writer for appending data to a Fluss table
#[pyclass]
pub struct AppendWriter {
    inner: fcore::client::AppendWriter,
}

#[pymethods]
impl AppendWriter {
    /// Write Arrow table data
    pub fn write_arrow(&mut self, py: Python, table: Py<PyAny>) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract(py)?;

        for batch in batch_list {
            self.write_arrow_batch(py, batch)?;
        }
        Ok(())
    }

    /// Write Arrow batch data
    pub fn write_arrow_batch(&mut self, py: Python, batch: Py<PyAny>) -> PyResult<()> {
        // Extract number of rows and columns from the Arrow batch
        let num_rows: usize = batch.getattr(py, "num_rows")?.extract(py)?;
        let num_columns: usize = batch.getattr(py, "num_columns")?.extract(py)?;

        // Process each row in the batch
        for row_idx in 0..num_rows {
            let mut generic_row = fcore::row::GenericRow::new();

            // Extract values for each column in this row
            for col_idx in 0..num_columns {
                let column = batch.call_method1(py, "column", (col_idx,))?;
                let value = column.call_method1(py, "__getitem__", (row_idx,))?;

                // Convert the Python value to a Datum and add to the row
                let datum = self.convert_python_value_to_datum(py, value)?;
                generic_row.set_field(col_idx, datum);
            }

            // Append this row using the async append method
            TOKIO_RUNTIME.block_on(async {
                self.inner
                    .append(generic_row)
                    .await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&mut self, py: Python, df: Py<PyAny>) -> PyResult<()> {
        // Import pyarrow module
        let pyarrow = py.import("pyarrow")?;

        // Get the Table class from pyarrow module
        let table_class = pyarrow.getattr("Table")?;

        // Call Table.from_pandas(df) - from_pandas is a class method
        let pa_table = table_class.call_method1("from_pandas", (df,))?;

        // Then call write_arrow with the converted table
        self.write_arrow(py, pa_table.into())
    }

    /// Flush any pending data
    pub fn flush(&mut self) -> PyResult<()> {
        TOKIO_RUNTIME.block_on(async {
            self.inner
                .flush()
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))
        })
    }

    fn __repr__(&self) -> String {
        "AppendWriter()".to_string()
    }
}

impl AppendWriter {
    /// Create a AppendWriter from a core append writer
    pub fn from_core(append: fcore::client::AppendWriter) -> Self {
        Self { inner: append }
    }

    fn convert_python_value_to_datum(
        &self,
        py: Python,
        value: Py<PyAny>,
    ) -> PyResult<fcore::row::Datum<'static>> {
        use fcore::row::{Blob, Datum, F32, F64};

        // Check for None (null)
        if value.is_none(py) {
            return Ok(Datum::Null);
        }

        // Try to extract different types
        if let Ok(type_name) = value.bind(py).get_type().name() {
            if type_name == "StringScalar" {
                if let Ok(py_value) = value.call_method0(py, "as_py") {
                    if let Ok(str_val) = py_value.extract::<String>(py) {
                        let leaked_str: &'static str = Box::leak(str_val.into_boxed_str());
                        return Ok(Datum::String(leaked_str));
                    }
                }
            }
        }

        if let Ok(bool_val) = value.extract::<bool>(py) {
            return Ok(Datum::Bool(bool_val));
        }

        if let Ok(int_val) = value.extract::<i32>(py) {
            return Ok(Datum::Int32(int_val));
        }

        if let Ok(int_val) = value.extract::<i64>(py) {
            return Ok(Datum::Int64(int_val));
        }

        if let Ok(float_val) = value.extract::<f32>(py) {
            return Ok(Datum::Float32(F32::from(float_val)));
        }

        if let Ok(float_val) = value.extract::<f64>(py) {
            return Ok(Datum::Float64(F64::from(float_val)));
        }

        if let Ok(str_val) = value.extract::<String>(py) {
            // Convert String to &'static str by leaking memory
            // This is a simplified approach - in production, you might want better lifetime management
            let leaked_str: &'static str = Box::leak(str_val.into_boxed_str());
            return Ok(Datum::String(leaked_str));
        }

        if let Ok(bytes_val) = value.extract::<Vec<u8>>(py) {
            let blob = Blob::from(bytes_val);
            return Ok(Datum::Blob(blob));
        }

        // If we can't convert, return an error
        let type_name = value.bind(py).get_type().name()?;
        Err(FlussError::new_err(format!(
            "Cannot convert Python value to Datum: {type_name:?}"
        )))
    }
}

/// Scanner for reading log data from a Fluss table
#[pyclass]
pub struct LogScanner {
    inner: fcore::client::LogScanner,
    admin: fcore::client::FlussAdmin,
    table_info: fcore::metadata::TableInfo,
    #[allow(dead_code)]
    start_timestamp: Option<i64>,
    #[allow(dead_code)]
    end_timestamp: Option<i64>,
}

#[pymethods]
impl LogScanner {
    /// Subscribe to log data with timestamp range
    fn subscribe(
        &mut self,
        _start_timestamp: Option<i64>,
        _end_timestamp: Option<i64>,
    ) -> PyResult<()> {
        if _start_timestamp.is_some() {
            return Err(FlussError::new_err(
                "Specifying start_timestamp is not yet supported. Please use None.".to_string(),
            ));
        }
        if _end_timestamp.is_some() {
            return Err(FlussError::new_err(
                "Specifying end_timestamp is not yet supported. Please use None.".to_string(),
            ));
        }

        let num_buckets = self.table_info.get_num_buckets();
        for bucket_id in 0..num_buckets {
            let start_offset = EARLIEST_OFFSET;

            TOKIO_RUNTIME.block_on(async {
                self.inner
                    .subscribe(bucket_id, start_offset)
                    .await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    /// Convert all data to Arrow Table
    fn to_arrow(&self, py: Python) -> PyResult<Py<PyAny>> {
        use std::collections::HashMap;
        use std::time::Duration;

        let mut all_batches = Vec::new();

        let num_buckets = self.table_info.get_num_buckets();
        let bucket_ids: Vec<i32> = (0..num_buckets).collect();

        // todo: after supporting list_offsets with timestamp, we can use start_timestamp and end_timestamp here
        let mut stopping_offsets: HashMap<i32, i64> = TOKIO_RUNTIME
            .block_on(async {
                self.admin
                    .list_offsets(
                        &self.table_info.table_path,
                        bucket_ids.as_slice(),
                        OffsetSpec::Latest,
                    )
                    .await
            })
            .map_err(|e| FlussError::new_err(e.to_string()))?;

        if !stopping_offsets.is_empty() {
            loop {
                let batch_result = TOKIO_RUNTIME
                    .block_on(async { self.inner.poll(Duration::from_millis(500)).await });

                match batch_result {
                    Ok(scan_records) => {
                        let mut result_records: Vec<fcore::record::ScanRecord> = vec![];
                        for (bucket, records) in scan_records.into_records_by_buckets() {
                            let stopping_offset = stopping_offsets.get(&bucket.bucket_id());

                            if stopping_offset.is_none() {
                                // not to include this bucket, skip records for this bucket
                                // since we already reach end offset for this bucket
                                continue;
                            }
                            if let Some(last_record) = records.last() {
                                let offset = last_record.offset();
                                result_records.extend(records);
                                if offset >= stopping_offset.unwrap() - 1 {
                                    stopping_offsets.remove(&bucket.bucket_id());
                                }
                            }
                        }

                        if !result_records.is_empty() {
                            let arrow_batch = Utils::convert_scan_records_to_arrow(result_records);
                            all_batches.extend(arrow_batch);
                        }

                        // we have reach end offsets of all bucket
                        if stopping_offsets.is_empty() {
                            break;
                        }
                    }
                    Err(e) => return Err(FlussError::new_err(e.to_string())),
                }
            }
        }

        Utils::combine_batches_to_table(py, all_batches)
    }

    /// Convert all data to Pandas DataFrame
    fn to_pandas(&self, py: Python) -> PyResult<Py<PyAny>> {
        let arrow_table = self.to_arrow(py)?;

        // Convert Arrow Table to Pandas DataFrame using pyarrow
        let df = arrow_table.call_method0(py, "to_pandas")?;
        Ok(df)
    }

    fn __repr__(&self) -> String {
        format!("LogScanner(table={})", self.table_info.table_path)
    }
}

impl LogScanner {
    /// Create LogScanner from core LogScanner
    pub fn from_core(
        inner_scanner: fcore::client::LogScanner,
        admin: fcore::client::FlussAdmin,
        table_info: fcore::metadata::TableInfo,
    ) -> Self {
        Self {
            inner: inner_scanner,
            admin,
            table_info,
            start_timestamp: None,
            end_timestamp: None,
        }
    }
}
