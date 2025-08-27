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

use pyo3::prelude::*;
use crate::*;
use std::sync::Arc;
use pyo3_async_runtimes::tokio::future_into_py;
use crate::TOKIO_RUNTIME;

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
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata,
                table_info,
            );

            let table_append = fluss_table.new_append()
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let rust_writer = table_append.create_writer();

            let py_writer = AppendWriter::from_core(rust_writer);

            Python::with_gil(|py| {
                Py::new(py, py_writer)
            })
        })
    }

    /// Create a new log scanner for the table
    fn new_log_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata.clone(),
                table_info.clone(),
            );

            let table_scan = fluss_table.new_scan();

            let rust_scanner = table_scan.create_log_scanner();

            let py_scanner = LogScanner::from_core(
                rust_scanner,
                table_info.clone(),
            );

            Python::with_gil(|py| {
                Py::new(py, py_scanner)
            })
        })
    }

    /// synchronous version of new_log_scanner
    fn new_log_scanner_sync(&self) -> PyResult<LogScanner> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        let rust_scanner = TOKIO_RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata.clone(),
                table_info.clone(),
            );

            let table_scan = fluss_table.new_scan();
            table_scan.create_log_scanner()
        });

        let py_scanner = LogScanner::from_core(
            rust_scanner,
            table_info.clone(),
        );

        Ok(py_scanner)
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
        format!("FlussTable(path={}.{})", 
                self.table_path.database(), 
                self.table_path.table())
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
    pub fn write_arrow(&mut self, py: Python, table: PyObject) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<PyObject> = batches.extract(py)?;
        
        for batch in batch_list {
            self.write_arrow_batch(py, batch)?;
        }
        Ok(())
    }

    /// Write Arrow batch data
    pub fn write_arrow_batch(&mut self, py: Python, batch: PyObject) -> PyResult<()> {
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
                self.inner.append(generic_row).await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })?;
        }
        
        Ok(())
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&mut self, py: Python, df: PyObject) -> PyResult<()> {
        // Import pyarrow module
        let pyarrow = py.import("pyarrow")?;
        
        // Get the Table class from pyarrow module
        let table_class = pyarrow.getattr("Table")?;
        
        // Call Table.from_pandas(df) - from_pandas is a class method
        let pa_table = table_class.call_method1("from_pandas", (df,))?;
        
        // Then call write_arrow with the converted table
        self.write_arrow(py, pa_table.into_py(py))
    }

    /// Flush any pending data
    pub fn flush(&mut self) -> PyResult<()> {
        TOKIO_RUNTIME.block_on(async {
            self.inner.flush().await
                .map_err(|e| FlussError::new_err(e.to_string()))
        })
    }

    fn __repr__(&self) -> String {
        "AppendWriter()".to_string()
    }
}

impl AppendWriter {
    /// Create a TableWriter from a core append writer
    pub fn from_core(append: fcore::client::AppendWriter) -> Self {
        Self {
            inner: append,
        }
    }

    fn convert_python_value_to_datum(&self, py: Python, value: PyObject) -> PyResult<fcore::row::Datum<'static>> {
        use fcore::row::{Datum, F32, F64, Blob};
        
        // Check for None (null)
        if value.is_none(py) {
            return Ok(Datum::Null);
        }
        
        // Try to extract different types
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
        Err(FlussError::new_err(format!(
            "Cannot convert Python value to Datum: {:?}", 
            type_name
        )))
    }
}

/// Scanner for reading log data from a Fluss table
#[pyclass]
pub struct LogScanner {
    inner: fcore::client::LogScanner,
    table_info: fcore::metadata::TableInfo,
    start_timestamp: Option<i64>,
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
            // -2 for the earliest offset.
            let start_offset = -2;

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
    fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        use std::collections::HashMap;
        use std::time::Duration;

        let mut all_batches = Vec::new();

        let num_buckets = self.table_info.get_num_buckets();
        let bucket_ids: Vec<i32> = (0..num_buckets).collect();

        // todo: after supporting list_offsets with timestamp, we can use start_timestamp and end_timestamp here
        let target_offsets: HashMap<i32, i64> = if !bucket_ids.is_empty() {
            TOKIO_RUNTIME
                .block_on(async { self.inner.list_offsets_latest(bucket_ids).await })
                .map_err(|e| FlussError::new_err(e.to_string()))?
        } else {
            HashMap::new()
        };

        let mut current_offsets: HashMap<i32, i64> = HashMap::new();

        if !target_offsets.is_empty() {
            loop {
                let batch_result = TOKIO_RUNTIME.block_on(async {
                    self.inner.poll(Duration::from_millis(1000)).await
                });

                match batch_result {
                    Ok(scan_records) => {
                        if !scan_records.is_empty() {
                            let records_map = scan_records.into_records();
                            for (bucket_id, records) in &records_map {
                                if let Some(last_record) = records.last() {
                                    let max_offset_in_batch = last_record.offset();
                                    let entry = current_offsets.entry(*bucket_id).or_insert(0);
                                    *entry = (*entry).max(max_offset_in_batch);
                                }
                            }

                            let scan_records = fcore::record::ScanRecords::new(records_map);
                            let arrow_batch = Utils::convert_scan_records_to_arrow(scan_records);
                            all_batches.extend(arrow_batch);
                        }

                        if Self::check_if_done(&target_offsets, &current_offsets) {
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
    fn to_pandas(&self, py: Python) -> PyResult<PyObject> {
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
        inner: fcore::client::LogScanner,
        table_info: fcore::metadata::TableInfo,
    ) -> Self {
        Self {
            inner,
            table_info,
            start_timestamp: None,
            end_timestamp: None,
        }
    }
}
