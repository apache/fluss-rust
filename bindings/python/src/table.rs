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
use arrow::array::RecordBatch;
use arrow_pyarrow::FromPyArrow;
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

/// Internal enum to represent different projection types
enum ProjectionType {
    Indices(Vec<usize>),
    Names(Vec<String>),
}

#[pymethods]
impl FlussTable {
    /// Create a new append writer for the table
    fn new_append_writer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(&conn, metadata, table_info.clone());

            let table_append = fluss_table
                .new_append()
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let rust_writer = table_append.create_writer();

            let py_writer = AppendWriter::from_core(rust_writer, table_info);

            Python::attach(|py| Py::new(py, py_writer))
        })
    }

    /// Create a new log scanner for the table.
    ///
    /// Args:
    ///     project: Optional list of column indices (0-based) to include in the scan.
    ///     columns: Optional list of column names to include in the scan.
    ///
    /// Returns:
    ///     LogScanner, optionally with projection applied
    ///
    /// Note:
    ///     Specify only one of 'project' or 'columns'.
    ///     If neither is specified, all columns are included.
    ///     Rust side will validate the projection parameters.
    ///
    #[pyo3(signature = (project=None, columns=None))]
    pub fn new_log_scanner<'py>(
        &self,
        py: Python<'py>,
        project: Option<Vec<usize>>,
        columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let projection = match (project, columns) {
            (Some(_), Some(_)) => {
                return Err(FlussError::new_err(
                    "Specify only one of 'project' or 'columns'".to_string(),
                ));
            }
            (Some(indices), None) => Some(ProjectionType::Indices(indices)),
            (None, Some(names)) => Some(ProjectionType::Names(names)),
            (None, None) => None,
        };

        self.create_log_scanner_internal(py, projection)
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

    /// Internal helper to create log scanner with optional projection
    fn create_log_scanner_internal<'py>(
        &self,
        py: Python<'py>,
        projection: Option<ProjectionType>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table =
                fcore::client::FlussTable::new(&conn, metadata.clone(), table_info.clone());

            let mut table_scan = fluss_table.new_scan();

            // Apply projection if specified
            if let Some(proj) = projection {
                table_scan = match proj {
                    ProjectionType::Indices(indices) => {
                        table_scan.project(&indices).map_err(|e| {
                            FlussError::new_err(format!("Failed to project columns: {e}"))
                        })?
                    }
                    ProjectionType::Names(names) => {
                        // Convert Vec<String> to Vec<&str> for the API
                        let column_name_refs: Vec<&str> =
                            names.iter().map(|s| s.as_str()).collect();
                        table_scan.project_by_name(&column_name_refs).map_err(|e| {
                            FlussError::new_err(format!("Failed to project columns: {e}"))
                        })?
                    }
                };
            }

            let rust_scanner = table_scan
                .create_log_scanner()
                .map_err(|e| FlussError::new_err(format!("Failed to create log scanner: {e}")))?;

            let admin = conn
                .get_admin()
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let py_scanner = LogScanner::from_core(rust_scanner, admin, table_info.clone());
            Python::attach(|py| Py::new(py, py_scanner))
        })
    }
}

/// Writer for appending data to a Fluss table
#[pyclass]
pub struct AppendWriter {
    inner: Arc<fcore::client::AppendWriter>,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl AppendWriter {
    /// Write Arrow table data
    pub fn write_arrow(&self, py: Python, table: Py<PyAny>) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract(py)?;

        for batch in batch_list {
            self.write_arrow_batch(py, batch)?;
        }
        Ok(())
    }

    /// Write Arrow batch data
    pub fn write_arrow_batch(&self, py: Python, batch: Py<PyAny>) -> PyResult<()> {
        // This shares the underlying Arrow buffers without copying data
        let batch_bound = batch.bind(py);
        let rust_batch: RecordBatch = FromPyArrow::from_pyarrow_bound(batch_bound)
            .map_err(|e| FlussError::new_err(format!("Failed to convert RecordBatch: {e}")))?;

        let inner = self.inner.clone();
        // Release the GIL before blocking on async operation
        let result = py.detach(|| {
            TOKIO_RUNTIME.block_on(async { inner.append_arrow_batch(rust_batch).await })
        });

        result.map_err(|e| FlussError::new_err(e.to_string()))
    }

    /// Append a single row to the table
    pub fn append<'py>(
        &self,
        py: Python<'py>,
        row: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let generic_row = python_to_generic_row(row, &self.table_info)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .append(generic_row)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))
        })
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&self, py: Python, df: Py<PyAny>) -> PyResult<()> {
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
    pub fn flush(&self, py: Python) -> PyResult<()> {
        let inner = self.inner.clone();
        // Release the GIL before blocking on I/O
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                inner
                    .flush()
                    .await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    fn __repr__(&self) -> String {
        "AppendWriter()".to_string()
    }
}

impl AppendWriter {
    /// Create a AppendWriter from a core append writer
    pub fn from_core(
        append: fcore::client::AppendWriter,
        table_info: fcore::metadata::TableInfo,
    ) -> Self {
        Self {
            inner: Arc::new(append),
            table_info,
        }
    }
}

/// Represents different input shapes for a row
#[derive(FromPyObject)]
enum RowInput<'py> {
    Dict(Bound<'py, pyo3::types::PyDict>),
    Tuple(Bound<'py, pyo3::types::PyTuple>),
    List(Bound<'py, pyo3::types::PyList>),
}

/// Helper function to process sequence types (list/tuple) into datums
fn process_sequence_to_datums<'a, I>(
    values: I,
    len: usize,
    fields: &[fcore::metadata::DataField],
) -> PyResult<Vec<fcore::row::Datum<'static>>>
where
    I: Iterator<Item = Bound<'a, PyAny>>,
{
    if len != fields.len() {
        return Err(FlussError::new_err(format!(
            "Expected {} values, got {}",
            fields.len(),
            len
        )));
    }

    let mut datums = Vec::with_capacity(fields.len());
    for (i, (field, value)) in fields.iter().zip(values).enumerate() {
        datums.push(
            python_value_to_datum(&value, field.data_type()).map_err(|e| {
                FlussError::new_err(format!("Field '{}' (index {}): {}", field.name(), i, e))
            })?,
        );
    }
    Ok(datums)
}

/// Convert Python row (dict/list/tuple) to GenericRow based on schema
fn python_to_generic_row(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<fcore::row::GenericRow<'static>> {
    // Extract with user-friendly error message
    let row_input: RowInput = row.extract().map_err(|_| {
        let type_name = row
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        FlussError::new_err(format!(
            "Row must be a dict, list, or tuple; got {}",
            type_name
        ))
    })?;
    let schema = table_info.row_type();
    let fields = schema.fields();

    let datums = match row_input {
        RowInput::Dict(dict) => {
            // Strict: reject unknown keys (and also reject non-str keys nicely)
            for (k, _) in dict.iter() {
                let key_str = k.extract::<&str>().map_err(|_| {
                    let key_type = k
                        .get_type()
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    FlussError::new_err(format!("Row dict keys must be strings; got {}", key_type))
                })?;

                if fields.iter().all(|f| f.name() != key_str) {
                    let expected = fields
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(FlussError::new_err(format!(
                        "Unknown field '{}'. Expected fields: {}",
                        key_str, expected
                    )));
                }
            }

            let mut datums = Vec::with_capacity(fields.len());
            for field in fields {
                let value = dict.get_item(field.name())?.ok_or_else(|| {
                    FlussError::new_err(format!("Missing field: {}", field.name()))
                })?;
                datums.push(
                    python_value_to_datum(&value, field.data_type()).map_err(|e| {
                        FlussError::new_err(format!("Field '{}': {}", field.name(), e))
                    })?,
                );
            }
            datums
        }

        RowInput::List(list) => process_sequence_to_datums(list.iter(), list.len(), fields)?,

        RowInput::Tuple(tuple) => process_sequence_to_datums(tuple.iter(), tuple.len(), fields)?,
    };

    Ok(fcore::row::GenericRow { values: datums })
}

/// Convert Python value to Datum based on data type
fn python_value_to_datum(
    value: &Bound<PyAny>,
    data_type: &fcore::metadata::DataType,
) -> PyResult<fcore::row::Datum<'static>> {
    use fcore::row::{Datum, F32, F64};

    if value.is_none() {
        return Ok(Datum::Null);
    }

    match data_type {
        fcore::metadata::DataType::Boolean(_) => {
            let v: bool = value.extract()?;
            Ok(Datum::Bool(v))
        }
        fcore::metadata::DataType::TinyInt(_) => {
            // Strict type checking: reject bool for int columns
            if value.is_instance_of::<pyo3::types::PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for TinyInt column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i8 = value.extract()?;
            Ok(Datum::Int8(v))
        }
        fcore::metadata::DataType::SmallInt(_) => {
            if value.is_instance_of::<pyo3::types::PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for SmallInt column, got bool. Use 0 or 1 explicitly."
                        .to_string(),
                ));
            }
            let v: i16 = value.extract()?;
            Ok(Datum::Int16(v))
        }
        fcore::metadata::DataType::Int(_) => {
            if value.is_instance_of::<pyo3::types::PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for Int column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i32 = value.extract()?;
            Ok(Datum::Int32(v))
        }
        fcore::metadata::DataType::BigInt(_) => {
            if value.is_instance_of::<pyo3::types::PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for BigInt column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i64 = value.extract()?;
            Ok(Datum::Int64(v))
        }
        fcore::metadata::DataType::Float(_) => {
            let v: f32 = value.extract()?;
            Ok(Datum::Float32(F32::from(v)))
        }
        fcore::metadata::DataType::Double(_) => {
            let v: f64 = value.extract()?;
            Ok(Datum::Float64(F64::from(v)))
        }
        fcore::metadata::DataType::String(_) | fcore::metadata::DataType::Char(_) => {
            let v: String = value.extract()?;
            Ok(v.into())
        }
        fcore::metadata::DataType::Bytes(_) | fcore::metadata::DataType::Binary(_) => {
            // Efficient extraction: downcast to specific type and use bulk copy.
            // PyBytes::as_bytes() and PyByteArray::to_vec() are O(n) bulk copies of the underlying data.
            if let Ok(bytes) = value.downcast::<pyo3::types::PyBytes>() {
                Ok(bytes.as_bytes().to_vec().into())
            } else if let Ok(bytearray) = value.downcast::<pyo3::types::PyByteArray>() {
                Ok(bytearray.to_vec().into())
            } else {
                Err(FlussError::new_err(format!(
                    "Expected bytes or bytearray, got {}",
                    value.get_type().name()?
                )))
            }
        }
        _ => Err(FlussError::new_err(format!(
            "Unsupported data type for row-level operations: {:?}",
            data_type
        ))),
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
