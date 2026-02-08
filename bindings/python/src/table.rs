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
use arrow::array::RecordBatch as ArrowRecordBatch;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::SchemaRef;
use fluss::record::to_arrow_schema;
use fluss::rpc::message::OffsetSpec;
use pyo3::types::IntoPyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// Time conversion constants
const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60_000;
const MILLIS_PER_HOUR: i64 = 3_600_000;
const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

/// Represents a single scan record with metadata
#[pyclass]
pub struct ScanRecord {
    #[pyo3(get)]
    bucket: TableBucket,
    #[pyo3(get)]
    offset: i64,
    #[pyo3(get)]
    timestamp: i64,
    #[pyo3(get)]
    change_type: ChangeType,
    /// Store row as a Python dict directly
    row_dict: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl ScanRecord {
    /// Get the row data as a dictionary
    #[getter]
    pub fn row(&self, py: Python) -> Py<pyo3::types::PyDict> {
        self.row_dict.clone_ref(py)
    }

    fn __str__(&self) -> String {
        format!(
            "ScanRecord(bucket={}, offset={}, timestamp={}, change_type={})",
            self.bucket.__str__(),
            self.offset,
            self.timestamp,
            self.change_type.short_string()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl ScanRecord {
    /// Create a ScanRecord from core types
    pub fn from_core(
        py: Python,
        bucket: &fcore::metadata::TableBucket,
        record: &fcore::record::ScanRecord,
        row_type: &fcore::metadata::RowType,
    ) -> PyResult<Self> {
        let fields = row_type.fields();
        let row = record.row();
        let dict = pyo3::types::PyDict::new(py);

        for (pos, field) in fields.iter().enumerate() {
            let value = datum_to_python_value(py, row, pos, field.data_type())?;
            dict.set_item(field.name(), value)?;
        }

        Ok(ScanRecord {
            bucket: TableBucket::from_core(bucket.clone()),
            offset: record.offset(),
            timestamp: record.timestamp(),
            change_type: ChangeType::from_core(*record.change_type()),
            row_dict: dict.unbind(),
        })
    }
}

/// Represents a batch of records with metadata
#[pyclass]
pub struct RecordBatch {
    batch: Arc<ArrowRecordBatch>,
    #[pyo3(get)]
    bucket: TableBucket,
    #[pyo3(get)]
    base_offset: i64,
    #[pyo3(get)]
    last_offset: i64,
}

#[pymethods]
impl RecordBatch {
    /// Get the Arrow RecordBatch as PyArrow RecordBatch
    #[getter]
    pub fn batch(&self, py: Python) -> PyResult<Py<PyAny>> {
        let pyarrow_batch = self
            .batch
            .as_ref()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert batch: {e}")))?;
        Ok(pyarrow_batch.unbind())
    }

    fn __str__(&self) -> String {
        format!(
            "RecordBatch(bucket={}, base_offset={}, last_offset={}, rows={})",
            self.bucket.__str__(),
            self.base_offset,
            self.last_offset,
            self.batch.num_rows()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl RecordBatch {
    /// Create a RecordBatch from core ScanBatch
    pub fn from_scan_batch(scan_batch: fcore::record::ScanBatch) -> Self {
        RecordBatch {
            bucket: TableBucket::from_core(scan_batch.bucket().clone()),
            base_offset: scan_batch.base_offset(),
            last_offset: scan_batch.last_offset(),
            batch: Arc::new(scan_batch.into_batch()),
        }
    }
}

/// Represents a Fluss table for data operations
#[pyclass]
pub struct FlussTable {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_primary_key: bool,
}

/// Builder for creating log scanners with flexible configuration.
///
/// Use this builder to configure projection, and in the future, filters
/// before creating a log scanner.
#[pyclass]
pub struct TableScan {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    projection: Option<ProjectionType>,
}

/// Scanner type for internal use
enum ScannerType {
    Record,
    Batch,
}

#[pymethods]
impl TableScan {
    /// Project to specific columns by their indices.
    ///
    /// Args:
    ///     indices: List of column indices (0-based) to include in the scan.
    ///
    /// Returns:
    ///     Self for method chaining.
    pub fn project(mut slf: PyRefMut<'_, Self>, indices: Vec<usize>) -> PyRefMut<'_, Self> {
        slf.projection = Some(ProjectionType::Indices(indices));
        slf
    }

    /// Project to specific columns by their names.
    ///
    /// Args:
    ///     names: List of column names to include in the scan.
    ///
    /// Returns:
    ///     Self for method chaining.
    pub fn project_by_name(mut slf: PyRefMut<'_, Self>, names: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(ProjectionType::Names(names));
        slf
    }

    /// Create a record-based log scanner.
    ///
    /// Use this scanner with `poll()` to get individual records with metadata
    /// (offset, timestamp, change_type).
    ///
    /// Returns:
    ///     LogScanner for record-by-record scanning with `poll()`
    pub fn create_log_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.create_scanner_internal(py, ScannerType::Record)
    }

    /// Create a batch-based log scanner.
    ///
    /// Use this scanner with `poll_arrow()` to get Arrow Tables, or with
    /// `poll_batches()` to get individual batches with metadata.
    ///
    /// Returns:
    ///     LogScanner for batch-based scanning with `poll_arrow()` or `poll_batches()`
    pub fn create_batch_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.create_scanner_internal(py, ScannerType::Batch)
    }

    fn __repr__(&self) -> String {
        format!(
            "TableScan(table={}.{})",
            self.table_info.table_path.database(),
            self.table_info.table_path.table()
        )
    }
}

impl TableScan {
    fn create_scanner_internal<'py>(
        &self,
        py: Python<'py>,
        scanner_type: ScannerType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();
        let projection = self.projection.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(&conn, metadata, table_info.clone());

            let projection_indices = resolve_projection_indices(&projection, &table_info)?;
            let table_scan = apply_projection(fluss_table.new_scan(), projection)?;

            let admin = conn
                .get_admin()
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let (projected_schema, projected_row_type) =
                calculate_projected_types(&table_info, projection_indices)?;

            let scanner_kind = match scanner_type {
                ScannerType::Record => {
                    let s = table_scan.create_log_scanner().map_err(|e| {
                        FlussError::new_err(format!("Failed to create log scanner: {e}"))
                    })?;
                    ScannerKind::Record(s)
                }
                ScannerType::Batch => {
                    let s = table_scan.create_record_batch_log_scanner().map_err(|e| {
                        FlussError::new_err(format!("Failed to create batch scanner: {e}"))
                    })?;
                    ScannerKind::Batch(s)
                }
            };

            let py_scanner = LogScanner::new(
                scanner_kind,
                admin,
                table_info,
                projected_schema,
                projected_row_type,
            );

            Python::attach(|py| Py::new(py, py_scanner))
        })
    }
}

/// Internal enum to represent different projection types
#[derive(Clone)]
enum ProjectionType {
    Indices(Vec<usize>),
    Names(Vec<String>),
}

/// Resolve projection to column indices
fn resolve_projection_indices(
    projection: &Option<ProjectionType>,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<Option<Vec<usize>>> {
    match projection {
        Some(ProjectionType::Indices(indices)) => Ok(Some(indices.clone())),
        Some(ProjectionType::Names(names)) => {
            let schema = table_info.get_schema();
            let columns = schema.columns();
            let mut indices = Vec::with_capacity(names.len());
            for name in names {
                let idx = columns
                    .iter()
                    .position(|c| c.name() == name)
                    .ok_or_else(|| FlussError::new_err(format!("Column '{name}' not found")))?;
                indices.push(idx);
            }
            Ok(Some(indices))
        }
        None => Ok(None),
    }
}

/// Apply projection to table scan
fn apply_projection(
    table_scan: fcore::client::TableScan,
    projection: Option<ProjectionType>,
) -> PyResult<fcore::client::TableScan> {
    match projection {
        Some(ProjectionType::Indices(indices)) => table_scan
            .project(&indices)
            .map_err(|e| FlussError::new_err(format!("Failed to project columns: {e}"))),
        Some(ProjectionType::Names(names)) => {
            let column_name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            table_scan
                .project_by_name(&column_name_refs)
                .map_err(|e| FlussError::new_err(format!("Failed to project columns: {e}")))
        }
        None => Ok(table_scan),
    }
}

/// Calculate projected schema and row type from projection indices
fn calculate_projected_types(
    table_info: &fcore::metadata::TableInfo,
    projection_indices: Option<Vec<usize>>,
) -> PyResult<(SchemaRef, fcore::metadata::RowType)> {
    let full_schema = to_arrow_schema(table_info.get_row_type())
        .map_err(|e| FlussError::new_err(format!("Failed to get arrow schema: {e}")))?;
    let full_row_type = table_info.get_row_type();

    match projection_indices {
        Some(indices) => {
            let arrow_fields: Vec<_> = indices
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect();
            let row_fields: Vec<_> = indices
                .iter()
                .map(|&i| full_row_type.fields()[i].clone())
                .collect();
            Ok((
                Arc::new(arrow_schema::Schema::new(arrow_fields)),
                fcore::metadata::RowType::new(row_fields),
            ))
        }
        None => Ok((full_schema, full_row_type.clone())),
    }
}

#[pymethods]
impl FlussTable {
    /// Create a new table scan builder for configuring and creating log scanners.
    ///
    /// Use this method to create scanners with the builder pattern:
    /// Returns:
    ///     TableScan builder for configuring the scanner.
    pub fn new_scan(&self) -> TableScan {
        TableScan {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
            projection: None,
        }
    }

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

            let rust_writer = table_append
                .create_writer()
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let py_writer = AppendWriter::from_core(rust_writer, table_info);

            Python::attach(|py| Py::new(py, py_writer))
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

    /// Create a new lookuper for primary key lookups.
    ///
    /// This is only available for tables with a primary key.
    pub fn new_lookup(&self, _py: Python) -> PyResult<crate::Lookuper> {
        if !self.has_primary_key {
            return Err(FlussError::new_err(
                "Lookup is only supported for primary key tables",
            ));
        }

        crate::Lookuper::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        )
    }

    /// Create a new upsert writer for the table.
    ///
    /// This is only available for tables with a primary key.
    ///
    /// Args:
    ///     columns: Optional list of column names for partial update.
    ///              Only the specified columns will be updated.
    ///     column_indices: Optional list of column indices (0-based) for partial update.
    ///                     Alternative to `columns` parameter.
    #[pyo3(signature = (columns=None, column_indices=None))]
    pub fn new_upsert(
        &self,
        _py: Python,
        columns: Option<Vec<String>>,
        column_indices: Option<Vec<usize>>,
    ) -> PyResult<crate::UpsertWriter> {
        if !self.has_primary_key {
            return Err(FlussError::new_err(
                "Upsert is only supported for primary key tables",
            ));
        }

        // Validate that at most one parameter is specified
        if columns.is_some() && column_indices.is_some() {
            return Err(FlussError::new_err(
                "Specify only one of 'columns' or 'column_indices', not both",
            ));
        }

        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| FlussError::new_err(e.to_string()))?;

        crate::UpsertWriter::new(
            table_upsert,
            self.table_info.clone(),
            columns,
            column_indices,
        )
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
    inner: Arc<fcore::client::AppendWriter>,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl AppendWriter {
    /// Write Arrow table data (fire-and-forget, use flush() to ensure delivery)
    pub fn write_arrow(&self, py: Python, table: Py<PyAny>) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract(py)?;

        for batch in batch_list {
            // Drop the handle — fire-and-forget for bulk writes
            drop(self.write_arrow_batch(py, batch)?);
        }
        Ok(())
    }

    /// Write Arrow batch data.
    ///
    /// Returns:
    ///     WriteResultHandle that can be ignored (fire-and-forget) or
    ///     awaited via `handle.wait()` for server acknowledgment.
    pub fn write_arrow_batch(&self, py: Python, batch: Py<PyAny>) -> PyResult<WriteResultHandle> {
        // This shares the underlying Arrow buffers without copying data
        let batch_bound = batch.bind(py);
        let rust_batch: ArrowRecordBatch = FromPyArrow::from_pyarrow_bound(batch_bound)
            .map_err(|e| FlussError::new_err(format!("Failed to convert RecordBatch: {e}")))?;

        let result_future = self
            .inner
            .append_arrow_batch(rust_batch)
            .map_err(|e| FlussError::new_err(e.to_string()))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Append a single row to the table.
    ///
    /// Returns:
    ///     WriteResultHandle that can be ignored (fire-and-forget) or
    ///     awaited via `handle.wait()` for server acknowledgment.
    pub fn append(&self, row: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let generic_row = python_to_generic_row(row, &self.table_info)?;

        let result_future = self
            .inner
            .append(&generic_row)
            .map_err(|e| FlussError::new_err(e.to_string()))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&self, py: Python, df: Py<PyAny>) -> PyResult<()> {
        // Get the expected Arrow schema from the Fluss table
        let row_type = self.table_info.get_row_type();
        let expected_schema = fcore::record::to_arrow_schema(row_type)
            .map_err(|e| FlussError::new_err(format!("Failed to get table schema: {e}")))?;

        // Convert Arrow schema to PyArrow schema
        let py_schema = expected_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert schema: {e}")))?;

        // Import pyarrow module
        let pyarrow = py.import("pyarrow")?;

        // Get the Table class from pyarrow module
        let table_class = pyarrow.getattr("Table")?;

        // Call Table.from_pandas(df, schema=expected_schema) to ensure proper type casting
        let pa_table = table_class.call_method(
            "from_pandas",
            (df,),
            Some(&[("schema", py_schema)].into_py_dict(py)?),
        )?;

        // Then call write_arrow with the converted table
        self.write_arrow(py, pa_table.into())
    }

    /// Flush any pending data
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
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
pub fn python_to_generic_row(
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
            "Row must be a dict, list, or tuple; got {type_name}"
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
                    FlussError::new_err(format!("Row dict keys must be strings; got {key_type}"))
                })?;

                if fields.iter().all(|f| f.name() != key_str) {
                    let expected = fields
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(FlussError::new_err(format!(
                        "Unknown field '{key_str}'. Expected fields: {expected}"
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

/// Convert Python primary key values (dict/list/tuple) to GenericRow.
/// Only requires PK columns; non-PK columns are filled with Null.
/// For dict: keys should be PK column names.
/// For list/tuple: values should be PK values in PK column order.
pub fn python_pk_to_generic_row(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<fcore::row::GenericRow<'static>> {
    let schema = table_info.get_schema();
    let row_type = table_info.row_type();
    let fields = row_type.fields();
    let pk_indexes = schema.primary_key_indexes();
    let pk_names: Vec<&str> = schema.primary_key_column_names();

    if pk_indexes.is_empty() {
        return Err(FlussError::new_err(
            "Table has no primary key; cannot use PK-only row",
        ));
    }

    // Initialize all datums as Null
    let mut datums: Vec<fcore::row::Datum<'static>> = vec![fcore::row::Datum::Null; fields.len()];

    // Extract with user-friendly error message
    let row_input: RowInput = row.extract().map_err(|_| {
        let type_name = row
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        FlussError::new_err(format!(
            "PK row must be a dict, list, or tuple; got {type_name}"
        ))
    })?;

    match row_input {
        RowInput::Dict(dict) => {
            // Validate keys are PK columns
            for (k, _) in dict.iter() {
                let key_str = k.extract::<&str>().map_err(|_| {
                    let key_type = k
                        .get_type()
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    FlussError::new_err(format!("PK dict keys must be strings; got {key_type}"))
                })?;

                if !pk_names.contains(&key_str) {
                    return Err(FlussError::new_err(format!(
                        "Unknown PK field '{}'. Expected PK fields: {}",
                        key_str,
                        pk_names.join(", ")
                    )));
                }
            }

            // Extract PK values
            for (i, pk_idx) in pk_indexes.iter().enumerate() {
                let pk_name = pk_names[i];
                let field: &fcore::metadata::DataField = &fields[*pk_idx];
                let value = dict
                    .get_item(pk_name)?
                    .ok_or_else(|| FlussError::new_err(format!("Missing PK field: {pk_name}")))?;
                datums[*pk_idx] = python_value_to_datum(&value, field.data_type())
                    .map_err(|e| FlussError::new_err(format!("PK field '{pk_name}': {e}")))?;
            }
        }

        RowInput::List(list) => {
            if list.len() != pk_indexes.len() {
                return Err(FlussError::new_err(format!(
                    "PK list must have {} elements (PK columns), got {}",
                    pk_indexes.len(),
                    list.len()
                )));
            }
            for (i, pk_idx) in pk_indexes.iter().enumerate() {
                let field: &fcore::metadata::DataField = &fields[*pk_idx];
                let value = list.get_item(i)?;
                datums[*pk_idx] =
                    python_value_to_datum(&value, field.data_type()).map_err(|e| {
                        FlussError::new_err(format!("PK field '{}': {}", field.name(), e))
                    })?;
            }
        }

        RowInput::Tuple(tuple) => {
            if tuple.len() != pk_indexes.len() {
                return Err(FlussError::new_err(format!(
                    "PK tuple must have {} elements (PK columns), got {}",
                    pk_indexes.len(),
                    tuple.len()
                )));
            }
            for (i, pk_idx) in pk_indexes.iter().enumerate() {
                let field: &fcore::metadata::DataField = &fields[*pk_idx];
                let value = tuple.get_item(i)?;
                datums[*pk_idx] =
                    python_value_to_datum(&value, field.data_type()).map_err(|e| {
                        FlussError::new_err(format!("PK field '{}': {}", field.name(), e))
                    })?;
            }
        }
    }

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
        fcore::metadata::DataType::Decimal(decimal_type) => {
            python_decimal_to_datum(value, decimal_type.precision(), decimal_type.scale())
        }
        fcore::metadata::DataType::Date(_) => python_date_to_datum(value),
        fcore::metadata::DataType::Time(_) => python_time_to_datum(value),
        fcore::metadata::DataType::Timestamp(_) => python_datetime_to_timestamp_ntz(value),
        fcore::metadata::DataType::TimestampLTz(_) => python_datetime_to_timestamp_ltz(value),
        _ => Err(FlussError::new_err(format!(
            "Unsupported data type for row-level operations: {data_type}"
        ))),
    }
}

/// Convert Rust Datum to Python value based on data type.
/// This is the reverse of python_value_to_datum.
pub fn datum_to_python_value(
    py: Python,
    row: &dyn fcore::row::InternalRow,
    pos: usize,
    data_type: &fcore::metadata::DataType,
) -> PyResult<Py<PyAny>> {
    use fcore::metadata::DataType;

    // Check for null first
    if row.is_null_at(pos) {
        return Ok(py.None());
    }

    match data_type {
        DataType::Boolean(_) => Ok(row
            .get_boolean(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::TinyInt(_) => Ok(row
            .get_byte(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::SmallInt(_) => Ok(row
            .get_short(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Int(_) => Ok(row
            .get_int(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::BigInt(_) => Ok(row
            .get_long(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Float(_) => Ok(row
            .get_float(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Double(_) => Ok(row
            .get_double(pos)
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::String(_) => {
            let s = row.get_string(pos);
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        DataType::Char(char_type) => {
            let s = row.get_char(pos, char_type.length() as usize);
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        DataType::Bytes(_) => {
            let b = row.get_bytes(pos);
            Ok(pyo3::types::PyBytes::new(py, b).into_any().unbind())
        }
        DataType::Binary(binary_type) => {
            let b = row.get_binary(pos, binary_type.length());
            Ok(pyo3::types::PyBytes::new(py, b).into_any().unbind())
        }
        DataType::Decimal(decimal_type) => {
            let decimal = row.get_decimal(
                pos,
                decimal_type.precision() as usize,
                decimal_type.scale() as usize,
            );
            rust_decimal_to_python(py, &decimal)
        }
        DataType::Date(_) => {
            let date = row.get_date(pos);
            rust_date_to_python(py, date)
        }
        DataType::Time(_) => {
            let time = row.get_time(pos);
            rust_time_to_python(py, time)
        }
        DataType::Timestamp(ts_type) => {
            let ts = row.get_timestamp_ntz(pos, ts_type.precision());
            rust_timestamp_ntz_to_python(py, ts)
        }
        DataType::TimestampLTz(ts_type) => {
            let ts = row.get_timestamp_ltz(pos, ts_type.precision());
            rust_timestamp_ltz_to_python(py, ts)
        }
        _ => Err(FlussError::new_err(format!(
            "Unsupported data type for conversion to Python: {data_type}"
        ))),
    }
}

/// Convert Rust Decimal to Python decimal.Decimal
fn rust_decimal_to_python(py: Python, decimal: &fcore::row::Decimal) -> PyResult<Py<PyAny>> {
    let decimal_ty = get_decimal_type(py)?;
    let decimal_str = decimal.to_string();
    let py_decimal = decimal_ty.call1((decimal_str,))?;
    Ok(py_decimal.into_any().unbind())
}

/// Convert Rust Date (days since epoch) to Python datetime.date
fn rust_date_to_python(py: Python, date: fcore::row::Date) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDate;

    let days_since_epoch = date.get_inner();
    let epoch = jiff::civil::date(1970, 1, 1);
    let civil_date = epoch + jiff::Span::new().days(days_since_epoch as i64);

    let py_date = PyDate::new(
        py,
        civil_date.year() as i32,
        civil_date.month() as u8,
        civil_date.day() as u8,
    )?;
    Ok(py_date.into_any().unbind())
}

/// Convert Rust Time (millis since midnight) to Python datetime.time
fn rust_time_to_python(py: Python, time: fcore::row::Time) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyTime;

    let millis = time.get_inner() as i64;
    let hours = millis / MILLIS_PER_HOUR;
    let minutes = (millis % MILLIS_PER_HOUR) / MILLIS_PER_MINUTE;
    let seconds = (millis % MILLIS_PER_MINUTE) / MILLIS_PER_SECOND;
    let microseconds = (millis % MILLIS_PER_SECOND) * MICROS_PER_MILLI;

    let py_time = PyTime::new(
        py,
        hours as u8,
        minutes as u8,
        seconds as u8,
        microseconds as u32,
        None,
    )?;
    Ok(py_time.into_any().unbind())
}

/// Convert Rust TimestampNtz to Python naive datetime
fn rust_timestamp_ntz_to_python(py: Python, ts: fcore::row::TimestampNtz) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDateTime;

    let millis = ts.get_millisecond();
    let nanos = ts.get_nano_of_millisecond();
    let total_micros = millis * MICROS_PER_MILLI + (nanos as i64 / NANOS_PER_MICRO);

    // Convert to civil datetime via jiff
    let timestamp = jiff::Timestamp::from_microsecond(total_micros)
        .map_err(|e| FlussError::new_err(format!("Invalid timestamp: {e}")))?;
    let civil_dt = timestamp.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    let py_dt = PyDateTime::new(
        py,
        civil_dt.year() as i32,
        civil_dt.month() as u8,
        civil_dt.day() as u8,
        civil_dt.hour() as u8,
        civil_dt.minute() as u8,
        civil_dt.second() as u8,
        (civil_dt.subsec_nanosecond() / 1000) as u32, // microseconds
        None,
    )?;
    Ok(py_dt.into_any().unbind())
}

/// Convert Rust TimestampLtz to Python timezone-aware datetime (UTC)
fn rust_timestamp_ltz_to_python(py: Python, ts: fcore::row::TimestampLtz) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDateTime;

    let millis = ts.get_epoch_millisecond();
    let nanos = ts.get_nano_of_millisecond();
    let total_micros = millis * MICROS_PER_MILLI + (nanos as i64 / NANOS_PER_MICRO);

    // Convert to civil datetime via jiff
    let timestamp = jiff::Timestamp::from_microsecond(total_micros)
        .map_err(|e| FlussError::new_err(format!("Invalid timestamp: {e}")))?;
    let civil_dt = timestamp.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    let utc = get_utc_timezone(py)?;
    let py_dt = PyDateTime::new(
        py,
        civil_dt.year() as i32,
        civil_dt.month() as u8,
        civil_dt.day() as u8,
        civil_dt.hour() as u8,
        civil_dt.minute() as u8,
        civil_dt.second() as u8,
        (civil_dt.subsec_nanosecond() / 1000) as u32, // microseconds
        Some(&utc),
    )?;
    Ok(py_dt.into_any().unbind())
}

/// Convert an InternalRow to a Python dictionary
pub fn internal_row_to_dict(
    py: Python,
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<Py<PyAny>> {
    let row_type = table_info.row_type();
    let fields = row_type.fields();
    let dict = pyo3::types::PyDict::new(py);

    for (pos, field) in fields.iter().enumerate() {
        let value = datum_to_python_value(py, row, pos, field.data_type())?;
        dict.set_item(field.name(), value)?;
    }

    Ok(dict.into_any().unbind())
}

/// Cached decimal.Decimal type
/// Uses PyOnceLock for thread-safety and subinterpreter compatibility.
static DECIMAL_TYPE: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> =
    pyo3::sync::PyOnceLock::new();

/// Cached UTC timezone
static UTC_TIMEZONE: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

/// Cached UTC epoch type
static UTC_EPOCH: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

/// Get the cached decimal.Decimal type, importing it once per interpreter.
fn get_decimal_type(py: Python) -> PyResult<Bound<pyo3::types::PyType>> {
    let ty = DECIMAL_TYPE.get_or_try_init(py, || -> PyResult<_> {
        let decimal_mod = py.import("decimal")?;
        let decimal_ty = decimal_mod
            .getattr("Decimal")?
            .downcast_into::<pyo3::types::PyType>()?;
        Ok(decimal_ty.unbind())
    })?;
    Ok(ty.bind(py).clone())
}

/// Get the cached UTC timezone (datetime.timezone.utc), creating it once per interpreter.
fn get_utc_timezone(py: Python) -> PyResult<Bound<pyo3::types::PyTzInfo>> {
    let tz = UTC_TIMEZONE.get_or_try_init(py, || -> PyResult<_> {
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;
        Ok(utc.unbind())
    })?;
    // Downcast to PyTzInfo for use with PyDateTime::new()
    Ok(tz
        .bind(py)
        .clone()
        .downcast_into::<pyo3::types::PyTzInfo>()?)
}

/// Get the cached UTC epoch datetime, creating it once per interpreter.
fn get_utc_epoch(py: Python) -> PyResult<Bound<PyAny>> {
    let epoch = UTC_EPOCH.get_or_try_init(py, || -> PyResult<_> {
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;
        let epoch = datetime_mod
            .getattr("datetime")?
            .call1((1970, 1, 1, 0, 0, 0, 0, &utc))?;
        Ok(epoch.unbind())
    })?;
    Ok(epoch.bind(py).clone())
}

/// Validate that value is a decimal.Decimal instance.
fn ensure_is_decimal(value: &Bound<PyAny>) -> PyResult<()> {
    let decimal_ty = get_decimal_type(value.py())?;
    if !value.is_instance(&decimal_ty.into_any())? {
        return Err(FlussError::new_err(format!(
            "Expected decimal.Decimal, got {}",
            get_type_name(value)
        )));
    }
    Ok(())
}

/// Convert Python decimal.Decimal to Datum::Decimal.
/// Only accepts decimal.Decimal
fn python_decimal_to_datum(
    value: &Bound<PyAny>,
    precision: u32,
    scale: u32,
) -> PyResult<fcore::row::Datum<'static>> {
    use std::str::FromStr;

    ensure_is_decimal(value)?;

    let decimal_str: String = value.str()?.extract()?;
    let bd = bigdecimal::BigDecimal::from_str(&decimal_str).map_err(|e| {
        FlussError::new_err(format!("Failed to parse decimal '{decimal_str}': {e}"))
    })?;

    let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale).map_err(|e| {
        FlussError::new_err(format!(
            "Failed to convert decimal '{decimal_str}' to DECIMAL({precision}, {scale}): {e}"
        ))
    })?;

    Ok(fcore::row::Datum::Decimal(decimal))
}

/// Convert Python datetime.date to Datum::Date.
fn python_date_to_datum(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    use pyo3::types::{PyDate, PyDateAccess, PyDateTime};

    // Reject datetime.datetime (subclass of date) - use timestamp columns for those
    if value.downcast::<PyDateTime>().is_ok() {
        return Err(FlussError::new_err(
            "Expected datetime.date, got datetime.datetime. Use a TIMESTAMP column for datetime values.",
        ));
    }

    let date = value.downcast::<PyDate>().map_err(|_| {
        FlussError::new_err(format!(
            "Expected datetime.date, got {}",
            get_type_name(value)
        ))
    })?;

    let year = date.get_year();
    let month = date.get_month();
    let day = date.get_day();

    // Calculate days since Unix epoch (1970-01-01)
    let civil_date = jiff::civil::date(year as i16, month as i8, day as i8);
    let epoch = jiff::civil::date(1970, 1, 1);
    let days_since_epoch = (civil_date - epoch).get_days();

    Ok(fcore::row::Datum::Date(fcore::row::Date::new(
        days_since_epoch,
    )))
}

/// Convert Python datetime.time to Datum::Time.
/// Uses PyO3's native PyTime type for efficient access.
///
/// Note: Fluss TIME is always stored as milliseconds since midnight (i32) regardless
/// of the schema's precision setting. This matches the Java Fluss wire protocol.
/// Sub-millisecond precision (microseconds not divisible by 1000) will raise an error
/// to prevent silent data loss and ensure fail-fast behavior.
fn python_time_to_datum(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    use pyo3::types::{PyTime, PyTimeAccess};

    let time = value.downcast::<PyTime>().map_err(|_| {
        FlussError::new_err(format!(
            "Expected datetime.time, got {}",
            get_type_name(value)
        ))
    })?;

    let hour = time.get_hour() as i32;
    let minute = time.get_minute() as i32;
    let second = time.get_second() as i32;
    let microsecond = time.get_microsecond() as i32;

    // Strict validation: reject sub-millisecond precision
    if microsecond % MICROS_PER_MILLI as i32 != 0 {
        return Err(FlussError::new_err(format!(
            "TIME values with sub-millisecond precision are not supported. \
             Got time with {microsecond} microseconds (not divisible by 1000). \
             Fluss stores TIME as milliseconds since midnight. \
             Please round to milliseconds before insertion."
        )));
    }

    // Convert to milliseconds since midnight
    let millis = hour * MILLIS_PER_HOUR as i32
        + minute * MILLIS_PER_MINUTE as i32
        + second * MILLIS_PER_SECOND as i32
        + microsecond / MICROS_PER_MILLI as i32;

    Ok(fcore::row::Datum::Time(fcore::row::Time::new(millis)))
}

/// Convert Python datetime-like object to Datum::TimestampNtz.
/// Supports: datetime.datetime (naive preferred), pd.Timestamp, np.datetime64
fn python_datetime_to_timestamp_ntz(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    let (epoch_millis, nano_of_milli) = extract_datetime_components_ntz(value)?;

    let ts = fcore::row::TimestampNtz::from_millis_nanos(epoch_millis, nano_of_milli)
        .map_err(|e| FlussError::new_err(format!("Failed to create TimestampNtz: {e}")))?;

    Ok(fcore::row::Datum::TimestampNtz(ts))
}

/// Convert Python datetime-like object to Datum::TimestampLtz.
/// For naive datetimes, assumes UTC. For aware datetimes, converts to UTC.
/// Supports: datetime.datetime, pd.Timestamp, np.datetime64
fn python_datetime_to_timestamp_ltz(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    let (epoch_millis, nano_of_milli) = extract_datetime_components_ltz(value)?;

    let ts = fcore::row::TimestampLtz::from_millis_nanos(epoch_millis, nano_of_milli)
        .map_err(|e| FlussError::new_err(format!("Failed to create TimestampLtz: {e}")))?;

    Ok(fcore::row::Datum::TimestampLtz(ts))
}

/// Extract epoch milliseconds for TimestampNtz (wall-clock time, no timezone conversion).
/// Uses integer arithmetic to avoid float precision issues.
/// For clarity, tz-aware datetimes are rejected - use TimestampLtz for those.
fn extract_datetime_components_ntz(value: &Bound<PyAny>) -> PyResult<(i64, i32)> {
    use pyo3::types::PyDateTime;

    // Try PyDateTime first
    if let Ok(dt) = value.downcast::<PyDateTime>() {
        // Reject tz-aware datetime for NTZ - it's ambiguous what the user wants
        let tzinfo = dt.getattr("tzinfo")?;
        if !tzinfo.is_none() {
            return Err(FlussError::new_err(
                "TIMESTAMP (without timezone) requires a naive datetime. \
                 Got timezone-aware datetime. Either remove tzinfo or use TIMESTAMP_LTZ column.",
            ));
        }
        return datetime_to_epoch_millis_as_utc(dt);
    }

    // Check for pandas Timestamp by verifying module name
    if is_pandas_timestamp(value) {
        // For NTZ, reject tz-aware pandas Timestamps for consistency with datetime behavior
        if let Ok(tz) = value.getattr("tz") {
            if !tz.is_none() {
                return Err(FlussError::new_err(
                    "TIMESTAMP (without timezone) requires a naive pd.Timestamp. \
                     Got timezone-aware Timestamp. Either use tz_localize(None) or use TIMESTAMP_LTZ column.",
                ));
            }
        }
        // Naive pandas Timestamp: .value is nanoseconds since epoch (wall-clock as UTC)
        let nanos: i64 = value.getattr("value")?.extract()?;
        return Ok(nanos_to_millis_and_submillis(nanos));
    }

    // Try to_pydatetime() for objects that support it
    if let Ok(py_dt) = value.call_method0("to_pydatetime") {
        if let Ok(dt) = py_dt.downcast::<PyDateTime>() {
            let tzinfo = dt.getattr("tzinfo")?;
            if !tzinfo.is_none() {
                return Err(FlussError::new_err(
                    "TIMESTAMP (without timezone) requires a naive datetime. \
                     Got timezone-aware value. Use TIMESTAMP_LTZ column instead.",
                ));
            }
            return datetime_to_epoch_millis_as_utc(dt);
        }
    }

    Err(FlussError::new_err(format!(
        "Expected naive datetime.datetime or pd.Timestamp, got {}",
        get_type_name(value)
    )))
}

/// Extract epoch milliseconds for TimestampLtz (instant in time, UTC-based).
/// For naive datetimes, assumes UTC. For aware datetimes, converts to UTC.
fn extract_datetime_components_ltz(value: &Bound<PyAny>) -> PyResult<(i64, i32)> {
    use pyo3::types::PyDateTime;

    // Try PyDateTime first
    if let Ok(dt) = value.downcast::<PyDateTime>() {
        // Check if timezone-aware
        let tzinfo = dt.getattr("tzinfo")?;
        if tzinfo.is_none() {
            // Naive datetime: assume UTC (treat components as UTC time)
            return datetime_to_epoch_millis_as_utc(dt);
        } else {
            // Aware datetime: use timedelta from epoch to get correct UTC instant
            return datetime_to_epoch_millis_utc_aware(dt);
        }
    }

    // Check for pandas Timestamp
    if is_pandas_timestamp(value) {
        // pandas Timestamp.value is always nanoseconds since UTC epoch
        let nanos: i64 = value.getattr("value")?.extract()?;
        return Ok(nanos_to_millis_and_submillis(nanos));
    }

    // Try to_pydatetime()
    if let Ok(py_dt) = value.call_method0("to_pydatetime") {
        if let Ok(dt) = py_dt.downcast::<PyDateTime>() {
            let tzinfo = dt.getattr("tzinfo")?;
            if tzinfo.is_none() {
                return datetime_to_epoch_millis_as_utc(dt);
            } else {
                return datetime_to_epoch_millis_utc_aware(dt);
            }
        }
    }

    Err(FlussError::new_err(format!(
        "Expected datetime.datetime or pd.Timestamp, got {}",
        get_type_name(value)
    )))
}

/// Convert datetime components to epoch milliseconds treating them as UTC
fn datetime_to_epoch_millis_as_utc(
    dt: &pyo3::Bound<'_, pyo3::types::PyDateTime>,
) -> PyResult<(i64, i32)> {
    use pyo3::types::{PyDateAccess, PyTimeAccess};

    let year = dt.get_year();
    let month = dt.get_month();
    let day = dt.get_day();
    let hour = dt.get_hour();
    let minute = dt.get_minute();
    let second = dt.get_second();
    let microsecond = dt.get_microsecond();

    // Create jiff civil datetime and convert to UTC timestamp
    // Safe casts: hour (0-23), minute (0-59), second (0-59) all fit in i8
    let civil_dt = jiff::civil::date(year as i16, month as i8, day as i8).at(
        hour as i8,
        minute as i8,
        second as i8,
        microsecond as i32 * 1000,
    );

    let timestamp = jiff::tz::Offset::UTC
        .to_timestamp(civil_dt)
        .map_err(|e| FlussError::new_err(format!("Invalid datetime: {e}")))?;

    let millis = timestamp.as_millisecond();
    let nano_of_milli = (timestamp.subsec_nanosecond() % NANOS_PER_MILLI as i32) as i32;

    Ok((millis, nano_of_milli))
}

/// Convert timezone-aware datetime to epoch milliseconds using Python's timedelta.
/// This correctly handles timezone conversions by computing (dt - UTC_EPOCH).
/// The UTC epoch is cached for performance.
fn datetime_to_epoch_millis_utc_aware(
    dt: &pyo3::Bound<'_, pyo3::types::PyDateTime>,
) -> PyResult<(i64, i32)> {
    use pyo3::types::{PyDelta, PyDeltaAccess};

    let py = dt.py();
    let epoch = get_utc_epoch(py)?;

    // Compute delta = dt - epoch (this handles timezone conversion correctly)
    let delta = dt.call_method1("__sub__", (epoch,))?;
    let delta = delta.downcast::<PyDelta>()?;

    // Extract components using integer arithmetic
    let days = delta.get_days() as i64;
    let seconds = delta.get_seconds() as i64;
    let microseconds = delta.get_microseconds() as i64;

    // Total milliseconds (note: days can be negative for dates before epoch)
    let total_micros = days * MICROS_PER_DAY + seconds * MICROS_PER_SECOND + microseconds;
    let millis = total_micros / MICROS_PER_MILLI;
    let nano_of_milli = ((total_micros % MICROS_PER_MILLI) * MICROS_PER_MILLI) as i32;

    // Handle negative microseconds remainder
    let (millis, nano_of_milli) = if nano_of_milli < 0 {
        (millis - 1, nano_of_milli + NANOS_PER_MILLI as i32)
    } else {
        (millis, nano_of_milli)
    };

    Ok((millis, nano_of_milli))
}

/// Convert nanoseconds to (milliseconds, nano_of_millisecond)
fn nanos_to_millis_and_submillis(nanos: i64) -> (i64, i32) {
    let millis = nanos / NANOS_PER_MILLI;
    let nano_of_milli = (nanos % NANOS_PER_MILLI) as i32;

    // Handle negative nanoseconds correctly (Euclidean remainder)
    if nano_of_milli < 0 {
        (millis - 1, nano_of_milli + NANOS_PER_MILLI as i32)
    } else {
        (millis, nano_of_milli)
    }
}

/// Check if value is a pandas Timestamp by examining its type.
fn is_pandas_timestamp(value: &Bound<PyAny>) -> bool {
    // Check module and class name to avoid importing pandas
    if let Ok(cls) = value.get_type().getattr("__module__") {
        if let Ok(module) = cls.extract::<&str>() {
            if module.starts_with("pandas") {
                if let Ok(name) = value.get_type().getattr("__name__") {
                    if let Ok(name_str) = name.extract::<&str>() {
                        return name_str == "Timestamp";
                    }
                }
            }
        }
    }
    false
}

/// Get type name
fn get_type_name(value: &Bound<PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Wraps the two scanner variants so we never have an impossible state
/// (both None or both Some).
enum ScannerKind {
    Record(fcore::client::LogScanner),
    Batch(fcore::client::RecordBatchLogScanner),
}

impl ScannerKind {
    fn as_record(&self) -> PyResult<&fcore::client::LogScanner> {
        match self {
            Self::Record(s) => Ok(s),
            Self::Batch(_) => Err(FlussError::new_err(
                "poll() requires a record-based scanner. Use new_scan().create_log_scanner().",
            )),
        }
    }

    fn as_batch(&self) -> PyResult<&fcore::client::RecordBatchLogScanner> {
        match self {
            Self::Batch(s) => Ok(s),
            Self::Record(_) => Err(FlussError::new_err(
                "This method requires a batch-based scanner. Use new_scan().create_batch_scanner().",
            )),
        }
    }
}

/// Dispatch a method call to whichever scanner variant is active.
/// Both `LogScanner` and `RecordBatchLogScanner` share the same subscribe interface.
macro_rules! with_scanner {
    ($scanner:expr, $method:ident($($arg:expr),*)) => {
        match $scanner {
            ScannerKind::Record(s) => s.$method($($arg),*).await,
            ScannerKind::Batch(s) => s.$method($($arg),*).await,
        }
    };
}

/// Scanner for reading log data from a Fluss table.
///
/// This scanner supports two modes:
/// - Record-based scanning via `poll()` - returns individual records with metadata
/// - Batch-based scanning via `poll_arrow()` / `poll_batches()` - returns Arrow batches
#[pyclass]
pub struct LogScanner {
    scanner: ScannerKind,
    admin: fcore::client::FlussAdmin,
    table_info: fcore::metadata::TableInfo,
    /// The projected Arrow schema to use for empty table creation
    projected_schema: SchemaRef,
    /// The projected row type to use for record-based scanning
    projected_row_type: fcore::metadata::RowType,
    /// Cache for partition_id -> partition_name mapping (avoids repeated list_partition_infos calls)
    partition_name_cache: std::sync::RwLock<Option<HashMap<i64, String>>>,
}

#[pymethods]
impl LogScanner {
    /// Subscribe to a single bucket at a specific offset (non-partitioned tables).
    ///
    /// Args:
    ///     bucket_id: The bucket ID to subscribe to
    ///     start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
    fn subscribe(&self, py: Python, bucket_id: i32, start_offset: i64) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.scanner, subscribe(bucket_id, start_offset))
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    /// Subscribe to multiple buckets at specified offsets (non-partitioned tables).
    ///
    /// Args:
    ///     bucket_offsets: A dict mapping bucket_id -> start_offset
    fn subscribe_buckets(&self, py: Python, bucket_offsets: HashMap<i32, i64>) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.scanner, subscribe_buckets(&bucket_offsets))
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    /// Subscribe to a bucket within a specific partition (partitioned tables only).
    ///
    /// Args:
    ///     partition_id: The partition ID (from PartitionInfo.partition_id)
    ///     bucket_id: The bucket ID within the partition
    ///     start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
    fn subscribe_partition(
        &self,
        py: Python,
        partition_id: i64,
        bucket_id: i32,
        start_offset: i64,
    ) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(
                    &self.scanner,
                    subscribe_partition(partition_id, bucket_id, start_offset)
                )
                .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    /// Subscribe to multiple partition+bucket combinations at once (partitioned tables only).
    ///
    /// Args:
    ///     partition_bucket_offsets: A dict mapping (partition_id, bucket_id) tuples to start_offsets
    fn subscribe_partition_buckets(
        &self,
        py: Python,
        partition_bucket_offsets: HashMap<(i64, i32), i64>,
    ) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(
                    &self.scanner,
                    subscribe_partition_buckets(&partition_bucket_offsets)
                )
                .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    /// Unsubscribe from a specific partition bucket (partitioned tables only).
    ///
    /// Args:
    ///     partition_id: The partition ID to unsubscribe from
    ///     bucket_id: The bucket ID within the partition
    fn unsubscribe_partition(&self, py: Python, partition_id: i64, bucket_id: i32) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(
                    &self.scanner,
                    unsubscribe_partition(partition_id, bucket_id)
                )
                .map_err(|e| FlussError::new_err(e.to_string()))
            })
        })
    }

    /// Poll for individual records with metadata.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for records
    ///
    /// Returns:
    ///     List of ScanRecord objects, each containing bucket, offset, timestamp,
    ///     change_type, and row data as a dictionary.
    ///
    /// Note:
    ///     - Requires a record-based scanner (created with new_scan().create_log_scanner())
    ///     - Returns an empty list if no records are available
    ///     - When timeout expires, returns an empty list (NOT an error)
    fn poll(&self, py: Python, timeout_ms: i64) -> PyResult<Vec<ScanRecord>> {
        let scanner = self.scanner.as_record()?;

        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scan_records = py
            .detach(|| TOKIO_RUNTIME.block_on(async { scanner.poll(timeout).await }))
            .map_err(|e| FlussError::new_err(e.to_string()))?;

        // Convert ScanRecords to Python ScanRecord list
        // Use projected_row_type to handle column projection correctly
        let row_type = &self.projected_row_type;
        let mut result = Vec::new();

        for (bucket, records) in scan_records.into_records_by_buckets() {
            for record in records {
                let scan_record = ScanRecord::from_core(py, &bucket, &record, row_type)?;
                result.push(scan_record);
            }
        }

        Ok(result)
    }

    /// Poll for batches with metadata.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for batches
    ///
    /// Returns:
    ///     List of RecordBatch objects, each containing the Arrow batch along with
    ///     bucket, base_offset, and last_offset metadata.
    ///
    /// Note:
    ///     - Requires a batch-based scanner (created with new_scan().create_batch_scanner())
    ///     - Returns an empty list if no batches are available
    ///     - When timeout expires, returns an empty list (NOT an error)
    fn poll_batches(&self, py: Python, timeout_ms: i64) -> PyResult<Vec<RecordBatch>> {
        let scanner = self.scanner.as_batch()?;

        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scan_batches = py
            .detach(|| TOKIO_RUNTIME.block_on(async { scanner.poll(timeout).await }))
            .map_err(|e| FlussError::new_err(e.to_string()))?;

        // Convert ScanBatch to RecordBatch with metadata
        let result = scan_batches
            .into_iter()
            .map(RecordBatch::from_scan_batch)
            .collect();

        Ok(result)
    }

    /// Poll for new records as an Arrow Table.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for records
    ///
    /// Returns:
    ///     PyArrow Table containing the polled records (batches merged)
    ///
    /// Note:
    ///     - Requires a batch-based scanner (created with new_scan().create_batch_scanner())
    ///     - Returns an empty table (with correct schema) if no records are available
    ///     - When timeout expires, returns an empty table (NOT an error)
    fn poll_arrow(&self, py: Python, timeout_ms: i64) -> PyResult<Py<PyAny>> {
        let scanner = self.scanner.as_batch()?;

        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scan_batches = py
            .detach(|| TOKIO_RUNTIME.block_on(async { scanner.poll(timeout).await }))
            .map_err(|e| FlussError::new_err(e.to_string()))?;

        // Convert ScanBatch to Arrow batches
        if scan_batches.is_empty() {
            return self.create_empty_table(py);
        }

        let arrow_batches: Vec<_> = scan_batches
            .into_iter()
            .map(|scan_batch| Arc::new(scan_batch.into_batch()))
            .collect();

        Utils::combine_batches_to_table(py, arrow_batches)
    }

    /// Create an empty PyArrow table with the correct (projected) schema
    fn create_empty_table(&self, py: Python) -> PyResult<Py<PyAny>> {
        // Use the projected schema stored in the scanner
        let py_schema = self
            .projected_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert schema: {e}")))?;

        let pyarrow = py.import("pyarrow")?;
        let empty_table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (vec![] as Vec<Py<PyAny>>, py_schema))?;

        Ok(empty_table.into())
    }

    /// Convert all data to Arrow Table.
    ///
    /// Reads from currently subscribed buckets until reaching their latest offsets.
    /// Works for both partitioned and non-partitioned tables.
    ///
    /// You must call subscribe(), subscribe_buckets(), subscribe_partition(), or subscribe_partition_buckets() first.
    ///
    /// Returns:
    ///     PyArrow Table containing all data from subscribed buckets
    fn to_arrow(&self, py: Python) -> PyResult<Py<PyAny>> {
        let scanner = self.scanner.as_batch()?;
        let subscribed = scanner.get_subscribed_buckets();
        if subscribed.is_empty() {
            return Err(FlussError::new_err(
                "No buckets subscribed. Call subscribe(), subscribe_buckets(), subscribe_partition(), or subscribe_partition_buckets() first.",
            ));
        }

        // 2. Query latest offsets for all subscribed buckets
        let stopping_offsets = self.query_latest_offsets(py, &subscribed)?;

        // 3. Poll until all buckets reach their stopping offsets
        self.poll_until_offsets(py, stopping_offsets)
    }

    /// Convert all data to Pandas DataFrame.
    ///
    /// Reads from currently subscribed buckets until reaching their latest offsets.
    /// Works for both partitioned and non-partitioned tables.
    ///
    /// You must call subscribe(), subscribe_buckets(), subscribe_partition(), or subscribe_partition_buckets() first.
    ///
    /// Returns:
    ///     Pandas DataFrame containing all data from subscribed buckets
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
    fn new(
        scanner: ScannerKind,
        admin: fcore::client::FlussAdmin,
        table_info: fcore::metadata::TableInfo,
        projected_schema: SchemaRef,
        projected_row_type: fcore::metadata::RowType,
    ) -> Self {
        Self {
            scanner,
            admin,
            table_info,
            projected_schema,
            projected_row_type,
            partition_name_cache: std::sync::RwLock::new(None),
        }
    }

    /// Get partition_id -> partition_name mapping, using cache if available
    fn get_partition_name_map(
        &self,
        py: Python,
        table_path: &fcore::metadata::TablePath,
    ) -> PyResult<HashMap<i64, String>> {
        // Check cache first (read lock)
        {
            let cache = self.partition_name_cache.read().unwrap();
            if let Some(map) = cache.as_ref() {
                return Ok(map.clone());
            }
        }

        // Fetch partition infos (releases GIL during async call)
        let partition_infos: Vec<fcore::metadata::PartitionInfo> = py
            .detach(|| {
                TOKIO_RUNTIME.block_on(async { self.admin.list_partition_infos(table_path).await })
            })
            .map_err(|e| FlussError::new_err(format!("Failed to list partition infos: {e}")))?;

        // Build and cache the mapping
        let map: HashMap<i64, String> = partition_infos
            .into_iter()
            .map(|info| (info.get_partition_id(), info.get_partition_name()))
            .collect();

        // Store in cache (write lock)
        {
            let mut cache = self.partition_name_cache.write().unwrap();
            *cache = Some(map.clone());
        }

        Ok(map)
    }

    /// Query latest offsets for subscribed buckets (handles both partitioned and non-partitioned)
    fn query_latest_offsets(
        &self,
        py: Python,
        subscribed: &[(fcore::metadata::TableBucket, i64)],
    ) -> PyResult<HashMap<fcore::metadata::TableBucket, i64>> {
        let scanner = self.scanner.as_batch()?;
        let is_partitioned = scanner.is_partitioned();
        let table_path = &self.table_info.table_path;

        if !is_partitioned {
            // Non-partitioned: simple case - just query all bucket IDs
            let bucket_ids: Vec<i32> = subscribed.iter().map(|(tb, _)| tb.bucket_id()).collect();

            let offsets: HashMap<i32, i64> = py
                .detach(|| {
                    TOKIO_RUNTIME.block_on(async {
                        self.admin
                            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
                            .await
                    })
                })
                .map_err(|e| FlussError::new_err(format!("Failed to list offsets: {e}")))?;

            // Convert to TableBucket-keyed map
            let table_id = self.table_info.table_id;
            Ok(offsets
                .into_iter()
                .filter(|(_, offset)| *offset > 0)
                .map(|(bucket_id, offset)| {
                    (
                        fcore::metadata::TableBucket::new(table_id, bucket_id),
                        offset,
                    )
                })
                .collect())
        } else {
            // Partitioned: need to query per partition
            self.query_partitioned_offsets(py, subscribed)
        }
    }

    /// Query offsets for partitioned table subscriptions
    fn query_partitioned_offsets(
        &self,
        py: Python,
        subscribed: &[(fcore::metadata::TableBucket, i64)],
    ) -> PyResult<HashMap<fcore::metadata::TableBucket, i64>> {
        let table_path = &self.table_info.table_path;

        // Get partition_id -> partition_name mapping (cached)
        let partition_id_to_name = self.get_partition_name_map(py, table_path)?;

        // Group subscribed buckets by partition_id
        let mut by_partition: HashMap<i64, Vec<i32>> = HashMap::new();
        for (tb, _) in subscribed {
            if let Some(partition_id) = tb.partition_id() {
                by_partition
                    .entry(partition_id)
                    .or_default()
                    .push(tb.bucket_id());
            }
        }

        // Query offsets for each partition
        let mut result: HashMap<fcore::metadata::TableBucket, i64> = HashMap::new();
        let table_id = self.table_info.table_id;

        for (partition_id, bucket_ids) in by_partition {
            let partition_name = partition_id_to_name.get(&partition_id).ok_or_else(|| {
                FlussError::new_err(format!("Unknown partition_id: {partition_id}"))
            })?;

            let offsets: HashMap<i32, i64> = py
                .detach(|| {
                    TOKIO_RUNTIME.block_on(async {
                        self.admin
                            .list_partition_offsets(
                                table_path,
                                partition_name,
                                &bucket_ids,
                                OffsetSpec::Latest,
                            )
                            .await
                    })
                })
                .map_err(|e| {
                    FlussError::new_err(format!(
                        "Failed to list offsets for partition {partition_name}: {e}"
                    ))
                })?;

            for (bucket_id, offset) in offsets {
                if offset > 0 {
                    let tb = fcore::metadata::TableBucket::new_with_partition(
                        table_id,
                        Some(partition_id),
                        bucket_id,
                    );
                    result.insert(tb, offset);
                }
            }
        }

        Ok(result)
    }

    /// Poll until all buckets reach their stopping offsets
    fn poll_until_offsets(
        &self,
        py: Python,
        mut stopping_offsets: HashMap<fcore::metadata::TableBucket, i64>,
    ) -> PyResult<Py<PyAny>> {
        let scanner = self.scanner.as_batch()?;
        let mut all_batches = Vec::new();

        while !stopping_offsets.is_empty() {
            let scan_batches = py
                .detach(|| {
                    TOKIO_RUNTIME.block_on(async { scanner.poll(Duration::from_millis(500)).await })
                })
                .map_err(|e| FlussError::new_err(format!("Failed to poll: {e}")))?;

            if scan_batches.is_empty() {
                continue;
            }

            for scan_batch in scan_batches {
                let table_bucket = scan_batch.bucket().clone();

                // Check if this bucket is still being tracked
                let Some(&stop_at) = stopping_offsets.get(&table_bucket) else {
                    continue;
                };

                let base_offset = scan_batch.base_offset();
                let last_offset = scan_batch.last_offset();

                // If the batch starts at or after the stop_at offset, the bucket is exhausted
                if base_offset >= stop_at {
                    stopping_offsets.remove(&table_bucket);
                    continue;
                }

                let batch = if last_offset >= stop_at {
                    // Slice batch to keep only records where offset < stop_at
                    let num_to_keep = (stop_at - base_offset) as usize;
                    let b = scan_batch.into_batch();
                    let limit = num_to_keep.min(b.num_rows());
                    b.slice(0, limit)
                } else {
                    scan_batch.into_batch()
                };

                all_batches.push(Arc::new(batch));

                // Check if we're done with this bucket
                if last_offset >= stop_at - 1 {
                    stopping_offsets.remove(&table_bucket);
                }
            }
        }

        Utils::combine_batches_to_table(py, all_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_to_millis_and_submillis() {
        // Simple positive case
        assert_eq!(nanos_to_millis_and_submillis(1_500_000), (1, 500_000));

        // Exact millisecond boundary
        assert_eq!(nanos_to_millis_and_submillis(2_000_000), (2, 0));

        // Zero
        assert_eq!(nanos_to_millis_and_submillis(0), (0, 0));

        // Large value
        assert_eq!(
            nanos_to_millis_and_submillis(86_400_000_000_000), // 1 day in nanos
            (86_400_000, 0)
        );

        // Negative: -1.5 milliseconds should be (-2 millis, +500_000 nanos)
        // Because -1_500_000 nanos = -2ms + 500_000ns
        assert_eq!(nanos_to_millis_and_submillis(-1_500_000), (-2, 500_000));

        // Negative exact boundary
        assert_eq!(nanos_to_millis_and_submillis(-2_000_000), (-2, 0));

        // Small negative
        assert_eq!(nanos_to_millis_and_submillis(-1), (-1, 999_999));

        // Negative with sub-millisecond part
        assert_eq!(nanos_to_millis_and_submillis(-500_000), (-1, 500_000));
    }
}
