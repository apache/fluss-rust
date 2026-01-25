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
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use fluss::client::EARLIEST_OFFSET;
use fluss::rpc::message::OffsetSpec;
use pyo3::types::IntoPyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

// Time conversion constants
const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60_000;
const MILLIS_PER_HOUR: i64 = 3_600_000;
const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

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

/// Cached decimal.Decimal type
/// Uses PyOnceLock for thread-safety and subinterpreter compatibility.
static DECIMAL_TYPE: pyo3::sync::PyOnceLock<Py<pyo3::types::PyType>> =
    pyo3::sync::PyOnceLock::new();

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
