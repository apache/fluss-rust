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

use crate::table::{python_to_generic_row, python_to_sparse_generic_row};
use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::{Arc, Mutex};

/// Writer for upserting and deleting data in a Fluss primary key table.
///
/// Each upsert/delete operation synchronously queues the write. Call `flush()`
/// to ensure all queued writes are delivered to the server.
///
/// # Example:
///     writer = table.new_upsert()
///
///     # Fire-and-forget — ignore the returned handle
///     writer.upsert(row1)
///     writer.upsert(row2)
///     await writer.flush()
///
///     # Per-record ack — call wait() on the handle
///     handle = writer.upsert(critical_row)
///     await handle.wait()
#[pyclass]
pub struct UpsertWriter {
    inner: Arc<UpsertWriterInner>,
}

struct UpsertWriterInner {
    table_upsert: fcore::client::TableUpsert,
    /// Lazily initialized writer - created on first write operation
    writer: Mutex<Option<Arc<fcore::client::UpsertWriter>>>,
    table_info: fcore::metadata::TableInfo,
    /// Column indices for partial updates (None = full row)
    target_columns: Option<Vec<usize>>,
}

#[pymethods]
impl UpsertWriter {
    /// Upsert a row into the table.
    ///
    /// If a row with the same primary key exists, it will be updated.
    /// Otherwise, a new row will be inserted.
    ///
    /// The write is queued synchronously. Call `flush()` to ensure delivery.
    ///
    /// Args:
    ///     row: A dict, list, or tuple containing the row data.
    ///          For dict: keys are column names, values are column values.
    ///          For list/tuple: values must be in schema order.
    pub fn upsert(&self, row: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let generic_row = if let Some(target_cols) = &self.inner.target_columns {
            python_to_sparse_generic_row(row, &self.inner.table_info, target_cols)?
        } else {
            python_to_generic_row(row, &self.inner.table_info)?
        };

        let writer = self.inner.get_or_create_writer()?;
        let result_future = writer
            .upsert(&generic_row)
            .map_err(|e| FlussError::new_err(e.to_string()))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Delete a row from the table by primary key.
    ///
    /// The delete is queued synchronously. Call `flush()` to ensure delivery.
    ///
    /// Args:
    ///     pk: A dict, list, or tuple containing only the primary key values.
    ///         For dict: keys are PK column names.
    ///         For list/tuple: values in PK column order.
    pub fn delete(&self, pk: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let pk_indices = self.inner.table_info.get_schema().primary_key_indexes();
        let generic_row = python_to_sparse_generic_row(pk, &self.inner.table_info, &pk_indices)?;

        let writer = self.inner.get_or_create_writer()?;
        let result_future = writer
            .delete(&generic_row)
            .map_err(|e| FlussError::new_err(e.to_string()))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Flush all pending upsert/delete operations to the server.
    ///
    /// This method sends all buffered operations and waits until they are
    /// acknowledged according to the writer's ack configuration.
    ///
    /// Returns:
    ///     None on success
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Clone the Arc<UpsertWriter> out of the lock so we don't hold the guard across await
        let writer = {
            let guard = self
                .inner
                .writer
                .lock()
                .map_err(|e| FlussError::new_err(format!("Lock poisoned: {e}")))?;
            guard.as_ref().cloned()
        };

        future_into_py(py, async move {
            if let Some(writer) = writer {
                writer
                    .flush()
                    .await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            } else {
                Ok(())
            }
        })
    }

    fn __repr__(&self) -> String {
        "UpsertWriter()".to_string()
    }
}

impl UpsertWriter {
    /// Create an UpsertWriter from a TableUpsert.
    ///
    /// Optionally supports partial updates via column names or indices.
    pub fn new(
        table_upsert: fcore::client::TableUpsert,
        table_info: fcore::metadata::TableInfo,
        columns: Option<Vec<String>>,
        column_indices: Option<Vec<usize>>,
    ) -> PyResult<Self> {
        // Resolve target column indices (names → indices, or use provided indices directly)
        let target_columns = if let Some(cols) = columns {
            let row_type = table_info.row_type();
            Some(
                cols.iter()
                    .map(|name| {
                        row_type
                            .get_field_index(name)
                            .ok_or_else(|| FlussError::new_err(format!("Unknown column: {name}")))
                    })
                    .collect::<PyResult<Vec<usize>>>()?,
            )
        } else {
            column_indices
        };

        // Apply partial update to the Rust core using resolved indices
        let table_upsert = if let Some(ref indices) = target_columns {
            table_upsert
                .partial_update(Some(indices.clone()))
                .map_err(|e| FlussError::new_err(e.to_string()))?
        } else {
            table_upsert
        };

        Ok(Self {
            inner: Arc::new(UpsertWriterInner {
                table_upsert,
                writer: Mutex::new(None),
                table_info,
                target_columns,
            }),
        })
    }
}

impl UpsertWriterInner {
    /// Get the cached writer or create one on first use.
    fn get_or_create_writer(&self) -> PyResult<Arc<fcore::client::UpsertWriter>> {
        let mut guard = self
            .writer
            .lock()
            .map_err(|e| FlussError::new_err(format!("Lock poisoned: {e}")))?;
        if guard.is_none() {
            let writer = self
                .table_upsert
                .create_writer()
                .map_err(|e| FlussError::new_err(e.to_string()))?;
            *guard = Some(Arc::new(writer));
        }
        Ok(guard.as_ref().unwrap().clone())
    }
}
