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

use crate::table::{python_pk_to_generic_row, python_to_generic_row};
use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Writer for upserting and deleting data in a Fluss primary key table.
///
/// Each upsert/delete operation is sent to the server and waits for acknowledgment.
/// Multiple concurrent writers share a common WriterClient which batches requests
/// for efficiency.
///
/// Example:
///     writer = table.new_upsert()
///     await writer.upsert(row1)
///     await writer.upsert(row2)
///     await writer.delete(pk)
///     await writer.flush()  # Ensures all pending operations are acknowledged
#[pyclass]
pub struct UpsertWriter {
    inner: Arc<UpsertWriterInner>,
}

struct UpsertWriterInner {
    table_upsert: fcore::client::TableUpsert,
    /// Lazily initialized writer - created on first write operation
    writer: Mutex<Option<fcore::client::UpsertWriter>>,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl UpsertWriter {
    /// Upsert a row into the table.
    ///
    /// If a row with the same primary key exists, it will be updated.
    /// Otherwise, a new row will be inserted.
    ///
    /// Args:
    ///     row: A dict, list, or tuple containing the row data.
    ///          For dict: keys are column names, values are column values.
    ///          For list/tuple: values must be in schema order.
    ///
    /// Returns:
    ///     None on success
    pub fn upsert<'py>(
        &self,
        py: Python<'py>,
        row: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let generic_row = python_to_generic_row(row, &self.inner.table_info)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let mut guard = inner.get_or_create_writer().await?;
            let writer = guard.as_mut().unwrap();
            writer
                .upsert(&generic_row)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Delete a row from the table by primary key.
    ///
    /// Args:
    ///     pk: A dict, list, or tuple containing only the primary key values.
    ///         For dict: keys are PK column names.
    ///         For list/tuple: values in PK column order.
    ///
    /// Returns:
    ///     None on success
    pub fn delete<'py>(
        &self,
        py: Python<'py>,
        pk: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let generic_row = python_pk_to_generic_row(pk, &self.inner.table_info)?;
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let mut guard = inner.get_or_create_writer().await?;
            let writer = guard.as_mut().unwrap();
            writer
                .delete(&generic_row)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Flush all pending upsert/delete operations to the server.
    ///
    /// This method sends all buffered operations and blocks until they are
    /// acknowledged according to the writer's ack configuration.
    ///
    /// Returns:
    ///     None on success
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let writer_guard = inner.writer.lock().await;

            if let Some(writer) = writer_guard.as_ref() {
                writer
                    .flush()
                    .await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            } else {
                // Nothing to flush - no writer was created yet
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
        // Apply partial update configuration if specified
        let table_upsert = if let Some(cols) = columns {
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            table_upsert
                .partial_update_with_column_names(&col_refs)
                .map_err(|e| FlussError::new_err(e.to_string()))?
        } else if let Some(indices) = column_indices {
            table_upsert
                .partial_update(Some(indices))
                .map_err(|e| FlussError::new_err(e.to_string()))?
        } else {
            table_upsert
        };

        Ok(Self {
            inner: Arc::new(UpsertWriterInner {
                table_upsert,
                writer: Mutex::new(None),
                table_info,
            }),
        })
    }
}

impl UpsertWriterInner {
    /// Get the cached writer or create one on first use.
    async fn get_or_create_writer(
        &self,
    ) -> PyResult<tokio::sync::MutexGuard<'_, Option<fcore::client::UpsertWriter>>> {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let writer = self
                .table_upsert
                .create_writer()
                .map_err(|e| FlussError::new_err(e.to_string()))?;
            *guard = Some(writer);
        }
        Ok(guard)
    }
}
