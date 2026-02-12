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

use crate::table::{internal_row_to_dict, python_to_sparse_generic_row};
use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Lookuper for performing primary key lookups on a Fluss table.
///
/// The Lookuper caches key encoders and bucketing functions, making
/// repeated lookups efficient. Create once and reuse for multiple lookups.
///
/// # Example:
///     lookuper = table.new_lookup()
///     result = await lookuper.lookup({"user_id": 1})
///     result2 = await lookuper.lookup({"user_id": 2})  # Reuses cached encoders
#[pyclass]
pub struct Lookuper {
    inner: Arc<Mutex<fcore::client::Lookuper>>,
    table_info: Arc<fcore::metadata::TableInfo>,
}

#[pymethods]
impl Lookuper {
    /// Lookup a row by its primary key.
    ///
    /// Args:
    ///     pk: A dict, list, or tuple containing only the primary key values.
    ///         For dict: keys are PK column names.
    ///         For list/tuple: values in PK column order.
    ///
    /// Returns:
    ///     A dict containing the row data if found, None otherwise.
    pub fn lookup<'py>(
        &self,
        py: Python<'py>,
        pk: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let generic_row = python_to_sparse_generic_row(pk, &self.table_info, &pk_indices)?;
        let inner = self.inner.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            // Perform async lookup
            let result = {
                let mut lookuper = inner.lock().await;
                lookuper
                    .lookup(&generic_row)
                    .await
                    .map_err(|e| FlussError::from_core_error(&e))?
            };

            // Extract row data
            let row_opt = result
                .get_single_row()
                .map_err(|e| FlussError::from_core_error(&e))?;

            // Convert to Python with GIL
            Python::attach(|py| match row_opt {
                Some(compacted_row) => internal_row_to_dict(py, &compacted_row, &table_info),
                None => Ok(py.None()),
            })
        })
    }

    fn __repr__(&self) -> String {
        "Lookuper()".to_string()
    }
}

impl Lookuper {
    /// Create a Lookuper from connection components.
    ///
    /// This creates the core Lookuper which caches encoders and bucketing functions.
    pub fn new(
        connection: &Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
    ) -> PyResult<Self> {
        let fluss_table = fcore::client::FlussTable::new(connection, metadata, table_info.clone());

        let table_lookup = fluss_table
            .new_lookup()
            .map_err(|e| FlussError::from_core_error(&e))?;

        let lookuper = table_lookup
            .create_lookuper()
            .map_err(|e| FlussError::from_core_error(&e))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(lookuper)),
            table_info: Arc::new(table_info),
        })
    }
}
