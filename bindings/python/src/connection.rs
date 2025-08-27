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

/// Connection to a Fluss cluster
#[pyclass]
pub struct FlussConnection {
    inner: Arc<fcore::client::FlussConnection>,
}

#[pymethods]
impl FlussConnection {
    #[new]
    fn new(config: &Config) -> PyResult<Self> {
        // Always use connect to create a new connection
        Err(FlussError::new_err("Use FlussConnection.connect() instead."))
    }

    /// Create a new FlussConnection (async)
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, config: &Config) -> PyResult<Bound<'py, PyAny>> {
        let rust_config = config.get_core_config();

        future_into_py(py, async move {
            let connection = fcore::client::FlussConnection::new(rust_config)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;
        
            let py_connection = FlussConnection {
                inner: Arc::new(connection),
            };

            Python::with_gil(|py| {
                Py::new(py, py_connection)
            })
        })
    }
    
    /// Get admin interface
    fn get_admin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();

        future_into_py(py, async move {
            let admin = client.get_admin()
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let py_admin = FlussAdmin::from_core(admin);

            Python::with_gil(|py| {
                Py::new(py, py_admin)
            })
        })
    }

    /// Get a table
    fn get_table<'py>(&self, py: Python<'py>, table_path: &TablePath) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let core_path = table_path.to_core().clone();

        future_into_py(py, async move {
            let metadata = client.get_metadata();
            
            metadata.update_table_metadata(&core_path).await
                .map_err(|e| FlussError::new_err(e.to_string()))?;
            
            let table_info = metadata.get_cluster().get_table(&core_path).clone();
            let table_path = table_info.table_path.clone();
            let has_primary_key = table_info.has_primary_key();

            let py_table = FlussTable::new_table(
                client,          
                metadata,        
                table_info,
                table_path,
                has_primary_key,
            );

            Python::with_gil(|py| {
                Py::new(py, py_table)
            })
        })
    }

    // Close the connection
    fn close(&mut self) -> PyResult<()> {
        Ok(())
    }

    // Enter the runtime context (for 'with' statement)
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    
    // Exit the runtime context (for 'with' statement)
    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        "FlussConnection()".to_string()
    }
}
