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

use fluss_test_cluster::{stop_cluster, FlussTestingClusterBuilder};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

const DEFAULT_SASL_USERS: &[(&str, &str)] = &[("root", "password"), ("guest", "password2")];

/// Opaque handle to a Fluss testing cluster running in Docker.
///
/// Containers are detached from this handle's lifetime — call `stop()` to tear them
/// down (or rely on the registered atexit cleanup in conftest.py).
#[pyclass]
pub struct FlussTestCluster {
    name: String,
    #[pyo3(get)]
    bootstrap_servers: String,
    #[pyo3(get)]
    sasl_bootstrap_servers: Option<String>,
}

#[pymethods]
impl FlussTestCluster {
    /// Start (or attach to) a cluster. Idempotent: if containers for `name` already
    /// exist, returns a handle to the running cluster instead of restarting it.
    #[staticmethod]
    #[pyo3(signature = (name, sasl=true, port=None))]
    fn start<'py>(
        py: Python<'py>,
        name: String,
        sasl: bool,
        port: Option<u16>,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let mut builder = FlussTestingClusterBuilder::new(name.clone());
            if let Some(p) = port {
                builder = builder.with_port(p);
            }
            if sasl {
                let users = DEFAULT_SASL_USERS
                    .iter()
                    .map(|(u, p)| (u.to_string(), p.to_string()))
                    .collect();
                builder = builder.with_sasl(users);
            }

            let info = builder.build_detached().await;

            let cluster = FlussTestCluster {
                name,
                bootstrap_servers: info.bootstrap_servers,
                sasl_bootstrap_servers: info.sasl_bootstrap_servers,
            };
            Python::attach(|py| Py::new(py, cluster))
        })
    }

    /// Tear down all containers belonging to this cluster name. Safe to call multiple times.
    fn stop(&self) -> PyResult<()> {
        stop_cluster(&self.name);
        Ok(())
    }

    /// Stop a named cluster without holding a handle. Useful for atexit cleanup.
    #[staticmethod]
    fn stop_by_name(name: &str) -> PyResult<()> {
        stop_cluster(name);
        Ok(())
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __repr__(&self) -> String {
        format!(
            "FlussTestCluster(name={:?}, bootstrap_servers={:?})",
            self.name, self.bootstrap_servers
        )
    }
}

