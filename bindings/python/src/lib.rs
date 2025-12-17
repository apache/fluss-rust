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

use std::sync::LazyLock;

pub use ::fluss as fcore;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

mod admin;
mod config;
mod connection;
mod error;
mod metadata;
mod table;
mod utils;

pub use admin::*;
pub use config::*;
pub use connection::*;
pub use error::*;
pub use metadata::*;
pub use table::*;
pub use utils::*;

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

#[pymodule]
fn _fluss(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register all classes
    m.add_class::<Config>()?;
    m.add_class::<FlussConnection>()?;
    m.add_class::<TablePath>()?;
    m.add_class::<TableInfo>()?;
    m.add_class::<TableDescriptor>()?;
    m.add_class::<FlussAdmin>()?;
    m.add_class::<FlussTable>()?;
    m.add_class::<AppendWriter>()?;
    m.add_class::<Schema>()?;
    m.add_class::<LogScanner>()?;
    m.add_class::<LakeSnapshot>()?;
    m.add_class::<TableBucket>()?;

    // Register exception types
    m.add_class::<FlussError>()?;

    Ok(())
}
