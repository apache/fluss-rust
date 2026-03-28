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

use crate::atoms::to_nif_err;
use crate::connection::ConnectionResource;
use crate::RUNTIME;
use fluss::client::{FlussConnection, FlussTable, Metadata};
use fluss::metadata::{Column, TableInfo, TablePath};
use rustler::ResourceArc;
use std::sync::Arc;

/// Holds the data needed to reconstruct FlussTable (which has a lifetime
/// tied to FlussConnection). We store the Arc<FlussConnection> to keep
/// it alive and reconstruct short-lived FlussTable instances on demand.
pub struct TableResource {
    pub connection: Arc<FlussConnection>,
    pub metadata: Arc<Metadata>,
    pub table_info: TableInfo,
}

impl std::panic::RefUnwindSafe for TableResource {}

#[rustler::resource_impl]
impl rustler::Resource for TableResource {}

impl TableResource {
    pub fn columns(&self) -> &[Column] {
        self.table_info.schema.columns()
    }

    pub fn with_table<T>(&self, f: impl FnOnce(&FlussTable<'_>) -> T) -> T {
        let table = FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );
        f(&table)
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn table_get(
    conn: ResourceArc<ConnectionResource>,
    database_name: String,
    table_name: String,
) -> Result<ResourceArc<TableResource>, rustler::Error> {
    let path = TablePath::new(&database_name, &table_name);
    let table = RUNTIME
        .block_on(conn.0.get_table(&path))
        .map_err(to_nif_err)?;

    Ok(ResourceArc::new(TableResource {
        connection: conn.0.clone(),
        metadata: table.metadata().clone(),
        table_info: table.get_table_info().clone(),
    }))
}

#[rustler::nif]
fn table_has_primary_key(table: ResourceArc<TableResource>) -> bool {
    table.table_info.has_primary_key()
}

#[rustler::nif]
fn table_column_names(table: ResourceArc<TableResource>) -> Vec<String> {
    table
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect()
}
