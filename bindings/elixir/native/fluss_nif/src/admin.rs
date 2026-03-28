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

use crate::atoms::{self, to_nif_err};
use crate::connection::ConnectionResource;
use crate::schema::TableDescriptorResource;
use crate::RUNTIME;
use fluss::client::FlussAdmin;
use fluss::metadata::TablePath;
use rustler::{Atom, ResourceArc};
use std::sync::Arc;

pub struct AdminResource(pub Arc<FlussAdmin>);

impl std::panic::RefUnwindSafe for AdminResource {}

#[rustler::resource_impl]
impl rustler::Resource for AdminResource {}

#[rustler::nif]
fn admin_new(conn: ResourceArc<ConnectionResource>) -> Result<ResourceArc<AdminResource>, rustler::Error> {
    let admin = conn.0.get_admin().map_err(to_nif_err)?;
    Ok(ResourceArc::new(AdminResource(admin)))
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_create_database(
    admin: ResourceArc<AdminResource>,
    database_name: String,
    ignore_if_exists: bool,
) -> Result<Atom, rustler::Error> {
    RUNTIME
        .block_on(admin.0.create_database(&database_name, None, ignore_if_exists))
        .map_err(to_nif_err)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_drop_database(
    admin: ResourceArc<AdminResource>,
    database_name: String,
    ignore_if_not_exists: bool,
) -> Result<Atom, rustler::Error> {
    RUNTIME
        .block_on(admin.0.drop_database(&database_name, ignore_if_not_exists, false))
        .map_err(to_nif_err)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_list_databases(admin: ResourceArc<AdminResource>) -> Result<Vec<String>, rustler::Error> {
    RUNTIME
        .block_on(admin.0.list_databases())
        .map_err(to_nif_err)
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_create_table(
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    descriptor: ResourceArc<TableDescriptorResource>,
    ignore_if_exists: bool,
) -> Result<Atom, rustler::Error> {
    let path = TablePath::new(&database_name, &table_name);
    RUNTIME
        .block_on(admin.0.create_table(&path, &descriptor.0, ignore_if_exists))
        .map_err(to_nif_err)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_drop_table(
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    ignore_if_not_exists: bool,
) -> Result<Atom, rustler::Error> {
    let path = TablePath::new(&database_name, &table_name);
    RUNTIME
        .block_on(admin.0.drop_table(&path, ignore_if_not_exists))
        .map_err(to_nif_err)?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn admin_list_tables(
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Result<Vec<String>, rustler::Error> {
    RUNTIME
        .block_on(admin.0.list_tables(&database_name))
        .map_err(to_nif_err)
}
