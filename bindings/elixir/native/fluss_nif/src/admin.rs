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

use crate::async_nif;
use crate::atoms::to_nif_err;
use crate::connection::ConnectionResource;
use crate::schema::TableDescriptorResource;
use fluss::client::FlussAdmin;
use fluss::metadata::{DatabaseDescriptor, DatabaseInfo, TablePath};
use fluss::{ServerNode, ServerType};
use rustler::{Env, NifStruct, NifUnitEnum, ResourceArc, Term};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(NifUnitEnum)]
pub enum NifServerType {
    TabletServer,
    CoordinatorServer,
}

#[derive(NifStruct)]
#[module = "Fluss.ServerNode"]
pub struct NifServerNode {
    pub id: i32,
    pub uid: String,
    pub host: String,
    pub port: u32,
    pub server_type: NifServerType,
}

impl NifServerNode {
    pub fn from_core(node: &ServerNode) -> Self {
        Self {
            id: node.id(),
            uid: node.uid().to_string(),
            host: node.host().to_string(),
            port: node.port(),
            server_type: match node.server_type() {
                ServerType::TabletServer => NifServerType::TabletServer,
                ServerType::CoordinatorServer => NifServerType::CoordinatorServer,
            },
        }
    }
}

#[derive(NifStruct)]
#[module = "Fluss.DatabaseDescriptor"]
pub struct NifDatabaseDescriptor {
    pub comment: Option<String>,
    pub custom_properties: HashMap<String, String>,
}

impl NifDatabaseDescriptor {
    pub fn from_core(desc: &DatabaseDescriptor) -> Self {
        Self {
            comment: desc.comment().map(String::from),
            custom_properties: desc.custom_properties().clone(),
        }
    }
}

#[derive(NifStruct)]
#[module = "Fluss.DatabaseInfo"]
pub struct NifDatabaseInfo {
    pub database_name: String,
    pub descriptor: NifDatabaseDescriptor,
    pub created_time: i64,
    pub modified_time: i64,
}

impl NifDatabaseInfo {
    pub fn from_core(info: &DatabaseInfo) -> Self {
        Self {
            database_name: info.database_name().to_string(),
            descriptor: NifDatabaseDescriptor::from_core(info.database_descriptor()),
            created_time: info.created_time(),
            modified_time: info.modified_time(),
        }
    }
}

pub struct AdminResource {
    pub inner: Arc<FlussAdmin>,
}

impl std::panic::RefUnwindSafe for AdminResource {}

#[rustler::resource_impl]
impl rustler::Resource for AdminResource {}

#[rustler::nif]
fn admin_new(
    conn: ResourceArc<ConnectionResource>,
) -> Result<ResourceArc<AdminResource>, rustler::Error> {
    let inner = conn.inner.get_admin().map_err(to_nif_err)?;
    Ok(ResourceArc::new(AdminResource { inner }))
}

#[rustler::nif]
fn admin_get_server_nodes<'a>(env: Env<'a>, admin: ResourceArc<AdminResource>) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let nodes: Vec<ServerNode> = admin.inner.get_server_nodes().await?;
        let wrapped: Vec<NifServerNode> = nodes.iter().map(NifServerNode::from_core).collect();
        Ok(wrapped)
    })
}

#[rustler::nif]
fn admin_create_database<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    ignore_if_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        admin
            .inner
            .create_database(&database_name, None, ignore_if_exists)
            .await
    })
}

#[rustler::nif]
fn admin_get_database_info<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let info: DatabaseInfo = admin.inner.get_database_info(&database_name).await?;
        Ok(NifDatabaseInfo::from_core(&info))
    })
}

#[rustler::nif]
fn admin_drop_database<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    ignore_if_not_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        admin
            .inner
            .drop_database(&database_name, ignore_if_not_exists, false)
            .await
    })
}

#[rustler::nif]
fn admin_list_databases<'a>(env: Env<'a>, admin: ResourceArc<AdminResource>) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move { admin.inner.list_databases().await })
}

#[rustler::nif]
fn admin_database_exists<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        admin.inner.database_exists(&database_name).await
    })
}

#[rustler::nif]
fn admin_create_table<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    descriptor: ResourceArc<TableDescriptorResource>,
    ignore_if_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        let path = TablePath::new(&database_name, &table_name);
        admin
            .inner
            .create_table(&path, &descriptor.inner, ignore_if_exists)
            .await
    })
}

#[rustler::nif]
fn admin_drop_table<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
    ignore_if_not_exists: bool,
) -> Term<'a> {
    async_nif::spawn_task(env, async move {
        let path = TablePath::new(&database_name, &table_name);
        admin.inner.drop_table(&path, ignore_if_not_exists).await
    })
}

#[rustler::nif]
fn admin_list_tables<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(
        env,
        async move { admin.inner.list_tables(&database_name).await },
    )
}

#[rustler::nif]
fn admin_table_exists<'a>(
    env: Env<'a>,
    admin: ResourceArc<AdminResource>,
    database_name: String,
    table_name: String,
) -> Term<'a> {
    async_nif::spawn_task_with_result(env, async move {
        let table_path = TablePath::new(database_name, table_name);
        admin.inner.table_exists(&table_path).await
    })
}
