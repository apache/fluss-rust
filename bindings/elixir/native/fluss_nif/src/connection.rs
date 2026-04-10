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

use crate::RUNTIME;
use crate::atoms::to_nif_err;
use crate::config::ConfigResource;
use fluss::client::FlussConnection;
use rustler::ResourceArc;
use std::sync::Arc;

pub struct ConnectionResource(pub Arc<FlussConnection>);

impl std::panic::RefUnwindSafe for ConnectionResource {}

#[rustler::resource_impl]
impl rustler::Resource for ConnectionResource {}

#[rustler::nif(schedule = "DirtyIo")]
fn connection_new(
    config: ResourceArc<ConfigResource>,
) -> Result<ResourceArc<ConnectionResource>, rustler::Error> {
    let conn = RUNTIME
        .block_on(FlussConnection::new(config.0.clone()))
        .map_err(to_nif_err)?;
    Ok(ResourceArc::new(ConnectionResource(Arc::new(conn))))
}
