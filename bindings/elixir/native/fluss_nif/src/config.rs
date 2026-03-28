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

use fluss::config::Config;
use rustler::ResourceArc;

pub struct ConfigResource(pub Config);

impl std::panic::RefUnwindSafe for ConfigResource {}

#[rustler::resource_impl]
impl rustler::Resource for ConfigResource {}

#[rustler::nif]
fn config_new(bootstrap_servers: String) -> ResourceArc<ConfigResource> {
    let mut config = Config::default();
    config.bootstrap_servers = bootstrap_servers;
    ResourceArc::new(ConfigResource(config))
}

#[rustler::nif]
fn config_default() -> ResourceArc<ConfigResource> {
    ResourceArc::new(ConfigResource(Config::default()))
}

#[rustler::nif]
fn config_set_bootstrap_servers(
    config: ResourceArc<ConfigResource>,
    servers: String,
) -> ResourceArc<ConfigResource> {
    let mut new_config = config.0.clone();
    new_config.bootstrap_servers = servers;
    ResourceArc::new(ConfigResource(new_config))
}

#[rustler::nif]
fn config_set_writer_batch_size(
    config: ResourceArc<ConfigResource>,
    size: i32,
) -> ResourceArc<ConfigResource> {
    let mut new_config = config.0.clone();
    new_config.writer_batch_size = size;
    ResourceArc::new(ConfigResource(new_config))
}

#[rustler::nif]
fn config_set_writer_batch_timeout_ms(
    config: ResourceArc<ConfigResource>,
    timeout_ms: i64,
) -> ResourceArc<ConfigResource> {
    let mut new_config = config.0.clone();
    new_config.writer_batch_timeout_ms = timeout_ms;
    ResourceArc::new(ConfigResource(new_config))
}

#[rustler::nif]
fn config_get_bootstrap_servers(config: ResourceArc<ConfigResource>) -> String {
    config.0.bootstrap_servers.clone()
}
