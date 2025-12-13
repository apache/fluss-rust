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

use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::rpc::message::GetSecurityTokenRequest;
use crate::rpc::RpcClient;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Deserialize)]
struct Credentials {
    access_key_id: String,
    access_key_secret: String,
    security_token: Option<String>,
}

struct CachedToken {
    access_key_id: String,
    secret_access_key: String,
    security_token: Option<String>,
    addition_infos: HashMap<String, String>,
    cached_at: Instant,
}

impl CachedToken {
    fn to_s3_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();

        props.insert("access_key_id".to_string(), self.access_key_id.clone());
        props.insert(
            "secret_access_key".to_string(),
            self.secret_access_key.clone(),
        );

        if let Some(token) = &self.security_token {
            props.insert("security_token".to_string(), token.clone());
        }

        for (key, value) in &self.addition_infos {
            if let Some((opendal_key, transform)) = convert_hadoop_key_to_opendal(key) {
                let final_value = if transform {
                    // Invert boolean value (path_style_access -> enable_virtual_host_style)
                    if value == "true" { "false".to_string() } else { "true".to_string() }
                } else {
                    value.clone()
                };
                props.insert(opendal_key, final_value);
            }
        }

        props
    }
}

/// Returns (opendal_key, needs_inversion)
/// needs_inversion is true for path_style_access -> enable_virtual_host_style conversion
fn convert_hadoop_key_to_opendal(hadoop_key: &str) -> Option<(String, bool)> {
    match hadoop_key {
        // Standard S3A keys
        "fs.s3a.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.s3a.endpoint.region" => Some(("region".to_string(), false)),
        // path.style.access = false means virtual_host_style = true (inverted)
        "fs.s3a.path.style.access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.s3a.connection.ssl.enabled" => None,
        // Red-S3 keys (Fluss custom format)
        "fs.red-s3.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.red-s3.region" => Some(("region".to_string(), false)),
        "fs.red-s3.path-style-access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.red-s3.connection.ssl.enabled" => None,
        _ => None,
    }
}

pub struct CredentialsCache {
    inner: RwLock<Option<CachedToken>>,
}

impl CredentialsCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    pub async fn get_or_refresh(
        &self,
        rpc_client: &Arc<RpcClient>,
        metadata: &Arc<Metadata>,
    ) -> Result<HashMap<String, String>> {
        {
            let guard = self.inner.read();
            if let Some(cached) = guard.as_ref() {
                if cached.cached_at.elapsed() < CACHE_TTL {
                    return Ok(cached.to_s3_props());
                }
            }
        }

        self.refresh_from_server(rpc_client, metadata).await
    }

    async fn refresh_from_server(
        &self,
        rpc_client: &Arc<RpcClient>,
        metadata: &Arc<Metadata>,
    ) -> Result<HashMap<String, String>> {
        let cluster = metadata.get_cluster();
        let server_node = cluster
            .get_coordinator_server()
            .or_else(|| Some(cluster.get_one_available_server()))
            .expect("no available server to fetch security token");
        let conn = rpc_client.get_connection(server_node).await?;

        let request = GetSecurityTokenRequest::new();
        let response = conn.request(request).await?;

        let credentials: Credentials = serde_json::from_slice(&response.token)
            .map_err(|e| Error::JsonSerdeError(e.to_string()))?;

        let mut addition_infos = HashMap::new();
        for kv in &response.addition_info {
            addition_infos.insert(kv.key.clone(), kv.value.clone());
        }

        let cached = CachedToken {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.access_key_secret,
            security_token: credentials.security_token,
            addition_infos,
            cached_at: Instant::now(),
        };

        let props = cached.to_s3_props();
        *self.inner.write() = Some(cached);

        Ok(props)
    }
}

impl Default for CredentialsCache {
    fn default() -> Self {
        Self::new()
    }
}







