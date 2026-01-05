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
use crate::rpc::RpcClient;
use crate::rpc::message::GetSecurityTokenRequest;
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
    fn to_remote_fs_props(&self) -> HashMap<String, String> {
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
                    if value == "true" {
                        "false".to_string()
                    } else {
                        "true".to_string()
                    }
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
        "fs.s3a.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.s3a.endpoint.region" => Some(("region".to_string(), false)),
        "fs.s3a.path.style.access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.s3a.connection.ssl.enabled" => None,
        _ => None,
    }
}

pub struct CredentialsCache {
    inner: RwLock<Option<CachedToken>>,
    rpc_client: Arc<RpcClient>,
    metadata: Arc<Metadata>,
}

impl CredentialsCache {
    pub fn new(rpc_client: Arc<RpcClient>, metadata: Arc<Metadata>) -> Self {
        Self {
            inner: RwLock::new(None),
            rpc_client,
            metadata,
        }
    }

    pub async fn get_or_refresh(&self) -> Result<HashMap<String, String>> {
        {
            let guard = self.inner.read();
            if let Some(cached) = guard.as_ref() {
                if cached.cached_at.elapsed() < CACHE_TTL {
                    return Ok(cached.to_remote_fs_props());
                }
            }
        }

        self.refresh_from_server().await
    }

    async fn refresh_from_server(&self) -> Result<HashMap<String, String>> {
        let cluster = self.metadata.get_cluster();
        let server_node = cluster
            .get_one_available_server()
            .expect("no tablet server available");
        let conn = self.rpc_client.get_connection(server_node).await?;

        let request = GetSecurityTokenRequest::new();
        let response = conn.request(request).await?;

        // the token may be empty if the remote filesystem
        // doesn't require token to access
        if response.token.is_empty() {
            return Ok(HashMap::new());
        }

        let credentials: Credentials =
            serde_json::from_slice(&response.token).map_err(|e| Error::JsonSerdeError {
                message: format!("Error when parse token from server: {e}"),
            })?;

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

        let props = cached.to_remote_fs_props();
        *self.inner.write() = Some(cached);

        Ok(props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::metadata::Metadata;
    use crate::cluster::Cluster;

    #[test]
    fn convert_hadoop_key_to_opendal_maps_known_keys() {
        let (key, invert) = convert_hadoop_key_to_opendal("fs.s3a.endpoint").expect("key");
        assert_eq!(key, "endpoint");
        assert!(!invert);

        let (key, invert) =
            convert_hadoop_key_to_opendal("fs.s3a.path.style.access").expect("key");
        assert_eq!(key, "enable_virtual_host_style");
        assert!(invert);

        assert!(convert_hadoop_key_to_opendal("fs.s3a.connection.ssl.enabled").is_none());
        assert!(convert_hadoop_key_to_opendal("unknown.key").is_none());
    }

    #[tokio::test]
    async fn credentials_cache_returns_cached_props() -> Result<()> {
        let cached = CachedToken {
            access_key_id: "ak".to_string(),
            secret_access_key: "sk".to_string(),
            security_token: Some("token".to_string()),
            addition_infos: HashMap::from([(
                "fs.s3a.path.style.access".to_string(),
                "true".to_string(),
            )]),
            cached_at: Instant::now(),
        };

        let cache = CredentialsCache {
            inner: RwLock::new(Some(cached)),
            rpc_client: Arc::new(RpcClient::new()),
            metadata: Arc::new(Metadata::new_for_test(Arc::new(Cluster::default()))),
        };

        let props = cache.get_or_refresh().await?;
        assert_eq!(props.get("access_key_id"), Some(&"ak".to_string()));
        assert_eq!(props.get("secret_access_key"), Some(&"sk".to_string()));
        assert_eq!(props.get("security_token"), Some(&"token".to_string()));
        assert_eq!(
            props.get("enable_virtual_host_style"),
            Some(&"false".to_string())
        );
        Ok(())
    }
}
