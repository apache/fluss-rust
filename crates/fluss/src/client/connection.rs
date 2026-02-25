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

use crate::client::WriterClient;
use crate::client::admin::FlussAdmin;
use crate::client::metadata::Metadata;
use crate::client::table::FlussTable;
use crate::config::Config;
use crate::rpc::RpcClient;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, FlussError, Result};
use crate::metadata::TablePath;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<RpcClient>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        arg.validate_security()
            .map_err(|msg| Error::IllegalArgument { message: msg })?;

        let timeout = Duration::from_millis(arg.connect_timeout_ms);
        let connections = if arg.is_sasl_enabled() {
            Arc::new(
                RpcClient::new()
                    .with_sasl(
                        arg.security_sasl_username.clone(),
                        arg.security_sasl_password.clone(),
                    )
                    .with_timeout(timeout),
            )
        } else {
            Arc::new(RpcClient::new().with_timeout(timeout))
        };
        let metadata = Metadata::new(arg.bootstrap_servers.as_str(), connections.clone()).await?;

        Ok(FlussConnection {
            metadata: Arc::new(metadata),
            network_connects: connections.clone(),
            args: arg.clone(),
            writer_client: Default::default(),
        })
    }

    pub fn get_metadata(&self) -> Arc<Metadata> {
        self.metadata.clone()
    }

    pub fn get_connections(&self) -> Arc<RpcClient> {
        self.network_connects.clone()
    }

    pub fn config(&self) -> &Config {
        &self.args
    }

    pub async fn get_admin(&self) -> Result<FlussAdmin> {
        FlussAdmin::new(self.network_connects.clone(), self.metadata.clone()).await
    }

    pub fn get_or_create_writer_client(&self) -> Result<Arc<WriterClient>> {
        // 1. Fast path: Attempt to acquire a read lock to check if the client already exists.
        if let Some(client) = self.writer_client.read().as_ref() {
            return Ok(client.clone());
        }

        // 2. Slow path: Acquire the write lock.
        let mut writer_guard = self.writer_client.write();

        // 3. Double-check: Another thread might have initialized the client
        // while this thread was waiting for the write lock.
        if let Some(client) = writer_guard.as_ref() {
            return Ok(client.clone());
        }

        // 4. Initialize the client since we are certain it doesn't exist yet.
        let new_client = Arc::new(WriterClient::new(self.args.clone(), self.metadata.clone())?);

        // 5. Store and return the newly created client.
        *writer_guard = Some(new_client.clone());
        Ok(new_client)
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self
            .metadata
            .get_cluster()
            .get_table(table_path)
            .map_err(|e| {
                if e.api_error() == Some(FlussError::InvalidTableException) {
                    Error::table_not_exist(format!("Table not found: {table_path}"))
                } else {
                    e
                }
            })?
            .clone();
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }
}
