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

use crate::error::Result;
use crate::metadata::TablePath;

pub struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<RpcClient>,
    args: Config,
    writer_client: RwLock<Option<Arc<WriterClient>>>,
}

impl FlussConnection {
    pub async fn new(arg: Config) -> Result<Self> {
        let connections = Arc::new(RpcClient::new());
        let metadata = Metadata::new(
            arg.bootstrap_server.as_ref().unwrap().as_str(),
            connections.clone(),
        )
        .await?;

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

    #[cfg(test)]
    pub(crate) fn new_for_test(
        metadata: Arc<Metadata>,
        network_connects: Arc<RpcClient>,
        args: Config,
    ) -> Self {
        FlussConnection {
            metadata,
            network_connects,
            args,
            writer_client: Default::default(),
        }
    }

    pub async fn get_admin(&self) -> Result<FlussAdmin> {
        FlussAdmin::new(self.network_connects.clone(), self.metadata.clone()).await
    }

    pub fn get_or_create_writer_client(&self) -> Result<Arc<WriterClient>> {
        if let Some(client) = self.writer_client.read().as_ref() {
            return Ok(client.clone());
        }

        // If not exists, create new one
        let client = Arc::new(WriterClient::new(self.args.clone(), self.metadata.clone())?);
        *self.writer_client.write() = Some(client.clone());
        Ok(client)
    }

    pub async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>> {
        self.metadata.update_table_metadata(table_path).await?;
        let table_info = self.metadata.get_cluster().get_table(table_path).clone();
        if table_info.is_partitioned() {
            return Err(crate::error::Error::UnsupportedOperation {
                message: "Partitioned tables are not supported".to_string(),
            });
        }
        Ok(FlussTable::new(self, self.metadata.clone(), table_info))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Cluster, ServerNode, ServerType};
    use crate::metadata::{DataField, DataTypes, Schema, TableDescriptor, TableInfo, TablePath};
    use crate::test_utils::build_mock_connection;
    use std::collections::HashMap;

    fn build_cluster() -> Arc<Cluster> {
        let coordinator = ServerNode::new(
            1,
            "127.0.0.1".to_string(),
            9092,
            ServerType::CoordinatorServer,
        );

        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let row_type = DataTypes::row(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let mut schema_builder = Schema::builder().with_row_type(&row_type);
        let schema = schema_builder.build().expect("schema");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor");
        let table_info = TableInfo::of(table_path.clone(), 1, 1, descriptor, 0, 0);

        let mut table_id_by_path = HashMap::new();
        table_id_by_path.insert(table_path.clone(), 1);

        let mut table_info_by_path = HashMap::new();
        table_info_by_path.insert(table_path, table_info);

        Arc::new(Cluster::new(
            Some(coordinator),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            table_id_by_path,
            table_info_by_path,
        ))
    }

    #[tokio::test]
    async fn get_or_create_writer_client_is_cached() -> Result<()> {
        let metadata = Arc::new(Metadata::new_for_test(build_cluster()));
        let rpc_client = Arc::new(RpcClient::new());
        let config = Config::default();

        let connection = FlussConnection {
            metadata,
            network_connects: rpc_client,
            args: config,
            writer_client: Default::default(),
        };

        let first = connection.get_or_create_writer_client()?;
        let second = connection.get_or_create_writer_client()?;
        assert!(Arc::ptr_eq(&first, &second));

        drop(first);
        drop(second);

        let stored = connection
            .writer_client
            .write()
            .take()
            .expect("writer client");
        let client = Arc::try_unwrap(stored).unwrap_or_else(|_| {
            panic!("writer client still shared");
        });
        client.close().await?;
        Ok(())
    }

    #[test]
    fn exposes_config_and_clients() {
        let metadata = Arc::new(Metadata::new_for_test(build_cluster()));
        let rpc_client = Arc::new(RpcClient::new());
        let config = Config::default();

        let connection = FlussConnection {
            metadata: metadata.clone(),
            network_connects: rpc_client.clone(),
            args: config.clone(),
            writer_client: Default::default(),
        };

        assert_eq!(
            connection.config().request_max_size,
            config.request_max_size
        );
        assert!(Arc::ptr_eq(&connection.get_metadata(), &metadata));
        assert!(Arc::ptr_eq(&connection.get_connections(), &rpc_client));
    }

    #[tokio::test]
    async fn get_admin_uses_cached_connection() -> Result<()> {
        let metadata = Arc::new(Metadata::new_for_test(build_cluster()));
        let rpc_client = Arc::new(RpcClient::new());
        let (connection, handle) =
            build_mock_connection(|_api_key: crate::rpc::ApiKey, _, _| Vec::new()).await;
        let coordinator = metadata
            .get_cluster()
            .get_coordinator_server()
            .expect("coordinator")
            .clone();
        rpc_client.insert_connection_for_test(&coordinator, connection);

        let connection = FlussConnection {
            metadata,
            network_connects: rpc_client,
            args: Config::default(),
            writer_client: Default::default(),
        };

        let _admin = connection.get_admin().await?;
        handle.abort();
        Ok(())
    }
}
