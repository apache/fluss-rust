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

use fluss::client::FlussConnection;
use fluss::config::Config;
use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::ContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const FLUSS_VERSION: &str = "0.7.0";

pub struct FlussTestingClusterBuilder {
    number_of_tablet_servers: i32,
    network: &'static str,
    cluster_conf: HashMap<String, String>,
    testing_name: String,
    remote_data_dir: Option<std::path::PathBuf>,
}

impl FlussTestingClusterBuilder {
    pub fn new(testing_name: impl Into<String>) -> Self {
        Self::new_with_cluster_conf(testing_name.into(), &HashMap::default())
    }

    pub fn with_remote_data_dir(mut self, dir: std::path::PathBuf) -> Self {
        // Ensure the directory exists before mounting
        std::fs::create_dir_all(&dir).expect("Failed to create remote data directory");
        self.remote_data_dir = Some(dir);
        self
    }

    pub fn new_with_cluster_conf(
        testing_name: impl Into<String>,
        conf: &HashMap<String, String>,
    ) -> Self {
        // reduce testing resources
        let mut cluster_conf = conf.clone();
        cluster_conf.insert(
            "netty.server.num-network-threads".to_string(),
            "1".to_string(),
        );
        cluster_conf.insert(
            "netty.server.num-worker-threads".to_string(),
            "3".to_string(),
        );

        FlussTestingClusterBuilder {
            number_of_tablet_servers: 1,
            cluster_conf,
            network: "fluss-cluster-network",
            testing_name: testing_name.into(),
            remote_data_dir: None,
        }
    }

    fn tablet_server_container_name(&self, server_id: i32) -> String {
        format!("tablet-server-{}-{}", self.testing_name, server_id)
    }

    fn coordinator_server_container_name(&self) -> String {
        format!("coordinator-server-{}", self.testing_name)
    }

    fn zookeeper_container_name(&self) -> String {
        format!("zookeeper-{}", self.testing_name)
    }

    pub async fn build(&mut self) -> FlussTestingCluster {
        let zookeeper = Arc::new(
            GenericImage::new("zookeeper", "3.9.2")
                .with_network(self.network)
                .with_container_name(self.zookeeper_container_name())
                .start()
                .await
                .unwrap(),
        );

        let coordinator_server = Arc::new(self.start_coordinator_server().await);

        let mut tablet_servers = HashMap::new();
        for server_id in 0..self.number_of_tablet_servers {
            tablet_servers.insert(
                server_id,
                Arc::new(self.start_tablet_server(server_id).await),
            );
        }

        FlussTestingCluster {
            zookeeper,
            coordinator_server,
            tablet_servers,
            bootstrap_servers: "127.0.0.1:9123".to_string(),
            remote_data_dir: self.remote_data_dir.clone(),
        }
    }

    async fn start_coordinator_server(&mut self) -> ContainerAsync<GenericImage> {
        let mut coordinator_confs = HashMap::new();
        coordinator_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );
        coordinator_confs.insert(
            "bind.listeners",
            format!(
                "INTERNAL://{}:0, CLIENT://{}:9123",
                self.coordinator_server_container_name(),
                self.coordinator_server_container_name()
            ),
        );
        coordinator_confs.insert(
            "advertised.listeners",
            "CLIENT://localhost:9123".to_string(),
        );
        coordinator_confs.insert("internal.listener.name", "INTERNAL".to_string());
        GenericImage::new("fluss/fluss", FLUSS_VERSION)
            .with_container_name(self.coordinator_server_container_name())
            .with_mapped_port(9123, ContainerPort::Tcp(9123))
            .with_network(self.network)
            .with_cmd(vec!["coordinatorServer"])
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(coordinator_confs),
            )
            .start()
            .await
            .unwrap()
    }

    async fn start_tablet_server(&self, server_id: i32) -> ContainerAsync<GenericImage> {
        let mut tablet_server_confs = HashMap::new();
        let bind_listeners = format!(
            "INTERNAL://{}:0, CLIENT://{}:9123",
            self.tablet_server_container_name(server_id),
            self.tablet_server_container_name(server_id),
        );
        let expose_host_port = 9124 + server_id;
        let advertised_listeners = format!("CLIENT://localhost:{}", expose_host_port);
        let tablet_server_id = format!("{}", server_id);
        tablet_server_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );
        tablet_server_confs.insert("bind.listeners", bind_listeners);
        tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        tablet_server_confs.insert("internal.listener.name", "INTERNAL".to_string());
        tablet_server_confs.insert("tablet-server.id", tablet_server_id);

        // Set remote.data.dir to use the same path as host when volume mount is provided
        // This ensures the path is consistent between host and container
        if let Some(remote_data_dir) = &self.remote_data_dir {
            tablet_server_confs.insert(
                "remote.data.dir",
                remote_data_dir.to_string_lossy().to_string(),
            );
        }
        let mut image = GenericImage::new("fluss/fluss", FLUSS_VERSION)
            .with_cmd(vec!["tabletServer"])
            .with_mapped_port(expose_host_port as u16, ContainerPort::Tcp(9123))
            .with_network(self.network)
            .with_container_name(self.tablet_server_container_name(server_id))
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(tablet_server_confs),
            );

        // Add volume mount if remote_data_dir is provided
        if let Some(ref remote_data_dir) = self.remote_data_dir {
            use testcontainers::core::Mount;
            // Ensure directory exists before mounting (double check)
            std::fs::create_dir_all(remote_data_dir)
                .expect("Failed to create remote data directory for mount");
            let host_path = remote_data_dir.to_string_lossy().to_string();
            let container_path = remote_data_dir.to_string_lossy().to_string();
            image = image.with_mount(Mount::bind_mount(host_path, container_path));
        }

        image.start().await.unwrap()
    }

    fn to_fluss_properties_with(&self, extra_properties: HashMap<&str, String>) -> String {
        let mut fluss_properties = Vec::new();
        for (k, v) in self.cluster_conf.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        for (k, v) in extra_properties.iter() {
            fluss_properties.push(format!("{}: {}", k, v));
        }
        fluss_properties.join("\n")
    }
}

/// Provides an easy way to launch a Fluss cluster with coordinator and tablet servers.
#[derive(Clone)]
pub struct FlussTestingCluster {
    zookeeper: Arc<ContainerAsync<GenericImage>>,
    coordinator_server: Arc<ContainerAsync<GenericImage>>,
    tablet_servers: HashMap<i32, Arc<ContainerAsync<GenericImage>>>,
    bootstrap_servers: String,
    remote_data_dir: Option<std::path::PathBuf>,
}

impl FlussTestingCluster {
    pub async fn stop(&self) {
        for tablet_server in self.tablet_servers.values() {
            tablet_server.stop().await.unwrap()
        }
        self.coordinator_server.stop().await.unwrap();
        self.zookeeper.stop().await.unwrap();
        if let Some(remote_data_dir) = &self.remote_data_dir {
            // Try to clean up the remote data directory, but don't fail if it can't be deleted.
            // This can happen in CI environments or if Docker containers are still using the directory.
            // The directory will be cleaned up by the CI system or OS eventually.
            if let Err(e) = tokio::fs::remove_dir_all(remote_data_dir).await {
                eprintln!(
                    "Warning: Failed to delete remote data directory: {:?}, error: {:?}. \
                     This is non-fatal and the directory may be cleaned up later.",
                    remote_data_dir, e
                );
            }
        }
    }

    pub async fn get_fluss_connection(&self) -> FlussConnection {
        let mut config = Config::default();
        config.writer_acks = "all".to_string();
        config.bootstrap_server = Some(self.bootstrap_servers.clone());

        // Retry mechanism: retry for up to 1 minute
        let max_retries = 60; // 60 retry attempts
        let retry_interval = Duration::from_secs(1); // 1 second interval between retries

        for attempt in 1..=max_retries {
            match FlussConnection::new(config.clone()).await {
                Ok(connection) => {
                    return connection;
                }
                Err(e) => {
                    if attempt == max_retries {
                        panic!(
                            "Failed to connect to Fluss cluster after {} attempts: {}",
                            max_retries, e
                        );
                    }
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
        unreachable!()
    }
}
