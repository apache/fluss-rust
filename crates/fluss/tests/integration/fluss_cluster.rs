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

const FLUSS_VERSION: &str = "0.8.0-incubating";
const FLUSS_IMAGE: &str = "apache/fluss";

pub struct FlussTestingClusterBuilder {
    number_of_tablet_servers: i32,
    network: &'static str,
    cluster_conf: HashMap<String, String>,
    testing_name: String,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_enabled: bool,
    sasl_users: Vec<(String, String)>,
    /// Host port for the coordinator server (default 9123).
    coordinator_host_port: u16,
    /// Host port for the plaintext (PLAIN_CLIENT) listener.
    /// When set together with `sasl_enabled`, the cluster exposes two listeners:
    /// CLIENT (SASL) on `coordinator_host_port` and PLAIN_CLIENT on this port.
    plain_client_port: Option<u16>,
    image: String,
    image_tag: String,
}

impl FlussTestingClusterBuilder {
    #[allow(dead_code)]
    pub fn new(testing_name: impl Into<String>) -> Self {
        Self::new_with_cluster_conf(testing_name.into(), &HashMap::default())
    }

    pub fn with_remote_data_dir(mut self, dir: std::path::PathBuf) -> Self {
        // Ensure the directory exists before mounting
        std::fs::create_dir_all(&dir).expect("Failed to create remote data directory");
        self.remote_data_dir = Some(dir);
        self
    }

    /// Enable SASL/PLAIN authentication on the cluster with dual listeners.
    /// Users are specified as `(username, password)` pairs.
    /// This automatically configures a PLAIN_CLIENT (plaintext) listener in addition
    /// to the CLIENT (SASL) listener, allowing both authenticated and unauthenticated
    /// connections on the same cluster.
    pub fn with_sasl(mut self, users: Vec<(String, String)>) -> Self {
        self.sasl_enabled = true;
        self.sasl_users = users;
        self.plain_client_port = Some(self.coordinator_host_port + 100);
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
            sasl_enabled: false,
            sasl_users: Vec::new(),
            coordinator_host_port: 9123,
            plain_client_port: None,
            image: FLUSS_IMAGE.to_string(),
            image_tag: FLUSS_VERSION.to_string(),
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
        // Remove stale containers from previous runs (if any) so we can reuse names.
        let stale_containers: Vec<String> = std::iter::once(self.zookeeper_container_name())
            .chain(std::iter::once(self.coordinator_server_container_name()))
            .chain(
                (0..self.number_of_tablet_servers).map(|id| self.tablet_server_container_name(id)),
            )
            .collect();
        for name in &stale_containers {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", name])
                .output();
        }

        // Inject SASL server-side configuration into cluster_conf
        if self.sasl_enabled && !self.sasl_users.is_empty() {
            self.cluster_conf.insert(
                "security.protocol.map".to_string(),
                "CLIENT:sasl".to_string(),
            );
            self.cluster_conf.insert(
                "security.sasl.enabled.mechanisms".to_string(),
                "plain".to_string(),
            );
            // Build JAAS config: user_<name>="<password>" for each user
            let user_entries: Vec<String> = self
                .sasl_users
                .iter()
                .map(|(u, p)| format!("user_{}=\"{}\"", u, p))
                .collect();
            let jaas_config = format!(
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required {};",
                user_entries.join(" ")
            );
            self.cluster_conf
                .insert("security.sasl.plain.jaas.config".to_string(), jaas_config);
        }

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

        // When dual listeners are configured, bootstrap_servers points to the plaintext
        // listener and sasl_bootstrap_servers points to the SASL listener.
        let (bootstrap_servers, sasl_bootstrap_servers) =
            if let Some(plain_port) = self.plain_client_port {
                (
                    format!("127.0.0.1:{}", plain_port),
                    Some(format!("127.0.0.1:{}", self.coordinator_host_port)),
                )
            } else {
                (format!("127.0.0.1:{}", self.coordinator_host_port), None)
            };

        FlussTestingCluster {
            zookeeper,
            coordinator_server,
            tablet_servers,
            bootstrap_servers,
            sasl_bootstrap_servers,
            remote_data_dir: self.remote_data_dir.clone(),
            sasl_users: self.sasl_users.clone(),
            container_names: stale_containers,
        }
    }

    async fn start_coordinator_server(&mut self) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.coordinator_server_container_name();
        let mut coordinator_confs = HashMap::new();
        coordinator_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );

        if let Some(plain_port) = self.plain_client_port {
            // Dual listeners: CLIENT (SASL) + PLAIN_CLIENT (plaintext)
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                    container_name, container_name, port, container_name, plain_port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!(
                    "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                    port, plain_port
                ),
            );
        } else {
            coordinator_confs.insert(
                "bind.listeners",
                format!(
                    "INTERNAL://{}:0, CLIENT://{}:{}",
                    container_name, container_name, port
                ),
            );
            coordinator_confs.insert(
                "advertised.listeners",
                format!("CLIENT://localhost:{}", port),
            );
        }

        coordinator_confs.insert("internal.listener.name", "INTERNAL".to_string());

        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_container_name(self.coordinator_server_container_name())
            .with_mapped_port(port, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_cmd(vec!["coordinatorServer"])
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(coordinator_confs),
            );

        if let Some(plain_port) = self.plain_client_port {
            image = image.with_mapped_port(plain_port, ContainerPort::Tcp(plain_port));
        }

        image.start().await.unwrap()
    }

    async fn start_tablet_server(&self, server_id: i32) -> ContainerAsync<GenericImage> {
        let port = self.coordinator_host_port;
        let container_name = self.tablet_server_container_name(server_id);
        let mut tablet_server_confs = HashMap::new();
        let expose_host_port = (port as i32) + 1 + server_id;
        let tablet_server_id = format!("{}", server_id);

        if let Some(plain_port) = self.plain_client_port {
            // Dual listeners: CLIENT (SASL) + PLAIN_CLIENT (plaintext)
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}, PLAIN_CLIENT://{}:{}",
                container_name, container_name, port, container_name, plain_port,
            );
            let plain_expose_host_port = (plain_port as i32) + 1 + server_id;
            let advertised_listeners = format!(
                "CLIENT://localhost:{}, PLAIN_CLIENT://localhost:{}",
                expose_host_port, plain_expose_host_port
            );
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        } else {
            let bind_listeners = format!(
                "INTERNAL://{}:0, CLIENT://{}:{}",
                container_name, container_name, port,
            );
            let advertised_listeners = format!("CLIENT://localhost:{}", expose_host_port);
            tablet_server_confs.insert("bind.listeners", bind_listeners);
            tablet_server_confs.insert("advertised.listeners", advertised_listeners);
        }

        tablet_server_confs.insert(
            "zookeeper.address",
            format!("{}:2181", self.zookeeper_container_name()),
        );
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
        let mut image = GenericImage::new(&self.image, &self.image_tag)
            .with_cmd(vec!["tabletServer"])
            .with_mapped_port(expose_host_port as u16, ContainerPort::Tcp(port))
            .with_network(self.network)
            .with_container_name(self.tablet_server_container_name(server_id))
            .with_env_var(
                "FLUSS_PROPERTIES",
                self.to_fluss_properties_with(tablet_server_confs),
            );

        // Add port mapping for plaintext listener
        if let Some(plain_port) = self.plain_client_port {
            let plain_expose_host_port = (plain_port as i32) + 1 + server_id;
            image = image.with_mapped_port(
                plain_expose_host_port as u16,
                ContainerPort::Tcp(plain_port),
            );
        }

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
#[allow(dead_code)] // Fields held for RAII (keeping Docker containers alive).
pub struct FlussTestingCluster {
    zookeeper: Arc<ContainerAsync<GenericImage>>,
    coordinator_server: Arc<ContainerAsync<GenericImage>>,
    tablet_servers: HashMap<i32, Arc<ContainerAsync<GenericImage>>>,
    /// Bootstrap servers for plaintext connections.
    /// When dual listeners are configured, this points to the PLAIN_CLIENT listener.
    bootstrap_servers: String,
    /// Bootstrap servers for SASL connections (only set when dual listeners are configured).
    sasl_bootstrap_servers: Option<String>,
    remote_data_dir: Option<std::path::PathBuf>,
    sasl_users: Vec<(String, String)>,
    container_names: Vec<String>,
}

impl FlussTestingCluster {
    /// Synchronously stops and removes all Docker containers and cleans up the
    /// remote data directory. Safe to call from non-async contexts (e.g. atexit).
    #[allow(dead_code)]
    pub fn stop(&self) {
        for name in &self.container_names {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", name])
                .output();
        }
        if let Some(ref dir) = self.remote_data_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    pub fn sasl_users(&self) -> &[(String, String)] {
        &self.sasl_users
    }

    /// Returns the plaintext (non-SASL) bootstrap servers address.
    pub fn plaintext_bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub async fn get_fluss_connection(&self) -> FlussConnection {
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: self.bootstrap_servers.clone(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    /// Connect with SASL/PLAIN credentials.
    /// Uses `sasl_bootstrap_servers` when dual listeners are configured.
    pub async fn get_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> FlussConnection {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        self.connect_with_retry(config).await
    }

    /// Try to connect with SASL/PLAIN credentials, returning the error on failure.
    /// Uses `sasl_bootstrap_servers` when dual listeners are configured.
    pub async fn try_fluss_connection_with_sasl(
        &self,
        username: &str,
        password: &str,
    ) -> fluss::error::Result<FlussConnection> {
        let bootstrap = self
            .sasl_bootstrap_servers
            .clone()
            .unwrap_or_else(|| self.bootstrap_servers.clone());
        let config = Config {
            writer_acks: "all".to_string(),
            bootstrap_servers: bootstrap,
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: username.to_string(),
            security_sasl_password: password.to_string(),
            ..Default::default()
        };

        FlussConnection::new(config).await
    }

    async fn connect_with_retry(&self, config: Config) -> FlussConnection {
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
