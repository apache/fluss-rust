/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "fluss.hpp"

// Macro to assert Result is OK and print error message on failure
#define ASSERT_OK(result) ASSERT_TRUE((result).Ok()) << (result).error_message
#define EXPECT_OK(result) EXPECT_TRUE((result).Ok()) << (result).error_message

namespace fluss_test {

static constexpr const char* kFlussImage = "apache/fluss";
static constexpr const char* kFlussVersion = "0.8.0-incubating";
static constexpr const char* kNetworkName = "fluss-cpp-test-network";
static constexpr const char* kZookeeperName = "zookeeper-cpp-test";
static constexpr const char* kCoordinatorName = "coordinator-server-cpp-test";
static constexpr const char* kTabletServerName = "tablet-server-cpp-test";
static constexpr int kCoordinatorPort = 9123;
static constexpr int kTabletServerPort = 9124;
static constexpr int kPlainClientPort = 9223;
static constexpr int kPlainClientTabletPort = 9224;

/// Execute a shell command and return its exit code.
inline int RunCommand(const std::string& cmd) { return system(cmd.c_str()); }

/// Join property lines with the escaped newline separator used by `printf` in docker commands.
inline std::string JoinProps(const std::vector<std::string>& lines) {
    std::string result;
    for (size_t i = 0; i < lines.size(); ++i) {
        if (i > 0) result += "\\n";
        result += lines[i];
    }
    return result;
}

/// Build a `docker run` command with FLUSS_PROPERTIES.
inline std::string DockerRunCmd(const std::string& name, const std::string& props,
                                const std::vector<std::string>& port_mappings,
                                const std::string& server_type) {
    std::string cmd = "docker run -d --rm --name " + name + " --network " + kNetworkName;
    for (const auto& pm : port_mappings) {
        cmd += " -p " + pm;
    }
    cmd += " -e FLUSS_PROPERTIES=\"$(printf '" + props + "')\"";
    cmd += " " + std::string(kFlussImage) + ":" + kFlussVersion + " " + server_type;
    return cmd;
}

/// Wait until a TCP port is accepting connections, or timeout.
inline bool WaitForPort(const std::string& host, int port, int timeout_seconds = 60) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);

    while (std::chrono::steady_clock::now() < deadline) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        struct sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        int result = connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
#ifdef _WIN32
        closesocket(sock);
#else
        close(sock);
#endif
        if (result == 0) {
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

/// Manages a Docker-based Fluss cluster for integration testing.
class FlussTestCluster {
   public:
    FlussTestCluster() = default;

    bool Start() {
        const char* env_servers = std::getenv("FLUSS_BOOTSTRAP_SERVERS");
        if (env_servers && std::strlen(env_servers) > 0) {
            bootstrap_servers_ = env_servers;
            external_cluster_ = true;
            std::cout << "Using external cluster: " << bootstrap_servers_ << std::endl;
            return true;
        }

        std::cout << "Starting Fluss cluster via Docker..." << std::endl;

        // Create network
        RunCommand(std::string("docker network create ") + kNetworkName + " 2>/dev/null || true");

        // Start ZooKeeper
        std::string zk_cmd = std::string("docker run -d --rm") + " --name " + kZookeeperName +
                             " --network " + kNetworkName + " zookeeper:3.9.2";
        if (RunCommand(zk_cmd) != 0) {
            std::cerr << "Failed to start ZooKeeper" << std::endl;
            return false;
        }

        // Wait for ZooKeeper to be ready before starting Fluss servers
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Start Coordinator Server (dual listeners: CLIENT=SASL on 9123, PLAIN_CLIENT=plaintext on
        // 9223)
        std::string sasl_jaas =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required"
            " user_admin=\"admin-secret\" user_alice=\"alice-secret\";";

        std::string coord = std::string(kCoordinatorName);
        std::string zk = std::string(kZookeeperName);
        std::string coord_props = JoinProps({
            "zookeeper.address: " + zk + ":2181",
            "bind.listeners: INTERNAL://" + coord + ":0, CLIENT://" + coord +
                ":9123, PLAIN_CLIENT://" + coord + ":9223",
            "advertised.listeners: CLIENT://localhost:9123, PLAIN_CLIENT://localhost:9223",
            "internal.listener.name: INTERNAL",
            "security.protocol.map: CLIENT:sasl",
            "security.sasl.enabled.mechanisms: plain",
            "security.sasl.plain.jaas.config: " + sasl_jaas,
            "netty.server.num-network-threads: 1",
            "netty.server.num-worker-threads: 3",
        });

        std::string coord_cmd = DockerRunCmd(kCoordinatorName, coord_props,
                                             {"9123:9123", "9223:9223"}, "coordinatorServer");
        if (RunCommand(coord_cmd) != 0) {
            std::cerr << "Failed to start Coordinator Server" << std::endl;
            Stop();
            return false;
        }

        // Wait for coordinator to be ready
        if (!WaitForPort("127.0.0.1", kCoordinatorPort)) {
            std::cerr << "Coordinator Server did not become ready" << std::endl;
            Stop();
            return false;
        }

        // Start Tablet Server (dual listeners: CLIENT=SASL on 9123, PLAIN_CLIENT=plaintext on 9223)
        std::string ts = std::string(kTabletServerName);
        std::string ts_props = JoinProps({
            "zookeeper.address: " + zk + ":2181",
            "bind.listeners: INTERNAL://" + ts + ":0, CLIENT://" + ts + ":9123, PLAIN_CLIENT://" +
                ts + ":9223",
            "advertised.listeners: CLIENT://localhost:" + std::to_string(kTabletServerPort) +
                ", PLAIN_CLIENT://localhost:" + std::to_string(kPlainClientTabletPort),
            "internal.listener.name: INTERNAL",
            "security.protocol.map: CLIENT:sasl",
            "security.sasl.enabled.mechanisms: plain",
            "security.sasl.plain.jaas.config: " + sasl_jaas,
            "tablet-server.id: 0",
            "netty.server.num-network-threads: 1",
            "netty.server.num-worker-threads: 3",
        });

        std::string ts_cmd = DockerRunCmd(kTabletServerName, ts_props,
                                          {std::to_string(kTabletServerPort) + ":9123",
                                           std::to_string(kPlainClientTabletPort) + ":9223"},
                                          "tabletServer");
        if (RunCommand(ts_cmd) != 0) {
            std::cerr << "Failed to start Tablet Server" << std::endl;
            Stop();
            return false;
        }

        // Wait for tablet server to be ready
        if (!WaitForPort("127.0.0.1", kTabletServerPort)) {
            std::cerr << "Tablet Server did not become ready" << std::endl;
            Stop();
            return false;
        }

        // Wait for plaintext listeners
        if (!WaitForPort("127.0.0.1", kPlainClientPort)) {
            std::cerr << "Coordinator plaintext listener did not become ready" << std::endl;
            Stop();
            return false;
        }
        if (!WaitForPort("127.0.0.1", kPlainClientTabletPort)) {
            std::cerr << "Tablet Server plaintext listener did not become ready" << std::endl;
            Stop();
            return false;
        }

        bootstrap_servers_ = "127.0.0.1:" + std::to_string(kPlainClientPort);
        sasl_bootstrap_servers_ = "127.0.0.1:" + std::to_string(kCoordinatorPort);
        std::cout << "Fluss cluster started successfully." << std::endl;
        return true;
    }

    void Stop() {
        if (external_cluster_) return;

        std::cout << "Stopping Fluss cluster..." << std::endl;
        RunCommand(std::string("docker stop ") + kTabletServerName + " 2>/dev/null || true");
        RunCommand(std::string("docker stop ") + kCoordinatorName + " 2>/dev/null || true");
        RunCommand(std::string("docker stop ") + kZookeeperName + " 2>/dev/null || true");
        RunCommand(std::string("docker network rm ") + kNetworkName + " 2>/dev/null || true");
        std::cout << "Fluss cluster stopped." << std::endl;
    }

    const std::string& GetBootstrapServers() const { return bootstrap_servers_; }
    const std::string& GetSaslBootstrapServers() const { return sasl_bootstrap_servers_; }

   private:
    std::string bootstrap_servers_;
    std::string sasl_bootstrap_servers_;
    bool external_cluster_{false};
};

/// GoogleTest Environment that manages the Fluss cluster lifecycle.
class FlussTestEnvironment : public ::testing::Environment {
   public:
    static FlussTestEnvironment* Instance() {
        static FlussTestEnvironment* instance = nullptr;
        if (!instance) {
            instance = new FlussTestEnvironment();
        }
        return instance;
    }

    void SetUp() override {
        if (!cluster_.Start()) {
            GTEST_SKIP() << "Failed to start Fluss cluster. Skipping integration tests.";
        }

        // Retry connection creation until the coordinator is fully initialized.
        fluss::Configuration config;
        config.bootstrap_servers = cluster_.GetBootstrapServers();

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline) {
            auto result = fluss::Connection::Create(config, connection_);
            if (result.Ok()) {
                auto admin_result = connection_.GetAdmin(admin_);
                if (admin_result.Ok()) {
                    std::cout << "Connected to Fluss cluster." << std::endl;
                    return;
                }
            }
            std::cout << "Waiting for Fluss cluster to be ready..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        GTEST_SKIP() << "Fluss cluster did not become ready within timeout.";
    }

    void TearDown() override { cluster_.Stop(); }

    fluss::Connection& GetConnection() { return connection_; }
    fluss::Admin& GetAdmin() { return admin_; }
    const std::string& GetBootstrapServers() { return cluster_.GetBootstrapServers(); }
    const std::string& GetSaslBootstrapServers() { return cluster_.GetSaslBootstrapServers(); }

   private:
    FlussTestEnvironment() = default;

    FlussTestCluster cluster_;
    fluss::Connection connection_;
    fluss::Admin admin_;
};

/// Helper: create a table (assert success). Drops existing table first if it exists.
inline void CreateTable(fluss::Admin& admin, const fluss::TablePath& path,
                        const fluss::TableDescriptor& descriptor) {
    admin.DropTable(path, true);  // ignore if not exists
    auto result = admin.CreateTable(path, descriptor, false);
    ASSERT_OK(result);
}

/// Helper: create partitions for a partitioned table.
inline void CreatePartitions(fluss::Admin& admin, const fluss::TablePath& path,
                             const std::string& partition_column,
                             const std::vector<std::string>& values) {
    for (const auto& value : values) {
        std::unordered_map<std::string, std::string> spec;
        spec[partition_column] = value;
        auto result = admin.CreatePartition(path, spec, true);
        ASSERT_OK(result);
    }
}

/// Poll a LogScanner for ScanRecords until `expected_count` items are collected or timeout.
/// `extract_fn` is called for each ScanRecord and should return a value of type T.
template <typename T, typename ExtractFn>
void PollRecords(fluss::LogScanner& scanner, size_t expected_count, ExtractFn extract_fn,
                 std::vector<T>& out) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (out.size() < expected_count && std::chrono::steady_clock::now() < deadline) {
        fluss::ScanRecords records;
        ASSERT_OK(scanner.Poll(1000, records));
        for (auto rec : records) {
            out.push_back(extract_fn(rec));
        }
    }
}

/// Poll a LogScanner for ArrowRecordBatches until `expected_count` items are collected or timeout.
/// `extract_fn` is called with the full ArrowRecordBatches and should return a std::vector<T>.
template <typename T, typename ExtractFn>
void PollRecordBatches(fluss::LogScanner& scanner, size_t expected_count, ExtractFn extract_fn,
                       std::vector<T>& out) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (out.size() < expected_count && std::chrono::steady_clock::now() < deadline) {
        fluss::ArrowRecordBatches batches;
        ASSERT_OK(scanner.PollRecordBatch(1000, batches));
        auto items = extract_fn(batches);
        out.insert(out.end(), items.begin(), items.end());
    }
}

}  // namespace fluss_test
