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

static constexpr const char* kFlussVersion = "0.7.0";
static constexpr const char* kNetworkName = "fluss-cpp-test-network";
static constexpr const char* kZookeeperName = "zookeeper-cpp-test";
static constexpr const char* kCoordinatorName = "coordinator-server-cpp-test";
static constexpr const char* kTabletServerName = "tablet-server-cpp-test";
static constexpr int kCoordinatorPort = 9123;
static constexpr int kTabletServerPort = 9124;

/// Execute a shell command and return its exit code.
inline int RunCommand(const std::string& cmd) {
    return system(cmd.c_str());
}

/// Wait until a TCP port is accepting connections, or timeout.
inline bool WaitForPort(const std::string& host, int port, int timeout_seconds = 60) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);

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
        std::string zk_cmd = std::string("docker run -d --rm") +
                              " --name " + kZookeeperName +
                              " --network " + kNetworkName +
                              " zookeeper:3.9.2";
        if (RunCommand(zk_cmd) != 0) {
            std::cerr << "Failed to start ZooKeeper" << std::endl;
            return false;
        }

        // Wait for ZooKeeper to be ready before starting Fluss servers
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Start Coordinator Server
        std::string coord_props =
            "zookeeper.address: " + std::string(kZookeeperName) + ":2181\\n"
            "bind.listeners: INTERNAL://" + std::string(kCoordinatorName) + ":0, CLIENT://" +
            std::string(kCoordinatorName) + ":9123\\n"
            "advertised.listeners: CLIENT://localhost:9123\\n"
            "internal.listener.name: INTERNAL\\n"
            "netty.server.num-network-threads: 1\\n"
            "netty.server.num-worker-threads: 3";

        std::string coord_cmd = std::string("docker run -d --rm") +
                                " --name " + kCoordinatorName +
                                " --network " + kNetworkName +
                                " -p 9123:9123" +
                                " -e FLUSS_PROPERTIES=\"$(printf '" + coord_props + "')\"" +
                                " fluss/fluss:" + kFlussVersion +
                                " coordinatorServer";
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

        // Start Tablet Server
        std::string ts_props =
            "zookeeper.address: " + std::string(kZookeeperName) + ":2181\\n"
            "bind.listeners: INTERNAL://" + std::string(kTabletServerName) + ":0, CLIENT://" +
            std::string(kTabletServerName) + ":9123\\n"
            "advertised.listeners: CLIENT://localhost:" + std::to_string(kTabletServerPort) + "\\n"
            "internal.listener.name: INTERNAL\\n"
            "tablet-server.id: 0\\n"
            "netty.server.num-network-threads: 1\\n"
            "netty.server.num-worker-threads: 3";

        std::string ts_cmd = std::string("docker run -d --rm") +
                             " --name " + kTabletServerName +
                             " --network " + kNetworkName +
                             " -p " + std::to_string(kTabletServerPort) + ":9123" +
                             " -e FLUSS_PROPERTIES=\"$(printf '" + ts_props + "')\"" +
                             " fluss/fluss:" + kFlussVersion +
                             " tabletServer";
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

        bootstrap_servers_ = "127.0.0.1:9123";
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

   private:
    std::string bootstrap_servers_;
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

        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
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

    void TearDown() override {
        cluster_.Stop();
    }

    fluss::Connection& GetConnection() { return connection_; }
    fluss::Admin& GetAdmin() { return admin_; }
    const std::string& GetBootstrapServers() { return cluster_.GetBootstrapServers(); }

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

}  // namespace fluss_test
