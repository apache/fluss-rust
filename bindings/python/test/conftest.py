# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Shared fixtures for Fluss Python integration tests.

If FLUSS_BOOTSTRAP_SERVERS is set, tests connect to an existing cluster.
Otherwise, a Fluss cluster is started automatically via testcontainers.

Run with:
    uv run maturin develop && uv run pytest test/ -v
"""

import os
import socket
import time

import pytest
import pytest_asyncio

import fluss

FLUSS_VERSION = "0.7.0"
BOOTSTRAP_SERVERS_ENV = os.environ.get("FLUSS_BOOTSTRAP_SERVERS")


def _wait_for_port(host, port, timeout=60):
    """Wait for a TCP port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except (ConnectionRefusedError, TimeoutError, OSError):
            time.sleep(1)
    raise TimeoutError(f"Port {port} on {host} not available after {timeout}s")


@pytest.fixture(scope="session")
def fluss_cluster():
    """Start a Fluss cluster using testcontainers, or use an existing one."""
    if BOOTSTRAP_SERVERS_ENV:
        yield BOOTSTRAP_SERVERS_ENV
        return

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    network = Network()
    network.create()

    zookeeper = (
        DockerContainer("zookeeper:3.9.2")
        .with_network(network)
        .with_name("zookeeper-python-test")
    )

    coordinator_props = "\n".join([
        "zookeeper.address: zookeeper-python-test:2181",
        "bind.listeners: INTERNAL://coordinator-server-python-test:0,"
        " CLIENT://coordinator-server-python-test:9123",
        "advertised.listeners: CLIENT://localhost:9123",
        "internal.listener.name: INTERNAL",
        "netty.server.num-network-threads: 1",
        "netty.server.num-worker-threads: 3",
    ])
    coordinator = (
        DockerContainer(f"fluss/fluss:{FLUSS_VERSION}")
        .with_network(network)
        .with_name("coordinator-server-python-test")
        .with_bind_ports(9123, 9123)
        .with_command("coordinatorServer")
        .with_env("FLUSS_PROPERTIES", coordinator_props)
    )

    tablet_props = "\n".join([
        "zookeeper.address: zookeeper-python-test:2181",
        "bind.listeners: INTERNAL://tablet-server-python-test:0,"
        " CLIENT://tablet-server-python-test:9123",
        "advertised.listeners: CLIENT://localhost:9124",
        "internal.listener.name: INTERNAL",
        "tablet-server.id: 0",
        "netty.server.num-network-threads: 1",
        "netty.server.num-worker-threads: 3",
    ])
    tablet_server = (
        DockerContainer(f"fluss/fluss:{FLUSS_VERSION}")
        .with_network(network)
        .with_name("tablet-server-python-test")
        .with_bind_ports(9123, 9124)
        .with_command("tabletServer")
        .with_env("FLUSS_PROPERTIES", tablet_props)
    )

    zookeeper.start()
    coordinator.start()
    tablet_server.start()

    _wait_for_port("localhost", 9123)
    _wait_for_port("localhost", 9124)
    # Extra wait for cluster to fully initialize
    time.sleep(10)

    yield "127.0.0.1:9123"

    tablet_server.stop()
    coordinator.stop()
    zookeeper.stop()
    network.remove()


@pytest_asyncio.fixture(scope="session")
async def connection(fluss_cluster):
    """Session-scoped connection to the Fluss cluster."""
    config = fluss.Config({"bootstrap.servers": fluss_cluster})
    conn = await fluss.FlussConnection.create(config)
    yield conn
    conn.close()


@pytest_asyncio.fixture(scope="session")
async def admin(connection):
    """Session-scoped admin client."""
    return await connection.get_admin()
