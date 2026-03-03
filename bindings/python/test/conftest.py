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

The first pytest-xdist worker to run starts the cluster; other workers
detect it via port check and reuse it (matching the C++ test pattern).
Containers are cleaned up after all workers finish via pytest_unconfigure.

Run with:
    uv run maturin develop && uv run pytest test/ -v -n auto
"""

import asyncio
import os
import socket
import subprocess
import time

# Disable testcontainers Ryuk reaper for xdist runs — it would kill
# containers when the first worker exits, while others are still running.
# We handle cleanup ourselves in pytest_unconfigure.
# In single-process mode, keep Ryuk as a safety net for hard crashes.
if "PYTEST_XDIST_WORKER" in os.environ:
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

import pytest
import pytest_asyncio

import fluss

FLUSS_IMAGE = "apache/fluss"
FLUSS_VERSION = "0.9.0-incubating"
BOOTSTRAP_SERVERS_ENV = os.environ.get("FLUSS_BOOTSTRAP_SERVERS")

# Container / network names
NETWORK_NAME = "fluss-python-test-network"
ZOOKEEPER_NAME = "zookeeper-python-test"
COORDINATOR_NAME = "coordinator-server-python-test"
TABLET_SERVER_NAME = "tablet-server-python-test"

# Fixed host ports (must match across workers)
COORDINATOR_PORT = 9123
TABLET_SERVER_PORT = 9124
PLAIN_CLIENT_PORT = 9223
PLAIN_CLIENT_TABLET_PORT = 9224

ALL_PORTS = [COORDINATOR_PORT, TABLET_SERVER_PORT, PLAIN_CLIENT_PORT, PLAIN_CLIENT_TABLET_PORT]


def _wait_for_port(host, port, timeout=60):
    """Wait for a TCP port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, TimeoutError, OSError):
            time.sleep(1)
    return False


def _all_ports_ready(timeout=60):
    """Wait for all cluster ports to become available."""
    deadline = time.time() + timeout
    for port in ALL_PORTS:
        remaining = deadline - time.time()
        if remaining <= 0 or not _wait_for_port("localhost", port, timeout=remaining):
            return False
    return True


def _run_cmd(cmd):
    """Run a command (list form), return exit code."""
    return subprocess.run(cmd, capture_output=True).returncode


def _wait_for_coordinator_ready(host, port, timeout=60):
    """Poll list_databases() until the CoordinatorEventProcessor is ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            async def _probe():
                cfg = fluss.Config({"bootstrap.servers": f"{host}:{port}"})
                conn = await fluss.FlussConnection.create(cfg)
                try:
                    admin = await conn.get_admin()
                    await admin.list_databases()
                    return True
                finally:
                    conn.close()

            if asyncio.run(_probe()):
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Fluss coordinator on {host}:{port} did not become ready after {timeout}s. "
        "CoordinatorEventProcessor may not be initialised."
    )


def _start_cluster():
    """Start the Fluss Docker cluster via testcontainers.

    If another worker already started the cluster (detected via port check),
    reuse it. If container creation fails (name conflict from a racing worker),
    wait for the other worker's cluster to become ready.
    """
    # Reuse cluster started by another parallel worker or previous run.
    if _wait_for_port("localhost", PLAIN_CLIENT_PORT, timeout=1):
        print("Reusing existing cluster via port check.")
        return

    from testcontainers.core.container import DockerContainer

    print("Starting Fluss cluster via testcontainers...")

    # Create a named network via Docker CLI (idempotent, avoids orphaned
    # random-named networks when multiple xdist workers race).
    _run_cmd(["docker", "network", "create", NETWORK_NAME])

    sasl_jaas = (
        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required"
        ' user_admin="admin-secret" user_alice="alice-secret";'
    )
    coordinator_props = "\n".join([
        f"zookeeper.address: {ZOOKEEPER_NAME}:2181",
        f"bind.listeners: INTERNAL://{COORDINATOR_NAME}:0,"
        f" CLIENT://{COORDINATOR_NAME}:9123,"
        f" PLAIN_CLIENT://{COORDINATOR_NAME}:9223",
        "advertised.listeners: CLIENT://localhost:9123,"
        " PLAIN_CLIENT://localhost:9223",
        "internal.listener.name: INTERNAL",
        "security.protocol.map: CLIENT:sasl",
        "security.sasl.enabled.mechanisms: plain",
        f"security.sasl.plain.jaas.config: {sasl_jaas}",
        "netty.server.num-network-threads: 1",
        "netty.server.num-worker-threads: 3",
    ])
    tablet_props = "\n".join([
        f"zookeeper.address: {ZOOKEEPER_NAME}:2181",
        f"bind.listeners: INTERNAL://{TABLET_SERVER_NAME}:0,"
        f" CLIENT://{TABLET_SERVER_NAME}:9123,"
        f" PLAIN_CLIENT://{TABLET_SERVER_NAME}:9223",
        "advertised.listeners: CLIENT://localhost:9124,"
        " PLAIN_CLIENT://localhost:9224",
        "internal.listener.name: INTERNAL",
        "security.protocol.map: CLIENT:sasl",
        "security.sasl.enabled.mechanisms: plain",
        f"security.sasl.plain.jaas.config: {sasl_jaas}",
        "tablet-server.id: 0",
        "netty.server.num-network-threads: 1",
        "netty.server.num-worker-threads: 3",
    ])

    zookeeper = (
        DockerContainer("zookeeper:3.9.2")
        .with_kwargs(network=NETWORK_NAME)
        .with_name(ZOOKEEPER_NAME)
    )
    coordinator = (
        DockerContainer(f"{FLUSS_IMAGE}:{FLUSS_VERSION}")
        .with_kwargs(network=NETWORK_NAME)
        .with_name(COORDINATOR_NAME)
        .with_bind_ports(9123, 9123)
        .with_bind_ports(9223, 9223)
        .with_command("coordinatorServer")
        .with_env("FLUSS_PROPERTIES", coordinator_props)
    )
    tablet_server = (
        DockerContainer(f"{FLUSS_IMAGE}:{FLUSS_VERSION}")
        .with_kwargs(network=NETWORK_NAME)
        .with_name(TABLET_SERVER_NAME)
        .with_bind_ports(9123, 9124)
        .with_bind_ports(9223, 9224)
        .with_command("tabletServer")
        .with_env("FLUSS_PROPERTIES", tablet_props)
    )

    try:
        zookeeper.start()
        coordinator.start()
        tablet_server.start()
    except Exception as e:
        # Another worker may have started containers with the same names.
        # Wait for the cluster to become ready instead of failing.
        print(f"Container start failed ({e}), waiting for cluster from another worker...")
        if _all_ports_ready():
            return
        raise

    if not _all_ports_ready():
        raise RuntimeError("Cluster listeners did not become ready")

    _wait_for_coordinator_ready("localhost", PLAIN_CLIENT_PORT)
    print("Fluss cluster started successfully.")


def _stop_cluster():
    """Stop and remove the Fluss Docker cluster containers."""
    for name in [TABLET_SERVER_NAME, COORDINATOR_NAME, ZOOKEEPER_NAME]:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(["docker", "network", "rm", NETWORK_NAME], capture_output=True)


async def _connect_with_retry(bootstrap_servers, timeout=60):
    """Connect to the Fluss cluster with retries until it's fully ready.

    Waits until both the coordinator and at least one tablet server are
    available, matching the Rust wait_for_cluster_ready pattern.
    """
    config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    start = time.time()
    last_err = None
    while time.time() - start < timeout:
        conn = None
        try:
            conn = await fluss.FlussConnection.create(config)
            admin = await conn.get_admin()
            nodes = await admin.get_server_nodes()
            if any(n.server_type == "TabletServer" for n in nodes):
                return conn
            last_err = RuntimeError("No TabletServer available yet")
        except Exception as e:
            last_err = e
        if conn is not None:
            conn.close()
        await asyncio.sleep(1)
    raise RuntimeError(
        f"Could not connect to cluster after {timeout}s: {last_err}"
    )


def pytest_unconfigure(config):
    """Clean up Docker containers after all xdist workers finish.

    Runs once on the controller process (or the single process when
    not using xdist). Workers are identified by the 'workerinput' attr.
    """
    if BOOTSTRAP_SERVERS_ENV:
        return
    if hasattr(config, "workerinput"):
        return  # This is a worker, skip
    _stop_cluster()


@pytest.fixture(scope="session")
def fluss_cluster():
    """Start a Fluss cluster using testcontainers, or use an existing one."""
    if BOOTSTRAP_SERVERS_ENV:
        sasl_env = os.environ.get(
            "FLUSS_SASL_BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS_ENV
        )
        yield (BOOTSTRAP_SERVERS_ENV, sasl_env)
        return

    _start_cluster()

    # (plaintext_bootstrap, sasl_bootstrap)
    yield (
        f"127.0.0.1:{PLAIN_CLIENT_PORT}",
        f"127.0.0.1:{COORDINATOR_PORT}",
    )


@pytest_asyncio.fixture(scope="session")
async def connection(fluss_cluster):
    """Session-scoped connection to the Fluss cluster (plaintext)."""
    plaintext_addr, _sasl_addr = fluss_cluster
    conn = await _connect_with_retry(plaintext_addr)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sasl_bootstrap_servers(fluss_cluster):
    """Bootstrap servers for the SASL listener."""
    _plaintext_addr, sasl_addr = fluss_cluster
    return sasl_addr


@pytest.fixture(scope="session")
def plaintext_bootstrap_servers(fluss_cluster):
    """Bootstrap servers for the plaintext (non-SASL) listener."""
    plaintext_addr, _sasl_addr = fluss_cluster
    return plaintext_addr


@pytest_asyncio.fixture(scope="session")
async def admin(connection):
    """Session-scoped admin client."""
    return await connection.get_admin()
