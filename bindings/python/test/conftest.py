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

import asyncio
import os
import time

import pytest
import pytest_asyncio

import fluss

CLUSTER_NAME = "shared-test"


async def _connect(bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    start = time.time()
    last_err = None
    while time.time() - start < 60:
        try:
            conn = await fluss.FlussConnection.create(config)
            admin = conn.get_admin()
            nodes = await admin.get_server_nodes()
            if any(n.server_type == "TabletServer" for n in nodes):
                return conn
            conn.close()
            last_err = RuntimeError("No TabletServer available yet")
        except Exception as e:
            last_err = e
        await asyncio.sleep(1)
    raise RuntimeError(f"Could not connect after 60s: {last_err}")


def pytest_unconfigure(config):
    if os.environ.get("FLUSS_BOOTSTRAP_SERVERS"):
        return
    if hasattr(config, "workerinput"):
        return
    fluss.FlussTestCluster.stop_by_name(CLUSTER_NAME)


@pytest.fixture(scope="session")
def fluss_cluster():
    env = os.environ.get("FLUSS_BOOTSTRAP_SERVERS")
    if env:
        sasl_env = os.environ.get("FLUSS_SASL_BOOTSTRAP_SERVERS", env)
        yield (env, sasl_env)
        return

    loop = asyncio.new_event_loop()
    try:
        cluster = loop.run_until_complete(
            fluss.FlussTestCluster.start(CLUSTER_NAME, sasl=True)
        )
    finally:
        loop.close()
    plaintext_addr = cluster.bootstrap_servers
    sasl_addr = cluster.sasl_bootstrap_servers or plaintext_addr
    yield (plaintext_addr, sasl_addr)


_cached_connection = None


@pytest_asyncio.fixture
async def connection(fluss_cluster):
    global _cached_connection
    if _cached_connection is None:
        plaintext_addr, _sasl_addr = fluss_cluster
        _cached_connection = await _connect(plaintext_addr)
    yield _cached_connection


@pytest.fixture(scope="session")
def sasl_bootstrap_servers(fluss_cluster):
    _plaintext_addr, sasl_addr = fluss_cluster
    return sasl_addr


@pytest.fixture(scope="session")
def plaintext_bootstrap_servers(fluss_cluster):
    plaintext_addr, _sasl_addr = fluss_cluster
    return plaintext_addr


@pytest_asyncio.fixture
async def admin(connection):
    return connection.get_admin()
