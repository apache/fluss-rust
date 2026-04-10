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
import pytest
import pyarrow as pa
import time
import fluss

def _poll_records(scanner, expected_count, timeout_s=10):
    """Poll a record-based scanner until expected_count records are collected."""
    collected = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected_count and time.monotonic() < deadline:
        records = scanner.poll(5000)
        collected.extend(records)
    return collected

@pytest.mark.asyncio
async def test_connection_context_manager(plaintext_bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": plaintext_bootstrap_servers})
    async with await fluss.FlussConnection.create(config) as conn:
        admin = conn.get_admin()
        nodes = await admin.get_server_nodes()
        assert len(nodes) > 0
    # conn should be closed (though currently close is a no-op in python side, but verifies syntax)

@pytest.mark.asyncio
async def test_append_writer_success_flush(connection, admin):
    table_path = fluss.TablePath("fluss", "test_append_ctx_success")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    
    table = await connection.get_table(table_path)
    
    async with table.new_append().create_writer() as writer:
        writer.append({"a": 1})
        writer.append({"a": 2})
        # No explicit flush here
        
    # After context exit, data should be flushed
    scanner = await table.new_scan().create_log_scanner()
    scanner.subscribe(0, fluss.EARLIEST_OFFSET)
    records = _poll_records(scanner, expected_count=2)
    assert len(records) == 2
    assert sorted([r.row["a"] for r in records]) == [1, 2]

@pytest.mark.asyncio
async def test_append_writer_exception_no_flush(connection, admin):
    table_path = fluss.TablePath("fluss", "test_append_ctx_fail")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    table = await connection.get_table(table_path)
    
    class TestException(Exception): pass
    
    start_time = time.perf_counter()
    try:
        async with table.new_append().create_writer() as writer:
            writer.append({"a": 100})
            raise TestException("abort")
    except TestException:
        pass
    duration = time.perf_counter() - start_time
    
    # Verification:
    # 1. The exception was propagated immediately.
    # 2. The block exited nearly instantly because it bypassed the network flush.
    assert duration < 0.1, f"Context exit took too long ({duration:.3f}s), likely performed a flush"

    # NOTE: Records may still eventually arrive because of the background sender threads.
    # We don't assert 0 records here because Fluss does not support true transactional rollback.

@pytest.mark.asyncio
async def test_upsert_writer_context_manager(connection, admin):
    table_path = fluss.TablePath("fluss", "test_upsert_ctx")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    
    schema = fluss.Schema(pa.schema([pa.field("id", pa.int32()), pa.field("v", pa.string())]), primary_keys=["id"])
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    
    table = await connection.get_table(table_path)
    
    # Success path: verify it flushes
    async with table.new_upsert().create_writer() as writer:
        writer.upsert({"id": 1, "v": "a"})
        
    lookuper = table.new_lookup().create_lookuper()
    res = await lookuper.lookup({"id": 1})
    assert res is not None
    assert res["v"] == "a"
    
    # Failure path: verify it bypasses flush
    class TestException(Exception): pass
    start_time = time.perf_counter()
    try:
        async with table.new_upsert().create_writer() as writer:
            writer.upsert({"id": 2, "v": "b"})
            raise TestException("abort")
    except TestException:
        pass
    duration = time.perf_counter() - start_time
    assert duration < 0.1, f"Context exit took too long ({duration:.3f}s), likely performed a flush"

@pytest.mark.asyncio
async def test_log_scanner_context_manager(connection, admin):
    table_path = fluss.TablePath("fluss", "test_scanner_ctx")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    table = await connection.get_table(table_path)
    
    async with await table.new_scan().create_log_scanner() as scanner:
        scanner.subscribe(0, fluss.EARLIEST_OFFSET)
        # Verifies it works and closes properly
        res = scanner.poll(100)
        assert len(res) == 0

@pytest.mark.asyncio
async def test_connection_context_manager_exception(plaintext_bootstrap_servers):
    config = fluss.Config({"bootstrap.servers": plaintext_bootstrap_servers})
    class TestException(Exception): pass
    
    try:
        async with await fluss.FlussConnection.create(config) as conn:
            raise TestException("connection error")
    except TestException:
        pass
    # If we reach here without hanging, the connection __aexit__ gracefully handled the error

@pytest.mark.asyncio
async def test_record_batch_scanner_context_manager(connection, admin):
    table_path = fluss.TablePath("fluss", "test_batch_scanner_ctx")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    table = await connection.get_table(table_path)
    
    async with await table.new_scan().create_record_batch_log_scanner() as scanner:
        scanner.subscribe(0, fluss.EARLIEST_OFFSET)
        res = scanner.poll_arrow(100)
        assert res.num_rows == 0

@pytest.mark.asyncio
async def test_scanner_exception_propagation(connection, admin):
    table_path = fluss.TablePath("fluss", "test_scanner_ctx_fail")
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    schema = fluss.Schema(pa.schema([pa.field("a", pa.int32())]))
    await admin.create_table(table_path, fluss.TableDescriptor(schema))
    table = await connection.get_table(table_path)
    
    class TestException(Exception): pass
    
    # Test record scanner exception
    try:
        async with await table.new_scan().create_log_scanner() as scanner:
            scanner.subscribe(0, fluss.EARLIEST_OFFSET)
            raise TestException("scanner error")
    except TestException:
        pass

    # Test batch scanner exception
    try:
        async with await table.new_scan().create_record_batch_log_scanner() as scanner:
            scanner.subscribe(0, fluss.EARLIEST_OFFSET)
            raise TestException("batch scanner error")
    except TestException:
        pass