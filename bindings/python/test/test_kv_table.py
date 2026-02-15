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

"""Integration tests for KV (primary key) table operations.

Mirrors the Rust integration tests in crates/fluss/tests/integration/kv_table.rs.
"""

import math
from datetime import date, datetime
from datetime import time as dt_time
from decimal import Decimal

import pyarrow as pa

import fluss


async def test_upsert_delete_and_lookup(connection, admin):
    """Test upsert, lookup, update, delete, and non-existent key lookup."""
    table_path = fluss.TablePath("fluss", "py_test_upsert_and_lookup")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    test_data = [(1, "Verso", 32), (2, "Noco", 25), (3, "Esquie", 35)]

    # Upsert rows (fire-and-forget, then flush)
    for id_, name, age in test_data:
        upsert_writer.upsert({"id": id_, "name": name, "age": age})
    await upsert_writer.flush()

    # Lookup and verify
    lookuper = table.new_lookup().create_lookuper()

    for id_, expected_name, expected_age in test_data:
        result = await lookuper.lookup({"id": id_})
        assert result is not None, f"Row with id={id_} should exist"
        assert result["id"] == id_
        assert result["name"] == expected_name
        assert result["age"] == expected_age

    # Update record with id=1 (await acknowledgment)
    handle = upsert_writer.upsert({"id": 1, "name": "Verso", "age": 33})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["age"] == 33
    assert result["name"] == "Verso"

    # Delete record with id=1 (await acknowledgment)
    handle = upsert_writer.delete({"id": 1})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is None, "Record 1 should not exist after delete"

    # Verify other records still exist
    for id_ in [2, 3]:
        result = await lookuper.lookup({"id": id_})
        assert result is not None, f"Record {id_} should still exist"

    # Lookup non-existent key
    result = await lookuper.lookup({"id": 999})
    assert result is None, "Non-existent key should return None"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_composite_primary_keys(connection, admin):
    """Test upsert and lookup with composite (multi-column) primary keys."""
    table_path = fluss.TablePath("fluss", "py_test_composite_pk")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("user_id", pa.int32()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["region", "user_id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    test_data = [
        ("US", 1, 100),
        ("US", 2, 200),
        ("EU", 1, 150),
        ("EU", 2, 250),
    ]

    for region, user_id, score in test_data:
        upsert_writer.upsert({"region": region, "user_id": user_id, "score": score})
    await upsert_writer.flush()

    lookuper = table.new_lookup().create_lookuper()

    # Lookup (US, 1) -> score 100
    result = await lookuper.lookup({"region": "US", "user_id": 1})
    assert result is not None
    assert result["score"] == 100

    # Lookup (EU, 2) -> score 250
    result = await lookuper.lookup({"region": "EU", "user_id": 2})
    assert result is not None
    assert result["score"] == 250

    # Update (US, 1) score (await acknowledgment)
    handle = upsert_writer.upsert({"region": "US", "user_id": 1, "score": 500})
    await handle.wait()

    result = await lookuper.lookup({"region": "US", "user_id": 1})
    assert result is not None
    assert result["score"] == 500

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partial_update(connection, admin):
    """Test partial column update via partial_update_by_name."""
    table_path = fluss.TablePath("fluss", "py_test_partial_update")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)

    # Insert initial record
    upsert_writer = table.new_upsert().create_writer()
    handle = upsert_writer.upsert(
        {"id": 1, "name": "Verso", "age": 32, "score": 6942}
    )
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Verso"
    assert result["age"] == 32
    assert result["score"] == 6942

    # Partial update: only update score column
    partial_writer = (
        table.new_upsert().partial_update_by_name(["id", "score"]).create_writer()
    )
    handle = partial_writer.upsert({"id": 1, "score": 420})
    await handle.wait()

    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Verso", "name should remain unchanged"
    assert result["age"] == 32, "age should remain unchanged"
    assert result["score"] == 420, "score should be updated to 420"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partial_update_by_index(connection, admin):
    """Test partial column update via partial_update_by_index."""
    table_path = fluss.TablePath("fluss", "py_test_partial_update_by_index")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("age", pa.int64()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["id"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)

    upsert_writer = table.new_upsert().create_writer()
    handle = upsert_writer.upsert(
        {"id": 1, "name": "Verso", "age": 32, "score": 6942}
    )
    await handle.wait()

    # Partial update by indices: columns 0=id (PK), 1=name
    partial_writer = (
        table.new_upsert().partial_update_by_index([0, 1]).create_writer()
    )
    handle = partial_writer.upsert([1, "Verso Renamed"])
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"id": 1})
    assert result is not None
    assert result["name"] == "Verso Renamed", "name should be updated"
    assert result["score"] == 6942, "score should remain unchanged"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_partitioned_table_upsert_and_lookup(connection, admin):
    """Test upsert/lookup/delete on a partitioned KV table."""
    table_path = fluss.TablePath("fluss", "py_test_partitioned_kv_table")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("region", pa.string()),
                pa.field("user_id", pa.int32()),
                pa.field("name", pa.string()),
                pa.field("score", pa.int64()),
            ]
        ),
        primary_keys=["region", "user_id"],
    )
    table_descriptor = fluss.TableDescriptor(
        schema,
        partition_keys=["region"],
    )
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    # Create partitions
    for region in ["US", "EU", "APAC"]:
        await admin.create_partition(
            table_path, {"region": region}, ignore_if_exists=True
        )

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    test_data = [
        ("US", 1, "Gustave", 100),
        ("US", 2, "Lune", 200),
        ("EU", 1, "Sciel", 150),
        ("EU", 2, "Maelle", 250),
        ("APAC", 1, "Noco", 300),
    ]

    for region, user_id, name, score in test_data:
        upsert_writer.upsert(
            {"region": region, "user_id": user_id, "name": name, "score": score}
        )
    await upsert_writer.flush()

    lookuper = table.new_lookup().create_lookuper()

    # Verify all rows across partitions
    for region, user_id, expected_name, expected_score in test_data:
        result = await lookuper.lookup({"region": region, "user_id": user_id})
        assert result is not None, f"Row ({region}, {user_id}) should exist"
        assert result["region"] == region
        assert result["user_id"] == user_id
        assert result["name"] == expected_name
        assert result["score"] == expected_score

    # Update within a partition (await acknowledgment)
    handle = upsert_writer.upsert(
        {"region": "US", "user_id": 1, "name": "Gustave Updated", "score": 999}
    )
    await handle.wait()

    result = await lookuper.lookup({"region": "US", "user_id": 1})
    assert result is not None
    assert result["name"] == "Gustave Updated"
    assert result["score"] == 999

    # Lookup in non-existent partition should return None
    result = await lookuper.lookup({"region": "UNKNOWN_REGION", "user_id": 1})
    assert result is None, "Lookup in non-existent partition should return None"

    # Delete within a partition (await acknowledgment)
    handle = upsert_writer.delete({"region": "EU", "user_id": 1})
    await handle.wait()

    result = await lookuper.lookup({"region": "EU", "user_id": 1})
    assert result is None, "Deleted record should not exist"

    # Verify sibling record still exists
    result = await lookuper.lookup({"region": "EU", "user_id": 2})
    assert result is not None
    assert result["name"] == "Maelle"

    await admin.drop_table(table_path, ignore_if_not_exists=False)


async def test_all_supported_datatypes(connection, admin):
    """Test upsert/lookup for all supported data types, including nulls."""
    table_path = fluss.TablePath("fluss", "py_test_kv_all_datatypes")
    await admin.drop_table(table_path, ignore_if_not_exists=True)

    schema = fluss.Schema(
        pa.schema(
            [
                pa.field("pk_int", pa.int32()),
                pa.field("col_boolean", pa.bool_()),
                pa.field("col_tinyint", pa.int8()),
                pa.field("col_smallint", pa.int16()),
                pa.field("col_int", pa.int32()),
                pa.field("col_bigint", pa.int64()),
                pa.field("col_float", pa.float32()),
                pa.field("col_double", pa.float64()),
                pa.field("col_string", pa.string()),
                pa.field("col_decimal", pa.decimal128(10, 2)),
                pa.field("col_date", pa.date32()),
                pa.field("col_time", pa.time32("ms")),
                pa.field("col_timestamp_ntz", pa.timestamp("us")),
                pa.field("col_timestamp_ltz", pa.timestamp("us", tz="UTC")),
                pa.field("col_bytes", pa.binary()),
            ]
        ),
        primary_keys=["pk_int"],
    )
    table_descriptor = fluss.TableDescriptor(schema)
    await admin.create_table(table_path, table_descriptor, ignore_if_exists=False)

    table = await connection.get_table(table_path)
    upsert_writer = table.new_upsert().create_writer()

    # Test data for all types
    row_data = {
        "pk_int": 1,
        "col_boolean": True,
        "col_tinyint": 127,
        "col_smallint": 32767,
        "col_int": 2147483647,
        "col_bigint": 9223372036854775807,
        "col_float": 3.14,
        "col_double": 2.718281828459045,
        "col_string": "world of fluss python client",
        "col_decimal": Decimal("123.45"),
        "col_date": date(2026, 1, 23),
        "col_time": dt_time(10, 13, 47, 123000),  # millisecond precision
        "col_timestamp_ntz": datetime(2026, 1, 23, 10, 13, 47, 123000),
        "col_timestamp_ltz": datetime(2026, 1, 23, 10, 13, 47, 123000),
        "col_bytes": b"binary data",
    }

    handle = upsert_writer.upsert(row_data)
    await handle.wait()

    lookuper = table.new_lookup().create_lookuper()
    result = await lookuper.lookup({"pk_int": 1})
    assert result is not None, "Row should exist"

    assert result["pk_int"] == 1
    assert result["col_boolean"] is True
    assert result["col_tinyint"] == 127
    assert result["col_smallint"] == 32767
    assert result["col_int"] == 2147483647
    assert result["col_bigint"] == 9223372036854775807
    assert math.isclose(result["col_float"], 3.14, rel_tol=1e-6)
    assert math.isclose(result["col_double"], 2.718281828459045, rel_tol=1e-15)
    assert result["col_string"] == "world of fluss python client"
    assert result["col_decimal"] == Decimal("123.45")
    assert result["col_date"] == date(2026, 1, 23)
    assert result["col_time"] == dt_time(10, 13, 47, 123000)
    assert result["col_bytes"] == b"binary data"

    # Test with null values for all nullable columns
    null_row = {"pk_int": 2}
    for col in row_data:
        if col != "pk_int":
            null_row[col] = None
    handle = upsert_writer.upsert(null_row)
    await handle.wait()

    result = await lookuper.lookup({"pk_int": 2})
    assert result is not None, "Row with nulls should exist"
    assert result["pk_int"] == 2
    for col in row_data:
        if col != "pk_int":
            assert result[col] is None, f"{col} should be null"

    await admin.drop_table(table_path, ignore_if_not_exists=False)
