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
import time
from datetime import date, time as dt_time, datetime
from decimal import Decimal

import pandas as pd
import pyarrow as pa

import fluss


async def main():
    # Create connection configuration
    config_spec = {
        "bootstrap.servers": "127.0.0.1:9123",
        # Add other configuration options as needed
        "request.max.size": "10485760",  # 10 MB
        "writer.acks": "all",  # Wait for all replicas to acknowledge
        "writer.retries": "3",  # Retry up to 3 times on failure
        "writer.batch.size": "1000",  # Batch size for writes
    }
    config = fluss.Config(config_spec)

    # Create connection using the static connect method
    conn = await fluss.FlussConnection.connect(config)

    # Define fields for PyArrow (including new temporal and decimal types)
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
        pa.field("age", pa.int32()),
        pa.field("birth_date", pa.date32()),
        pa.field("check_in_time", pa.time32("ms")),
        pa.field("created_at", pa.timestamp("us")),  # TIMESTAMP (NTZ)
        pa.field("salary", pa.decimal128(10, 2)),
    ]

    # Create a PyArrow schema
    schema = pa.schema(fields)

    # Create a Fluss Schema first (this is what TableDescriptor expects)
    fluss_schema = fluss.Schema(schema)

    # Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(fluss_schema)

    # Get the admin for Fluss
    admin = await conn.get_admin()

    # Create a Fluss table
    table_path = fluss.TablePath("fluss", "sample_table_types")

    try:
        await admin.create_table(table_path, table_descriptor, True)
        print(f"Created table: {table_path}")
    except Exception as e:
        print(f"Table creation failed: {e}")

    # Get table information via admin
    try:
        table_info = await admin.get_table(table_path)
        print(f"Table info: {table_info}")
        print(f"Table ID: {table_info.table_id}")
        print(f"Schema ID: {table_info.schema_id}")
        print(f"Created time: {table_info.created_time}")
        print(f"Primary keys: {table_info.get_primary_keys()}")
    except Exception as e:
        print(f"Failed to get table info: {e}")

    # Get the table instance
    table = await conn.get_table(table_path)
    print(f"Got table: {table}")

    # Create a writer for the table
    append_writer = await table.new_append_writer()
    print(f"Created append writer: {append_writer}")

    try:
        # Test 1: Write PyArrow Table
        print("\n--- Testing PyArrow Table write ---")
        pa_table = pa.Table.from_arrays(
            [
                pa.array([1, 2, 3], type=pa.int32()),
                pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
                pa.array([95.2, 87.2, 92.1], type=pa.float32()),
                pa.array([25, 30, 35], type=pa.int32()),
                pa.array([date(1999, 5, 15), date(1994, 3, 20), date(1989, 11, 8)], type=pa.date32()),
                pa.array([dt_time(9, 0, 0), dt_time(9, 30, 0), dt_time(10, 0, 0)], type=pa.time32("ms")),
                pa.array([datetime(2024, 1, 15, 10, 30), datetime(2024, 1, 15, 11, 0), datetime(2024, 1, 15, 11, 30)], type=pa.timestamp("us")),
                pa.array([Decimal("75000.00"), Decimal("82000.50"), Decimal("95000.75")], type=pa.decimal128(10, 2)),
            ],
            schema=schema,
        )

        append_writer.write_arrow(pa_table)
        print("Successfully wrote PyArrow Table")

        # Test 2: Write PyArrow RecordBatch
        print("\n--- Testing PyArrow RecordBatch write ---")
        pa_record_batch = pa.RecordBatch.from_arrays(
            [
                pa.array([4, 5], type=pa.int32()),
                pa.array(["David", "Eve"], type=pa.string()),
                pa.array([88.5, 91.0], type=pa.float32()),
                pa.array([28, 32], type=pa.int32()),
                pa.array([date(1996, 7, 22), date(1992, 12, 1)], type=pa.date32()),
                pa.array([dt_time(14, 15, 0), dt_time(8, 45, 0)], type=pa.time32("ms")),
                pa.array([datetime(2024, 1, 16, 9, 0), datetime(2024, 1, 16, 9, 30)], type=pa.timestamp("us")),
                pa.array([Decimal("68000.00"), Decimal("72500.25")], type=pa.decimal128(10, 2)),
            ],
            schema=schema,
        )

        append_writer.write_arrow_batch(pa_record_batch)
        print("Successfully wrote PyArrow RecordBatch")

        # Test 3: Append single rows with Date, Time, Timestamp, Decimal
        print("\n--- Testing single row append with temporal/decimal types ---")
        # Dict input with all types including Date, Time, Timestamp, Decimal
        await append_writer.append({
            "id": 8,
            "name": "Helen",
            "score": 93.5,
            "age": 26,
            "birth_date": date(1998, 4, 10),
            "check_in_time": dt_time(11, 30, 45),
            "created_at": datetime(2024, 1, 17, 14, 0, 0),
            "salary": Decimal("88000.00"),
        })
        print("Successfully appended row (dict with Date, Time, Timestamp, Decimal)")

        # List input with all types
        await append_writer.append([
            9, "Ivan", 90.0, 31,
            date(1993, 8, 25),
            dt_time(16, 45, 0),
            datetime(2024, 1, 17, 15, 30, 0),
            Decimal("91500.50"),
        ])
        print("Successfully appended row (list with Date, Time, Timestamp, Decimal)")

        # Test 4: Write Pandas DataFrame
        print("\n--- Testing Pandas DataFrame write ---")
        df = pd.DataFrame(
            {
                "id": [10, 11],
                "name": ["Frank", "Grace"],
                "score": [89.3, 94.7],
                "age": [29, 27],
                "birth_date": [date(1995, 2, 14), date(1997, 9, 30)],
                "check_in_time": [dt_time(10, 0, 0), dt_time(10, 30, 0)],
                "created_at": [datetime(2024, 1, 18, 8, 0), datetime(2024, 1, 18, 8, 30)],
                "salary": [Decimal("79000.00"), Decimal("85500.75")],
            }
        )

        append_writer.write_pandas(df)
        print("Successfully wrote Pandas DataFrame")

        # Flush all pending data
        print("\n--- Flushing data ---")
        append_writer.flush()
        print("Successfully flushed data")

    except Exception as e:
        print(f"Error during writing: {e}")

    # Now scan the table to verify data was written
    print("\n--- Scanning table ---")
    try:
        log_scanner = await table.new_log_scanner()
        print(f"Created log scanner: {log_scanner}")

        # Subscribe to scan from earliest to latest
        # start_timestamp=None (earliest), end_timestamp=None (latest)
        log_scanner.subscribe(None, None)

        print("Scanning results using to_arrow():")

        # Try to get as PyArrow Table
        try:
            pa_table_result = log_scanner.to_arrow()
            print(f"\nAs PyArrow Table: {pa_table_result}")
        except Exception as e:
            print(f"Could not convert to PyArrow: {e}")

        # Let's subscribe from the beginning again.
        # Reset subscription
        log_scanner.subscribe(None, None)

        # Try to get as Pandas DataFrame
        try:
            df_result = log_scanner.to_pandas()
            print(f"\nAs Pandas DataFrame:\n{df_result}")
        except Exception as e:
            print(f"Could not convert to Pandas: {e}")

        # TODO: support to_arrow_batch_reader()
        # which is reserved for streaming use cases

        # TODO: support to_duckdb()

    except Exception as e:
        print(f"Error during scanning: {e}")

    # Demo: Column projection
    print("\n--- Testing Column Projection ---")
    try:
        # Project specific columns by index
        print("\n1. Projection by index [0, 1] (id, name):")
        scanner_index = await table.new_log_scanner(project=[0, 1])
        scanner_index.subscribe(None, None)
        df_projected = scanner_index.to_pandas()
        print(df_projected.head())
        print(f"   Projected {df_projected.shape[1]} columns: {list(df_projected.columns)}")

        # Project specific columns by name (Pythonic!)
        print("\n2. Projection by name ['name', 'score'] (Pythonic):")
        scanner_names = await table.new_log_scanner(columns=["name", "score"])
        scanner_names.subscribe(None, None)
        df_named = scanner_names.to_pandas()
        print(df_named.head())
        print(f"   Projected {df_named.shape[1]} columns: {list(df_named.columns)}")

    except Exception as e:
        print(f"Error during projection: {e}")

    # Close connection
    conn.close()
    print("\nConnection closed")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
