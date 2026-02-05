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

"""Type stubs for Fluss Python bindings."""

from enum import IntEnum
from types import TracebackType
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa

class ChangeType(IntEnum):
    """Represents the type of change for a record in a log."""

    AppendOnly = 0
    """Append-only operation"""
    Insert = 1
    """Insert operation"""
    UpdateBefore = 2
    """Update operation containing the previous content of the updated row"""
    UpdateAfter = 3
    """Update operation containing the new content of the updated row"""
    Delete = 4
    """Delete operation"""

    def short_string(self) -> str:
        """Returns a short string representation (+A, +I, -U, +U, -D)."""
        ...

class ScanRecord:
    """Represents a single scan record with metadata."""

    @property
    def bucket(self) -> TableBucket:
        """The bucket this record belongs to."""
        ...
    @property
    def offset(self) -> int:
        """The position of this record in the log."""
        ...
    @property
    def timestamp(self) -> int:
        """The timestamp of this record."""
        ...
    @property
    def change_type(self) -> ChangeType:
        """The type of change (insert, update, delete, etc.)."""
        ...
    @property
    def row(self) -> Dict[str, object]:
        """The row data as a dictionary mapping column names to values."""
        ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class RecordBatch:
    """Represents a batch of records with metadata."""

    @property
    def batch(self) -> pa.RecordBatch:
        """The Arrow RecordBatch containing the data."""
        ...
    @property
    def bucket(self) -> TableBucket:
        """The bucket this batch belongs to."""
        ...
    @property
    def base_offset(self) -> int:
        """The offset of the first record in this batch."""
        ...
    @property
    def last_offset(self) -> int:
        """The offset of the last record in this batch."""
        ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class Config:
    def __init__(self, properties: Optional[Dict[str, str]] = None) -> None: ...
    @property
    def bootstrap_server(self) -> Optional[str]: ...
    @bootstrap_server.setter
    def bootstrap_server(self, server: str) -> None: ...
    @property
    def request_max_size(self) -> int: ...
    @request_max_size.setter
    def request_max_size(self, size: int) -> None: ...
    @property
    def writer_batch_size(self) -> int: ...
    @writer_batch_size.setter
    def writer_batch_size(self, size: int) -> None: ...

class FlussConnection:
    @staticmethod
    async def connect(config: Config) -> FlussConnection: ...
    async def get_admin(self) -> FlussAdmin: ...
    async def get_table(self, table_path: TablePath) -> FlussTable: ...
    def close(self) -> None: ...
    def __enter__(self) -> FlussConnection: ...
    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool: ...
    def __repr__(self) -> str: ...

class FlussAdmin:
    async def create_table(
        self,
        table_path: TablePath,
        table_descriptor: TableDescriptor,
        ignore_if_exists: Optional[bool] = False,
    ) -> None: ...
    async def get_table(self, table_path: TablePath) -> TableInfo: ...
    async def get_latest_lake_snapshot(self, table_path: TablePath) -> LakeSnapshot: ...
    def __repr__(self) -> str: ...

class FlussTable:
    async def new_append_writer(self) -> AppendWriter: ...
    async def new_log_scanner(
        self,
        project: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
    ) -> LogScanner:
        """Create a record-based log scanner.

        Use this scanner with `poll()` to get individual records with metadata
        (offset, timestamp, change_type).

        Args:
            project: Optional list of column indices (0-based) to include.
            columns: Optional list of column names to include.

        Returns:
            LogScanner for record-by-record scanning with `poll()`
        """
        ...
    async def new_batch_scanner(
        self,
        project: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
    ) -> LogScanner:
        """Create a batch-based log scanner.

        Use this scanner with `poll_arrow()` to get Arrow Tables, or with
        `poll_batches()` to get individual batches with metadata.

        Args:
            project: Optional list of column indices (0-based) to include.
            columns: Optional list of column names to include.

        Returns:
            LogScanner for batch-based scanning with `poll_arrow()` or `poll_batches()`
        """
        ...
    def new_upsert(
        self,
        columns: Optional[List[str]] = None,
        column_indices: Optional[List[int]] = None,
    ) -> UpsertWriter: ...
    def new_lookup(self) -> Lookuper: ...
    def get_table_info(self) -> TableInfo: ...
    def get_table_path(self) -> TablePath: ...
    def has_primary_key(self) -> bool: ...
    def __repr__(self) -> str: ...

class AppendWriter:
    async def append(self, row: dict | list | tuple) -> None:
        """Append a single row to the table.

        Args:
            row: Dictionary mapping field names to values, or
                 list/tuple of values in schema order

        Supported Types:
            Currently supports primitive types only:
            - Boolean, TinyInt, SmallInt, Int, BigInt (integers)
            - Float, Double (floating point)
            - String, Char (text)
            - Bytes, Binary (binary data)
            - Null values

            Temporal types (Date, Timestamp, Decimal) are not yet supported.

        Example:
            await writer.append({'id': 1, 'name': 'Alice', 'score': 95.5})
            await writer.append([1, 'Alice', 95.5])

        Note:
            For high-throughput bulk loading, prefer write_arrow_batch().
            Use flush() to ensure all queued records are sent and acknowledged.
        """
        ...
    def write_arrow(self, table: pa.Table) -> None: ...
    def write_arrow_batch(self, batch: pa.RecordBatch) -> None: ...
    def write_pandas(self, df: pd.DataFrame) -> None: ...
    def flush(self) -> None: ...
    def __repr__(self) -> str: ...

class UpsertWriter:
    """Writer for upserting and deleting data in a Fluss primary key table."""

    async def upsert(self, row: dict | list | tuple) -> None:
        """Upsert a row into the table.

        If a row with the same primary key exists, it will be updated.
        Otherwise, a new row will be inserted.

        Args:
            row: Dictionary mapping field names to values, or
                 list/tuple of values in schema order
        """
        ...
    async def delete(self, pk: dict | list | tuple) -> None:
        """Delete a row from the table by primary key.

        Args:
            pk: Dictionary with PK column names as keys, or
                list/tuple of PK values in PK column order
        """
        ...
    async def flush(self) -> None:
        """Flush all pending upsert/delete operations to the server."""
        ...
    def __repr__(self) -> str: ...

class Lookuper:
    """Lookuper for performing primary key lookups on a Fluss table."""

    async def lookup(self, pk: dict | list | tuple) -> Optional[Dict[str, object]]:
        """Lookup a row by its primary key.

        Args:
            pk: Dictionary with PK column names as keys, or
                list/tuple of PK values in PK column order

        Returns:
            A dict containing the row data if found, None otherwise.
        """
        ...
    def __repr__(self) -> str: ...

class LogScanner:
    """Scanner for reading log data from a Fluss table.

    This scanner supports two modes:
    - Record-based scanning via `poll()` - returns individual records with metadata
    - Batch-based scanning via `poll_arrow()` / `poll_batches()` - returns Arrow batches

    Use `new_log_scanner()` for record-based scanning (poll method).
    Use `new_batch_scanner()` for batch-based scanning (poll_arrow/poll_batches methods).
    """

    def subscribe(
        self, start_timestamp: Optional[int], end_timestamp: Optional[int]
    ) -> None:
        """Subscribe to log data with timestamp range.

        Args:
            start_timestamp: Not yet supported, must be None.
            end_timestamp: Not yet supported, must be None.
        """
        ...
    def poll(self, timeout_ms: int) -> List[ScanRecord]:
        """Poll for individual records with metadata.

        Requires a record-based scanner (created with new_log_scanner).

        Args:
            timeout_ms: Timeout in milliseconds to wait for records.

        Returns:
            List of ScanRecord objects, each containing bucket, offset, timestamp,
            change_type, and row data as a dictionary.

        Note:
            Returns an empty list if no records are available or timeout expires.
        """
        ...
    def poll_batches(self, timeout_ms: int) -> List[RecordBatch]:
        """Poll for batches with metadata.

        Requires a batch-based scanner (created with new_batch_scanner).

        Args:
            timeout_ms: Timeout in milliseconds to wait for batches.

        Returns:
            List of RecordBatch objects, each containing the Arrow batch along with
            bucket, base_offset, and last_offset metadata.

        Note:
            Returns an empty list if no batches are available or timeout expires.
        """
        ...
    def poll_arrow(self, timeout_ms: int) -> pa.Table:
        """Poll for records as an Arrow Table.

        Requires a batch-based scanner (created with new_batch_scanner).

        Args:
            timeout_ms: Timeout in milliseconds to wait for records.

        Returns:
            PyArrow Table containing the polled records (batches merged).

        Note:
            Returns an empty table (with correct schema) if no records are available
            or timeout expires.
        """
        ...
    def to_pandas(self) -> pd.DataFrame:
        """Convert all data to Pandas DataFrame.

        Requires a batch-based scanner (created with new_batch_scanner).
        """
        ...
    def to_arrow(self) -> pa.Table:
        """Convert all data to Arrow Table.

        Requires a batch-based scanner (created with new_batch_scanner).
        """
        ...
    def __repr__(self) -> str: ...

class Schema:
    def __init__(
        self, schema: pa.Schema, primary_keys: Optional[List[str]] = None
    ) -> None: ...
    def get_column_names(self) -> List[str]: ...
    def get_column_types(self) -> List[str]: ...
    def get_columns(self) -> List[Tuple[str, str]]: ...
    def __str__(self) -> str: ...

class TableDescriptor:
    def __init__(
        self,
        schema: Schema,
        *,
        partition_keys: Optional[List[str]] = None,
        bucket_count: Optional[int] = None,
        bucket_keys: Optional[List[str]] = None,
        comment: Optional[str] = None,
        log_format: Optional[str] = None,
        kv_format: Optional[str] = None,
        **properties: str,
    ) -> None: ...
    def get_schema(self) -> Schema: ...

class TablePath:
    def __init__(self, database: str, table: str) -> None: ...
    @property
    def database_name(self) -> str: ...
    @property
    def table_name(self) -> str: ...
    def table_path_str(self) -> str: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...

class TableInfo:
    @property
    def table_id(self) -> int: ...
    @property
    def schema_id(self) -> int: ...
    @property
    def created_time(self) -> int: ...
    @property
    def modified_time(self) -> int: ...
    @property
    def table_path(self) -> TablePath: ...
    @property
    def num_buckets(self) -> int: ...
    @property
    def comment(self) -> Optional[str]: ...
    def get_primary_keys(self) -> List[str]: ...
    def get_bucket_keys(self) -> List[str]: ...
    def get_partition_keys(self) -> List[str]: ...
    def has_primary_key(self) -> bool: ...
    def is_partitioned(self) -> bool: ...
    def get_properties(self) -> Dict[str, str]: ...
    def get_custom_properties(self) -> Dict[str, str]: ...
    def get_schema(self) -> Schema: ...
    def get_column_names(self) -> List[str]: ...
    def get_column_count(self) -> int: ...

class FlussError(Exception):
    message: str
    def __init__(self, message: str) -> None: ...
    def __str__(self) -> str: ...

class LakeSnapshot:
    def __init__(self, snapshot_id: int) -> None: ...
    @property
    def snapshot_id(self) -> int: ...
    @property
    def table_buckets_offset(self) -> Dict[TableBucket, int]: ...
    def get_bucket_offset(self, bucket: TableBucket) -> Optional[int]: ...
    def get_table_buckets(self) -> List[TableBucket]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class TableBucket:
    def __init__(self, table_id: int, bucket: int) -> None: ...
    @staticmethod
    def with_partition(
        table_id: int, partition_id: int, bucket: int
    ) -> TableBucket: ...
    @property
    def table_id(self) -> int: ...
    @property
    def bucket_id(self) -> int: ...
    @property
    def partition_id(self) -> Optional[int]: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class TableDistribution:
    def bucket_keys(self) -> List[str]: ...
    def bucket_count(self) -> Optional[int]: ...

__version__: str
