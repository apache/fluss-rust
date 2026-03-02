---
sidebar_position: 4
---
# Error Handling

The client raises `fluss.FlussError` for all Fluss-specific errors. Each error carries a `message` and an `error_code`.

## Basic Usage

```python
import fluss

try:
    await admin.create_table(table_path, table_descriptor)
except fluss.FlussError as e:
    print(f"Error (code {e.error_code}): {e.message}")
```

## Error Codes

Server-side errors carry a specific error code (>0 or -1). Client-side errors (connection failures, type mismatches, etc.) use `ErrorCode.CLIENT_ERROR` (-2). Use `fluss.ErrorCode` to match on specific codes:

```python
import fluss

try:
    await admin.drop_table(table_path)
except fluss.FlussError as e:
    if e.error_code == fluss.ErrorCode.TABLE_NOT_EXIST:
        print("Table does not exist")
    elif e.error_code == fluss.ErrorCode.PARTITION_NOT_EXISTS:
        print("Partition does not exist")
    elif e.error_code == fluss.ErrorCode.CLIENT_ERROR:
        print(f"Client-side error: {e.message}")
    else:
        print(f"Server error (code {e.error_code}): {e.message}")
```

### Common Error Codes

| Constant                                     | Code | Description                         |
|----------------------------------------------|------|-------------------------------------|
| `ErrorCode.CLIENT_ERROR`                     | -2   | Client-side error (not from server) |
| `ErrorCode.UNKNOWN_SERVER_ERROR`             | -1   | Unexpected server error             |
| `ErrorCode.NETWORK_EXCEPTION`                | 1    | Server disconnected before response |
| `ErrorCode.DATABASE_NOT_EXIST`               | 4    | Database does not exist             |
| `ErrorCode.DATABASE_ALREADY_EXIST`           | 6    | Database already exists             |
| `ErrorCode.TABLE_NOT_EXIST`                  | 7    | Table does not exist                |
| `ErrorCode.TABLE_ALREADY_EXIST`              | 8    | Table already exists                |
| `ErrorCode.INVALID_TABLE_EXCEPTION`          | 15   | Invalid table operation             |
| `ErrorCode.REQUEST_TIME_OUT`                 | 25   | Request timed out                   |
| `ErrorCode.PARTITION_NOT_EXISTS`             | 36   | Partition does not exist            |
| `ErrorCode.PARTITION_ALREADY_EXISTS`         | 42   | Partition already exists            |
| `ErrorCode.PARTITION_SPEC_INVALID_EXCEPTION` | 43   | Invalid partition spec              |
| `ErrorCode.LEADER_NOT_AVAILABLE_EXCEPTION`   | 44   | No leader available for partition   |
| `ErrorCode.AUTHENTICATE_EXCEPTION`           | 46   | Authentication failed (bad credentials) |

See `fluss.ErrorCode` for the full list of named constants.

## Common Error Scenarios

### Connection Refused

The Fluss cluster is not running or the address is incorrect.

```python
try:
    config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
    conn = await fluss.FlussConnection.create(config)
except fluss.FlussError as e:
    # error_code == ErrorCode.CLIENT_ERROR for connection failures
    print(f"Cannot connect to cluster: {e.message}")
```

### Table Not Found

The table does not exist or has been dropped.

```python
try:
    await admin.drop_table(table_path)
except fluss.FlussError as e:
    if e.error_code == fluss.ErrorCode.TABLE_NOT_EXIST:
        print("Table not found")
```

### Partition Not Found

Writing to a partitioned table before creating partitions.

```python
try:
    await admin.drop_partition(table_path, {"region": "US"})
except fluss.FlussError as e:
    if e.error_code == fluss.ErrorCode.PARTITION_NOT_EXISTS:
        print("Partition does not exist, create it first")
```

### Authentication Failed

SASL credentials are incorrect or the user does not exist.

```python
try:
    config = fluss.Config({
        "bootstrap.servers": "127.0.0.1:9123",
        "client.security.protocol": "sasl",
        "client.security.sasl.username": "admin",
        "client.security.sasl.password": "wrong-password",
    })
    conn = await fluss.FlussConnection.create(config)
except fluss.FlussError as e:
    if e.error_code == fluss.ErrorCode.AUTHENTICATE_EXCEPTION:
        print(f"Authentication failed: {e.message}")
```

### Schema Mismatch

Row data doesn't match the table schema.

```python
try:
    writer.append({"wrong_column": "value"})
    await writer.flush()
except fluss.FlussError as e:
    # error_code == ErrorCode.CLIENT_ERROR for type/schema mismatches
    print(f"Schema mismatch: {e.message}")
```
