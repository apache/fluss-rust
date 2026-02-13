---
sidebar_position: 4
---
# Error Handling

The Fluss Rust client uses a unified `Error` type and a `Result<T>` alias for all fallible operations.

## Basic Usage

```rust
use fluss::error::{Error, Result};

// All operations return Result<T>
let conn = FlussConnection::new(config).await?;
let admin = conn.get_admin().await?;
let table = conn.get_table(&table_path).await?;
```

Use the `?` operator to propagate errors, or `match` on specific variants for fine-grained handling.

## Matching Error Variants

```rust
use fluss::error::Error;

match result {
    Ok(val) => {
        // handle success
    }
    Err(Error::InvalidTableError { message }) => {
        eprintln!("Invalid table: {}", message);
    }
    Err(Error::RpcError { message, source }) => {
        eprintln!("RPC failure: {}", message);
    }
    Err(Error::FlussAPIError { api_error }) => {
        eprintln!("Server error: {}", api_error);
    }
    Err(e) => {
        eprintln!("Unexpected error: {}", e);
    }
}
```

## Error Variants

| Variant | Description |
|---|---|
| `UnexpectedError` | General unexpected errors with a message and optional source |
| `IoUnexpectedError` | I/O errors (network, file system) |
| `RemoteStorageUnexpectedError` | Remote storage errors (OpenDAL backend failures) |
| `InvalidTableError` | Invalid table configuration or table not found |
| `RpcError` | RPC communication failures (connection refused, timeout) |
| `RowConvertError` | Row conversion failures (type mismatch, invalid data) |
| `ArrowError` | Arrow data handling errors (schema mismatch, encoding) |
| `IllegalArgument` | Invalid arguments passed to an API method |
| `InvalidPartition` | Invalid partition configuration |
| `PartitionNotExist` | Partition does not exist |
| `UnsupportedOperation` | Operation not supported on the table type |
| `LeaderNotAvailable` | Leader not available for the requested bucket |
| `FlussAPIError` | Server-side API errors returned by the Fluss cluster |

## Common Error Scenarios

### Connection Refused

The Fluss cluster is not running or the address is incorrect.

```rust
let result = FlussConnection::new(config).await;
match result {
    Err(Error::RpcError { message, .. }) => {
        eprintln!("Cannot connect to cluster: {}", message);
    }
    _ => {}
}
```

### Table Not Found

The table does not exist or has been dropped.

```rust
let result = conn.get_table(&table_path).await;
match result {
    Err(Error::InvalidTableError { message }) => {
        eprintln!("Table not found: {}", message);
    }
    _ => {}
}
```

### Partition Not Found

The partition does not exist on a partitioned table.

```rust
let result = admin.drop_partition(&table_path, &spec, false).await;
match result {
    Err(Error::PartitionNotExist { .. }) => {
        eprintln!("Partition does not exist");
    }
    _ => {}
}
```

### Schema Mismatch

Row data does not match the expected table schema.

```rust
let result = writer.append(&row);
match result {
    Err(Error::RowConvertError { .. }) => {
        eprintln!("Row does not match table schema");
    }
    _ => {}
}
```

## Using `Result<T>` in Application Code

The `fluss::error::Result<T>` type alias makes it easy to use Fluss errors with the `?` operator in your application functions:

```rust
use fluss::error::Result;

async fn my_pipeline() -> Result<()> {
    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin().await?;
    let table = conn.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;
    writer.append(&row)?;
    writer.flush().await?;
    Ok(())
}
```

For applications that use other error types alongside Fluss errors, you can convert with standard `From` / `Into` traits or use crates like `anyhow`:

```rust
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let conn = FlussConnection::new(config).await?;
    // fluss::error::Error implements std::error::Error,
    // so it converts into anyhow::Error automatically
    Ok(())
}
```
