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
    Err(Error::RpcError { message, .. }) => {
        eprintln!("RPC failure: {}", message);
    }
    Err(Error::UnsupportedOperation { message }) => {
        eprintln!("Unsupported: {}", message);
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

| Variant                        | Description                                                  |
|--------------------------------|--------------------------------------------------------------|
| `UnexpectedError`              | General unexpected errors with a message and optional source |
| `IoUnexpectedError`            | I/O errors (network, file system)                            |
| `RemoteStorageUnexpectedError` | Remote storage errors (OpenDAL backend failures)             |
| `RpcError`                     | RPC communication failures (connection refused, timeout)     |
| `RowConvertError`              | Row conversion failures (type mismatch, invalid data)        |
| `ArrowError`                   | Arrow data handling errors (schema mismatch, encoding)       |
| `IllegalArgument`              | Invalid arguments passed to an API method                    |
| `UnsupportedOperation`         | Operation not supported on the table type                    |
| `FlussAPIError`                | Server-side API errors returned by the Fluss cluster         |

Server side errors are represented as `FlussAPIError` with a specific error code. Use the `api_error()` helper to match them ergonomically:

```rust
use fluss::error::FlussError;

match result {
    Err(ref e) if e.api_error() == Some(FlussError::InvalidTableException) => {
        eprintln!("Invalid table: {}", e);
    }
    Err(ref e) if e.api_error() == Some(FlussError::PartitionNotExists) => {
        eprintln!("Partition does not exist: {}", e);
    }
    Err(ref e) if e.api_error() == Some(FlussError::LeaderNotAvailableException) => {
        eprintln!("Leader not available: {}", e);
    }
    _ => {}
}
```

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
use fluss::error::{Error, FlussError};

// Admin operations return FlussError::TableNotExist (code 7)
let result = admin.drop_table(&table_path, false).await;
match result {
    Err(ref e) if e.api_error() == Some(FlussError::TableNotExist) => {
        eprintln!("Table not found: {}", e);
    }
    _ => {}
}

// conn.get_table() wraps the error differently, match on FlussAPIError directly
let result = conn.get_table(&table_path).await;
match result {
    Err(Error::FlussAPIError { ref api_error }) => {
        eprintln!("Server error (code {}): {}", api_error.code, api_error.message);
    }
    _ => {}
}
```

### Partition Not Found

The partition does not exist on a partitioned table.

```rust
use fluss::error::FlussError;

let result = admin.drop_partition(&table_path, &spec, false).await;
match result {
    Err(ref e) if e.api_error() == Some(FlussError::PartitionNotExists) => {
        eprintln!("Partition does not exist: {}", e);
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
