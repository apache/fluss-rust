---
sidebar_position: 4
---
# Error Handling

All C++ client operations return a `fluss::Result` struct instead of throwing exceptions. This gives you explicit control over error handling.

## The `Result` Struct

```cpp
#include "fluss.hpp"

// All operations return fluss::Result
fluss::Result result = admin.CreateTable(path, descriptor);
if (!result.Ok()) {
    std::cerr << "Error code: " << result.error_code << std::endl;
    std::cerr << "Error message: " << result.error_message << std::endl;
}
```

| Field / Method | Type | Description |
|---|---|---|
| `error_code` | `int32_t` | 0 for success, non-zero for errors |
| `error_message` | `std::string` | Human-readable error description |
| `Ok()` | `bool` | Returns `true` if the operation succeeded |

## Common Pattern: Helper Function

A common pattern is to define a `check` helper that exits on failure:

```cpp
static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: " << r.error_message << std::endl;
        std::exit(1);
    }
}

// Usage
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
check("create", fluss::Connection::Create(config, conn));
check("create_table", admin.CreateTable(table_path, descriptor, true));
check("flush", writer.Flush());
```

## Connection State Checking

Use `Available()` to verify that a connection or object is valid before using it:

```cpp
fluss::Connection conn;
if (!conn.Available()) {
    // Connection not initialized or already moved
}

fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
fluss::Result result = fluss::Connection::Create(config, conn);
if (result.Ok() && conn.Available()) {
    // Connection is ready to use
}
```

## Common Error Scenarios

### Connection Refused

The cluster is not running or the address is incorrect:

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
if (!result.Ok()) {
    // "Connection refused" or timeout error
    std::cerr << "Cannot connect to cluster: " << result.error_message << std::endl;
}
```

### Table Not Found

Attempting to access a table that does not exist:

```cpp
fluss::Table table;
fluss::Result result = conn.GetTable(fluss::TablePath("fluss", "nonexistent"), table);
if (!result.Ok()) {
    // Table not found error
    std::cerr << "Table error: " << result.error_message << std::endl;
}
```

### Partition Not Found

Writing to a partitioned primary key table before creating partitions:

```cpp
// This will fail if partitions are not created first
auto row = table.NewRow();
row.Set("user_id", 1);
row.Set("region", "US");
row.Set("score", static_cast<int64_t>(100));
fluss::WriteResult wr;
fluss::Result result = writer.Upsert(row, wr);
if (!result.Ok()) {
    // Partition not found — create partitions before writing
    std::cerr << "Write error: " << result.error_message << std::endl;
}
```

### Schema Mismatch

Using incorrect types or column indices when writing:

```cpp
fluss::GenericRow row;
// Setting wrong type for a column will result in an error
// when the row is sent to the server
row.SetString(0, "not_an_integer");  // Column 0 expects Int
fluss::Result result = writer.Append(row);
if (!result.Ok()) {
    std::cerr << "Schema mismatch: " << result.error_message << std::endl;
}
```

## Best Practices

1. **Always check `Result`** -- Never ignore the return value of operations that return `Result`.
2. **Use a helper function** -- Define a `check()` helper to reduce boilerplate for fatal errors.
3. **Handle errors gracefully** -- For production code, log errors and retry or fail gracefully instead of calling `std::exit()`.
4. **Verify connection state** -- Use `Available()` to check connection validity before operations.
5. **Create partitions before writing** -- For partitioned primary key tables, always create partitions before attempting upserts.
