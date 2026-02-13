---
sidebar_position: 2
---
# Configuration

## Connection Setup

```cpp
#include "fluss.hpp"

fluss::Configuration config;
config.bootstrap_server = "127.0.0.1:9123";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);

if (!result.Ok()) {
    std::cerr << "Connection failed: " << result.error_message << std::endl;
}
```

## Configuration Options

All fields have sensible defaults. Only `bootstrap_server` typically needs to be set.

```cpp
fluss::Configuration config;
config.bootstrap_server = "127.0.0.1:9123";    // Coordinator address
config.request_max_size = 10 * 1024 * 1024;     // Max request size (10 MB)
config.writer_acks = "all";                      // Wait for all replicas
config.writer_retries = std::numeric_limits<int32_t>::max();  // Retry on failure
config.writer_batch_size = 2 * 1024 * 1024;     // Batch size (2 MB)
config.scanner_remote_log_prefetch_num = 4;      // Remote log prefetch count
config.scanner_remote_log_download_threads = 3;  // Download threads
```

## Error Handling

All C++ operations return a `fluss::Result`. Check with `Ok()` before continuing:

```cpp
static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code
                  << " msg=" << r.error_message << std::endl;
        std::exit(1);
    }
}
```
