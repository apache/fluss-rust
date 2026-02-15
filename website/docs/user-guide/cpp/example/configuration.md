---
sidebar_position: 2
---
# Configuration

## Connection Setup

```cpp
#include "fluss.hpp"

fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);

if (!result.Ok()) {
    std::cerr << "Connection failed: " << result.error_message << std::endl;
}
```

## Connection Configurations

All fields have sensible defaults. Only `bootstrap_servers` typically needs to be set.

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";    // Coordinator address
config.writer_request_max_size = 10 * 1024 * 1024;     // Max request size (10 MB)
config.writer_acks = "all";                      // Wait for all replicas
config.writer_retries = std::numeric_limits<int32_t>::max();  // Retry on failure
config.writer_batch_size = 2 * 1024 * 1024;     // Batch size (2 MB)
config.scanner_remote_log_prefetch_num = 4;      // Remote log prefetch count
config.remote_file_download_thread_num = 3;  // Download threads
```
