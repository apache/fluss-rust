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
config.bootstrap_servers = "127.0.0.1:9123";                  // Coordinator address
config.writer_request_max_size = 10 * 1024 * 1024;            // Max request size (10 MB)
config.writer_acks = "all";                                    // Wait for all replicas
config.writer_retries = std::numeric_limits<int32_t>::max();   // Retry on failure
config.writer_batch_size = 2 * 1024 * 1024;                   // Batch size (2 MB)
config.writer_batch_timeout_ms = 100;                          // Max time to wait for a batch to fill
config.writer_bucket_no_key_assigner = "sticky";               // "sticky" or "round_robin"
config.scanner_remote_log_prefetch_num = 4;                    // Remote log prefetch count
config.remote_file_download_thread_num = 3;                    // Download threads
config.scanner_remote_log_read_concurrency = 4;                // In-file remote log read concurrency
config.scanner_log_max_poll_records = 500;                     // Max records per poll
config.connect_timeout_ms = 120000;                            // TCP connect timeout (ms)
```

## SASL Authentication

To connect to a Fluss cluster with SASL/PLAIN authentication enabled:

```cpp
fluss::Configuration config;
config.bootstrap_servers = "127.0.0.1:9123";
config.security_protocol = "sasl";
config.security_sasl_mechanism = "PLAIN";
config.security_sasl_username = "admin";
config.security_sasl_password = "admin-secret";

fluss::Connection conn;
fluss::Result result = fluss::Connection::Create(config, conn);
```
