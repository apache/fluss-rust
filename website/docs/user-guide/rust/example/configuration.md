---
sidebar_position: 2
---
# Configuration

## Connection Setup

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;

let mut config = Config::default();
config.bootstrap_servers = "127.0.0.1:9123".to_string();

let conn = FlussConnection::new(config).await?;
```

## Connection Configurations

| Option                          | Description                                                                          | Default          |
|---------------------------------|--------------------------------------------------------------------------------------|------------------|
| `bootstrap_servers`             | Coordinator server address                                                           | `127.0.0.1:9123` |
| `writer_request_max_size`       | Maximum request size in bytes                                                        | 10 MB            |
| `writer_acks`                   | Acknowledgment setting (`all` waits for all replicas)                                | `all`            |
| `writer_retries`                | Number of retries on failure                                                         | `i32::MAX`       |
| `writer_batch_size`             | Batch size for writes                                                                | 2 MB             |
| `writer_batch_timeout_ms`       | The maximum time to wait for a writer batch to fill up before sending.               | `100`            |
| `writer_bucket_no_key_assigner` | Bucket assignment strategy for tables without bucket keys: `sticky` or `round_robin` | `sticky`         |
| `scanner_remote_log_prefetch_num` | Number of remote log segments to prefetch                                           | `4`              |
| `remote_file_download_thread_num` | Number of concurrent remote log file downloads                                      | `3`              |
| `scanner_remote_log_read_concurrency` | Streaming read concurrency within a remote log file                           | `4`              |
| `scanner_log_max_poll_records`  | Maximum records returned in a single `poll()`                                       | `500`            |
