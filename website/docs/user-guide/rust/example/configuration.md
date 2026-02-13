---
sidebar_position: 2
---
# Configuration

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;

let mut config = Config::default();
config.bootstrap_server = "127.0.0.1:9123".to_string();

let conn = FlussConnection::new(config).await?;
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `bootstrap_server` | Coordinator server address | `127.0.0.1:9123` |
| `request_max_size` | Maximum request size in bytes | 10 MB |
| `writer_acks` | Acknowledgment setting (`all` waits for all replicas) | `all` |
| `writer_retries` | Number of retries on failure | `i32::MAX` |
| `writer_batch_size` | Batch size for writes | 2 MB |
