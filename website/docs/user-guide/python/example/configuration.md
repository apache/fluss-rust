---
sidebar_position: 2
---
# Configuration

## Connection Setup

```python
import fluss

config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
conn = await fluss.FlussConnection.create(config)
```

The connection also supports context managers:

```python
with await fluss.FlussConnection.create(config) as conn:
    ...
```

## Connection Configurations

| Key                                   | Description                                                                           | Default            |
|---------------------------------------|---------------------------------------------------------------------------------------|--------------------|
| `bootstrap.servers`                   | Coordinator server address                                                            | `127.0.0.1:9123`   |
| `writer.request-max-size`             | Maximum request size in bytes                                                         | `10485760` (10 MB) |
| `writer.acks`                         | Acknowledgment setting (`all` waits for all replicas)                                 | `all`              |
| `writer.retries`                      | Number of retries on failure                                                          | `2147483647`       |
| `writer.batch-size`                   | Batch size for writes in bytes                                                        | `2097152` (2 MB)   |
| `writer.batch-timeout-ms`             | The maximum time to wait for a writer batch to fill up before sending.                | `100`              |
| `writer.bucket.no-key-assigner`       | Bucket assignment strategy for tables without bucket keys: `sticky` or `round_robin`  | `sticky`           |
| `scanner.remote-log.prefetch-num`     | Number of remote log segments to prefetch                                             | `4`                |
| `remote-file.download-thread-num`     | Number of threads for remote log downloads                                            | `3`                |
| `scanner.remote-log.read-concurrency` | Streaming read concurrency within a remote log file                                   | `4`                |
| `scanner.log.max-poll-records`        | Max records returned in a single poll()                                               | `500`              |
| `connect-timeout`                     | TCP connect timeout in milliseconds                                                   | `120000`           |
| `request-timeout`                     | Per-request RPC timeout in milliseconds (also accepts `request-timeout-ms`)           | `30000`            |
| `security.protocol`                   | `PLAINTEXT` (default) or `sasl` for SASL auth                                        | `PLAINTEXT`        |
| `security.sasl.mechanism`             | SASL mechanism (only `PLAIN` is supported)                                            | `PLAIN`            |
| `security.sasl.username`              | SASL username (required when protocol is `sasl`)                                      | (empty)            |
| `security.sasl.password`              | SASL password (required when protocol is `sasl`)                                      | (empty)            |

## SASL Authentication

To connect to a Fluss cluster with SASL/PLAIN authentication enabled:

```python
config = fluss.Config({
    "bootstrap.servers": "127.0.0.1:9123",
    "security.protocol": "sasl",
    "security.sasl.mechanism": "PLAIN",
    "security.sasl.username": "admin",
    "security.sasl.password": "admin-secret",
})
conn = await fluss.FlussConnection.create(config)
```

## Connection Lifecycle

Remember to close the connection when done:

```python
conn.close()
```
