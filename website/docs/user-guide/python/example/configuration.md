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

| Key                                | Description                                                                          | Default            |
|------------------------------------|--------------------------------------------------------------------------------------|--------------------|
| `bootstrap.servers`               | Coordinator server address                                                            | `127.0.0.1:9123`   |
| `writer.request-max-size`         | Maximum request size in bytes                                                         | `10485760` (10 MB) |
| `writer.acks`                     | Acknowledgment setting (`all` waits for all replicas)                                 | `all`              |
| `writer.retries`                  | Number of retries on failure                                                          | `2147483647`       |
| `writer.batch-size`               | Batch size for writes in bytes                                                        | `2097152` (2 MB)   |
| `writer.bucket.no-key-assigner`   | Bucket assignment strategy for tables without bucket keys: `sticky` or `round_robin`  | `sticky`           |
| `scanner.remote-log.prefetch-num` | Number of remote log segments to prefetch                                             | `4`                |
| `remote-file.download-thread-num` | Number of threads for remote log downloads                                            | `3`                |
| `scanner.remote-log.read-concurrency` | Streaming read concurrency within a remote log file                              | `4`                |
| `scanner.log.max-poll-records`    | Max records returned in a single poll()                                               | `500`              |

Remember to close the connection when done:

```python
conn.close()
```
