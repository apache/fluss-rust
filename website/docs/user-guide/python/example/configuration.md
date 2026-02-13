---
sidebar_position: 2
---
# Configuration

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

## Configuration Options

| Key                 | Description                                           | Default            |
|---------------------|-------------------------------------------------------|--------------------|
| `bootstrap.servers` | Coordinator server address                            | `127.0.0.1:9123`   |
| `request.max.size`  | Maximum request size in bytes                         | `10485760` (10 MB) |
| `writer.acks`       | Acknowledgment setting (`all` waits for all replicas) | `all`              |
| `writer.retries`    | Number of retries on failure                          | `2147483647`       |
| `writer.batch.size` | Batch size for writes in bytes                        | `2097152` (2 MB)   |

Remember to close the connection when done:

```python
conn.close()
```
