---
sidebar_position: 1
---
# Example

Minimal working example: connect to Fluss, create a table, write data, and read it back.

```python
import asyncio
import pyarrow as pa
import fluss

async def main():
    # Connect
    config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
    conn = await fluss.FlussConnection.connect(config)
    admin = await conn.get_admin()

    # Create a log table
    schema = fluss.Schema(pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
    ]))
    table_path = fluss.TablePath("fluss", "quick_start")
    await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=True)

    # Write
    table = await conn.get_table(table_path)
    writer = await table.new_append_writer()
    writer.append({"id": 1, "name": "Alice", "score": 95.5})
    writer.append({"id": 2, "name": "Bob", "score": 87.0})
    await writer.flush()

    # Read
    num_buckets = (await admin.get_table(table_path)).num_buckets
    scanner = await table.new_scan().create_batch_scanner()
    scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})
    print(scanner.to_pandas())

    # Cleanup
    await admin.drop_table(table_path, ignore_if_not_exists=True)
    conn.close()

asyncio.run(main())
```
