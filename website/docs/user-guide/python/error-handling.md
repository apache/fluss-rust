---
sidebar_position: 4
---
# Error Handling

The client raises `fluss.FlussError` for Fluss-specific errors:

```python
try:
    await admin.create_table(table_path, table_descriptor)
except fluss.FlussError as e:
    print(f"Fluss error: {e.message}")
```

Common error scenarios:
- **Connection refused**: Fluss cluster is not running or wrong address in `bootstrap.servers`
- **Table not found**: table doesn't exist or wrong database/table name
- **Partition not found**: writing to a partitioned table before creating partitions
- **Schema mismatch**: row data doesn't match the table schema
