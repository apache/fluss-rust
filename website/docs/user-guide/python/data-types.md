---
sidebar_position: 3
---
# Data Types

The Python client uses PyArrow types for schema definitions:

| PyArrow Type | Fluss Type | Python Type |
|---|---|---|
| `pa.boolean()` | Boolean | `bool` |
| `pa.int8()` / `int16()` / `int32()` / `int64()` | TinyInt / SmallInt / Int / BigInt | `int` |
| `pa.float32()` / `float64()` | Float / Double | `float` |
| `pa.string()` | String | `str` |
| `pa.binary()` | Bytes | `bytes` |
| `pa.date32()` | Date | `datetime.date` |
| `pa.time32("ms")` | Time | `datetime.time` |
| `pa.timestamp("us")` | Timestamp (NTZ) | `datetime.datetime` |
| `pa.timestamp("us", tz="UTC")` | TimestampLTZ | `datetime.datetime` |
| `pa.decimal128(precision, scale)` | Decimal | `decimal.Decimal` |

All Python native types (`date`, `time`, `datetime`, `Decimal`) work when appending rows via dicts.
