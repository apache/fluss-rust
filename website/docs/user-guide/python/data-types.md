---
sidebar_position: 3
---
# Data Types

The Python client uses PyArrow types for schema definitions:

| PyArrow Type                                    | Fluss Type                        | Python Type         |
|-------------------------------------------------|-----------------------------------|---------------------|
| `pa.bool_()`                                    | Boolean                           | `bool`              |
| `pa.int8()` / `int16()` / `int32()` / `int64()` | TinyInt / SmallInt / Int / BigInt | `int`               |
| `pa.float32()` / `float64()`                    | Float / Double                    | `float`             |
| `pa.string()`                                   | String                            | `str`               |
| `pa.binary()`                                   | Bytes                             | `bytes`             |
| `pa.date32()`                                   | Date                              | `datetime.date`     |
| `pa.time32("ms")`                               | Time                              | `datetime.time`     |
| `pa.timestamp("us")`                            | Timestamp (NTZ)                   | `datetime.datetime` |
| `pa.timestamp("us", tz="UTC")`                  | TimestampLTZ                      | `datetime.datetime` |
| `pa.decimal128(precision, scale)`               | Decimal                           | `decimal.Decimal`   |

All Python native types (`date`, `time`, `datetime`, `Decimal`) work when appending rows via dicts.

## Writing Data

Rows can be dicts, lists, or tuples:

```python
from datetime import date, time, datetime
from decimal import Decimal

row = {
    "user_id": 1,
    "name": "Alice",
    "active": True,
    "score": 95.5,
    "balance": Decimal("1234.56"),
    "birth_date": date(1990, 3, 15),
    "login_time": time(9, 30, 0),
    "created_at": datetime(2024, 1, 1, 0, 0, 0),
    "nickname": None,  # null value
}
handle = writer.append(row)
```

Lists and tuples must have values in column order:

```python
row = [1, "Alice", True, 95.5, Decimal("1234.56"), date(1990, 3, 15), time(9, 30, 0), datetime(2024, 1, 1), None]
handle = writer.append(row)
```

## Reading Data

```python
records = scanner.poll(timeout_ms=1000)
for record in records:
    row = record.row  # dict[str, Any]
    print(row["user_id"])     # int
    print(row["name"])        # str
    print(row["balance"])     # decimal.Decimal
    print(row["birth_date"])  # datetime.date
    print(row["created_at"])  # datetime.datetime

    if row["nickname"] is None:
        print("nickname is null")
```
