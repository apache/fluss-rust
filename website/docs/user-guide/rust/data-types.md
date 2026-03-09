---
sidebar_position: 3
---
# Data Types

| Fluss Type      | Rust Type      | Getter                               | Setter                         |
|-----------------|----------------|--------------------------------------|--------------------------------|
| `BOOLEAN`       | `bool`         | `get_boolean()`                      | `set_field(idx, bool)`         |
| `TINYINT`       | `i8`           | `get_byte()`                         | `set_field(idx, i8)`           |
| `SMALLINT`      | `i16`          | `get_short()`                        | `set_field(idx, i16)`          |
| `INT`           | `i32`          | `get_int()`                          | `set_field(idx, i32)`          |
| `BIGINT`        | `i64`          | `get_long()`                         | `set_field(idx, i64)`          |
| `FLOAT`         | `f32`          | `get_float()`                        | `set_field(idx, f32)`          |
| `DOUBLE`        | `f64`          | `get_double()`                       | `set_field(idx, f64)`          |
| `CHAR`          | `&str`         | `get_char(idx, length)`              | `set_field(idx, &str)`         |
| `STRING`        | `&str`         | `get_string()`                       | `set_field(idx, &str)`         |
| `DECIMAL`       | `Decimal`      | `get_decimal(idx, precision, scale)` | `set_field(idx, Decimal)`      |
| `DATE`          | `Date`         | `get_date()`                         | `set_field(idx, Date)`         |
| `TIME`          | `Time`         | `get_time()`                         | `set_field(idx, Time)`         |
| `TIMESTAMP`     | `TimestampNtz` | `get_timestamp_ntz(idx, precision)`  | `set_field(idx, TimestampNtz)` |
| `TIMESTAMP_LTZ` | `TimestampLtz` | `get_timestamp_ltz(idx, precision)`  | `set_field(idx, TimestampLtz)` |
| `BYTES`         | `&[u8]`        | `get_bytes()`                        | `set_field(idx, &[u8])`        |
| `BINARY(n)`     | `&[u8]`        | `get_binary(idx, length)`            | `set_field(idx, &[u8])`        |
| `ARRAY<T>`      | `FlussArray`   | `get_array()`                        | `set_field(idx, FlussArray)`   |

## Constructing Special Types

Primitive types (`bool`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, `&str`, `&[u8]`) can be passed directly to `set_field`. The following types require explicit construction:

```rust
use fluss::row::{Date, Time, TimestampNtz, TimestampLtz, Decimal};

// Date: days since Unix epoch
let date = Date::new(19738);

// Time: milliseconds since midnight
let time = Time::new(43200000);

// Timestamp without timezone: milliseconds since epoch
// DataTypes::timestamp() defaults to precision 6 (microseconds).
// Use DataTypes::timestamp_with_precision(p) for a different precision (0–9).
let ts = TimestampNtz::new(1704067200000);

// Timestamp with local timezone: milliseconds since epoch
// DataTypes::timestamp_ltz() also defaults to precision 6.
let ts_ltz = TimestampLtz::new(1704067200000);

// Decimal: from an unscaled long value with precision and scale
let decimal = Decimal::from_unscaled_long(12345, 10, 2)?; // represents 123.45
```

## Creating Rows from Data

`GenericRow::from_data` accepts a `Vec<Datum>`. Because multiple crates implement `From<&str>`, Rust cannot infer the target type from `.into()` alone. Annotate the vector type explicitly:

```rust
use fluss::row::{Datum, GenericRow};

let data: Vec<Datum> = vec![1i32.into(), "hello".into(), Datum::Null];
let row = GenericRow::from_data(data);
```

## Arrays

Use `DataTypes::array(element_type)` in schema definitions. At runtime, read arrays with `row.get_array(idx)?`.

To construct array values for writes, build a `FlussArray` and wrap it with `Datum::Array`:

```rust
use fluss::metadata::DataTypes;
use fluss::row::binary_array::FlussArrayWriter;
use fluss::row::{Datum, GenericRow};

let mut writer = FlussArrayWriter::new(3, &DataTypes::int());
writer.write_int(0, 10);
writer.write_int(1, 20);
writer.set_null_at(2);
let arr = writer.complete()?;

let mut row = GenericRow::new(1);
row.set_field(0, Datum::Array(arr));
```

`ARRAY` is supported for row values and nested row fields. For key encoding, Rust follows Java parity: `ARRAY` can be encoded by the compacted key encoder, while table-level key constraints are validated by the server (which may reject unsupported key types).

## Reading Row Data

```rust
use fluss::row::InternalRow;

for record in scan_records {
    let row = record.row();

    if row.is_null_at(0)? {
        // field is null
    }
    let id: i32 = row.get_int(0)?;
    let name: &str = row.get_string(1)?;
    let score: f32 = row.get_float(2)?;
    let date: Date = row.get_date(3)?;
    let ts: TimestampNtz = row.get_timestamp_ntz(4, 6)?;
    let decimal: Decimal = row.get_decimal(5, 10, 2)?;
}
```
