---
sidebar_position: 3
---
# Data Types

## Schema DataTypes

| DataType | Description |
|---|---|
| `DataType::Boolean()` | Boolean value |
| `DataType::TinyInt()` | 8-bit signed integer |
| `DataType::SmallInt()` | 16-bit signed integer |
| `DataType::Int()` | 32-bit signed integer |
| `DataType::BigInt()` | 64-bit signed integer |
| `DataType::Float()` | 32-bit floating point |
| `DataType::Double()` | 64-bit floating point |
| `DataType::String()` | UTF-8 string |
| `DataType::Bytes()` | Binary data |
| `DataType::Date()` | Date (days since epoch) |
| `DataType::Time()` | Time (milliseconds since midnight) |
| `DataType::Timestamp()` | Timestamp without timezone |
| `DataType::TimestampLtz()` | Timestamp with timezone |
| `DataType::Decimal(p, s)` | Decimal with precision and scale |

## GenericRow Setters

```cpp
fluss::GenericRow row;
row.SetNull(0);
row.SetBool(1, true);
row.SetInt32(2, 42);
row.SetInt64(3, 1234567890L);
row.SetFloat32(4, 3.14f);
row.SetFloat64(5, 2.71828);
row.SetString(6, "hello");
row.SetBytes(7, {0x01, 0x02, 0x03});
```

## Name-Based Setters

When using `table.NewRow()`, you can set fields by column name. The setter automatically routes to the correct type based on the schema:

```cpp
auto row = table.NewRow();
row.Set("user_id", 1);
row.Set("name", "Alice");
row.Set("score", 95.5f);
row.Set("balance", "1234.56");   // decimal as string
row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
row.Set("login_time", fluss::Time::FromHMS(9, 30, 0));
row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
```

## GenericRow Getters

```cpp
std::string name = result_row.GetString(1);
float score = result_row.GetFloat32(3);
std::string balance = result_row.DecimalToString(4);
fluss::Date date = result_row.GetDate(5);
fluss::Time time = result_row.GetTime(6);
fluss::Timestamp ts = result_row.GetTimestamp(7);
```

## DatumType Enum

| DatumType | C++ Type | Getter |
|---|---|---|
| `Null` | — | `IsNull(idx)` |
| `Bool` | `bool` | `GetBool(idx)` |
| `Int32` | `int32_t` | `GetInt32(idx)` |
| `Int64` | `int64_t` | `GetInt64(idx)` |
| `Float32` | `float` | `GetFloat32(idx)` |
| `Float64` | `double` | `GetFloat64(idx)` |
| `String` | `std::string` | `GetString(idx)` |
| `Bytes` | `std::vector<uint8_t>` | `GetBytes(idx)` |
| `Date` | `Date` | `GetDate(idx)` |
| `Time` | `Time` | `GetTime(idx)` |
| `TimestampNtz` | `Timestamp` | `GetTimestamp(idx)` |
| `TimestampLtz` | `Timestamp` | `GetTimestamp(idx)` |
| `DecimalString` | `std::string` | `DecimalToString(idx)` |

## Type Checking

```cpp
if (rec.row.GetType(0) == fluss::DatumType::Int32) {
    int32_t value = rec.row.GetInt32(0);
}
if (rec.row.IsNull(1)) {
    // field is null
}
if (rec.row.IsDecimal(2)) {
    std::string decimal_str = rec.row.DecimalToString(2);
}
```

## Constants

```cpp
constexpr int64_t fluss::EARLIEST_OFFSET = -2;  // Start from earliest
constexpr int64_t fluss::LATEST_OFFSET = -1;    // Start from latest
```
