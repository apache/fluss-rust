---
date: 2026-03-15T11:29:18Z
researcher: hemanth
git_commit: 7d4bfd663be7d3edf527ffcba56d5c370c67cf20
branch: main
repository: hemanthsavasere/fluss-rust
topic: "ROW (nested struct) column end-to-end support"
tags: [research, codebase, row-type, datum, compacted-row, internal-row, field-getter, value-writer, key-encoder]
status: complete
last_updated: 2026-03-15
last_updated_by: hemanth
---

# Research: ROW (Nested Struct) Column End-to-End Support

**Date**: 2026-03-15T11:29:18Z
**Researcher**: hemanth
**Git Commit**: [7d4bfd6](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20)
**Branch**: main
**Repository**: hemanthsavasere/fluss-rust

---

## Research Question

Add end-to-end support for ROW (nested struct) columns. A nested Row is just a recursively-embedded CompactedRow — the same format as a top-level row, length-prefixed. The `Datum` enum needs a `Row` variant, `InternalRow` needs `get_row()`. Java references: `CompactedRowWriter.java` lines 339–346 and `CompactedRowReader.java` lines 370–378 — it's serialize-to-bytes then `write_bytes`, same as strings. The value writer, field getter, and key encoder need the same additions as the other two types.

---

## Summary

`DataType::Row(RowType)` is already fully modeled at the schema level (type definition, JSON serde, Arrow type conversion). **Nothing in that layer needs changing.** What is missing is the entire row-serialization stack: `Datum` has no `Row` variant, `InternalRow` has no `get_row()`, `CompactedRowWriter`/`CompactedRowDeserializer` panic on `DataType::Row`, and `FieldGetter`/`ValueWriter` hit `unimplemented!()`.

The implementation pattern is the same as `String`/`Bytes` in the writer: serialize the nested row to bytes with a recursive `CompactedRowWriter`, then store with `write_bytes()` (varint length prefix + raw bytes). On the read side, `read_bytes()` returns the raw slice, which is recursively wrapped in a `CompactedRow`. No new binary format is needed.

Array and Map are in the exact same situation — none of the serialization layers implement them either. All three complex types share the same set of `TODO` comments, `unimplemented!()` guards, and `_ => panic!` arms.

---

## Detailed Findings

### What Is Already Implemented (No Changes Needed)

| Component | File | Status |
|---|---|---|
| `DataType::Row(RowType)` enum variant | [`metadata/datatype.rs:26-46`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/metadata/datatype.rs#L26-L46) | ✅ Fully defined |
| `RowType` struct with `fields: Vec<DataField>` | [`metadata/datatype.rs:918`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/metadata/datatype.rs#L918) | ✅ Fully defined |
| `is_nullable` / `as_non_nullable` / `Display` for Row | [`metadata/datatype.rs:49-119`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/metadata/datatype.rs#L49-L119) | ✅ Dispatched |
| JSON serde for `Row` | [`metadata/json_serde.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/metadata/json_serde.rs) | ✅ Serialize + deserialize |
| Arrow type conversion (`Row` → `ArrowDataType::Struct`) | [`record/arrow.rs:1054-1076`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/record/arrow.rs#L1054-L1076) | ✅ Implemented |

### What Needs to Be Implemented

#### 1. `Datum` Enum — `row/datum.rs:39-71`

**Current state**: 15 variants covering all scalar types. No `Array`, `Map`, or `Row` variant. The TODO is implicit — the comment `//TODO Array, Map, Row` appears in `field_getter.rs:180` and `binary_writer.rs:139`.

```rust
// Current variants (datum.rs:39-71):
pub enum Datum<'a> {
    Null, Bool(bool), Int8(i8), Int16(i16), Int32(i32), Int64(i64),
    Float32(F32), Float64(F64),
    String(Str<'a>),   // Cow<'a, str> — zero-copy borrow from buffer
    Blob(Blob<'a>),    // Cow<'a, [u8]> — zero-copy borrow from buffer
    Decimal(Decimal), Date(Date), Time(Time),
    TimestampNtz(TimestampNtz), TimestampLtz(TimestampLtz),
    // No Row variant yet
}
```

Need to add a `Row` variant carrying a nested row. The `String`/`Blob` variants use `Cow<'a, …>` for zero-copy borrowing — the Row variant will need a similar lifetime or boxing strategy.

#### 2. `InternalRow` Trait — `row/mod.rs:61-126`

**Current state**: 18 typed `get_*` methods (get_boolean through get_bytes). No `get_row()`.

```rust
// Current trait (mod.rs:61-126) — abridged:
pub trait InternalRow: Send + Sync {
    fn get_field_count(&self) -> usize;
    fn is_null_at(&self, pos: usize) -> Result<bool>;
    fn get_boolean(&self, pos: usize) -> Result<bool>;
    // ... 15 more typed getters ...
    fn get_bytes(&self, pos: usize) -> Result<&[u8]>;
    // No get_row yet
}
```

Need to add: `fn get_row(&self, pos: usize) -> Result<...>` with an appropriate return type (boxed trait object or concrete type). Must be implemented on `GenericRow`, `CompactedRow`, and `ColumnarRow`.

#### 3. `CompactedRowWriter` + `BinaryWriter` Trait

**Current state**: `BinaryWriter` trait has TODO stubs for `write_array` and `write_row` commented out at [`binary_writer.rs:70-74`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L70-L74). `CompactedRowWriter` has no `write_row` method.

The reference pattern in the same writer is `write_bytes` / `write_string` ([`compacted_row_writer.rs:115-130`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_writer.rs#L115-L130)):

```rust
// write_bytes: varint(len) + raw bytes
fn write_bytes(&mut self, value: &[u8]) {
    self.write_int(value.len() as i32);
    self.write_raw(value);
}

// write_string: delegates to write_bytes
fn write_string(&mut self, value: &str) {
    self.write_bytes(value.as_ref());
}
```

Java's `CompactedRowWriter.java:339-346` does the same for nested rows: serialize the nested row to bytes, then call `writeBytes`. The Rust implementation should:

```rust
fn write_row(&mut self, value: &dyn InternalRow, row_type: &RowType) {
    let mut nested_writer = CompactedRowWriter::new(row_type.fields().len());
    // ... encode each field of value into nested_writer ...
    let bytes = nested_writer.to_bytes();
    self.write_bytes(&bytes);
}
```

#### 4. `CompactedRowDeserializer` — `row/compacted/compacted_row_reader.rs:52-171`

**Current state**: The `deserialize` method's `DataType` match falls through to `panic!` for all unsupported types at [`compacted_row_reader.rs:163-165`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_reader.rs#L163-L165):

```rust
_ => {
    panic!("Unsupported DataType in CompactedRowDeserializer: {dtype:?}");
}
```

The reference pattern is `read_bytes` / `read_string` ([`compacted_row_reader.rs:275-287`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_reader.rs#L275-L287)):

```rust
// read_bytes: returns (&[u8], next_cursor)
pub fn read_bytes(&self, pos: usize) -> (&[u8], usize) {
    let (len, data_pos) = self.read_int(pos);
    (&self.segment[data_pos..data_pos + len as usize], data_pos + len as usize)
}

pub fn read_string(&self, pos: usize) -> (&str, usize) {
    let (bytes, next) = self.read_bytes(pos);
    (std::str::from_utf8(bytes).unwrap(), next)
}
```

Java's `CompactedRowReader.java:370-378` does the same for nested rows: call `readBytes`, then deserialize the result. The Rust implementation should:

```rust
DataType::Row(row_type) => {
    let (nested_bytes, next_cursor) = reader.read_bytes(cursor);
    let nested_row = CompactedRow::from_bytes(row_type, nested_bytes);
    row.set_field(i, Datum::Row(Box::new(nested_row)));
    cursor = next_cursor;
}
```

#### 5. `FieldGetter` + `InnerFieldGetter` — `row/field_getter.rs`

**Current state**: `InnerFieldGetter` has 16 variants ([`field_getter.rs:97-152`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/field_getter.rs#L97-L152)). `create()` hits `unimplemented!()` for unknown types ([`field_getter.rs:85`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/field_getter.rs#L85)). `get_field` has `//TODO Array, Map, Row` at [`field_getter.rs:180`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/field_getter.rs#L180).

All existing variants follow the same pattern:
```rust
// Example: InnerFieldGetter::String { pos }
InnerFieldGetter::String { pos } => {
    Datum::String(Str::from(row.get_string(*pos)?))
}
```

Need to add:
```rust
// In InnerFieldGetter enum:
Row { pos },   // (RowType not needed here since get_row returns the whole row)

// In create():
DataType::Row(_) => InnerFieldGetter::Row { pos },

// In get_field():
InnerFieldGetter::Row { pos } => {
    Datum::Row(row.get_row(*pos)?)
}
```

#### 6. `InnerValueWriter` + `create_inner_value_writer` + `write_value` — `row/binary/binary_writer.rs`

**Current state**: `InnerValueWriter` has 16 variants ([`binary_writer.rs:122-140`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L122-L140)) with `// TODO Array, Row` at line 139. `create_inner_value_writer` hits `unimplemented!()` at [`binary_writer.rs:178`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L178). `write_value` hits `Err(IllegalArgument)` for mismatched pairs.

The existing variant pattern (Decimal stores type params):
```rust
// In InnerValueWriter enum:
Decimal(u32, u32),      // precision, scale
TimestampNtz(u32),      // precision
```

For Row, the `RowType` is needed to recursively encode fields:
```rust
// In InnerValueWriter enum:
Row(RowType),           // schema of the nested row

// In create_inner_value_writer():
DataType::Row(row_type) => InnerValueWriter::Row(row_type.clone()),

// In write_value():
(InnerValueWriter::Row(row_type), Datum::Row(inner_row)) => {
    writer.write_row(inner_row.as_ref(), row_type)?;
}
```

#### 7. `CompactedKeyEncoder` — `row/encode/compacted_key_encoder.rs:266-268`

**Current state**: The Java-compatibility test has explicit TODO comments:
```rust
// TODO: Add support for ARRAY type
// TODO: Add support for MAP type
// TODO: Add support for ROW type
```

ROW as a primary key column would flow through `FieldGetter` and `ValueWriter` just like any other type. Once steps 5 and 6 above are done, `CompactedKeyEncoder` should work for Row automatically (since it constructs its `FieldGetter`s and `ValueWriter`s by calling `FieldGetter::create` and `ValueWriter::create_value_writer`). The test just needs the TODO comments replaced with actual test cases.

---

## Wire Format Documentation

The nested-row wire format follows the same length-prefixed bytes pattern as `String` and `Bytes`:

```
[ varint(nested_row_byte_length) ][ nested_row_bytes ... ]
```

Where `nested_row_bytes` is a complete compacted row:
```
[ null-bit-header: ceil(field_count/8) bytes ][ field_0 ][ field_1 ] ... [ field_N ]
```

This is recursive — a `Row<Row<INT>>` stores:
```
varint(outer_len) [
  null-bits(1 byte)
  varint(inner_len) [
    null-bits(1 byte)
    varint(int_value)
  ]
]
```

No new binary primitives are needed. The existing `write_bytes`/`read_bytes` plus recursive `CompactedRowWriter`/`CompactedRowDeserializer` cover everything.

---

## Code References

| File | Line(s) | Description |
|---|---|---|
| [`row/datum.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/datum.rs#L39-L71) | 39–71 | `Datum` enum — add `Row` variant here |
| [`row/mod.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/mod.rs#L61-L126) | 61–126 | `InternalRow` trait — add `get_row()` here |
| [`row/compacted/compacted_row_writer.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_writer.rs#L115-L130) | 115–130 | `write_bytes`/`write_string` — reference pattern for `write_row` |
| [`row/compacted/compacted_row_reader.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_reader.rs#L163-L165) | 163–165 | `panic!` arm in deserializer — replace with `DataType::Row` arm |
| [`row/compacted/compacted_row_reader.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/compacted/compacted_row_reader.rs#L275-L287) | 275–287 | `read_bytes`/`read_string` — reference pattern for reading nested rows |
| [`row/field_getter.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/field_getter.rs#L85) | 85 | `unimplemented!()` in `create()` — add `DataType::Row` arm |
| [`row/field_getter.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/field_getter.rs#L97-L180) | 97–180 | `InnerFieldGetter` enum + `get_field` dispatch — add `Row` variant |
| [`row/binary/binary_writer.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L70-L74) | 70–74 | `BinaryWriter` trait — uncomment/add `write_row` method |
| [`row/binary/binary_writer.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L122-L140) | 122–140 | `InnerValueWriter` enum — add `Row(RowType)` variant |
| [`row/binary/binary_writer.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L145-L181) | 145–181 | `create_inner_value_writer` — add `DataType::Row` arm |
| [`row/binary/binary_writer.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/binary/binary_writer.rs#L184-L247) | 184–247 | `write_value` dispatch — add `(Row(t), Datum::Row(r))` arm |
| [`row/encode/compacted_key_encoder.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/row/encode/compacted_key_encoder.rs#L266-L268) | 266–268 | TODO comments for Array/Map/Row in Java-compat test |
| [`metadata/datatype.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/metadata/datatype.rs#L918) | 918 | `RowType` struct — already fully defined, no changes needed |
| [`record/arrow.rs`](https://github.com/hemanthsavasere/fluss-rust/blob/7d4bfd663be7d3edf527ffcba56d5c370c67cf20/crates/fluss/src/record/arrow.rs#L1054-L1076) | 1054–1076 | Arrow `Struct` conversion for Row — already implemented |

---

## Architecture Documentation

### Existing Patterns That ROW Must Follow

**String/Bytes in `CompactedRowWriter`** (the write-side template):
1. `write_bytes(value)` = `write_int(len)` + `write_raw(bytes)` (`compacted_row_writer.rs:115-120`)
2. `write_string(value)` = `write_bytes(value.as_ref())` (`compacted_row_writer.rs:128-130`)
3. ROW follows: `write_row(row, type)` = serialize row → bytes, then `write_bytes(bytes)`

**String/Bytes in `CompactedRowDeserializer`** (the read-side template):
1. `read_bytes(pos)` = `read_int(pos)` → len, return `(segment[pos..pos+len], pos+len)` (`compacted_row_reader.rs:275-281`)
2. `read_string(pos)` = `read_bytes(pos)` + `from_utf8` (`compacted_row_reader.rs:283-287`)
3. ROW follows: `read_bytes(pos)` → raw slice, then `CompactedRow::from_bytes(row_type, slice)`

**`Decimal(precision, scale)` in `InnerValueWriter`** (template for type params):
- The variant carries type parameters needed at write time
- `InnerValueWriter::Row(RowType)` follows the same pattern — RowType carries the field schema

**`TimestampNtz(precision)` / `TimestampLtz(precision)` in `InnerFieldGetter`** (template for type params in FieldGetter):
- The variant carries read parameters needed at get time
- `InnerFieldGetter::Row { pos }` is simpler — no extra params needed beyond `pos` since `get_row` returns the whole row

### Data Flow for ROW Fields (once implemented)

**Write path**:
```
GenericRow { field: Datum::Row(inner_row) }
  → FieldGetter::Row { pos }.get_field(outer_row)
  → Datum::Row(inner_row)
  → InnerValueWriter::Row(row_type).write_value(writer, pos, datum)
  → CompactedRowWriter::write_row(inner_row, row_type)
      → nested CompactedRowWriter serializes inner_row
      → write_bytes(nested_bytes)    // varint(len) + raw
```

**Read path**:
```
raw bytes from wire
  → CompactedRowDeserializer: DataType::Row(row_type) arm
  → reader.read_bytes(cursor)       // read varint(len) + raw slice
  → CompactedRow::from_bytes(row_type, slice)
  → store as Datum::Row(nested_compacted_row)
  → cached in GenericRow via OnceLock
  → outer row.get_row(pos) returns the nested CompactedRow
```

---

## Planned Tests

Per the original task description:

1. **Simple nested row** — `ROW<INT, STRING>`, single level of nesting
2. **Deeply nested** — `ROW<ROW<INT>>`, recursive nesting
3. **Nullable fields** — nested row containing nullable fields; outer nullable ROW column
4. **Combination** (separate issue) — `ROW<ARRAY<INT>, MAP<STRING, INT>>` once Array and Map also land

---

## Open Questions

1. **Return type of `get_row`** — Should it return `Box<dyn InternalRow>`, `&dyn InternalRow`, or a concrete type like `CompactedRow`? The lifetime constraints from `GenericRow<'a>` (which stores `Datum<'a>`) may require a lifetime on the return type.

2. **`Datum::Row` inner type** — `Box<GenericRow<'a>>` works but loses the zero-copy property that `Cow<'a, …>` provides for String/Blob. Alternatively, storing the raw bytes in `Datum::Row(Blob<'a>)` and lazily deserializing preserves zero-copy but complicates access patterns.

3. **`write_row` signature in `BinaryWriter` trait** — Needs `&RowType` to know how many fields and their types for serializing. Whether the encoder carries the `RowType` in `InnerValueWriter::Row(RowType)` or passes it at call time affects the API.

4. **`ColumnarRow::get_row`** — Extracting a struct sub-row from an Arrow `StructArray` is more complex than the other implementations. This may need its own sub-task.
