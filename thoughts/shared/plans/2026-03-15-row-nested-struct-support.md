# ROW (Nested Struct) Column End-to-End Support

## Overview

Add end-to-end serialization support for `DataType::Row(RowType)` columns. The schema layer (type definition, JSON serde, Arrow conversion) is already complete. What is missing is the entire row-serialization stack: `Datum` has no `Row` variant, `InternalRow` has no `get_row()`, `CompactedRowWriter`/`CompactedRowDeserializer` panic on `DataType::Row`, and `FieldGetter`/`ValueWriter` hit `unimplemented!()`.

Wire format: identical to `String`/`Bytes` — a varint-length-prefixed blob where the blob is a complete compacted row (null-bit header + fields). No new binary primitives needed.

**Java reference verified**: `CompactedRowWriter.java:339-346` — `writeRow` converts the inner row to bytes via `RowSerializer.toBinaryRow(value)` (which serializes in CompactedRow format) then calls `write(length, segments, offset)` which is `writeInt(len)` + raw bytes, identical to `writeBytes`. `CompactedRowReader.java:372-378` — `readRow` calls `readInt()` for the length, then creates a `CompactedRow` pointing into the buffer at the current position (zero-copy lazy decode). The Rust plan uses eager decode to `GenericRow` instead of lazy decode, which is simpler and correct.

## Current State Analysis

- `DataType::Row(RowType)`, `RowType { fields: Vec<DataField> }` — fully defined at `metadata/datatype.rs:918`
- JSON serde and Arrow `Struct` conversion — already implemented
- `Datum` enum (`datum.rs:39-71`) — 15 scalar variants, no `Row`
- `InternalRow` trait (`mod.rs:61-126`) — 18 typed getters, no `get_row()`
- `CompactedRowDeserializer::deserialize` (`compacted_row_reader.rs:163-165`) — `_ => panic!(...)`
- `CompactedRowWriter` — no `write_row` method
- `FieldGetter::create` (`field_getter.rs:85`) — `_ => unimplemented!(...)`
- `InnerFieldGetter::get_field` (`field_getter.rs:180`) — `//TODO Array, Map, Row`
- `InnerValueWriter` (`binary_writer.rs:139`) — `// TODO Array, Row`
- `InnerValueWriter::create_inner_value_writer` (`binary_writer.rs:178`) — `_ => unimplemented!(...)`

## Desired End State

After this plan, the following must work:

```rust
// Build and serialize a row containing a nested ROW<INT, STRING>
let inner_row_type = RowType::with_data_types_and_field_names(
    vec![DataTypes::int(), DataTypes::string()],
    vec!["x", "label"],
);
let outer_row_type = RowType::with_data_types_and_field_names(
    vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
    vec!["id", "nested"],
);

let mut inner = GenericRow::new(2);
inner.set_field(0, 42_i32);
inner.set_field(1, "hello");

let mut outer = GenericRow::new(2);
outer.set_field(0, 1_i32);
outer.set_field(1, Datum::Row(Box::new(inner)));

// Write
let mut writer = CompactedRowWriter::new(2);
// ... (via FieldGetter + ValueWriter)

// Read back via CompactedRowDeserializer and assert values match
```

Tests must pass: `cargo test -p fluss`

## Key Discoveries

- `RowType` derives `Debug, Clone, PartialEq, Eq, Hash, Serialize` — safe to store in `InnerValueWriter::Row(RowType)` (`datatype.rs:917`)
- `CompactedRow` delegates all `InternalRow` getters through `decoded_row()` → `&GenericRow` (`compacted_row.rs:71-74`). Adding `get_row` to `CompactedRow` follows this same delegation pattern.
- `CompactedRowReader::read_bytes` at line 275 returns `(&'a [u8], usize)` — the slice lifetime `'a` ties to the underlying data buffer, so nested rows can be deserialized with matching lifetimes.
- `GenericRow` currently derives only `Debug`. Its field `values: Vec<Datum<'a>>` means all the needed traits (`Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize`) are derivable because `Vec<T>` derives them when `T` does and `Datum` already implements all of them.
- `parse_display::Display` on `Datum` uses `#[display("...")]` per variant — adding `#[display("ROW({0:?})")]` on `Datum::Row` satisfies the Display derive requirement.
- `ColumnarRow::get_row` would require extracting a `StructArray` sub-row — this is non-trivial and is explicitly excluded from this plan.

## What We're NOT Doing

- Array or Map type support (same pattern, separate task)
- `ColumnarRow::get_row` — requires Arrow `StructArray` extraction, separate sub-task
- `Datum::append_to` for `Row` (Arrow `StructBuilder`) — not needed for the serialization path
- Extending the Java-compat hex test (`test_all_data_types_java_compatible`) in `CompactedKeyEncoder` for ROW (needs Java reference hex data from `encoded_key.hex`)
- Performance optimization of `InnerValueWriter::Row` (currently creates `InnerValueWriter` per-field per-call; a cache can be added later)

## Key Encoder: No Code Changes Needed

`CompactedKeyEncoder` (`compacted_key_encoder.rs`) uses `FieldGetter::create` + `CompactedKeyWriter::create_value_writer` (which delegates to `ValueWriter::create_value_writer`). `CompactedKeyWriter` implements `BinaryWriter` by delegating all methods (including `write_bytes`) to `CompactedRowWriter`. Once FieldGetter and ValueWriter support Row (Phase 2), the key encoder works automatically for ROW primary key columns — no additional code changes needed. A test is included in Phase 3.

## Implementation Approach

Work bottom-up: type system first, then wire up write/read, then FieldGetter/ValueWriter, then tests. Each phase compiles cleanly before proceeding.

---

## Phase 1: Core Type System

### Overview
Add `Datum::Row` variant and derive required traits on `GenericRow`. Add `get_row()` to `InternalRow` and implement it on `GenericRow` and `CompactedRow`. Leave `ColumnarRow` as `unimplemented!()`.

### Changes Required

#### 1. `GenericRow` — add derives
**File**: `crates/fluss/src/row/mod.rs`

Add `Clone, PartialEq, Eq, PartialOrd, Ord, Hash` to `GenericRow`'s derive. `Serialize` is also needed for `Datum::Row`.

```rust
// Before:
#[derive(Debug)]
pub struct GenericRow<'a> {
    pub values: Vec<Datum<'a>>,
}

// After:
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct GenericRow<'a> {
    pub values: Vec<Datum<'a>>,
}
```

Note: `Serialize` is required because `Datum` derives `Serialize` (serde). Adding `Datum::Row(Box<GenericRow<'a>>)` means the serde derive on `Datum` requires `GenericRow<'a>: Serialize`. The recursive type (`Datum` → `GenericRow` → `Vec<Datum>`) is handled correctly by serde through `Box`. Add `use serde::Serialize;` to the imports in `mod.rs` (serde is already a dependency — `datum.rs` imports it at line 33).

#### 2. `Datum::Row` variant
**File**: `crates/fluss/src/row/datum.rs`

```rust
// In the Datum enum, after TimestampLtz:
#[display("ROW({0:?})")]
Row(Box<GenericRow<'a>>),
```

Also add `use crate::row::GenericRow;` at the top of datum.rs (check if it's already imported via `mod.rs` re-exports).

The `is_null()` method already uses `matches!(self, Datum::Null)` so no change needed there.

Add an accessor on `Datum`:
```rust
pub fn as_row(&self) -> &GenericRow<'_> {
    match self {
        Self::Row(r) => r.as_ref(),
        _ => panic!("not a row: {self:?}"),
    }
}
```

#### 3. `InternalRow::get_row` — trait method
**File**: `crates/fluss/src/row/mod.rs`

Add to the `InternalRow` trait after `get_bytes`:
```rust
/// Returns the nested row value at the given position.
fn get_row(&self, pos: usize) -> Result<GenericRow<'_>>;
```

#### 4. `GenericRow::get_row` implementation
**File**: `crates/fluss/src/row/mod.rs`

Add to `impl<'a> InternalRow for GenericRow<'a>`:
```rust
fn get_row(&self, pos: usize) -> Result<GenericRow<'_>> {
    match self.get_value(pos)? {
        Datum::Row(r) => Ok(*r.clone()),
        other => Err(IllegalArgument {
            message: format!(
                "type mismatch at position {pos}: expected Row, got {other:?}"
            ),
        }),
    }
}
```

Note: This clones the inner `Box<GenericRow>` — `*r.clone()` dereferences the cloned Box to get an owned `GenericRow`. Acceptable for correctness; optimize later if needed.

#### 5. `CompactedRow::get_row` implementation
**File**: `crates/fluss/src/row/compacted/compacted_row.rs`

Following the same delegation pattern as all other getters:
```rust
fn get_row(&self, pos: usize) -> Result<GenericRow<'_>> {
    self.decoded_row().get_row(pos)
}
```

#### 6. `ColumnarRow::get_row` — stub
**File**: `crates/fluss/src/row/column.rs`

```rust
fn get_row(&self, pos: usize) -> Result<crate::row::GenericRow<'_>> {
    unimplemented!("ColumnarRow::get_row is not yet implemented — requires Arrow StructArray extraction")
}
```

### Success Criteria

#### Automated Verification:
- [x] `cargo build -p fluss` compiles with no errors
- [x] `cargo test -p fluss` — all existing tests pass (250 passed)

---

## Phase 2: Serialization Stack

### Overview
Wire up the write path (`CompactedRowWriter` + `InnerValueWriter`) and the read path (`CompactedRowDeserializer`), then hook up `FieldGetter` and `InnerValueWriter` to expose Row to the encoder pipeline.

### Changes Required

#### 1. `InnerValueWriter::Row` variant
**File**: `crates/fluss/src/row/binary/binary_writer.rs`

```rust
// In InnerValueWriter enum, replacing the TODO comment:
Row(RowType),  // schema needed to iterate fields during write
```

Remove or update the `// TODO Array, Row` comment.

#### 2. `InnerValueWriter::create_inner_value_writer` — Row arm
**File**: `crates/fluss/src/row/binary/binary_writer.rs`

```rust
// In the match, before the _ => unimplemented! arm:
DataType::Row(row_type) => Ok(InnerValueWriter::Row(row_type.clone())),
```

Add `use crate::metadata::RowType;` if not already imported.

#### 3. `InnerValueWriter::write_value` — Row arm
**File**: `crates/fluss/src/row/binary/binary_writer.rs`

`write_value` is generic `W: BinaryWriter`. The `BinaryWriter` trait has `write_bytes`, so we create a nested `CompactedRowWriter` (always, regardless of outer writer type), serialize the inner row into it, then call `writer.write_bytes(nested.buffer())`. This mirrors Java's `writeRow`: `RowSerializer.toBinaryRow(value)` produces compacted-format bytes, then `write(length, segments, offset)` = `writeInt(len)` + raw bytes = `writeBytes`.

```rust
// In write_value, before the _ => Err(...) arm:
(InnerValueWriter::Row(row_type), Datum::Row(inner_row)) => {
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
    let field_count = row_type.fields().len();
    let mut nested = CompactedRowWriter::new(field_count);
    for (i, field) in row_type.fields().iter().enumerate() {
        let datum = &inner_row.values[i];
        if datum.is_null() {
            if field.data_type.is_nullable() {
                nested.set_null_at(i);
            }
        } else {
            let vw = InnerValueWriter::create_inner_value_writer(&field.data_type, None)
                .expect("create_inner_value_writer failed for nested row field");
            vw.write_value(&mut nested, i, datum)
                .expect("write_value failed for nested row field");
        }
    }
    writer.write_bytes(nested.buffer());
}
```

Note: `nested.buffer()` = `&self.buffer[..self.position]` = the full compacted row (null-bit header + field bytes). `BinaryWriter::write_bytes` prepends a varint length — this is the complete wire encoding.

#### 4. `CompactedRowDeserializer::deserialize` — Row arm
**File**: `crates/fluss/src/row/compacted/compacted_row_reader.rs`

Mirrors Java's `readRow`: `readInt()` (varint length) → slice the buffer → create `CompactedRow.pointTo(...)`. The Rust equivalent uses eager decode instead of lazy `pointTo`. Add before the `_ => panic!(...)` arm:

```rust
DataType::Row(row_type) => {
    // read_bytes returns (&'a [u8], next_cursor) — varint len + raw slice
    let (nested_bytes, next) = reader.read_bytes(cursor);
    let nested_reader = CompactedRowReader::new(
        row_type.fields().len(),
        nested_bytes,
        0,
        nested_bytes.len(),
    );
    // new_from_owned avoids borrowing the local row_type reference across the loop
    let nested_deser = CompactedRowDeserializer::new_from_owned(row_type.clone());
    let nested_row = nested_deser.deserialize(&nested_reader);
    (Datum::Row(Box::new(nested_row)), next)
}
```

Lifetime note: `nested_bytes: &'a [u8]` borrows from the outer buffer (same lifetime `'a`). The returned `GenericRow<'a>` may contain `Cow::Borrowed` string/bytes datums that also borrow from `'a`. The local `nested_deser` is dropped after the block; this is fine because `GenericRow` borrows only from the reader's data buffer, not from the deserializer.

#### 5. `FieldGetter::create` — Row arm
**File**: `crates/fluss/src/row/field_getter.rs`

```rust
// In FieldGetter::create match, before the _ => unimplemented!(...) arm:
DataType::Row(_) => InnerFieldGetter::Row { pos },
```

#### 6. `InnerFieldGetter` — Row variant
**File**: `crates/fluss/src/row/field_getter.rs`

```rust
// In InnerFieldGetter enum, after TimestampLtz:
Row {
    pos: usize,
},
```

#### 7. `InnerFieldGetter::get_field` — Row arm
**File**: `crates/fluss/src/row/field_getter.rs`

```rust
// In get_field match, replacing the //TODO Array, Map, Row comment:
InnerFieldGetter::Row { pos } => {
    Datum::Row(Box::new(row.get_row(*pos)?))
}
```

Remove or update the `//TODO Array, Map, Row` comment.

#### 8. `InnerFieldGetter::pos` — Row arm
**File**: `crates/fluss/src/row/field_getter.rs`

Add `Row { pos }` to the `pos()` method's catch-all pattern:

```rust
// In pos() match:
| Self::TimestampLtz { pos, .. }
| Self::Row { pos } => *pos,
```

### Success Criteria

#### Automated Verification:
- [x] `cargo build -p fluss` compiles with no errors
- [x] `cargo test -p fluss` — all existing tests pass

---

## Phase 3: Tests

### Overview
Add unit tests covering simple nesting, deep nesting, and nullable nested fields. All tests exercise the full round-trip: `GenericRow` → `CompactedRowWriter` (via `ValueWriter`/`FieldGetter`) → bytes → `CompactedRowDeserializer` → `GenericRow`.

### Changes Required

#### 1. Round-trip tests for ROW type
**File**: `crates/fluss/src/row/compacted/compacted_row_reader.rs` (in the existing `#[cfg(test)]` module, or add a new test module at the bottom)

```rust
#[cfg(test)]
mod row_type_tests {
    use crate::metadata::{DataTypes, DataType, RowType};
    use crate::row::{Datum, GenericRow};
    use crate::row::binary::{BinaryWriter, ValueWriter};
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
    use crate::row::compacted::compacted_row_reader::{
        CompactedRowDeserializer, CompactedRowReader,
    };
    use crate::row::field_getter::FieldGetter;

    fn round_trip(outer_row_type: &RowType, outer_row: &GenericRow) -> GenericRow {
        // Write
        let field_getters = FieldGetter::create_field_getters(outer_row_type);
        let value_writers: Vec<ValueWriter> = outer_row_type
            .fields()
            .iter()
            .map(|f| ValueWriter::create_value_writer(f.data_type(), None).unwrap())
            .collect();
        let mut writer = CompactedRowWriter::new(outer_row_type.fields().len());
        for (i, (getter, vw)) in field_getters.iter().zip(value_writers.iter()).enumerate() {
            let datum = getter.get_field(outer_row as &dyn crate::row::InternalRow).unwrap();
            vw.write_value(&mut writer, i, &datum).unwrap();
        }
        let bytes = writer.to_bytes();

        // Read
        let deser = CompactedRowDeserializer::new(outer_row_type);
        let reader = CompactedRowReader::new(
            outer_row_type.fields().len(),
            bytes.as_ref(),
            0,
            bytes.len(),
        );
        deser.deserialize(&reader)
    }

    #[test]
    fn test_row_simple_nesting() {
        // ROW<INT, STRING> nested inside an outer row
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["x", "label"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
            vec!["id", "nested"],
        );

        let mut inner = GenericRow::new(2);
        inner.set_field(0, 42_i32);
        inner.set_field(1, "hello");

        let mut outer = GenericRow::new(2);
        outer.set_field(0, 1_i32);
        outer.set_field(1, Datum::Row(Box::new(inner)));

        let result = round_trip(&outer_row_type, &outer);

        assert_eq!(result.get_int(0).unwrap(), 1);
        let nested = result.get_row(1).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");
    }

    #[test]
    fn test_row_deep_nesting() {
        // ROW<ROW<INT>> — two levels of nesting
        let inner_inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int()],
            vec!["n"],
        );
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataType::Row(inner_inner_row_type.clone())],
            vec!["inner"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataType::Row(inner_row_type.clone())],
            vec!["outer"],
        );

        let mut innermost = GenericRow::new(1);
        innermost.set_field(0, 99_i32);

        let mut middle = GenericRow::new(1);
        middle.set_field(0, Datum::Row(Box::new(innermost)));

        let mut outer = GenericRow::new(1);
        outer.set_field(0, Datum::Row(Box::new(middle)));

        let result = round_trip(&outer_row_type, &outer);

        let mid = result.get_row(0).unwrap();
        let inner = mid.get_row(0).unwrap();
        assert_eq!(inner.get_int(0).unwrap(), 99);
    }

    #[test]
    fn test_row_with_nullable_fields() {
        // Outer nullable ROW column; nested row with a nullable STRING field set to null
        // DataTypes::string() and RowType::with_data_types_and_field_names both default to nullable=true
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "optional_name"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
            vec!["k", "nested"],
        );

        // Case 1: non-null nested row with a null field inside
        let mut inner = GenericRow::new(2);
        inner.set_field(0, 7_i32);
        inner.set_field(1, Datum::Null);

        let mut outer = GenericRow::new(2);
        outer.set_field(0, 10_i32);
        outer.set_field(1, Datum::Row(Box::new(inner)));

        let result = round_trip(&outer_row_type, &outer);
        assert_eq!(result.get_int(0).unwrap(), 10);
        let nested = result.get_row(1).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 7);
        assert!(nested.is_null_at(1).unwrap());

        // Case 2: outer ROW column is null
        let mut outer_null = GenericRow::new(2);
        outer_null.set_field(0, 20_i32);
        outer_null.set_field(1, Datum::Null);

        let result2 = round_trip(&outer_row_type, &outer_null);
        assert_eq!(result2.get_int(0).unwrap(), 20);
        assert!(result2.is_null_at(1).unwrap());
    }
}
```

Note: `DataTypes::string()` creates `StringType::with_nullable(true)` — nullable by default. `RowType::with_data_types_and_field_names(...)` creates `RowType::with_nullable(true, fields)` — also nullable by default. To make non-nullable types, use the inner type constructors: `StringType::with_nullable(false)`, `RowType::with_nullable(false, fields)`. The tests above use the default nullable types, which is correct for testing null handling.

#### 2. Key encoder test for ROW
**File**: `crates/fluss/src/row/encode/compacted_key_encoder.rs` (add to the existing `#[cfg(test)] mod tests`)

This verifies that `CompactedKeyEncoder` works with ROW as a primary key column. No code changes to the key encoder itself — this exercises the FieldGetter + ValueWriter Row support through the key encoder pipeline.

```rust
#[test]
fn test_row_as_primary_key() {
    // ROW<INT, STRING> as a primary key column
    let inner_row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string()],
        vec!["x", "label"],
    );
    let row_type = RowType::with_data_types_and_field_names(
        vec![
            DataTypes::int(),
            DataType::Row(inner_row_type.clone()),
        ],
        vec!["id", "nested"],
    );

    let mut inner = GenericRow::new(2);
    inner.set_field(0, 42_i32);
    inner.set_field(1, "hello");

    let mut row = GenericRow::new(2);
    row.set_field(0, 1_i32);
    row.set_field(1, Datum::Row(Box::new(inner)));

    let mut encoder = for_test_row_type(&row_type);
    let encoded = encoder.encode_key(&row).unwrap();

    // Verify it encodes without error and produces non-empty bytes
    assert!(!encoded.is_empty());

    // Encode the same row again to verify determinism
    let encoded2 = encoder.encode_key(&row).unwrap();
    assert_eq!(encoded, encoded2);

    // Encode a different nested row and verify different output
    let mut inner2 = GenericRow::new(2);
    inner2.set_field(0, 99_i32);
    inner2.set_field(1, "world");

    let mut row2 = GenericRow::new(2);
    row2.set_field(0, 1_i32);
    row2.set_field(1, Datum::Row(Box::new(inner2)));

    let encoded3 = encoder.encode_key(&row2).unwrap();
    assert_ne!(encoded, encoded3);
}
```

### Success Criteria

#### Automated Verification:
- [x] `cargo test -p fluss` — all tests pass including the new row tests
- [x] `cargo test -p fluss row_type_tests` — row round-trip tests pass
- [x] `cargo test -p fluss test_row_as_primary_key` — key encoder test passes
- [x] `cargo clippy -p fluss -- -D warnings` — no new warnings

#### Manual Verification:
- [ ] Run tests and confirm the nested row values round-trip correctly at each nesting level
- [ ] Confirm that the `DataType::Row` arm in `CompactedRowDeserializer` no longer panics

---

## Testing Strategy

### Unit Tests
- Simple `ROW<INT, STRING>`: single level, non-null fields
- Deep `ROW<ROW<INT>>`: two levels, verifies recursion in both write and read
- Nullable outer ROW: verifies null-bit handling for the column-level nullable flag
- Nullable inner field: nested row has a nullable field set to Datum::Null
- ROW as primary key: verifies the key encoder pipeline works with Row via FieldGetter + ValueWriter

### Not Tested Here (separate tasks)
- `ROW<ARRAY<INT>, MAP<STRING, INT>>` — blocked on Array/Map support
- `ColumnarRow::get_row` — blocked on StructArray extraction
- Java-compat hex test for ROW in `test_all_data_types_java_compatible` — needs Java reference hex data

---

## Migration Notes

No migration needed — this is additive. No existing wire-format bytes change.

---

## References

- Research document: `thoughts/shared/research/2026-03-15-row-nested-struct-support.md`
- Java reference — write: `CompactedRowWriter.java:339-346`
- Java reference — read: `CompactedRowReader.java:370-378`
- Wire format template: `compacted_row_writer.rs:115-130` (`write_bytes`/`write_string`)
- Read template: `compacted_row_reader.rs:275-287` (`read_bytes`/`read_string`)
