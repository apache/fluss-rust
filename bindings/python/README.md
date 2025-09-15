<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Fluss™ Python Bindings

Python bindings for Fluss using PyO3 and Maturin.

## API Overview

### Basic Usage

TODO: Add basic usage examples here

### Core Classes

#### `Config`

Configuration for Fluss connection parameters

#### `FlussConnection`

Main interface for connecting to Fluss cluster

#### `FlussAdmin`

Administrative operations for managing tables (create, delete, etc.)

#### `FlussTable`

Represents a Fluss table, providing read and write operations

#### `TableWriter`

Used for writing data to tables, supports PyArrow and Pandas

#### `LogScanner`

Used for scanning table log data

## Development

## Requirements

- Python 3.9+
- Rust 1.70+
- [uv](https://docs.astral.sh/uv/) package manager
- Linux or MacOS

> **⚠️ Before you start:**  
> Please make sure you can successfully build and run the [Fluss Rust client](../../crates/fluss/README.md) on your machine.  
> The Python bindings require a working Fluss Rust backend and compatible environment.

### Install Development Dependencies

```bash
cd bindings/python
uv sync --all-extras
```

### Build Development Version

```bash
source .venv/bin/activate
uv run maturin develop
```

### Build Release Version

```bash
uv run maturin build --release
```

### Code Formatting and Linting

```bash
uv run ruff format python/
uv run ruff check python/
```

### Type Checking

```bash
uv run mypy python/
```

### Run Examples

```bash
uv run python example/example.py
```

### Build API docs:

```bash
uv run pdoc fluss_python
```

### Release

```bash
# Build wheel
uv run maturin build --release

# Publish to PyPI
uv run maturin publish
```

## Project Structure
```
bindings/python/
├── Cargo.toml              # Rust dependency configuration
├── pyproject.toml          # Python project configuration
├── README.md              # This file
├── src/                   # Rust source code
│   ├── lib.rs            # Main entry module
│   ├── config.rs         # Configuration related
│   ├── connection.rs     # Connection management
│   ├── admin.rs          # Admin operations
│   ├── table.rs          # Table operations
│   ├── types.rs          # Data types
│   └── error.rs          # Error handling
├── python/               # Python package source
│   └── fluss_python/
│       ├── __init__.py   # Python package entry
        ├── __init__.pyi  # Stub file
│       └── py.typed      # Type declarations
└── example/              # Example code
    └── example.py
```

## License

Apache 2.0 License
