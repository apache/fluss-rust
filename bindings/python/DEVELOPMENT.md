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

# Development

## Requirements

- Python 3.9+
- Rust 1.70+
- [uv](https://docs.astral.sh/uv/) package manager
- Linux or MacOS

> **Before you start:**
> Please make sure you can successfully build and run the [Fluss Rust client](../../crates/fluss/README.md) on your machine.
> The Python bindings require a working Fluss Rust backend and compatible environment.

## Install Development Dependencies

```bash
cd bindings/python
uv sync --all-extras
```

## Build Development Version

```bash
source .venv/bin/activate
uv run maturin develop
```

## Build Release Version

```bash
uv run maturin build --release
```

## Code Formatting and Linting

```bash
uv run ruff format python/
uv run ruff check python/
```

## Type Checking

```bash
uv run mypy python/
```

## Run Examples

```bash
uv run python example/example.py
```

## Build API Docs

```bash
uv run pdoc fluss
```

## Release

```bash
# Build wheel
uv run maturin build --release

# Publish to PyPI
uv run maturin publish
```

## Project Structure

```
bindings/python/
├── Cargo.toml            # Rust dependency configuration
├── pyproject.toml         # Python project configuration
├── README.md              # User guide
├── DEVELOPMENT.md         # This file
├── API_REFERENCE.md       # API reference
├── src/                   # Rust source code (PyO3 bindings)
│   ├── lib.rs
│   ├── config.rs
│   ├── connection.rs
│   ├── admin.rs
│   ├── table.rs
│   └── error.rs
├── fluss/                 # Python package
│   ├── __init__.py
│   ├── __init__.pyi       # Type stubs
│   └── py.typed
└── example/
    └── example.py
```

## License

Apache 2.0 License
