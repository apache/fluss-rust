<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Development Guide

Welcome to the development guide of `fluss-rust`! This project builds `fluss-rust` client and language specific bindings.  

## Pre-requisites

- protobuf
- rust

You can install these using your favourite package / version manager. Example installation using mise:

```bash
mise install protobuf
mise install rust
```

We recommend [RustRover](https://www.jetbrains.com/rust/) IDE to work with fluss-rust code base.

## Project directories

Source files are organized in the following manner

1. `crates/fluss` - fluss rust client crate source
2. `crates/examples` - fluss rust client examples
3. `bindings` - bindings to other languages e.g. C++ under `bindings/cpp` and Python under `bindings/python`

## Building & Testing

See [quickstart](README.md#quick-start) for steps to run example code.

Running all unit tests for fluss rust client: 

```bash
cargo test --release
```



### Formatting and Clippy

Our CI runs cargo formatting and clippy to help keep the code base styling tidy and readable. Run the following commands and address any errors or warnings to ensure that your PR can complete CI successfully.

```bash
cargo fmt --all
cargo clippy --all-targets --fix --allow-dirty --allow-staged
```

