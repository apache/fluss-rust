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

# Apache Fluss™ C++ Bindings (Incubating)

C++ bindings for Fluss, built on top of the [fluss-rust](../../crates/fluss) client. The API is exposed via a C++ header ([include/fluss.hpp](include/fluss.hpp)) and implemented with Rust FFI.

## Requirements

- Rust (see [rust-toolchain.toml](../../rust-toolchain.toml) at repo root)
- C++17-capable compiler
- CMake 3.18+ and/or Bazel
- Apache Arrow (for Arrow-based APIs)

## Build

From the repository root or from `bindings/cpp`:

**With CMake:**

```bash
cd bindings/cpp
mkdir build && cd build
cmake ..
cmake --build .
```

**With Bazel:**

```bash
cd bindings/cpp
bazel build //...
```
See [ci.sh](ci.sh) for the CI build sequence.


## TODO

- [] How to introduce fluss-cpp in your own project, https://github.com/apache/opendal/blob/main/bindings/cpp/README.md is a good reference
- [ ] Add CMake/Bazel install and packaging instructions.
- [ ] Document API usage and minimal example in this README.
- [ ] Add more C++ examples (log scan, upsert, etc.).
