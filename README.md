<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Apache Fluss™ Rust Client has moved 🚚

[![crates.io](https://img.shields.io/crates/v/fluss-rs.svg)](https://crates.io/crates/fluss-rs)
[![docs.rs](https://img.shields.io/docsrs/fluss-rs)](https://docs.rs/fluss-rs/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

**The Rust client and the Python, C++, and Elixir bindings built on it now live in the main Apache Fluss repository:**

### 👉 [github.com/apache/fluss → `fluss-rust/`](https://github.com/apache/fluss/tree/main/fluss-rust)

Development continues there, at full speed. Under [FIP-40](https://cwiki.apache.org/confluence/spaces/FLUSS/pages/421957144/FIP-40+Consolidate+apache+fluss-rust+into+apache+fluss), the clients now share a single repository with the Fluss server: one protocol definition, one CI pipeline, one set of docs, and jointly numbered releases — so every Fluss release ships a matching Rust client, and cross-cutting changes land in a single PR instead of two.

## Where to find things

| |                                                                                      |
| --- |--------------------------------------------------------------------------------------|
| **Source code** | [apache/fluss → `fluss-rust/`](https://github.com/apache/fluss/tree/main/fluss-rust) |
| **Documentation** | [fluss.apache.org/docs/apis/](https://fluss.apache.org/docs/apis/)                   |
| **Rust crate** | [`fluss-rs` on crates.io](https://crates.io/crates/fluss-rs) — unchanged             |
| **API reference** | [docs.rs/fluss-rs](https://docs.rs/fluss-rs/)                                        |
| **Issues & PRs** | [apache/fluss/issues](https://github.com/apache/fluss/issues)                        |
| **Community** | [Mailing lists](https://fluss.apache.org/community/welcome/)                         |

Published artifacts are unchanged, if you depend on `fluss-rs`, there is nothing to update.

## Contributing

Contributions are as welcome as ever, just one repo over. Clone [apache/fluss](https://github.com/apache/fluss) and open your pull request against `fluss-rust/`; see [`fluss-rust/DEVELOPMENT.md`](https://github.com/apache/fluss/blob/main/fluss-rust/DEVELOPMENT.md) for build and test instructions. If you have an in-flight PR here, please reopen it there, we'd love to see it land.

---

This repository is read-only. Its history, issues, and pull requests remain available at their existing URLs.

Apache Fluss, Fluss, Apache, the Apache feather logo, and the Apache Fluss project logo
are either registered trademarks or trademarks of The Apache Software Foundation
in the United States and other countries.