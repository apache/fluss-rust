---
slug: /
sidebar_position: 1
title: Introduction
---

# Introduction

[Apache Fluss](https://fluss.apache.org/) (incubating) is a streaming storage system built for real-time analytics, serving as the real-time data layer for Lakehouse architectures.

This documentation covers the **Fluss client libraries** for Rust, Python, and C++, which are developed in the [fluss-rust](https://github.com/apache/fluss-rust) repository. These clients allow you to:

- **Create and manage** databases, tables, and partitions
- **Write** data to log tables (append-only) and primary key tables (upsert/delete)
- **Read** data via log scanning and key lookups
- **Integrate** with the broader Fluss ecosystem including lakehouse snapshots

## Client Overview

| | Rust | Python | C++ |
|---|---|---|---|
| **Package** | [fluss-rs](https://crates.io/crates/fluss-rs) on crates.io | Build from source (PyO3) | Build from source (CMake) |
| **Async runtime** | Tokio | asyncio | Synchronous (Tokio runtime managed internally) |
| **Data format** | Arrow RecordBatch / GenericRow | PyArrow / Pandas / dict | Arrow RecordBatch / GenericRow |
| **Log tables** | Read + Write | Read + Write | Read + Write |
| **Primary key tables** | Upsert + Delete + Lookup | Upsert + Delete + Lookup | Upsert + Delete + Lookup |
| **Partitioned tables** | Full support | Write support | Full support |

## How This Guide Is Organised

The **User Guide** walks through installation, configuration, and working with each table type across all three languages. Code examples are shown side by side under **Rust**, **Python**, and **C++** headings.

The **Developer Guide** covers building from source, running tests, and the release process for contributors.
