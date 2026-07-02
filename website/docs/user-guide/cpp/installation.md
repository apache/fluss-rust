---
sidebar_position: 1
---

# Installation

The C++ bindings are built from the `fluss-rust` source tree. The build uses
Rust for the core client and exposes a C++17 API through the `fluss_cpp` CMake
target.

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust
```

## Prerequisites

- Rust toolchain with `cargo` in `PATH`, or `CARGO=/path/to/cargo`
- C++17-capable compiler
- CMake 3.22+
- `protoc`
- Apache Arrow C++ when using system dependency mode

The current build guidance is tested with `protoc` 3.25.5 and Arrow C++ 19.0.1.
CMake warns, but does not fail, when local versions differ from these baselines.

`protoc` is required because Rust `prost-build` runs while building the C++
bindings. If you do not already have `protoc`, the repository helper can
download a pinned binary:

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
export PATH="$(dirname "$PROTOC_BIN"):$PATH"
```

## Build With CMake

The CMake build supports two dependency modes:

- `system`: use an existing Arrow C++ installation.
- `build`: fetch and build Arrow C++ from source.

### System Mode

Use `system` mode when your environment already provides Arrow C++ headers and
shared libraries.

```bash
cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-system \
  -DFLUSS_CPP_DEP_MODE=system \
  -DFLUSS_CPP_ARROW_SYSTEM_ROOT=/path/to/arrow/prefix
cmake --build /tmp/fluss-cpp-cmake-system --target fluss_cpp -j
```

Typical Arrow prefixes are `/usr`, `/usr/local`, or `/opt/arrow`.

### Build Mode

Use `build` mode when Arrow C++ is not preinstalled and CMake should provision
it for you.

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
export PATH="$(dirname "$PROTOC_BIN"):$PATH"

cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-build \
  -DFLUSS_CPP_DEP_MODE=build
cmake --build /tmp/fluss-cpp-cmake-build --target fluss_cpp -j
```

Optional overrides:

- `-DFLUSS_CPP_ARROW_VERSION=19.0.1`
- `-DFLUSS_CPP_ARROW_SOURCE_URL=...`
- `-DFLUSS_CPP_PROTOBUF_VERSION=3.25.5`

If your environment needs a proxy for CMake or FetchContent downloads, export
standard proxy variables before configure and build:

```bash
export http_proxy=http://host:port
export https_proxy=http://host:port
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
```

The first `build` mode run can take a while because Arrow C++ is compiled from
source.

## Integrate With Your Project

### CMake FetchContent

```cmake
include(FetchContent)

FetchContent_Declare(
    fluss-cpp
    GIT_REPOSITORY https://github.com/apache/fluss-rust.git
    SOURCE_SUBDIR bindings/cpp
)
FetchContent_MakeAvailable(fluss-cpp)

target_link_libraries(your_target PRIVATE fluss_cpp)
```

### CMake Subdirectory

```cmake
add_subdirectory(vendor/fluss-rust/bindings/cpp)
target_link_libraries(your_target PRIVATE fluss_cpp)
```

### Manual CMake Import

```cmake
find_package(Arrow REQUIRED)

add_library(fluss_cpp STATIC IMPORTED)
set_target_properties(fluss_cpp PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/lib/libfluss_cpp.a
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(your_target
    PRIVATE
    fluss_cpp
    Arrow::arrow_shared
    ${CMAKE_DL_LIBS}
    Threads::Threads
)

if(APPLE)
    target_link_libraries(your_target PRIVATE
        "-framework CoreFoundation"
        "-framework Security"
    )
endif()
```

## Bazel Users

The C++ bindings can also be consumed from Bazel using the `fluss_cpp` target.
See the repository-local Bazel guide at
[`docs/cpp-bazel-usage.md`](https://github.com/apache/fluss-rust/blob/main/docs/cpp-bazel-usage.md)
for system/build dependency modes, root module examples, and validation
commands.

## Troubleshooting

- `cargo not found`
  - Install Rust or set `CARGO=/path/to/cargo`.
- `protoc not found`
  - Install `protoc` and ensure it is in `PATH`.
  - For `build` mode, use `bindings/cpp/scripts/ensure_protoc.sh` and prepend
    the returned directory to `PATH`.
- `arrow/c/bridge.h` not found in build mode
  - Reconfigure after updating to the latest `bindings/cpp/CMakeLists.txt`;
    build mode adds Arrow source/build include directories during configure.
- Long first build in build mode
  - Expected. Arrow C++ source builds dominate the first build.
