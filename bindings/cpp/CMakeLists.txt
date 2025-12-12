# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.22)

if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

project(fluss-cpp LANGUAGES CXX)

include(FetchContent)
set(FLUSS_GOOGLETEST_VERSION 1.15.2 CACHE STRING "version of GoogleTest")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(FLUSS_ENABLE_ADDRESS_SANITIZER "Enable address sanitizer" OFF)
option(FLUSS_ENABLE_TESTING "Enable building test binary for fluss" OFF)
option(FLUSS_DEV "Enable dev mode" OFF)

if (FLUSS_DEV)
    set(FLUSS_ENABLE_ADDRESS_SANITIZER ON)
    set(FLUSS_ENABLE_TESTING ON)
endif()

# Get cargo target dir
execute_process(COMMAND cargo locate-project --workspace --message-format plain
    OUTPUT_VARIABLE CARGO_TARGET_DIR
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
string(REGEX REPLACE "/Cargo.toml\n$" "/target" CARGO_TARGET_DIR "${CARGO_TARGET_DIR}")

set(CARGO_MANIFEST ${PROJECT_SOURCE_DIR}/Cargo.toml)
set(RUST_SOURCE_FILE ${PROJECT_SOURCE_DIR}/src/lib.rs)
set(RUST_BRIDGE_CPP ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src/lib.rs.cc)
set(RUST_HEADER_FILE ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src/lib.rs.h)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_LIB ${CARGO_TARGET_DIR}/debug/${CMAKE_STATIC_LIBRARY_PREFIX}fluss_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(RUST_LIB ${CARGO_TARGET_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}fluss_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

set(CPP_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include
                    ${PROJECT_SOURCE_DIR}/src
                    ${CARGO_TARGET_DIR}/cxxbridge
                    ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src)

file(GLOB CPP_SOURCE_FILE "src/*.cpp")
file(GLOB CPP_HEADER_FILE "include/*.hpp")

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    list(APPEND CARGO_BUILD_FLAGS "--release")
endif()

add_custom_target(cargo_build
    COMMAND cargo build --manifest-path ${CARGO_MANIFEST} ${CARGO_BUILD_FLAGS}
    BYPRODUCTS ${RUST_BRIDGE_CPP} ${RUST_LIB} ${RUST_HEADER_FILE}
    DEPENDS ${RUST_SOURCE_FILE}
    USES_TERMINAL
    COMMENT "Running cargo..."
)

add_library(fluss_cpp STATIC ${CPP_SOURCE_FILE} ${RUST_BRIDGE_CPP})
target_sources(fluss_cpp PUBLIC ${CPP_HEADER_FILE})
target_sources(fluss_cpp PRIVATE ${RUST_HEADER_FILE})
target_include_directories(fluss_cpp PUBLIC ${CPP_INCLUDE_DIR})
target_link_libraries(fluss_cpp PUBLIC ${RUST_LIB})
target_link_libraries(fluss_cpp PRIVATE ${CMAKE_DL_LIBS} Threads::Threads)
if(APPLE)
    target_link_libraries(fluss_cpp PUBLIC "-framework CoreFoundation" "-framework Security")
endif()

add_executable(fluss_cpp_example examples/example.cpp)
target_link_libraries(fluss_cpp_example fluss_cpp)
target_include_directories(fluss_cpp_example PUBLIC ${CPP_INCLUDE_DIR})

set_target_properties(fluss_cpp
    PROPERTIES ADDITIONAL_CLEAN_FILES ${CARGO_TARGET_DIR}
)
add_dependencies(fluss_cpp cargo_build)

if (FLUSS_ENABLE_ADDRESS_SANITIZER)
    target_compile_options(fluss_cpp PRIVATE -fsanitize=leak,address,undefined -fno-omit-frame-pointer -fno-common -O1)
    target_link_options(fluss_cpp PRIVATE -fsanitize=leak,address,undefined)
endif()