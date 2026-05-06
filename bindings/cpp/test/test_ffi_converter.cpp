/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>
#include <stdexcept>

#include "ffi_converter.hpp"

namespace {

fluss::ffi::FfiColumn MakeArrayColumn(int32_t nesting, int32_t element_type) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("bad_array");
    col.data_type = static_cast<int32_t>(fluss::TypeId::Array);
    col.comment = rust::String("");
    col.precision = 0;
    col.scale = 0;
    col.array_nesting = nesting;
    col.element_data_type = element_type;
    col.element_precision = 0;
    col.element_scale = 0;
    return col;
}

}  // namespace

TEST(FfiConverterTest, RejectsArrayWithoutElementType) {
    auto col = MakeArrayColumn(1, 0);
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsArrayWithArrayLeafType) {
    auto col = MakeArrayColumn(2, static_cast<int32_t>(fluss::TypeId::Array));
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsArrayWithUnknownLeafType) {
    auto col = MakeArrayColumn(1, 999);
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, SupportsLegacyOneLevelArrayMetadata) {
    auto col = MakeArrayColumn(0, static_cast<int32_t>(fluss::TypeId::Int));
    auto converted = fluss::utils::from_ffi_column(col);
    EXPECT_EQ(converted.data_type.id(), fluss::TypeId::Array);
    ASSERT_NE(converted.data_type.element_type(), nullptr);
    EXPECT_EQ(converted.data_type.element_type()->id(), fluss::TypeId::Int);
}
