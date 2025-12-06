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

#include "fluss.hpp"
#include "lib.rs.h"
#include "ffi_converter.hpp"

namespace fluss {

// Admin implementation
Admin::Admin() noexcept = default;

Admin::Admin(ffi::Admin* admin) noexcept : admin_(admin) {}

Admin::~Admin() noexcept { Destroy(); }

void Admin::Destroy() noexcept {
    if (admin_) {
        ffi::delete_admin(admin_);
        admin_ = nullptr;
    }
}

Admin::Admin(Admin&& other) noexcept : admin_(other.admin_) {
    other.admin_ = nullptr;
}

Admin& Admin::operator=(Admin&& other) noexcept {
    if (this != &other) {
        Destroy();
        admin_ = other.admin_;
        other.admin_ = nullptr;
    }
    return *this;
}

bool Admin::Available() const { return admin_ != nullptr; }

ErrorCode Admin::CreateTable(const TablePath& table_path,
                             const TableDescriptor& descriptor,
                             bool ignore_if_exists) {
    if (!Available()) {
        return ErrorCode::AdminNotAvailable;
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_desc = utils::to_ffi_table_descriptor(descriptor);

    auto ffi_result = admin_->create_table(ffi_path, ffi_desc, ignore_if_exists);
    if (ffi_result.success) {
        return ErrorCode::Ok;
    }
    return ErrorCode::OperationFailed;
}

ErrorCode Admin::GetTable(const TablePath& table_path, TableInfo& out) {
    if (!Available()) {
        return ErrorCode::AdminNotAvailable;
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->get_table_info(ffi_path);

    if (!ffi_result.result.success) {
        return ErrorCode::OperationFailed;
    }

    out = utils::from_ffi_table_info(ffi_result.table_info);
    return ErrorCode::Ok;
}

ErrorCode Admin::GetLatestLakeSnapshot(const TablePath& table_path, LakeSnapshot& out) {
    if (!Available()) {
        return ErrorCode::AdminNotAvailable;
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->get_latest_lake_snapshot(ffi_path);

    if (!ffi_result.result.success) {
        return ErrorCode::OperationFailed;
    }

    out = utils::from_ffi_lake_snapshot(ffi_result.lake_snapshot);
    return ErrorCode::Ok;
}

}  // namespace fluss
