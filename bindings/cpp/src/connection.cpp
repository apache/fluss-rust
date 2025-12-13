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
#include "rust/cxx.h"

namespace fluss {

Connection::Connection() noexcept = default;

Connection::~Connection() noexcept { Destroy(); }

void Connection::Destroy() noexcept {
    if (conn_) {
        ffi::delete_connection(conn_);
        conn_ = nullptr;
    }
}

Connection::Connection(Connection&& other) noexcept : conn_(other.conn_) {
    other.conn_ = nullptr;
}

Connection& Connection::operator=(Connection&& other) noexcept {
    if (this != &other) {
        Destroy();
        conn_ = other.conn_;
        other.conn_ = nullptr;
    }
    return *this;
}

Result Connection::Connect(const std::string& bootstrap_server, Connection& out) {
    try {
        out.conn_ = ffi::new_connection(bootstrap_server);
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

bool Connection::Available() const { return conn_ != nullptr; }

Result Connection::GetAdmin(Admin& out) {
    if (!Available()) {
        return utils::make_error(1, "Connection not available");
    }

    try {
        out.admin_ = conn_->get_admin();
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

Result Connection::GetTable(const TablePath& table_path, Table& out) {
    if (!Available()) {
        return utils::make_error(1, "Connection not available");
    }

    try {
        auto ffi_path = utils::to_ffi_table_path(table_path);
        out.table_ = conn_->get_table(ffi_path);
        return utils::make_ok();
    } catch (const rust::Error& e) {
        return utils::make_error(1, e.what());
    } catch (const std::exception& e) {
        return utils::make_error(1, e.what());
    }
}

}  // namespace fluss
