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

defmodule Fluss.ConfigTest do
  use ExUnit.Case, async: true

  test "new/1 creates config with bootstrap_servers; all other fields default to nil" do
    config = Fluss.Config.new("localhost:9123")
    assert config == %Fluss.Config{bootstrap_servers: "localhost:9123"}
  end

  test "set_writer_acks/2 sets the acks value" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_acks("all")

    assert config.writer_acks == "all"
  end

  test "set_writer_bucket_no_key_assigner/2 sets a valid assigner" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:sticky)

    assert config.writer_bucket_no_key_assigner == :sticky
  end

  test "set_writer_bucket_no_key_assigner/2 only accepts :sticky or :round_robin" do
    assert_raise FunctionClauseError, fn ->
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:custom)
    end
  end

  test "set_writer_buffer_memory_size/2 sets the buffer memory size" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_buffer_memory_size(67_108_864)

    assert config.writer_buffer_memory_size == 67_108_864
  end

  test "set_writer_buffer_wait_timeout_ms/2 sets the wait timeout" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_buffer_wait_timeout_ms(5_000)

    assert config.writer_buffer_wait_timeout_ms == 5_000
  end

  test "set_writer_enable_idempotence/2 sets the idempotence flag" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_enable_idempotence(false)

    assert config.writer_enable_idempotence == false
  end

  test "set_writer_max_inflight_requests_per_bucket/2 sets the inflight limit" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_max_inflight_requests_per_bucket(3)

    assert config.writer_max_inflight_requests_per_bucket == 3
  end

  test "set_writer_request_max_size/2 sets the request max size" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_request_max_size(2_097_152)

    assert config.writer_request_max_size == 2_097_152
  end

  test "set_writer_retries/2 sets the retry count" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_retries(5)

    assert config.writer_retries == 5
  end

  test "setters chain correctly" do
    config =
      Fluss.Config.new("localhost:9123")
      |> Fluss.Config.set_writer_acks("all")
      |> Fluss.Config.set_writer_retries(3)
      |> Fluss.Config.set_writer_bucket_no_key_assigner(:round_robin)

    assert config.writer_acks == "all"
    assert config.writer_retries == 3
    assert config.writer_bucket_no_key_assigner == :round_robin
  end
end
