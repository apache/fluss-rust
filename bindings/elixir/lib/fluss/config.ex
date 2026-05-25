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

defmodule Fluss.Config do
  @moduledoc """
  Client configuration for connecting to a Fluss cluster.

  Fields left as `nil` use the client's defaults.

  ## Examples

      config = Fluss.Config.new("localhost:9123")

      config =
        Fluss.Config.new("host1:9123,host2:9123")
        |> Fluss.Config.set_writer_batch_size(1_048_576)

  """

  @enforce_keys [:bootstrap_servers]
  defstruct bootstrap_servers: nil,
            writer_acks: nil,
            writer_batch_size: nil,
            writer_batch_timeout_ms: nil,
            writer_bucket_no_key_assigner: nil,
            writer_buffer_memory_size: nil,
            writer_buffer_wait_timeout_ms: nil,
            writer_dynamic_batch_size_enabled: nil,
            writer_dynamic_batch_size_min: nil,
            writer_enable_idempotence: nil,
            writer_max_inflight_requests_per_bucket: nil,
            writer_request_max_size: nil,
            writer_retries: nil

  @type t :: %__MODULE__{
          bootstrap_servers: String.t(),
          writer_acks: String.t() | nil,
          writer_batch_size: non_neg_integer() | nil,
          writer_batch_timeout_ms: non_neg_integer() | nil,
          writer_bucket_no_key_assigner: :sticky | :round_robin | nil,
          writer_buffer_memory_size: non_neg_integer() | nil,
          writer_buffer_wait_timeout_ms: non_neg_integer() | nil,
          writer_dynamic_batch_size_enabled: boolean() | nil,
          writer_dynamic_batch_size_min: non_neg_integer() | nil,
          writer_enable_idempotence: boolean() | nil,
          writer_max_inflight_requests_per_bucket: non_neg_integer() | nil,
          writer_request_max_size: non_neg_integer() | nil,
          writer_retries: non_neg_integer() | nil
        }

  @spec new(String.t()) :: t()
  def new(bootstrap_servers) when is_binary(bootstrap_servers) do
    %__MODULE__{bootstrap_servers: bootstrap_servers}
  end

  @spec default() :: t()
  def default, do: %__MODULE__{bootstrap_servers: ""}

  @spec set_bootstrap_servers(t(), String.t()) :: t()
  def set_bootstrap_servers(%__MODULE__{} = config, servers) when is_binary(servers),
    do: %{config | bootstrap_servers: servers}

  @spec set_writer_acks(t(), String.t()) :: t()
  def set_writer_acks(%__MODULE__{} = config, acks) when is_binary(acks),
    do: %{config | writer_acks: acks}

  @spec set_writer_batch_size(t(), non_neg_integer()) :: t()
  def set_writer_batch_size(%__MODULE__{} = config, size) when is_integer(size),
    do: %{config | writer_batch_size: size}

  @spec set_writer_batch_timeout_ms(t(), non_neg_integer()) :: t()
  def set_writer_batch_timeout_ms(%__MODULE__{} = config, ms) when is_integer(ms),
    do: %{config | writer_batch_timeout_ms: ms}

  @spec set_writer_bucket_no_key_assigner(t(), :sticky | :round_robin) :: t()
  def set_writer_bucket_no_key_assigner(%__MODULE__{} = config, assigner)
      when assigner in [:sticky, :round_robin],
      do: %{config | writer_bucket_no_key_assigner: assigner}

  @spec set_writer_buffer_memory_size(t(), non_neg_integer()) :: t()
  def set_writer_buffer_memory_size(%__MODULE__{} = config, size) when is_integer(size),
    do: %{config | writer_buffer_memory_size: size}

  @spec set_writer_buffer_wait_timeout_ms(t(), non_neg_integer()) :: t()
  def set_writer_buffer_wait_timeout_ms(%__MODULE__{} = config, ms) when is_integer(ms),
    do: %{config | writer_buffer_wait_timeout_ms: ms}

  @spec set_writer_dynamic_batch_size_enabled(t(), boolean()) :: t()
  def set_writer_dynamic_batch_size_enabled(%__MODULE__{} = config, enabled)
      when is_boolean(enabled),
      do: %{config | writer_dynamic_batch_size_enabled: enabled}

  @spec set_writer_dynamic_batch_size_min(t(), non_neg_integer()) :: t()
  def set_writer_dynamic_batch_size_min(%__MODULE__{} = config, size) when is_integer(size),
    do: %{config | writer_dynamic_batch_size_min: size}

  @spec set_writer_enable_idempotence(t(), boolean()) :: t()
  def set_writer_enable_idempotence(%__MODULE__{} = config, enabled)
      when is_boolean(enabled),
      do: %{config | writer_enable_idempotence: enabled}

  @spec set_writer_max_inflight_requests_per_bucket(t(), non_neg_integer()) :: t()
  def set_writer_max_inflight_requests_per_bucket(%__MODULE__{} = config, n)
      when is_integer(n),
      do: %{config | writer_max_inflight_requests_per_bucket: n}

  @spec set_writer_request_max_size(t(), non_neg_integer()) :: t()
  def set_writer_request_max_size(%__MODULE__{} = config, size) when is_integer(size),
    do: %{config | writer_request_max_size: size}

  @spec set_writer_retries(t(), non_neg_integer()) :: t()
  def set_writer_retries(%__MODULE__{} = config, n) when is_integer(n),
    do: %{config | writer_retries: n}

  @spec get_bootstrap_servers(t()) :: String.t()
  def get_bootstrap_servers(%__MODULE__{bootstrap_servers: servers}), do: servers
end
