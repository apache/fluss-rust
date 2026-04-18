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
            writer_batch_size: nil,
            writer_batch_timeout_ms: nil

  @type t :: %__MODULE__{
          bootstrap_servers: String.t(),
          writer_batch_size: non_neg_integer() | nil,
          writer_batch_timeout_ms: non_neg_integer() | nil
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

  @spec set_writer_batch_size(t(), non_neg_integer()) :: t()
  def set_writer_batch_size(%__MODULE__{} = config, size) when is_integer(size),
    do: %{config | writer_batch_size: size}

  @spec set_writer_batch_timeout_ms(t(), non_neg_integer()) :: t()
  def set_writer_batch_timeout_ms(%__MODULE__{} = config, ms) when is_integer(ms),
    do: %{config | writer_batch_timeout_ms: ms}

  @spec get_bootstrap_servers(t()) :: String.t()
  def get_bootstrap_servers(%__MODULE__{bootstrap_servers: servers}), do: servers
end
