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

  ## Examples

      config = Fluss.Config.new("localhost:9123")

      config =
        Fluss.Config.default()
        |> Fluss.Config.set_bootstrap_servers("host1:9123,host2:9123")
        |> Fluss.Config.set_writer_batch_size(1_048_576)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new(String.t()) :: t()
  def new(bootstrap_servers) when is_binary(bootstrap_servers) do
    Native.config_new(bootstrap_servers)
  end

  @spec default() :: t()
  def default, do: Native.config_default()

  @spec set_bootstrap_servers(t(), String.t()) :: t()
  def set_bootstrap_servers(config, servers),
    do: Native.config_set_bootstrap_servers(config, servers)

  @spec set_writer_batch_size(t(), integer()) :: t()
  def set_writer_batch_size(config, size), do: Native.config_set_writer_batch_size(config, size)

  @spec set_writer_batch_timeout_ms(t(), integer()) :: t()
  def set_writer_batch_timeout_ms(config, ms),
    do: Native.config_set_writer_batch_timeout_ms(config, ms)

  @spec get_bootstrap_servers(t()) :: String.t()
  def get_bootstrap_servers(config), do: Native.config_get_bootstrap_servers(config)
end
