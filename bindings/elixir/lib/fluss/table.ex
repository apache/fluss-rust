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

defmodule Fluss.Table do
  @moduledoc """
  A handle to a Fluss table, used to create writers and scanners.
  """

  alias Fluss.Native

  @type t :: reference()

  @spec get(Fluss.Connection.t(), String.t(), String.t()) :: {:ok, t()} | {:error, String.t()}
  def get(conn, database, table) do
    case Native.table_get(conn, database, table) do
      {:error, _} = err -> err
      t -> {:ok, t}
    end
  end

  @spec get!(Fluss.Connection.t(), String.t(), String.t()) :: t()
  def get!(conn, database, table) do
    case Native.table_get(conn, database, table) do
      {:error, reason} -> raise "failed to get table: #{reason}"
      t -> t
    end
  end

  @spec has_primary_key?(t()) :: boolean()
  def has_primary_key?(table), do: Native.table_has_primary_key(table)

  @spec column_names(t()) :: [String.t()]
  def column_names(table), do: Native.table_column_names(table)
end
