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

defmodule Fluss.Admin do
  @moduledoc """
  Admin client for DDL operations (create/drop databases and tables).

  ## Examples

      admin = Fluss.Admin.new!(conn)
      :ok = Fluss.Admin.create_database(admin, "my_db")
      :ok = Fluss.Admin.create_table(admin, "my_db", "events", descriptor)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new(Fluss.Connection.t()) :: {:ok, t()} | {:error, String.t()}
  def new(conn) do
    case Native.admin_new(conn) do
      {:error, _} = err -> err
      admin -> {:ok, admin}
    end
  end

  @spec new!(Fluss.Connection.t()) :: t()
  def new!(conn) do
    case Native.admin_new(conn) do
      {:error, reason} -> raise "failed to create admin: #{reason}"
      admin -> admin
    end
  end

  @spec create_database(t(), String.t(), boolean()) :: :ok | {:error, String.t()}
  def create_database(admin, name, ignore_if_exists \\ true),
    do: Native.admin_create_database(admin, name, ignore_if_exists)

  @spec drop_database(t(), String.t(), boolean()) :: :ok | {:error, String.t()}
  def drop_database(admin, name, ignore_if_not_exists \\ true),
    do: Native.admin_drop_database(admin, name, ignore_if_not_exists)

  @spec list_databases(t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def list_databases(admin) do
    case Native.admin_list_databases(admin) do
      {:error, _} = err -> err
      dbs -> {:ok, dbs}
    end
  end

  @spec list_databases!(t()) :: [String.t()]
  def list_databases!(admin) do
    case Native.admin_list_databases(admin) do
      {:error, reason} -> raise "failed to list databases: #{reason}"
      dbs -> dbs
    end
  end

  @spec create_table(t(), String.t(), String.t(), Fluss.TableDescriptor.t(), boolean()) ::
          :ok | {:error, String.t()}
  def create_table(admin, database, table, descriptor, ignore_if_exists \\ true),
    do: Native.admin_create_table(admin, database, table, descriptor, ignore_if_exists)

  @spec drop_table(t(), String.t(), String.t(), boolean()) :: :ok | {:error, String.t()}
  def drop_table(admin, database, table, ignore_if_not_exists \\ true),
    do: Native.admin_drop_table(admin, database, table, ignore_if_not_exists)

  @spec list_tables(t(), String.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def list_tables(admin, database) do
    case Native.admin_list_tables(admin, database) do
      {:error, _} = err -> err
      tables -> {:ok, tables}
    end
  end

  @spec list_tables!(t(), String.t()) :: [String.t()]
  def list_tables!(admin, database) do
    case Native.admin_list_tables(admin, database) do
      {:error, reason} -> raise "failed to list tables: #{reason}"
      tables -> tables
    end
  end
end
