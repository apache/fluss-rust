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

defmodule Fluss.Integration.AdminTest do
  use Fluss.Test.IntegrationCase, async: false

  @database "fluss"

  describe "get_server_nodes/1" do
    test "returns a non-empty list of %Fluss.ServerNode{} structs", %{admin: admin} do
      assert {:ok, nodes} = Fluss.Admin.get_server_nodes(admin)
      assert is_list(nodes)
      assert nodes != []

      for node <- nodes do
        assert %Fluss.ServerNode{} = node
        assert is_integer(node.id)
        assert is_binary(node.uid) and node.uid != ""
        assert is_binary(node.host) and node.host != ""
        assert is_integer(node.port) and node.port > 0
        assert node.server_type in [:coordinator_server, :tablet_server]
      end
    end

    test "cluster has exactly one coordinator", %{admin: admin} do
      {:ok, nodes} = Fluss.Admin.get_server_nodes(admin)
      coordinators = Enum.filter(nodes, &(&1.server_type == :coordinator_server))

      assert [_coordinator] = coordinators
    end
  end

  describe "get_server_nodes!/1" do
    test "returns the list directly without an :ok tuple", %{admin: admin} do
      nodes = Fluss.Admin.get_server_nodes!(admin)
      assert is_list(nodes)
      assert nodes != []
      assert %Fluss.ServerNode{} = hd(nodes)
    end
  end

  describe "get_database_info/2" do
    test "returns DatabaseInfo for an existing database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      :ok = Fluss.Admin.create_database(admin, db, true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert {:ok, %Fluss.DatabaseInfo{} = info} = Fluss.Admin.get_database_info(admin, db)
      assert info.database_name == db
      assert %Fluss.DatabaseDescriptor{} = info.descriptor
      assert is_integer(info.created_time)
      assert is_integer(info.modified_time)
    end

    test "returns error for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"
      assert {:error, %Fluss.Error{}} = Fluss.Admin.get_database_info(admin, db)
    end
  end

  describe "database_exists/2" do
    test "returns {:ok, true} for an existing database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      :ok = Fluss.Admin.create_database(admin, db, true)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert {:ok, true} = Fluss.Admin.database_exists(admin, db)
    end

    test "returns {:ok, false} for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      assert Fluss.Admin.database_exists(admin, db) == {:ok, false}
    end
  end

  describe "database_exists!/2" do
    test "returns true for an existing database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      :ok = Fluss.Admin.create_database(admin, db)
      on_exit(fn -> Fluss.Admin.drop_database(admin, db, true) end)

      assert Fluss.Admin.database_exists!(admin, db)
    end

    test "returns false for a non-existent database", %{admin: admin} do
      db = "fluss_data_sources_#{:rand.uniform(100_000)}"

      refute Fluss.Admin.database_exists!(admin, db)
    end
  end

  describe "table_exists/3" do
    test "returns {:ok, true} for an existing table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("c1", :int)
        |> Fluss.Schema.column("c2", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

      assert {:ok, true} = Fluss.Admin.table_exists(admin, @database, table)
    end

    test "returns {:ok, false} for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      assert {:ok, false} = Fluss.Admin.table_exists(admin, @database, table)
    end
  end

  describe "table_exists!/3" do
    test "returns true for an existing table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"

      schema =
        Fluss.Schema.new()
        |> Fluss.Schema.column("c1", :int)
        |> Fluss.Schema.column("c2", :string)

      descriptor = Fluss.TableDescriptor.new!(schema)
      :ok = Fluss.Admin.create_table(admin, @database, table, descriptor, true)
      on_exit(fn -> Fluss.Admin.drop_table(admin, @database, table, true) end)

      assert Fluss.Admin.table_exists!(admin, @database, table)
    end

    test "returns false for a non-existent table", %{admin: admin} do
      table = "fluss_table_#{:rand.uniform(100_000)}"
      refute Fluss.Admin.table_exists!(admin, @database, table)
    end
  end
end
