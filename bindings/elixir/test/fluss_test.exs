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

defmodule FlussTest do
  use ExUnit.Case

  describe "Config" do
    test "creates config with bootstrap servers" do
      config = Fluss.Config.new("localhost:9123")
      assert Fluss.Config.get_bootstrap_servers(config) == "localhost:9123"
    end

    test "default config uses localhost" do
      config = Fluss.Config.default()
      assert Fluss.Config.get_bootstrap_servers(config) == "127.0.0.1:9123"
    end

    test "config chaining" do
      config =
        Fluss.Config.default()
        |> Fluss.Config.set_bootstrap_servers("host1:9123,host2:9123")

      assert Fluss.Config.get_bootstrap_servers(config) == "host1:9123,host2:9123"
    end
  end

  describe "Schema" do
    test "builds a simple log table schema" do
      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("ts", :bigint)
        |> Fluss.Schema.column("message", :string)
        |> Fluss.Schema.build!()

      assert is_reference(schema)
    end

    test "builds a schema with all simple types" do
      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("a", :boolean)
        |> Fluss.Schema.column("b", :tinyint)
        |> Fluss.Schema.column("c", :smallint)
        |> Fluss.Schema.column("d", :int)
        |> Fluss.Schema.column("e", :bigint)
        |> Fluss.Schema.column("f", :float)
        |> Fluss.Schema.column("g", :double)
        |> Fluss.Schema.column("h", :string)
        |> Fluss.Schema.column("i", :bytes)
        |> Fluss.Schema.column("j", :date)
        |> Fluss.Schema.column("k", :time)
        |> Fluss.Schema.column("l", :timestamp)
        |> Fluss.Schema.column("m", :timestamp_ltz)
        |> Fluss.Schema.build!()

      assert is_reference(schema)
    end

    test "builds a schema with parameterized types" do
      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("amount", {:decimal, 10, 2})
        |> Fluss.Schema.column("code", {:char, 5})
        |> Fluss.Schema.column("data", {:binary, 16})
        |> Fluss.Schema.build!()

      assert is_reference(schema)
    end
  end

  describe "TableDescriptor" do
    test "creates descriptor from schema" do
      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.build!()

      descriptor = Fluss.TableDescriptor.new!(schema)
      assert is_reference(descriptor)
    end

    test "creates descriptor with bucket count" do
      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.build!()

      descriptor = Fluss.TableDescriptor.new!(schema, bucket_count: 3)
      assert is_reference(descriptor)
    end
  end

  describe "earliest_offset/0" do
    test "returns -2" do
      assert Fluss.earliest_offset() == -2
    end
  end
end
