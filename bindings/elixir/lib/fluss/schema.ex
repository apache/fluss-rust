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

defmodule Fluss.Schema do
  @moduledoc """
  Schema builder for defining table columns and primary keys.

  Simple types: `:boolean`, `:tinyint`, `:smallint`, `:int`, `:bigint`,
  `:float`, `:double`, `:string`, `:bytes`, `:date`, `:time`, `:timestamp`, `:timestamp_ltz`

  Parameterized types: `{:decimal, precision, scale}`, `{:char, length}`, `{:binary, length}`

  ## Examples

      schema =
        Fluss.Schema.build()
        |> Fluss.Schema.column("id", :int)
        |> Fluss.Schema.column("name", :string)
        |> Fluss.Schema.column("amount", {:decimal, 10, 2})
        |> Fluss.Schema.build!()

  """

  alias Fluss.Native

  @type t :: reference()
  @type builder :: reference()

  @type data_type ::
          :boolean
          | :tinyint
          | :smallint
          | :int
          | :bigint
          | :float
          | :double
          | :string
          | :bytes
          | :date
          | :time
          | :timestamp
          | :timestamp_ltz
          | {:decimal, non_neg_integer(), non_neg_integer()}
          | {:char, non_neg_integer()}
          | {:binary, non_neg_integer()}

  @spec build() :: builder()
  def build, do: Native.schema_builder_new()

  @spec column(builder(), String.t(), data_type()) :: builder()
  def column(builder, name, data_type) do
    case Native.schema_builder_column(builder, name, data_type) do
      {:error, reason} -> raise "failed to add column: #{reason}"
      ref -> ref
    end
  end

  @spec primary_key(builder(), [String.t()]) :: builder()
  def primary_key(builder, keys) do
    case Native.schema_builder_primary_key(builder, keys) do
      {:error, reason} -> raise "failed to set primary key: #{reason}"
      ref -> ref
    end
  end

  @spec build!(builder()) :: t()
  def build!(builder) do
    case Native.schema_builder_build(builder) do
      {:error, reason} -> raise "failed to build schema: #{reason}"
      ref -> ref
    end
  end
end
