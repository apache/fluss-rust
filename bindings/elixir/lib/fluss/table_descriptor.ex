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

defmodule Fluss.TableDescriptor do
  @moduledoc """
  Descriptor for creating a Fluss table.

  Options: `:bucket_count`, `:properties` (list of `{key, value}` string tuples).

  ## Examples

      Fluss.TableDescriptor.new!(schema)
      Fluss.TableDescriptor.new!(schema, bucket_count: 3)

  """

  alias Fluss.Native

  @type t :: reference()

  @spec new!(Fluss.Schema.t(), keyword()) :: t()
  def new!(schema, opts \\ []) do
    result =
      cond do
        Keyword.has_key?(opts, :bucket_count) ->
          Native.table_descriptor_with_bucket_count(schema, opts[:bucket_count])

        Keyword.has_key?(opts, :properties) ->
          Native.table_descriptor_with_properties(schema, opts[:properties])

        true ->
          Native.table_descriptor_new(schema)
      end

    case result do
      {:error, reason} -> raise "failed to create table descriptor: #{reason}"
      ref -> ref
    end
  end
end
