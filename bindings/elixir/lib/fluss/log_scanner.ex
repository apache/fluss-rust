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

defmodule Fluss.LogScanner do
  @moduledoc """
  Scanner for reading records from a log table.

  `poll/2` is non-blocking — it returns `:ok` immediately and sends results
  as `{:fluss_records, records}` or `{:fluss_poll_error, reason}` to the
  calling process. No dirty scheduler threads are held during the wait.

  Each record is an atom-keyed map: `:offset`, `:timestamp`, `:change_type`, `:row`.
  Row values are also atom-keyed (column names interned as atoms).

  ## Examples

      scanner = Fluss.LogScanner.new!(table)
      :ok = Fluss.LogScanner.subscribe(scanner, 0, Fluss.earliest_offset())
      :ok = Fluss.LogScanner.poll(scanner, 5_000)

      receive do
        {:fluss_records, records} ->
          for record <- records, do: IO.inspect(record[:row])
        {:fluss_poll_error, reason} ->
          IO.puts("poll error: \#{reason}")
      end

  """

  alias Fluss.Native

  @type t :: reference()
  @type record :: %{atom() => term()}

  @spec new(Fluss.Table.t()) :: {:ok, t()} | {:error, String.t()}
  def new(table) do
    case Native.log_scanner_new(table) do
      {:error, _} = err -> err
      s -> {:ok, s}
    end
  end

  @spec new!(Fluss.Table.t()) :: t()
  def new!(table) do
    case Native.log_scanner_new(table) do
      {:error, reason} -> raise "failed to create log scanner: #{reason}"
      s -> s
    end
  end

  @spec subscribe(t(), integer(), integer()) :: :ok | {:error, String.t()}
  def subscribe(scanner, bucket, offset),
    do: Native.log_scanner_subscribe(scanner, bucket, offset)

  @doc """
  Subscribes to multiple buckets. Takes a list of `{bucket_id, offset}` tuples.
  """
  @spec subscribe_buckets(t(), [{integer(), integer()}]) :: :ok | {:error, String.t()}
  def subscribe_buckets(scanner, bucket_offsets) when is_list(bucket_offsets),
    do: Native.log_scanner_subscribe_buckets(scanner, bucket_offsets)

  @spec unsubscribe(t(), integer()) :: :ok | {:error, String.t()}
  def unsubscribe(scanner, bucket),
    do: Native.log_scanner_unsubscribe(scanner, bucket)

  @doc """
  Starts a non-blocking poll. Returns `:ok` immediately.
  Results arrive as `{:fluss_records, [record]}` or `{:fluss_poll_error, reason}`.
  """
  @spec poll(t(), non_neg_integer()) :: :ok
  def poll(scanner, timeout_ms),
    do: Native.log_scanner_poll(scanner, timeout_ms)
end
