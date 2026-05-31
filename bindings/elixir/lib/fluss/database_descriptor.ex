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

defmodule Fluss.DatabaseDescriptor do
  @moduledoc """
  User-supplied configuration of a Fluss database — its comment and any custom
  properties.

  Embedded as the `:descriptor` field of `Fluss.DatabaseInfo`; produced
  indirectly via `Fluss.Admin.get_database_info/2`.
  """

  @enforce_keys [:comment, :custom_properties]
  defstruct [:comment, :custom_properties]

  @type t :: %__MODULE__{
          comment: String.t() | nil,
          custom_properties: %{optional(String.t()) => String.t()}
        }
end
