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

defmodule Fluss.Test.Cluster do
  @moduledoc false

  @fluss_image "apache/fluss"
  @fluss_version "0.9.0-incubating"

  @network_name "fluss-elixir-test-network"
  @zookeeper_name "zookeeper-elixir-test"
  @coordinator_name "coordinator-server-elixir-test"
  @tablet_server_name "tablet-server-elixir-test"

  # Same fixed ports used by Python/C++ integration tests.
  @coordinator_sasl_port 9123
  @coordinator_plain_port 9223
  @tablet_sasl_port 9124
  @tablet_plain_port 9224

  def bootstrap_servers, do: "127.0.0.1:#{@coordinator_plain_port}"

  def ensure_started do
    case System.get_env("FLUSS_BOOTSTRAP_SERVERS") do
      nil -> start_cluster()
      servers -> {:ok, servers}
    end
  end

  def stop do
    for name <- [@tablet_server_name, @coordinator_name, @zookeeper_name] do
      System.cmd("docker", ["rm", "-f", name], stderr_to_stdout: true)
    end

    System.cmd("docker", ["network", "rm", @network_name], stderr_to_stdout: true)
    :ok
  end

  defp start_cluster do
    if port_open?(@coordinator_plain_port) do
      IO.puts("Reusing existing Fluss cluster on port #{@coordinator_plain_port}")
      {:ok, bootstrap_servers()}
    else
      do_start_cluster()
    end
  end

  defp do_start_cluster do
    IO.puts("Starting Fluss cluster via Docker...")

    # Remove any leftover containers from previous runs
    for name <- [@tablet_server_name, @coordinator_name, @zookeeper_name] do
      System.cmd("docker", ["rm", "-f", name], stderr_to_stdout: true)
    end

    System.cmd("docker", ["network", "create", @network_name], stderr_to_stdout: true)

    sasl_jaas =
      ~s(org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-secret" user_alice="alice-secret";)

    coordinator_props =
      Enum.join(
        [
          "zookeeper.address: #{@zookeeper_name}:2181",
          "bind.listeners: INTERNAL://#{@coordinator_name}:0, CLIENT://#{@coordinator_name}:9123, PLAIN_CLIENT://#{@coordinator_name}:9223",
          "advertised.listeners: CLIENT://localhost:#{@coordinator_sasl_port}, PLAIN_CLIENT://localhost:#{@coordinator_plain_port}",
          "internal.listener.name: INTERNAL",
          "security.protocol.map: CLIENT:sasl",
          "security.sasl.enabled.mechanisms: plain",
          "security.sasl.plain.jaas.config: #{sasl_jaas}",
          "netty.server.num-network-threads: 1",
          "netty.server.num-worker-threads: 3"
        ],
        "\n"
      )

    tablet_props =
      Enum.join(
        [
          "zookeeper.address: #{@zookeeper_name}:2181",
          "bind.listeners: INTERNAL://#{@tablet_server_name}:0, CLIENT://#{@tablet_server_name}:9123, PLAIN_CLIENT://#{@tablet_server_name}:9223",
          "advertised.listeners: CLIENT://localhost:#{@tablet_sasl_port}, PLAIN_CLIENT://localhost:#{@tablet_plain_port}",
          "internal.listener.name: INTERNAL",
          "security.protocol.map: CLIENT:sasl",
          "security.sasl.enabled.mechanisms: plain",
          "security.sasl.plain.jaas.config: #{sasl_jaas}",
          "tablet-server.id: 0",
          "netty.server.num-network-threads: 1",
          "netty.server.num-worker-threads: 3"
        ],
        "\n"
      )

    docker_run([
      "--name",
      @zookeeper_name,
      "--network",
      @network_name,
      "-d",
      "zookeeper:3.9.2"
    ])

    docker_run([
      "--name",
      @coordinator_name,
      "--network",
      @network_name,
      "-p",
      "#{@coordinator_sasl_port}:9123",
      "-p",
      "#{@coordinator_plain_port}:9223",
      "-e",
      "FLUSS_PROPERTIES=#{coordinator_props}",
      "-d",
      "#{@fluss_image}:#{@fluss_version}",
      "coordinatorServer"
    ])

    docker_run([
      "--name",
      @tablet_server_name,
      "--network",
      @network_name,
      "-p",
      "#{@tablet_sasl_port}:9123",
      "-p",
      "#{@tablet_plain_port}:9223",
      "-e",
      "FLUSS_PROPERTIES=#{tablet_props}",
      "-d",
      "#{@fluss_image}:#{@fluss_version}",
      "tabletServer"
    ])

    all_ports = [@coordinator_plain_port, @tablet_plain_port]

    if wait_for_ports(all_ports, 90) do
      IO.puts("Fluss cluster started successfully.")
      {:ok, bootstrap_servers()}
    else
      {:error, "Cluster ports did not become ready within timeout"}
    end
  end

  defp docker_run(args) do
    {output, code} = System.cmd("docker", ["run" | args], stderr_to_stdout: true)

    if code != 0 do
      IO.puts("Docker run warning (code #{code}): #{output}")
    end
  end

  defp wait_for_ports(ports, timeout_s) do
    deadline = System.monotonic_time(:second) + timeout_s

    Enum.all?(ports, fn port ->
      remaining = deadline - System.monotonic_time(:second)
      remaining > 0 and wait_for_port(port, remaining)
    end)
  end

  defp wait_for_port(port, timeout_s) do
    deadline = System.monotonic_time(:second) + timeout_s

    Stream.repeatedly(fn ->
      case :gen_tcp.connect(~c"localhost", port, [], 1000) do
        {:ok, socket} ->
          :gen_tcp.close(socket)
          :ok

        {:error, _} ->
          Process.sleep(1000)
          :retry
      end
    end)
    |> Enum.reduce_while(false, fn
      :ok, _acc ->
        {:halt, true}

      :retry, _acc ->
        if System.monotonic_time(:second) >= deadline,
          do: {:halt, false},
          else: {:cont, false}
    end)
  end

  defp port_open?(port) do
    case :gen_tcp.connect(~c"localhost", port, [], 1000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        false
    end
  end
end
