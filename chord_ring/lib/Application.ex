defmodule Chord.Application do
  @kill_interval 500
  @kill_count_threshold 10
  use GenServer

  def start_link(caller, args) do
    GenServer.start_link(
      __MODULE__,
      [caller: caller, args: args],
      []
    )
  end

  def init(opts) do
    # number of nodes
    number_of_nodes = String.to_integer(Enum.at(opts[:args], 0))
    # number of requests
    number_of_requests = String.to_integer(Enum.at(opts[:args], 1))
    # IO.puts(length(opts[:args]))
    kill_ratio =
      if length(opts[:args]) > 2 do
        String.to_integer(Enum.at(opts[:args], 2)) / 100
      else
        0
      end

    # IO.puts("number_of_nodes = #{number_of_nodes}")
    # IO.puts("number_of_requests = #{number_of_requests}")

    # node is a map of pid => Number of queries asked till now.
    # Once a node has asked required number of queries, stop it and
    # remove from this map.
    # When the map count reaches 0, we are done.
    # total_hops_made is the total hops made till now to answer all
    # the queries asked by the application. We can divide this number
    # by the number_of_requests number to get the average hops made.
    state = %{
      number_of_nodes: number_of_nodes,
      number_of_requests: number_of_requests,
      nodes: %{},
      caller: opts[:caller],
      total_hops_made: 0,
      kill_ratio: kill_ratio,
      nodes_killed: 0
    }

    GenServer.cast(self(), {:create_network})

    if kill_ratio != 0 do
      schedule_kill_timer()
    end

    {:ok, state}
  end

  def handle_cast({:create_network}, state) do
    # IO.puts("Creating network")
    n = state |> Map.get(:number_of_nodes)
    m = (:math.log2(n) |> round) + 3

    # We would be using this node to connect other nodes to the network.
    ring_id = hash_for_node_at_index(0, m)
    {:ok, ring_node} = Chord.Node.start_link(m, ring_id, self())
    Chord.Node.create_ring(ring_node)

    map = %{ring_node => 0}

    map =
      elem(
        Enum.map_reduce(1..(n - 1), map, fn x, acc ->
          {x,
           acc
           |> Map.put(
             elem(Chord.Node.start_link(m, hash_for_node_at_index(x, m), ring_node, self()), 1),
             0
           )}
        end),
        1
      )

    # IO.inspect(map)
    state = state |> Map.put(:nodes, map)
    {:noreply, state}
  end

  def handle_cast({:query_answered, source_pid, hops}, state) do
    nodes = state |> Map.get(:nodes)

    updated_state =
      if nodes |> Map.has_key?(source_pid) do
        current_answer_count = nodes |> Map.get(source_pid)
        # IO.puts("A node just finished its job")
        # Update nodes to reflect the new count.
        nodes = nodes |> Map.put(source_pid, current_answer_count + 1)
        state = state |> Map.put(:nodes, nodes)

        # Update the total hops till now.
        current_hops = state |> Map.get(:total_hops_made)
        state = state |> Map.put(:total_hops_made, current_hops + hops)

        number_of_requests = state |> Map.get(:number_of_requests)

        deleted_nodes_state =
          if current_answer_count + 1 >= number_of_requests do
            # IO.puts("Deleting a node that reached its potential")
            # Remove the node from the map.
            current_nodes = state |> Map.get(:nodes)
            current_nodes = current_nodes |> Map.delete(source_pid)
            # Tell the node to shut up.
            Chord.Node.stop_talking(source_pid)
            state |> Map.put(:nodes, current_nodes)
          else
            state
          end

        remaining_nodes_count = deleted_nodes_state |> Map.get(:nodes) |> Map.keys() |> length

        if remaining_nodes_count == 0 do
          printResult(deleted_nodes_state)
        end

        deleted_nodes_state
      else
        # The node is already terminated. Dont need to do anything.
        state
      end

    {:noreply, updated_state}
  end

  defp printResult(state) do
    number_of_nodes = state |> Map.get(:number_of_nodes)
    number_of_requests = state |> Map.get(:number_of_requests)
    kill_ratio = state |> Map.get(:kill_ratio)
    nodes_killed = state |> Map.get(:nodes_killed)

    number_of_nodes =
      if kill_ratio != 0 do
        number_of_nodes - nodes_killed
      else
        number_of_nodes
      end

    average_hops = (state |> Map.get(:total_hops_made)) / (number_of_requests * number_of_nodes)

    IO.puts("Average hops = #{average_hops}")
    send(state.caller, {:terminate})
  end

  def handle_info({:kill_a_node}, state) do
    number_of_nodes = state |> Map.get(:number_of_nodes)
    kill_ratio = state |> Map.get(:kill_ratio)
    nodes_killed_till_now = state |> Map.get(:nodes_killed)
    nodes_alive_map = state |> Map.get(:nodes)
    nodes_alive_count = nodes_alive_map |> Map.keys() |> length
    total_nodes_to_kill = number_of_nodes * kill_ratio

    updated_state =
      if nodes_alive_count > @kill_count_threshold and nodes_killed_till_now < total_nodes_to_kill do
        # We are going to kill a node.
        # If this is the second last alive_node, we should stop the
        # app, as the remaining node will not be answer any query
        # other than its own.

        # IO.puts("A node will die.")
        keys = nodes_alive_map |> Map.keys()
        random = Enum.random(0..(length(keys) - 1))
        kill_key = keys |> Enum.at(random)
        Chord.Node.simulate_failure(kill_key)
        # Remove the node from map
        nodes_alive_map = nodes_alive_map |> Map.delete(kill_key)
        # Schedule time for next kill
        schedule_kill_timer()
        state = state |> Map.put(:nodes_killed, nodes_killed_till_now + 1)
        state |> Map.put(:nodes, nodes_alive_map)
      else
        state
      end

    {:noreply, updated_state}
  end

  defp schedule_kill_timer() do
    Process.send_after(self(), {:kill_a_node}, @kill_interval)
  end

  defp hash_for_node_at_index(index, m) do
    rem(index * (index + 3), :math.pow(2, m) |> round)
  end
end
