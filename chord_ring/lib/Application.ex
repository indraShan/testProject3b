defmodule Chord.Application do
  use GenServer

  def start_link(caller, args) do
    GenServer.start_link(
      __MODULE__,
      [caller: caller, args: args],
      []
    )
  end

  def init(opts) do
    # create nodes
    number_of_nodes = String.to_integer(Enum.at(opts[:args], 0))
    # Topology ignored for now.
    number_of_requests = Enum.at(opts[:args], 1)

    IO.puts("number_of_nodes = #{number_of_nodes}")
    IO.puts("number_of_requests = #{number_of_requests}")

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
      active_nodes_count: 0,
      total_hops_made: 0
    }

    GenServer.cast(self(), {:create_network})

    {:ok, state}
  end

  def handle_cast({:create_network}, state) do
    IO.puts("Creating network")
    n = state |> Map.get(:number_of_nodes)
    m = (:math.log(n) |> round) + 16

    # We would be using this node to connect other nodes to the network.
    ring_id = hash_for_node_at_index(1, m)
    {:ok, ring_node} = Chord.Node.start_link(m, ring_id)
    Chord.Node.create_ring(ring_node)

    map = %{ring_node => 0}

    map =
      elem(
        Enum.map_reduce(2..n, map, fn x, acc ->
          {x,
           acc
           |> Map.put(
             elem(Chord.Node.start_link(m, hash_for_node_at_index(x, m), ring_node), 1),
             0
           )}
        end),
        1
      )

    IO.inspect(map)
    state = state |> Map.put(:nodes, map)
    state = state |> Map.put(:active_nodes_count, n)
    {:noreply, state}
  end

  defp hash_for_node_at_index(index, m) do
    rem(index * (index + 3), :math.pow(2, m) |> round)
  end
end
