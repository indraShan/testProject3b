# TODO: Check predecessor testing
# TODO: Send message. Count hops. Let the app know.
# TODO: When to stop stabilize flow?
# TODO: From where to restart the timer?
defmodule Chord.Node do
  @stabilize_interval 50
  @fix_fingers_interval 100
  @check_predecessor_interval 100
  @query_interval 1000

  # Public methods
  def start_link(m, id, application) do
    start_link(m, id, nil, application)
  end

  def simulate_failure(pid) do
    Process.send_after(pid, {:you_will_die}, 0)
  end

  def stop_talking(pid) do
    GenServer.cast(pid, {:stop_talking})
  end

  def start_link(m, id, existing_node, application) do
    GenServer.start_link(
      __MODULE__,
      [m: m, id: id, existing_node: existing_node, application: application],
      []
    )
  end

  def print_state(pid) do
    GenServer.cast(pid, {:print_state})
  end

  # Sets the ID (integer) of the node. This gets used by the protocol
  # algorithm to route queries and chose successors
  def set_ID(pid, id) do
    GenServer.cast(pid, {:set_ID, id})
  end

  def create_ring(pid) do
    GenServer.cast(pid, {:create_ring})
  end

  def joinNode(pid, existing_pid) do
    # Ask the node with pid to join the existing node with pid existing_pid
    GenServer.cast(pid, {:join_node, existing_pid})
  end

  # TODO: Should be private?
  # def fix_fingers(pid) do
  #   GenServer.cast(pid, {:fix_fingers})
  # end

  # Private methods after this point.
  def init(opts) do
    max_hash_value = (:math.pow(2, opts[:m]) |> round) - 1

    state = %{
      m: opts[:m],
      predecessor: nil,
      max_hash_value: max_hash_value,
      next: 0,
      ID: opts[:id],
      stop_asking_queries: false,
      application: opts[:application],
      node_has_asked_enough_queries: false,
      who_is_predecessor_attemps: 0
    }

    if opts[:existing_node] != nil do
      Chord.Node.joinNode(self(), opts[:existing_node])
    end

    # Start stabilize flow.
    schedule_stabilize_timer()
    # Start fix fingers flow
    schedule_fix_fingers_timer()
    # Start fix fingers flow
    schedule_check_predecessor_timer()

    # Start the query timer
    schedule_query_timer()
    {:ok, state}
  end

  defp schedule_query_timer() do
    Process.send_after(self(), {:ask_someone_something}, @query_interval)
  end

  defp schedule_check_predecessor_timer() do
    Process.send_after(self(), {:check_predecessor}, @check_predecessor_interval)
  end

  defp schedule_fix_fingers_timer() do
    Process.send_after(self(), {:fix_fingers}, @fix_fingers_interval)
  end

  defp schedule_stabilize_timer() do
    Process.send_after(self(), {:stabilize}, @stabilize_interval)
  end

  defp get_ID(state) do
    Map.get(state, :ID)
  end

  def key_inbetween(id, key1, key2, max_hash_value) do
    cond do
      key1 < key2 ->
        # id falls between key1 and key2?
        if id > key1 and id < key2 do
          true
        else
          false
        end

      key1 > key2 ->
        if (id >= 0 and id < key2) or (id > key1 and id <= max_hash_value) do
          true
        else
          false
        end

      true ->
        if id != key2 and id != key1 do
          true
        else
          false
        end
    end
  end

  def key_inbetween_inclusive_right(id, key1, key2, max_hash_value) do
    cond do
      key1 < key2 ->
        # id falls between key1 and key2?
        if id > key1 and id <= key2 do
          true
        else
          false
        end

      key1 > key2 ->
        if (id >= 0 and id <= key2) or (id > key1 and id <= max_hash_value) do
          true
        else
          false
        end

      true ->
        if id != key1 do
          true
        else
          false
        end
    end
  end

  # Returns a list.
  # 0th element is the Hash ID of the process
  # 1st element is the process ID of the process.
  defp get_successor(state) do
    if state |> Map.has_key?(:successor_map) do
      successor_map = state |> Map.get(:successor_map)
      first = 1
      successor_map |> Map.get(first)
    else
      nil
    end
  end

  # Returns a list or nil
  # 0th element is the Hash ID of the process
  # 1st element is the process ID of the process.
  # 2nd element is the number of times we have tried to ping it
  # and haven't heard back.
  defp get_predecessor(state) do
    state |> Map.get(:predecessor)
  end

  # defp print_id(state) do
  #   if state |> Map.has_key?(:ID) do
  #     IO.puts("ID = #{get_ID(state)}")
  #   else
  #     IO.puts("No ID set yet!")
  #   end
  # end
  #
  # defp print_successor(state) do
  #   if state |> Map.has_key?(:successor_map) do
  #     IO.inspect(get_successor(state))
  #   else
  #     IO.puts("No successor set yet!")
  #   end
  # end

  # Send a message to querySourcePid, so that it knows about its
  # successor_id and successor_pid
  defp notify_successor(successor_id, successor_pid, finger_index, id, querySourcePid) do
    # Hack?
    # If the successor_pid node does not have a successor yet, it should set querySourcePid
    # as its successor.
    GenServer.cast(
      successor_pid,
      {:set_successor_if_you_dont_have_one, 1, id, querySourcePid}
    )

    # Let the query node know that we have a successor for it.
    GenServer.cast(
      querySourcePid,
      {:set_finger_table_entry, finger_index, successor_id, successor_pid}
    )
  end

  defp closest_preceding_node(id, state) do
    # n.closest preceding node(id)
    # for i = m downto 1
    #  if (finger[i] ∈ (n, id))
    #   return finger[i];
    # return n;

    # TODO: Efficiency. Using Map might be slower here.
    successor_map = state |> Map.get(:successor_map)
    successor_keys = successor_map |> Map.keys()
    # Descending sort
    successor_sorted_keys = Enum.sort(successor_keys, &(&1 >= &2))

    node_ID = get_ID(state)
    max_hash_value = state |> Map.get(:max_hash_value)

    matches =
      Enum.filter(successor_sorted_keys, fn key ->
        key_inbetween(successor_map |> Map.get(key) |> Enum.at(0), node_ID, id, max_hash_value)
      end)

    if length(matches) != 0 do
      matching_key = Enum.at(matches, 0)
      successor = successor_map |> Map.get(matching_key)
      Enum.at(successor, 1)
    else
      self()
    end
  end

  defp ask_query(query, state, hops, source_pid) do
    successor = get_successor(state)
    successor_id = Enum.at(successor, 0)
    max_hash_value = state |> Map.get(:max_hash_value)
    application = state |> Map.get(:application)
    # Passed-in id falls within this and successor
    if key_inbetween_inclusive_right(query, get_ID(state), successor_id, max_hash_value) do
      # successor_pid should have the answer for current query.
      # Let the app, know the query was answered.
      GenServer.cast(application, {:query_answered, source_pid, hops + 1})
    else
      closest_successor_pid = closest_preceding_node(query, state)

      if closest_successor_pid == self() do
        GenServer.cast(application, {:query_answered, source_pid, hops})
      else
        # Its someone else's headache
        GenServer.cast(
          closest_successor_pid,
          {:find_successor_for_answering, query, hops + 1, source_pid}
        )
      end
    end
  end

  # id: HashId that should be searched for a successor.
  # state: self's current state
  # querySourcePid: the pid of node that is asking for a successor.
  defp find_successor(id, state, finger_index, querySourcePid) do
    successor = get_successor(state)
    successor_id = Enum.at(successor, 0)
    successor_pid = Enum.at(successor, 1)
    max_hash_value = state |> Map.get(:max_hash_value)
    # Passed-in id falls within this and successor
    if key_inbetween_inclusive_right(id, get_ID(state), successor_id, max_hash_value) do
      # successor_pid should be the successor of node 'id'
      notify_successor(successor_id, successor_pid, finger_index, id, querySourcePid)
    else
      closest_successor_pid = closest_preceding_node(id, state)

      if closest_successor_pid == self() do
        notify_successor(get_ID(state), self(), finger_index, id, querySourcePid)
      else
        # Its someone else's headache
        GenServer.cast(
          closest_successor_pid,
          {:find_successor_for_node, finger_index, id, querySourcePid}
        )
      end
    end

    # n.find successor(id)
    # if (id ∈ (n, successor]) return successor;
    # else
    # n′ = closest preceding node(id);
    # return n′.find successor(id);
    #
    #
  end

  # handle_*
  def handle_cast({:create_ring}, state) do
    # IO.puts("Create ring called. ID = #{get_ID(state)}")
    # Make self the first successor. predecessor stays nil.
    # successor_map is a map of integer => [id, pid]
    # [id, pid] => Hash ID of the othe process, pid is the processId of that
    # process.
    successor_map = %{1 => [get_ID(state), self()]}
    {:noreply, state |> Map.put(:successor_map, successor_map)}
  end

  def handle_cast({:set_ID, id}, state) do
    {:noreply, state |> Map.put(:ID, id)}
  end

  def handle_cast({:print_state}, state) do
    # print_id(state)
    # print_successor(state)
    # IO.puts("~~~STATE~~~~")
    # IO.inspect(state)
    node_Id = get_ID(state)
    successor = get_successor(state)

    successor =
      if successor == nil do
        ""
      else
        Enum.at(successor, 0)
      end

    predecessor = get_predecessor(state)

    predecessor =
      if predecessor == nil do
        ""
      else
        Enum.at(predecessor, 0)
      end

    printable_map =
      if state |> Map.has_key?(:successor_map) do
        successor_map = state |> Map.get(:successor_map)
        keys = successor_map |> Map.keys()

        Enum.map(keys, fn key ->
          string_key = key |> Integer.to_string()
          string_value = successor_map |> Map.get(key) |> Enum.at(0) |> Integer.to_string()
          string_key <> " -> " <> string_value <> ", "
        end)
      else
        ""
      end

    IO.puts(
      "ID = #{node_Id}, successor = #{successor}, predecessor = #{predecessor}, successor_map = #{
        printable_map
      }"
    )

    {:noreply, state}
  end

  def handle_cast({:who_is_your_predecessor, askingPid}, state) do
    # x = successor.predecessor;
    #   if (x ∈ (n, successor))
    #     successor = x;
    #
    # successor.notify(n);
    predecessor = get_predecessor(state)

    if predecessor != nil do
      predecessor_id = Enum.at(predecessor, 0)
      predecessor_pid = Enum.at(predecessor, 1)
      GenServer.cast(askingPid, {:predecessor_response, predecessor_id, predecessor_pid, self()})
    else
      GenServer.cast(askingPid, {:predecessor_response, nil, nil, self()})
    end

    {:noreply, state}
  end

  def handle_cast({:predecessor_response, predecessor_id, predecessor_pid, successor_pid}, state) do
    # x = successor.predecessor;
    #   if (x ∈ (n, successor))
    #     successor = x;
    #
    # successor.notify(n);

    # reset the attempt counter to 0.
    state = state |> Map.put(:who_is_predecessor_attemps, 0)
    # Make sure that the responding node is still the successor of this node.
    successor = get_successor(state)
    current_successor_id = Enum.at(successor, 0)
    current_successor_pid = Enum.at(successor, 1)
    # If the successor is unchanged and
    # the successor's predecessor is not nil
    # and the predecessor falls within current nodes ID and the successors ID
    max_hash_value = state |> Map.get(:max_hash_value)

    new_successor =
      if current_successor_pid == successor_pid and predecessor_pid != nil and
           key_inbetween_inclusive_right(
             predecessor_id,
             get_ID(state),
             current_successor_id,
             max_hash_value
           ) do
        # We have a new successor
        [predecessor_id, predecessor_pid]
      else
        # successor remains unchanged.
        successor
      end

    successor_map = state |> Map.get(:successor_map)
    first = 1
    updated_successor_map = successor_map |> Map.put(first, new_successor)

    # Notify the successor that 'this' node should be its predecessor
    updated_successor_pid = Enum.at(new_successor, 1)
    GenServer.cast(updated_successor_pid, {:notify, get_ID(state), self()})

    # Update the state to include new successor
    {:noreply, state |> Map.put(:successor_map, updated_successor_map)}
  end

  # Gets called at the end of stabilize flow.
  def handle_cast({:notify, predecessor_id, predecessor_pid}, state) do
    # if (predecessor is nil or n′ ∈ (predecessor, n))
    #  predecessor = n′;
    current_predecessor = get_predecessor(state)
    max_hash_value = state |> Map.get(:max_hash_value)

    updated_predecessor =
      cond do
        predecessor_pid == self() ->
          # Message from self, do nothing.
          current_predecessor

        current_predecessor == nil ->
          # Current predecessor is nil. So now we have a new predecessor.
          [predecessor_id, predecessor_pid, 0]

        true ->
          current_predecessor_id = Enum.at(current_predecessor, 0)

          if key_inbetween_inclusive_right(
               predecessor_id,
               current_predecessor_id,
               get_ID(state),
               max_hash_value
             ) do
            # New predecessor is closer to this node than previous predecessor
            [predecessor_id, predecessor_pid, 0]
          else
            # Old predecessor stays.
            current_predecessor
          end
      end

    # Update the state.
    {:noreply, state |> Map.put(:predecessor, updated_predecessor)}
  end

  def next_valid_successor(state) do
    successor = get_successor(state)
    current_successor_ID = Enum.at(successor, 0)

    successor_map = state |> Map.get(:successor_map)
    successor_keys = successor_map |> Map.keys()
    # Ascending sort
    successor_sorted_keys = Enum.sort(successor_keys)

    matches =
      Enum.filter(successor_sorted_keys, fn key ->
        successor_map |> Map.get(key) |> Enum.at(0) != current_successor_ID
      end)

    if length(matches) != 0 do
      matches |> Enum.at(0)
    else
      -1
    end
  end

  def check_predecessor(state) do
    successor = get_successor(state)
    attempts = state |> Map.get(:who_is_predecessor_attemps)

    if attempts <= -3 do
      # Current successor has failed. From the finger table
      # Get the next valid successor
      next_successor_key = next_valid_successor(state)

      if next_successor_key != -1 do
        successor_map = state |> Map.get(:successor_map)
        valid_successor = successor_map |> Map.get(next_successor_key)

        map =
          elem(
            Enum.map_reduce(1..next_successor_key, successor_map, fn x, acc ->
              {x,
               acc
               |> Map.put(x, valid_successor)}
            end),
            1
          )

        successor_map = map
        state = state |> Map.put(:successor_map, successor_map)
        state |> Map.put(:who_is_predecessor_attemps, 0)
      else
        state
      end
    else
      attempts = attempts - 1
      successor_pid = Enum.at(successor, 1)
      # Ask the successor for its predecessor.
      GenServer.cast(successor_pid, {:who_is_your_predecessor, self()})

      state |> Map.put(:who_is_predecessor_attemps, attempts)
    end
  end

  def handle_info({:stabilize}, state) do
    # x = successor.predecessor;
    #   if (x ∈ (n, successor))
    #     successor = x;
    #
    # successor.notify(n);
    successor = get_successor(state)
    # IO.puts("stabilizing node #{get_ID(state)}")
    # print_state(self())

    updated_state =
      if successor != nil do
        check_predecessor(state)
      else
        state
      end

    # Reschedule the timer
    schedule_stabilize_timer()

    {:noreply, updated_state}
  end

  # Gets called when this node is asked by the caller to join
  # an existing node existing_pid
  def handle_cast({:join_node, existing_pid}, state) do
    # Ask the existing_pid for a successor.
    # This nodes successor would be set only after someone responds with
    # an answer.
    GenServer.cast(existing_pid, {:find_successor_for_node, 1, get_ID(state), self()})
    {:noreply, state}
  end

  # id is the ID (integer value) for which this node should find a successor
  # querySourcePid is the node asking for successor. If we do  find the
  # successor, thats the pid to which send a 'success' message
  def handle_cast({:find_successor_for_node, finger_index, id, querySourcePid}, state) do
    # Gets called when some other in the network asks for a successor
    find_successor(id, state, finger_index, querySourcePid)
    {:noreply, state}
  end

  def handle_cast({:set_finger_table_entry, finger_index, successor_id, successor_pid}, state) do
    # IO.puts("set_finger_table_entry called finger_index = #{finger_index}")

    # if finger_index != 1 do
    #   IO.puts("Updating finger table entry")
    # end

    # Update the corresponding entry in the successor list if it exists.
    # Otherwise create the successor list
    updated_state =
      if state |> Map.has_key?(:successor_map) do
        successor_map = state |> Map.get(:successor_map)
        # if successor_map |> Map.has_key?(finger_index) do
        #   old_entry = successor_map |> Map.get(finger_index)
        #   old_entry_successor_id = Enum.at(old_entry, 0)
        #   if old_entry_successor_id == successor_id do
        #     IO.puts("No change in value.")
        #   else
        #     IO.puts("Entry changed")
        #   end
        # else
        #   IO.puts("Brand new entry")
        # end
        successor_map = successor_map |> Map.put(finger_index, [successor_id, successor_pid])
        state |> Map.put(:successor_map, successor_map)
      else
        successor_map = %{finger_index => [successor_id, successor_pid]}
        state |> Map.put(:successor_map, successor_map)
      end

    # print_state(self())
    {:noreply, updated_state}
  end

  def handle_cast({:predecessor_ping, ping_node_pid}, state) do
    GenServer.cast(ping_node_pid, {:predecessor_ping_response, self()})
    {:noreply, state}
  end

  def handle_cast({:predecessor_ping_response, predecessor_pid}, state) do
    # If the predecessor is nil or has changed, ignore this callback.
    current_predecessor = get_predecessor(state)

    state =
      if current_predecessor == nil or Enum.at(current_predecessor, 1) != predecessor_pid do
        state
      else
        # As the predecessor has responded, reset its attempt counter to 0.
        current_predecessor = current_predecessor |> List.replace_at(2, 0)
        # IO.puts("Resetting timer")
        state |> Map.put(:predecessor, current_predecessor)
      end

    {:noreply, state}
  end

  def handle_info({:check_predecessor}, state) do
    # if (predecessor has failed)
    #   predecessor = nil;
    current_predecessor = get_predecessor(state)

    state =
      if current_predecessor != nil do
        # If we have tried enough number of times with no response, assume that the node has failed.
        attempts = Enum.at(current_predecessor, 2)
        # IO.puts("attempts = #{attempts}")
        if attempts <= -3 do
          # Node has failed
          # IO.puts("predecessor failed")
          state |> Map.put(:predecessor, nil)
        else
          # Try ping again.
          current_predecessor_pid = Enum.at(current_predecessor, 1)
          GenServer.cast(current_predecessor_pid, {:predecessor_ping, self()})

          schedule_check_predecessor_timer()

          current_predecessor = current_predecessor |> List.replace_at(2, attempts - 1)
          state |> Map.put(:predecessor, current_predecessor)
        end
      else
        # We don't have a predecessor, so we are good.
        state
      end

    {:noreply, state}
  end

  def handle_cast(
        {:set_successor_if_you_dont_have_one, finger_index, successor_id, successor_pid},
        state
      ) do
    current_successor = get_successor(state)
    current_successor_pid = Enum.at(current_successor, 1)

    state =
      if current_successor_pid == self() do
        # This node still think its own successor. Thats sad. Lets change that.
        successor_map = state |> Map.get(:successor_map)
        successor_map = successor_map |> Map.put(finger_index, [successor_id, successor_pid])
        state |> Map.put(:successor_map, successor_map)
      else
        # Current node has a valid successor, so ignore this call.
        state
      end

    {:noreply, state}
  end

  def handle_info({:ask_someone_something}, state) do
    successor = get_successor(state)
    node_has_asked_enough_queries = state |> Map.get(:node_has_asked_enough_queries)
    # We cant ask a query till we have a successor.
    # If the node has been asked to stop, it should not send a query now.
    if node_has_asked_enough_queries == false and successor != nil and
         successor |> Enum.at(1) != self() do
      # Generate a random number.
      max_hash_value = state |> Map.get(:max_hash_value)
      query = Enum.random(0..max_hash_value)
      ask_query(query, state, 0, self())
      schedule_query_timer()
    end

    {:noreply, state}
  end

  def handle_info({:fix_fingers}, state) do
    next = state |> Map.get(:next)
    next = next + 1
    m = state |> Map.get(:m)

    next =
      if next > m do
        1
      else
        next
      end

    searchID = get_ID(state) + (:math.pow(2, next - 1) |> round)
    searchID = rem(searchID, Map.get(state, :max_hash_value) + 1) |> round

    find_successor(searchID, state, next, self())

    # Reschedule the timer
    schedule_fix_fingers_timer()

    {:noreply, state |> Map.put(:next, next)}
  end

  def handle_cast({:find_successor_for_answering, query, hops, querySourcePid}, state) do
    ask_query(query, state, hops, querySourcePid)
    {:noreply, state}
  end

  def handle_cast({:stop_talking}, state) do
    {:noreply, state |> Map.put(:node_has_asked_enough_queries, true)}
  end

  def handle_info({:you_will_die}, state) do
    {:stop, :normal, state}
  end
end
