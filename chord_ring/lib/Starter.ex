defmodule Starter do
  use Application

  def start(_type, _args) do
    {:ok, app} = Chord.Application.start_link(self(), System.argv())
    waitForResult()
    {:ok, app}
  end

  def waitForResult() do
    receive do
      {:terminate} ->
        nil
        # IO.puts("Done")
    end
  end
end
