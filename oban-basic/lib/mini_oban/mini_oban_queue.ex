defmodule MiniOban.Queue do
  use GenServer

  alias MiniOban.Job
  alias MiniOban.Worker

  @name __MODULE__

  # state map: %{pending: [], running: %{}, max_concurrency: max_concurrency}

  # Client
  def start_link(opts \\ []) do
    GenServer.start_link(@name, opts, name: @name)
  end

  def queue_job(%Job{} = job) do
    job = if job.id, do: job, else: %{job | id: make_ref()}
    GenServer.cast(@name, {:queue_job, job})
  end

  def get_state() do
    GenServer.call(@name, :get_state)
  end

  # Server
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 3)
    state = %{pending: [], running: %{}, max_concurrency: max_concurrency}
    {:ok, state}
  end

  def handle_cast({:queue_job, job}, state) do
    IO.puts("[Queue] Queued job #{inspect(job.id)} | type=#{job.type}")
    new_state = %{state | pending: state.pending ++ [job]}
    send(self(), :dispatch)
    {:noreply, new_state}
  end

  def handle_info(:dispatch, state) do
    slots = state.max_concurrency - map_size(state.running)

    if slots <= 0 do
      {:noreply, state}
    else
      {to_run, remaining} = Enum.split(state.pending, slots)

      new_running =
        Enum.reduce(to_run, state.running, fn (job, running) ->
          task = Task.async(fn -> Worker.perform(job) end)
          IO.puts("[Queue] Dispatching job #{inspect(job.id)} | running=#{map_size(running) + 1}/#{state.max_concurrency}")
          Map.put(running, task.ref, %{job | status: :running})
        end)

      {:noreply, %{state | pending: remaining, running: new_running}}
    end
  end

  # Task completed (success or controlled error)
  def handle_info({ref, result}, state) when is_map_key(state.running, ref) do
    Process.demonitor(ref, [:flush])
    {job, new_running} = Map.pop(state.running, ref)

    new_state =
      case result do
        :ok ->
          IO.puts("[Queue] Job #{inspect(job.id)} succeeded after #{job.attempts + 1} attempt(s)")
          %{state | running: new_running}

        {:error, reason} ->
          if job.attempts + 1 < job.max_attempts do
            retry_job = %{job | attempts: job.attempts + 1, status: :pending}
            IO.puts("[Queue] Job #{inspect(job.id)} failed (#{reason}) — retrying (#{retry_job.attempts}/#{retry_job.max_attempts})")
            %{state | running: new_running, pending: state.pending ++ [retry_job]}
          else
            IO.puts("[Queue] Job #{inspect(job.id)} permanently failed after #{job.attempts + 1} attempt(s)")
            %{state | running: new_running}
          end
      end

    send(self(), :dispatch)
    {:noreply, new_state}
  end

  # Task crashed (exception/exit) — treat as failure
  def handle_info({:DOWN, ref, :process, _, reason}, state) when is_map_key(state.running, ref) do
    {job, new_running} = Map.pop(state.running, ref)

    new_state =
      if job.attempts + 1 < job.max_attempts do
        retry_job = %{job | attempts: job.attempts + 1, status: :pending}
        IO.puts("[Queue] Job #{inspect(job.id)} crashed (#{inspect(reason)}) — retrying (#{retry_job.attempts}/#{retry_job.max_attempts})")
        %{state | running: new_running, pending: state.pending ++ [retry_job]}
      else
        IO.puts("[Queue] Job #{inspect(job.id)} crashed permanently after #{job.attempts + 1} attempt(s)")
        %{state | running: new_running}
      end

    send(self(), :dispatch)
    {:noreply, new_state}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end
end
