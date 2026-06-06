defmodule MiniOban.Queue do
  use GenServer

  alias MiniOban.{Job, Worker}

  # state map:
  # %{
  #   pending: [],
  #   running: %{ref => job},
  #   completed: [job],
  #   max_concurrency: integer
  # }

  # Client

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def queue_job(%Job{} = job, queue \\ __MODULE__) do
    job = %{job |
      id: job.id || System.unique_integer([:positive]),
      inserted_at: job.inserted_at || DateTime.utc_now()
    }
    GenServer.cast(queue, {:queue_job, job})
  end

  def get_state(queue \\ __MODULE__) do
    GenServer.call(queue, :get_state)
  end

  def get_report(queue \\ __MODULE__) do
    GenServer.call(queue, :get_report)
  end

  # Server

  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 5)
    state = %{
      pending: [],
      running: %{},
      completed: [],
      max_concurrency: max_concurrency
    }
    {:ok, state}
  end

  def handle_cast({:queue_job, job}, state) do
    IO.puts("[Queue] Queued job #{job.id} | type=#{job.type}")
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
        Enum.reduce(to_run, state.running, fn job, running ->
          started_job = %{job | status: :running, started_at: DateTime.utc_now()}
          task = Task.async(fn -> Worker.perform(started_job) end)
          IO.puts("[Queue] Dispatching job #{job.id} | running=#{map_size(running) + 1}/#{state.max_concurrency}")
          Map.put(running, task.ref, started_job)
        end)

      {:noreply, %{state | pending: remaining, running: new_running}}
    end
  end

  # Requeue agendado pelo backoff exponencial
  def handle_info({:requeue, job}, state) do
    IO.puts("[Queue] Requeueing job #{job.id} after exponential backoff")
    new_state = %{state | pending: state.pending ++ [job]}
    send(self(), :dispatch)
    {:noreply, new_state}
  end

  # Task concluída (sucesso ou erro controlado)
  def handle_info({ref, result}, state) when is_map_key(state.running, ref) do
    Process.demonitor(ref, [:flush])
    {job, new_running} = Map.pop(state.running, ref)
    finished_at = DateTime.utc_now()
    duration_ms = DateTime.diff(finished_at, job.started_at, :millisecond)

    new_state =
      case result do
        :ok ->
          IO.puts("[Queue] Job #{job.id} succeeded | attempt=#{job.attempts + 1} | duration=#{duration_ms}ms")
          finished_job = %{job | status: :success, finished_at: finished_at}
          %{state | running: new_running, completed: state.completed ++ [finished_job]}

        {:error, reason} ->
          next_attempt = job.attempts + 1
          if next_attempt < job.max_attempts do
            retry_job = %{job | attempts: next_attempt, status: :pending, started_at: nil}
            # Exponential backoff: 2^attempt seconds (attempt 1 → 2s, 2 → 4s, 3 → 8s…)
            delay_ms = trunc(:math.pow(2, next_attempt) * 1_000)
            IO.puts("[Queue] Job #{job.id} failed (#{reason}) — retrying in #{delay_ms}ms (attempt #{next_attempt}/#{retry_job.max_attempts})")
            Process.send_after(self(), {:requeue, retry_job}, delay_ms)
            %{state | running: new_running}
          else
            IO.puts("[Queue] Job #{job.id} permanently failed after #{next_attempt} attempt(s)")
            finished_job = %{job | status: :failed, finished_at: finished_at}
            %{state | running: new_running, completed: state.completed ++ [finished_job]}
          end
      end

    send(self(), :dispatch)
    {:noreply, new_state}
  end

  # Task crashou (exceção/exit inesperado)
  def handle_info({:DOWN, ref, :process, _, reason}, state) when is_map_key(state.running, ref) do
    {job, new_running} = Map.pop(state.running, ref)
    finished_at = DateTime.utc_now()
    next_attempt = job.attempts + 1

    new_state =
      if next_attempt < job.max_attempts do
        retry_job = %{job | attempts: next_attempt, status: :pending, started_at: nil}
        # Exponential backoff: 2^attempt seconds
        delay_ms = trunc(:math.pow(2, next_attempt) * 1_000)
        IO.puts("[Queue] Job #{job.id} crashed (#{inspect(reason)}) — retrying in #{delay_ms}ms (attempt #{next_attempt}/#{retry_job.max_attempts})")
        Process.send_after(self(), {:requeue, retry_job}, delay_ms)
        %{state | running: new_running}
      else
        IO.puts("[Queue] Job #{job.id} crashed permanently after #{next_attempt} attempt(s)")
        finished_job = %{job | status: :failed, finished_at: finished_at}
        %{state | running: new_running, completed: state.completed ++ [finished_job]}
      end

    send(self(), :dispatch)
    {:noreply, new_state}
  end

  def handle_call(:get_state, _from, state) do
    current = %{
      pending: state.pending,
      running: state.running,
      max_concurrency: state.max_concurrency
    }
    {:reply, current, state}
  end

  def handle_call(:get_report, _from, state) do
    success = Enum.filter(state.completed, &(&1.status == :success))
    failed  = Enum.filter(state.completed, &(&1.status == :failed))

    report = %{
      success: success,
      failed: failed,
      total_success: length(success),
      total_failed: length(failed)
    }
    {:reply, report, state}
  end
end
