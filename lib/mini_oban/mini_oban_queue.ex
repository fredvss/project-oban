defmodule MiniOban.Queue do
  use GenServer

  @name __MODULE__

  defmodule MiniOban.Job do
    defstruct [
      :id,
      :type,
      :payload,
      attempts: 0,
      max_attempts: 3
    ]
  end

  #queue control
  # jobs pendentes
  # jobs rodando
  # quantidade máxima de concorrência
  # attempt atual de cada job
  # quando tentar de novo
  # quando marcar como sucesso
  # quando marcar como failed

  #job example: %{type: :send_email, payload: payload, max_attempts: 3}
  #state example: [%{type: :send_email, payload: payload, max_attempts: 3, attempts: 0}]

  # Public API
  def start_link() do
    GenServer.start_link(@name, :ok, name: @name)
  end

  def queue_job(job) do
    GenServer.cast(@name, {:queue_job, job})
  end

  def dequeue_job() do
    GenServer.call(@name, :dequeue_job)
  end


  # GenServer Callbacks
  def init(:ok) do
    {:ok, []} # Initial state is an empty list of jobs
  end

  def handle_cast({:queue_job, job}, state) do
    # Here you would normally add the job to a database or in-memory store
    # For simplicity, we just print it and keep it in the state
    IO.puts("Queued job: #{inspect(job)}")
    {:noreply, [job | state]} # Add the new job to the state
  end

  def handle_call(:dequeue_job, _from, state) do
    case state do
      [] -> {:reply, nil, state} # No jobs to dequeue
      [job | rest] -> {:reply, job, rest} # Return the first job and update the state
    end
  end
end
