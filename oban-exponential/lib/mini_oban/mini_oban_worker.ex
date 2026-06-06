defmodule MiniOban.Worker do
  alias MiniOban.Job

  def perform(%Job{} = job) do
    duration = :rand.uniform(2000)
    IO.puts("[Worker] Starting job #{inspect(job.id)} | type=#{job.type} | attempt=#{job.attempts + 1}/#{job.max_attempts} | will take #{duration}ms")
    Process.sleep(duration)

    if :rand.uniform(2) == 1 do
      IO.puts("[Worker] Job #{inspect(job.id)} succeeded")
      :ok
    else
      IO.puts("[Worker] Job #{inspect(job.id)} failed")
      {:error, "random failure"}
    end
  end
end
