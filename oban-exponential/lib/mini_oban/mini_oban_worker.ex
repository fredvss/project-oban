defmodule MiniOban.Worker do
  alias MiniOban.Job

  def perform(%Job{} = job) do
    duration = :rand.uniform(2000)
    IO.puts("[Worker] Starting job #{job.id} | type=#{job.type} | attempt=#{job.attempts + 1}/#{job.max_attempts} | will take #{duration}ms")
    Process.sleep(duration)

    if :rand.uniform(2) == 1 do
      Bunt.puts([:green, "[Worker] Job #{job.id} succeeded"])
      :ok
    else
      Bunt.puts([:red, "[Worker] Job #{job.id} failed"])
      {:error, "random failure"}
    end
  end
end
