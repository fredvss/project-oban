defmodule MiniOban.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {MiniOban.Queue, [name: MiniOban.Queue, max_concurrency: 3]},
      {MiniOban.Queue, [name: :critical, max_concurrency: 1]}
    ]

    opts = [strategy: :one_for_one, name: MiniOban.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
