defmodule MiniOban.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Supervisor.child_spec({MiniOban.Queue, [name: MiniOban.Queue, max_concurrency: 3]}, id: :queue_default),
      Supervisor.child_spec({MiniOban.Queue, [name: :critical, max_concurrency: 1]}, id: :queue_critical)
    ]

    opts = [strategy: :one_for_one, name: MiniOban.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
