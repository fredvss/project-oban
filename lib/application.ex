defmodule MiniOban.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      MiniOban.Queue
    ]

    opts = [strategy: :one_for_one, name: MiniOban.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
