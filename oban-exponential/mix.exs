defmodule MiniOban.MixProject do
  use Mix.Project

  def project do
    [
      app: :mini_oban,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {MiniOban.Application, []}
    ]
  end

  defp deps do
    []
  end
end
