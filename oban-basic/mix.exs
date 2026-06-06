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

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {MiniOban.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bunt, "~> 1.0"}
    ]
  end
end
