defmodule HLL.MixProject do
  use Mix.Project

  def project do
    [
      app: :hll,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "HLL",
      description: "Redis compatible HyperLogLog implementation in Elixir",
      source_url: "https://github.com/gyson/hll"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:hypex, "~> 1.1", only: :dev},
      {:benchee, "~> 0.13", only: :dev},
      {:redix, "~> 0.9", only: [:dev, :test]},
      {:stream_data, "~> 0.4", only: [:dev, :test]},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0.0-rc.4", only: :dev, runtime: false}
    ]
  end

  def package do
    %{
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/gyson/hll"}
    }
  end
end
