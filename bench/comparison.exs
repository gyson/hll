defmodule HLL.Bench.Comparison do
  @p 14

  def add(n) do
    data = StreamData.binary() |> Enum.take(n)

    Benchee.run(
      %{
        "HLL.add" => fn ->
          Enum.reduce(data, HLL.new(@p), fn x, acc ->
            HLL.add(acc, x)
          end)
        end,
        "HLL.Redis.add" => fn ->
          Enum.reduce(data, HLL.Redis.new(), fn x, acc ->
            HLL.Redis.add(acc, x)
          end)
        end,
        "Hypex.update" => fn ->
          Enum.reduce(data, Hypex.new(@p), fn x, acc ->
            Hypex.update(acc, x)
          end)
        end
      },
      time: 10,
      memory_time: 2
    )
  end

  def cardinality(n) do
    data = StreamData.binary() |> Enum.take(n)

    hll =
      Enum.reduce(data, HLL.new(@p), fn x, acc ->
        HLL.add(acc, x)
      end)

    hll_redis =
      Enum.reduce(data, HLL.Redis.new(), fn x, acc ->
        HLL.Redis.add(acc, x)
      end)

    hypex =
      Enum.reduce(data, Hypex.new(@p), fn x, acc ->
        Hypex.update(acc, x)
      end)

    Benchee.run(%{
      "HLL.cardinality" => fn ->
        HLL.cardinality(hll)
      end,
      "HLL.Redis.cardinality" => fn ->
        HLL.Redis.cardinality(hll_redis)
      end,
      "Hypex.cardinality" => fn ->
        hypex |> Hypex.cardinality() |> round
      end
    })
  end

  def merge(n) do
    data1 = StreamData.binary() |> Enum.take(n)
    data2 = StreamData.binary() |> Enum.take(n)

    hll1 =
      Enum.reduce(data1, HLL.new(@p), fn x, acc ->
        HLL.add(acc, x)
      end)

    hll2 =
      Enum.reduce(data2, HLL.new(@p), fn x, acc ->
        HLL.add(acc, x)
      end)

    hll_redis1 =
      Enum.reduce(data1, HLL.Redis.new(), fn x, acc ->
        HLL.Redis.add(acc, x)
      end)

    hll_redis2 =
      Enum.reduce(data2, HLL.Redis.new(), fn x, acc ->
        HLL.Redis.add(acc, x)
      end)

    hypex1 =
      Enum.reduce(data1, Hypex.new(@p), fn x, acc ->
        Hypex.update(acc, x)
      end)

    hypex2 =
      Enum.reduce(data2, Hypex.new(@p), fn x, acc ->
        Hypex.update(acc, x)
      end)

    Benchee.run(%{
      "HLL.merge" => fn ->
        HLL.merge([hll1, hll2])
      end,
      "HLL.Redis.merge" => fn ->
        HLL.Redis.merge([hll_redis1, hll_redis2])
      end,
      "Hypex.merge" => fn ->
        Hypex.merge(hypex1, hypex2)
      end
    })
  end

  def encode(n) do
    data = StreamData.binary() |> Enum.take(n)

    hll =
      Enum.reduce(data, HLL.new(@p), fn x, acc ->
        HLL.add(acc, x)
      end)

    hll_redis =
      Enum.reduce(data, HLL.Redis.new(), fn x, acc ->
        HLL.Redis.add(acc, x)
      end)

    Benchee.run(%{
      "HLL.encode" => fn ->
        HLL.encode(hll)
      end,
      "HLL.Redis.encode" => fn ->
        HLL.Redis.encode(hll_redis)
      end
    })
  end

  def decode(n) do
    data = StreamData.binary() |> Enum.take(n)

    hll =
      data
      |> Enum.reduce(HLL.new(@p), fn x, acc ->
        HLL.add(acc, x)
      end)
      |> HLL.encode()

    hll_redis =
      data
      |> Enum.reduce(HLL.Redis.new(), fn x, acc ->
        HLL.Redis.add(acc, x)
      end)
      |> HLL.Redis.encode()

    Benchee.run(%{
      "HLL.decode" => fn ->
        HLL.decode(hll)
      end,
      "HLL.Redis.decode" => fn ->
        HLL.Redis.decode(hll_redis)
      end
    })
  end
end

# sparse
# HLL.Bench.Comparison.add(1000)
# HLL.Bench.Comparison.merge(1000)
HLL.Bench.Comparison.encode(1000)
# HLL.Bench.Comparison.decode(1000)
# HLL.Bench.Comparison.cardinality(1000)

# dense
# HLL.Bench.Comparison.add(10000)
# HLL.Bench.Comparison.merge(10000)
HLL.Bench.Comparison.encode(10000)
# HLL.Bench.Comparison.decode(10000)
# HLL.Bench.Comparison.cardinality(10000)
