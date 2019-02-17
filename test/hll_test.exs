defmodule HLLTest do
  use ExUnit.Case
  doctest HLL

  use Bitwise

  test "it should works" do
    hll = HLL.new(14)

    hll = HLL.add(hll, "foo")
    assert HLL.cardinality(hll) == 1

    hll = HLL.add(hll, "bar")
    assert HLL.cardinality(hll) == 2

    hll = HLL.add(hll, "bar")
    assert HLL.cardinality(hll) == 2

    hll = HLL.add(hll, "okk")
    assert HLL.cardinality(hll) == 3
  end

  test "it should work in more cases" do
    for p <- [12, 14, 16], n <- [1, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000] do
      hll = HLL.new(p)

      hll =
        1..n
        |> Enum.reduce(hll, fn x, acc ->
          HLL.add(acc, x)
        end)

      hll_transformed = hll |> HLL.encode() |> HLL.decode()

      assert hll == hll_transformed

      hll_cardinality = HLL.cardinality(hll)

      error = 1.04 / :math.pow(1 <<< p, 0.5)

      assert hll_cardinality >= floor(n * (1 - error))
      assert hll_cardinality <= ceil(n * (1 + error))
    end
  end
end
