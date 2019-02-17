defmodule HLL.Helper do
  @moduledoc false

  use Bitwise

  def merge_hll_maps(maps) do
    maps
    |> Enum.sort(&(map_size(&1) >= map_size(&2)))
    |> Enum.reduce(fn map, acc ->
      Enum.reduce(map, acc, fn {index, zeros}, acc ->
        case acc do
          %{^index => value} when value >= zeros ->
            acc

          _ ->
            Map.put(acc, index, zeros)
        end
      end)
    end)
  end

  # based on Algorithm 6 from "New cardinality estimation algorithms for HyperLogLog sketches" paper.

  @hll_alpha_inf 0.5 / :math.log(2)

  def estimate_cardinality(_, 0, _) do
    0
  end

  def estimate_cardinality(p, size, values) do
    q = 64 - p
    m = 1 <<< p

    histo =
      Enum.reduce(values, %{}, fn value, acc ->
        case acc do
          %{^value => count} -> Map.put(acc, value, count + 1)
          _ -> Map.put(acc, value, 1)
        end
      end)

    z = m * hll_tau(1 - Map.get(histo, q + 1, 0) / m)

    z =
      Enum.reduce(q..1, z, fn k, z ->
        0.5 * (z + Map.get(histo, k, 0))
      end)

    # note: size != 0, therefore, (1 - size / m) < 1
    z = z + m * hll_sigma(1 - size / m)

    round(@hll_alpha_inf * m * m / z)
  end

  defp hll_sigma(x) do
    hll_sigma_continue(x, 1, x)
  end

  defp hll_sigma_continue(x, y, z) do
    x = x * x
    z_prime = z
    z = z + x * y
    y = y + y

    if z_prime == z do
      z
    else
      hll_sigma_continue(x, y, z)
    end
  end

  defp hll_tau(x) when x == 0.0 or x == 1.0 do
    0.0
  end

  defp hll_tau(x) do
    hll_tau_continue(x, 1.0, 1 - x)
  end

  defp hll_tau_continue(x, y, z) do
    x = :math.sqrt(x)
    z_prime = z
    y = 0.5 * y
    z = z - :math.pow(1 - x, 2) * y

    if z_prime == z do
      z / 3
    else
      hll_tau_continue(x, y, z)
    end
  end
end
