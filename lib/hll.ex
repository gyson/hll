defmodule HLL do
  @moduledoc """

  Default HyperLogLog module.

  Note that this module is not Redis compatible. Use alternative `HLL.Redis` module
  if you need to interact with Redis and need it to be Redis compatible.

  This module use `:erlang.phash2` as hash function.

  ## Example

      iex> hll = HLL.new(14)
      iex> hll = Enum.reduce(1..2000, hll, fn i, acc -> HLL.add(acc, i) end)
      iex> HLL.cardinality(hll)
      1998

  ## Serialization

  It has two representations, sparse and dense. When encode HyperLogLog with `HLL.encode`,
  this module would automatically choose the representation with smaller encoded size.

      # sparse representation:
      <<0::4, precision_with_offset::4, index0::p, count0::6, index1::p, count1::6 ..., padding::x>>

      # dense representation:
      <<1::4, precision_with_offset::4, count0::6, count1::6, count2::6 ...>>

  """

  use Bitwise

  alias HLL.Helper

  @type t :: {__MODULE__, 8..16, map()}

  # "New cardinality estimation algorithms for HyperLogLog sketches" paper
  # suggests min precision to be 8. (page 8, formula 13)
  @p_min 8
  @p_max 16
  @p_range @p_min..@p_max

  @doc """

  Create a HyperLogLog instance with specified precision in range from 8 to 16.

  ## Example

      iex> HLL.new(12)
      {HLL, 12, %{}}
      iex> HLL.new(14)
      {HLL, 14, %{}}

  """

  @spec new(8..16) :: t()

  def new(p) when p in @p_range do
    {__MODULE__, p, %{}}
  end

  @doc """

  Add a value to HyperLogLog instance.

  ## Example

      iex> h = HLL.new(12)
      {HLL, 12, %{}}
      iex> HLL.add(h, "hello")
      {HLL, 12, %{1581 => 2}}

  """

  @spec add(t(), any()) :: t()

  def add({__MODULE__, p, map} = hll, item) do
    {index, count} = hash(p, item)

    case map do
      %{^index => value} when value >= count ->
        hll

      _ ->
        {__MODULE__, p, Map.put(map, index, count)}
    end
  end

  @doc """

  Merge multiple HyperLogLog instances into one.

  ## Example

      iex> h1 = HLL.new(12) |> HLL.add("foo")
      iex> h2 = HLL.new(12) |> HLL.add("bar")
      iex> h3 = HLL.new(12) |> HLL.add("foo") |> HLL.add("bar")
      iex> h_merged = HLL.merge([h1, h2])
      iex> h3 == h_merged
      true

  """

  @spec merge([t()]) :: t()

  def merge([{_, p, _} | _] = list_of_hll) do
    result =
      list_of_hll
      |> Enum.map(fn {__MODULE__, ^p, map} -> map end)
      |> Helper.merge_hll_maps()

    {__MODULE__, p, result}
  end

  @doc """

  Estimate cardinality of HyperLogLog instance.

  ## Example

      iex> h = HLL.new(14)
      iex> HLL.cardinality(h)
      0
      iex> h = HLL.add(h, "foo")
      iex> HLL.cardinality(h)
      1
      iex> h = HLL.add(h, "bar")
      iex> HLL.cardinality(h)
      2

  """

  @spec cardinality(t()) :: non_neg_integer()

  def cardinality({__MODULE__, p, map} = _hll) do
    Helper.estimate_cardinality(p, map_size(map), Map.values(map))
  end

  @doc """

  Encode HyperLogLog instance to HLL binary format.

  ## Example

      iex> HLL.new(14) |> HLL.encode()
      <<6>>
      iex> HLL.new(14) |> HLL.add("foo") |> HLL.encode()
      <<6, 9, 164, 16>>
      iex> HLL.new(14) |> HLL.add("foo") |> HLL.add("bar") |> HLL.encode()
      <<6, 9, 164, 16, 219, 129, 0>>

  """

  @spec encode(t()) :: binary()

  def encode(hll)

  @doc """

  Decode HLL binary format to HyperLogLog instance.

  ## Example

      iex> h = HLL.new(14) |> HLL.add("foo")
      {HLL, 14, %{617 => 1}}
      iex> encoded = HLL.encode(h)
      <<6, 9, 164, 16>>
      iex> HLL.decode(encoded)
      {HLL, 14, %{617 => 1}}

  """

  @spec decode(binary()) :: t()

  def decode(hll_binary)

  # <<format::4, p_code::4, entries, padding>>

  for p <- @p_range do
    m = 1 <<< p
    dense_size = m * 6
    encode_sparse = String.to_atom("encode_sparse_p#{p}")

    def encode({__MODULE__, unquote(p), map}) do
      sparse_size = unquote(p + 6) * map_size(map)

      if sparse_size < unquote(dense_size) do
        # encode sparse
        [<<0::4, unquote(p - @p_min)::4>> | unquote(encode_sparse)(Map.to_list(map), [])]
      else
        # encode dense
        entries =
          Enum.reduce(unquote(div(m, 8) - 1)..0, [], fn i, acc ->
            index = i * 8
            b0 = Map.get(map, index, 0)
            b1 = Map.get(map, index + 1, 0)
            b2 = Map.get(map, index + 2, 0)
            b3 = Map.get(map, index + 3, 0)
            b4 = Map.get(map, index + 4, 0)
            b5 = Map.get(map, index + 5, 0)
            b6 = Map.get(map, index + 6, 0)
            b7 = Map.get(map, index + 7, 0)
            [<<b0::6, b1::6, b2::6, b3::6, b4::6, b5::6, b6::6, b7::6>> | acc]
          end)

        [<<1::4, unquote(p - @p_min)::4>>, entries]
      end
      |> IO.iodata_to_binary()
    end

    compute_sparse_padding_size = fn n ->
      8 - rem(n * (p + 6), 8)
    end

    defp unquote(encode_sparse)(
           [
             {i1, c1},
             {i2, c2},
             {i3, c3},
             {i4, c4},
             {i5, c5},
             {i6, c6},
             {i7, c7},
             {i8, c8} | rest
           ],
           acc
         ) do
      unquote(encode_sparse)(rest, [
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6, i4::unquote(p),
          c4::6, i5::unquote(p), c5::6, i6::unquote(p), c6::6, i7::unquote(p), c7::6,
          i8::unquote(p), c8::6>>
        | acc
      ])
    end

    defp unquote(encode_sparse)(
           [{i1, c1}, {i2, c2}, {i3, c3}, {i4, c4}, {i5, c5}, {i6, c6}, {i7, c7}],
           acc
         ) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6, i4::unquote(p),
          c4::6, i5::unquote(p), c5::6, i6::unquote(p), c6::6, i7::unquote(p), c7::6,
          0::unquote(compute_sparse_padding_size.(7))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}, {i2, c2}, {i3, c3}, {i4, c4}, {i5, c5}, {i6, c6}], acc) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6, i4::unquote(p),
          c4::6, i5::unquote(p), c5::6, i6::unquote(p), c6::6,
          0::unquote(compute_sparse_padding_size.(6))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}, {i2, c2}, {i3, c3}, {i4, c4}, {i5, c5}], acc) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6, i4::unquote(p),
          c4::6, i5::unquote(p), c5::6, 0::unquote(compute_sparse_padding_size.(5))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}, {i2, c2}, {i3, c3}, {i4, c4}], acc) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6, i4::unquote(p),
          c4::6, 0::unquote(compute_sparse_padding_size.(4))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}, {i2, c2}, {i3, c3}], acc) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6, i3::unquote(p), c3::6,
          0::unquote(compute_sparse_padding_size.(3))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}, {i2, c2}], acc) do
      [
        acc,
        <<i1::unquote(p), c1::6, i2::unquote(p), c2::6,
          0::unquote(compute_sparse_padding_size.(2))>>
      ]
    end

    defp unquote(encode_sparse)([{i1, c1}], acc) do
      [acc, <<i1::unquote(p), c1::6, 0::unquote(compute_sparse_padding_size.(1))>>]
    end

    defp unquote(encode_sparse)([], acc) do
      acc
    end

    decode_sparse = String.to_atom("decode_sparse_p#{p}")

    def decode(<<0::4, unquote(p - @p_min)::4, rest::bits>>) do
      map = unquote(decode_sparse)(rest, []) |> Map.new()
      {__MODULE__, unquote(p), map}
    end

    defp unquote(decode_sparse)(<<index::unquote(p), count::6, rest::bits>>, acc) do
      unquote(decode_sparse)(rest, [{index, count} | acc])
    end

    defp unquote(decode_sparse)(<<_padding::bits>>, acc) do
      acc
    end

    decode_dense = String.to_atom("decode_dense_p#{p}")

    def decode(<<1::4, unquote(p - @p_min)::4, rest::bits>>) do
      map = unquote(decode_dense)(rest, 0, []) |> Map.new()
      {__MODULE__, unquote(p), map}
    end

    defp unquote(decode_dense)(<<0::6, rest::bits>>, index, acc) do
      unquote(decode_dense)(rest, index + 1, acc)
    end

    defp unquote(decode_dense)(<<value::6, rest::bits>>, index, acc) do
      unquote(decode_dense)(rest, index + 1, [{index, value} | acc])
    end

    defp unquote(decode_dense)(<<>>, unquote(1 <<< p), acc) do
      acc
    end
  end

  @range_32 1 <<< 32

  defp hash(p, item) do
    <<index::size(p), rest::bits>> = <<:erlang.phash2(item, @range_32)::32>>

    count_zeros(rest, index, 1, item)
  end

  defp count_zeros(<<1::1, _::bits>>, index, count, _item) do
    {index, count}
  end

  defp count_zeros(<<0::1, rest::bits>>, index, count, item) do
    count_zeros(rest, index, count + 1, item)
  end

  defp count_zeros(<<>>, index, count, item) do
    count_zeros2(<<:erlang.phash2([item], @range_32)::32>>, index, count)
  end

  defp count_zeros2(<<1::1, _::bits>>, index, count) do
    {index, count}
  end

  defp count_zeros2(<<0::1, rest::bits>>, index, count) do
    count_zeros2(rest, index, count + 1)
  end

  defp count_zeros2(<<>>, index, count) do
    {index, count}
  end
end
