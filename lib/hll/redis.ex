defmodule HLL.Redis do
  @moduledoc """

  Redis compatible HyperLogLog module.

  This module is Redis (v5) compatible. It uses the same hash algorithm,
  same HyperLogLog estimation algorithm and same serialization format as
  Redis (v5) does.

  Therefore, it could consume HyperLogLog sketches from Redis and it could
  generate HyperLogLog sketches for Redis as well.

  It has fixed precision 14 (16384 buckets) as Redis does. If you are looking
  for using different precision, you could use `HLL` module instead.

  `HLL.Redis` module is generally slower than alternative `HLL` module:

    - `HLL.Redis` hash function is slower: Hash function in `HLL.Redis`
      is ported from Redis and written in Elixir. Hash function in `HLL`
      is `:erlang.phash2`, which is in native code.
    - `HLL.Redis` serialization is slower: `HLL.Redis` uses Redis binary
      format for serialization. `HLL` uses a binary format which is closer
      to `HLL`'s internal data structure, which makes it faster to encode
      and decode.

  Therefore, if you do not require "Redis compatible", it's recommanded to
  use `HLL` module for performance gain.

  ## Example

      iex> hll_redis = HLL.Redis.new()
      iex> hll_redis = Enum.reduce(1..2000, hll_redis, fn i, acc -> HLL.Redis.add(acc, Integer.to_string(i)) end)
      iex> HLL.Redis.cardinality(hll_redis)
      2006

  """

  use Bitwise

  alias HLL.Helper

  @type t :: {__MODULE__, map()}

  @doc """

  Create a Redis compatible HyperLogLog instance with precision = 14.

  ## Example

      iex> HLL.Redis.new()
      {HLL.Redis, %{}}

  """

  @spec new() :: t()

  def new() do
    {__MODULE__, %{}}
  end

  @doc """

  Add a value to Redis compatible HyperLogLog instance.

  If `item` is binary, it would use Redis compatible murmur2 hash function directly.

  If `item` is not binary, it would be transformed to binary via `:erlang.term_to_binary/1`
  and then use Redis compatible murmur2 hash function.

  ## Example

      iex> HLL.Redis.new() |> HLL.Redis.add("hello")
      {HLL.Redis, %{9216 => 1}}

  """

  @spec add(t(), any) :: t()

  def add({__MODULE__, map} = hll_redis, item) do
    {index, count} = hash(item)

    case map do
      %{^index => old_count} when old_count >= count ->
        hll_redis

      _ ->
        {__MODULE__, Map.put(map, index, count)}
    end
  end

  @doc """

  Merge multiple Redis compatible HyperLogLog instances into one.

  ## Example

      iex> h1 = HLL.Redis.new() |> HLL.Redis.add("foo")
      iex> h2 = HLL.Redis.new() |> HLL.Redis.add("bar")
      iex> h3 = HLL.Redis.new() |> HLL.Redis.add("foo") |> HLL.Redis.add("bar")
      iex> h_merged = HLL.Redis.merge([h1, h2])
      iex> h3 == h_merged
      true

  """

  def merge(list_of_hll_redis) do
    result =
      list_of_hll_redis
      |> Enum.map(fn {__MODULE__, map} -> map end)
      |> Helper.merge_hll_maps()

    {__MODULE__, result}
  end

  defp hash(item) when is_binary(item) do
    <<x::50, index::14>> = <<murmur_64a(item)::64>>

    if x == 0 do
      {index, 51}
    else
      {index, count_zeros(x, 1)}
    end
  end

  defp hash(item) do
    hash(:erlang.term_to_binary(item))
  end

  defp count_zeros(x, acc) do
    if (x &&& 1) == 0 do
      count_zeros(x >>> 1, acc + 1)
    else
      acc
    end
  end

  # port from Redis v5: https://github.com/antirez/redis/blob/86802d4f2681baa04869fabfbd0ca6c2fe0a94d7/src/hyperloglog.c

  @seed 0xADC83B19
  @m 0xC6A4A7935BD1E995
  @r 47
  @mask64 (1 <<< 64) - 1

  defp murmur_64a(item) do
    len = byte_size(item)
    h = @mask64 &&& @seed ^^^ (len * @m)

    h = murmur_process(item, h)

    h = h ^^^ (h >>> @r)
    h = @mask64 &&& h * @m
    h ^^^ (h >>> @r)
  end

  defp murmur_process(<<k::little-integer-64, rest::bits>>, h) do
    k = @mask64 &&& k * @m
    k = k ^^^ (k >>> @r)
    k = @mask64 &&& k * @m
    h = k ^^^ h
    h = @mask64 &&& h * @m
    murmur_process(rest, h)
  end

  defp murmur_process(<<k::little-integer-56>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-48>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-40>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-32>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-24>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-16>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<k::little-integer-8>>, h), do: @mask64 &&& (h ^^^ k) * @m
  defp murmur_process(<<>>, h), do: h

  @doc """

  Encode Redis compatible HyperLogLog instance to Redis HyperLogLog binary format.

  ## Example

      iex> HLL.Redis.new() |> HLL.Redis.add("hello") |> HLL.Redis.encode()
      <<72, 89, 76, 76, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 99, 255, 128, 91, 254>>
      iex> {:ok, conn} = Redix.start_link()
      iex> Redix.command!(conn, ["PFADD", "test_hll_redis_encode", "hello"])
      iex> Redix.command!(conn, ["GET", "test_hll_redis_encode"])
      <<72, 89, 76, 76, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 99, 255, 128, 91, 254>>

  """

  @spec encode(t()) :: binary()

  def encode({__MODULE__, map} = _hll_redis) do
    # 2000 size is about 3000 bytes in Redis sparse representation,
    # the default config to switch between sparse and dense
    if map_size(map) <= 2000 do
      encode_sparse(map)
    else
      encode_dense(map)
    end
  end

  @sparse_header <<"HYLL", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128>>
  @dense_header <<"HYLL", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128>>

  defp encode_sparse(map) do
    pairs = map |> Map.to_list() |> Enum.sort()

    case encode_sparse_body(pairs, -1, []) do
      :dense ->
        encode_dense(map)

      result ->
        IO.iodata_to_binary([@sparse_header, result])
    end
  end

  defp encode_sparse_body([{index, count} | rest], prev_index, acc) do
    if count > 32 do
      :dense
    else
      acc = add_sparse_zeros(acc, index, prev_index)

      sparse_val = count - 1
      next_index = index + 1

      case rest do
        [{^next_index, ^count} | next_rest] ->
          next_index_2 = next_index + 1

          case next_rest do
            [{^next_index_2, ^count} | next_rest_2] ->
              next_index_3 = next_index_2 + 1

              case next_rest_2 do
                [{^next_index_3, ^count} | next_rest_3] ->
                  encode_sparse_body(next_rest_3, next_index_3, [
                    <<1::1, sparse_val::5, 3::2>> | acc
                  ])

                _ ->
                  encode_sparse_body(next_rest_2, next_index_2, [
                    <<1::1, sparse_val::5, 2::2>> | acc
                  ])
              end

            _ ->
              encode_sparse_body(next_rest, next_index, [<<1::1, sparse_val::5, 1::2>> | acc])
          end

        _ ->
          encode_sparse_body(rest, index, [<<1::1, sparse_val::5, 0::2>> | acc])
      end
    end
  end

  defp encode_sparse_body([], prev_index, acc) do
    acc
    |> add_sparse_zeros(16384, prev_index)
    |> Enum.reverse()
  end

  defp add_sparse_zeros(acc, curr_index, prev_index) do
    case curr_index - prev_index - 2 do
      diff when diff >= 64 ->
        # XZERO
        [<<1::2, diff::14>> | acc]

      diff when diff >= 0 ->
        # ZERO
        [<<0::2, diff::6>> | acc]

      _ ->
        acc
    end
  end

  # Redis dense format:
  # +--------+--------+--------+------//
  # |11000000|22221111|33333322|55444444
  # +--------+--------+--------+------//

  defp encode_dense(map) do
    body =
      ((1 <<< 12) - 1)..0
      |> Enum.reduce([], fn i, acc ->
        b_0 = i * 4
        b_1 = b_0 + 1
        b_2 = b_0 + 2
        b_3 = b_0 + 3
        x_0 = Map.get(map, b_0, 0)
        x_1 = Map.get(map, b_1, 0)
        x_2 = Map.get(map, b_2, 0)
        x_3 = Map.get(map, b_3, 0)
        [<<x_1 &&& 3::2, x_0::6, x_2 &&& 15::4, x_1 >>> 2::4, x_3::6, x_2 >>> 4::2>> | acc]
      end)

    IO.iodata_to_binary([@dense_header | body])
  end

  @doc """

  Decode Redis HyperLogLog binary format to Redis compatible HyperLogLog instance.

  ## Example

      iex> {:ok, conn} = Redix.start_link()
      iex> Redix.command!(conn, ["PFADD", "test_hll_redis_decode", "okk"])
      iex> bin = Redix.command!(conn, ["GET", "test_hll_redis_decode"])
      <<72, 89, 76, 76, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 108, 180, 132, 83, 73>>
      iex> HLL.Redis.decode(bin)
      {HLL.Redis, %{11445 => 2}}
      iex> HLL.Redis.new() |> HLL.Redis.add("okk")
      {HLL.Redis, %{11445 => 2}}

  """

  @spec decode(binary()) :: t()

  def decode(redis_binary)

  def decode(<<"HYLL", 1, _::88, body::bits>>) do
    {__MODULE__, decode_sparse_body(body, 0, [])}
  end

  def decode(<<"HYLL", 0, _::88, body::bits>>) do
    {__MODULE__, decode_dense_body(body, 0, [])}
  end

  defp decode_sparse_body(<<0::1, 0::1, zero::6, rest::bits>>, index, acc) do
    decode_sparse_body(rest, index + zero + 1, acc)
  end

  defp decode_sparse_body(<<0::1, 1::1, xzero::14, rest::bits>>, index, acc) do
    decode_sparse_body(rest, index + xzero + 1, acc)
  end

  defp decode_sparse_body(<<1::1, val::5, x::2, rest::bits>>, index, acc) do
    count = val + 1

    case x do
      0 ->
        decode_sparse_body(rest, index + 1, [{index, count} | acc])

      1 ->
        decode_sparse_body(rest, index + 2, [{index, count}, {index + 1, count} | acc])

      2 ->
        decode_sparse_body(rest, index + 3, [
          {index, count},
          {index + 1, count},
          {index + 2, count} | acc
        ])

      3 ->
        decode_sparse_body(rest, index + 4, [
          {index, count},
          {index + 1, count},
          {index + 2, count},
          {index + 3, count} | acc
        ])
    end
  end

  defp decode_sparse_body(<<>>, 16384, acc) do
    Map.new(acc)
  end

  # Redis dense format:
  # +--------+--------+--------+------//
  # |11000000|22221111|33333322|55444444
  # +--------+--------+--------+------//

  defp decode_dense_body(
         <<b1_low::2, b0::6, b2_low::4, b1_high::4, b3::6, b2_high::2, rest::bits>>,
         index,
         acc
       ) do
    b1 = b1_high <<< 2 ||| b1_low
    b2 = b2_high <<< 4 ||| b2_low

    acc = if b0 == 0, do: acc, else: [{index, b0} | acc]
    acc = if b1 == 0, do: acc, else: [{index + 1, b1} | acc]
    acc = if b2 == 0, do: acc, else: [{index + 2, b2} | acc]
    acc = if b3 == 0, do: acc, else: [{index + 3, b3} | acc]

    decode_dense_body(rest, index + 4, acc)
  end

  defp decode_dense_body(<<>>, 16384, acc) do
    Map.new(acc)
  end

  @doc """

  Estimate cardinality of Redis compatible instance.

  ## Example

      iex> data = Enum.map(1..5000, &Integer.to_string/1)
      iex> h = HLL.Redis.new()
      iex> h = Enum.reduce(data, h, fn x, acc -> HLL.Redis.add(acc, x) end)
      iex> HLL.Redis.cardinality(h)
      4985
      iex> {:ok, conn} = Redix.start_link()
      iex> for x <- data do Redix.command!(conn, ["PFADD", "test_hll_redis_cardinality", x]) end
      iex> Redix.command!(conn, ["PFCOUNT", "test_hll_redis_cardinality"])
      4985

  """

  @spec cardinality(t()) :: non_neg_integer()

  def cardinality({__MODULE__, map} = _hll_redis) do
    Helper.estimate_cardinality(14, map_size(map), Map.values(map))
  end
end
