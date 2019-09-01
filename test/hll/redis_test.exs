defmodule HLL.RedisTest do
  use ExUnit.Case
  doctest HLL.Redis

  setup_all do
    redis_host = System.fetch_env!("REDIS_HOST")
    redis_port = System.fetch_env!("REDIS_PORT")
    {:ok, conn} = Redix.start_link("redis://#{redis_host}:#{redis_port}", name: :redix)
    {:ok, conn: conn}
  end

  test "serialization with dense representation" do
    dense =
      Enum.reduce(1..5000, HLL.Redis.new(), fn i, acc ->
        HLL.Redis.add(acc, i)
      end)

    encoded_dense = HLL.Redis.encode(dense)
    decoded_dense = HLL.Redis.decode(encoded_dense)

    assert dense == decoded_dense
  end

  test "serialization with sparse representation" do
    sparse =
      Enum.reduce(1..100, HLL.Redis.new(), fn i, acc ->
        HLL.Redis.add(acc, i)
      end)

    encoded_sparse = HLL.Redis.encode(sparse)
    decoded_sparse = HLL.Redis.decode(encoded_sparse)

    assert sparse == decoded_sparse
  end

  test "it should be compatible with Redis", ctx do
    key_base = "redis_test"

    for n <- [1, 50, 100, 500, 1000, 5000, 10_000, 50_000, 100_000] do
      Task.async(fn ->
        key = "#{key_base}_#{n}"
        data = StreamData.binary() |> Enum.take(n)

        Redix.command!(ctx.conn, ["DEL", key])

        for item <- data do
          Redix.command!(ctx.conn, ["PFADD", key, item])
        end

        redis_encoded = Redix.command!(ctx.conn, ["GET", key])
        redis_cardinality = Redix.command!(ctx.conn, ["PFCOUNT", key])

        hll_redis =
          Enum.reduce(data, HLL.Redis.new(), fn item, acc ->
            HLL.Redis.add(acc, item)
          end)

        hll_redis_encoded = HLL.Redis.encode(hll_redis)
        hll_redis_cardinality = HLL.Redis.cardinality(hll_redis)

        assert redis_encoded == hll_redis_encoded
        assert redis_cardinality == hll_redis_cardinality
      end)
    end
    |> Enum.each(fn task -> Task.await(task, 60_000) end)
  end
end
