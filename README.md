# HLL

Redis compatible HyperLogLog implementation in Elixir.

This library uses algorithms from following papers:

- [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
    - It describes original HyperLogLog algorithm.
- [HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf)
    - It suggests 64 bit hash to avoid large range correction.
- [New cardinality estimation algorithms for HyperLogLog sketches](https://arxiv.org/pdf/1702.01284.pdf)
    - It suggests the improved raw estimation algorithm (Algorithm 6 from the paper) for avoiding emprical numbers.
    - It guarantees monotonicity of the cardinality estimate.

The `HLL.Redis` module is Redis (v5) compatible. It uses the same hash algorithm, same HyperLogLog estimation algorithm and same binary format as Redis (v5) does. Therefore, it could consume HyperLogLog sketches from Redis and it could generate HyperLogLog sketches for Redis as well.

## Features

- HyperLogLog operations (add, merge, cardinality)
- Redis compatible (use `HLL.Redis` module)
- Serialization

## Installation

The package can be installed by adding `hll` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hll, "~> 0.1"}
  ]
end
```

Documentation can be found at [https://hexdocs.pm/hll](https://hexdocs.pm/hll).

## `HLL` vs `HLL.Redis`

This library provides two different HyperLogLog modules, `HLL` and `HLL.Redis`.

#### Similarity

- Both modules use 64 bit hash to avoid large range correction.
- Both modules use "improved raw estimation algorithm" from [New cardinality estimation algorithms for HyperLogLog sketches](https://arxiv.org/pdf/1702.01284.pdf) paper as cardinality estimation algorithm.

#### Difference

- `HLL.Redis` is Redis (v5) compatible (same hash fucntion, same cardinality estimation algorithm, same serialization format). `HLL` is *NOT* Redis compatible.
- `HLL` uses `:erlang.phash2` (in native code) as hash function, which is faster than the `HLL.Redis`'s hash function (written in Elixir).
- `HLL`'s serialization format is closer to `HLL` internal data structure, which makes `encode` and `decode` generally faster than `HLL.Redis`'s Redis binary format.

Therefore, if you do not require "Redis compatible", it's recommanded to use `HLL` module for performance gain.

## License

MIT
