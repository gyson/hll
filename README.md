# HLL

Redis compatible HyperLogLog implementation in Elixir.

This library used algorithms from following papers:

- [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
    - It describes original HyperLogLog algorithm.
- [HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm](http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf)
    - It suggests 64 bit hash to avoid large range correction.
- [New cardinality estimation algorithms for HyperLogLog sketches](https://arxiv.org/pdf/1702.01284.pdf)
    - It suggests the improved raw estimation algorithm (Algorithm 6 from the paper) for avoiding emprical numbers.
    - It guarantees monotonicity of the cardinality estimate.

The `HLL.Redis` module is Redis (v5) compatible. It uses the same hash algorithm and same HyperLogLog estimation algorithm as Redis (v5) does. Therefore, it could consume HyperLogLog sketches from Redis and it could generate HyperLogLog sketches for Redis as well.

## Installation

The package can be installed by adding `hll` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hll, "~> 0.1.0"}
  ]
end
```

Documentation can be found at [https://hexdocs.pm/hll](https://hexdocs.pm/hll).

