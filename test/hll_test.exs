defmodule HLLTest do
  use ExUnit.Case
  doctest HLL

  test "greets the world" do
    assert HLL.hello() == :world
  end
end
