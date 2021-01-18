#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <iostream>

#include "data_structures/ring_buffer.hpp"

class RingBufferMultiThreaded : public benchmark::Fixture {
 protected:
  RingBuffer<int> buffer{1024};
};

BENCHMARK_DEFINE_F(RingBufferMultiThreaded, MT)(benchmark::State &st) {
  while (st.KeepRunning()) {
    buffer.emplace(42);
    buffer.pop();
  }
}

BENCHMARK_REGISTER_F(RingBufferMultiThreaded, MT)->Threads(1);
BENCHMARK_REGISTER_F(RingBufferMultiThreaded, MT)->Threads(4);
BENCHMARK_REGISTER_F(RingBufferMultiThreaded, MT)->Threads(16);
BENCHMARK_REGISTER_F(RingBufferMultiThreaded, MT)->Threads(64);

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
