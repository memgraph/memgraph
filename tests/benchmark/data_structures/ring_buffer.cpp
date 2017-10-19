#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>
#include <iostream>

#include "data_structures/ring_buffer.hpp"

class RingBufferMultiThreaded : public benchmark::Fixture {
 protected:
  RingBuffer<int, 1024> buffer;
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
