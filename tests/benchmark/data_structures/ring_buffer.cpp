// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <benchmark/benchmark.h>
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
