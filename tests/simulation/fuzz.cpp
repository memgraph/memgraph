// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <thread>

#include "io/simulator/simulator.hpp"

using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorTransport;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size == 77) {
    spdlog::error("got 77");
    MG_ASSERT(false);
  }
  spdlog::error("got {}", size);
  // MG_ASSERT(false, "got {}", size);
  return 0;
}
