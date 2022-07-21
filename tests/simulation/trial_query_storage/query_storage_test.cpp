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

#include <iostream>

#include "io/address.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "io/transport.hpp"

#include "messages.hpp"

namespace memgraph::tests::simulation {
using memgraph::io::Io;
using memgraph::io::simulator::SimulatorTransport;

void run_server(Io<SimulatorTransport> io) {
  while (!io.ShouldShutDown()) {
    std::cout << "[STORAGE] Is receiving..." << std::endl;
    auto request_result = io.ReceiveWithTimeout<ScanVerticesRequest>(100000);
    if (request_result.HasError()) {
      std::cout << "[STORAGE] Error, continue" << std::endl;
      continue;
    }
    auto request_envelope = request_result.GetValue();
    auto req = std::get<ScanVerticesRequest>(request_envelope.message);

    VerticesResponse response{};
    const int64_t start_index = std::invoke([&req] {
      if (req.continuation.has_value()) {
        return *req.continuation;
      }
      return 0L;
    });
    for (auto index = start_index; index < start_index + req.count; ++index) {
      response.vertices.push_back({std::string("Vertex_") + std::to_string(index)});
    }
    io.Send(request_envelope.from_address, request_envelope.request_id, response);
  }
}

int main() {
  using memgraph::io::Address;
  using memgraph::io::simulator::Simulator;
  using memgraph::io::simulator::SimulatorConfig;
  auto config = SimulatorConfig{
      .drop_percent = 0,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
  };
  auto simulator = Simulator(config);

  auto cli_addr = Address::TestAddress(1);
  auto srv_addr = Address::TestAddress(2);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> srv_io = simulator.Register(srv_addr);

  auto srv_thread = std::jthread(run_server, std::move(srv_io));

  return 0;
}
}  // namespace memgraph::tests::simulation
