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

struct CounterRequest {
  uint64_t proposal;
};

struct CounterResponse {
  uint64_t highest_seen;
};

void run_server(Io<SimulatorTransport> io) {
  uint64_t highest_seen = 0;

  while (!io.ShouldShutDown()) {
    std::cout << "[SERVER] Is receiving..." << std::endl;
    auto request_result = io.Receive<CounterRequest>();
    if (request_result.HasError()) {
      std::cout << "[SERVER] Error, continue" << std::endl;
      continue;
    }
    auto request_envelope = request_result.GetValue();
    auto req = std::get<CounterRequest>(request_envelope.message);

    highest_seen = std::max(highest_seen, req.proposal);
    auto srv_res = CounterResponse{highest_seen};

    io.Send(request_envelope.from_address, request_envelope.request_id, srv_res);
  }
}

int main() {
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
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr);

  for (int i = 1; i < 3; ++i) {
    // send request
    CounterRequest cli_req;
    cli_req.proposal = i;
    auto res_f = cli_io.Request<CounterRequest, CounterResponse>(srv_addr, cli_req);
    auto res_rez = std::move(res_f).Wait();
    if (!res_rez.HasError()) {
      std::cout << "[CLIENT] Got a valid response" << std::endl;
      auto env = res_rez.GetValue();
      MG_ASSERT(env.message.highest_seen == i);
      std::cout << "response latency: " << env.response_latency.count() << " microseconds" << std::endl;
    } else {
      std::cout << "[CLIENT] Got an error" << std::endl;
    }
  }

  simulator.ShutDown();
  return 0;
}
