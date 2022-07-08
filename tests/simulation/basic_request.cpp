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

//#include <gtest/gtest.h>

#include <thread>

#include "io/v3/simulator.hpp"

struct CounterRequest {
  uint64_t proposal_;
};

struct CounterResponse {
  uint64_t highest_seen_;
};

void run_server(Io<SimulatorTransport> srv_io) {
  uint64_t highest_seen_;

  while (!srv_io.ShouldShutDown()) {
    auto request_result = srv_io.Receive<CounterRequest>();
    if (request_result.HasError()) {
      continue;
    }
    auto request_envelope = request_result.GetValue();
    auto req = std::get<CounterRequest>(request_envelope.message);

    state.highest_seen_ = std::max(state.highest_seen_, req.proposal_);

    auto srv_res = CounterResponse{state.highest_seen_};

    request_envelope.Reply(srv_res, srv_io);
  }
}

int main() {
  auto simulator = Simulator();
  auto cli_addr = Address::TestAddress(1);
  auto srv_addr = Address::TestAddress(2);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr, false);
  Io<SimulatorTransport> srv_io = simulator.Register(srv_addr, true);

  auto srv_thread = std::jthread(run_server, std::move(srv_io));

  // send request
  CounterRequest cli_req;
  cli_req.proposal_ = 1;

  auto response_future = cli_io.Request<CounterRequest, CounterResponse>(srv_addr, cli_req);

  // receive response
  auto response_result = response_future.Wait();
  auto response_envelope = response_result.GetValue();

  MG_ASSERT(response_envelope.message.highest_seen_ == 1);

  simulator.ShutDown();

  srv_thread.join();

  return 0;
}
