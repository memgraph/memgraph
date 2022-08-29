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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "io/local_transport/local_system.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/transport.hpp"

namespace memgraph::io::tests {

struct CounterRequest {
  uint64_t proposal;
};

struct CounterResponse {
  uint64_t highest_seen;
};

void RunServer(Io<LocalTransport> io) {
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

TEST(LocalTransport, BasicRequest) {
  LocalSystem local_system;

  // rely on uuid to be unique on default Address
  auto cli_addr = Address::UniqueLocalAddress();
  auto srv_addr = Address::UniqueLocalAddress();

  Io<LocalTransport> cli_io = local_system.Register(cli_addr);
  Io<LocalTransport> srv_io = local_system.Register(srv_addr);

  auto srv_thread = std::jthread(run_server, std::move(srv_io));

  for (int i = 1; i < 3; ++i) {
    // send request
    CounterRequest cli_req;
    auto value = 1;  // i;
    cli_req.proposal = value;
    std::cout << "[CLIENT] sending request" << std::endl;
    auto res_f = cli_io.Request<CounterRequest, CounterResponse>(srv_addr, cli_req);
    std::cout << "[CLIENT] waiting on future" << std::endl;

    auto res_rez = std::move(res_f).Wait();
    std::cout << "[CLIENT] future returned" << std::endl;
    MG_ASSERT(!res_rez.HasError());
    std::cout << "[CLIENT] Got a valid response" << std::endl;
    auto env = res_rez.GetValue();
    MG_ASSERT(env.message.highest_seen == value);
  }

  local_system.ShutDown();
}
