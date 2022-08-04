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
#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "io/address.hpp"
#include "io/rsm/coordinator_rsm.hpp"
#include "io/rsm/raft.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorRsm;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

int main() {
  SimulatorConfig config{
      /*
          .drop_percent = 5,
          .perform_timeouts = true,
          .scramble_messages = true,
          .rng_seed = 0,
          .start_time = 256 * 1024,
          .abort_time = std::chrono::microseconds{8 * 1024 * 1024},
          */
  };

  auto simulator = Simulator(config);

  auto cli_addr = Address::TestAddress(1);
  auto srv_addr_1 = Address::TestAddress(2);
  auto srv_addr_2 = Address::TestAddress(3);
  auto srv_addr_3 = Address::TestAddress(4);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> srv_io_1 = simulator.Register(srv_addr_1);
  Io<SimulatorTransport> srv_io_2 = simulator.Register(srv_addr_2);
  Io<SimulatorTransport> srv_io_3 = simulator.Register(srv_addr_3);

  std::vector<Address> srv_1_peers = {srv_addr_2, srv_addr_3};
  std::vector<Address> srv_2_peers = {srv_addr_1, srv_addr_3};
  std::vector<Address> srv_3_peers = {srv_addr_1, srv_addr_2};

  using ConcreteCoordinatorRsm = CoordinatorRsm<SimulatorTransport>;
  ConcreteCoordinatorRsm srv_1{std::move(srv_io_1), srv_1_peers, Coordinator{}};
  ConcreteCoordinatorRsm srv_2{std::move(srv_io_2), srv_2_peers, Coordinator{}};
  ConcreteCoordinatorRsm srv_3{std::move(srv_io_3), srv_3_peers, Coordinator{}};

  return 0;
}
