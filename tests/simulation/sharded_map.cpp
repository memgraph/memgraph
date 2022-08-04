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
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"

using memgraph::coordinator::Coordinator;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::rsm::CoordinatorRsm;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::StorageGetRequest;
using memgraph::io::rsm::StorageGetResponse;
using memgraph::io::rsm::StorageRsm;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

using ConcreteCoordinatorRsm = CoordinatorRsm<SimulatorTransport>;
using ConcreteStorageRsm = Raft<SimulatorTransport, StorageRsm, StorageWriteRequest, StorageWriteResponse,
                                StorageGetRequest, StorageGetResponse>;

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

  Io<SimulatorTransport> cli_io = simulator.RegisterNew();

  // spin up coordinators

  Io<SimulatorTransport> c_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> c_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> c_io_3 = simulator.RegisterNew();

  Address c_addrs[] = {c_io_1.GetAddress(), c_io_2.GetAddress(), c_io_3.GetAddress()};

  std::vector<Address> c_1_peers = {c_addrs[1], c_addrs[2]};
  std::vector<Address> c_2_peers = {c_addrs[0], c_addrs[2]};
  std::vector<Address> c_3_peers = {c_addrs[0], c_addrs[1]};

  ConcreteCoordinatorRsm c_1{std::move(c_io_1), c_1_peers, Coordinator{}};
  ConcreteCoordinatorRsm c_2{std::move(c_io_2), c_2_peers, Coordinator{}};
  ConcreteCoordinatorRsm c_3{std::move(c_io_3), c_3_peers, Coordinator{}};

  auto c_thread_1 = std::jthread([c_1]() mutable { c_1.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[0]);

  /*
    auto c_thread_2 = std::jthread(RunRaft< Coordinator>, std::move(c_2));
    simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[1]);

    auto c_thread_3 = std::jthread(RunRaft<Coordinator>, std::move(c_3));
    simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[2]);
    */

  // spin up shard A

  Io<SimulatorTransport> a_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> a_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> a_io_3 = simulator.RegisterNew();

  Address a_addrs[] = {a_io_1.GetAddress(), a_io_2.GetAddress(), a_io_3.GetAddress()};

  std::vector<Address> a_1_peers = {a_addrs[1], a_addrs[2]};
  std::vector<Address> a_2_peers = {a_addrs[0], a_addrs[2]};
  std::vector<Address> a_3_peers = {a_addrs[0], a_addrs[1]};

  ConcreteStorageRsm a_1{std::move(a_io_1), a_1_peers, StorageRsm{}};
  ConcreteStorageRsm a_2{std::move(a_io_2), a_2_peers, StorageRsm{}};
  ConcreteStorageRsm a_3{std::move(a_io_3), a_3_peers, StorageRsm{}};

  // spin up shard B

  Io<SimulatorTransport> b_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> b_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> b_io_3 = simulator.RegisterNew();

  Address b_addrs[] = {b_io_1.GetAddress(), b_io_2.GetAddress(), b_io_3.GetAddress()};

  std::vector<Address> b_1_peers = {b_addrs[1], b_addrs[2]};
  std::vector<Address> b_2_peers = {b_addrs[0], b_addrs[2]};
  std::vector<Address> b_3_peers = {b_addrs[0], b_addrs[1]};

  ConcreteStorageRsm b_1{std::move(b_io_1), b_1_peers, StorageRsm{}};
  ConcreteStorageRsm b_2{std::move(b_io_2), b_2_peers, StorageRsm{}};
  ConcreteStorageRsm b_3{std::move(b_io_3), b_3_peers, StorageRsm{}};

  std::cout << "beginning test after servers have become quiescent" << std::endl;

  return 0;
}
