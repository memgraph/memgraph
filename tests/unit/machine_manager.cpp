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

#include <coordinator/coordinator.hpp>
#include <coordinator/shard_map.hpp>
#include <io/local_transport/local_system.hpp>
#include <io/local_transport/local_transport.hpp>
#include <io/transport.hpp>
#include <machine_manager/machine_config.hpp>
#include <machine_manager/machine_manager.hpp>

namespace memgraph::io::tests {

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::ShardMap;
using memgraph::io::Io;
using memgraph::io::local_transport::LocalSystem;
using memgraph::io::local_transport::LocalTransport;
using memgraph::machine_manager::MachineConfig;
using memgraph::machine_manager::MachineManager;

MachineManager<LocalTransport> MkMm(LocalSystem &local_system, std::vector<Address> coordinator_addresses, Address addr,
                                    ShardMap shard_map) {
  MachineConfig config{
      .coordinator_addresses = coordinator_addresses,
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = addr.last_known_ip,
      .listen_port = addr.last_known_port,
  };

  Io<LocalTransport> io = local_system.Register(addr);

  Coordinator coordinator{shard_map};

  return MachineManager{io, config, coordinator};
}

void RunMachine(MachineManager<LocalTransport> mm) { mm.Run(); }

TEST(MachineManager, BasicFunctionality) {
  LocalSystem local_system;

  auto cli_addr = Address::TestAddress(0);
  auto machine_1_addr = Address::TestAddress(1);
  auto machine_2_addr = Address::TestAddress(2);
  auto machine_3_addr = Address::TestAddress(3);

  Io<LocalTransport> cli_io = local_system.Register(cli_addr);

  auto coordinator_addresses = std::vector{
      machine_1_addr,
      machine_2_addr,
      machine_3_addr,
  };

  ShardMap sm{};

  auto mm_1 = MkMm(local_system, coordinator_addresses, machine_1_addr, sm);
  auto mm_2 = MkMm(local_system, coordinator_addresses, machine_2_addr, sm);
  auto mm_3 = MkMm(local_system, coordinator_addresses, machine_3_addr, sm);

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));
  auto mm_thread_2 = std::jthread(RunMachine, std::move(mm_2));
  auto mm_thread_3 = std::jthread(RunMachine, std::move(mm_3));
};

}  // namespace memgraph::io::tests
