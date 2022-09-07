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
#include "storage/v3/id_types.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::io::tests {

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::ShardMap;
using memgraph::io::Io;
using memgraph::io::local_transport::LocalSystem;
using memgraph::io::local_transport::LocalTransport;
using memgraph::machine_manager::MachineConfig;
using memgraph::machine_manager::MachineManager;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;

using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;

ShardMap TestShardMap() {
  ShardMap sm{};

  const std::string label_name = std::string("test_label");

  // register new properties
  const std::vector<std::string> property_names = {"property_1", "property_2"};
  const auto properties = sm.AllocatePropertyIds(property_names);
  const auto property_id_1 = properties.at("property_1");
  const auto property_id_2 = properties.at("property_2");
  const auto type_1 = memgraph::common::SchemaType::INT;
  const auto type_2 = memgraph::common::SchemaType::INT;

  // register new label space
  std::vector<SchemaProperty> schema = {
      SchemaProperty{.property_id = property_id_1, .type = type_1},
      SchemaProperty{.property_id = property_id_2, .type = type_2},
  };

  size_t replication_factor = 1;

  bool initialize_success = sm.InitializeNewLabel(label_name, schema, replication_factor, sm.shard_map_version);
  MG_ASSERT(initialize_success);

  std::vector<CompoundKey> split_points = {

  };

  for (auto &split_point : split_points) {
  }

  return sm;
}

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

  ShardMap sm = TestShardMap();

  auto mm_1 = MkMm(local_system, coordinator_addresses, machine_1_addr, sm);
  auto mm_2 = MkMm(local_system, coordinator_addresses, machine_2_addr, sm);
  auto mm_3 = MkMm(local_system, coordinator_addresses, machine_3_addr, sm);

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));
  auto mm_thread_2 = std::jthread(RunMachine, std::move(mm_2));
  auto mm_thread_3 = std::jthread(RunMachine, std::move(mm_3));

  // TODO(tyler) register SM w/ coordinators
  // TODO(tyler) coordinator assigns replicas
  // TODO(tyler) have SM reconcile ShardMap, adjust Raft membership

  local_system.ShutDown();
};

}  // namespace memgraph::io::tests
