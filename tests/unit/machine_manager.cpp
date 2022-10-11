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
#include <limits>
#include <thread>

#include <gtest/gtest.h>

#include <coordinator/coordinator.hpp>
#include <coordinator/coordinator_client.hpp>
#include <coordinator/hybrid_logical_clock.hpp>
#include <coordinator/shard_map.hpp>
#include <io/local_transport/local_system.hpp>
#include <io/local_transport/local_transport.hpp>
#include <io/rsm/rsm_client.hpp>
#include <io/transport.hpp>
#include <machine_manager/machine_config.hpp>
#include <machine_manager/machine_manager.hpp>
#include <query/v2/requests.hpp>
#include "io/rsm/rsm_client.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::io::tests {

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorClient;
using memgraph::coordinator::CoordinatorReadRequests;
using memgraph::coordinator::CoordinatorReadResponses;
using memgraph::coordinator::CoordinatorWriteRequests;
using memgraph::coordinator::CoordinatorWriteResponses;
using memgraph::coordinator::Hlc;
using memgraph::coordinator::HlcResponse;
using memgraph::coordinator::Shard;
using memgraph::coordinator::ShardMap;
using memgraph::io::Io;
using memgraph::io::local_transport::LocalSystem;
using memgraph::io::local_transport::LocalTransport;
using memgraph::machine_manager::MachineConfig;
using memgraph::machine_manager::MachineManager;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;

using memgraph::io::rsm::RsmClient;
using memgraph::msgs::ReadRequests;
using memgraph::msgs::ReadResponses;
using memgraph::msgs::WriteRequests;
using memgraph::msgs::WriteResponses;

using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;
using ShardClient = RsmClient<LocalTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

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

  const size_t replication_factor = 1;

  std::optional<LabelId> label_id = sm.InitializeNewLabel(label_name, schema, replication_factor, sm.shard_map_version);
  MG_ASSERT(label_id);

  // split the shard at N split points
  // NB: this is the logic that should be provided by the "split file"
  // TODO(tyler) split points should account for signedness
  const size_t n_splits = 16;
  const auto split_interval = std::numeric_limits<int64_t>::max() / n_splits;

  for (int64_t i = 0; i < n_splits; ++i) {
    const int64_t value = i * split_interval;

    const auto key1 = memgraph::storage::v3::PropertyValue(value);
    const auto key2 = memgraph::storage::v3::PropertyValue(0);

    const CompoundKey split_point = {key1, key2};

    const bool split_success = sm.SplitShard(sm.shard_map_version, label_id.value(), split_point);

    MG_ASSERT(split_success);
  }

  return sm;
}

template <typename ShardRequestManager>
void TestScanAll(ShardRequestManager &shard_request_manager) {
  msgs::ExecutionState<msgs::ScanVerticesRequest> state{.label = "test_label"};

  auto result = shard_request_manager.Request(state);
  MG_ASSERT(result.size() == 2, "{}", result.size());
}

template <typename ShardRequestManager>
void TestCreateVertices(ShardRequestManager &shard_request_manager) {
  using PropVal = msgs::Value;
  msgs::ExecutionState<msgs::CreateVerticesRequest> state;
  std::vector<msgs::NewVertex> new_vertices;
  auto label_id = shard_request_manager.LabelNameToLabelId("test_label");
  msgs::NewVertex a1{.primary_key = {PropVal(int64_t(0)), PropVal(int64_t(0))}};
  a1.label_ids.push_back({label_id});
  msgs::NewVertex a2{.primary_key = {PropVal(int64_t(13)), PropVal(int64_t(13))}};
  a2.label_ids.push_back({label_id});
  new_vertices.push_back(std::move(a1));
  new_vertices.push_back(std::move(a2));

  auto result = shard_request_manager.Request(state, std::move(new_vertices));
  MG_ASSERT(result.size() == 1);
}

template <typename ShardRequestManager>
void TestExpand(ShardRequestManager &shard_request_manager) {}

template <typename ShardRequestManager>
void TestAggregate(ShardRequestManager &shard_request_manager) {}

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

  return MachineManager{io, config, coordinator, shard_map};
}

void RunMachine(MachineManager<LocalTransport> mm) { mm.Run(); }

void WaitForShardsToInitialize(CoordinatorClient<LocalTransport> &cc) {
  // TODO(tyler) call coordinator client's read method for GetShardMap
  // and keep reading it until the shard map contains proper replicas
  // for each shard in the label space.

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2010ms);
}

TEST(MachineManager, BasicFunctionality) {
  LocalSystem local_system;

  auto cli_addr = Address::TestAddress(1);
  auto machine_1_addr = cli_addr.ForkUniqueAddress();

  Io<LocalTransport> cli_io = local_system.Register(cli_addr);

  auto coordinator_addresses = std::vector{
      machine_1_addr,
  };

  ShardMap initialization_sm = TestShardMap();

  auto mm_1 = MkMm(local_system, coordinator_addresses, machine_1_addr, initialization_sm);
  Address coordinator_address = mm_1.CoordinatorAddress();

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));

  // TODO(tyler) clarify addresses of coordinator etc... as it's a mess
  CoordinatorClient<LocalTransport> cc{cli_io, coordinator_address, {coordinator_address}};

  WaitForShardsToInitialize(cc);

  CoordinatorClient<LocalTransport> coordinator_client(cli_io, coordinator_address, {coordinator_address});

  msgs::ShardRequestManager<LocalTransport> shard_request_manager(std::move(coordinator_client), std::move(cli_io));

  shard_request_manager.StartTransaction();
  TestCreateVertices(shard_request_manager);
  TestScanAll(shard_request_manager);
  local_system.ShutDown();
};

}  // namespace memgraph::io::tests
