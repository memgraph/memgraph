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
#include <iostream>
#include <limits>
#include <set>
#include <thread>

#include <rapidcheck.h>

#include "cluster_config.hpp"
#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "generated_operations.hpp"
#include "io/address.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "machine_manager/machine_config.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/v2/requests.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "testing_constants.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::tests::simulation {

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorClient;
using memgraph::coordinator::CoordinatorReadRequests;
using memgraph::coordinator::CoordinatorWriteRequests;
using memgraph::coordinator::CoordinatorWriteResponses;
using memgraph::coordinator::GetShardMapRequest;
using memgraph::coordinator::GetShardMapResponse;
using memgraph::coordinator::Hlc;
using memgraph::coordinator::HlcResponse;
using memgraph::coordinator::Shard;
using memgraph::coordinator::ShardMap;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::rsm::RsmClient;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::machine_manager::MachineConfig;
using memgraph::machine_manager::MachineManager;
using memgraph::msgs::ReadRequests;
using memgraph::msgs::ReadResponses;
using memgraph::msgs::WriteRequests;
using memgraph::msgs::WriteResponses;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;

using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;
using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

MachineManager<SimulatorTransport> MkMm(Simulator &simulator, std::vector<Address> coordinator_addresses, Address addr,
                                        ShardMap shard_map) {
  MachineConfig config{
      .coordinator_addresses = coordinator_addresses,
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = addr.last_known_ip,
      .listen_port = addr.last_known_port,
  };

  Io<SimulatorTransport> io = simulator.Register(addr);

  Coordinator coordinator{shard_map};

  return MachineManager{io, config, coordinator, shard_map};
}

void RunMachine(MachineManager<SimulatorTransport> mm) { mm.Run(); }

void WaitForShardsToInitialize(CoordinatorClient<SimulatorTransport> &coordinator_client) {
  // Call coordinator client's read method for GetShardMap and keep
  // reading it until the shard map contains proper replicas for
  // each shard in the label space.

  while (true) {
    GetShardMapRequest req{};
    CoordinatorReadRequests read_req = req;
    auto read_res = coordinator_client.SendReadRequest(read_req);
    if (read_res.HasError()) {
      // timed out
      continue;
    }
    auto response_result = read_res.GetValue();
    auto response = std::get<GetShardMapResponse>(response_result);
    auto shard_map = response.shard_map;

    if (shard_map.ClusterInitialized()) {
      spdlog::info("cluster stabilized - beginning workload");
      return;
    }
  }
}

ShardMap TestShardMap(int n_splits, int replication_factor) {
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

  std::optional<LabelId> label_id = sm.InitializeNewLabel(label_name, schema, replication_factor, sm.shard_map_version);
  RC_ASSERT(label_id.has_value());

  // split the shard at N split points
  // NB: this is the logic that should be provided by the "split file"
  // TODO(tyler) split points should account for signedness
  const auto split_interval = std::numeric_limits<int64_t>::max() / n_splits;

  for (int64_t i = 0; i < n_splits; ++i) {
    const int64_t value = i * split_interval;

    const auto key1 = memgraph::storage::v3::PropertyValue(value);
    const auto key2 = memgraph::storage::v3::PropertyValue(0);

    const CompoundKey split_point = {key1, key2};

    const bool split_success = sm.SplitShard(sm.shard_map_version, label_id.value(), split_point);

    RC_ASSERT(split_success);
  }

  return sm;
}

void executeOp(msgs::ShardRequestManager<SimulatorTransport> &shard_request_manager,
               std::set<CompoundKey> &correctness_model, CreateVertex create_vertex) {
  const auto key1 = memgraph::storage::v3::PropertyValue(create_vertex.key);
  const auto key2 = memgraph::storage::v3::PropertyValue(create_vertex.value);
  const CompoundKey primary_key = {key1, key2};

  if (correctness_model.contains(primary_key)) {
    // TODO(tyler) remove this early-return when we have properly handled setting non-unique vertexes
    return;
  }

  msgs::ExecutionState<msgs::CreateVerticesRequest> state;

  auto label_id = shard_request_manager.NameToLabel("test_label");

  std::vector<msgs::Value> new_vertex_key = {msgs::Value(int64_t(create_vertex.key)),
                                             msgs::Value(int64_t(create_vertex.value))};

  msgs::NewVertex nv{.primary_key = new_vertex_key};
  nv.label_ids.push_back({label_id});

  std::vector<msgs::NewVertex> new_vertices;
  new_vertices.push_back(std::move(nv));

  auto result = shard_request_manager.Request(state, std::move(new_vertices));
  if (result.size() != 1) {
    spdlog::error("did not get a result back when creating the vertex");
    RC_ASSERT(result.size() == 1);
  }

  correctness_model.emplace(primary_key);
}

void executeOp(msgs::ShardRequestManager<SimulatorTransport> &shard_request_manager,
               std::set<CompoundKey> &correctness_model, ScanAll scan_all) {
  msgs::ExecutionState<msgs::ScanVerticesRequest> request{.label = "test_label"};

  auto result = shard_request_manager.Request(request);

  if (result.size() != correctness_model.size()) {
    spdlog::error("got {} results but we expected {}", result.size(), correctness_model.size());
    RC_ASSERT(result.size() == correctness_model.size());
  }
}

void RunClusterSimulation(const SimulatorConfig &sim_config, const ClusterConfig &cluster_config,
                          const std::vector<Op> &ops) {
  spdlog::info("========================== NEW SIMULATION ==========================");

  auto simulator = Simulator(sim_config);

  auto cli_addr = Address::TestAddress(1);
  auto machine_1_addr = cli_addr.ForkUniqueAddress();

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);

  auto coordinator_addresses = std::vector{
      machine_1_addr,
  };

  ShardMap initialization_sm = TestShardMap(cluster_config.shards - 1, cluster_config.replication_factor);

  auto mm_1 = MkMm(simulator, coordinator_addresses, machine_1_addr, initialization_sm);
  Address coordinator_address = mm_1.CoordinatorAddress();

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));

  // Need to detach this thread so that the destructor does not
  // block before we can propagate assertion failures.
  mm_thread_1.detach();

  // TODO(tyler) clarify addresses of coordinator etc... as it's a mess

  CoordinatorClient<SimulatorTransport> coordinator_client(cli_io, coordinator_address, {coordinator_address});
  WaitForShardsToInitialize(coordinator_client);

  msgs::ShardRequestManager<SimulatorTransport> shard_request_manager(std::move(coordinator_client), std::move(cli_io));

  shard_request_manager.StartTransaction();

  std::set<CompoundKey> correctness_model{};

  for (const Op &op : ops) {
    std::visit([&](auto &o) { executeOp(shard_request_manager, correctness_model, o); }, op.inner);
  }

  simulator.ShutDown();

  SimulatorStats stats = simulator.Stats();

  spdlog::info("total messages:     {}", stats.total_messages);
  spdlog::info("dropped messages:   {}", stats.dropped_messages);
  spdlog::info("timed out requests: {}", stats.timed_out_requests);
  spdlog::info("total requests:     {}", stats.total_requests);
  spdlog::info("total responses:    {}", stats.total_responses);
  spdlog::info("simulator ticks:    {}", stats.simulator_ticks);

  spdlog::info("========================== SUCCESS :) ==========================");
}

}  // namespace memgraph::tests::simulation
