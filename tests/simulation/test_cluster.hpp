// Copyright 2023 Memgraph Ltd.
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
#include <memory>
#include <set>
#include <thread>

#include <rapidcheck.h>

#include "cluster_config.hpp"
#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "generated_operations.hpp"
#include "io/address.hpp"
#include "io/message_histogram_collector.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "machine_manager/machine_config.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/v2/request_router.hpp"
#include "query/v2/requests.hpp"
#include "testing_constants.hpp"
#include "utils/print_helpers.hpp"
#include "utils/variant_helpers.hpp"

#include "generated_operations.hpp"
#include "simulation_interpreter.hpp"

namespace memgraph::tests::simulation {

using coordinator::Coordinator;
using coordinator::CoordinatorClient;
using coordinator::CoordinatorReadRequests;
using coordinator::CoordinatorWriteRequests;
using coordinator::CoordinatorWriteResponses;
using coordinator::GetShardMapRequest;
using coordinator::GetShardMapResponse;
using coordinator::Hlc;
using coordinator::HlcResponse;
using coordinator::ShardMap;
using coordinator::ShardMetadata;
using io::Address;
using io::Io;
using io::rsm::RsmClient;
using io::simulator::Simulator;
using io::simulator::SimulatorConfig;
using io::simulator::SimulatorStats;
using io::simulator::SimulatorTransport;
using machine_manager::MachineConfig;
using machine_manager::MachineManager;
using memgraph::io::LatencyHistogramSummaries;
using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;
using storage::v3::LabelId;
using storage::v3::SchemaProperty;

using CompoundKey = std::pair<int, int>;
using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

struct SimClientContext {
  CoordinatorClient<SimulatorTransport> coordinator_client;
  const ClusterConfig &cluster_config;
  io::simulator::SimulatedInterpreter interpreter;
  std::set<CompoundKey> correctness_model;
};

MachineManager<SimulatorTransport> MkMm(Simulator &simulator, std::vector<Address> coordinator_addresses, Address addr,
                                        ShardMap shard_map) {
  MachineConfig config{
      .coordinator_addresses = coordinator_addresses,
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = addr.last_known_ip,
      .listen_port = addr.last_known_port,
      .shard_worker_threads = 4,
      .sync_message_handling = true,
  };

  Io<SimulatorTransport> io = simulator.Register(addr);

  Coordinator coordinator{shard_map};

  return MachineManager{io, config, coordinator};
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

ShardMap TestShardMap(const ClusterConfig &cluster_config) {
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

  std::optional<LabelId> label_id = sm.InitializeNewLabel(label_name, schema, cluster_config.replication_factor,
                                                          cluster_config.split_threshold, sm.shard_map_version);
  RC_ASSERT(label_id.has_value());

  return sm;
}

void ExecuteOp(SimClientContext &context, CreateVertex create_vertex) {
  const auto key1 = memgraph::storage::v3::PropertyValue(create_vertex.first);
  const auto key2 = memgraph::storage::v3::PropertyValue(create_vertex.second);

  std::vector<msgs::Value> primary_key = {msgs::Value(int64_t(create_vertex.first)),
                                          msgs::Value(int64_t(create_vertex.second))};

  if (context.correctness_model.contains(std::make_pair(create_vertex.first, create_vertex.second))) {
    // TODO(tyler) remove this early-return when we have properly handled setting non-unique vertexes
    return;
  }

  std::string query = fmt::format("CREATE (n:test_label{{property_1: {}, property_2: {}}});", create_vertex.first,
                                  create_vertex.second);

  auto result_stream = context.interpreter.RunQuery(query);

  // TODO(tyler) is there a better way to assert the actual success of the operation?
  RC_ASSERT(result_stream.size() == 0);

  // RC_ASSERT(!result_stream[0].error.has_value());

  context.correctness_model.emplace(std::make_pair(create_vertex.first, create_vertex.second));
}

void ExecuteOp(SimClientContext &context, ScanAll scan_all) {
  auto results = context.interpreter.RunQuery("MATCH (n) RETURN n;");

  RC_ASSERT(results.size() == context.correctness_model.size());

  for (const auto &typed_value : results) {
    // TODO(tyler) assert on actual values returned
    // const auto properties = vertex_accessor.Properties();
    // const auto primary_key = vertex_accessor.Id().second;
    // const CompoundKey model_key = std::make_pair(primary_key[0].int_v, primary_key[1].int_v);
    // RC_ASSERT(context.correctness_model.contains(model_key));
  }
}

void ExecuteOp(SimClientContext &context, AssertShardsSplit assert_shards_split) {
  const int minimum_expected_shards = (context.correctness_model.size() / context.cluster_config.split_threshold) + 1;
  // TODO(tyler) make this a higher number of retries
  const int maximum_attempts = 100;
  size_t initialized_shards;

  for (int i = 0; i < maximum_attempts; i++) {
    GetShardMapRequest req{};
    CoordinatorReadRequests read_req = req;
    auto read_res = context.coordinator_client.SendReadRequest(read_req);
    if (read_res.HasError()) {
      // timed out
      continue;
    }
    auto response_result = read_res.GetValue();
    auto response = std::get<GetShardMapResponse>(response_result);
    auto shard_map = response.shard_map;

    initialized_shards = shard_map.InitializedShards();

    if (initialized_shards >= minimum_expected_shards) {
      MG_ASSERT(initialized_shards < 3, "just kidding, this is great, we now have {} initialized shards",
                initialized_shards);
      return;
    }
  }

  spdlog::error("expected {} shards, but we only have {}", minimum_expected_shards, initialized_shards);
  MG_ASSERT(false, "exceeded maximum attempts for waiting for the expected number of shard splits to occur");
}

/// This struct exists as a way of detaching
/// a thread if something causes an uncaught
/// exception - because that thread would not
/// receive a ShutDown message otherwise, and
/// would cause the test to hang forever.
struct DetachIfDropped {
  std::jthread &handle;
  bool detach = true;

  ~DetachIfDropped() {
    if (detach && handle.joinable()) {
      handle.detach();
    }
  }
};

std::pair<SimulatorStats, LatencyHistogramSummaries> RunClusterSimulation(const SimulatorConfig &sim_config,
                                                                          const ClusterConfig &cluster_config,
                                                                          const std::vector<Op> &ops) {
  spdlog::info("========================== NEW SIMULATION ==========================");

  auto simulator = Simulator(sim_config);

  auto machine_1_addr = Address::TestAddress(1);
  auto cli_addr = Address::TestAddress(2);
  auto cli_addr_2 = Address::TestAddress(3);
  auto cli_addr_3 = Address::TestAddress(4);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> cli_io_2 = simulator.Register(cli_addr_2);
  Io<SimulatorTransport> cli_io_3 = simulator.Register(cli_addr_3);

  auto coordinator_addresses = std::vector{
      machine_1_addr,
  };

  ShardMap initialization_sm = TestShardMap(cluster_config);

  auto mm_1 = MkMm(simulator, coordinator_addresses, machine_1_addr, initialization_sm);
  Address coordinator_address = mm_1.CoordinatorAddress();

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));
  simulator.IncrementServerCountAndWaitForQuiescentState(machine_1_addr);

  auto detach_on_error = DetachIfDropped{.handle = mm_thread_1};

  // TODO(tyler) clarify addresses of coordinator etc... as it's a mess

  auto correctness_model = std::set<CompoundKey>{};

  SimClientContext context{
      .coordinator_client = CoordinatorClient<SimulatorTransport>(cli_io, coordinator_address, {coordinator_address}),
      .cluster_config = cluster_config,
      .interpreter = io::simulator::SetUpInterpreter(coordinator_address, simulator),
      .correctness_model = correctness_model,
  };

  WaitForShardsToInitialize(context.coordinator_client);

  for (const Op &op : ops) {
    std::visit([&](auto &o) { ExecuteOp(context, o); }, op.inner);
  }

  // We have now completed our workload without failing any assertions, so we can
  // disable detaching the worker thread, which will cause the mm_thread_1 jthread
  // to be joined when this function returns.
  detach_on_error.detach = false;

  simulator.ShutDown();

  mm_thread_1.join();

  SimulatorStats stats = simulator.Stats();

  spdlog::info("total messages:     {}", stats.total_messages);
  spdlog::info("dropped messages:   {}", stats.dropped_messages);
  spdlog::info("timed out requests: {}", stats.timed_out_requests);
  spdlog::info("total requests:     {}", stats.total_requests);
  spdlog::info("total responses:    {}", stats.total_responses);
  spdlog::info("simulator ticks:    {}", stats.simulator_ticks);

  auto histo = cli_io_2.ResponseLatencies();

  spdlog::info("========================== SUCCESS :) ==========================");
  return std::make_pair(stats, histo);
}

}  // namespace memgraph::tests::simulation
