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
#include <memory>
#include <set>
#include <thread>

#include <gtest/gtest.h>

#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/local_transport/local_system.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "machine_manager/machine_config.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/v2/requests.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "utils/variant_helpers.hpp"

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
using coordinator::Shard;
using coordinator::ShardMap;
using io::Address;
using io::Io;
using io::local_transport::LocalSystem;
using io::local_transport::LocalTransport;
using io::rsm::RsmClient;
using machine_manager::MachineConfig;
using machine_manager::MachineManager;
using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;
using storage::v3::LabelId;
using storage::v3::SchemaProperty;

using CompoundKey = std::pair<int, int>;
using ShardClient = RsmClient<LocalTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

struct CreateVertex {
  int first;
  int second;

  friend std::ostream &operator<<(std::ostream &in, const CreateVertex &add) {
    in << "CreateVertex { first: " << add.first << ", second: " << add.second << " }";
    return in;
  }
};

struct ScanAll {
  friend std::ostream &operator<<(std::ostream &in, const ScanAll &get) {
    in << "ScanAll {}";
    return in;
  }
};

MachineManager<LocalTransport> MkMm(LocalSystem &local_system, std::vector<Address> coordinator_addresses, Address addr,
                                    ShardMap shard_map, size_t shard_worker_threads) {
  MachineConfig config{
      .coordinator_addresses = std::move(coordinator_addresses),
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = addr.last_known_ip,
      .listen_port = addr.last_known_port,
      .shard_worker_threads = shard_worker_threads,
  };

  Io<LocalTransport> io = local_system.Register(addr);

  Coordinator coordinator{shard_map};

  return MachineManager{io, config, std::move(coordinator)};
}

void RunMachine(MachineManager<LocalTransport> mm) { mm.Run(); }

void WaitForShardsToInitialize(CoordinatorClient<LocalTransport> &coordinator_client) {
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

ShardMap TestShardMap(int shards, int replication_factor, int gap_between_shards) {
  ShardMap sm{};

  const auto label_name = std::string("test_label");

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
  MG_ASSERT(label_id.has_value());

  // split the shard at N split points
  for (int64_t i = 1; i < shards; ++i) {
    const auto key1 = memgraph::storage::v3::PropertyValue(i * gap_between_shards);
    const auto key2 = memgraph::storage::v3::PropertyValue(0);

    const auto split_point = {key1, key2};

    const bool split_success = sm.SplitShard(sm.shard_map_version, label_id.value(), split_point);

    MG_ASSERT(split_success);
  }

  return sm;
}

void ExecuteOp(msgs::ShardRequestManager<LocalTransport> &shard_request_manager,
               std::set<CompoundKey> &correctness_model, CreateVertex create_vertex) {
  const auto key1 = memgraph::storage::v3::PropertyValue(create_vertex.first);
  const auto key2 = memgraph::storage::v3::PropertyValue(create_vertex.second);

  std::vector<msgs::Value> primary_key = {msgs::Value(int64_t(create_vertex.first)),
                                          msgs::Value(int64_t(create_vertex.second))};

  if (correctness_model.contains(std::make_pair(create_vertex.first, create_vertex.second))) {
    // TODO(tyler) remove this early-return when we have properly handled setting non-unique vertexes
    return;
  }

  msgs::ExecutionState<msgs::CreateVerticesRequest> state;

  auto label_id = shard_request_manager.NameToLabel("test_label");

  msgs::NewVertex nv{.primary_key = primary_key};
  nv.label_ids.push_back({label_id});

  std::vector<msgs::NewVertex> new_vertices;
  new_vertices.push_back(std::move(nv));

  auto result = shard_request_manager.Request(state, std::move(new_vertices));

  MG_ASSERT(result.size() == 1);
  MG_ASSERT(result[0].success);

  correctness_model.emplace(std::make_pair(create_vertex.first, create_vertex.second));
}

void ExecuteOp(msgs::ShardRequestManager<LocalTransport> &shard_request_manager,
               std::set<CompoundKey> &correctness_model, ScanAll scan_all) {
  msgs::ExecutionState<msgs::ScanVerticesRequest> request{.label = "test_label"};

  auto results = shard_request_manager.Request(request);

  MG_ASSERT(results.size() == correctness_model.size());

  for (const auto &vertex_accessor : results) {
    const auto properties = vertex_accessor.Properties();
    const auto primary_key = vertex_accessor.Id().second;
    const CompoundKey model_key = std::make_pair(primary_key[0].int_v, primary_key[1].int_v);
    MG_ASSERT(correctness_model.contains(model_key));
  }
}

void RunWorkload(int shards, int replication_factor, int create_ops, int scan_ops, int shard_worker_threads,
                 int gap_between_shards) {
  spdlog::info("======================== NEW TEST ========================");
  spdlog::info("shards:               ", shards);
  spdlog::info("replication factor:   ", replication_factor);
  spdlog::info("create ops:           ", create_ops);
  spdlog::info("scan all ops:         ", scan_ops);
  spdlog::info("shard worker threads: ", shard_worker_threads);
  spdlog::info("gap between shards:   ", gap_between_shards);

  LocalSystem local_system;

  auto cli_addr = Address::TestAddress(1);
  auto machine_1_addr = cli_addr.ForkUniqueAddress();

  Io<LocalTransport> cli_io = local_system.Register(cli_addr);
  Io<LocalTransport> cli_io_2 = local_system.Register(Address::TestAddress(2));

  auto coordinator_addresses = std::vector{
      machine_1_addr,
  };

  auto time_before_shard_map_creation = cli_io_2.Now();
  ShardMap initialization_sm = TestShardMap(shards, replication_factor, gap_between_shards);
  auto time_after_shard_map_creation = cli_io_2.Now();

  auto mm_1 = MkMm(local_system, coordinator_addresses, machine_1_addr, initialization_sm, shard_worker_threads);
  Address coordinator_address = mm_1.CoordinatorAddress();

  auto mm_thread_1 = std::jthread(RunMachine, std::move(mm_1));

  CoordinatorClient<LocalTransport> coordinator_client(cli_io, coordinator_address, {coordinator_address});

  auto time_before_shard_stabilization = cli_io_2.Now();
  WaitForShardsToInitialize(coordinator_client);
  auto time_after_shard_stabilization = cli_io_2.Now();

  msgs::ShardRequestManager<LocalTransport> shard_request_manager(std::move(coordinator_client), std::move(cli_io));

  shard_request_manager.StartTransaction();

  auto correctness_model = std::set<CompoundKey>{};

  auto time_before_creates = cli_io_2.Now();

  for (int i = 0; i < create_ops; i++) {
    ExecuteOp(shard_request_manager, correctness_model, CreateVertex{.first = i, .second = i});
  }

  auto time_after_creates = cli_io_2.Now();

  for (int i = 0; i < scan_ops; i++) {
    ExecuteOp(shard_request_manager, correctness_model, ScanAll{});
  }

  auto time_after_scan = cli_io_2.Now();

  local_system.ShutDown();

  auto latencies = cli_io_2.ResponseLatencies();

  spdlog::info("response latencies: \n{}", latencies.SummaryTable());

  spdlog::info("serial time break-down: (μs)");

  spdlog::info("{: >20}: {: >10}", "split shard map",
               (time_after_shard_map_creation - time_before_shard_map_creation).count());
  spdlog::info("{: >20}: {: >10}", "shard stabilization",
               (time_after_shard_stabilization - time_before_shard_stabilization).count());
  spdlog::info("{: >20}: {: >10}", "create nodes", (time_after_creates - time_before_creates).count());
  spdlog::info("{: >20}: {: >10}", "scan nodes", (time_after_scan - time_after_creates).count());

  std::cout << fmt::format("{} {} {}\n", shards, shard_worker_threads, (time_after_scan - time_after_creates).count());
}

TEST(MachineManager, ManyShards) {
  auto shards_attempts = {1, 2, 4, 8};
  auto shard_worker_thread_attempts = {1, 2, 4, 8};
  auto replication_factor = 1;
  auto create_ops = 1024;
  auto scan_ops = 1;

  std::cout << "splits threads scan_all_microseconds\n";

  for (const auto shards : shards_attempts) {
    auto gap_between_shards = create_ops / shards;

    for (const auto shard_worker_threads : shard_worker_thread_attempts) {
      RunWorkload(shards, replication_factor, create_ops, scan_ops, shard_worker_threads, gap_between_shards);
    }
  }
}

}  // namespace memgraph::tests::simulation
