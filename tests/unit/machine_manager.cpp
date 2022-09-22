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

void WaitForShardsToInitialize(CoordinatorClient<LocalTransport> &cc) {
  // TODO(tyler) call coordinator client's read method for GetShardMap
  // and keep reading it until the shard map contains proper replicas
  // for each shard in the label space.
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

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2010ms);

  // get ShardMap from coordinator
  memgraph::coordinator::HlcRequest req;
  req.last_shard_map_version = Hlc{
      .logical_id = 0,
  };

  BasicResult<TimedOut, memgraph::coordinator::CoordinatorWriteResponses> read_res = cc.SendWriteRequest(req);
  MG_ASSERT(!read_res.HasError(), "HLC request unexpectedly timed out");

  auto coordinator_read_response = read_res.GetValue();
  HlcResponse hlc_response = std::get<HlcResponse>(coordinator_read_response);
  ShardMap sm = hlc_response.fresher_shard_map.value();

  // Get shard for key and create rsm client
  const auto cm_key_1 = memgraph::storage::v3::PropertyValue(3);
  const auto cm_key_2 = memgraph::storage::v3::PropertyValue(4);

  const CompoundKey compound_key = {cm_key_1, cm_key_2};

  std::string label_name = "test_label";

  Shard shard_for_key = sm.GetShardForKey(label_name, compound_key);

  auto shard_for_client = std::vector<Address>{};

  for (const auto &aas : shard_for_key) {
    spdlog::info("got address for shard: {}", aas.address.ToString());
    shard_for_client.push_back(aas.address);
  }

  ShardClient shard_client{cli_io, shard_for_client[0], shard_for_client};

  // submit a read request and assert that the requested key does not yet exist

  LabelId label_id = sm.labels.at(label_name);

  ReadRequests storage_get_req;
  /*
  TODO(tyler,kostas) set this to a real request
  storage_get_req.label_id = label_id;
  storage_get_req.key = compound_key;
  storage_get_req.transaction_id = hlc_response.new_hlc;
  */

  auto get_response_result = shard_client.SendReadRequest(storage_get_req);
  auto get_response = get_response_result.GetValue();
  /*
  auto val = get_response.value;

  MG_ASSERT(!val.has_value());
  */

  local_system.ShutDown();
};

}  // namespace memgraph::io::tests
