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
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "utils/result.hpp"

using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::Time;
using memgraph::io::TimedOut;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::RsmClient;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;
using memgraph::utils::BasicResult;

using memgraph::storage::v3::PropertyValue;
using memgraph::storage::v3::Shard;
using memgraph::storage::v3::ShardRsm;

using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

using ConcreteShardRsm = Raft<SimulatorTransport, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

template <typename IoImpl>
void RunShardRaft(Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

namespace localschema {
// struct SchemaProperty {
//   PropertyId property_id;
//   common::SchemaType type;

//   friend bool operator==(const SchemaProperty &lhs, const SchemaProperty &rhs);
// };

// bool Shard::CreateSchema(const LabelId primary_label, const std::vector<SchemaProperty> &schemas_types);
LabelId primary_label = LabelId::FromUint(0);
SchemaProperty schema_prop = {.property_id = PropertyId::FromUint(0), .type = memgraph::common::SchemaType::INT};
}  // namespace localschema

// tests
namespace {

void TestCreateVertices(ShardClient &client) {
  // struct CreateVerticesRequest {
  //   Hlc transaction_id;
  //   std::vector<NewVertex> new_vertices;
  // };

  // struct CreateVerticesResponse {
  //   bool success;
  // };

  // struct NewVertex {
  //   std::vector<Label> label_ids;
  //   PrimaryKey primary_key;
  //   std::vector<std::pair<PropertyId, Value>> properties;
  // };

  // struct Label {
  //   LabelId id;
  // };

  // using PrimaryKey = std::vector<memgraph::storage::v3::PropertyValue>;

  // Specify Labels.
  Label label1 = {.id = LabelId::FromUint(1)};
  Label label2 = {.id = LabelId::FromUint(2)};

  std::vector<Label> label_ids = {label1, label2};

  std::vector<NewVertex> vertices;

  // Specify primary key.
  PropertyValue prop_val(100);
  PrimaryKey primary_key = {prop_val};

  // Specify properties
  auto val1 = Value(static_cast<int64_t>(1001));
  // auto val2 = Value(static_cast<int64_t>(1002));
  auto prop1 = std::make_pair(PropertyId::FromUint(0), val1);
  // auto prop2 = std::make_pair(PropertyId::FromUint(2), val2);

  // std::vector<std::pair<PropertyId, Value>> properties{prop1, prop2};
  std::vector<std::pair<PropertyId, Value>> properties{prop1};

  // Create NewVertex
  NewVertex vertex = {.label_ids = label_ids, .primary_key = primary_key, .properties = properties};

  auto req = CreateVerticesRequest{};
  req.new_vertices = {vertex};
  req.transaction_id.logical_id = 1;

  while (true) {
    auto write_res = client.SendWriteRequest(req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<CreateVerticesResponse>(write_response_result);

    MG_ASSERT(write_response.success);
    break;
  }
}

}  // namespace

int main() {
  SimulatorConfig config{
      .drop_percent = 0,
      .perform_timeouts = false,
      .scramble_messages = false,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{4 * 8 * 1024 * 1024},
  };

  auto simulator = Simulator(config);

  Io<SimulatorTransport> shard_server_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> shard_server_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> shard_server_io_3 = simulator.RegisterNew();
  Io<SimulatorTransport> shard_client_io = simulator.RegisterNew();

  PropertyValue age(50);

  std::vector<PropertyValue> prim_key = {age};

  std::optional<std::vector<PropertyValue>> max_prim_key = std::nullopt;

  auto shard_ptr1 = std::make_unique<memgraph::storage::v3::Shard>(localschema::primary_label, prim_key, max_prim_key);
  auto shard_ptr2 = std::make_unique<memgraph::storage::v3::Shard>(localschema::primary_label, prim_key, max_prim_key);
  auto shard_ptr3 = std::make_unique<memgraph::storage::v3::Shard>(localschema::primary_label, prim_key, max_prim_key);

  // bool Shard::CreateSchema(const LabelId primary_label, const std::vector<SchemaProperty> &schemas_types);
  shard_ptr1->CreateSchema(localschema::primary_label, {localschema::schema_prop});
  shard_ptr2->CreateSchema(localschema::primary_label, {localschema::schema_prop});

  std::vector<Address> address_for_1{shard_server_io_2.GetAddress(), shard_server_io_3.GetAddress()};
  std::vector<Address> address_for_2{shard_server_io_1.GetAddress(), shard_server_io_3.GetAddress()};
  std::vector<Address> address_for_3{shard_server_io_1.GetAddress(), shard_server_io_2.GetAddress()};

  ConcreteShardRsm shard_server1(std::move(shard_server_io_1), address_for_1,
                                 std::move(ShardRsm(std::move(shard_ptr1))));
  ConcreteShardRsm shard_server2(std::move(shard_server_io_2), address_for_2,
                                 std::move(ShardRsm(std::move(shard_ptr2))));
  ConcreteShardRsm shard_server3(std::move(shard_server_io_3), address_for_3,
                                 std::move(ShardRsm(std::move(shard_ptr3))));

  auto server_thread1 = std::jthread([&shard_server1]() { shard_server1.Run(); });
  auto server_thread2 = std::jthread([&shard_server2]() { shard_server2.Run(); });
  auto server_thread3 = std::jthread([&shard_server3]() { shard_server3.Run(); });

  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_io_1.GetAddress());
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_io_2.GetAddress());
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_io_3.GetAddress());

  std::cout << "Beginning test after servers have become quiescent." << std::endl;

  ShardClient client(shard_client_io, shard_server_io_1.GetAddress(), {shard_server_io_2.GetAddress()});

  TestCreateVertices(client);

  simulator.ShutDown();

  SimulatorStats stats = simulator.Stats();

  std::cout << "total messages:     " << stats.total_messages << std::endl;
  std::cout << "dropped messages:   " << stats.dropped_messages << std::endl;
  std::cout << "timed out requests: " << stats.timed_out_requests << std::endl;
  std::cout << "total requests:     " << stats.total_requests << std::endl;
  std::cout << "total responses:    " << stats.total_responses << std::endl;
  std::cout << "simulator ticks:    " << stats.simulator_ticks << std::endl;

  std::cout << "========================== SUCCESS :) ==========================" << std::endl;

  return 0;
}
