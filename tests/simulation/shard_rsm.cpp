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

#include <iostream>
#include <optional>
#include <thread>
#include <utility>
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

// TODO(gvolfing) test vertex deletion with DETACH_DELETE

template <typename IoImpl>
void RunShardRaft(Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

namespace {

uint64_t GetTransactionId() {
  static uint64_t transaction_id = 0;
  return transaction_id++;
}

uint64_t GetUniquePropertyValueValue() {
  static uint64_t prop_val_val = 1001;
  return prop_val_val++;
}

LabelId get_primary_label() { return LabelId::FromUint(0); }

SchemaProperty get_schema_property() {
  return {.property_id = PropertyId::FromUint(0), .type = memgraph::common::SchemaType::INT};
}

PrimaryKey GetPrimaryKey() {
  PropertyValue prop_val(50);
  PrimaryKey primary_key = {prop_val};
  return primary_key;
}

NewVertex get_new_vertex(int64_t value) {
  // Specify Labels.
  Label label1 = {.id = LabelId::FromUint(1)};
  std::vector<Label> label_ids = {label1};

  // Specify primary key.
  PrimaryKey primary_key = GetPrimaryKey();

  // Specify properties
  auto val1 = Value(static_cast<int64_t>(value));
  auto prop1 = std::make_pair(PropertyId::FromUint(0), val1);

  auto val2 = Value(static_cast<int64_t>(value));
  auto prop2 = std::make_pair(PropertyId::FromUint(1), val1);

  std::vector<std::pair<PropertyId, Value>> properties{prop1, prop2};

  // NewVertex
  return {.label_ids = label_ids, .primary_key = primary_key, .properties = properties};
}

// TODO(gvolfing) maybe rename that something that makes sense.
std::vector<std::vector<Value>> GetValuePrimaryKeysWithValue(int64_t value) {
  Value val(static_cast<int64_t>(value));
  return {{val}};
}

}  // namespace

// attempts to sending different requests
namespace {

bool AttemtpToCreateVertex(ShardClient &client, int64_t value) {
  NewVertex vertex = get_new_vertex(value);

  auto create_req = CreateVerticesRequest{};
  create_req.new_vertices = {vertex};
  create_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(create_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<CreateVerticesResponse>(write_response_result);

    return write_response.success;
  }
}

bool AttemptToDeleteVertex(ShardClient &client, int64_t value) {
  auto delete_req = DeleteVerticesRequest{};
  delete_req.deletion_type = DeleteVerticesRequest::DeletionType::DELETE;
  delete_req.primary_keys = GetValuePrimaryKeysWithValue(value);
  delete_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(delete_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<DeleteVerticesResponse>(write_response_result);

    return write_response.success;
  }
}

bool AttemptToUpdateVertex(ShardClient &client, int64_t value) {
  auto vertex_id = GetValuePrimaryKeysWithValue(value)[0];

  std::vector<std::pair<PropertyId, Value>> property_updates;
  auto property_update = std::make_pair(PropertyId::FromUint(1), Value(static_cast<int64_t>(10000)));

  auto vertex_prop = UpdateVertexProp{};
  vertex_prop.vertex = vertex_id;
  vertex_prop.property_updates = {property_update};

  auto update_req = UpdateVerticesRequest{};
  update_req.transaction_id.logical_id = GetTransactionId();
  update_req.new_properties = {vertex_prop};

  while (true) {
    auto write_res = client.SendWriteRequest(update_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<UpdateVerticesResponse>(write_response_result);

    return write_response.success;
  }
}

}  // namespace

// tests
namespace {

void TestCreateVertices(ShardClient &client) {
  MG_ASSERT(AttemtpToCreateVertex(client, GetUniquePropertyValueValue()));
}

void TestCreateAndDeleteVertices(ShardClient &client) {
  auto unique_prop_val = GetUniquePropertyValueValue();

  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val));
  MG_ASSERT(AttemptToDeleteVertex(client, unique_prop_val));
}

void TestCreateAndUpdateVertices(ShardClient &client) {
  auto unique_prop_val = GetUniquePropertyValueValue();

  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val));
  MG_ASSERT(AttemptToUpdateVertex(client, unique_prop_val));
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

  PropertyValue min_pk(static_cast<int64_t>(0));
  std::vector<PropertyValue> min_prim_key = {min_pk};

  PropertyValue max_pk(static_cast<int64_t>(100000));
  std::vector<PropertyValue> max_prim_key = {max_pk};

  auto shard_ptr1 = std::make_unique<memgraph::storage::v3::Shard>(get_primary_label(), min_prim_key, max_prim_key);
  auto shard_ptr2 = std::make_unique<memgraph::storage::v3::Shard>(get_primary_label(), min_prim_key, max_prim_key);
  auto shard_ptr3 = std::make_unique<memgraph::storage::v3::Shard>(get_primary_label(), min_prim_key, max_prim_key);

  shard_ptr1->CreateSchema(get_primary_label(), {get_schema_property()});
  shard_ptr2->CreateSchema(get_primary_label(), {get_schema_property()});
  shard_ptr3->CreateSchema(get_primary_label(), {get_schema_property()});

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

  std::vector server_addrs = {shard_server_io_1.GetAddress(), shard_server_io_2.GetAddress(),
                              shard_server_io_3.GetAddress()};
  ShardClient client(shard_client_io, shard_server_io_1.GetAddress(), server_addrs);

  TestCreateVertices(client);
  TestCreateAndDeleteVertices(client);
  TestCreateAndUpdateVertices(client);

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
