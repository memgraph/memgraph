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
#include "storage/v3/view.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::v3::tests {

using io::Address;
using io::Io;
using io::ResponseEnvelope;
using io::ResponseFuture;
using io::Time;
using io::TimedOut;
using io::rsm::Raft;
using io::rsm::ReadRequest;
using io::rsm::ReadResponse;
using io::rsm::RsmClient;
using io::rsm::WriteRequest;
using io::rsm::WriteResponse;
using io::simulator::Simulator;
using io::simulator::SimulatorConfig;
using io::simulator::SimulatorStats;
using io::simulator::SimulatorTransport;
using utils::BasicResult;

using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;

using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

using ConcreteShardRsm = Raft<SimulatorTransport, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

// TODO(gvolfing) test vertex deletion with DETACH_DELETE as well
template <typename IoImpl>
void RunShardRaft(Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

namespace {

uint64_t GetTransactionId() {
  static uint64_t transaction_id = 0;
  return transaction_id++;
}

uint64_t GetUniqueInteger() {
  static uint64_t prop_val_val = 1001;
  return prop_val_val++;
}

LabelId get_primary_label() { return LabelId::FromUint(0); }

SchemaProperty get_schema_property() {
  return {.property_id = PropertyId::FromUint(0), .type = common::SchemaType::INT};
}

msgs::PrimaryKey GetPrimaryKey(int64_t value) {
  msgs::Value prop_val(static_cast<int64_t>(value));
  msgs::PrimaryKey primary_key = {prop_val};
  return primary_key;
}

msgs::NewVertex get_new_vertex(int64_t value) {
  // Specify Labels.
  msgs::Label label1 = {.id = LabelId::FromUint(1)};
  std::vector<msgs::Label> label_ids = {label1};

  // Specify primary key.
  msgs::PrimaryKey primary_key = GetPrimaryKey(value);

  // Specify properties
  auto val1 = msgs::Value(static_cast<int64_t>(value));
  auto prop1 = std::make_pair(PropertyId::FromUint(0), val1);

  auto val2 = msgs::Value(static_cast<int64_t>(value));
  auto prop2 = std::make_pair(PropertyId::FromUint(1), val1);

  std::vector<std::pair<PropertyId, msgs::Value>> properties{prop1, prop2};

  // NewVertex
  return {.label_ids = label_ids, .primary_key = primary_key, .properties = properties};
}

// TODO(gvolfing) maybe rename that something that makes sense.
std::vector<std::vector<msgs::Value>> GetValuePrimaryKeysWithValue(int64_t value) {
  msgs::Value val(static_cast<int64_t>(value));
  return {{val}};
}

}  // namespace

// attempts to sending different requests
namespace {

bool AttemtpToCreateVertex(ShardClient &client, int64_t value) {
  msgs::NewVertex vertex = get_new_vertex(value);

  auto create_req = msgs::CreateVerticesRequest{};
  create_req.new_vertices = {vertex};
  create_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(create_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::CreateVerticesResponse>(write_response_result);

    return write_response.success;
  }
}

bool AttemptToAddEdge(ShardClient &client, int64_t value_of_vertex_1, int64_t value_of_vertex_2, int64_t edge_gid,
                      int64_t edge_type_id) {
  auto id = msgs::EdgeId{};
  msgs::Label label = {.id = get_primary_label()};

  auto src = std::make_pair(label, GetPrimaryKey(value_of_vertex_1));
  auto dst = std::make_pair(label, GetPrimaryKey(value_of_vertex_2));
  id.gid = edge_gid;

  auto type = msgs::EdgeType{};
  type.id = edge_type_id;

  auto edge = msgs::Edge{};
  edge.id = id;
  edge.type = type;
  edge.src = src;
  edge.dst = dst;

  msgs::CreateEdgesRequest create_req{};
  create_req.edges = {edge};
  create_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(create_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::CreateEdgesResponse>(write_response_result);

    return write_response.success;
  }
}

std::tuple<size_t, std::optional<msgs::VertexId>> AttemptToScanAllWithBatchLimit(ShardClient &client,
                                                                                 msgs::VertexId start_id,
                                                                                 uint64_t batch_limit) {
  msgs::ScanVerticesRequest scan_req{};
  scan_req.batch_limit = batch_limit;
  scan_req.filter_expressions = std::nullopt;
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::OLD;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(write_response.success);
    return {write_response.results.size(), write_response.next_start_id};
  }
}

}  // namespace

// tests
namespace {

void TestCreateVertices(ShardClient &client) { MG_ASSERT(AttemtpToCreateVertex(client, GetUniqueInteger())); }

void TestAddEdge(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();

  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_2));

  auto edge_gid = GetUniqueInteger();
  auto edge_type_id = GetUniqueInteger();

  MG_ASSERT(AttemptToAddEdge(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_type_id));
}

void TestScanAllOneGo(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();
  auto unique_prop_val_3 = GetUniqueInteger();
  auto unique_prop_val_4 = GetUniqueInteger();
  auto unique_prop_val_5 = GetUniqueInteger();

  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_2));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_3));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_4));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_5));

  msgs::Label prim_label = {.id = get_primary_label()};
  msgs::PrimaryKey prim_key = {msgs::Value(static_cast<int64_t>(unique_prop_val_1))};

  msgs::VertexId v_id = {prim_label, prim_key};

  auto [result_size, next_id] = AttemptToScanAllWithBatchLimit(client, v_id, 5);
  MG_ASSERT(result_size == 5);
}

void TestScanAllWithSmallBatchSize(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();
  auto unique_prop_val_3 = GetUniqueInteger();
  auto unique_prop_val_4 = GetUniqueInteger();
  auto unique_prop_val_5 = GetUniqueInteger();
  auto unique_prop_val_6 = GetUniqueInteger();
  auto unique_prop_val_7 = GetUniqueInteger();
  auto unique_prop_val_8 = GetUniqueInteger();
  auto unique_prop_val_9 = GetUniqueInteger();
  auto unique_prop_val_10 = GetUniqueInteger();

  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_2));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_3));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_4));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_5));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_6));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_7));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_8));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_9));
  MG_ASSERT(AttemtpToCreateVertex(client, unique_prop_val_10));

  msgs::Label prim_label = {.id = get_primary_label()};
  msgs::PrimaryKey prim_key1 = {msgs::Value(static_cast<int64_t>(unique_prop_val_1))};

  msgs::VertexId v_id_1 = {prim_label, prim_key1};

  auto [result_size1, next_id1] = AttemptToScanAllWithBatchLimit(client, v_id_1, 3);
  MG_ASSERT(result_size1 == 3);

  auto [result_size2, next_id2] = AttemptToScanAllWithBatchLimit(client, next_id1.value(), 3);
  MG_ASSERT(result_size2 == 3);

  auto [result_size3, next_id3] = AttemptToScanAllWithBatchLimit(client, next_id2.value(), 3);
  MG_ASSERT(result_size3 == 3);

  auto [result_size4, next_id4] = AttemptToScanAllWithBatchLimit(client, next_id3.value(), 3);
  MG_ASSERT(result_size4 == 1);
  MG_ASSERT(!next_id4);
}

}  // namespace

int TestMessages() {
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
  const auto shard_server_1_address = shard_server_io_1.GetAddress();
  Io<SimulatorTransport> shard_server_io_2 = simulator.RegisterNew();
  const auto shard_server_2_address = shard_server_io_2.GetAddress();
  Io<SimulatorTransport> shard_server_io_3 = simulator.RegisterNew();
  const auto shard_server_3_address = shard_server_io_3.GetAddress();
  Io<SimulatorTransport> shard_client_io = simulator.RegisterNew();

  PropertyValue min_pk(static_cast<int64_t>(0));
  std::vector<PropertyValue> min_prim_key = {min_pk};

  PropertyValue max_pk(static_cast<int64_t>(10000000));
  std::vector<PropertyValue> max_prim_key = {max_pk};

  auto shard_ptr1 = std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key);
  auto shard_ptr2 = std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key);
  auto shard_ptr3 = std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key);

  shard_ptr1->CreateSchema(get_primary_label(), {get_schema_property()});
  shard_ptr2->CreateSchema(get_primary_label(), {get_schema_property()});
  shard_ptr3->CreateSchema(get_primary_label(), {get_schema_property()});

  std::vector<Address> address_for_1{shard_server_2_address, shard_server_3_address};
  std::vector<Address> address_for_2{shard_server_1_address, shard_server_3_address};
  std::vector<Address> address_for_3{shard_server_1_address, shard_server_2_address};

  ConcreteShardRsm shard_server1(std::move(shard_server_io_1), address_for_1, ShardRsm(std::move(shard_ptr1)));
  ConcreteShardRsm shard_server2(std::move(shard_server_io_2), address_for_2, ShardRsm(std::move(shard_ptr2)));
  ConcreteShardRsm shard_server3(std::move(shard_server_io_3), address_for_3, ShardRsm(std::move(shard_ptr3)));

  auto server_thread1 = std::jthread([&shard_server1]() { shard_server1.Run(); });
  auto server_thread2 = std::jthread([&shard_server2]() { shard_server2.Run(); });
  auto server_thread3 = std::jthread([&shard_server3]() { shard_server3.Run(); });

  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_1_address);
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_2_address);
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_3_address);

  std::cout << "Beginning test after servers have become quiescent." << std::endl;

  std::vector server_addrs = {shard_server_1_address, shard_server_2_address, shard_server_3_address};
  ShardClient client(shard_client_io, shard_server_1_address, server_addrs);

  TestCreateVertices(client);
  TestAddEdge(client);
  TestScanAllOneGo(client);
  TestScanAllWithSmallBatchSize(client);

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

}  // namespace memgraph::storage::v3::tests

int main() { return memgraph::storage::v3::tests::TestMessages(); }
