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
#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "common.hpp"
#include "common/types.hpp"
#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/accessors.hpp"
#include "query/v2/conversions.hpp"
#include "query/v2/requests.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/result.hpp"

namespace memgraph::query::v2::tests {
using coordinator::AddressAndStatus;
using CompoundKey = coordinator::PrimaryKey;
using coordinator::Coordinator;
using coordinator::CoordinatorClient;
using coordinator::CoordinatorRsm;
using coordinator::HlcRequest;
using coordinator::HlcResponse;
using coordinator::Shard;
using coordinator::ShardMap;
using coordinator::Shards;
using coordinator::Status;
using io::Address;
using io::Io;
using io::ResponseEnvelope;
using io::ResponseFuture;
using io::Time;
using io::TimedOut;
using io::rsm::Raft;
using io::rsm::ReadRequest;
using io::rsm::ReadResponse;
using io::rsm::StorageReadRequest;
using io::rsm::StorageReadResponse;
using io::rsm::StorageWriteRequest;
using io::rsm::StorageWriteResponse;
using io::rsm::WriteRequest;
using io::rsm::WriteResponse;
using io::simulator::Simulator;
using io::simulator::SimulatorConfig;
using io::simulator::SimulatorStats;
using io::simulator::SimulatorTransport;
using msgs::CreateVerticesRequest;
using msgs::CreateVerticesResponse;
using msgs::ListedValues;
using msgs::NewVertexLabel;
using msgs::ScanVerticesRequest;
using msgs::ScanVerticesResponse;
using msgs::VertexId;
using storage::v3::LabelId;
using storage::v3::SchemaProperty;
using storage::v3::tests::MockedShardRsm;
using utils::BasicResult;

namespace {

ShardMap CreateDummyShardmap(coordinator::Address a_io_1, coordinator::Address a_io_2, coordinator::Address a_io_3,
                             coordinator::Address b_io_1, coordinator::Address b_io_2, coordinator::Address b_io_3) {
  static const std::string label_name = std::string("test_label");
  ShardMap sm;

  // register new properties
  const std::vector<std::string> property_names = {"property_1", "property_2"};
  const auto properties = sm.AllocatePropertyIds(property_names);
  const auto property_id_1 = properties.at("property_1");
  const auto property_id_2 = properties.at("property_2");
  const auto type_1 = common::SchemaType::INT;
  const auto type_2 = common::SchemaType::INT;

  // register new label space
  std::vector<SchemaProperty> schema = {
      SchemaProperty{.property_id = property_id_1, .type = type_1},
      SchemaProperty{.property_id = property_id_2, .type = type_2},
  };

  auto label_success = sm.InitializeNewLabel(label_name, schema, 1, sm.shard_map_version);
  MG_ASSERT(label_success);

  const LabelId label_id = sm.labels.at(label_name);
  auto &label_space = sm.label_spaces.at(label_id);
  Shards &shards_for_label = label_space.shards;
  shards_for_label.clear();

  // add first shard at [0, 0]
  AddressAndStatus aas1_1{.address = a_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_2{.address = a_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_3{.address = a_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard1 = {aas1_1, aas1_2, aas1_3};

  auto key1 = storage::v3::PropertyValue(0);
  auto key2 = storage::v3::PropertyValue(0);
  CompoundKey compound_key_1 = {key1, key2};
  shards_for_label[compound_key_1] = shard1;

  // add second shard at [12, 13]
  AddressAndStatus aas2_1{.address = b_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_2{.address = b_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_3{.address = b_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard2 = {aas2_1, aas2_2, aas2_3};

  auto key3 = storage::v3::PropertyValue(12);
  auto key4 = storage::v3::PropertyValue(13);
  CompoundKey compound_key_2 = {key3, key4};
  shards_for_label[compound_key_2] = shard2;

  sm.AllocateEdgeTypeIds(std::vector<coordinator::EdgeTypeName>{"edge_type"});

  return sm;
}

}  // namespace

using WriteRequests = msgs::WriteRequests;
using WriteResponses = msgs::WriteResponses;
using ReadRequests = msgs::ReadRequests;
using ReadResponses = msgs::ReadResponses;

using ConcreteCoordinatorRsm = CoordinatorRsm<SimulatorTransport>;
using ConcreteStorageRsm =
    Raft<SimulatorTransport, MockedShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

template <typename IoImpl>
void RunStorageRaft(Raft<IoImpl, MockedShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

void TestScanVertices(msgs::ShardRequestManagerInterface &io) {
  msgs::ExecutionState<ScanVerticesRequest> state{.label = "test_label"};

  auto result = io.Request(state);
  MG_ASSERT(result.size() == 2);
  {
    auto prop = result[0].GetProperty(msgs::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 0);
    prop = result[1].GetProperty(msgs::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 444);
  }

  result = io.Request(state);
  {
    MG_ASSERT(result.size() == 1);
    auto prop = result[0].GetProperty(msgs::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 1);
  }

  // Exhaust it, request should be empty
  result = io.Request(state);
  MG_ASSERT(result.size() == 0);
}

void TestCreateVertices(msgs::ShardRequestManagerInterface &io) {
  using PropVal = msgs::Value;
  msgs::ExecutionState<CreateVerticesRequest> state;
  std::vector<msgs::NewVertex> new_vertices;
  auto label_id = io.NameToLabel("test_label");
  msgs::NewVertex a1{.primary_key = {PropVal(int64_t(1)), PropVal(int64_t(0))}};
  a1.label_ids.push_back({label_id});
  msgs::NewVertex a2{.primary_key = {PropVal(int64_t(13)), PropVal(int64_t(13))}};
  a2.label_ids.push_back({label_id});
  new_vertices.push_back(std::move(a1));
  new_vertices.push_back(std::move(a2));

  auto result = io.Request(state, std::move(new_vertices));
  MG_ASSERT(result.size() == 2);
}

void TestCreateExpand(msgs::ShardRequestManagerInterface &io) {
  using PropVal = msgs::Value;
  msgs::ExecutionState<msgs::CreateExpandRequest> state;
  std::vector<msgs::NewExpand> new_expands;

  const auto edge_type_id = io.NameToEdgeType("edge_type");
  const auto label = msgs::Label{io.NameToLabel("test_label")};
  const msgs::VertexId vertex_id_1{label, {PropVal(int64_t(0)), PropVal(int64_t(0))}};
  const msgs::VertexId vertex_id_2{label, {PropVal(int64_t(13)), PropVal(int64_t(13))}};
  msgs::NewExpand expand_1{
      .id = {.gid = 0}, .type = {edge_type_id}, .src_vertex = vertex_id_1, .dest_vertex = vertex_id_2};
  msgs::NewExpand expand_2{
      .id = {.gid = 1}, .type = {edge_type_id}, .src_vertex = vertex_id_2, .dest_vertex = vertex_id_1};
  new_expands.push_back(std::move(expand_1));
  new_expands.push_back(std::move(expand_2));

  auto responses = io.Request(state, std::move(new_expands));
  MG_ASSERT(responses.size() == 2);
  MG_ASSERT(responses[0].success);
  MG_ASSERT(responses[1].success);
}

void TestExpandOne(msgs::ShardRequestManagerInterface &shard_request_manager) {
  msgs::ExecutionState<msgs::ExpandOneRequest> state{};
  msgs::ExpandOneRequest request;
  const auto edge_type_id = shard_request_manager.NameToEdgeType("edge_type");
  const auto label = msgs::Label{shard_request_manager.NameToLabel("test_label")};
  request.src_vertices.push_back(msgs::VertexId{label, {msgs::Value(int64_t(0)), msgs::Value(int64_t(0))}});
  request.edge_types.push_back(msgs::EdgeType{edge_type_id});
  request.direction = msgs::EdgeDirection::BOTH;
  auto responses = shard_request_manager.Request(state, std::move(request));
  MG_ASSERT(responses.size() == 2);
}

template <typename ShardRequestManager>
void TestAggregate(ShardRequestManager &io) {}

void DoTest() {
  SimulatorConfig config{
      .drop_percent = 0,
      .perform_timeouts = false,
      .scramble_messages = false,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{2 * 8 * 1024 * 1024},
  };

  auto simulator = Simulator(config);
  const auto one_second = std::chrono::seconds(1);

  Io<SimulatorTransport> cli_io = simulator.RegisterNew();
  cli_io.SetDefaultTimeout(one_second);

  // Register
  Io<SimulatorTransport> a_io_1 = simulator.RegisterNew();
  a_io_1.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> a_io_2 = simulator.RegisterNew();
  a_io_2.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> a_io_3 = simulator.RegisterNew();
  a_io_3.SetDefaultTimeout(one_second);

  Io<SimulatorTransport> b_io_1 = simulator.RegisterNew();
  b_io_1.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> b_io_2 = simulator.RegisterNew();
  b_io_2.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> b_io_3 = simulator.RegisterNew();
  b_io_3.SetDefaultTimeout(one_second);

  // Preconfigure coordinator with kv shard 'A' and 'B'
  auto sm1 = CreateDummyShardmap(a_io_1.GetAddress(), a_io_2.GetAddress(), a_io_3.GetAddress(), b_io_1.GetAddress(),
                                 b_io_2.GetAddress(), b_io_3.GetAddress());
  auto sm2 = CreateDummyShardmap(a_io_1.GetAddress(), a_io_2.GetAddress(), a_io_3.GetAddress(), b_io_1.GetAddress(),
                                 b_io_2.GetAddress(), b_io_3.GetAddress());
  auto sm3 = CreateDummyShardmap(a_io_1.GetAddress(), a_io_2.GetAddress(), a_io_3.GetAddress(), b_io_1.GetAddress(),
                                 b_io_2.GetAddress(), b_io_3.GetAddress());

  // Spin up shard A
  std::vector<Address> a_addrs = {a_io_1.GetAddress(), a_io_2.GetAddress(), a_io_3.GetAddress()};

  std::vector<Address> a_1_peers = {a_addrs[1], a_addrs[2]};
  std::vector<Address> a_2_peers = {a_addrs[0], a_addrs[2]};
  std::vector<Address> a_3_peers = {a_addrs[0], a_addrs[1]};

  ConcreteStorageRsm a_1{std::move(a_io_1), a_1_peers, MockedShardRsm{}};
  ConcreteStorageRsm a_2{std::move(a_io_2), a_2_peers, MockedShardRsm{}};
  ConcreteStorageRsm a_3{std::move(a_io_3), a_3_peers, MockedShardRsm{}};

  auto a_thread_1 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(a_1));
  simulator.IncrementServerCountAndWaitForQuiescentState(a_addrs[0]);

  auto a_thread_2 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(a_2));
  simulator.IncrementServerCountAndWaitForQuiescentState(a_addrs[1]);

  auto a_thread_3 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(a_3));
  simulator.IncrementServerCountAndWaitForQuiescentState(a_addrs[2]);

  // Spin up shard B
  std::vector<Address> b_addrs = {b_io_1.GetAddress(), b_io_2.GetAddress(), b_io_3.GetAddress()};

  std::vector<Address> b_1_peers = {b_addrs[1], b_addrs[2]};
  std::vector<Address> b_2_peers = {b_addrs[0], b_addrs[2]};
  std::vector<Address> b_3_peers = {b_addrs[0], b_addrs[1]};

  ConcreteStorageRsm b_1{std::move(b_io_1), b_1_peers, MockedShardRsm{}};
  ConcreteStorageRsm b_2{std::move(b_io_2), b_2_peers, MockedShardRsm{}};
  ConcreteStorageRsm b_3{std::move(b_io_3), b_3_peers, MockedShardRsm{}};

  auto b_thread_1 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_1));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[0]);

  auto b_thread_2 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_2));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[1]);

  auto b_thread_3 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_3));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[2]);

  // Spin up coordinators

  Io<SimulatorTransport> c_io_1 = simulator.RegisterNew();
  c_io_1.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> c_io_2 = simulator.RegisterNew();
  c_io_2.SetDefaultTimeout(one_second);
  Io<SimulatorTransport> c_io_3 = simulator.RegisterNew();
  c_io_3.SetDefaultTimeout(one_second);

  std::vector<Address> c_addrs = {c_io_1.GetAddress(), c_io_2.GetAddress(), c_io_3.GetAddress()};

  std::vector<Address> c_1_peers = {c_addrs[1], c_addrs[2]};
  std::vector<Address> c_2_peers = {c_addrs[0], c_addrs[2]};
  std::vector<Address> c_3_peers = {c_addrs[0], c_addrs[1]};

  ConcreteCoordinatorRsm c_1{std::move(c_io_1), c_1_peers, Coordinator{(sm1)}};
  ConcreteCoordinatorRsm c_2{std::move(c_io_2), c_2_peers, Coordinator{(sm2)}};
  ConcreteCoordinatorRsm c_3{std::move(c_io_3), c_3_peers, Coordinator{(sm3)}};

  auto c_thread_1 = std::jthread([c_1]() mutable { c_1.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[0]);

  auto c_thread_2 = std::jthread([c_2]() mutable { c_2.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[1]);

  auto c_thread_3 = std::jthread([c_3]() mutable { c_3.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[2]);

  std::cout << "beginning test after servers have become quiescent" << std::endl;

  // Have client contact coordinator RSM for a new transaction ID and
  // also get the current shard map
  CoordinatorClient<SimulatorTransport> coordinator_client(cli_io, c_addrs[0], c_addrs);

  msgs::ShardRequestManager<SimulatorTransport> io(std::move(coordinator_client), std::move(cli_io));

  io.StartTransaction();
  TestScanVertices(io);
  TestCreateVertices(io);
  TestCreateExpand(io);

  simulator.ShutDown();

  SimulatorStats stats = simulator.Stats();

  std::cout << "total messages:     " << stats.total_messages << std::endl;
  std::cout << "dropped messages:   " << stats.dropped_messages << std::endl;
  std::cout << "timed out requests: " << stats.timed_out_requests << std::endl;
  std::cout << "total requests:     " << stats.total_requests << std::endl;
  std::cout << "total responses:    " << stats.total_responses << std::endl;
  std::cout << "simulator ticks:    " << stats.simulator_ticks << std::endl;

  std::cout << "========================== SUCCESS :) ==========================" << std::endl;
}
}  // namespace memgraph::query::v2::tests

int main() { memgraph::query::v2::tests::DoTest(); }
