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

using memgraph::coordinator::AddressAndStatus;
using memgraph::coordinator::CompoundKey;
using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorClient;
using memgraph::coordinator::CoordinatorRsm;
using memgraph::coordinator::HlcRequest;
using memgraph::coordinator::HlcResponse;
using memgraph::coordinator::Shard;
using memgraph::coordinator::ShardMap;
using memgraph::coordinator::Shards;
using memgraph::coordinator::Status;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::Time;
using memgraph::io::TimedOut;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::StorageReadRequest;
using memgraph::io::rsm::StorageReadResponse;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;
using memgraph::utils::BasicResult;
using requests::CreateVerticesRequest;
using requests::CreateVerticesResponse;
using requests::ListedValues;
using requests::NewVertexLabel;
using requests::ScanVerticesRequest;
using requests::ScanVerticesResponse;

namespace {

ShardMap CreateDummyShardmap(memgraph::coordinator::Address a_io_1, memgraph::coordinator::Address a_io_2,
                             memgraph::coordinator::Address a_io_3, memgraph::coordinator::Address b_io_1,
                             memgraph::coordinator::Address b_io_2, memgraph::coordinator::Address b_io_3) {
  static const std::string label_name = std::string("test_label");
  ShardMap sm;

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

  bool label_success = sm.InitializeNewLabel(label_name, schema, sm.shard_map_version);
  MG_ASSERT(label_success);

  const LabelId label_id = sm.labels.at(label_name);
  auto &label_space = sm.label_spaces.at(label_id);
  Shards &shards_for_label = label_space.shards;

  // add first shard at [0, 0]
  AddressAndStatus aas1_1{.address = a_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_2{.address = a_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_3{.address = a_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard1 = {aas1_1, aas1_2, aas1_3};

  auto key1 = memgraph::storage::v3::PropertyValue(0);
  auto key2 = memgraph::storage::v3::PropertyValue(0);
  CompoundKey compound_key_1 = {key1, key2};
  shards_for_label[compound_key_1] = shard1;

  // add second shard at [12, 13]
  AddressAndStatus aas2_1{.address = b_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_2{.address = b_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_3{.address = b_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard2 = {aas2_1, aas2_2, aas2_3};

  auto key3 = memgraph::storage::v3::PropertyValue(12);
  auto key4 = memgraph::storage::v3::PropertyValue(13);
  CompoundKey compound_key_2 = {key3, key4};
  shards_for_label[compound_key_2] = shard2;

  return sm;
}

}  // namespace

using WriteRequests = CreateVerticesRequest;
using WriteResponses = CreateVerticesResponse;
using ReadRequests = std::variant<ScanVerticesRequest, ExpandOneRequest>;
using ReadResponses = std::variant<ScanVerticesResponse, ExpandOneResponse>;

using ConcreteCoordinatorRsm = CoordinatorRsm<SimulatorTransport>;
using ConcreteStorageRsm =
    Raft<SimulatorTransport, MockedShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

template <typename IoImpl>
void RunStorageRaft(Raft<IoImpl, MockedShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

template <typename ShardRequestManager>
void TestScanAll(ShardRequestManager &io) {
  requests::ExecutionState<ScanVerticesRequest> state{.label = "test_label"};

  auto result = io.Request(state);
  MG_ASSERT(result.size() == 2);
  {
    auto prop = result[0].GetProperty(requests::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 0);
    prop = result[1].GetProperty(requests::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 444);
  }

  result = io.Request(state);
  {
    MG_ASSERT(result.size() == 1);
    auto prop = result[0].GetProperty(requests::PropertyId::FromUint(0));
    MG_ASSERT(prop.int_v == 1);
  }

  // Exhaust it, request should be empty
  result = io.Request(state);
  MG_ASSERT(result.size() == 0);
}

template <typename ShardRequestManager>
void TestCreateVertices(ShardRequestManager &io) {
  using PropVal = memgraph::storage::v3::PropertyValue;
  requests::ExecutionState<CreateVerticesRequest> state;
  std::vector<requests::NewVertex> new_vertices;
  auto label_id = io.LabelNameToLabelId("test_label");
  requests::NewVertex a1{.label_ids = label_id, .primary_key = {PropVal(1), PropVal(0)}};
  requests::NewVertex a2{.label_ids = label_id, .primary_key = {PropVal(13), PropVal(13)}};
  new_vertices.push_back(std::move(a1));
  new_vertices.push_back(std::move(a2));

  auto result = io.Request(state, std::move(new_vertices));
  MG_ASSERT(result.size() == 2);
}

template <typename ShardRequestManager>
void TestExpand(ShardRequestManager &io) {}

template <typename ShardRequestManager>
void TestAggregate(ShardRequestManager &io) {}

int main() {
  SimulatorConfig config{
      .drop_percent = 0,
      .perform_timeouts = false,
      .scramble_messages = false,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{2 * 8 * 1024 * 1024},
  };

  auto simulator = Simulator(config);

  Io<SimulatorTransport> cli_io = simulator.RegisterNew();

  // Register
  Io<SimulatorTransport> a_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> a_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> a_io_3 = simulator.RegisterNew();

  Io<SimulatorTransport> b_io_1 = simulator.RegisterNew();
  Io<SimulatorTransport> b_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> b_io_3 = simulator.RegisterNew();

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
  Io<SimulatorTransport> c_io_2 = simulator.RegisterNew();
  Io<SimulatorTransport> c_io_3 = simulator.RegisterNew();

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

  requests::ShardRequestManager<SimulatorTransport> io(std::move(coordinator_client), std::move(cli_io));

  io.StartTransaction();
  TestScanAll(io);
  TestCreateVertices(io);

  simulator.ShutDown();
  return 0;
}
