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

#include "io/address.hpp"
#include "io/rsm/coordinator_rsm.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "utils/rsm_client.hpp"

using memgraph::coordinator::Address;
using memgraph::coordinator::AddressAndStatus;
using memgraph::coordinator::CompoundKey;
using memgraph::coordinator::Coordinator;
using memgraph::coordinator::Shard;
using memgraph::coordinator::ShardMap;
using memgraph::coordinator::Shards;
using memgraph::coordinator::Status;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::Time;
using memgraph::io::rsm::CoordinatorRsm;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::StorageGetRequest;
using memgraph::io::rsm::StorageGetResponse;
using memgraph::io::rsm::StorageRsm;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

using StorageClient =
    RsmClient<Io<SimulatorTransport>, StorageWriteRequest, StorageWriteResponse, StorageGetRequest, StorageGetResponse>;
namespace {
ShardMap CreateDummyShardmap(memgraph::coordinator::Address a_io_1, memgraph::coordinator::Address a_io_2,
                             memgraph::coordinator::Address a_io_3, memgraph::coordinator::Address b_io_1,
                             memgraph::coordinator::Address b_io_2, memgraph::coordinator::Address b_io_3) {
  ShardMap sm1;
  auto &shards = sm1.GetShards();

  // 1
  std::string label1 = std::string("label1");
  auto key1 = memgraph::storage::v3::PropertyValue(3);
  auto key2 = memgraph::storage::v3::PropertyValue(4);
  CompoundKey cm1 = {key1, key2};
  AddressAndStatus aas1_1{.address = a_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_2{.address = a_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas1_3{.address = a_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard1 = {aas1_1, aas1_2, aas1_3};
  Shards shards1;
  shards1[cm1] = shard1;

  // 2
  std::string label2 = std::string("label2");
  auto key3 = memgraph::storage::v3::PropertyValue(12);
  auto key4 = memgraph::storage::v3::PropertyValue(13);
  CompoundKey cm2 = {key3, key4};
  AddressAndStatus aas2_1{.address = b_io_1, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_2{.address = b_io_2, .status = Status::CONSENSUS_PARTICIPANT};
  AddressAndStatus aas2_3{.address = b_io_3, .status = Status::CONSENSUS_PARTICIPANT};

  Shard shard2 = {aas2_1, aas2_2, aas2_3};
  Shards shards2;
  shards2[cm2] = shard2;

  shards[label2] = shards2;

  return sm1;
}

std::optional<StorageClient> DetermineShardLocation(Shard target_shard, const std::vector<Address> &a_addrs,
                                                    StorageClient a_client, const std::vector<Address> &b_addrs,
                                                    StorageClient b_client) {
  for (const auto &addr : target_shard) {
    if (addr.address == b_addrs[0]) {
      return b_client;
    }
    if (addr.address == a_addrs[0]) {
      return a_client;
    }
  }
  return {};
}

}  // namespace

using ConcreteCoordinatorRsm = CoordinatorRsm<SimulatorTransport>;
using ConcreteStorageRsm = Raft<SimulatorTransport, StorageRsm, StorageWriteRequest, StorageWriteResponse,
                                StorageGetRequest, StorageGetResponse>;

template <typename IoImpl>
void RunStorageRaft(
    Raft<IoImpl, StorageRsm, StorageWriteRequest, StorageWriteResponse, StorageGetRequest, StorageGetResponse> server) {
  server.Run();
}

int main() {
  SimulatorConfig config{
      .drop_percent = 5,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{8 * 1024 * 1024},
  };

  auto simulator = Simulator(config);

  Io<SimulatorTransport> cli_io = simulator.RegisterNew();

  // auto c_thread_1 = std::jthread(RunRaft< Coordinator>, std::move(c_1));
  // simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[0]);

  // auto c_thread_2 = std::jthread(RunRaft< Coordinator>, std::move(c_2));
  // simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[1]);

  // auto c_thread_3 = std::jthread(RunRaft<Coordinator>, std::move(c_3));
  // simulator.IncrementServerCountAndWaitForQuiescentState(c_addrs[2]);

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

  ConcreteStorageRsm a_1{std::move(a_io_1), a_1_peers, StorageRsm{}};
  ConcreteStorageRsm a_2{std::move(a_io_2), a_2_peers, StorageRsm{}};
  ConcreteStorageRsm a_3{std::move(a_io_3), a_3_peers, StorageRsm{}};

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

  ConcreteStorageRsm b_1{std::move(b_io_1), b_1_peers, StorageRsm{}};
  ConcreteStorageRsm b_2{std::move(b_io_2), b_2_peers, StorageRsm{}};
  ConcreteStorageRsm b_3{std::move(b_io_3), b_3_peers, StorageRsm{}};

  auto b_thread_1 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_1));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[0]);

  auto b_thread_2 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_2));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[1]);

  auto b_thread_3 = std::jthread(RunStorageRaft<SimulatorTransport>, std::move(b_3));
  simulator.IncrementServerCountAndWaitForQuiescentState(b_addrs[2]);

  std::cout << "beginning test after servers have become quiescent" << std::endl;

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

  // Have client contact coordinator RSM for a new transaction ID and
  // also get the current shard map
  using CoordinatorClient =
      RsmClient<Io<SimulatorTransport>, memgraph::coordinator::WriteRequests, memgraph::coordinator::WriteResponses,
                memgraph::coordinator::ReadRequests, memgraph::coordinator::ReadResponses>;
  CoordinatorClient coordinator_client(cli_io, c_addrs[0], c_addrs);

  StorageClient shard_a_client(cli_io, a_addrs[0], a_addrs);
  StorageClient shard_b_client(cli_io, b_addrs[0], b_addrs);

  memgraph::coordinator::HlcRequest req;

  // Last ShardMap Version The query engine knows about.
  ShardMap client_shard_map;
  req.last_shard_map_version = client_shard_map.GetHlc();

  while (true) {
    // Create CompoundKey
    auto cm_key_1 = memgraph::storage::v3::PropertyValue(3);
    auto cm_key_2 = memgraph::storage::v3::PropertyValue(4);

    CompoundKey cm_k = {cm_key_1, cm_key_2};

    // Look for Shard
    auto read_res_opt = coordinator_client.SendReadRequest(req);
    if (!read_res_opt) {
      std::cout << "ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!0" << std::endl;
      continue;
    }

    std::cout << "Before" << std::endl;
    auto read_res = read_res_opt.value();
    std::cout << "After" << std::endl;

    auto res = std::get<memgraph::coordinator::HlcResponse>(read_res.read_return);
    auto transaction_id = res.new_hlc;

    std::cout << "transaction_id: " << transaction_id.logical_id << std::endl;

    if (!res.fresher_shard_map) {
      // continue;
      std::cout << "Something is really not OK..." << std::endl;
    }

    std::cout << "Before2" << std::endl;
    client_shard_map = res.fresher_shard_map.value();
    std::cout << "After2" << std::endl;

    auto target_shard = client_shard_map.GetShardForKey("label1", cm_k);

    // Determine which shard to send the requests to
    auto storage_client_opt = DetermineShardLocation(target_shard, a_addrs, shard_a_client, b_addrs, shard_b_client);
    MG_ASSERT(storage_client_opt);

    std::cout << "Before3" << std::endl;
    auto storage_client = storage_client_opt.value();
    std::cout << "After3" << std::endl;

    // Have client use shard map to decide which shard to communicate
    // with in order to write a new value
    // client_shard_map.
    StorageWriteRequest storage_req;
    auto write_key_1 = memgraph::storage::PropertyValue(3);
    auto write_key_2 = memgraph::storage::PropertyValue(4);
    storage_req.key = {write_key_1, write_key_2};
    storage_req.value = 1000;
    auto write_res_opt = storage_client.SendWriteRequest(storage_req);
    if (!write_res_opt) {
      std::cout << "ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1" << std::endl;
      continue;
    }
    auto write_res = write_res_opt.value().write_return;

    bool cas_succeeded = write_res.shard_rsm_success;

    if (!cas_succeeded) {
      continue;
    }

    // Have client use shard map to decide which shard to communicate
    // with to read that same value back

    StorageGetRequest storage_get_req;
    storage_get_req.key = {write_key_1, write_key_2};
    auto get_res_opt = storage_client.SendReadRequest(storage_get_req);
    if (!get_res_opt) {
      std::cout << "ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!2" << std::endl;
      continue;
    }
    auto get_res = get_res_opt.value();
    auto val = get_res.read_return.value.value();

    std::cout << "val -> " << val << std::endl;

    MG_ASSERT(get_res.read_return.value == 1000);
    break;
  }

  simulator.ShutDown();

  return 0;
}
