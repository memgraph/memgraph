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
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/property_value.hpp"
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
using memgraph::storage::v3::ShardRsm;

// int main(){}

using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

using ConcreteShardRsm = Raft<SimulatorTransport, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

template <typename IoImpl>
void RunShardRaft(
    Raft<IoImpl, ShardRsm, CreateVerticesRequest, CreateVerticesResponse, ScanVerticesRequest, ScanVerticesResponse>
        server) {
  server.Run();
}

// tests
namespace {

void TestCreateVertices(const ShardClient &client) {}

}  // namespace

int main() {
  // SimulatorConfig config{
  //     .drop_percent = 0,
  //     .perform_timeouts = false,
  //     .scramble_messages = false,
  //     .rng_seed = 0,
  //     .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
  //     .abort_time = Time::min() + std::chrono::microseconds{2 * 8 * 1024 * 1024},
  // };

  // auto simulator = Simulator(config);

  // Io<SimulatorTransport> shard_server_io = simulator.RegisterNew();
  // Io<SimulatorTransport> shard_client_io = simulator.RegisterNew();

  // //   explicit ShardRsm(LabelId primary_label, PrimaryKey min_primary_key, std::optional<PrimaryKey>
  // max_primary_key,
  // //                 Config config = Config())

  // PropertyValue age{50};
  // PropertyValue is_prop{true};

  // auto primary_label = LabelId::FromUint(5);
  // std::vector<PropertyValue> prim_key = {age, is_prop};

  // auto asd = ShardRsm(primary_label, prim_key, std::nullopt);

  // ConcreteShardRsm shard_server(std::move(shard_server_io), {}, std::move(asd));

  // auto server_thread = std::jthread([&shard_server] {shard_server.Run(); });
  // simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_io.GetAddress());

  // std::cout << "Beginning test after servers have become quiescent." << std::endl;

  // ShardClient client(shard_client_io, shard_server_io.GetAddress(), {shard_client_io.GetAddress()});

  // TestCreateVertices(client);
}
