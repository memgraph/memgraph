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

#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "io/address.hpp"
#include "io/rsm/raft.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"

using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::rsm::Raft;
// using memgraph::io::rsm::ReplicationRequest;
// using memgraph::io::rsm::ReplicationResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

struct CasRequest {
  int key;
  std::optional<int> old_value;
  std::optional<int> new_value;
};

struct CasResponse {
  bool success;
  std::optional<int> last_value;
  std::optional<Address> retry_leader;
};

struct ReadRequest {
  int key;
};

struct ReadResponse {
  int value;
  std::optional<Address> retry_leader;
};

class TestState {
  std::map<int, int> state_;

 public:
  ReadResponse read(ReadRequest request) {
    ReadResponse ret;
    ret.value = state_.at(request.key);
    return ret;
  }

  CasResponse apply(CasRequest request) {
    CasResponse ret;

    // TODO(gabor) remove my annoying-ass comments
    // Key exist
    if (state_.find(request.key) != state_.end()) {
      auto &val = state_[request.key];

      /*
       *   Delete
       */
      if (!request.new_value) {
        ret.last_value = val;
        ret.success = true;

        state_.erase(state_.find(request.key));
      }

      /*
       *   Update
       */
      // Does old_value match?
      if (request.old_value == val) {
        ret.last_value = val;
        ret.success = true;

        val = request.new_value.value();
      } else {
        ret.last_value = val;
        ret.success = false;
      }
    }
    /*
     *   Create
     */
    else {
      ret.last_value = std::nullopt;
      ret.success = true;

      state_[request.key] = request.new_value.value();
    }

    return ret;
  }
};

template <typename IoImpl>
void RunRaft(Raft<IoImpl, TestState, CasRequest, CasResponse, ReadRequest, ReadResponse> server) {
  server.Run();
}

void RunSimulation() {
  SimulatorConfig config{
      .drop_percent = 5,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
      .start_time = 256 * 1024,
      .abort_time = 8 * 1024 * 1024,
  };

  auto simulator = Simulator(config);

  auto cli_addr = Address::TestAddress(1);
  auto srv_addr_1 = Address::TestAddress(2);
  auto srv_addr_2 = Address::TestAddress(3);
  auto srv_addr_3 = Address::TestAddress(4);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> srv_io_1 = simulator.Register(srv_addr_1);
  Io<SimulatorTransport> srv_io_2 = simulator.Register(srv_addr_2);
  Io<SimulatorTransport> srv_io_3 = simulator.Register(srv_addr_3);

  std::vector<Address> srv_1_peers = {srv_addr_2, srv_addr_3};
  std::vector<Address> srv_2_peers = {srv_addr_1, srv_addr_3};
  std::vector<Address> srv_3_peers = {srv_addr_1, srv_addr_2};

  // TODO(tyler / gabor) supply default TestState to Raft constructor
  using RaftClass = Raft<SimulatorTransport, TestState, CasRequest, CasResponse, ReadRequest, ReadResponse>;
  RaftClass srv_1{std::move(srv_io_1), srv_1_peers, TestState{}};
  RaftClass srv_2{std::move(srv_io_2), srv_2_peers, TestState{}};
  RaftClass srv_3{std::move(srv_io_3), srv_3_peers, TestState{}};

  auto srv_thread_1 = std::jthread(RunRaft<SimulatorTransport>, std::move(srv_1));
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr_1);

  auto srv_thread_2 = std::jthread(RunRaft<SimulatorTransport>, std::move(srv_2));
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr_2);

  auto srv_thread_3 = std::jthread(RunRaft<SimulatorTransport>, std::move(srv_3));
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr_3);

  std::cout << "beginning test after servers have become quiescent" << std::endl;

  std::mt19937 cli_rng_{0};
  Address server_addrs[]{srv_addr_1, srv_addr_2, srv_addr_3};
  bool success = false;
  Address leader = server_addrs[0];

  while (true) {
    // send request
    CasRequest cli_req;
    cli_req.key = 0;
    cli_req.old_value = std::nullopt;
    cli_req.new_value = 12;

    // TODO(tyler / gabor) replace Replication* with Cas/Read

    std::cout << "client sending ReplicationRequest to Leader " << leader.last_known_port << std::endl;
    ResponseFuture<CasResponse> response_future =
        cli_io.RequestWithTimeout<CasRequest, CasResponse>(leader, cli_req, 50000);

    // receive response
    ResponseResult<CasResponse> response_result = response_future.Wait();

    if (response_result.HasError()) {
      std::cout << "client timed out while trying to communicate with leader server " << std::endl;
      continue;
    }

    ResponseEnvelope<CasResponse> response_envelope = response_result.GetValue();
    CasResponse response = response_envelope.message;

    if (response.success) {
      success = true;
      break;
    }

    if (response.retry_leader) {
      leader = response.retry_leader.value();
      std::cout << "client redirected to leader server " << leader.last_known_port << std::endl;
    } else {
      std::uniform_int_distribution<size_t> addr_distrib(0, 2);
      size_t addr_index = addr_distrib(cli_rng_);
      leader = server_addrs[addr_index];

      std::cout << "client NOT redirected to leader server, trying a random one at index " << addr_index
                << " with port " << leader.last_known_port << std::endl;
    }
  }

  MG_ASSERT(success);

  simulator.ShutDown();

  SimulatorStats stats = simulator.Stats();

  std::cout << "total messages:     " << stats.total_messages << std::endl;
  std::cout << "dropped messages:   " << stats.dropped_messages << std::endl;
  std::cout << "timed out requests: " << stats.timed_out_requests << std::endl;
  std::cout << "total requests:     " << stats.total_requests << std::endl;
  std::cout << "total responses:    " << stats.total_responses << std::endl;
  std::cout << "simulator ticks:    " << stats.simulator_ticks << std::endl;

  std::cout << "========================== SUCCESS :) ==========================" << std::endl;

  /*
  this is implicit in jthread's dtor
  srv_thread_1.join();
  srv_thread_2.join();
  srv_thread_3.join();
  */
}

int main() {
  int n_tests = 50;

  for (int i = 0; i < n_tests; i++) {
    std::cout << "========================== NEW SIMULATION " << i << " ==========================" << std::endl;
    std::cout << "\tTime\tTerm\tPort\tRole\t\tMessage\n";
    RunSimulation();
  }

  std::cout << "passed " << n_tests << " tests!" << std::endl;

  return 0;
}
