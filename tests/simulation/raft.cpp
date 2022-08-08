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
#include "io/rsm/raft.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"

using memgraph::io::Address;
using memgraph::io::Duration;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::Time;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
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
  bool cas_success;
  std::optional<int> last_value;
};

struct GetRequest {
  int key;
};

struct GetResponse {
  std::optional<int> value;
};

class TestState {
  std::map<int, int> state_;

 public:
  GetResponse Read(GetRequest request) {
    GetResponse ret;
    if (state_.contains(request.key)) {
      ret.value = state_[request.key];
    }
    return ret;
  }

  CasResponse Apply(CasRequest request) {
    CasResponse ret;

    // Key exist
    if (state_.contains(request.key)) {
      auto &val = state_[request.key];

      /*
       *   Delete
       */
      if (!request.new_value) {
        ret.last_value = val;
        ret.cas_success = true;

        state_.erase(state_.find(request.key));
      }

      /*
       *   Update
       */
      // Does old_value match?
      if (request.old_value == val) {
        ret.last_value = val;
        ret.cas_success = true;

        val = request.new_value.value();
      } else {
        ret.last_value = val;
        ret.cas_success = false;
      }
    }
    /*
     *   Create
     */
    else {
      ret.last_value = std::nullopt;
      ret.cas_success = true;

      state_.emplace(request.key, std::move(request.new_value).value());
    }

    return ret;
  }
};

template <typename IoImpl, typename WriteRequestT, typename WriteResponseT, typename ReadRequestT,
          typename ReadResponseT>
class RsmClient {
  using ServerPool = std::vector<Address>;

  IoImpl io_;
  Address leader_;

  std::mt19937 cli_rng_{0};
  ServerPool server_addrs_;

  template <typename ResponseT>
  std::optional<ResponseT> CheckForCorrectLeader(ResponseT response) {
    if (response.retry_leader) {
      MG_ASSERT(!response.success, "retry_leader should never be set for successful responses");
      leader_ = response.retry_leader.value();
      std::cout << "client redirected to leader server " << leader_.last_known_port << std::endl;
    } else if (!response.success) {
      std::uniform_int_distribution<size_t> addr_distrib(0, (server_addrs_.size() - 1));
      size_t addr_index = addr_distrib(cli_rng_);
      leader_ = server_addrs_[addr_index];

      std::cout << "client NOT redirected to leader server, trying a random one at index " << addr_index
                << " with port " << leader_.last_known_port << std::endl;
      return {};
    }

    return response;
  }

 public:
  RsmClient(IoImpl &&io, Address &&leader, ServerPool &&server_addrs)
      : io_{io}, leader_{leader}, server_addrs_{server_addrs} {}

  RsmClient() = delete;

  std::optional<WriteResponse<WriteResponseT>> SendWriteRequest(WriteRequestT req) {
    WriteRequest<WriteRequestT> client_req;
    client_req.operation = req;

    std::cout << "client sending CasRequest to Leader " << leader_.last_known_port << std::endl;
    ResponseFuture<WriteResponse<WriteResponseT>> response_future =
        io_.template Request<WriteRequest<WriteRequestT>, WriteResponse<WriteResponseT>>(leader_, client_req);
    ResponseResult<WriteResponse<WriteResponseT>> response_result = std::move(response_future).Wait();

    if (response_result.HasError()) {
      std::cout << "client timed out while trying to communicate with leader server " << std::endl;
      // continue;
      return std::nullopt;
    }

    ResponseEnvelope<WriteResponse<WriteResponseT>> response_envelope = response_result.GetValue();
    WriteResponse<WriteResponseT> write_response = response_envelope.message;

    return CheckForCorrectLeader(write_response);
  }

  std::optional<ReadResponse<ReadResponseT>> SendReadRequest(ReadRequestT req) {
    ReadRequest<ReadRequestT> read_req;
    read_req.operation = req;

    std::cout << "client sending GetRequest to Leader " << leader_.last_known_port << std::endl;
    ResponseFuture<ReadResponse<ReadResponseT>> get_response_future =
        io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req);

    // receive response
    ResponseResult<ReadResponse<ReadResponseT>> get_response_result = std::move(get_response_future).Wait();

    if (get_response_result.HasError()) {
      std::cout << "client timed out while trying to communicate with leader server " << std::endl;
      return {};
    }

    ResponseEnvelope<ReadResponse<ReadResponseT>> get_response_envelope = get_response_result.GetValue();
    ReadResponse<ReadResponseT> read_get_response = get_response_envelope.message;

    if (!read_get_response.success) {
      // sent to a non-leader
      return {};
    }

    return CheckForCorrectLeader(read_get_response);
  }
};

template <typename IoImpl>
void RunRaft(Raft<IoImpl, TestState, CasRequest, CasResponse, GetRequest, GetResponse> server) {
  server.Run();
}

void RunSimulation() {
  SimulatorConfig config{
      .drop_percent = 5,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{8 * 1024 * 1024},
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

  using RaftClass = Raft<SimulatorTransport, TestState, CasRequest, CasResponse, GetRequest, GetResponse>;
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
  std::vector<Address> server_addrs{srv_addr_1, srv_addr_2, srv_addr_3};
  Address leader = server_addrs[0];

  RsmClient<Io<SimulatorTransport>, CasRequest, CasResponse, GetRequest, GetResponse> client(
      std::move(cli_io), std::move(leader), std::move(server_addrs));

  const int key = 0;
  std::optional<int> last_known_value;

  bool success = false;

  for (int i = 0; !success; i++) {
    /*
     * Write Request
     */
    CasRequest cas_req;
    cas_req.key = key;

    cas_req.old_value = last_known_value;

    cas_req.new_value = i;

    auto write_cas_response_opt = client.SendWriteRequest(cas_req);
    if (!write_cas_response_opt) {
      continue;
    }
    auto write_cas_response = write_cas_response_opt.value();

    CasResponse cas_response = write_cas_response.write_return;

    bool cas_succeeded = cas_response.cas_success;

    std::cout << "Client received CasResponse! success: " << cas_succeeded
              << " last_known_value: " << (int)*last_known_value << std::endl;

    if (cas_succeeded) {
      last_known_value = i;
    } else {
      last_known_value = cas_response.last_value;
      continue;
    }

    /*
     * Get Request
     */
    GetRequest get_req;
    get_req.key = key;

    auto read_get_response_opt = client.SendReadRequest(get_req);
    if (!read_get_response_opt) {
      continue;
    }
    auto read_get_response = read_get_response_opt.value();

    GetResponse get_response = read_get_response.read_return;

    MG_ASSERT(get_response.value == i);

    std::cout << "client successfully cas'd a value and read it back! value: " << i << std::endl;

    success = true;
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
