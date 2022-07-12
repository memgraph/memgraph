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
#include <thread>
#include <vector>

#include "io/v3/simulator.hpp"

using Op = std::vector<uint8_t>;
using Term = uint64_t;
using LogIndex = uint64_t;

/// The request that a client sends to request that
/// the cluster replicates their data.
struct ReplicationRequest {
  std::vector<uint8_t> opaque_data;
};

struct ReplicationResponse {
  bool success;
  std::optional<Address> retry_leader;
};

struct AppendEntriesRequest {
  Term term;
  Term prev_log_index;
  Term prev_log_term;
  std::vector<std::pair<Term, Op>> entries;
  Term leader_commit;
};

struct AppendEntriesResponse {
  bool success;
  Term last_log_term;
  // a small optimization over the raft paper, tells
  // the leader the offset that we are interested in
  // to send log offsets from for us. This will only
  // be useful at the beginning of a leader's term.
  Term last_log_index;
};

struct RequestVotesRequest {
  Term term;
  Term candidate_id;
  Term last_log_index;
  Term last_log_term;
};

struct RequestVotesResponse {
  Term term;
  bool vote_granted;
};

struct CommonState {
  Term current_term;
  std::optional<Address> voted_for;
  std::vector<std::pair<Term, Op>> log;
  LogIndex commit_index;
  LogIndex last_applied;
};

struct FollowerTracker {
  Address address;
  LogIndex next_index;
  std::optional<ResponseFuture<AppendEntriesResponse>> in_flight_message;
};

struct Leader {
  std::vector<FollowerTracker> followers;
};

struct Candidate {
  std::vector<ResponseFuture<RequestVotesResponse>> outstanding_votes;
  size_t successful_votes;
};

struct Follower {};

class Server {
 public:
  Server(Io<SimulatorTransport> io, std::vector<Address> peers) : io_(io), peers_(peers) {}

  void Run() {
    // 120ms between Cron calls
    io_.SetDefaultTimeoutMicroseconds(120000);

    while (!io_.ShouldShutDown()) {
      auto request_result = io_.Receive<AppendEntriesRequest, RequestVotesRequest, ReplicationRequest>();
      if (request_result.HasError()) {
        Cron();
        continue;
      }

      auto request = request_result.GetValue();

      // dispatch the message to a handler based on our role,
      // which can be specified in the Handle first argument,
      // or it can be `auto` if it's a handler for several roles
      // or messages.
      std::visit([&](auto &&msg, auto &&role) { return Handle(role, msg, request.request_id, request.from_address); },
                 request.message, role_);
    }
  }

 private:
  CommonState common_state_;
  std::variant<Leader, Candidate, Follower> role_ = Candidate{};
  Io<SimulatorTransport> io_;
  std::vector<Address> peers_;
  uint64_t last_heard_from_leader_;

  /// Periodic protocol maintenance. Leaders (re)send AppendEntriesRequest to followers
  /// and followers try to become the leader if they haven't heard from the leader within
  /// a randomized timeout.
  void Cron() {}

  // all roles can receive RequestVotes and possibly become a follower
  void Handle(auto &, RequestVotesRequest &req, uint64_t request_id, Address from_address) {
    std::cout << "RECEIVED RequestVotes :)" << std::endl;
    auto res = RequestVotesResponse{};

    io_.Send(from_address, request_id, res);
  }

  // only leaders actually handle replication requests
  void Handle(Leader &, ReplicationRequest &req, uint64_t request_id, Address from_address) {
    std::cout << "RECEIVED ReplicationRequest :)" << std::endl;

    // we are the leader. add item to log and send AppendEntries to peers
    common_state_.log.emplace_back(std::pair(common_state_.current_term, std::move(req.opaque_data)));

    // TODO add message to pending requests buffer, reply asynchronously
  }

  // non-leaders respond to replication requests with a redirection to the leader
  void Handle(auto &, ReplicationRequest &req, uint64_t request_id, Address from_address) {
    auto res = ReplicationResponse{};

    res.success = false;
    if (common_state_.voted_for) {
      std::cout << "redirecting client to known leader with port " << common_state_.voted_for->last_known_port
                << std::endl;
      res.retry_leader = *common_state_.voted_for;
    }

    io_.Send(from_address, request_id, res);
  }

  // anyone can receive an AppendEntriesRequest and potentially be flipped to a follower
  // state.
  void Handle(auto &, AppendEntriesRequest &aer, uint64_t request_id, Address from_address) {
    std::cout << "RECEIVED AppendEntries from a leader" << std::endl;
    bool error = false;

    if (from_address != common_state_.voted_for) {
      std::cout << "req.from_address is not who we voted for" << std::endl;
      error |= true;
    } else if (aer.term != common_state_.current_term) {
      std::cout << "req.term differs from our current leader term" << std::endl;
      error |= true;
    } else if (aer.prev_log_index > common_state_.log.size()) {
      std::cout << "req.prev_log_index is above our last applied log index" << std::endl;
      // TODO: buffer this and apply it later rather than having to wait for
      // the leader to double-send future segments to us.
      error |= true;
    } else {
      auto [prev_log_term, data] = common_state_.log.at(aer.prev_log_index);

      if (aer.prev_log_term != prev_log_term) {
        std::cout << "req.prev_log_term differs from our leader term at that slot" << std::endl;
        error |= true;
      }
    }

    if (!error) {
      // happy path
      last_heard_from_leader_ = io_.Now();

      // possibly chop-off stuff that was replaced by
      // things with different terms (we got data that
      // hasn't reached consensus yet, which is normal)
      // MG_ASSERT(req.last_log_index > common_state_.commit_index);
      common_state_.log.resize(aer.prev_log_index);

      common_state_.log.insert(common_state_.log.end(), aer.entries.begin(), aer.entries.end());

      common_state_.commit_index = std::min(aer.leader_commit, common_state_.log.size());
    }

    auto res = AppendEntriesResponse{
        .success = !error,
        .last_log_term = common_state_.current_term,
        .last_log_index = common_state_.log.size(),
    };

    io_.Send(from_address, request_id, res);
  }

  // unhandled messages should trigger an assertion failure
  void Handle(auto &, auto &, uint64_t request_id, Address from_address) {
    std::cout << "RECEIVED unhandled message :(" << std::endl;
    std::terminate();
  }
};

void RunServer(Server server) { server.Run(); }

int main() {
  auto simulator = Simulator();
  auto cli_addr = Address::TestAddress(1);
  auto srv_addr_1 = Address::TestAddress(2);
  auto srv_addr_2 = Address::TestAddress(3);
  auto srv_addr_3 = Address::TestAddress(4);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr, false);
  Io<SimulatorTransport> srv_io_1 = simulator.Register(srv_addr_1, true);
  Io<SimulatorTransport> srv_io_2 = simulator.Register(srv_addr_2, true);
  Io<SimulatorTransport> srv_io_3 = simulator.Register(srv_addr_3, true);

  std::vector<Address> srv_1_peers = {srv_addr_2, srv_addr_3};
  std::vector<Address> srv_2_peers = {srv_addr_1, srv_addr_3};
  std::vector<Address> srv_3_peers = {srv_addr_1, srv_addr_2};

  Server srv_1{srv_io_1, srv_1_peers};
  Server srv_2{srv_io_2, srv_2_peers};
  Server srv_3{srv_io_3, srv_3_peers};

  auto srv_thread_1 = std::jthread(RunServer, std::move(srv_1));
  auto srv_thread_2 = std::jthread(RunServer, std::move(srv_2));
  auto srv_thread_3 = std::jthread(RunServer, std::move(srv_3));

  // send request
  ReplicationRequest cli_req;
  cli_req.opaque_data = std::vector<uint8_t>{1, 2, 3, 4};

  auto response_future = cli_io.Request<ReplicationRequest, ReplicationResponse>(srv_addr_1, cli_req);

  // receive response
  auto response_result = response_future.Wait();
  auto response_envelope = response_result.GetValue();

  auto response = std::any_cast<ReplicationResponse>(response_envelope.message);

  MG_ASSERT(response.success);

  simulator.ShutDown();

  srv_thread_1.join();
  srv_thread_2.join();
  srv_thread_3.join();

  return 0;
}
