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

struct PersistentState {
  Term current_term;
  std::optional<Address> voted_for;
  std::vector<std::pair<Term, Op>> log;
};

struct VolatileState {
  Term commit_index;
  Term last_applied;
};

struct LeaderState {
  // for each server, index of the next log entry
  // to send to that server (initialized to leader
  // last log index + 1)
  std::map<Address, Term> next_index;
  // or each server, index of highest log entry
  // known to be replicated on server
  // (initialized to 0, increases monotonically)
  std::map<Address, Term> match_index;
};

template <Message M>
struct SingleRequestEnvelope {
  M message;
  uint64_t request_id;
  Address from_address;

  template <Message T, typename I>
  void Reply(T response, Io<I> &io) {
    io.Send(from_address, request_id, response);
  }
};

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
      std::variant<AppendEntriesRequest, RequestVotesRequest, ReplicationRequest> message = request.message;

      if (AppendEntriesRequest *m = std::get_if<AppendEntriesRequest>(&message)) {
        std::cout << "RECEIVED AppendEntries :)" << std::endl;
        SingleRequestEnvelope<AppendEntriesRequest> re = {
            .message = std::move(*m), .request_id = request.request_id, .from_address = request.from_address};
        HandleAppendEntriesRequest(re);
      } else if (RequestVotesRequest *m = std::get_if<RequestVotesRequest>(&message)) {
        std::cout << "RECEIVED RequestVotes :)" << std::endl;
        SingleRequestEnvelope<RequestVotesRequest> re = {
            .message = std::move(*m), .request_id = request.request_id, .from_address = request.from_address};
        HandleRequestVotesRequest(re);
      } else if (ReplicationRequest *m = std::get_if<ReplicationRequest>(&message)) {
        std::cout << "RECEIVED ReplicationRequest :)" << std::endl;
        SingleRequestEnvelope<ReplicationRequest> re = {
            .message = std::move(*m), .request_id = request.request_id, .from_address = request.from_address};
        HandleReplicationRequest(re);
      } else {
        std::cout << "RECEIVED BAD REQUEST :(" << std::endl;
      }
    }
  }

 private:
  PersistentState ps_;
  VolatileState vs_;
  std::optional<LeaderState> ls_;
  Io<SimulatorTransport> io_;
  std::vector<Address> peers_;
  uint64_t last_heard_from_leader_;

  /// Periodic protocol maintenance. Leaders (re)send AppendEntriesRequest to followers
  /// and followers try to become the leader if they haven't heard from the leader within
  /// a randomized timeout.
  void Cron() {
    if (ls_) {
    } else {
    }
  }

  void HandleAppendEntriesRequest(SingleRequestEnvelope<AppendEntriesRequest> &req) {
    auto res = AppendEntriesResponse{};
    auto &aer = req.message;

    bool error = false;

    if (req.from_address != ps_.voted_for) {
      std::cout << "req.from_address is not who we voted for" << std::endl;
      error |= true;
    } else if (aer.term != ps_.current_term) {
      std::cout << "req.term differs from our current leader term" << std::endl;
      error |= true;
    } else if (aer.prev_log_index > ps_.log.size()) {
      std::cout << "req.prev_log_index is above our last applied log index" << std::endl;
      // TODO: buffer this and apply it later rather than having to wait for
      // the leader to double-send future segments to us.
      error |= true;
    } else {
      auto [prev_log_term, data] = ps_.log.at(aer.prev_log_index);

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
      // MG_ASSERT(req.last_log_index > vs_.commit_index);
      ps_.log.resize(aer.prev_log_index);

      ps_.log.insert(ps_.log.end(), aer.entries.begin(), aer.entries.end());

      vs_.commit_index = std::min(aer.leader_commit, ps_.log.size());

      res.success = true;
    } else {
      res.success = false;
    }

    res.last_log_term = ps_.current_term;
    res.last_log_index = ps_.log.size();

    req.Reply(res, io_);
  }

  void HandleRequestVotesRequest(SingleRequestEnvelope<RequestVotesRequest> &req) {
    auto srv_res = RequestVotesResponse{};

    req.Reply(srv_res, io_);
  }

  void HandleReplicationRequest(SingleRequestEnvelope<ReplicationRequest> &req) {
    auto srv_res = ReplicationResponse{};

    if (ls_) {
      // we are the leader. add item to log and send AppendEntries to peers
      ps_.log.emplace_back(std::pair(ps_.current_term, std::move(req.message.opaque_data)));
    } else {
      srv_res.success = false;
      if (ps_.voted_for) {
        std::cout << "redirecting client to known leader with port " << ps_.voted_for->last_known_port << std::endl;
        srv_res.retry_leader = *ps_.voted_for;
      }
    }

    req.Reply(srv_res, io_);
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
