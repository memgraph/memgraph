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
#include <map>
#include <set>
#include <thread>
#include <vector>

#include "io/v3/simulator.hpp"

using Op = std::vector<uint8_t>;
using Term = uint64_t;
using LogIndex = uint64_t;
using Time = uint64_t;
using Duration = uint64_t;
using RequestId = uint64_t;

/// The request that a client sends to request that
/// the cluster replicates their data.
struct ReplicationRequest {
  std::vector<uint8_t> opaque_data;
};

struct ReplicationResponse {
  bool success;
  std::optional<Address> retry_leader;
};

struct AppendRequest {
  Term term;
  Term last_log_index;
  Term last_log_term;
  std::vector<std::pair<Term, Op>> entries;
  Term leader_commit;
};

struct AppendResponse {
  bool success;
  Term last_log_term;
  // a small optimization over the raft paper, tells
  // the leader the offset that we are interested in
  // to send log offsets from for us. This will only
  // be useful at the beginning of a leader's term.
  Term last_log_index;
};

struct VoteRequest {
  Term term;
  LogIndex last_log_index;
  Term last_log_term;
};

struct VoteResponse {
  Term term;
  LogIndex commit_index;
  bool vote_granted;
};

struct CommonState {
  Term term;
  std::vector<std::pair<Term, Op>> log;
  LogIndex commit_index;
  LogIndex last_applied;
};

struct FollowerTracker {
  LogIndex next_index;
  LogIndex confirmed_contiguous_index = 0;
};

struct Leader {
  std::map<Address, FollowerTracker> followers;
};

struct Candidate {
  std::map<Address, LogIndex> successful_votes;
  Time election_began;
  std::set<Address> outstanding_votes;
};

struct Follower {
  Time last_received_append_entries_timestamp;
  Address leader_address;
};

using Role = std::variant<Candidate, Leader, Follower>;

template <typename IoImpl>
class Server {
 public:
  Server(Io<IoImpl> io, std::vector<Address> peers) : io_(io), peers_(peers) {}

  void Run() {
    // 120ms between Cron calls
    Duration cron_interval = 120000;
    Time last_cron = 0;

    io_.SetDefaultTimeoutMicroseconds(RandomTimeout(100000, 150000));

    while (!io_.ShouldShutDown()) {
      auto now = io_.Now();
      if (now - last_cron > cron_interval) {
        Log("doing cron");
        Cron();
        last_cron = now;
      }

      Log("waiting for requests");

      auto request_result = io_.template Receive<AppendRequest, VoteRequest, VoteResponse, ReplicationRequest>();
      if (request_result.HasError()) {
        continue;
      }

      Log("received request");

      auto request = std::move(request_result.GetValue());

      Handle(std::move(request.message), request.request_id, request.from_address);
    }
  }

 private:
  CommonState state_;
  Role role_ = Candidate{};
  Io<IoImpl> io_;
  std::vector<Address> peers_;
  Time last_heard_from_leader_;

  Duration RandomTimeout(Duration min, Duration max) {
    std::uniform_int_distribution<> time_distrib(min, max);
    return io_.Rand(time_distrib);
  }

  LogIndex LastLogIndex() { return state_.log.size(); }

  Term LastLogTerm() {
    if (state_.log.empty()) {
      return 0;
    } else {
      auto &[term, data] = state_.log.back();
      return term;
    }
  }

  /// Periodic protocol maintenance.
  void Cron() {
    // dispatch periodic logic based on our role to a specific Cron method.
    std::optional<Role> new_role = std::visit([&](auto &&role) { return Cron(role); }, role_);

    if (new_role) {
      Log("becoming new role");
      role_ = std::move(new_role).value();
    }
  }

  // Candidates keep sending Vote to peers until:
  // 1. receiving Append with a higher term (become Follower)
  // 2. receiving Vote with a higher term (become a Follower)
  // 3. receiving a quorum of responses to our last batch of Vote (become a Leader)
  std::optional<Role> Cron(Candidate &candidate) {
    auto now = io_.Now();
    Duration election_timeout = RandomTimeout(100000, 150000);

    if (now - candidate.election_began > election_timeout) {
      Log("running for election");
      state_.term++;
      VoteRequest request{
          .term = state_.term,
          .last_log_index = LastLogIndex(),
          .last_log_term = LastLogTerm(),
      };

      auto outstanding_votes = std::set<Address>();

      for (const auto &peer : peers_) {
        // request_id not necessary to set because it's not a Future-backed Request.
        auto request_id = 0;
        io_.template Send<VoteRequest>(peer, request_id, request);
        outstanding_votes.insert(peer);
      }

      return Candidate{
          .successful_votes = std::map<Address, LogIndex>(),
          .election_began = now,
          .outstanding_votes = outstanding_votes,
      };
    }
    return std::nullopt;
  }

  // Followers become candidates if we haven't heard from the leader
  // after a randomized timeout.
  std::optional<Role> Cron(Follower &follower) {
    auto now = io_.Now();
    auto time_since_last_append_entries = now - follower.last_received_append_entries_timestamp;
    Duration election_timeout = RandomTimeout(100000, 150000);

    // randomized follower timeout with a range of 100-150ms.
    if (time_since_last_append_entries > election_timeout) {
      // become a Candidate if we haven't heard from the Leader after this timeout
      return Candidate{};
    } else {
      return std::nullopt;
    }
  }

  // Leaders (re)send AppendRequest to followers.
  std::optional<Role> Cron(Leader &) {
    // TODO time-out client requests if we haven't made progress after some threshold
    return std::nullopt;
  }

  /// **********************************************
  /// Handle + std::visit is how events are dispatched
  /// to certain code based on Server role.
  ///
  /// Handle(role, message, ...)
  /// takes as the first argument a reference
  /// to its role, and as the second argument, the
  /// message that has been received.
  /// **********************************************
  void Handle(std::variant<AppendRequest, VoteRequest, VoteResponse, ReplicationRequest> &&message_variant,
              RequestId request_id, Address from_address) {
    // dispatch the message to a handler based on our role,
    // which can be specified in the Handle first argument,
    // or it can be `auto` if it's a handler for several roles
    // or messages.
    std::optional<Role> new_role =
        std::visit([&](auto &&msg, auto &&role) { return Handle(role, std::move(msg), request_id, from_address); },
                   std::move(message_variant), role_);

    // TODO(m3) maybe replace std::visit with get_if for explicit prioritized matching, [[likely]] etc...
    if (new_role) {
      Log("becoming new role");
      role_ = std::move(new_role).value();
    }
  }

  // all roles can receive Vote and possibly become a follower
  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, VoteRequest &&req, RequestId request_id, Address from_address) {
    Log("received Vote");
    bool last_log_term_dominates = req.last_log_term >= LastLogTerm();
    bool term_dominates = req.term > state_.term;
    bool last_log_index_dominates = req.last_log_index >= LastLogIndex();
    bool new_leader = last_log_term_dominates && term_dominates && last_log_index_dominates;

    VoteResponse res{
        .term = std::max(req.term, state_.term),
        .commit_index = state_.commit_index,
        .vote_granted = new_leader,
    };

    io_.Send(from_address, request_id, res);

    if (new_leader) {
      // become a follower
      state_.term = req.term;
      return Follower{
          .last_received_append_entries_timestamp = io_.Now(),
          .leader_address = from_address,
      };
    } else {
      return std::nullopt;
    }
  }

  std::optional<Role> Handle(Candidate &candidate, VoteResponse &&res, RequestId, Address from_address) {
    Log("Candidate received VoteResponse");

    if (res.term != state_.term) {
      MG_ASSERT(res.term < state_.term, "Somehow received a VoteResponse from the future!");
      // we received a delayed VoteResponse from the past, which has to do with an election that is
      // no longer valid. We can simply drop this.
      Log("received VoteResponse from old term ", res.term, " but our candidacy term is ", state_.term);
      return std::nullopt;
    }

    if (res.vote_granted) {
      MG_ASSERT(candidate.outstanding_votes.contains(from_address),
                "Received unexpected VoteResponse from server not present in Candidate's outstanding_votes!");
      candidate.outstanding_votes.erase(from_address);

      MG_ASSERT(!candidate.successful_votes.contains(from_address),
                "Received unexpected VoteResponse from server already in Candidate's successful_votes!");
      candidate.successful_votes.insert({from_address, res.commit_index});

      if (candidate.successful_votes.size() >= candidate.outstanding_votes.size()) {
        Log("becoming Leader");

        std::map<Address, FollowerTracker> followers{};

        for (const auto &[address, commit_index] : candidate.successful_votes) {
          FollowerTracker follower{
              .next_index = commit_index,
              .confirmed_contiguous_index = commit_index,
          };
          followers.insert({address, std::move(follower)});
        }
        for (const auto &address : candidate.outstanding_votes) {
          FollowerTracker follower{
              .next_index = state_.log.size(),
              .confirmed_contiguous_index = 0,
          };
          followers.insert({address, follower});
        }

        return Leader{
            .followers = std::move(followers),
        };
      }
    }

    return std::nullopt;
  }

  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, VoteResponse &&res, RequestId request_id, Address from_address) {
    Log("non-Candidate received VoteResponse");
    return std::nullopt;
  }

  // only leaders actually handle replication requests from clients
  std::optional<Role> Handle(Leader &, ReplicationRequest &&req, RequestId request_id, Address from_address) {
    Log("leader received ReplicationRequest");

    // we are the leader. add item to log and send Append to peers
    state_.log.emplace_back(std::pair(state_.term, std::move(req.opaque_data)));

    // TODO add message to pending requests buffer, reply asynchronously
    return std::nullopt;
  }

  std::optional<Role> Handle(Follower &follower, ReplicationRequest &&req, RequestId request_id, Address from_address) {
    auto res = ReplicationResponse{};

    res.success = false;
    Log("redirecting client to known leader with port ", follower.leader_address.last_known_port);
    res.retry_leader = follower.leader_address;

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Candidate &, ReplicationRequest &&req, RequestId request_id, Address from_address) {
    Log("candidate received ReplicationRequest - not redirecting because no leader is known");
    auto res = ReplicationResponse{};

    res.success = false;

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Follower &follower, AppendRequest &&aer, RequestId request_id, Address from_address) {
    if (follower.leader_address != from_address) {
    } else if (aer.term != state_.term) {
    } else {
      follower.last_received_append_entries_timestamp = io_.Now();
    }

    /*
    if (from_address != state_.voted_for) {
      Log("req.from_address is not who we voted for");
      error |= true;
    } else if (aer.term != state_.term) {
      Log("req.term differs from our current leader term");
      error |= true;
    } else if (aer.last_log_index > state_.log.size()) {
      Log("req.last_log_index is above our last applied log index");
      // TODO: buffer this and apply it later rather than having to wait for
      // the leader to double-send future segments to us.
      error |= true;
    } else {
      auto [last_log_term, data] = state_.log.at(aer.last_log_index);

      if (aer.last_log_term != last_log_term) {
        Log("req.last_log_term differs from our leader term at that slot");
        error |= true;
      }
    }

    if (!error) {
      // happy path
      last_heard_from_leader_ = io_.Now();

      // possibly chop-off stuff that was replaced by
      // things with different terms (we got data that
      // hasn't reached consensus yet, which is normal)
      // MG_ASSERT(req.last_log_index > state_.commit_index);
      state_.log.resize(aer.last_log_index);

      state_.log.insert(state_.log.end(), aer.entries.begin(), aer.entries.end());

      state_.commit_index = std::min(aer.leader_commit, state_.log.size());
    }
    */

    return std::nullopt;
  }

  // anyone can receive an AppendRequest and potentially be flipped to a follower
  // state.
  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, AppendRequest &&req, RequestId request_id, Address from_address) {
    Log("non-Follower received Append from a Leader");

    bool error = true;

    std::optional<Role> ret = std::nullopt;

    if (req.term > state_.term) {
      // become follower of this leader, reply with our log status

      error = false;

      state_.term = req.term;

      ret = Follower{
          .last_received_append_entries_timestamp = io_.Now(),
          .leader_address = from_address,
      };
    }

    AppendResponse res{
        .success = error,
        .last_log_term = state_.term,
        .last_log_index = state_.log.size(),
    };

    io_.Send(from_address, request_id, res);

    return ret;
  }

  template <typename... Ts>
  void Log(Ts &&...args) {
    std::cout << "raft server " << (int)io_.GetAddress().last_known_port << " ";
    (std::cout << ... << args) << std::endl;
  }
};

template <typename IoImpl>
void RunServer(Server<IoImpl> server) {
  server.Run();
}

int main() {
  auto config = SimulatorConfig{
      .drop_percent = 0,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
  };

  auto simulator = Simulator(config);

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

  auto srv_thread_1 = std::jthread(RunServer<SimulatorTransport>, std::move(srv_1));
  auto srv_thread_2 = std::jthread(RunServer<SimulatorTransport>, std::move(srv_2));
  auto srv_thread_3 = std::jthread(RunServer<SimulatorTransport>, std::move(srv_3));

  // send request
  ReplicationRequest cli_req;
  cli_req.opaque_data = std::vector<uint8_t>{1, 2, 3, 4};

  ResponseFuture<ReplicationResponse> response_future =
      cli_io.RequestWithTimeout<ReplicationRequest, ReplicationResponse>(srv_addr_1, cli_req, 100);

  // receive response
  ResponseResult<ReplicationResponse> response_result = response_future.Wait();
  ResponseEnvelope<ReplicationResponse> response_envelope = response_result.GetValue();
  ReplicationResponse response = response_envelope.message;

  MG_ASSERT(response.success);

  simulator.ShutDown();

  srv_thread_1.join();
  srv_thread_2.join();
  srv_thread_3.join();

  return 0;
}
