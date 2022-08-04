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

// TODO(tyler) buffer out-of-order Append buffers to reassemble more quickly
// TODO(tyler) handle granular batch sizes based on simple flow control
// TODO(tyler) add "application" test that asserts that all state machines apply the same items in-order
// TODO(tyler) add proper token-based deterministic scheduling

#pragma once

#include <deque>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <vector>

#include "io/simulator/simulator.hpp"
#include "io/transport.hpp"

namespace memgraph::io::rsm {

using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

using Term = uint64_t;
using LogIndex = uint64_t;
using Time = uint64_t;
using Duration = uint64_t;
using RequestId = uint64_t;

template <typename WriteOperation>
struct WriteRequest {
  WriteOperation operation;
};

/// WriteResponse is returned to a client after
/// their WriteRequest was entered in to the raft
/// log and it reached consensus.
///
/// WriteReturn is the result of applying the WriteRequest to
/// ReplicatedState, and if the ReplicatedState::write
/// method is deterministic, all replicas will
/// have the same ReplicatedState after applying
/// the submitted WriteRequest.
template <typename WriteReturn>
struct WriteResponse {
  bool success;
  WriteReturn write_return;
  std::optional<Address> retry_leader;
};

template <typename ReadOperation>
struct ReadRequest {
  ReadOperation operation;
};

template <typename ReadReturn>
struct ReadResponse {
  bool success;
  ReadReturn read_return;
  std::optional<Address> retry_leader;
};

// TODO(tyler) add docs
template <typename WriteRequest>
struct AppendRequest {
  Term term = 0;
  LogIndex last_log_index;
  Term last_log_term;
  std::vector<std::pair<Term, WriteRequest>> entries;
  LogIndex leader_commit;
};

struct AppendResponse {
  bool success;
  Term term;
  Term last_log_term;
  // a small optimization over the raft paper, tells
  // the leader the offset that we are interested in
  // to send log offsets from for us. This will only
  // be useful at the beginning of a leader's term.
  LogIndex last_log_index;
};

struct VoteRequest {
  Term term = 0;
  LogIndex last_log_index;
  Term last_log_term;
};

struct VoteResponse {
  Term term = 0;
  LogIndex committed_log_size;
  bool vote_granted = false;
};

template <typename WriteRequest>
struct CommonState {
  Term term = 0;
  std::vector<std::pair<Term, WriteRequest>> log;
  LogIndex committed_log_size = 0;
  LogIndex last_applied = 0;
};

struct FollowerTracker {
  LogIndex next_index = 0;
  LogIndex confirmed_contiguous_index = 0;
};

struct PendingClientRequest {
  LogIndex log_index;
  RequestId request_id;
  Address address;
};

struct Leader {
  std::map<Address, FollowerTracker> followers;
  std::deque<PendingClientRequest> pending_client_requests;
  Time last_broadcast = 0;

  void Print() { std::cout << "\tLeader   \t"; }
};

struct Candidate {
  std::map<Address, LogIndex> successful_votes;
  Time election_began = 0;
  std::set<Address> outstanding_votes;

  void Print() { std::cout << "\tCandidate\t"; }
};

struct Follower {
  Time last_received_append_entries_timestamp;
  Address leader_address;

  void Print() { std::cout << "\tFollower \t"; }
};

using Role = std::variant<Candidate, Leader, Follower>;

/*
TODO make concept that expresses the fact that any RSM must
be able to have a specific type applied to it, returning
another interesting result type.

all ReplicatedState classes should have an apply method
that returns our WriteResponseValue:

ReadResponse read(ReadOperation);
WriteResponseValue ReplicatedState::apply(WriteRequest);

for examples:
if the state is uint64_t, and WriteRequest is `struct PlusOne {};`,
and WriteResponseValue is also uint64_t (the new value), then
each call to state.apply(PlusOne{}) will return the new value
after incrementing it. 0, 1, 2, 3... and this will be sent back
to the client that requested the mutation.

In practice, these mutations will usually be predicated on some
previous value, so that they are idempotent, functioning similarly
to a CAS operation.

template<typename Write, typename T, typename WriteResponse>
concept Rsm = requires(T t, Write w)
{
    { t.read(r) } -> std::same_as<ReadResponse>;
    { t.apply(w) } -> std::same_as<WriteResponseValue>;
};
*/

template <typename WriteOperation, typename ReadOperation, typename ReplicatedState, typename WriteResponseValue,
          typename ReadResponseValue>
concept Rsm = requires(ReplicatedState state, WriteOperation w, ReadOperation r) {
  { state.Read(r) } -> std::same_as<ReadResponseValue>;
  { state.Apply(w) } -> std::same_as<WriteResponseValue>;
};

/// Parameter           Purpose
/// --------------------------
/// IoImpl              the concrete Io provider - SimulatorTransport, ThriftTransport, etc...
/// ReplicatedState     the high-level data structure that is managed by the raft-backed replicated state machine
/// WriteOperation      the individual operation type that is applied to the ReplicatedState in identical order
///                     across each replica
/// WriteResponseValue  the return value of calling ReplicatedState::write(WriteOperation), which is executed in
///                     identical order across all replicas after an WriteOperation reaches consensus.
/// ReadOperation       the type of operations that do not require consensus before executing directly
///                     on a const ReplicatedState &
/// ReadResponseValue   the return value of calling ReplicatedState::read(ReadOperation), which is executed directly
///                     without going through consensus first
template <typename IoImpl, typename ReplicatedState, typename WriteOperation, typename WriteResponseValue,
          typename ReadOperation, typename ReadResponseValue>
requires Rsm<WriteOperation, ReadOperation, ReplicatedState, WriteResponseValue, ReadResponseValue>
class Raft {
  CommonState<WriteOperation> state_;
  Role role_ = Candidate{};
  Io<IoImpl> io_;
  std::vector<Address> peers_;
  ReplicatedState replicated_state_;

 public:
  Raft(Io<IoImpl> &&io, std::vector<Address> peers, ReplicatedState &&replicated_state)
      : io_(std::move(io)), peers_(peers), replicated_state_(std::move(replicated_state)) {}

  void Run() {
    Time last_cron = io_.Now();

    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();
      const Duration random_cron_interval = RandomTimeout(1000, 2000);
      if (now - last_cron > random_cron_interval) {
        Cron();
        last_cron = now;
      }

      Duration receive_timeout = RandomTimeout(10000, 50000);

      auto request_result =
          io_.template ReceiveWithTimeout<ReadRequest<ReadOperation>, AppendRequest<WriteOperation>, AppendResponse,
                                          WriteRequest<WriteOperation>, VoteRequest, VoteResponse>(receive_timeout);
      if (request_result.HasError()) {
        continue;
      }

      auto request = std::move(request_result.GetValue());

      Handle(std::move(request.message), request.request_id, request.from_address);
    }
  }

 private:
  // Raft paper - 5.3
  // When the entry has been safely replicated, the leader applies the
  // entry to its state machine and returns the result of that
  // execution to the client.
  void BumpCommitIndexAndReplyToClients(Leader &leader) {
    // set the current committed_log_size based on the
    auto indices = std::vector<LogIndex>{state_.log.size()};
    for (const auto &[addr, f] : leader.followers) {
      indices.push_back(f.confirmed_contiguous_index);
      Log("at port ", addr.last_known_port, " has confirmed contiguous index of: ", f.confirmed_contiguous_index);
    }
    std::ranges::sort(indices, std::ranges::greater());
    // assuming reverse sort (using std::ranges::greater)
    size_t new_committed_log_size = indices[(indices.size() / 2)];

    // TODO(tyler / gabor) for each index between the old
    // index and the new one, apply that log's WriteOperation
    // to our replicated_state_, and use the specific return
    // value of the ReplicatedState::apply method (WriteResponseValue)
    // to respondto the requester.
    //
    // this will completely replace the while loop below

    state_.committed_log_size = new_committed_log_size;

    Log("committed_log_size is now ", state_.committed_log_size);

    while (!leader.pending_client_requests.empty()) {
      const auto &front = leader.pending_client_requests.front();
      if (front.log_index <= state_.committed_log_size) {
        const auto &write_request = state_.log[front.log_index].second;
        WriteResponseValue write_return = replicated_state_.apply(write_request);
        WriteResponse<WriteResponseValue> resp;
        resp.success = true;
        resp.write_return = write_return;
        // Log("responding SUCCESS to client");
        // WriteResponse rr{
        //     .success = true,
        //     .retry_leader = std::nullopt,
        // };
        io_.Send(front.address, front.request_id, std::move(resp));
        leader.pending_client_requests.pop_front();
      } else {
        break;
      }
    }
  }

  // Raft paper - 5.1
  // AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat
  void BroadcastAppendEntries(std::map<Address, FollowerTracker> &followers) {
    for (auto &[address, follower] : followers) {
      const LogIndex index = follower.confirmed_contiguous_index;

      std::vector<std::pair<Term, WriteOperation>> entries;

      if (state_.log.size() > index) {
        entries.insert(entries.begin(), state_.log.begin() + index, state_.log.end());
      }

      const Term previous_term_from_index = PreviousTermFromIndex(index);

      Log("sending ", entries.size(), " entries to Follower ", address.last_known_port,
          " which are above its known index of ", index);

      AppendRequest<WriteOperation> ar{
          .term = state_.term,
          .last_log_index = index,
          .last_log_term = previous_term_from_index,
          .entries = entries,
          .leader_commit = state_.committed_log_size,
      };

      // request_id not necessary to set because it's not a Future-backed Request.
      const RequestId request_id = 0;

      io_.Send(address, request_id, ar);
    }
  }

  // Raft paper - 5.2
  // Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly
  Duration RandomTimeout(Duration min, Duration max) {
    std::uniform_int_distribution time_distrib(min, max);
    return io_.Rand(time_distrib);
  }

  Term PreviousTermFromIndex(LogIndex index) {
    if (index == 0 || state_.log.size() + 1 <= index) {
      return 0;
    }

    auto &[term, data] = state_.log.at(index - 1);
    return term;
  }

  LogIndex CommittedLogIndex() { return state_.committed_log_size; }

  Term CommittedLogTerm() {
    MG_ASSERT(state_.log.size() >= state_.committed_log_size);
    if (state_.log.empty() || state_.committed_log_size == 0) {
      return 0;
    }

    auto &[term, data] = state_.log.at(state_.committed_log_size - 1);
    return term;
  }

  LogIndex LastLogIndex() { return state_.log.size(); }

  Term LastLogTerm() {
    if (state_.log.empty()) {
      return 0;
    }

    auto &[term, data] = state_.log.back();
    return term;
  }

  template <typename... Ts>
  void Log(Ts &&...args) {
    const Time now = io_.Now();
    const Term term = state_.term;

    std::cout << '\t' << now << "\t" << term << "\t" << io_.GetAddress().last_known_port;

    std::visit([&](auto &&role) { role.Print(); }, role_);

    (std::cout << ... << args) << std::endl;
  }

  /////////////////////////////////////////////////////////////
  /// Raft-related Cron methods
  ///
  /// Cron + std::visit is how events are dispatched
  /// to certain code based on Raft role.
  ///
  /// Cron(role) takes as the first argument a reference to its
  /// role, and as the second argument, the message that has
  /// been received.
  /////////////////////////////////////////////////////////////

  /// Periodic protocol maintenance.
  void Cron() {
    // dispatch periodic logic based on our role to a specific Cron method.
    std::optional<Role> new_role = std::visit([&](auto &&role) { return Cron(role); }, role_);

    if (new_role) {
      role_ = std::move(new_role).value();
    }
  }

  // Raft paper - 5.2
  // Candidates keep sending Vote to peers until:
  // 1. receiving Append with a higher term (become Follower)
  // 2. receiving Vote with a higher term (become a Follower)
  // 3. receiving a quorum of responses to our last batch of Vote (become a Leader)
  std::optional<Role> Cron(Candidate &candidate) {
    const auto now = io_.Now();
    const Duration election_timeout = RandomTimeout(100000, 200000);

    if (now - candidate.election_began > election_timeout) {
      state_.term++;
      Log("becoming Candidate for term ", state_.term, " after leader timeout of ", election_timeout,
          " elapsed since last election attempt");

      const VoteRequest request{
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

  // Raft paper - 5.2
  // Followers become candidates if we haven't heard from the leader
  // after a randomized timeout.
  std::optional<Role> Cron(Follower &follower) {
    const auto now = io_.Now();
    const auto time_since_last_append_entries = now - follower.last_received_append_entries_timestamp;
    Duration election_timeout = RandomTimeout(100000, 200000);

    // randomized follower timeout with a range of 100-150ms.
    if (time_since_last_append_entries > election_timeout) {
      // become a Candidate if we haven't heard from the Leader after this timeout
      return Candidate{};
    }

    return std::nullopt;
  }

  // Leaders (re)send AppendRequest to followers.
  std::optional<Role> Cron(Leader &leader) {
    const Time now = io_.Now();
    const Duration broadcast_timeout = RandomTimeout(40000, 60000);

    if (now - leader.last_broadcast > broadcast_timeout) {
      BroadcastAppendEntries(leader.followers);
      leader.last_broadcast = now;
    }
    // TODO(tyler) TimeOutOldClientRequests();
    return std::nullopt;
  }

  /////////////////////////////////////////////////////////////
  /// Raft-related Handle methods
  ///
  /// Handle + std::visit is how events are dispatched
  /// to certain code based on Raft role.
  ///
  /// Handle(role, message, ...)
  /// takes as the first argument a reference
  /// to its role, and as the second argument, the
  /// message that has been received.
  /////////////////////////////////////////////////////////////

  // Don't we need more stuff in this variant?
  void Handle(std::variant<ReadRequest<ReadOperation>, AppendRequest<WriteOperation>, AppendResponse,
                           WriteRequest<WriteOperation>, VoteRequest, VoteResponse> &&message_variant,
              RequestId request_id, Address from_address) {
    // dispatch the message to a handler based on our role,
    // which can be specified in the Handle first argument,
    // or it can be `auto` if it's a handler for several roles
    // or messages.
    std::optional<Role> new_role =
        std::visit([&](auto &&msg, auto &&role) { return Handle(role, std::move(msg), request_id, from_address); },
                   std::move(message_variant), role_);

    // TODO(tyler) (M3) maybe replace std::visit with get_if for explicit prioritized matching, [[likely]] etc...
    if (new_role) {
      role_ = std::move(new_role).value();
    }
  }

  // all roles can receive Vote and possibly become a follower
  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, VoteRequest &&req, RequestId request_id, Address from_address) {
    Log("received Vote from ", from_address.last_known_port, " with term ", req.term);
    const bool last_log_term_dominates = req.last_log_term >= LastLogTerm();
    const bool term_dominates = req.term > state_.term;
    const bool last_log_index_dominates = req.last_log_index >= LastLogIndex();
    const bool new_leader = last_log_term_dominates && term_dominates && last_log_index_dominates;

    if (new_leader) {
      MG_ASSERT(req.term > state_.term);
      MG_ASSERT(std::max(req.term, state_.term) == req.term);
    }

    const VoteResponse res{
        .term = std::max(req.term, state_.term),
        .committed_log_size = state_.committed_log_size,
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
    } else if (term_dominates) {
      Log("received a vote from an inferior candidate. Becoming Candidate");
      state_.term = std::max(state_.term, req.term) + 1;
      return Candidate{};
    }

    return std::nullopt;
  }

  std::optional<Role> Handle(Candidate &candidate, VoteResponse &&res, RequestId, Address from_address) {
    Log("received VoteResponse");

    if (!res.vote_granted || res.term != state_.term) {
      Log("received unsuccessful VoteResponse from term ", res.term, " when our candidacy term is ", state_.term);
      // we received a delayed VoteResponse from the past, which has to do with an election that is
      // no longer valid. We can simply drop this.
      return std::nullopt;
    }

    MG_ASSERT(candidate.outstanding_votes.contains(from_address),
              "Received unexpected VoteResponse from server not present in Candidate's outstanding_votes!");
    candidate.outstanding_votes.erase(from_address);

    MG_ASSERT(!candidate.successful_votes.contains(from_address),
              "Received unexpected VoteResponse from server already in Candidate's successful_votes!");
    candidate.successful_votes.insert({from_address, res.committed_log_size});

    if (candidate.successful_votes.size() >= candidate.outstanding_votes.size()) {
      std::map<Address, FollowerTracker> followers{};

      for (const auto &[address, committed_log_size] : candidate.successful_votes) {
        FollowerTracker follower{
            .next_index = committed_log_size,
            .confirmed_contiguous_index = committed_log_size,
        };
        followers.insert({address, std::move(follower)});
      }
      for (const auto &address : candidate.outstanding_votes) {
        FollowerTracker follower{
            .next_index = state_.log.size(),
            .confirmed_contiguous_index = 0,
        };
        followers.insert({address, std::move(follower)});
      }

      Log("becoming Leader at term ", state_.term);

      BroadcastAppendEntries(followers);

      return Leader{
          .followers = std::move(followers),
          .pending_client_requests = std::deque<PendingClientRequest>(),
      };
    }

    return std::nullopt;
  }

  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, VoteResponse &&res, RequestId request_id, Address from_address) {
    Log("non-Candidate received VoteResponse");
    return std::nullopt;
  }

  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &role, AppendRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    AppendResponse res{
        .success = false,
        .term = state_.term,
        .last_log_term = CommittedLogTerm(),
        .last_log_index = CommittedLogIndex(),
    };

    if constexpr (std::is_same<AllRoles, Leader>()) {
      MG_ASSERT(req.term != state_.term, "Multiple leaders are acting under the term ", req.term);
    }

    const bool is_candidate = std::is_same<AllRoles, Candidate>();
    const bool is_failed_competitor = is_candidate && req.term == state_.term;
    const Time now = io_.Now();

    // Raft paper - 5.2
    // While waiting for votes, a candidate may receive an
    // AppendEntries RPC from another server claiming to be leader. If
    // the leader’s term (included in its RPC) is at least as large as
    // the candidate’s current term, then the candidate recognizes the
    // leader as legitimate and returns to follower state.
    if (req.term > state_.term || is_failed_competitor) {
      // become follower of this leader, reply with our log status
      state_.term = req.term;

      io_.Send(from_address, request_id, res);

      Log("becoming Follower of Leader ", from_address.last_known_port, " at term ", req.term);
      return Follower{
          .last_received_append_entries_timestamp = now,
          .leader_address = from_address,
      };
    } else if (req.term < state_.term) {
      // nack this request from an old leader
      io_.Send(from_address, request_id, res);

      return std::nullopt;
    }

    // at this point, we're dealing with our own leader
    if constexpr (std::is_same<AllRoles, Follower>()) {
      // small specialization for when we're already a Follower
      MG_ASSERT(role.leader_address == from_address, "Multiple Leaders are acting under the same term number!");
      role.last_received_append_entries_timestamp = now;
    } else {
      Log("Somehow entered Follower-specific logic as a non-Follower");
      MG_ASSERT(false, "Somehow entered Follower-specific logic as a non-Follower");
    }

    res.last_log_term = LastLogTerm();
    res.last_log_index = LastLogIndex();

    Log("returning last_log_index of ", res.last_log_index);

    // Handle steady-state conditions.
    if (req.last_log_index != LastLogIndex()) {
      Log("req.last_log_index is above our last applied log index");
    } else if (req.last_log_term != LastLogTerm()) {
      Log("req.last_log_term differs from our leader term at that slot, expected: ", LastLogTerm(), " but got ",
          req.last_log_term);
    } else {
      // happy path - apply log
      Log("applying batch of entries to log of size ", req.entries.size());

      MG_ASSERT(req.last_log_index >= state_.committed_log_size,
                "Applied history from Leader which goes back in time from our commit_index");

      // possibly chop-off stuff that was replaced by
      // things with different terms (we got data that
      // hasn't reached consensus yet, which is normal)
      state_.log.resize(req.last_log_index);

      state_.log.insert(state_.log.end(), req.entries.begin(), req.entries.end());

      state_.committed_log_size = std::min(req.leader_commit, LastLogIndex());

      res.success = true;
    }

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Leader &leader, AppendResponse &&res, RequestId request_id, Address from_address) {
    if (res.term != state_.term) {
    } else if (!leader.followers.contains(from_address)) {
      Log("received AppendResponse from unknown Follower");
      MG_ASSERT(false, "received AppendResponse from unknown Follower");
    } else {
      if (res.success) {
        Log("got successful AppendResponse from ", from_address.last_known_port, " with last_log_index of ",
            res.last_log_index);
      } else {
        Log("got unsuccessful AppendResponse from ", from_address.last_known_port, " with last_log_index of ",
            res.last_log_index);
      }
      FollowerTracker &follower = leader.followers.at(from_address);
      follower.next_index = std::max(follower.next_index, res.last_log_index);
      follower.confirmed_contiguous_index = std::max(follower.confirmed_contiguous_index, res.last_log_index);

      BumpCommitIndexAndReplyToClients(leader);
    }
    return std::nullopt;
  }

  template <typename AllRoles>
  std::optional<Role> Handle(AllRoles &, AppendResponse &&res, RequestId request_id, Address from_address) {
    // we used to be the leader, and are getting old delayed responses
    return std::nullopt;
  }

  /////////////////////////////////////////////////////////////
  /// RSM-related handle methods
  /////////////////////////////////////////////////////////////

  // Leaders are able to immediately respond to the requester (with a ReadResponseValue) applied to the ReplicatedState
  std::optional<Role> Handle(Leader &leader, ReadRequest<ReadOperation> &&req, RequestId request_id,
                             Address from_address) {
    // TODO(tyler / gabor) implement

    ReadOperation read_operation = req.operation;

    ReadResponseValue read_return = replicated_state_.read(read_operation);

    ReadResponse<ReadResponseValue> resp{
        .success = true,
        .read_return = std::move(read_return),
        .retry_leader = std::nullopt,
    };

    io_.Send(from_address, request_id, resp);

    return std::nullopt;
  }

  // Candidates should respond with a failure, similar to the Candidate + WriteRequest failure below
  std::optional<Role> Handle(Candidate &, ReadRequest<ReadOperation> &&req, RequestId request_id,
                             Address from_address) {
    // TODO(tyler / gabor) implement

    Log("received ReadOperation - not redirecting because no Leader is known");
    auto res = ReadResponse<ReadResponseValue>{};

    res.success = false;

    Cron();

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  // Leaders should respond with a redirection, similar to the Follower + WriteRequest response below
  std::optional<Role> Handle(Follower &follower, ReadRequest<ReadOperation> &&req, RequestId request_id,
                             Address from_address) {
    // TODO(tyler / gabor) implement

    auto res = ReadResponse<ReadResponseValue>{};

    res.success = false;
    Log("redirecting client to known Leader with port ", follower.leader_address.last_known_port);
    res.retry_leader = follower.leader_address;

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  // Raft paper - 8
  // When a client first starts up, it connects to a randomly chosen
  // server. If the client’s first choice is not the leader, that
  // server will reject the client’s request and supply information
  // about the most recent leader it has heard from.
  std::optional<Role> Handle(Follower &follower, WriteRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    auto res = WriteResponse<WriteResponseValue>{};

    res.success = false;
    Log("redirecting client to known Leader with port ", follower.leader_address.last_known_port);
    res.retry_leader = follower.leader_address;

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Candidate &, WriteRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    Log("received WriteRequest - not redirecting because no Leader is known");
    auto res = WriteResponse<WriteResponseValue>{};

    res.success = false;

    Cron();

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  // only leaders actually handle replication requests from clients
  std::optional<Role> Handle(Leader &leader, WriteRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    Log("received WriteRequest");

    // we are the leader. add item to log and send Append to peers
    state_.log.emplace_back(std::pair(state_.term, std::move(req.operation)));

    PendingClientRequest pcr{
        .log_index = state_.log.size() - 1,
        .request_id = request_id,
        .address = from_address,
    };

    leader.pending_client_requests.push_back(pcr);

    BroadcastAppendEntries(leader.followers);

    // TODO(tyler) add message to pending requests buffer, reply asynchronously
    return std::nullopt;
  }
};

};  // namespace memgraph::io::rsm
