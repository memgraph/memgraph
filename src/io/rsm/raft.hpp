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

// TODO(tyler) buffer out-of-order Append buffers on the Followers to reassemble more quickly
// TODO(tyler) handle granular batch sizes based on simple flow control

#pragma once

#include <deque>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "io/simulator/simulator.hpp"
#include "io/transport.hpp"
#include "utils/concepts.hpp"

namespace memgraph::io::rsm {

/// Timeout and replication tunables
using namespace std::chrono_literals;
static constexpr auto kMinimumElectionTimeout = 100ms;
static constexpr auto kMaximumElectionTimeout = 200ms;
static constexpr auto kMinimumBroadcastTimeout = 40ms;
static constexpr auto kMaximumBroadcastTimeout = 60ms;
static constexpr auto kMinimumCronInterval = 1ms;
static constexpr auto kMaximumCronInterval = 2ms;
static constexpr auto kMinimumReceiveTimeout = 40ms;
static constexpr auto kMaximumReceiveTimeout = 60ms;
static_assert(kMinimumElectionTimeout > kMaximumBroadcastTimeout,
              "The broadcast timeout has to be smaller than the election timeout!");
static_assert(kMinimumElectionTimeout < kMaximumElectionTimeout,
              "The minimum election timeout has to be smaller than the maximum election timeout!");
static_assert(kMinimumBroadcastTimeout < kMaximumBroadcastTimeout,
              "The minimum broadcast timeout has to be smaller than the maximum broadcast timeout!");
static_assert(kMinimumCronInterval < kMaximumCronInterval,
              "The minimum cron interval has to be smaller than the maximum cron interval!");
static_assert(kMinimumReceiveTimeout < kMaximumReceiveTimeout,
              "The minimum receive timeout has to be smaller than the maximum receive timeout!");
static constexpr size_t kMaximumAppendBatchSize = 1024;

using Term = uint64_t;
using LogIndex = uint64_t;
using LogSize = uint64_t;
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
  LogIndex raft_index;
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

/// AppendRequest is a raft-level message that the Leader
/// periodically broadcasts to all Follower peers. This
/// serves three main roles:
/// 1. acts as a heartbeat from the Leader to the Follower
/// 2. replicates new data that the Leader has received to the Follower
/// 3. informs Follower peers when the commit index has increased,
///    signalling that it is now safe to apply log items to the
///    replicated state machine
template <typename WriteRequest>
struct AppendRequest {
  Term term = 0;
  LogIndex batch_start_log_index;
  Term last_log_term;
  std::vector<std::pair<Term, WriteRequest>> entries;
  LogSize leader_commit;
};

struct AppendResponse {
  bool success;
  Term term;
  Term last_log_term;
  // a small optimization over the raft paper, tells
  // the leader the offset that we are interested in
  // to send log offsets from for us. This will only
  // be useful at the beginning of a leader's term.
  LogSize log_size;
};

struct VoteRequest {
  Term term = 0;
  LogSize log_size;
  Term last_log_term;
};

struct VoteResponse {
  Term term = 0;
  LogSize committed_log_size;
  bool vote_granted = false;
};

template <typename WriteRequest>
struct CommonState {
  Term term = 0;
  std::vector<std::pair<Term, WriteRequest>> log;
  LogSize committed_log_size = 0;
  LogSize applied_size = 0;
};

struct FollowerTracker {
  LogIndex next_index = 0;
  LogSize confirmed_log_size = 0;
};

struct PendingClientRequest {
  RequestId request_id;
  Address address;
  Time received_at;
};

struct Leader {
  std::map<Address, FollowerTracker> followers;
  std::unordered_map<LogIndex, PendingClientRequest> pending_client_requests;
  Time last_broadcast = Time::min();

  std::string static ToString() { return "\tLeader   \t"; }
};

struct Candidate {
  std::map<Address, LogSize> successful_votes;
  Time election_began = Time::min();
  std::set<Address> outstanding_votes;

  std::string static ToString() { return "\tCandidate\t"; }
};

struct Follower {
  Time last_received_append_entries_timestamp;
  Address leader_address;

  std::string static ToString() { return "\tFollower \t"; }
};

using Role = std::variant<Candidate, Leader, Follower>;

template <typename Role>
concept AllRoles = memgraph::utils::SameAsAnyOf<Role, Leader, Follower, Candidate>;

template <typename Role>
concept LeaderOrFollower = memgraph::utils::SameAsAnyOf<Role, Leader, Follower>;

/*
all ReplicatedState classes should have an Apply method
that returns our WriteResponseValue:

ReadResponse Read(ReadOperation);
WriteResponseValue ReplicatedState::Apply(WriteRequest);

for examples:
if the state is uint64_t, and WriteRequest is `struct PlusOne {};`,
and WriteResponseValue is also uint64_t (the new value), then
each call to state.Apply(PlusOne{}) will return the new value
after incrementing it. 0, 1, 2, 3... and this will be sent back
to the client that requested the mutation.

In practice, these mutations will usually be predicated on some
previous value, so that they are idempotent, functioning similarly
to a CAS operation.
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
/// WriteResponseValue  the return value of calling ReplicatedState::Apply(WriteOperation), which is executed in
///                     identical order across all replicas after an WriteOperation reaches consensus.
/// ReadOperation       the type of operations that do not require consensus before executing directly
///                     on a const ReplicatedState &
/// ReadResponseValue   the return value of calling ReplicatedState::Read(ReadOperation), which is executed directly
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
      : io_(std::forward<Io<IoImpl>>(io)),
        peers_(peers),
        replicated_state_(std::forward<ReplicatedState>(replicated_state)) {}

  void Run() {
    Time last_cron = io_.Now();

    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();
      const Duration random_cron_interval = RandomTimeout(kMinimumCronInterval, kMaximumCronInterval);
      if (now - last_cron > random_cron_interval) {
        Cron();
        last_cron = now;
      }

      const Duration receive_timeout = RandomTimeout(kMinimumReceiveTimeout, kMaximumReceiveTimeout);

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
  //
  // "Safely replicated" is defined as being known to be present
  // on at least a majority of all peers (inclusive of the Leader).
  void BumpCommitIndexAndReplyToClients(Leader &leader) {
    auto confirmed_log_sizes = std::vector<LogSize>{};

    // We include our own log size in the calculation of the log
    // confirmed log size that is present on at least a majority of all peers.
    confirmed_log_sizes.push_back(state_.log.size());

    for (const auto &[addr, f] : leader.followers) {
      confirmed_log_sizes.push_back(f.confirmed_log_size);
      Log("Follower at port ", addr.last_known_port, " has confirmed log size of: ", f.confirmed_log_size);
    }

    // reverse sort from highest to lowest (using std::ranges::greater)
    std::ranges::sort(confirmed_log_sizes, std::ranges::greater());

    // This is a particularly correctness-critical calculation because it
    // determines the committed log size that will be broadcast in
    // the next AppendRequest.
    //
    // If the following sizes are recorded for clusters of different numbers of peers,
    // these are the expected sizes that are considered to have reached consensus:
    //
    // state           | expected value | (confirmed_log_sizes.size() / 2)
    // [1]              1                 (1 / 2) => 0
    // [2, 1]           1                 (2 / 2) => 1
    // [3, 2, 1]        2                 (3 / 2) => 1
    // [4, 3, 2, 1]     2                 (4 / 2) => 2
    // [5, 4, 3, 2, 1]  3                 (5 / 2) => 2
    const size_t majority_index = confirmed_log_sizes.size() / 2;
    const LogSize new_committed_log_size = confirmed_log_sizes[majority_index];

    // We never go backwards in history.
    MG_ASSERT(state_.committed_log_size <= new_committed_log_size,
              "as a Leader, we have previously set our committed_log_size to {}, but our Followers have a majority "
              "committed_log_size of {}",
              state_.committed_log_size, new_committed_log_size);

    state_.committed_log_size = new_committed_log_size;

    // For each size between the old size and the new one (inclusive),
    // Apply that log's WriteOperation to our replicated_state_,
    // and use the specific return value of the ReplicatedState::Apply
    // method (WriteResponseValue) to respond to the requester.
    for (; state_.applied_size < state_.committed_log_size; state_.applied_size++) {
      const LogIndex apply_index = state_.applied_size;
      const auto &write_request = state_.log[apply_index].second;
      const WriteResponseValue write_return = replicated_state_.Apply(write_request);

      if (leader.pending_client_requests.contains(apply_index)) {
        const PendingClientRequest client_request = std::move(leader.pending_client_requests.at(apply_index));
        leader.pending_client_requests.erase(apply_index);

        const WriteResponse<WriteResponseValue> resp{
            .success = true,
            .write_return = std::move(write_return),
            .raft_index = apply_index,
        };

        io_.Send(client_request.address, client_request.request_id, std::move(resp));
      }
    }

    Log("committed_log_size is now ", state_.committed_log_size);
  }

  // Raft paper - 5.1
  // AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat
  void BroadcastAppendEntries(std::map<Address, FollowerTracker> &followers) {
    for (auto &[address, follower] : followers) {
      const LogSize follower_log_size = follower.confirmed_log_size;

      const auto missing = state_.log.size() - follower_log_size;
      const auto batch_size = std::min(missing, kMaximumAppendBatchSize);
      const auto start_index = follower_log_size;
      const auto end_index = start_index + batch_size;

      std::vector<std::pair<Term, WriteOperation>> entries;

      entries.insert(entries.begin(), state_.log.begin() + start_index, state_.log.begin() + end_index);

      const Term previous_term_from_index = PreviousTermFromIndex(start_index);

      Log("sending ", entries.size(), " entries to Follower ", address.last_known_port,
          " which are above its known confirmed log size of ", follower_log_size);

      AppendRequest<WriteOperation> ar{
          .term = state_.term,
          .batch_start_log_index = start_index,
          .last_log_term = previous_term_from_index,
          .entries = std::move(entries),
          .leader_commit = state_.committed_log_size,
      };

      // request_id not necessary to set because it's not a Future-backed Request.
      static constexpr RequestId request_id = 0;

      io_.Send(address, request_id, std::move(ar));
    }
  }

  // Raft paper - 5.2
  // Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly
  Duration RandomTimeout(Duration min, Duration max) {
    std::uniform_int_distribution time_distrib(min.count(), max.count());

    const auto rand_micros = io_.Rand(time_distrib);

    return Duration{rand_micros};
  }

  Duration RandomTimeout(int min_micros, int max_micros) {
    std::uniform_int_distribution time_distrib(min_micros, max_micros);

    const int rand_micros = io_.Rand(time_distrib);

    return std::chrono::microseconds{rand_micros};
  }

  Term PreviousTermFromIndex(LogIndex index) const {
    if (index == 0 || state_.log.size() + 1 <= index) {
      return 0;
    }

    const auto &[term, data] = state_.log.at(index - 1);
    return term;
  }

  Term CommittedLogTerm() {
    MG_ASSERT(state_.log.size() >= state_.committed_log_size);
    if (state_.log.empty() || state_.committed_log_size == 0) {
      return 0;
    }

    const auto &[term, data] = state_.log.at(state_.committed_log_size - 1);
    return term;
  }

  Term LastLogTerm() const {
    if (state_.log.empty()) {
      return 0;
    }

    const auto &[term, data] = state_.log.back();
    return term;
  }

  template <typename... Ts>
  void Log(Ts &&...args) {
    const Time now = io_.Now();
    const auto micros = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const Term term = state_.term;
    const std::string role_string = std::visit([&](const auto &role) { return role.ToString(); }, role_);

    std::ostringstream out;

    out << '\t' << (int)micros << "\t" << term << "\t" << io_.GetAddress().last_known_port;

    out << role_string;

    (out << ... << args);

    spdlog::info(out.str());
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
    std::optional<Role> new_role = std::visit([&](auto &role) { return Cron(role); }, role_);

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
    const Duration election_timeout = RandomTimeout(kMinimumElectionTimeout, kMaximumElectionTimeout);
    const auto election_timeout_us = std::chrono::duration_cast<std::chrono::milliseconds>(election_timeout).count();

    if (now - candidate.election_began > election_timeout) {
      state_.term++;
      Log("becoming Candidate for term ", state_.term, " after leader timeout of ", election_timeout_us,
          "ms elapsed since last election attempt");

      const VoteRequest request{
          .term = state_.term,
          .log_size = state_.log.size(),
          .last_log_term = LastLogTerm(),
      };

      auto outstanding_votes = std::set<Address>();

      for (const auto &peer : peers_) {
        // request_id not necessary to set because it's not a Future-backed Request.
        static constexpr auto request_id = 0;
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

    // randomized follower timeout
    const Duration election_timeout = RandomTimeout(kMinimumElectionTimeout, kMaximumElectionTimeout);

    if (time_since_last_append_entries > election_timeout) {
      // become a Candidate if we haven't heard from the Leader after this timeout
      return Candidate{};
    }

    return std::nullopt;
  }

  // Leaders (re)send AppendRequest to followers.
  std::optional<Role> Cron(Leader &leader) {
    const Time now = io_.Now();
    const Duration broadcast_timeout = RandomTimeout(kMinimumBroadcastTimeout, kMaximumBroadcastTimeout);

    if (now - leader.last_broadcast > broadcast_timeout) {
      BroadcastAppendEntries(leader.followers);
      leader.last_broadcast = now;
    }

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

  using ReceiveVariant = std::variant<ReadRequest<ReadOperation>, AppendRequest<WriteOperation>, AppendResponse,
                                      WriteRequest<WriteOperation>, VoteRequest, VoteResponse>;

  void Handle(ReceiveVariant &&message_variant, RequestId request_id, Address from_address) {
    // dispatch the message to a handler based on our role,
    // which can be specified in the Handle first argument,
    // or it can be `auto` if it's a handler for several roles
    // or messages.
    std::optional<Role> new_role = std::visit(
        [&](auto &&msg, auto &role) {
          // We use decltype(msg)(msg) in place of std::forward<?> because msg's type
          // is anonymous from our point of view.
          return Handle(role, decltype(msg)(msg), request_id, from_address);
        },
        std::forward<ReceiveVariant>(message_variant), role_);

    // TODO(tyler) (M3) maybe replace std::visit with get_if for explicit prioritized matching, [[likely]] etc...
    if (new_role) {
      role_ = std::move(new_role).value();
    }
  }

  // all roles can receive Vote and possibly become a follower
  template <AllRoles ALL>
  std::optional<Role> Handle(ALL &, VoteRequest &&req, RequestId request_id, Address from_address) {
    Log("received VoteRequest from ", from_address.last_known_port, " with term ", req.term);
    const bool last_log_term_dominates = req.last_log_term >= LastLogTerm();
    const bool term_dominates = req.term > state_.term;
    const bool log_size_dominates = req.log_size >= state_.log.size();
    const bool new_leader = last_log_term_dominates && term_dominates && log_size_dominates;

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
    }

    if (term_dominates) {
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
            .confirmed_log_size = committed_log_size,
        };
        followers.insert({address, follower});
      }
      for (const auto &address : candidate.outstanding_votes) {
        FollowerTracker follower{
            .next_index = state_.log.size(),
            .confirmed_log_size = 0,
        };
        followers.insert({address, follower});
      }

      Log("becoming Leader at term ", state_.term);

      BroadcastAppendEntries(followers);

      return Leader{
          .followers = std::move(followers),
          .pending_client_requests = std::unordered_map<LogIndex, PendingClientRequest>(),
      };
    }

    return std::nullopt;
  }

  template <LeaderOrFollower LOF>
  std::optional<Role> Handle(LOF &, VoteResponse &&, RequestId, Address) {
    Log("non-Candidate received VoteResponse");
    return std::nullopt;
  }

  template <AllRoles ALL>
  std::optional<Role> Handle(ALL &role, AppendRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    // log size starts out as state_.committed_log_size and only if everything is successful do we
    // switch it to the log length.
    AppendResponse res{
        .success = false,
        .term = state_.term,
        .last_log_term = CommittedLogTerm(),
        .log_size = state_.log.size(),
    };

    if constexpr (std::is_same<ALL, Leader>()) {
      MG_ASSERT(req.term != state_.term, "Multiple leaders are acting under the term ", req.term);
    }

    const bool is_candidate = std::is_same<ALL, Candidate>();
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
    if constexpr (std::is_same<ALL, Follower>()) {
      // small specialization for when we're already a Follower
      MG_ASSERT(role.leader_address == from_address, "Multiple Leaders are acting under the same term number!");
      role.last_received_append_entries_timestamp = now;
    } else {
      Log("Somehow entered Follower-specific logic as a non-Follower");
      MG_ASSERT(false, "Somehow entered Follower-specific logic as a non-Follower");
    }

    // Handle steady-state conditions.
    if (req.batch_start_log_index != state_.log.size()) {
      Log("req.batch_start_log_index of ", req.batch_start_log_index, " does not match our log size of ",
          state_.log.size());
    } else if (req.last_log_term != LastLogTerm()) {
      Log("req.last_log_term differs from our leader term at that slot, expected: ", LastLogTerm(), " but got ",
          req.last_log_term);
    } else {
      // happy path - Apply log
      Log("applying batch of ", req.entries.size(), " entries to our log starting at index ",
          req.batch_start_log_index);

      const auto resize_length = req.batch_start_log_index;

      MG_ASSERT(resize_length >= state_.committed_log_size,
                "Applied history from Leader which goes back in time from our commit_index");

      // possibly chop-off stuff that was replaced by
      // things with different terms (we got data that
      // hasn't reached consensus yet, which is normal)
      state_.log.resize(resize_length);

      if (req.entries.size() > 0) {
        auto &[first_term, op] = req.entries.at(0);
        MG_ASSERT(LastLogTerm() <= first_term);
      }

      state_.log.insert(state_.log.end(), std::make_move_iterator(req.entries.begin()),
                        std::make_move_iterator(req.entries.end()));

      MG_ASSERT(req.leader_commit >= state_.committed_log_size);
      state_.committed_log_size = std::min(req.leader_commit, state_.log.size());

      for (; state_.applied_size < state_.committed_log_size; state_.applied_size++) {
        const auto &write_request = state_.log[state_.applied_size].second;
        replicated_state_.Apply(write_request);
      }

      res.success = true;
    }

    res.last_log_term = LastLogTerm();
    res.log_size = state_.log.size();

    Log("returning log_size of ", res.log_size);

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Leader &leader, AppendResponse &&res, RequestId, Address from_address) {
    if (res.term != state_.term) {
      Log("received AppendResponse related to a previous term when we (presumably) were the leader");
      return std::nullopt;
    }

    // TODO(tyler) when we have dynamic membership, this assert will become incorrect, but we should
    // keep it in-place until then because it has bug finding value.
    MG_ASSERT(leader.followers.contains(from_address), "received AppendResponse from unknown Follower");

    // at this point, we know the term matches and we know this Follower

    FollowerTracker &follower = leader.followers.at(from_address);

    if (res.success) {
      Log("got successful AppendResponse from ", from_address.last_known_port, " with log_size of ", res.log_size);
      follower.next_index = std::max(follower.next_index, res.log_size);
    } else {
      Log("got unsuccessful AppendResponse from ", from_address.last_known_port, " with log_size of ", res.log_size);
      follower.next_index = res.log_size;
    }

    follower.confirmed_log_size = std::max(follower.confirmed_log_size, res.log_size);

    BumpCommitIndexAndReplyToClients(leader);

    return std::nullopt;
  }

  template <AllRoles ALL>
  std::optional<Role> Handle(ALL &, AppendResponse &&, RequestId, Address) {
    // we used to be the leader, and are getting old delayed responses
    return std::nullopt;
  }

  /////////////////////////////////////////////////////////////
  /// RSM-related handle methods
  /////////////////////////////////////////////////////////////

  // Leaders are able to immediately respond to the requester (with a ReadResponseValue) applied to the ReplicatedState
  std::optional<Role> Handle(Leader &, ReadRequest<ReadOperation> &&req, RequestId request_id, Address from_address) {
    Log("handling ReadOperation");
    ReadOperation read_operation = req.operation;

    ReadResponseValue read_return = replicated_state_.Read(read_operation);

    const ReadResponse<ReadResponseValue> resp{
        .success = true,
        .read_return = std::move(read_return),
        .retry_leader = std::nullopt,
    };

    io_.Send(from_address, request_id, resp);

    return std::nullopt;
  }

  // Candidates should respond with a failure, similar to the Candidate + WriteRequest failure below
  std::optional<Role> Handle(Candidate &, ReadRequest<ReadOperation> &&, RequestId request_id, Address from_address) {
    Log("received ReadOperation - not redirecting because no Leader is known");
    const ReadResponse<ReadResponseValue> res{
        .success = false,
    };

    io_.Send(from_address, request_id, res);

    Cron();

    return std::nullopt;
  }

  // Followers should respond with a redirection, similar to the Follower + WriteRequest response below
  std::optional<Role> Handle(Follower &follower, ReadRequest<ReadOperation> &&, RequestId request_id,
                             Address from_address) {
    Log("redirecting client to known Leader with port ", follower.leader_address.last_known_port);

    const ReadResponse<ReadResponseValue> res{
        .success = false,
        .retry_leader = follower.leader_address,
    };

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  // Raft paper - 8
  // When a client first starts up, it connects to a randomly chosen
  // server. If the client’s first choice is not the leader, that
  // server will reject the client’s request and supply information
  // about the most recent leader it has heard from.
  std::optional<Role> Handle(Follower &follower, WriteRequest<WriteOperation> &&, RequestId request_id,
                             Address from_address) {
    Log("redirecting client to known Leader with port ", follower.leader_address.last_known_port);

    const WriteResponse<WriteResponseValue> res{
        .success = false,
        .retry_leader = follower.leader_address,
    };

    io_.Send(from_address, request_id, res);

    return std::nullopt;
  }

  std::optional<Role> Handle(Candidate &, WriteRequest<WriteOperation> &&, RequestId request_id, Address from_address) {
    Log("received WriteRequest - not redirecting because no Leader is known");

    const WriteResponse<WriteResponseValue> res{
        .success = false,
    };

    io_.Send(from_address, request_id, res);

    Cron();

    return std::nullopt;
  }

  // only leaders actually handle replication requests from clients
  std::optional<Role> Handle(Leader &leader, WriteRequest<WriteOperation> &&req, RequestId request_id,
                             Address from_address) {
    Log("handling WriteRequest");

    // we are the leader. add item to log and send Append to peers
    MG_ASSERT(state_.term >= LastLogTerm());
    state_.log.emplace_back(std::pair(state_.term, std::move(req.operation)));

    LogIndex log_index = state_.log.size() - 1;

    PendingClientRequest pcr{
        .request_id = request_id,
        .address = from_address,
        .received_at = io_.Now(),
    };

    leader.pending_client_requests.emplace(log_index, pcr);

    BroadcastAppendEntries(leader.followers);

    return std::nullopt;
  }
};

};  // namespace memgraph::io::rsm
