#pragma once

#include "fmt/format.h"
#include "glog/logging.h"

namespace communication::raft {

namespace impl {

template <class State>
RaftMemberImpl<State>::RaftMemberImpl(RaftNetworkInterface<State> &network,
                                      RaftStorageInterface<State> &storage,
                                      const MemberId &id,
                                      const RaftConfig &config)
    : network_(network), storage_(storage), id_(id), config_(config) {
  std::lock_guard<std::mutex> lock(mutex_);

  tie(term_, voted_for_) = storage_.GetTermAndVotedFor();

  for (const auto &peer_id : config_.members) {
    peer_states_[peer_id] = std::make_unique<RaftPeerState>();
  }

  SetElectionTimer();
}

template <class State>
RaftMemberImpl<State>::~RaftMemberImpl() {
  Stop();
}

template <class State>
void RaftMemberImpl<State>::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!exiting_) {
      LogInfo("Stopping...");
      exiting_ = true;
    }
  }
  state_changed_.notify_all();
}

template <class State>
template <class... Args>
void RaftMemberImpl<State>::LogInfo(const std::string &format,
                                    Args &&... args) {
  LOG(INFO) << fmt::format("[id = {}, term = {}] {}", id_, term_,
                           fmt::format(format, std::forward<Args>(args)...))
            << std::endl;
}

template <class State>
void RaftMemberImpl<State>::TimerThreadMain() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!exiting_) {
    if (Clock::now() >= next_election_time_) {
      StartNewElection();
    }
    state_changed_.wait_until(lock, next_election_time_);
  }
}

template <class State>
void RaftMemberImpl<State>::PeerThreadMain(std::string peer_id) {
  RaftPeerState &peer_state = *peer_states_[peer_id];

  LogInfo("Peer thread started for {}", peer_id);

  std::unique_lock<std::mutex> lock(mutex_);

  /* This loop will either call a function that issues an RPC or wait on the
   * condition variable. It must not do both! Lock on `mutex_` is released while
   * waiting for RPC response, which might cause us to miss a notification on
   * `state_changed_` conditional variable and wait indefinitely. The safest
   * thing to do is to assume some important part of state was modified while we
   * were waiting for the response and loop around to check. */
  while (!exiting_) {
    TimePoint wait_until = TimePoint::max();

    switch (mode_) {
      case RaftMode::FOLLOWER:
        break;
      case RaftMode::CANDIDATE:
        if (!peer_state.request_vote_done) {
          RequestVote(peer_id, peer_state, lock);
          continue;
        }
        break;
      case RaftMode::LEADER:
        if (peer_state.next_index <= storage_.GetLastLogIndex() ||
            Clock::now() >= peer_state.next_heartbeat_time) {
          AppendEntries(peer_id, peer_state, lock);
          continue;
        } else {
          wait_until = peer_state.next_heartbeat_time;
        }
        break;
    }

    state_changed_.wait_until(lock, wait_until);
  }

  LogInfo("Peer thread exiting for {}", peer_id);
}

template <class State>
void RaftMemberImpl<State>::CandidateOrLeaderTransitionToFollower() {
  DCHECK(mode_ != RaftMode::FOLLOWER)
      << "`CandidateOrLeaderTransitionToFollower` called from follower mode";
  mode_ = RaftMode::FOLLOWER;
  leader_ = {};
  SetElectionTimer();
}

template <class State>
void RaftMemberImpl<State>::CandidateTransitionToLeader() {
  DCHECK(mode_ == RaftMode::CANDIDATE)
      << "`CandidateTransitionToLeader` called while not in candidate mode";
  mode_ = RaftMode::LEADER;
  leader_ = id_;

  /* We don't want to trigger elections while in leader mode. */
  next_election_time_ = TimePoint::max();

  /* [Raft thesis, Section 6.4]
   * "The Leader Completeness Property guarantees that a leader has all
   * committed entries, but at the start of its term, it may not know which
   * those are. To find out, it needs to commit an entry from its term. Raft
   * handles this by having each leader commit a blank no-op entry into the log
   * at the start of its term. As soon as this no-op entry is committed, the
   * leader’s commit index will be at least as large as any other servers’
   * during its term." */

  LogEntry<State> entry;
  entry.term = term_;
  entry.command = std::experimental::nullopt;
  storage_.AppendLogEntry(entry);
}

template <class State>
void RaftMemberImpl<State>::UpdateTermAndVotedFor(
    const TermId new_term,
    const std::experimental::optional<MemberId> &new_voted_for) {
  term_ = new_term;
  voted_for_ = new_voted_for;
  leader_ = {};

  storage_.WriteTermAndVotedFor(term_, voted_for_);
}

template <class State>
void RaftMemberImpl<State>::SetElectionTimer() {
  /* [Raft thesis, section 3.4]
   * "Raft uses randomized election timeouts to ensure that split votes are rare
   * and that they are resolved quickly. To prevent split votes in the first
   * place, election timeouts are chosen randomly from a fixed interval (e.g.,
   * 150–300 ms)." */
  std::uniform_int_distribution<uint64_t> distribution(
      config_.leader_timeout_min.count(), config_.leader_timeout_max.count());
  Clock::duration wait_interval = std::chrono::milliseconds(distribution(rng_));
  next_election_time_ = Clock::now() + wait_interval;
}

template <class State>
bool RaftMemberImpl<State>::SendRPC(const std::string &recipient,
                                    const PeerRPCRequest<State> &request,
                                    PeerRPCReply &reply,
                                    std::unique_lock<std::mutex> &lock) {
  DCHECK(mode_ != RaftMode::FOLLOWER) << "Follower should not send RPCs";

  bool was_candidate = mode_ == RaftMode::CANDIDATE;

  /* Release lock before issuing RPC and waiting for response. */
  /* TODO(mtomic): Revise how this will work with RPC cancellation. */
  lock.unlock();
  bool ok = network_.SendRPC(recipient, request, reply);
  lock.lock();

  /* TODO(mtomic): RPC retrying */
  if (!ok) {
    return false;
  }

  /* We released the lock while waiting for RPC response. It is possible that
   * the internal state has changed while we we're waiting and we don't care for
   * RPC reply anymore for any of these reasons:
   * (a) we are not the leader anymore
   * (b) election timeout
   * (c) we are elected as leader
   * (d) out election was interrupted by another leader
   * (e) destructor was called
   */
  if (term_ != request.Term() ||
      (was_candidate && mode_ != RaftMode::CANDIDATE) || exiting_) {
    LogInfo("Ignoring RPC reply from {}", recipient);
    return false;
  }

  DCHECK(reply.Term() >= term_)
      << "Response term should be >= request term";

  /* [Raft thesis, Section 3.3]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (reply.Term() > term_) {
    UpdateTermAndVotedFor(reply.Term(), {});
    CandidateOrLeaderTransitionToFollower();
    state_changed_.notify_all();
    return false;
  }

  return true;
}

template <class State>
void RaftMemberImpl<State>::StartNewElection() {
  LogInfo("Starting new election");
  /* [Raft thesis, section 3.4]
   * "To begin an election, a follower increments its current term and
   * transitions to candidate state.  It then votes for itself and issues
   * RequestVote RPCs in parallel to each of the other servers in the cluster."
   */
  UpdateTermAndVotedFor(term_ + 1, id_);
  mode_ = RaftMode::CANDIDATE;

  /* [Raft thesis, section 3.4]
   * "Each candidate restarts its randomized election timeout at the start of an
   * election, and it waits for that timeout to elapse before starting the next
   * election; this reduces the likelihood of another split vote in the new
   * election." */
  SetElectionTimer();

  for (const auto &peer_id : config_.members) {
    if (peer_id == id_) {
      continue;
    }
    auto &peer_state = peer_states_[peer_id];
    peer_state->request_vote_done = false;
    peer_state->voted_for_me = false;
    peer_state->match_index = 0;
    peer_state->next_index = storage_.GetLastLogIndex() + 1;

    /* [Raft thesis, section 3.5]
     * "Until the leader has discovered where it and the follower's logs match,
     * the leader can send AppendEntries with no entries (like heartbeats) to
     * save bandwidth. Then, once the matchIndex immediately precedes the
     * nextIndex, the leader should begin to send the actual entries." */
    peer_state->suppress_log_entries = true;

    /* [Raft thesis, section 3.4]
     * "Once a candidate wins an election, it becomes leader. It then sends
     * heartbeat messages to all of the other servers to establish its authority
     * and prevent new elections."
     *
     * This will make newly elected leader send heartbeats immediately.
     */
    peer_state->next_heartbeat_time = TimePoint::min();
  }

  /* Notify peer threads to start issuing RequestVote RPCs. */
  state_changed_.notify_all();
}

template <class State>
bool RaftMemberImpl<State>::CountVotes() {
  DCHECK(mode_ == RaftMode::CANDIDATE)
      << "`CountVotes` should only be called from candidate mode";
  int num_votes = 0;
  for (const auto &peer_id : config_.members) {
    if (peer_id == id_ || peer_states_[peer_id]->voted_for_me) {
      num_votes++;
    }
  }

  return 2 * num_votes > config_.members.size();
}

template <class State>
void RaftMemberImpl<State>::RequestVote(const std::string &peer_id,
                                        RaftPeerState &peer_state,
                                        std::unique_lock<std::mutex> &lock) {
  LogInfo("Requesting vote from {}", peer_id);

  PeerRPCRequest<State> request;
  request.type = PeerRPCRequest<State>::Type::REQUEST_VOTE;
  request.request_vote.candidate_term = term_;
  request.request_vote.candidate_id = id_;

  PeerRPCReply reply;

  /* Release lock before issuing RPC and waiting for response. */
  if (!SendRPC(peer_id, request, reply, lock)) {
    return;
  }

  DCHECK(reply.request_vote.term >= term_) << "Stale RequestVote RPC reply";

  peer_state.request_vote_done = true;

  if (reply.request_vote.vote_granted) {
    peer_state.voted_for_me = true;
    LogInfo("Got vote from {}", peer_id);

    if (CountVotes()) {
      LogInfo("Elected as leader.");
      CandidateTransitionToLeader();
    }
  } else {
    LogInfo("Vote denied from {}", peer_id);
  }

  state_changed_.notify_all();
}

template <class State>
void RaftMemberImpl<State>::AdvanceCommitIndex() {
  DCHECK(mode_ == RaftMode::LEADER)
      << "`AdvanceCommitIndex` can only be called from leader mode";

  std::vector<LogIndex> match_indices;
  for (const auto &peer : peer_states_) {
    match_indices.push_back(peer.second->match_index);
  }
  match_indices.push_back(storage_.GetLastLogIndex());
  sort(match_indices.begin(), match_indices.end(), std::greater<LogIndex>());
  LogIndex new_commit_index_ = match_indices[(config_.members.size() - 1) / 2];

  LogInfo("Trying to advance commit index {} to {}", commit_index_,
          new_commit_index_);

  /* This can happen because we reset `match_index` to 0 for every peer when
   * elected. */
  if (commit_index_ >= new_commit_index_) {
    return;
  }

  /* [Raft thesis, section 3.6.2]
   * (...) Raft never commits log entries from previous terms by counting
   * replicas. Only log entries from the leader's current term are committed by
   * counting replicas; once an entry from the current term has been committed
   * in this way, then all prior entries are committed indirectly because of the
   * Log Matching Property." */
  if (storage_.GetLogTerm(new_commit_index_) != term_) {
    LogInfo("Cannot commit log entry from previous term");
    return;
  }

  commit_index_ = std::max(commit_index_, new_commit_index_);
}

template <class State>
void RaftMemberImpl<State>::AppendEntries(const std::string &peer_id,
                                          RaftPeerState &peer_state,
                                          std::unique_lock<std::mutex> &lock) {
  LogInfo("Appending entries to {}", peer_id);

  PeerRPCRequest<State> request;
  request.type = PeerRPCRequest<State>::Type::APPEND_ENTRIES;
  request.append_entries.leader_term = term_;
  request.append_entries.leader_id = id_;

  request.append_entries.prev_log_index = peer_state.next_index - 1;
  request.append_entries.prev_log_term =
      storage_.GetLogTerm(peer_state.next_index - 1);

  if (!peer_state.suppress_log_entries &&
      peer_state.next_index <= storage_.GetLastLogIndex()) {
    request.append_entries.entries =
        storage_.GetLogSuffix(peer_state.next_index);
  } else {
    request.append_entries.entries = {};
  }

  request.append_entries.leader_commit = commit_index_;

  PeerRPCReply reply;

  if (!SendRPC(peer_id, request, reply, lock)) {
    /* There is probably something wrong with this peer, let's avoid sending log
     * entries. */
    peer_state.suppress_log_entries = true;
    return;
  }

  DCHECK(mode_ == RaftMode::LEADER)
      << "Elected leader for term should never change";
  DCHECK(reply.append_entries.term == term_) << "Got stale AppendEntries reply";

  if (reply.append_entries.success) {
    /* We've found a match, we can start sending log entries. */
    peer_state.suppress_log_entries = false;

    LogIndex new_match_index = request.append_entries.prev_log_index +
                               request.append_entries.entries.size();
    DCHECK(peer_state.match_index <= new_match_index)
        << "`match_index` should increase monotonically within a term";
    peer_state.match_index = new_match_index;
    AdvanceCommitIndex();
    peer_state.next_index = peer_state.match_index + 1;
    peer_state.next_heartbeat_time = Clock::now() + config_.heartbeat_interval;
  } else {
    DCHECK(peer_state.next_index > 1)
        << "Log replication should not fail for first log entry.";
    --peer_state.next_index;
  }

  state_changed_.notify_all();
}

template <class State>
PeerRPCReply::RequestVote RaftMemberImpl<State>::OnRequestVote(
    const typename PeerRPCRequest<State>::RequestVote &request) {
  std::lock_guard<std::mutex> lock(mutex_);
  LogInfo("RequestVote RPC request from {}", request.candidate_id);

  PeerRPCReply::RequestVote reply;

  /* [Raft thesis, Section 3.3]
   * "If a server receives a request with a stale term number, it rejects the
   * request." */
  if (request.candidate_term < term_) {
    reply.term = term_;
    reply.vote_granted = false;
    return reply;
  }

  /* [Raft thesis, Section 3.3]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (request.candidate_term > term_) {
    if (mode_ != RaftMode::FOLLOWER) {
      CandidateOrLeaderTransitionToFollower();
    }
    UpdateTermAndVotedFor(request.candidate_term, {});
  }

  /* [Raft thesis, Section 3.6.1]
   * "Raft uses the voting process to prevent a candidate from winning an
   * election unless its log contains all committed entries. (...) The
   * RequestVote RPC implements this restriction: the RPC includes information
   * about the candidate's log, and the voter denies its vote if its own log is
   * more up-to-date than that of the candidate. Raft determines which of two
   * logs is more up-to-date by comparing the index and term of the last entries
   * in the logs. If the logs have last entries with different terms, then the
   * log with the later term is more up-to-date. If the logs end with the same
   * term, then whichever log is longer is more up-to-date." */
  LogIndex my_last_log_index = storage_.GetLastLogIndex();
  TermId my_last_log_term = storage_.GetLogTerm(my_last_log_index);
  if (my_last_log_term > request.last_log_term ||
      (my_last_log_term == request.last_log_term &&
       my_last_log_index > request.last_log_index)) {
    reply.term = term_;
    reply.vote_granted = false;
    return reply;
  }

  /* [Raft thesis, Section 3.4]
   * "Each server will vote for at most one candidate in a given term, on a
   * firstcome-first-served basis."
   */

  /* We voted for someone else in this term. */
  if (request.candidate_term == term_ && voted_for_ &&
      *voted_for_ != request.candidate_id) {
    reply.term = term_;
    reply.vote_granted = false;
    return reply;
  }

  /* Now we know we will vote for this candidate, because it's term is at least
   * as big as ours and we haven't voted for anyone else. */
  UpdateTermAndVotedFor(request.candidate_term, request.candidate_id);

  /* [Raft thesis, Section 3.4]
   * A server remains in follower state as long as it receives valid RPCs from a
   * leader or candidate. */
  SetElectionTimer();
  state_changed_.notify_all();

  reply.term = request.candidate_term;
  reply.vote_granted = true;
  return reply;
}

template <class State>
PeerRPCReply::AppendEntries RaftMemberImpl<State>::OnAppendEntries(
    const typename PeerRPCRequest<State>::AppendEntries &request) {
  std::lock_guard<std::mutex> lock(mutex_);
  LogInfo("AppendEntries RPC request from {}", request.leader_id);

  PeerRPCReply::AppendEntries reply;

  /* [Raft thesis, Section 3.3]
   * "If a server receives a request with a stale term number, it rejects the
   * request." */
  if (request.leader_term < term_) {
    reply.term = term_;
    reply.success = false;
    return reply;
  }

  /* [Raft thesis, Section 3.3]
   * "Current terms are exchanged whenever servers communicate; if one server's
   * current term is smaller than the other's, then it updates its current term
   * to the larger value. If a candidate or leader discovers that its term is
   * out of date, it immediately reverts to follower state." */
  if (request.leader_term > term_) {
    if (mode_ != RaftMode::FOLLOWER) {
      CandidateOrLeaderTransitionToFollower();
    }
    UpdateTermAndVotedFor(request.leader_term, {});
  }

  /* [Raft thesis, Section 3.4]
   * "While waiting for votes, a candidate may receive an AppendEntries RPC from
   * another server claiming to be leader. If the leader's term (included in its
   * RPC) is at least as large as the candidate's current term, then the
   * candidate recognizes the leader as legitimate and returns to follower
   * state." */
  if (mode_ == RaftMode::CANDIDATE && request.leader_term == term_) {
    CandidateOrLeaderTransitionToFollower();
  }

  DCHECK(mode_ != RaftMode::LEADER)
      << "Leader cannot accept `AppendEntries` RPC";
  DCHECK(term_ == request.leader_term) << "Term should be equal to request "
                                          "term when accepting `AppendEntries` "
                                          "RPC";

  leader_ = request.leader_id;

  /* [Raft thesis, Section 3.4]
   * A server remains in follower state as long as it receives valid RPCs from a
   * leader or candidate. */
  SetElectionTimer();
  state_changed_.notify_all();

  /* [Raft thesis, Section 3.5]
   * "When sending an AppendEntries RPC, the leader includes the index and term
   * of the entry in its log that immediately precedes the new entries. If the
   * follower does not find an entry in its log with the same index and term,
   * then it refuses the new entries." */
  if (request.prev_log_index > storage_.GetLastLogIndex() ||
      storage_.GetLogTerm(request.prev_log_index) != request.prev_log_term) {
    reply.term = term_;
    reply.success = false;
    return reply;
  }

  /* [Raft thesis, Section 3.5]
   * "To bring a follower's log into consistency with its own, the leader must
   * find the latest log entry where the two logs agree, delete any entries in
   * the follower's log after that point, and send the follower all of the
   * leader's entries after that point." */

  /* Entry at `request.prev_log_index` is the last entry where ours and leader's
   * logs agree. It's time to replace the tail of the log with new entries from
   * the leader. We have to be careful here as duplicated AppendEntries RPCs
   * could cause data loss.
   *
   * There is a possibility that an old AppendEntries RPC is duplicated and
   * received after processing newer one. For example, leader appends entry 3
   * and then entry 4, but follower recieves entry 3, then entry 4, and then
   * entry 3 again. We have to be careful not to delete entry 4 from log when
   * processing the last RPC. */
  LogIndex index = request.prev_log_index;
  auto it = request.entries.begin();
  for (; it != request.entries.end(); ++it) {
    ++index;
    if (index > storage_.GetLastLogIndex()) {
      break;
    }
    if (storage_.GetLogTerm(index) != it->term) {
      LogInfo("Truncating log suffix from index {}", index);
      DCHECK(commit_index_ < index)
          << "Committed entries should never be truncated form the log";
      storage_.TruncateLogSuffix(index);
      break;
    }
  }

  LogInfo("Appending {} out of {} logs from {}.", request.entries.end() - it,
          request.entries.size(), request.leader_id);

  for (; it != request.entries.end(); ++it) {
    storage_.AppendLogEntry(*it);
  }

  commit_index_ = std::max(commit_index_, request.leader_commit);

  /* Let's bump election timer once again, we don't want to take down the leader
   * because of our long disk writes. */
  SetElectionTimer();
  state_changed_.notify_all();

  reply.term = term_;
  reply.success = true;
  return reply;
}

}  // namespace impl

template <class State>
RaftMember<State>::RaftMember(RaftNetworkInterface<State> &network,
                              RaftStorageInterface<State> &storage,
                              const MemberId &id, const RaftConfig &config)
    : impl_(network, storage, id, config) {
  timer_thread_ =
      std::thread(&impl::RaftMemberImpl<State>::TimerThreadMain, &impl_);

  for (const auto &peer_id : config.members) {
    if (peer_id != id) {
      peer_threads_.emplace_back(&impl::RaftMemberImpl<State>::PeerThreadMain,
                                 &impl_, peer_id);
    }
  }
}

template <class State>
RaftMember<State>::~RaftMember() {
  impl_.Stop();
  timer_thread_.join();

  for (auto &peer_thread : peer_threads_) {
    peer_thread.join();
  }
}

template <class State>
PeerRPCReply::RequestVote RaftMember<State>::OnRequestVote(
    const typename PeerRPCRequest<State>::RequestVote &request) {
  return impl_.OnRequestVote(request);
}

template <class State>
PeerRPCReply::AppendEntries RaftMember<State>::OnAppendEntries(
    const typename PeerRPCRequest<State>::AppendEntries &request) {
  return impl_.OnAppendEntries(request);
}

}  // namespace communication::raft
