#pragma once

#include <chrono>
#include <condition_variable>
#include <experimental/optional>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

namespace communication::raft {

enum class ClientResult { NOT_LEADER, OK };

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::system_clock::time_point;

using MemberId = std::string;
using TermId = uint64_t;

using ClientId = uint64_t;
using CommandId = uint64_t;

using LogIndex = uint64_t;

template <class State>
struct LogEntry {
  int term;

  std::experimental::optional<typename State::Change> command;

  bool operator==(const LogEntry &rhs) const {
    return term == rhs.term && command == rhs.command;
  }
  bool operator!=(const LogEntry &rhs) const { return !(*this == rhs); }
};

/* Raft RPC requests and replies as described in [Raft thesis, Figure 3.1]. */
enum class RPCType { REQUEST_VOTE, APPEND_ENTRIES };

template <class State>
struct PeerRPCRequest {
  RPCType type;

  struct RequestVote {
    TermId candidate_term;
    MemberId candidate_id;
    LogIndex last_log_index;
    TermId last_log_term;
  } request_vote;

  struct AppendEntries {
    TermId leader_term;
    MemberId leader_id;
    LogIndex prev_log_index;
    TermId prev_log_term;
    std::vector<LogEntry<State>> entries;
    LogIndex leader_commit;
  } append_entries;

  TermId Term() const {
    switch (type) {
      case RPCType::REQUEST_VOTE:
        return request_vote.candidate_term;
      case RPCType::APPEND_ENTRIES:
        return append_entries.leader_term;
    }
  }
};

struct PeerRPCReply {
  RPCType type;

  struct RequestVote {
    TermId term;
    bool vote_granted;
  } request_vote;

  struct AppendEntries {
    TermId term;
    bool success;
  } append_entries;

  TermId Term() const {
    switch (type) {
      case RPCType::REQUEST_VOTE:
        return request_vote.term;
      case RPCType::APPEND_ENTRIES:
        return append_entries.term;
    }
  }
};

template <class State>
class RaftNetworkInterface {
 public:
  virtual ~RaftNetworkInterface() = default;

  /* Returns false if RPC failed for some reason (e.g. cannot establish
   * connection, request timeout or request cancelled). Otherwise `reply`
   * contains response from peer. */
  virtual bool SendRPC(const MemberId &recipient,
                       const PeerRPCRequest<State> &request,
                       PeerRPCReply &reply) = 0;
};

template <class State>
class RaftStorageInterface {
 public:
  virtual ~RaftStorageInterface() = default;

  virtual void WriteTermAndVotedFor(
      const TermId term,
      const std::experimental::optional<std::string> &voted_for) = 0;
  virtual std::pair<TermId, std::experimental::optional<MemberId>>
  GetTermAndVotedFor() = 0;
  virtual void AppendLogEntry(const LogEntry<State> &entry) = 0;
  virtual TermId GetLogTerm(const LogIndex index) = 0;
  virtual LogEntry<State> GetLogEntry(const LogIndex index) = 0;
  virtual std::vector<LogEntry<State>> GetLogSuffix(const LogIndex index) = 0;
  virtual LogIndex GetLastLogIndex() = 0;
  virtual void TruncateLogSuffix(const LogIndex index) = 0;
};

struct RaftConfig {
  std::vector<MemberId> members;
  std::chrono::milliseconds leader_timeout_min;
  std::chrono::milliseconds leader_timeout_max;
  std::chrono::milliseconds heartbeat_interval;
};

namespace impl {

enum class RaftMode { FOLLOWER, CANDIDATE, LEADER };

struct RaftPeerState {
  bool request_vote_done;
  bool voted_for_me;
  LogIndex match_index;
  LogIndex next_index;
  bool suppress_log_entries;
  Clock::time_point next_heartbeat_time;
};

template <class State>
class RaftMemberImpl {
 public:
  explicit RaftMemberImpl(RaftNetworkInterface<State> &network,
                          RaftStorageInterface<State> &storage,
                          const MemberId &id, const RaftConfig &config);

  ~RaftMemberImpl();

  void Stop();

  void TimerThreadMain();
  void PeerThreadMain(std::string peer_id);

  void UpdateTermAndVotedFor(
      const TermId new_term,
      const std::experimental::optional<MemberId> &new_voted_for);
  void CandidateOrLeaderTransitionToFollower();
  void CandidateTransitionToLeader();

  bool SendRPC(const std::string &recipient,
               const PeerRPCRequest<State> &request, PeerRPCReply &reply,
               std::unique_lock<std::mutex> &lock);

  void StartNewElection();
  void SetElectionTimer();
  bool CountVotes();
  void RequestVote(const MemberId &peer_id, RaftPeerState &peer_state,
                   std::unique_lock<std::mutex> &lock);

  void AdvanceCommitIndex();
  void AppendEntries(const MemberId &peer_id, RaftPeerState &peer_state,
                     std::unique_lock<std::mutex> &lock);

  PeerRPCReply::RequestVote OnRequestVote(
      const typename PeerRPCRequest<State>::RequestVote &request);
  PeerRPCReply::AppendEntries OnAppendEntries(
      const typename PeerRPCRequest<State>::AppendEntries &request);

  ClientResult AddCommand(const typename State::Change &command, bool blocking);

  template <class... Args>
  void LogInfo(const std::string &, Args &&...);

  RaftNetworkInterface<State> &network_;
  RaftStorageInterface<State> &storage_;

  MemberId id_;
  RaftConfig config_;

  TermId term_;
  RaftMode mode_ = RaftMode::FOLLOWER;
  std::experimental::optional<MemberId> voted_for_ = std::experimental::nullopt;
  std::experimental::optional<MemberId> leader_ = std::experimental::nullopt;

  TimePoint next_election_time_;

  LogIndex commit_index_ = 0;

  bool exiting_ = false;

  std::map<std::string, std::unique_ptr<RaftPeerState>> peer_states_;

  /* This mutex protects all of the internal state. */
  std::mutex mutex_;

  /* Used to notify waiting threads that some of the internal state has changed.
   * It is notified when following events occurr:
   *  - mode change
   *  - election start
   *  - `next_election_time_` update on RPC from leader or candidate
   *  - destructor is called
   *  - `commit_index_` is advanced
   */
  std::condition_variable state_changed_;

  std::mt19937_64 rng_ = std::mt19937_64(std::random_device{}());
};

}  // namespace internal

template <class State>
class RaftMember final {
 public:
  explicit RaftMember(RaftNetworkInterface<State> &network,
                      RaftStorageInterface<State> &storage, const MemberId &id,
                      const RaftConfig &config);
  ~RaftMember();

  /* Just to make the tests work for now until we clean up the reactor stuff. */
  std::experimental::optional<MemberId> Leader() {
    std::lock_guard<std::mutex> lock(impl_.mutex_);
    return impl_.leader_;
  }
  MemberId Id() const { return impl_.id_; }

  PeerRPCReply::RequestVote OnRequestVote(
      const typename PeerRPCRequest<State>::RequestVote &request);
  PeerRPCReply::AppendEntries OnAppendEntries(
      const typename PeerRPCRequest<State>::AppendEntries &request);

  ClientResult AddCommand(const typename State::Change &command, bool blocking);

 private:
  impl::RaftMemberImpl<State> impl_;

  /* Timer thread for triggering elections. */
  std::thread timer_thread_;

  /* One thread per peer for outgoing RPCs. */
  std::vector<std::thread> peer_threads_;
};

}  // namespace communication::raft

#include "raft-inl.hpp"
