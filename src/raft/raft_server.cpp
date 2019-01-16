#include "raft/raft_server.hpp"

#include <kj/std/iostream.h>
#include <chrono>
#include <experimental/filesystem>
#include <memory>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/graph_db_accessor.hpp"
#include "durability/single_node_ha/recovery.hpp"
#include "durability/single_node_ha/snapshooter.hpp"
#include "raft/exceptions.hpp"
#include "utils/exceptions.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/serialization.hpp"
#include "utils/string.hpp"
#include "utils/thread.hpp"

namespace raft {

using namespace std::literals::chrono_literals;
namespace fs = std::experimental::filesystem;

const std::string kCurrentTermKey = "current_term";
const std::string kVotedForKey = "voted_for";
const std::string kLogSizeKey = "log_size";
const std::string kLogEntryPrefix = "log_entry_";
const std::string kSnapshotMetadataKey = "snapshot_metadata";
const std::string kRaftDir = "raft";
const std::chrono::duration<int64_t> kSnapshotPeriod = 1s;

RaftServer::RaftServer(uint16_t server_id, const std::string &durability_dir,
                       bool db_recover_on_startup, const Config &config,
                       Coordination *coordination,
                       database::StateDeltaApplier *delta_applier,
                       database::GraphDb *db)
    : config_(config),
      coordination_(coordination),
      delta_applier_(delta_applier),
      db_(db),
      rlog_(std::make_unique<ReplicationLog>()),
      mode_(Mode::FOLLOWER),
      server_id_(server_id),
      durability_dir_(durability_dir),
      db_recover_on_startup_(db_recover_on_startup),
      commit_index_(0),
      last_applied_(0),
      disk_storage_(fs::path(durability_dir) / kRaftDir) {}

void RaftServer::Start() {
  if (db_recover_on_startup_) {
    auto snapshot_metadata = GetSnapshotMetadata();
    if (snapshot_metadata) {
      RecoverSnapshot();

      last_applied_ = snapshot_metadata->second;
      commit_index_ = snapshot_metadata->second;
    }
  } else {
    // We need to clear persisted data if we don't want any recovery.
    disk_storage_.DeletePrefix("");
    durability::RemoveAllSnapshots(durability_dir_);
  }

  // Persistent storage initialization
  if (LogSize() == 0) {
    UpdateTerm(0);
    LogEntry empty_log_entry(0, {});
    AppendLogEntries(0, 0, {empty_log_entry});
  }

  // Peer state initialization
  int cluster_size = coordination_->WorkerCount() + 1;
  next_index_.resize(cluster_size);
  match_index_.resize(cluster_size);
  next_heartbeat_.resize(cluster_size);
  backoff_until_.resize(cluster_size);

  // RPC registration
  coordination_->Register<RequestVoteRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        std::lock_guard<std::mutex> guard(lock_);
        RequestVoteReq req;
        Load(&req, req_reader);

        // [Raft paper 5.1]
        // "If a server recieves a request with a stale term,
        // it rejects the request"
        uint64_t current_term = CurrentTerm();
        if (exiting_ || req.term < current_term) {
          RequestVoteRes res(false, current_term);
          Save(res, res_builder);
          return;
        }

        // [Raft paper figure 2]
        // If RPC request or response contains term T > currentTerm,
        // set currentTerm = T and convert to follower.
        if (req.term > current_term) {
          UpdateTerm(req.term);
          if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
        }

        // [Raft paper 5.2, 5.4]
        // "Each server will vote for at most one candidate in a given
        // term, on a first-come-first-serve basis with an additional
        // restriction on votes"
        // Restriction: "The voter denies its vote if its own log is more
        // up-to-date than that of the candidate"
        std::experimental::optional<uint16_t> voted_for = VotedFor();
        auto last_entry_data = LastEntryData();
        bool grant_vote =
            (!voted_for || voted_for.value() == req.candidate_id) &&
            AtLeastUpToDate(req.last_log_index, req.last_log_term,
                            last_entry_data.first, last_entry_data.second);
        RequestVoteRes res(grant_vote, current_term);
        if (grant_vote) SetNextElectionTimePoint();
        Save(res, res_builder);
      });

  coordination_->Register<AppendEntriesRpc>([this](const auto &req_reader,
                                                   auto *res_builder) {
    std::lock_guard<std::mutex> guard(lock_);
    AppendEntriesReq req;
    Load(&req, req_reader);

    // [Raft paper 5.1]
    // "If a server receives a request with a stale term, it rejects the
    // request"
    uint64_t current_term = CurrentTerm();
    if (req.term < current_term) {
      AppendEntriesRes res(false, current_term);
      Save(res, res_builder);
      return;
    }

    // Everything below is considered to be a valid RPC. This will ensure that
    // after we finish processing the current request, the election timeout will
    // be extended.
    utils::OnScopeExit extend_election_timeout([this] {
      // [Raft thesis 3.4]
      // A server remains in follower state as long as it receives valid RPCs
      // from a leader or candidate.
      SetNextElectionTimePoint();
      election_change_.notify_all();
    });

    // [Raft paper 5.3]
    // "If a follower's log is inconsistent with the leader's, the
    // consistency check will fail in the AppendEntries RPC."
    //
    // Consistency checking assures the Log Matching Property:
    //   - If two entries in different logs have the same index and
    //     term, then they store the same command.
    //   - If two entries in different logs have the same index and term,
    //     then the logs are identical in all preceding entries.
    if (LogSize() <= req.prev_log_index ||
        GetLogEntry(req.prev_log_index).term != req.prev_log_term) {
      AppendEntriesRes res(false, current_term);
      Save(res, res_builder);
      return;
    }

    // [Raft paper figure 2]
    // If RPC request or response contains term T > currentTerm,
    // set currentTerm = T and convert to follower.
    if (req.term > current_term) {
      UpdateTerm(req.term);
      if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
    }

    AppendLogEntries(req.leader_commit, req.prev_log_index + 1, req.entries);

    // [Raft paper 5.3]
    // "Once a follower learns that a log entry is committed, it applies
    // the entry to its state machine (in log order)
    while (req.leader_commit > last_applied_ && last_applied_ + 1 < LogSize()) {
      ++last_applied_;
      delta_applier_->Apply(GetLogEntry(last_applied_).deltas);
    }

    // Respond positively to a heartbeat.
    if (req.entries.empty()) {
      AppendEntriesRes res(true, current_term);
      Save(res, res_builder);
      if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
      return;
    }

    AppendEntriesRes res(true, current_term);
    Save(res, res_builder);
  });

  // start threads

  SetNextElectionTimePoint();
  election_thread_ = std::thread(&RaftServer::ElectionThreadMain, this);

  for (const auto &peer_id : coordination_->GetWorkerIds()) {
    if (peer_id == server_id_) continue;
    peer_threads_.emplace_back(&RaftServer::PeerThreadMain, this, peer_id);
  }

  no_op_issuer_thread_ = std::thread(&RaftServer::NoOpIssuerThreadMain, this);

  snapshot_thread_ = std::thread(&RaftServer::SnapshotThread, this);
}

void RaftServer::Shutdown() {
  {
    std::lock_guard<std::mutex> guard(lock_);
    exiting_ = true;

    state_changed_.notify_all();
    election_change_.notify_all();
    leader_changed_.notify_all();
  }

  for (auto &peer_thread : peer_threads_) {
    if (peer_thread.joinable()) peer_thread.join();
  }

  if (election_thread_.joinable()) election_thread_.join();
  if (no_op_issuer_thread_.joinable()) no_op_issuer_thread_.join();
  if (snapshot_thread_.joinable()) snapshot_thread_.join();
}

uint64_t RaftServer::CurrentTerm() {
  auto opt_value = disk_storage_.Get(kCurrentTermKey);
  if (opt_value == std::experimental::nullopt)
    throw MissingPersistentDataException(kCurrentTermKey);
  return std::stoull(opt_value.value());
}

std::experimental::optional<uint16_t> RaftServer::VotedFor() {
  auto opt_value = disk_storage_.Get(kVotedForKey);
  if (opt_value == std::experimental::nullopt)
    return std::experimental::nullopt;
  return {std::stoul(opt_value.value())};
}

uint64_t RaftServer::LogSize() {
  auto opt_value = disk_storage_.Get(kLogSizeKey);
  if (opt_value == std::experimental::nullopt) {
    disk_storage_.Put(kLogSizeKey, "0");
    return 0;
  }
  return std::stoull(opt_value.value());
}

std::experimental::optional<std::pair<uint64_t, uint64_t>>
RaftServer::GetSnapshotMetadata() {
  auto opt_value = disk_storage_.Get(kSnapshotMetadataKey);
  if (opt_value == std::experimental::nullopt) {
    return std::experimental::nullopt;
  }

  auto value = utils::Split(opt_value.value(), " ");
  if (value.size() != 2) {
    LOG(WARNING) << "Malformed snapshot metdata";
    return std::experimental::nullopt;
  }
  return std::make_pair(std::stoull(value[0]), std::stoull(value[1]));
}

void RaftServer::PersistSnapshotMetadata(uint64_t last_included_term,
                                         uint64_t last_included_index) {
  auto value = utils::Join(
      {std::to_string(last_included_term), std::to_string(last_included_index)},
      " ");
  disk_storage_.Put(kSnapshotMetadataKey, value);
}

void RaftServer::AppendToLog(const tx::TransactionId &tx_id,
                             const std::vector<database::StateDelta> &deltas) {
  std::unique_lock<std::mutex> lock(lock_);
  DCHECK(mode_ == Mode::LEADER)
      << "`AppendToLog` should only be called in LEADER mode";
  if (deltas.size() == 2) {
    DCHECK(deltas[0].type == database::StateDelta::Type::TRANSACTION_BEGIN &&
           deltas[1].type == database::StateDelta::Type::TRANSACTION_COMMIT)
        << "Transactions with two state deltas must be reads (start with BEGIN "
           "and end with COMMIT)";
    rlog_->set_replicated(tx_id);
    return;
  }

  uint64_t log_size = LogSize();
  DCHECK(last_applied_ == log_size - 1) << "Everything from the leaders log "
                                           "should be applied into our state "
                                           "machine";
  rlog_->set_active(tx_id);
  LogEntry new_entry(CurrentTerm(), deltas);

  ++last_applied_;
  disk_storage_.Put(LogEntryKey(log_size), SerializeLogEntry(new_entry));
  disk_storage_.Put(kLogSizeKey, std::to_string(log_size + 1));

  // Force issuing heartbeats
  TimePoint now = Clock::now();
  for (auto &peer_heartbeat : next_heartbeat_) peer_heartbeat = now;

  state_changed_.notify_all();
}

void RaftServer::Emplace(const database::StateDelta &delta) {
  log_entry_buffer_.Emplace(delta);
}

bool RaftServer::SafeToCommit(const tx::TransactionId &tx_id) {
  switch (mode_) {
    case Mode::CANDIDATE:
      // When Memgraph first starts, the Raft is initialized in candidate
      // mode and we try to perform recovery. Since everything for recovery
      // needs to be able to commit, we return true.
      return true;
    case Mode::FOLLOWER:
      // When in follower mode, we will only try to apply a Raft Log when we
      // receive a commit index greater or equal from the Log index from the
      // leader. At that moment we don't have to check the replication log
      // because the leader won't commit the Log locally if it's not replicated
      // on the majority of the peers in the cluster. This is why we can short
      // circut the check to always return true if in follower mode.
      return true;
    case Mode::LEADER:
      if (rlog_->is_active(tx_id)) return false;
      if (rlog_->is_replicated(tx_id)) return true;
      // The only possibility left is that our ReplicationLog doesn't contain
      // information about that tx.
      throw InvalidReplicationLogLookup();
      break;
  }
}

void RaftServer::GarbageCollectReplicationLog(const tx::TransactionId &tx_id) {
  rlog_->garbage_collect_older(tx_id);
}

bool RaftServer::IsLeader() { return mode_ == Mode::LEADER; }

RaftServer::LogEntryBuffer::LogEntryBuffer(RaftServer *raft_server)
    : raft_server_(raft_server) {
  CHECK(raft_server_) << "RaftServer can't be nullptr";
}

void RaftServer::LogEntryBuffer::Enable() {
  std::lock_guard<std::mutex> guard(buffer_lock_);
  enabled_ = true;
}

void RaftServer::LogEntryBuffer::Disable() {
  std::lock_guard<std::mutex> guard(buffer_lock_);
  enabled_ = false;
  // Clear all existing logs from buffers.
  logs_.clear();
}

void RaftServer::LogEntryBuffer::Emplace(const database::StateDelta &delta) {
  std::unique_lock<std::mutex> lock(buffer_lock_);
  if (!enabled_) return;

  tx::TransactionId tx_id = delta.transaction_id;
  if (delta.type == database::StateDelta::Type::TRANSACTION_COMMIT) {
    auto it = logs_.find(tx_id);
    CHECK(it != logs_.end()) << "Missing StateDeltas for transaction " << tx_id;

    std::vector<database::StateDelta> log(std::move(it->second));
    log.emplace_back(std::move(delta));
    logs_.erase(it);

    lock.unlock();
    raft_server_->AppendToLog(tx_id, log);

  } else if (delta.type == database::StateDelta::Type::TRANSACTION_ABORT) {
    auto it = logs_.find(tx_id);
    CHECK(it != logs_.end()) << "Missing StateDeltas for transaction " << tx_id;
    logs_.erase(it);
  } else {
    logs_[tx_id].emplace_back(std::move(delta));
  }
}

void RaftServer::Transition(const Mode &new_mode) {
  switch (new_mode) {
    case Mode::FOLLOWER: {
      VLOG(40) << "Server " << server_id_
               << ": Transition to FOLLOWER (Term: " << CurrentTerm() << ")";

      bool reset = mode_ == Mode::LEADER;
      mode_ = Mode::FOLLOWER;
      log_entry_buffer_.Disable();

      if (reset) {
        VLOG(40) << "Resetting internal state";
        // Temporary freeze election timer while we do the reset.
        next_election_ = TimePoint::max();

        db_->Reset();
        ResetReplicationLog();

        // Re-apply raft log.
        auto snapshot_metadata = GetSnapshotMetadata();
        uint64_t starting_index = 1;
        if (snapshot_metadata) {
          RecoverSnapshot();
          starting_index = snapshot_metadata->second + 1;
        }
        for (uint64_t i = starting_index; i <= commit_index_; ++i)
          delta_applier_->Apply(GetLogEntry(i).deltas);
        last_applied_ = commit_index_;
      }

      SetNextElectionTimePoint();
      election_change_.notify_all();
      break;
    }

    case Mode::CANDIDATE: {
      VLOG(40) << "Server " << server_id_
               << ": Transition to CANDIDATE (Term: " << CurrentTerm() << ")";

      // [Raft thesis, section 3.4]
      // "Each candidate restarts its randomized election timeout at the start
      // of an election, and it waits for that timeout to elapse before
      // starting the next election; this reduces the likelihood of another
      // split vote in the new election."
      SetNextElectionTimePoint();
      election_change_.notify_all();

      // [Raft thesis, section 3.4]
      // "To begin an election, a follower increments its current term and
      // transitions to candidate state.  It then votes for itself and issues
      // RequestVote RPCs in parallel to each of the other servers in the
      // cluster."
      disk_storage_.Put(kCurrentTermKey, std::to_string(CurrentTerm() + 1));
      disk_storage_.Put(kVotedForKey, std::to_string(server_id_));

      granted_votes_ = 1;
      vote_requested_.assign(coordination_->WorkerCount(), false);

      mode_ = Mode::CANDIDATE;

      if (HasMajortyVote()) {
        Transition(Mode::LEADER);
        state_changed_.notify_all();
        return;
      }

      break;
    }

    case Mode::LEADER: {
      VLOG(40) << "Server " << server_id_
               << ": Transition to LEADER (Term: " << CurrentTerm() << ")";
      // Freeze election timer
      next_election_ = TimePoint::max();
      election_change_.notify_all();
      uint64_t log_size = LogSize();

      // Set next heartbeat to correct values
      TimePoint now = Clock::now();
      for (auto &peer_heartbeat : next_heartbeat_)
        peer_heartbeat = now + config_.heartbeat_interval;

      // [Raft paper figure 2]
      // "For each server, index of the next log entry to send to that server
      // is initialized to leader's last log index + 1"
      for (int i = 1; i < coordination_->WorkerCount() + 1; ++i) {
        next_index_[i] = log_size;
        match_index_[i] = 0;
      }

      // Raft guarantees the Leader Append-Only property [Raft paper 5.2]
      // so its safe to apply everything from our log into our state machine
      for (int i = last_applied_ + 1; i < log_size; ++i)
        delta_applier_->Apply(GetLogEntry(i).deltas);
      last_applied_ = log_size - 1;

      mode_ = Mode::LEADER;
      log_entry_buffer_.Enable();

      leader_changed_.notify_all();
      break;
    }
  }
}

void RaftServer::UpdateTerm(uint64_t new_term) {
  disk_storage_.Put(kCurrentTermKey, std::to_string(new_term));
  disk_storage_.Delete(kVotedForKey);
}

void RaftServer::AdvanceCommitIndex() {
  DCHECK(mode_ == Mode::LEADER)
      << "Commit index can only be advanced by the leader";

  std::vector<uint64_t> known_replication_indices;
  uint64_t log_size = LogSize();
  for (int i = 1; i < coordination_->WorkerCount() + 1; ++i) {
    if (i != server_id_)
      known_replication_indices.push_back(match_index_[i]);
    else
      known_replication_indices.push_back(log_size - 1);
  }

  std::sort(known_replication_indices.begin(), known_replication_indices.end());
  uint64_t new_commit_index =
      known_replication_indices[(coordination_->WorkerCount() - 1) / 2];

  // This can happen because we reset `match_index` vector to 0 after a
  // new leader has been elected.
  if (commit_index_ >= new_commit_index) return;

  // [Raft thesis, section 3.6.2]
  // "(...) Raft never commits log entries from previous terms by counting
  // replicas. Only log entries from the leader's current term are committed by
  // counting replicas; once an entry from the current term has been committed
  // in this way, then all prior entries are committed indirectly because of the
  // Log Matching Property."
  if (GetLogEntry(new_commit_index).term != CurrentTerm()) {
    VLOG(40) << "Server " << server_id_
             << ": cannot commit log entry from "
                "previous term based on "
                "replication count.";
    return;
  }

  VLOG(40) << "Begin applying commited transactions";

  for (int i = commit_index_ + 1; i <= new_commit_index; ++i) {
    auto deltas = GetLogEntry(i).deltas;
    DCHECK(deltas.size() > 2)
        << "Log entry should consist of at least two state deltas.";
    rlog_->set_replicated(deltas[0].transaction_id);
  }

  commit_index_ = new_commit_index;
}

void RaftServer::Recover() {
  throw utils::NotYetImplemented("RaftServer Recover");
}

void RaftServer::SendEntries(uint16_t peer_id,
                             std::unique_lock<std::mutex> &lock) {
  uint64_t request_term = CurrentTerm();
  uint64_t request_prev_log_index = next_index_[peer_id] - 1;
  uint64_t request_prev_log_term = GetLogEntry(next_index_[peer_id] - 1).term;

  std::vector<LogEntry> request_entries;
  if (next_index_[peer_id] <= LogSize() - 1)
    GetLogSuffix(next_index_[peer_id], request_entries);

  bool unreachable_peer = false;
  auto peer_future = coordination_->ExecuteOnWorker<AppendEntriesRes>(
      peer_id, [&](int worker_id, auto &client) {
        try {
          auto res = client.template Call<AppendEntriesRpc>(
              server_id_, commit_index_, request_term, request_prev_log_index,
              request_prev_log_term, request_entries);
          return res;
        } catch (...) {
          // not being able to connect to peer means we need to retry.
          // TODO(ipaljak): Consider backoff.
          unreachable_peer = true;
          return AppendEntriesRes(false, request_term);
        }
      });

  VLOG(40) << "Entries size: " << request_entries.size();

  lock.unlock();  // Release lock while waiting for response.
  auto reply = peer_future.get();
  lock.lock();

  if (unreachable_peer) {
    next_heartbeat_[peer_id] = Clock::now() + config_.heartbeat_interval;
    return;
  }

  if (CurrentTerm() != request_term || exiting_) {
    return;
  }

  if (OutOfSync(reply.term)) {
    state_changed_.notify_all();
    return;
  }

  DCHECK(mode_ == Mode::LEADER)
      << "Elected leader for term should never change.";

  if (reply.term != CurrentTerm()) {
    VLOG(40) << "Server " << server_id_
             << ": Ignoring stale AppendEntriesRPC reply from " << peer_id;
    return;
  }

  if (!reply.success) {
    DCHECK(next_index_[peer_id] > 1)
        << "Log replication should not fail for first log entry";
    --next_index_[peer_id];
  } else {
    uint64_t new_match_index = request_prev_log_index + request_entries.size();
    DCHECK(match_index_[peer_id] <= new_match_index)
        << "`match_index` should increase monotonically within a term";
    match_index_[peer_id] = new_match_index;
    if (request_entries.size() > 0) AdvanceCommitIndex();
    next_index_[peer_id] = match_index_[peer_id] + 1;
    next_heartbeat_[peer_id] = Clock::now() + config_.heartbeat_interval;
  }

  state_changed_.notify_all();
}

void RaftServer::ElectionThreadMain() {
  std::unique_lock<std::mutex> lock(lock_);
  while (!exiting_) {
    if (Clock::now() >= next_election_) {
      VLOG(40) << "Server " << server_id_
               << ": Election timeout exceeded (Term: " << CurrentTerm() << ")";
      Transition(Mode::CANDIDATE);
      state_changed_.notify_all();
    }
    election_change_.wait_until(lock, next_election_);
  }
}

void RaftServer::PeerThreadMain(uint16_t peer_id) {
  utils::ThreadSetName(fmt::format("RaftPeer{}", peer_id));
  std::unique_lock<std::mutex> lock(lock_);

  /* This loop will either call a function that issues an RPC or wait on the
   * condition variable. It must not do both! Lock on `mutex_` is released
   * while waiting for RPC response, which might cause us to miss a
   * notification on `state_changed_` conditional variable and wait
   * indefinitely. The safest thing to do is to assume some important part of
   * state was modified while we were waiting for the response and loop around
   * to check. */
  while (!exiting_) {
    TimePoint now = Clock::now();
    TimePoint wait_until;

    if (mode_ != Mode::FOLLOWER && backoff_until_[peer_id] > now) {
      wait_until = backoff_until_[peer_id];
    } else {
      switch (mode_) {
        case Mode::FOLLOWER: {
          wait_until = TimePoint::max();
          break;
        }

        case Mode::CANDIDATE: {
          if (vote_requested_[peer_id]) break;

          // TODO(ipaljak): Consider backoff.
          wait_until = TimePoint::max();

          auto request_term = CurrentTerm();
          auto peer_future = coordination_->ExecuteOnWorker<RequestVoteRes>(
              peer_id, [&](int worker_id, auto &client) {
                auto last_entry_data = LastEntryData();
                try {
                  auto res = client.template Call<RequestVoteRpc>(
                      server_id_, request_term, last_entry_data.first,
                      last_entry_data.second);
                  return res;
                } catch (...) {
                  // not being able to connect to peer defaults to a vote
                  // being denied from that peer. This is correct but not
                  // optimal.
                  //
                  // TODO(ipaljak): reconsider this decision :)
                  return RequestVoteRes(false, request_term);
                }
              });

          lock.unlock();  // Release lock while waiting for response
          auto reply = peer_future.get();
          lock.lock();

          if (CurrentTerm() != request_term || mode_ != Mode::CANDIDATE ||
              exiting_) {
            VLOG(40) << "Server " << server_id_
                     << ": Ignoring RequestVoteRPC reply from " << peer_id;
            break;
          }

          if (OutOfSync(reply.term)) {
            state_changed_.notify_all();
            continue;
          }

          vote_requested_[peer_id] = true;

          if (reply.vote_granted) {
            VLOG(40) << "Server " << server_id_ << ": Got vote from "
                     << peer_id;
            ++granted_votes_;
            if (HasMajortyVote()) Transition(Mode::LEADER);
          } else {
            VLOG(40) << "Server " << server_id_ << ": Denied vote from "
                     << peer_id;
          }

          state_changed_.notify_all();
          continue;
        }

        case Mode::LEADER: {
          if (now >= next_heartbeat_[peer_id]) {
            VLOG(40) << "Server " << server_id_
                     << ": Send AppendEntries RPC to server " << peer_id
                     << " (Term: " << CurrentTerm() << ")";
            SendEntries(peer_id, lock);
            continue;
          }
          wait_until = next_heartbeat_[peer_id];
          break;
        }
      }
    }

    state_changed_.wait_until(lock, wait_until);
  }
}

void RaftServer::NoOpIssuerThreadMain() {
  utils::ThreadSetName(fmt::format("NoOpIssuer"));
  std::mutex m;
  auto lock = std::unique_lock<std::mutex>(m);
  while (!exiting_) {
    leader_changed_.wait(lock);
    // no_op_create_callback_ will create a new transaction that has a NO_OP
    // StateDelta. This will trigger the whole procedure of replicating logs
    // in our implementation of Raft.
    if (!exiting_) NoOpCreate();
  }
}

void RaftServer::SnapshotThread() {
  utils::ThreadSetName(fmt::format("RaftSnapshot"));
  while (!exiting_) {
    {
      std::unique_lock<std::mutex> lock(lock_);

      uint64_t uncompacted_log_size = LogSize();
      auto snapshot_metadata = GetSnapshotMetadata();
      if (snapshot_metadata) {
        uncompacted_log_size -= snapshot_metadata->second;
      }

      // Compare the log size to the config
      if (config_.log_size_snapshot_threshold < uncompacted_log_size) {
        // Create a DB accessor for snapshot creation
        std::unique_ptr<database::GraphDbAccessor> dba = db_->Access();
        uint64_t current_term = CurrentTerm();
        uint64_t last_applied = last_applied_;

        lock.unlock();
        bool status =
            durability::MakeSnapshot(*db_, *dba, fs::path(durability_dir_));
        lock.lock();

        if (status) {
          uint64_t log_compaction_start_index = 1;
          if (snapshot_metadata) {
            log_compaction_start_index = snapshot_metadata->second + 1;
          }

          PersistSnapshotMetadata(current_term, last_applied);

          // Log compaction.
          // TODO (msantl): In order to handle log compaction correctly, we need
          // to be able to send snapshots over the wire and implement additional
          // logic to handle log entries that were compacted into a snapshot.
          // for (int i = log_compaction_start_index; i <= last_applied_; ++i) {
          // disk_storage_.Delete(LogEntryKey(i));
          // }
        }

        lock.unlock();
        // Raft lock must be released when destroying dba object.
        dba = nullptr;
      }
    }

    std::this_thread::sleep_for(kSnapshotPeriod);
  }
}

void RaftServer::SetNextElectionTimePoint() {
  // [Raft thesis, section 3.4]
  // "Raft uses randomized election timeouts to ensure that split votes are
  // rare and that they are resolved quickly. To prevent split votes in the
  // first place, election timeouts are chosen randomly from a fixed interval
  // (e.g., 150-300 ms)."
  std::uniform_int_distribution<uint64_t> distribution(
      config_.election_timeout_min.count(),
      config_.election_timeout_max.count());
  Clock::duration wait_interval = std::chrono::milliseconds(distribution(rng_));
  next_election_ = Clock::now() + wait_interval;
}

bool RaftServer::HasMajortyVote() {
  if (2 * granted_votes_ > coordination_->WorkerCount()) {
    VLOG(40) << "Server " << server_id_
             << ": Obtained majority vote (Term: " << CurrentTerm() << ")";
    return true;
  }
  return false;
}

std::pair<uint64_t, uint64_t> RaftServer::LastEntryData() {
  uint64_t log_size = LogSize();
  if (log_size == 0) return {0, 0};
  return {log_size, GetLogEntry(log_size - 1).term};
}

bool RaftServer::AtLeastUpToDate(uint64_t last_log_index_a,
                                 uint64_t last_log_term_a,
                                 uint64_t last_log_index_b,
                                 uint64_t last_log_term_b) {
  if (last_log_term_a == last_log_term_b)
    return last_log_index_a >= last_log_index_b;
  return last_log_term_a > last_log_term_b;
}

bool RaftServer::OutOfSync(uint64_t reply_term) {
  DCHECK(mode_ != Mode::FOLLOWER) << "`OutOfSync` called from FOLLOWER mode";

  // [Raft thesis, Section 3.3]
  // "Current terms are exchanged whenever servers communicate; if one
  // server's current term is smaller than the other's, then it updates
  // its current term to the larger value. If a candidate or leader
  // discovers that its term is out of date, it immediately reverts to
  // follower state."
  if (CurrentTerm() < reply_term) {
    disk_storage_.Put(kCurrentTermKey, std::to_string(reply_term));
    disk_storage_.Delete(kVotedForKey);
    granted_votes_ = 0;
    Transition(Mode::FOLLOWER);
    return true;
  }
  return false;
}

LogEntry RaftServer::GetLogEntry(int index) {
  auto opt_value = disk_storage_.Get(LogEntryKey(index));
  DCHECK(opt_value != std::experimental::nullopt)
      << "Log index (" << index << ") out of bounds.";
  return DeserializeLogEntry(opt_value.value());
}

void RaftServer::DeleteLogSuffix(int starting_index) {
  uint64_t log_size = LogSize();
  DCHECK(0 <= starting_index && starting_index < log_size)
      << "Log index out of bounds.";
  for (int i = starting_index; i < log_size; ++i)
    disk_storage_.Delete(LogEntryKey(i));
  disk_storage_.Put(kLogSizeKey, std::to_string(starting_index));
}

void RaftServer::GetLogSuffix(int starting_index,
                              std::vector<raft::LogEntry> &entries) {
  uint64_t log_size = LogSize();
  DCHECK(0 <= starting_index && starting_index < log_size)
      << "Log index out of bounds.";
  for (int i = starting_index; i < log_size; ++i)
    entries.push_back(GetLogEntry(i));
}

void RaftServer::AppendLogEntries(uint64_t leader_commit_index,
                                  uint64_t starting_index,
                                  const std::vector<LogEntry> &new_entries) {
  uint64_t log_size = LogSize();
  for (int i = 0; i < new_entries.size(); ++i) {
    // If existing entry conflicts with new one, we need to delete the
    // existing entry and all that follow it.
    int current_index = i + starting_index;
    if (log_size > current_index &&
        GetLogEntry(current_index).term != new_entries[i].term) {
      DeleteLogSuffix(current_index);
      log_size = LogSize();
    }
    DCHECK(log_size >= current_index) << "Current Log index out of bounds.";
    if (log_size == current_index) {
      disk_storage_.Put(LogEntryKey(log_size),
                        SerializeLogEntry(new_entries[i]));
      disk_storage_.Put(kLogSizeKey, std::to_string(log_size + 1));
      log_size += 1;
    }
  }

  // See Raft paper 5.3
  if (leader_commit_index > commit_index_) {
    commit_index_ = std::min(leader_commit_index, log_size - 1);
  }
}

std::string RaftServer::LogEntryKey(uint64_t index) {
  return kLogEntryPrefix + std::to_string(index);
}

std::string RaftServer::SerializeLogEntry(const LogEntry &log_entry) {
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  {
    ::capnp::MallocMessageBuilder message;
    capnp::LogEntry::Builder log_builder = message.initRoot<capnp::LogEntry>();
    Save(log_entry, &log_builder);
    kj::std::StdOutputStream std_stream(stream);
    kj::BufferedOutputStreamWrapper buffered_stream(std_stream);
    writeMessage(buffered_stream, message);
  }
  return stream.str();
}

LogEntry RaftServer::DeserializeLogEntry(
    const std::string &serialized_log_entry) {
  ::capnp::MallocMessageBuilder message;
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  kj::std::StdInputStream std_stream(stream);
  kj::BufferedInputStreamWrapper buffered_stream(std_stream);
  stream << serialized_log_entry;
  readMessageCopy(buffered_stream, message);
  capnp::LogEntry::Reader log_reader =
      message.getRoot<capnp::LogEntry>().asReader();
  LogEntry deserialized_log;
  Load(&deserialized_log, log_reader);
  return deserialized_log;
}

void RaftServer::ResetReplicationLog() {
  rlog_ = nullptr;
  rlog_ = std::make_unique<ReplicationLog>();
}

void RaftServer::RecoverSnapshot() {
  durability::RecoveryData recovery_data;
  CHECK(durability::RecoverOnlySnapshot(durability_dir_, db_, &recovery_data))
      << "Failed to recover from snapshot";
  durability::RecoverIndexes(db_, recovery_data.indexes);
}

void RaftServer::NoOpCreate() {
  auto dba = db_->Access();
  Emplace(database::StateDelta::NoOp(dba->transaction_id()));
  dba->Commit();
}

}  // namespace raft
