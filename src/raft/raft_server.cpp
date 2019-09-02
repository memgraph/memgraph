#include "raft/raft_server.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <optional>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/rpc/client.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/single_node_ha/paths.hpp"
#include "durability/single_node_ha/recovery.hpp"
#include "durability/single_node_ha/snapshooter.hpp"
#include "raft/exceptions.hpp"
#include "slk/streams.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/thread.hpp"

namespace raft {

using namespace std::literals::chrono_literals;
namespace fs = std::filesystem;

const std::string kCurrentTermKey = "current_term";
const std::string kVotedForKey = "voted_for";
const std::string kLogSizeKey = "log_size";
const std::string kLogEntryPrefix = "log_entry_";
const std::string kSnapshotMetadataKey = "snapshot_metadata";
const std::string kRaftDir = "raft";
const std::chrono::duration<int64_t> kSnapshotPeriod = 1s;

RaftServer::RaftServer(uint16_t server_id, const std::string &durability_dir,
                       bool db_recover_on_startup, const Config &config,
                       Coordination *coordination, database::GraphDb *db)
    : config_(config),
      coordination_(coordination),
      db_(db),
      rlog_(std::make_unique<ReplicationLog>()),
      mode_(Mode::FOLLOWER),
      server_id_(server_id),
      durability_dir_(fs::path(durability_dir)),
      db_recover_on_startup_(db_recover_on_startup),
      commit_index_(0),
      last_applied_(0),
      last_entry_term_(0),
      issue_hb_(false),
      replication_timeout_(config.replication_timeout),
      disk_storage_(fs::path(durability_dir) / kRaftDir) {}

void RaftServer::Start() {
  if (db_recover_on_startup_) {
    snapshot_metadata_ = GetSnapshotMetadata();
    if (snapshot_metadata_) {
      RecoverSnapshot(snapshot_metadata_->snapshot_filename);
      last_applied_ = snapshot_metadata_->last_included_index;
      commit_index_ = snapshot_metadata_->last_included_index;
      last_entry_term_ = snapshot_metadata_->last_included_term;
    }
  } else {
    // We need to clear persisted data if we don't want any recovery.
    disk_storage_.DeletePrefix("");
    durability::RemoveAllSnapshots(durability_dir_);
  }

  // Persistent storage initialization
  if (!disk_storage_.Get(kLogSizeKey)) {
    SetCurrentTerm(0);
    SetLogSize(0);
    LogEntry empty_log_entry(0, {});
    AppendLogEntries(0, 0, {empty_log_entry});
  } else {
    RecoverPersistentData();
  }

  // Peer state initialization
  auto cluster_size = coordination_->GetAllNodeCount() + 1;
  next_index_.resize(cluster_size);
  index_offset_.resize(cluster_size);
  match_index_.resize(cluster_size);
  next_replication_.resize(cluster_size);
  next_heartbeat_.resize(cluster_size);

  // RPC registration
  coordination_->Register<RequestVoteRpc>(
      [this](auto *req_reader, auto *res_builder) {
        std::lock_guard<std::mutex> guard(lock_);
        RequestVoteReq req;
        slk::Load(&req, req_reader);

        // [Raft paper 5.1]
        // "If a server recieves a request with a stale term,
        // it rejects the request"
        if (exiting_ || req.term < current_term_) {
          RequestVoteRes res(false, current_term_);
          slk::Save(res, res_builder);
          return;
        }

        // [Raft paper figure 2]
        // If RPC request or response contains term T > currentTerm,
        // set currentTerm = T and convert to follower.
        if (req.term > current_term_) {
          SetCurrentTerm(req.term);
          if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
        }

        if (voted_for_) {
          bool grant_vote = voted_for_.value() == req.candidate_id;
          if (grant_vote) SetNextElectionTimePoint();
          RequestVoteRes res(grant_vote, current_term_);
          slk::Save(res, res_builder);
          return;
        }

        // [Raft paper 5.2, 5.4]
        // "Each server will vote for at most one candidate in a given
        // term, on a first-come-first-serve basis with an additional
        // restriction on votes"
        // Restriction: "The voter denies its vote if its own log is more
        // up-to-date than that of the candidate"
        auto last_entry_data = LastEntryData();
        bool grant_vote =
            AtLeastUpToDate(req.last_log_index, req.last_log_term,
                            last_entry_data.first, last_entry_data.second);
        if (grant_vote) {
          SetVotedFor(req.candidate_id);
          SetNextElectionTimePoint();
        }
        RequestVoteRes res(grant_vote, current_term_);
        slk::Save(res, res_builder);
      });

  coordination_->Register<AppendEntriesRpc>([this](auto *req_reader,
                                                   auto *res_builder) {
    std::lock_guard<std::mutex> guard(lock_);
    AppendEntriesReq req;
    slk::Load(&req, req_reader);

    // [Raft paper 5.1]
    // "If a server receives a request with a stale term, it rejects the
    // request"
    if (exiting_ || req.term < current_term_) {
      AppendEntriesRes res(false, current_term_);
      slk::Save(res, res_builder);
      return;
    }

    // Everything below is considered to be a valid RPC. This will ensure that
    // after we finish processing the current request, the election timeout will
    // be extended. During this process we will prevent the timeout from
    // occuring.
    next_election_ = TimePoint::max();
    election_change_.notify_all();
    utils::OnScopeExit extend_election_timeout([this] {
      // [Raft thesis 3.4]
      // A server remains in follower state as long as it receives valid RPCs
      // from a leader or candidate.
      SetNextElectionTimePoint();
      election_change_.notify_all();
    });

    // [Raft paper figure 2]
    // If RPC request or response contains term T > currentTerm,
    // set currentTerm = T and convert to follower.
    if (req.term > current_term_) {
      SetCurrentTerm(req.term);
      if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
    }

    // [Raft paper 5.3]
    // "If a follower's log is inconsistent with the leader's, the
    // consistency check will fail in the AppendEntries RPC."
    //
    // Consistency checking assures the Log Matching Property:
    //   - If two entries in different logs have the same index and
    //     term, then they store the same command.
    //   - If two entries in different logs have the same index and term,
    //     then the logs are identical in all preceding entries.
    if (snapshot_metadata_ &&
        snapshot_metadata_->last_included_index == req.prev_log_index) {
      if (req.prev_log_term != snapshot_metadata_->last_included_term) {
        AppendEntriesRes res(false, current_term_);
        slk::Save(res, res_builder);
        return;
      }
    } else if (snapshot_metadata_ &&
               snapshot_metadata_->last_included_index > req.prev_log_index) {
      LOG(ERROR) << "Received entries that are already commited and have been "
                    "compacted";
      AppendEntriesRes res(false, current_term_);
      slk::Save(res, res_builder);
      return;
    } else {
      if (log_size_ <= req.prev_log_index ||
          GetLogEntry(req.prev_log_index).term != req.prev_log_term) {
        AppendEntriesRes res(false, current_term_);
        slk::Save(res, res_builder);
        return;
      }
    }

    // No need to call this function for a heartbeat
    if (!req.entries.empty()) {
      AppendLogEntries(req.leader_commit, req.prev_log_index + 1, req.entries);
    }

    // [Raft paper 5.3]
    // "Once a follower learns that a log entry is committed, it applies
    // the entry to its state machine (in log order)
    while (req.leader_commit > last_applied_ && last_applied_ + 1 < log_size_) {
      ++last_applied_;
      ApplyStateDeltas(GetLogEntry(last_applied_).deltas);
    }

    // Respond positively to a heartbeat.
    if (req.entries.empty()) {
      AppendEntriesRes res(true, current_term_);
      slk::Save(res, res_builder);
      if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
      return;
    }

    AppendEntriesRes res(true, current_term_);
    slk::Save(res, res_builder);
  });

  coordination_->Register<HeartbeatRpc>(
      [this](auto *req_reader, auto *res_builder) {
        std::lock_guard<std::mutex> guard(lock_);
        HeartbeatReq req;
        slk::Load(&req, req_reader);

        if (exiting_ || req.term < current_term_) {
          HeartbeatRes res(false, current_term_);
          slk::Save(res, res_builder);
          return;
        }

        if (req.term > current_term_) {
          SetCurrentTerm(req.term);
          if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
        }

        SetNextElectionTimePoint();
        election_change_.notify_all();

        HeartbeatRes res(true, current_term_);
        slk::Save(res, res_builder);
      });

  coordination_->Register<InstallSnapshotRpc>(
      [this](auto *req_reader, auto *res_builder) {
        // Acquire snapshot lock.
        std::lock_guard<std::mutex> snapshot_guard(snapshot_lock_);
        std::lock_guard<std::mutex> guard(lock_);

        InstallSnapshotReq req;
        slk::Load(&req, req_reader);

        if (exiting_ || req.term < current_term_) {
          InstallSnapshotRes res(current_term_);
          slk::Save(res, res_builder);
          return;
        }

        // Check if the current state matches the one in snapshot
        if (req.snapshot_metadata.last_included_index == last_applied_ &&
            req.snapshot_metadata.last_included_term == current_term_) {
          InstallSnapshotRes res(current_term_);
          slk::Save(res, res_builder);
          return;
        }

        VLOG(40) << "[InstallSnapshotRpc] Starting.";

        if (req.term > current_term_) {
          VLOG(40) << "[InstallSnapshotRpc] Updating term.";
          SetCurrentTerm(req.term);
          if (mode_ != Mode::FOLLOWER) Transition(Mode::FOLLOWER);
        }

        utils::OnScopeExit extend_election_timeout([this] {
          SetNextElectionTimePoint();
          election_change_.notify_all();
        });

        // Temporary freeze election timer while we handle the snapshot.
        next_election_ = TimePoint::max();

        VLOG(40) << "[InstallSnapshotRpc] Remove all snapshots.";
        // Remove all previous snapshots
        durability::RemoveAllSnapshots(durability_dir_);

        const auto snapshot_path = durability::MakeSnapshotPath(
            durability_dir_, req.snapshot_metadata.snapshot_filename);

        // Save snapshot file
        {
          VLOG(40) << "[InstallSnapshotRpc] Saving received snapshot.";
          std::ofstream output_stream;
          output_stream.open(snapshot_path, std::ios::out | std::ios::binary);
          output_stream.write(req.data.data(), req.data.size());
          output_stream.flush();
          output_stream.close();
        }

        // Discard the all logs. We keep the one at index 0.
        VLOG(40) << "[InstallSnapshotRpc] Discarding logs.";
        log_.clear();
        for (uint64_t i = 1; i < log_size_; ++i)
          disk_storage_.Delete(LogEntryKey(i));

        // Reset the database.
        VLOG(40) << "[InstallSnapshotRpc] Reset database.";
        db_->Reset();

        // Apply the snapshot.
        VLOG(40) << "[InstallSnapshotRpc] Recover from received snapshot.";
        RecoverSnapshot(req.snapshot_metadata.snapshot_filename);

        VLOG(40) << "[InstallSnapshotRpc] Persist snapshot metadata.";
        PersistSnapshotMetadata(req.snapshot_metadata);

        // Update the state to match the one from snapshot.
        VLOG(40) << "[InstallSnapshotRpc] Update Raft state.";
        last_applied_ = req.snapshot_metadata.last_included_index;
        commit_index_ = req.snapshot_metadata.last_included_index;
        SetLogSize(req.snapshot_metadata.last_included_index + 1);

        InstallSnapshotRes res(current_term_);
        slk::Save(res, res_builder);
      });

  // start threads

  SetNextElectionTimePoint();
  election_thread_ = std::thread(&RaftServer::ElectionThreadMain, this);

  for (auto peer_id : coordination_->GetOtherNodeIds()) {
    peer_threads_.emplace_back(&RaftServer::PeerThreadMain, this, peer_id);
    hb_threads_.emplace_back(&RaftServer::HBThreadMain, this, peer_id);
  }

  no_op_issuer_thread_ = std::thread(&RaftServer::NoOpIssuerThreadMain, this);

  snapshot_thread_ = std::thread(&RaftServer::SnapshotThread, this);
}

void RaftServer::Shutdown() {
  exiting_ = true;
  {
    std::lock_guard<std::mutex> guard(lock_);

    state_changed_.notify_all();
    election_change_.notify_all();
    leader_changed_.notify_all();
    hb_condition_.notify_all();
  }

  for (auto &peer_thread : peer_threads_) {
    if (peer_thread.joinable()) peer_thread.join();
  }

  for (auto &hb_thread : hb_threads_) {
    if (hb_thread.joinable()) hb_thread.join();
  }

  if (election_thread_.joinable()) election_thread_.join();
  if (no_op_issuer_thread_.joinable()) no_op_issuer_thread_.join();
  if (snapshot_thread_.joinable()) snapshot_thread_.join();
}

void RaftServer::SetCurrentTerm(uint64_t new_current_term) {
  current_term_ = new_current_term;
  disk_storage_.Put(kCurrentTermKey, std::to_string(new_current_term));
  SetVotedFor(std::nullopt);
}

void RaftServer::SetVotedFor(std::optional<uint16_t> new_voted_for) {
  voted_for_ = new_voted_for;
  if (new_voted_for)
    disk_storage_.Put(kVotedForKey, std::to_string(new_voted_for.value()));
  else
    disk_storage_.Delete(kVotedForKey);
}

void RaftServer::SetLogSize(uint64_t new_log_size) {
  log_size_ = new_log_size;
  disk_storage_.Put(kLogSizeKey, std::to_string(new_log_size));
}

std::optional<SnapshotMetadata> RaftServer::GetSnapshotMetadata() {
  auto opt_value = disk_storage_.Get(kSnapshotMetadataKey);
  if (opt_value == std::nullopt) {
    return std::nullopt;
  }

  auto &value = *opt_value;
  slk::Reader reader(reinterpret_cast<const uint8_t *>(value.data()),
                     value.size());
  SnapshotMetadata deserialized;
  try {
    slk::Load(&deserialized, &reader);
    reader.Finalize();
  } catch (const slk::SlkReaderException &) {
    return std::nullopt;
  }
  return std::make_optional(deserialized);
}

void RaftServer::PersistSnapshotMetadata(
    const SnapshotMetadata &snapshot_metadata) {
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  slk::Builder builder(
      [&stream](const uint8_t *data, size_t size, bool have_more) {
        for (size_t i = 0; i < size; ++i) {
          stream << utils::MemcpyCast<char>(data[i]);
        }
      });
  slk::Save(snapshot_metadata, &builder);
  builder.Finalize();
  disk_storage_.Put(kSnapshotMetadataKey, stream.str());

  // Keep the metadata in memory for faster access.
  snapshot_metadata_.emplace(snapshot_metadata);
}

std::pair<std::optional<uint64_t>, std::optional<uint64_t>>
RaftServer::AppendToLog(const tx::TransactionId &tx_id,
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
    return {std::nullopt, std::nullopt};
  }

  rlog_->set_active(tx_id);
  LogEntry new_entry(current_term_, deltas);

  log_[log_size_] = new_entry;
  disk_storage_.Put(LogEntryKey(log_size_), SerializeLogEntry(new_entry));
  last_entry_term_ = new_entry.term;
  SetLogSize(log_size_ + 1);

  // Force replication
  TimePoint now = Clock::now();
  for (auto &peer_replication : next_replication_) peer_replication = now;

  // From this point on, we can say that the replication of a LogEntry started.
  replication_timeout_.Insert(tx_id);

  state_changed_.notify_all();
  return std::make_pair(current_term_.load(), log_size_ - 1);
}

DeltaStatus RaftServer::Emplace(const database::StateDelta &delta) {
  return log_entry_buffer_.Emplace(delta);
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
      // circuit the check to always return true if in follower mode.
      return true;
    case Mode::LEADER:
      // We are taking copies of the rlog_ status here so we avoid the case
      // where the call to `set_replicated` shadows the `active` bit that is
      // checked after the `replicated` bit. It is possible that both `active`
      // and `replicated` are `true` but  since we check `replicated` first this
      // shouldn't be a problem.
      bool active = rlog_->is_active(tx_id);
      bool replicated = rlog_->is_replicated(tx_id);

      // If we are shutting down, but we know that the Raft Log replicated
      // successfully, we return true. This will eventually commit since we
      // replicate NoOp on leader election.
      if (replicated) return true;

      // Only if the transaction isn't replicated, thrown an exception to inform
      // the client.
      if (exiting_) throw RaftShutdownException();

      if (active) {
        if (replication_timeout_.CheckTimeout(tx_id)) {
          throw ReplicationTimeoutException();
        }
        return false;
      }

      // The only possibility left is that our ReplicationLog doesn't contain
      // information about that tx.
      throw InvalidReplicationLogLookup();
      break;
  }
}

void RaftServer::GarbageCollectReplicationLog(const tx::TransactionId &tx_id) {
  rlog_->garbage_collect_older(tx_id);
}

bool RaftServer::IsLeader() { return !exiting_ && mode_ == Mode::LEADER; }

uint64_t RaftServer::TermId() { return current_term_; }

TxStatus RaftServer::TransactionStatus(uint64_t term_id, uint64_t log_index) {
  std::unique_lock<std::mutex> lock(lock_);
  if (term_id > current_term_ || log_index >= log_size_)
    return TxStatus::INVALID;

  auto log_entry = GetLogEntry(log_index);

  // This is correct because the leader can only append to the log and no two
  // workers can be leaders in the same term.
  if (log_entry.term != term_id) return TxStatus::ABORTED;

  if (last_applied_ < log_index) return TxStatus::WAITING;
  return TxStatus::REPLICATED;
}

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

DeltaStatus RaftServer::LogEntryBuffer::Emplace(
    const database::StateDelta &delta) {
  std::unique_lock<std::mutex> lock(buffer_lock_);
  if (!enabled_) return {false, std::nullopt, std::nullopt};

  tx::TransactionId tx_id = delta.transaction_id;

  std::optional<uint64_t> term_id = std::nullopt;
  std::optional<uint64_t> log_index = std::nullopt;

  if (delta.type == database::StateDelta::Type::TRANSACTION_COMMIT) {
    auto it = logs_.find(tx_id);
    CHECK(it != logs_.end()) << "Missing StateDeltas for transaction " << tx_id;

    std::vector<database::StateDelta> log(std::move(it->second));
    log.emplace_back(std::move(delta));
    logs_.erase(it);

    lock.unlock();
    auto metadata = raft_server_->AppendToLog(tx_id, log);
    term_id = metadata.first;
    log_index = metadata.second;

  } else if (delta.type == database::StateDelta::Type::TRANSACTION_ABORT) {
    auto it = logs_.find(tx_id);
    // Sometimes it's possible that we're aborting a transaction that was meant
    // to be commited and thus we don't have the StateDeltas anymore.
    if (it != logs_.end()) logs_.erase(it);
  } else {
    logs_[tx_id].emplace_back(std::move(delta));
  }

  return {true, term_id, log_index};
}

void RaftServer::RecoverPersistentData() {
  auto opt_term = disk_storage_.Get(kCurrentTermKey);
  if (opt_term) current_term_ = std::stoull(opt_term.value());

  auto opt_voted_for = disk_storage_.Get(kVotedForKey);
  if (!opt_voted_for) {
    voted_for_ = std::nullopt;
  } else {
    voted_for_ = {std::stoul(opt_voted_for.value())};
  }

  auto opt_log_size = disk_storage_.Get(kLogSizeKey);
  if (opt_log_size) log_size_ = std::stoull(opt_log_size.value());

  if (log_size_ != 0) {
    auto opt_last_log_entry = disk_storage_.Get(LogEntryKey(log_size_ - 1));
    DCHECK(opt_last_log_entry != std::nullopt)
        << "Log size is equal to " << log_size_
        << ", but there is no log entry on index: " << log_size_ - 1;
    last_entry_term_ = DeserializeLogEntry(opt_last_log_entry.value()).term;
  }
}

void RaftServer::Transition(const Mode &new_mode) {
  switch (new_mode) {
    case Mode::FOLLOWER: {
      LOG(INFO) << "Server " << server_id_
                << ": Transition to FOLLOWER (Term: " << current_term_ << ")";

      bool reset = mode_ == Mode::LEADER;
      issue_hb_ = false;
      mode_ = Mode::FOLLOWER;
      log_entry_buffer_.Disable();

      if (reset) {
        VLOG(40) << "Resetting internal state";
        // Temporary freeze election timer while we do the reset.
        next_election_ = TimePoint::max();

        db_->Reset();
        ResetReplicationLog();
        replication_timeout_.Clear();

        // Re-apply raft log.
        uint64_t starting_index = 1;
        if (snapshot_metadata_) {
          RecoverSnapshot(snapshot_metadata_->snapshot_filename);
          starting_index = snapshot_metadata_->last_included_index + 1;
        }

        for (uint64_t i = starting_index; i <= commit_index_; ++i) {
          ApplyStateDeltas(GetLogEntry(i).deltas);
        }

        last_applied_ = commit_index_;
      }

      SetNextElectionTimePoint();
      election_change_.notify_all();
      state_changed_.notify_all();
      break;
    }

    case Mode::CANDIDATE: {
      LOG(INFO) << "Server " << server_id_
                << ": Transition to CANDIDATE (Term: " << current_term_ << ")";

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
      SetCurrentTerm(current_term_ + 1);
      SetVotedFor(server_id_);

      granted_votes_ = 1;
      vote_requested_.assign(coordination_->GetAllNodeCount() + 1, false);

      issue_hb_ = false;
      mode_ = Mode::CANDIDATE;
      state_changed_.notify_all();

      break;
    }

    case Mode::LEADER: {
      LOG(INFO) << "Server " << server_id_
                << ": Transition to LEADER (Term: " << current_term_ << ")";
      // Freeze election timer
      next_election_ = TimePoint::max();
      election_change_.notify_all();

      // Set next heartbeat and replication to correct values
      TimePoint now = Clock::now();
      for (auto &peer_replication : next_replication_)
        peer_replication = now + config_.heartbeat_interval;
      for (auto &peer_heartbeat : next_heartbeat_)
        peer_heartbeat = now + config_.heartbeat_interval;

      issue_hb_ = true;
      hb_condition_.notify_all();

      // [Raft paper figure 2]
      // "For each server, index of the next log entry to send to that server
      // is initialized to leader's last log index + 1"
      for (int i = 1; i <= coordination_->GetAllNodeCount(); ++i) {
        next_index_[i] = log_size_;
        index_offset_[i] = 1;
        match_index_[i] = 0;
      }

      // Raft guarantees the Leader Append-Only property [Raft paper 5.2]
      // so its safe to apply everything from our log into our state machine
      for (int i = last_applied_ + 1; i < log_size_; ++i)
        ApplyStateDeltas(GetLogEntry(i).deltas);
      last_applied_ = log_size_ - 1;

      mode_ = Mode::LEADER;
      log_entry_buffer_.Enable();

      leader_changed_.notify_all();
      break;
    }
  }
}

void RaftServer::AdvanceCommitIndex() {
  DCHECK(mode_ == Mode::LEADER)
      << "Commit index can only be advanced by the leader";

  std::vector<uint64_t> known_replication_indices;
  for (int i = 1; i <= coordination_->GetAllNodeCount(); ++i) {
    if (i != server_id_)
      known_replication_indices.push_back(match_index_[i]);
    else
      known_replication_indices.push_back(log_size_ - 1);
  }

  std::sort(known_replication_indices.begin(), known_replication_indices.end());
  uint64_t new_commit_index =
      known_replication_indices[(coordination_->GetAllNodeCount() - 1) / 2];

  // This can happen because we reset `match_index` vector to 0 after a
  // new leader has been elected.
  if (commit_index_ >= new_commit_index) return;

  // [Raft thesis, section 3.6.2]
  // "(...) Raft never commits log entries from previous terms by counting
  // replicas. Only log entries from the leader's current term are committed by
  // counting replicas; once an entry from the current term has been committed
  // in this way, then all prior entries are committed indirectly because of the
  // Log Matching Property."
  if (GetLogEntry(new_commit_index).term != current_term_) {
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
    auto tx_id = deltas[0].transaction_id;
    rlog_->set_replicated(tx_id);
    replication_timeout_.Remove(tx_id);
  }

  commit_index_ = new_commit_index;
  last_applied_ = new_commit_index;
}

void RaftServer::SendEntries(uint16_t peer_id,
                             std::unique_lock<std::mutex> *lock) {
  if (snapshot_metadata_ &&
      snapshot_metadata_->last_included_index >= next_index_[peer_id]) {
    SendSnapshot(peer_id, *snapshot_metadata_, lock);
  } else {
    SendLogEntries(peer_id, snapshot_metadata_, lock);
  }
}

void RaftServer::SendLogEntries(
    uint16_t peer_id, const std::optional<SnapshotMetadata> &snapshot_metadata,
    std::unique_lock<std::mutex> *lock) {
  uint64_t request_term = current_term_;
  uint64_t request_prev_log_index = next_index_[peer_id] - 1;
  uint64_t request_prev_log_term;

  if (snapshot_metadata &&
      snapshot_metadata->last_included_index == next_index_[peer_id] - 1) {
    request_prev_log_term = snapshot_metadata->last_included_term;
  } else {
    request_prev_log_term = GetLogEntry(next_index_[peer_id] - 1).term;
  }

  std::vector<LogEntry> request_entries;
  if (next_index_[peer_id] <= log_size_ - 1)
    GetLogSuffix(next_index_[peer_id], request_entries);

  // Copy all internal variables before releasing the lock.
  auto server_id = server_id_;
  auto commit_index = commit_index_;

  VLOG(40) << "Server " << server_id_
           << ": Sending Entries RPC to server " << peer_id
           << " (Term: " << current_term_ << ")";
  VLOG(40) << "Entries size: " << request_entries.size();

  // Execute the RPC.
  lock->unlock();
  auto reply = coordination_->ExecuteOnOtherNode<AppendEntriesRpc>(
      peer_id, server_id, commit_index, request_term, request_prev_log_index,
      request_prev_log_term, request_entries);
  lock->lock();

  if (!reply) {
    next_replication_[peer_id] = Clock::now() + config_.heartbeat_interval;
    return;
  }

  // We can't early exit if the `exiting_` flag is true just yet. It is possible
  // that the response we handle here carries the last confirmation that the logs
  // have been replicated. We need to handle the response so the client doesn't
  // retry the query because he thinks the query failed.
  if (current_term_ != request_term || mode_ != Mode::LEADER) {
    return;
  }

  if (OutOfSync(reply->term)) {
    state_changed_.notify_all();
    return;
  }

  DCHECK(mode_ == Mode::LEADER)
      << "Elected leader for term should never change.";

  if (reply->term != current_term_) {
    VLOG(40) << "Server " << server_id_
             << ": Ignoring stale AppendEntriesRPC reply from " << peer_id;
    return;
  }

  if (!reply->success) {
    // Replication can fail for the first log entry if the peer that we're
    // sending the entry is in the process of shutting down.
    if (next_index_[peer_id] > index_offset_[peer_id]) {
      next_index_[peer_id] -= index_offset_[peer_id];
      // Overflow should be prevented by snapshot threshold constant.
      index_offset_[peer_id] <<= 1UL;
    } else {
      next_index_[peer_id] = 1UL;
    }
  } else {
    uint64_t new_match_index = request_prev_log_index + request_entries.size();
    DCHECK(match_index_[peer_id] <= new_match_index)
        << "`match_index` should increase monotonically within a term";
    match_index_[peer_id] = new_match_index;
    if (request_entries.size() > 0) AdvanceCommitIndex();
    next_index_[peer_id] = match_index_[peer_id] + 1;
    index_offset_[peer_id] = 1;
    next_replication_[peer_id] = Clock::now() + config_.heartbeat_interval;
  }

  if (exiting_) return;
  state_changed_.notify_all();
}

void RaftServer::SendSnapshot(uint16_t peer_id,
                              const SnapshotMetadata &snapshot_metadata,
                              std::unique_lock<std::mutex> *lock) {
  uint64_t request_term = current_term_;
  std::string snapshot_data;

  // TODO: The snapshot is currently sent all at once. Because the snapshot file
  // can be extremely large (>100GB, it contains the whole database) it must be
  // sent out in chunks! Reimplement this logic so that it sends out the
  // snapshot in chunks.

  {
    const auto snapshot_path = durability::MakeSnapshotPath(
        durability_dir_, snapshot_metadata.snapshot_filename);

    std::ifstream input_stream;
    input_stream.open(snapshot_path, std::ios::in | std::ios::binary);
    input_stream.seekg(0, std::ios::end);
    uint64_t snapshot_size = input_stream.tellg();

    snapshot_data = std::string(snapshot_size, '\0');

    input_stream.seekg(0, std::ios::beg);
    input_stream.read(snapshot_data.data(), snapshot_size);
    input_stream.close();
  }

  VLOG(40) << "Server " << server_id_
           << ": Sending Snapshot RPC to server " << peer_id
           << " (Term: " << current_term_ << ")";
  VLOG(40) << "Snapshot size: " << snapshot_data.size() << " bytes.";

  // Copy all internal variables before releasing the lock.
  auto server_id = server_id_;

  // Execute the RPC.
  lock->unlock();
  auto reply = coordination_->ExecuteOnOtherNode<InstallSnapshotRpc>(
      peer_id, server_id, request_term, snapshot_metadata,
      std::move(snapshot_data));
  lock->lock();

  if (!reply) {
    next_replication_[peer_id] = Clock::now() + config_.heartbeat_interval;
    return;
  }

  if (current_term_ != request_term || mode_ != Mode::LEADER || exiting_) {
    return;
  }

  if (OutOfSync(reply->term)) {
    state_changed_.notify_all();
    return;
  }

  if (reply->term != current_term_) {
    VLOG(40) << "Server " << server_id_
             << ": Ignoring stale InstallSnapshotRpc reply from " << peer_id;
    return;
  }

  match_index_[peer_id] = snapshot_metadata.last_included_index;
  next_index_[peer_id] = snapshot_metadata.last_included_index + 1;
  index_offset_[peer_id] = 1;
  next_replication_[peer_id] = Clock::now() + config_.heartbeat_interval;

  state_changed_.notify_all();
}

void RaftServer::ElectionThreadMain() {
  utils::ThreadSetName("ElectionThread");
  std::unique_lock<std::mutex> lock(lock_);
  while (!exiting_) {
    if (Clock::now() >= next_election_) {
      VLOG(40) << "Server " << server_id_
               << ": Election timeout exceeded (Term: " << current_term_ << ")";
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

    switch (mode_) {
      case Mode::FOLLOWER: {
        wait_until = TimePoint::max();
        break;
      }

      case Mode::CANDIDATE: {
        if (vote_requested_[peer_id]) {
          wait_until = TimePoint::max();
          break;
        }

        // TODO(ipaljak): Consider backoff.
        wait_until = TimePoint::max();

        // Copy all internal variables before releasing the lock.
        auto server_id = server_id_;
        auto request_term = current_term_.load();
        auto last_entry_data = LastEntryData();

        vote_requested_[peer_id] = true;

        // Execute the RPC.
        lock.unlock();  // Release lock while waiting for response
        auto reply = coordination_->ExecuteOnOtherNode<RequestVoteRpc>(
            peer_id, server_id, request_term, last_entry_data.first,
            last_entry_data.second);
        lock.lock();

        // If the peer isn't reachable, it is the same as if he didn't grant
        // us his vote.
        if (!reply) {
          reply = RequestVoteRes(false, request_term);
        }

        if (current_term_ != request_term || mode_ != Mode::CANDIDATE ||
            exiting_) {
          VLOG(40) << "Server " << server_id_
                   << ": Ignoring RequestVoteRPC reply from " << peer_id;
          break;
        }

        if (OutOfSync(reply->term)) {
          state_changed_.notify_all();
          continue;
        }

        if (reply->vote_granted) {
          VLOG(40) << "Server " << server_id_ << ": Got vote from "
                   << peer_id;
          ++granted_votes_;
          if (HasMajorityVote()) Transition(Mode::LEADER);
        } else {
          VLOG(40) << "Server " << server_id_ << ": Denied vote from "
                   << peer_id;
        }

        state_changed_.notify_all();
        continue;
      }

      case Mode::LEADER: {
        if (now >= next_replication_[peer_id]) {
          SendEntries(peer_id, &lock);
          continue;
        }
        wait_until = next_replication_[peer_id];
        break;
      }
    }

    if (exiting_) break;
    state_changed_.wait_until(lock, wait_until);
  }
}

void RaftServer::HBThreadMain(uint16_t peer_id) {
  utils::ThreadSetName(fmt::format("HBThread{}", peer_id));
  std::unique_lock<std::mutex> lock(heartbeat_lock_);

  // The heartbeat thread uses a dedicated RPC client for its peer so that it
  // can issue heartbeats in parallel with other RPC requests that are being
  // issued to the peer (replication, voting, etc.)
  std::unique_ptr<communication::rpc::Client> rpc_client;

  while (!exiting_) {
    TimePoint wait_until;

    if (!issue_hb_) {
      wait_until = TimePoint::max();
    } else {
      TimePoint now = Clock::now();
      if (now < next_heartbeat_[peer_id]) {
        wait_until = next_heartbeat_[peer_id];
      } else {
        VLOG(40) << "Server " << server_id_ << ": Sending HB to server "
                 << peer_id << " (Term: " << current_term_ << ")";

        lock.unlock();
        if (!rpc_client) {
          rpc_client = std::make_unique<communication::rpc::Client>(
              coordination_->GetOtherNodeEndpoint(peer_id),
              coordination_->GetRpcClientContext());
        }
        try {
          rpc_client->Call<HeartbeatRpc>(server_id_, current_term_);
        } catch (...) {
          // Invalidate the client so that we reconnect next time.
          rpc_client = nullptr;
        }
        lock.lock();

        // This is ok even if we don't receive a reply.
        next_heartbeat_[peer_id] = now + config_.heartbeat_interval;
        wait_until = next_heartbeat_[peer_id];
      }
    }

    if (exiting_) break;
    hb_condition_.wait_until(lock, wait_until);
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
  if (config_.log_size_snapshot_threshold == -1) return;

  while (true) {
    {
      // Acquire snapshot lock before we acquire the Raft lock. This should
      // avoid the situation where we release the lock to start writing a
      // snapshot but the `InstallSnapshotRpc` deletes it and we write wrong
      // metadata.
      std::lock_guard<std::mutex> snapshot_guard(snapshot_lock_);
      std::unique_lock<std::mutex> lock(lock_);
      if (exiting_) break;

      uint64_t committed_log_size = last_applied_;
      if (snapshot_metadata_) {
        committed_log_size -= snapshot_metadata_->last_included_index;
      }

      // Compare the log size to the config
      if (config_.log_size_snapshot_threshold < committed_log_size) {
        VLOG(40) << "[LogCompaction] Starting log compaction.";

        uint64_t last_included_term = 0;
        uint64_t last_included_index = 0;
        std::string snapshot_filename;
        bool status = false;

        {
          // Create a DB accessor for snapshot creation
          auto dba = db_->Access();
          last_included_term = GetLogEntry(last_applied_).term;
          last_included_index = last_applied_;
          snapshot_filename = durability::GetSnapshotFilename(
              last_included_term, last_included_index);

          lock.unlock();
          VLOG(40) << "[LogCompaction] Creating snapshot.";
          status = durability::MakeSnapshot(*db_, dba, durability_dir_,
                                            snapshot_filename);

          // Raft lock must be released when destroying dba object.
          // Destroy the db accessor
        }

        lock.lock();

        if (status) {
          uint64_t log_compaction_start_index = 1;
          if (snapshot_metadata_) {
            log_compaction_start_index =
                snapshot_metadata_->last_included_index + 1;
          }

          VLOG(40) << "[LogCompaction] Persisting snapshot metadata";
          PersistSnapshotMetadata(
              {last_included_term, last_included_index, snapshot_filename});

          // Log compaction.
          VLOG(40) << "[LogCompaction] Compacting log from "
                   << log_compaction_start_index << " to "
                   << last_included_index;
          for (int i = log_compaction_start_index; i <= last_included_index;
               ++i) {
            log_.erase(i);
            disk_storage_.Delete(LogEntryKey(i));
          }
          // After we deleted entries from the persistent store, make sure we
          // compact the files and actually reduce used disk space.
          disk_storage_.CompactRange(LogEntryKey(log_compaction_start_index),
                                     LogEntryKey(last_included_index));
        }
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

bool RaftServer::HasMajorityVote() {
  if (2 * granted_votes_ > coordination_->GetAllNodeCount()) {
    VLOG(40) << "Server " << server_id_
             << ": Obtained majority vote (Term: " << current_term_ << ")";
    return true;
  }
  return false;
}

std::pair<uint64_t, uint64_t> RaftServer::LastEntryData() {
  if (snapshot_metadata_ &&
      snapshot_metadata_->last_included_index == log_size_ - 1) {
    return {log_size_, snapshot_metadata_->last_included_term};
  }
  return {log_size_, last_entry_term_};
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
  if (current_term_ < reply_term) {
    disk_storage_.Put(kCurrentTermKey, std::to_string(reply_term));
    disk_storage_.Delete(kVotedForKey);
    granted_votes_ = 0;
    Transition(Mode::FOLLOWER);
    return true;
  }
  return false;
}

LogEntry RaftServer::GetLogEntry(int index) {
  auto it = log_.find(index);
  if (it != log_.end())
    return it->second; // retrieve in-mem if possible
  auto opt_value = disk_storage_.Get(LogEntryKey(index));
  DCHECK(opt_value != std::nullopt)
      << "Log index (" << index << ") out of bounds.";
  return DeserializeLogEntry(opt_value.value());
}

void RaftServer::DeleteLogSuffix(int starting_index) {
  DCHECK(0 <= starting_index && starting_index < log_size_)
      << "Log index out of bounds.";
  for (int i = starting_index; i < log_size_; ++i) {
    log_.erase(i);
    disk_storage_.Delete(LogEntryKey(i));
  }
  SetLogSize(starting_index);
}

void RaftServer::GetLogSuffix(int starting_index,
                              std::vector<raft::LogEntry> &entries) {
  DCHECK(0 <= starting_index && starting_index < log_size_)
      << "Log index out of bounds.";
  for (int i = starting_index; i < log_size_; ++i)
    entries.push_back(GetLogEntry(i));
}

void RaftServer::AppendLogEntries(uint64_t leader_commit_index,
                                  uint64_t starting_index,
                                  const std::vector<LogEntry> &new_entries) {
  for (int i = 0; i < new_entries.size(); ++i) {
    // If existing entry conflicts with new one, we need to delete the
    // existing entry and all that follow it.
    int current_index = i + starting_index;
    if (log_size_ > current_index &&
        GetLogEntry(current_index).term != new_entries[i].term) {
      DeleteLogSuffix(current_index);
    }
    DCHECK(log_size_ >= current_index) << "Current Log index out of bounds.";
    if (log_size_ == current_index) {
      log_[log_size_] = new_entries[i];
      disk_storage_.Put(LogEntryKey(log_size_),
                        SerializeLogEntry(new_entries[i]));
      last_entry_term_ = new_entries[i].term;
      SetLogSize(log_size_ + 1);
    }
  }

  // See Raft paper 5.3
  if (leader_commit_index > commit_index_) {
    commit_index_ = std::min(leader_commit_index, log_size_ - 1);
  }
}

std::string RaftServer::LogEntryKey(uint64_t index) {
  return kLogEntryPrefix + std::to_string(index);
}

std::string RaftServer::SerializeLogEntry(const LogEntry &log_entry) {
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  slk::Builder builder(
      [&stream](const uint8_t *data, size_t size, bool have_more) {
        for (size_t i = 0; i < size; ++i) {
          stream << utils::MemcpyCast<char>(data[i]);
        }
      });
  slk::Save(log_entry, &builder);
  builder.Finalize();
  return stream.str();
}

LogEntry RaftServer::DeserializeLogEntry(
    const std::string &serialized_log_entry) {
  slk::Reader reader(
      reinterpret_cast<const uint8_t *>(serialized_log_entry.data()),
      serialized_log_entry.size());
  LogEntry deserialized;
  try {
    slk::Load(&deserialized, &reader);
    reader.Finalize();
  } catch (const slk::SlkReaderException &) {
    LOG(FATAL) << "Couldn't load log from disk storage!";
  }
  return deserialized;
}

void RaftServer::ResetReplicationLog() {
  rlog_ = nullptr;
  rlog_ = std::make_unique<ReplicationLog>();
}

void RaftServer::RecoverSnapshot(const std::string &snapshot_filename) {
  durability::RecoveryData recovery_data;
  bool recovery = durability::RecoverSnapshot(
      db_, &recovery_data, durability_dir_, snapshot_filename);

  CHECK(recovery);
  durability::RecoverIndexes(db_, recovery_data.indexes);
}

void RaftServer::NoOpCreate() {
  auto dba = db_->Access();
  Emplace(database::StateDelta::NoOp(dba.transaction_id()));
  try {
    dba.Commit();
  } catch (const RaftException &) {
    // NoOp failure can be ignored.
    return;
  }
}

void RaftServer::ApplyStateDeltas(
    const std::vector<database::StateDelta> &deltas) {
  std::optional<database::GraphDbAccessor> dba;
  for (auto &delta : deltas) {
    switch (delta.type) {
      case database::StateDelta::Type::TRANSACTION_BEGIN:
        CHECK(!dba) << "Double transaction start";
        dba = db_->Access();
        break;
      case database::StateDelta::Type::TRANSACTION_COMMIT:
        CHECK(dba) << "Missing accessor for transaction"
                   << delta.transaction_id;
        dba->Commit();
        dba = std::nullopt;
        break;
      case database::StateDelta::Type::TRANSACTION_ABORT:
        LOG(FATAL) << "ApplyStateDeltas shouldn't know about aborted "
                      "transactions";
        break;
      default:
        CHECK(dba) << "Missing accessor for transaction"
                   << delta.transaction_id;
        delta.Apply(*dba);
    }
  }
  CHECK(!dba) << "StateDeltas missing commit command";
}

std::mutex &RaftServer::WithLock() { return lock_; }

}  // namespace raft
