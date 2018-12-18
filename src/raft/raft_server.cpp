#include "raft/raft_server.hpp"

#include <kj/std/iostream.h>
#include <experimental/filesystem>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "raft/exceptions.hpp"
#include "utils/exceptions.hpp"
#include "utils/serialization.hpp"

namespace raft {

namespace fs = std::experimental::filesystem;

const std::string kCurrentTermKey = "current_term";
const std::string kVotedForKey = "voted_for";
const std::string kLogKey = "log";
const std::string kRaftDir = "raft";

RaftServer::RaftServer(uint16_t server_id, const std::string &durability_dir,
                       const Config &config, Coordination *coordination,
                       database::StateDeltaApplier *delta_applier,
                       std::function<void(void)> reset_callback)
    : config_(config),
      coordination_(coordination),
      delta_applier_(delta_applier),
      rlog_(std::make_unique<ReplicationLog>()),
      mode_(Mode::FOLLOWER),
      server_id_(server_id),
      disk_storage_(fs::path(durability_dir) / kRaftDir),
      reset_callback_(reset_callback) {}

void RaftServer::Start() {
  // Persistent storage initialization/recovery.
  if (Log().empty()) {
    UpdateTerm(0);
  } else {
    Recover();
  }

  // Peer state
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
        if (req.term < current_term) {
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
        RequestVoteRes res(
            (!voted_for || voted_for.value() == req.candidate_id) &&
                AtLeastUpToDate(req.last_log_index, req.last_log_term,
                                last_entry_data.first, last_entry_data.second),
            current_term);
        Save(res, res_builder);
      });

  coordination_->Register<AppendEntriesRpc>([this](const auto &req_reader,
                                                   auto *res_builder) {
    std::lock_guard<std::mutex> guard(lock_);
    AppendEntriesReq req;
    Load(&req, req_reader);

    // [Raft paper 5.1]
    // "If a server recieves a request with a stale term,
    // it rejects the request"
    uint64_t current_term = CurrentTerm();
    if (req.term < current_term) {
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

    // respond positively to a heartbeat.
    // TODO(ipaljak) review this when implementing log replication.
    if (req.entries.empty()) {
      AppendEntriesRes res(true, current_term);
      Save(res, res_builder);
      if (mode_ != Mode::FOLLOWER) {
        Transition(Mode::FOLLOWER);
      } else {
        SetNextElectionTimePoint();
        election_change_.notify_all();
      }
      return;
    }

    throw utils::NotYetImplemented("AppendEntriesRpc which is not a heartbeat");

    // [Raft paper 5.3]
    // "If a follower's log is inconsistent with the leader's, the
    // consistency check will fail in the next AppendEntries RPC."
    //
    // Consistency checking assures the Log Matching Property:
    //   - If two entries in different logs have the same index and
    //     term, then they store the same command.
    //   - If two entries in different logs have the same index and term,
    //     then the logs are identical in all preceding entries.
    auto log = Log();
    if (log.size() < req.prev_log_index ||
        log[req.prev_log_index - 1].term != req.prev_log_term) {
      AppendEntriesRes res(false, current_term);
      Save(res, res_builder);
      return;
    }

    // If existing entry conflicts with new one, we need to delete the
    // existing entry and all that follow it.
    if (log.size() > req.prev_log_index &&
        log[req.prev_log_index].term != req.entries[0].term)
      DeleteLogSuffix(req.prev_log_index + 1);

    AppendLogEntries(req.leader_commit, req.entries);
    AppendEntriesRes res(true, current_term);
    Save(res, res_builder);
  });

  SetNextElectionTimePoint();
  election_thread_ = std::thread(&RaftServer::ElectionThreadMain, this);

  for (const auto &peer_id : coordination_->GetWorkerIds()) {
    if (peer_id == server_id_) continue;
    peer_threads_.emplace_back(&RaftServer::PeerThreadMain, this, peer_id);
  }
}

void RaftServer::Shutdown() {
  {
    std::lock_guard<std::mutex> guard(lock_);
    exiting_ = true;

    state_changed_.notify_all();
    election_change_.notify_all();
  }

  for (auto &peer_thread : peer_threads_) {
    if (peer_thread.joinable()) peer_thread.join();
  }

  if (election_thread_.joinable()) election_thread_.join();
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

std::vector<LogEntry> RaftServer::Log() {
  auto opt_value = disk_storage_.Get(kLogKey);
  if (opt_value == std::experimental::nullopt) {
    disk_storage_.Put(kLogKey, SerializeLog({}));
    return {};
  }
  return DeserializeLog(opt_value.value());
}

void RaftServer::AppendToLog(const tx::TransactionId &tx_id,
                             const std::vector<database::StateDelta> &log) {
  rlog_->set_active(tx_id);
  throw utils::NotYetImplemented("RaftServer replication");
}

void RaftServer::Emplace(const database::StateDelta &delta) {
  log_entry_buffer_.Emplace(delta);
}

bool RaftServer::SafeToCommit(const tx::TransactionId &tx_id) {
  std::unique_lock<std::mutex> lock(lock_);

  switch (mode_) {
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
      // information about that tx. Let's log that on our way out.
      break;
    case Mode::CANDIDATE:
      break;
  }

  throw InvalidReplicationLogLookup();
}

RaftServer::LogEntryBuffer::LogEntryBuffer(RaftServer *raft_server)
    : raft_server_(raft_server) {
  CHECK(raft_server_) << "RaftServer can't be nullptr";
}

void RaftServer::LogEntryBuffer::Enable() {
  std::lock_guard<std::mutex> guard(lock_);
  enabled_ = true;
}

void RaftServer::LogEntryBuffer::Disable() {
  std::lock_guard<std::mutex> guard(lock_);
  enabled_ = false;
  // Clear all existing logs from buffers.
  logs_.clear();
}

void RaftServer::LogEntryBuffer::Emplace(const database::StateDelta &delta) {
  std::lock_guard<std::mutex> guard(lock_);
  if (!enabled_) return;

  tx::TransactionId tx_id = delta.transaction_id;
  if (delta.type == database::StateDelta::Type::TRANSACTION_COMMIT) {
    auto it = logs_.find(tx_id);
    CHECK(it != logs_.end()) << "Missing StateDeltas for transaction " << tx_id;

    std::vector<database::StateDelta> log(std::move(it->second));
    log.emplace_back(std::move(delta));
    logs_.erase(it);

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
      if (mode_ == Mode::LEADER) {
        reset_callback_();
        ResetReplicationLog();
      }
      LOG(INFO) << "Server " << server_id_
                << ": Transition to FOLLOWER (Term: " << CurrentTerm() << ")";
      mode_ = Mode::FOLLOWER;
      log_entry_buffer_.Disable();
      SetNextElectionTimePoint();
      election_change_.notify_all();
      break;
    }

    case Mode::CANDIDATE: {
      LOG(INFO) << "Server " << server_id_
                << ": Transition to CANDIDATE (Term: " << CurrentTerm() << ")";
      log_entry_buffer_.Disable();

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
      LOG(INFO) << "Server " << server_id_
                << ": Transition to LEADER (Term: " << CurrentTerm() << ")";
      log_entry_buffer_.Enable();

      // Freeze election timer
      next_election_ = TimePoint::max();
      election_change_.notify_all();

      // Set next heartbeat to correct values
      TimePoint now = Clock::now();
      for (auto &peer_heartbeat : next_heartbeat_)
        peer_heartbeat = now + config_.heartbeat_interval;

      mode_ = Mode::LEADER;

      // TODO(ipaljak): Implement no-op replication. For now, we are only
      //                sending heartbeats.
      break;
    }
  }
}

void RaftServer::UpdateTerm(uint64_t new_term) {
  disk_storage_.Put(kCurrentTermKey, std::to_string(new_term));
  disk_storage_.Delete(kVotedForKey);
}

void RaftServer::Recover() {
  throw utils::NotYetImplemented("RaftServer recover");
}

void RaftServer::ElectionThreadMain() {
  std::unique_lock<std::mutex> lock(lock_);
  while (!exiting_) {
    if (Clock::now() >= next_election_) {
      LOG(INFO) << "Server " << server_id_
                << ": Election timeout exceeded (Term: " << CurrentTerm()
                << ")";
      Transition(Mode::CANDIDATE);
      state_changed_.notify_all();
    }
    election_change_.wait_until(lock, next_election_);
  }
}

void RaftServer::PeerThreadMain(int peer_id) {
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
            LOG(INFO) << "Server " << server_id_
                      << ": Ignoring RequestVoteRPC reply from " << peer_id;
            break;
          }

          if (OutOfSync(reply.term)) {
            state_changed_.notify_all();
            continue;
          }

          vote_requested_[peer_id] = true;

          if (reply.vote_granted) {
            LOG(INFO) << "Server " << server_id_ << ": Got vote from "
                      << peer_id;
            ++granted_votes_;
            if (HasMajortyVote()) Transition(Mode::LEADER);
          } else {
            LOG(INFO) << "Server " << server_id_ << ": Denied vote from "
                      << peer_id;
          }

          state_changed_.notify_all();
          continue;
        }

        case Mode::LEADER: {
          if (now >= next_heartbeat_[peer_id]) {
            LOG(INFO) << "Server " << server_id_ << ": Send HB to server "
                      << peer_id << " (Term: " << CurrentTerm() << ")";
            auto peer_future = coordination_->ExecuteOnWorker<AppendEntriesRes>(
                peer_id, [&](int worker_id, auto &client) {
                  auto last_entry_data = LastEntryData();
                  std::vector<raft::LogEntry> empty_entries;
                  auto res = client.template Call<AppendEntriesRpc>(
                      server_id_, commit_index_, CurrentTerm(),
                      last_entry_data.first, last_entry_data.second,
                      empty_entries);
                  return res;
                });
            next_heartbeat_[peer_id] =
                Clock::now() + config_.heartbeat_interval;
            state_changed_.notify_all();
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
    LOG(INFO) << "Server " << server_id_
              << ": Obtained majority vote (Term: " << CurrentTerm() << ")";
    return true;
  }
  return false;
}

std::pair<uint64_t, uint64_t> RaftServer::LastEntryData() {
  auto log = Log();
  if (log.empty()) return {0, 0};
  return {log.size(), log.back().term};
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

void RaftServer::DeleteLogSuffix(int starting_index) {
  auto log = Log();
  log.erase(log.begin() + starting_index - 1, log.end());  // 1-indexed
  disk_storage_.Put(kLogKey, SerializeLog(log));
}

void RaftServer::AppendLogEntries(uint64_t leader_commit_index,
                                  const std::vector<LogEntry> &new_entries) {
  auto log = Log();
  for (auto &entry : new_entries) log.emplace_back(entry);
  // See Raft paper 5.3
  if (leader_commit_index > commit_index_) {
    commit_index_ = std::min(leader_commit_index, log.size() - 1);
  }
  disk_storage_.Put(kLogKey, SerializeLog(log));
}

std::string RaftServer::SerializeLog(const std::vector<LogEntry> &log) {
  ::capnp::MallocMessageBuilder message;
  auto log_builder =
      message.initRoot<::capnp::List<capnp::LogEntry>>(log.size());
  utils::SaveVector<capnp::LogEntry, LogEntry>(
      log, &log_builder, [](auto *log_builder, const auto &log_entry) {
        Save(log_entry, log_builder);
      });

  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  kj::std::StdOutputStream std_stream(stream);
  kj::BufferedOutputStreamWrapper buffered_stream(std_stream);
  writeMessage(buffered_stream, message);
  return stream.str();
}

std::vector<LogEntry> RaftServer::DeserializeLog(
    const std::string &serialized_log) {
  if (serialized_log.empty()) return {};
  ::capnp::MallocMessageBuilder message;
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  kj::std::StdInputStream std_stream(stream);
  kj::BufferedInputStreamWrapper buffered_stream(std_stream);
  stream << serialized_log;
  readMessageCopy(buffered_stream, message);

  ::capnp::List<capnp::LogEntry>::Reader log_reader =
      message.getRoot<::capnp::List<capnp::LogEntry>>().asReader();
  std::vector<LogEntry> deserialized_log;
  utils::LoadVector<capnp::LogEntry, LogEntry>(&deserialized_log, log_reader,
                                               [](const auto &log_reader) {
                                                 LogEntry log_entry;
                                                 Load(&log_entry, log_reader);
                                                 return log_entry;
                                               });
  return deserialized_log;
}

void RaftServer::GarbageCollectReplicationLog(const tx::TransactionId &tx_id) {
  rlog_->garbage_collect_older(tx_id);
}

}  // namespace raft
