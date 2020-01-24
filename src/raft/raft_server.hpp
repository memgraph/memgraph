/// @file

#pragma once

#include <atomic>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "durability/single_node_ha/state_delta.hpp"
#include "kvstore/kvstore.hpp"
#include "raft/config.hpp"
#include "raft/coordination.hpp"
#include "raft/log_entry.hpp"
#include "raft/raft_interface.hpp"
#include "raft/raft_rpc_messages.hpp"
#include "raft/replication_log.hpp"
#include "raft/replication_timeout_map.hpp"
#include "transactions/type.hpp"
#include "utils/scheduler.hpp"

// Forward declaration
namespace database {
class GraphDb;
}  // namespace database

namespace raft {

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::system_clock::time_point;

enum class Mode { FOLLOWER, CANDIDATE, LEADER };

inline std::string ModeToString(const Mode &mode) {
  switch (mode) {
    case Mode::FOLLOWER:
      return "FOLLOWER";
    case Mode::CANDIDATE:
      return "CANDIDATE";
    case Mode::LEADER:
      return "LEADER";
  }
}

/// Class which models the behaviour of a single server within the Raft
/// cluster. The class is responsible for storing both volatile and
/// persistent internal state of the corresponding state machine as well
/// as performing operations that comply with the Raft protocol.
class RaftServer final : public RaftInterface {
 public:
  RaftServer() = delete;

  /// The implementation assumes that server IDs are unique integers between
  /// ranging from 1 to cluster_size.
  ///
  /// @param server_id ID of the current server.
  /// @param durbility_dir directory for persisted data.
  /// @param db_recover_on_startup flag indicating if recovery should happen at
  ///                              startup.
  /// @param config raft configuration.
  /// @param coordination Abstraction for coordination between Raft servers.
  /// @param db The current DB object.
  RaftServer(uint16_t server_id, const std::string &durability_dir,
             bool db_recover_on_startup, const Config &config,
             raft::Coordination *coordination, database::GraphDb *db);

  /// Starts the RPC servers and starts mechanisms inside Raft protocol.
  void Start();

  /// Stops all threads responsible for the Raft protocol.
  void Shutdown();

  /// Setter for the current term. It updates the persistent storage as well
  /// as its in-memory copy.
  void SetCurrentTerm(uint64_t new_current_term);

  /// Setter for `voted for` member. It updates the persistent storage as well
  /// as its in-memory copy.
  void SetVotedFor(std::optional<uint16_t> new_voted_for);

  /// Setter for `log size` member. It updates the persistent storage as well
  /// as its in-memory copy.
  void SetLogSize(uint64_t new_log_size);

  /// Emplace a new LogEntry in the raft log and start its replication. This
  /// entry is created from a given batched set of StateDelta objects.
  ///
  /// It is possible that the entry was not successfully emplaced. In that case,
  /// the method returns std::nullopt and the caller is responsible for handling
  /// situation correctly (e.g. aborting the corresponding transaction).
  ///
  /// @returns an optional LogEntryStatus object as result.
  std::optional<LogEntryStatus> Emplace(
      const std::vector<database::StateDelta> &deltas) override;

  /// Returns true if the current servers mode is LEADER. False otherwise.
  bool IsLeader() override;

  /// Returns the term ID of the current leader.
  uint64_t TermId() override;

  /// Returns the replication status of LogEntry which began its replication in
  /// a given term ID and was emplaced in the raft log at the given index.
  ///
  /// Replication status can be one of the following
  ///   1) REPLICATED -- LogEntry was successfully replicated across
  ///                    the Raft cluster
  ///   2) WAITING    -- LogEntry was successfully emplaced in the Raft
  ///                    log and is currently being replicated.
  ///   3) ABORTED    -- LogEntry will not be replicated.
  ///   4) INVALID    -- the request for the LogEntry was invalid, most
  ///                    likely either term_id or log_index were out of range.
  ReplicationStatus GetReplicationStatus(uint64_t term_id,
                                         uint64_t log_index) override;

  /// Checks if the LogEntry with the give term id and log index can safely be
  /// committed in local storage.
  ///
  /// @param term_id term when the LogEntry was created
  /// @param log_index index of the LogEntry in the Raft log
  ///
  /// @return bool True if the transaction is safe to commit, false otherwise.
  ///
  /// @throws ReplicationTimeoutException
  /// @throws RaftShutdownException
  /// @throws InvalidReplicationLogLookup
  bool SafeToCommit(uint64_t term_id, uint64_t log_index) override;

 private:
  mutable std::mutex lock_;           ///< Guards all internal state.
  mutable std::mutex heartbeat_lock_; ///< Guards HB issuing

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on all servers
  //////////////////////////////////////////////////////////////////////////////

  Config config_;                        ///< Raft config.
  Coordination *coordination_{nullptr};  ///< Cluster coordination.
  database::GraphDb *db_{nullptr};

  std::atomic<Mode> mode_;                ///< Server's current mode.
  uint16_t server_id_;                    ///< ID of the current server.
  std::filesystem::path durability_dir_;  ///< Durability directory.
  bool db_recover_on_startup_;  ///< Flag indicating if recovery should happen
                                ///< on startup.
  uint64_t commit_index_;       ///< Index of the highest known committed entry.
  uint64_t last_applied_;       ///< Index of the highest applied entry to SM.
  uint64_t last_entry_term_;    ///< Term of the last entry in Raft log

  std::atomic<bool> issue_hb_; ///< Flag which signalizes if the current server
                               ///< should send HBs to the rest of the cluster.

  std::vector<std::thread> peer_threads_;  ///< One thread per peer which
                                           ///< handles outgoing RPCs.

  std::vector<std::thread> hb_threads_; ///< One thread per peer which is
                                        ///< responsible for sending periodic
                                        ///< heartbeats.

  std::condition_variable state_changed_;  ///< Notifies all peer threads on
                                           ///< relevant state change.

  std::thread no_op_issuer_thread_;  ///< Thread responsible for issuing no-op
                                     ///< command on leader change.

  std::condition_variable leader_changed_;  ///< Notifies the
                                            ///< no_op_issuer_thread that a new
                                            ///< leader has been elected.

  std::condition_variable hb_condition_; ///< Notifies the HBIssuer thread
                                         ///< that it should start sending
                                         ///< heartbeats.

  std::atomic<bool> exiting_{false};  ///< True on server shutdown.

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on followers and candidates
  //////////////////////////////////////////////////////////////////////////////

  std::thread election_thread_;  ///< Timer thread for triggering elections.
  TimePoint next_election_;      ///< Next election `TimePoint`.

  std::condition_variable election_change_;  ///> Used to notify election_thread
                                             ///> on next_election_ change.

  std::mt19937_64 rng_ = std::mt19937_64(std::random_device{}());

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on candidates
  //////////////////////////////////////////////////////////////////////////////

  uint16_t granted_votes_;
  std::vector<bool> vote_requested_;

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on leaders
  //////////////////////////////////////////////////////////////////////////////

  std::vector<uint64_t> next_index_;  ///< for each server, index of the next
                                      ///< log entry to send to that server.

  std::vector<uint64_t> index_offset_; ///< for each server, the offset for
                                       ///< which we reduce the next_index_
                                       ///< field if the AppendEntries request
                                       ///< is denied. We use "binary lifting"
                                       ///< style technique to achieve at most
                                       ///< O(logn) requests.

  std::vector<uint64_t> match_index_;  ///< for each server, index of the
                                       ///< highest log entry known to be
                                       ///< replicated on server.

  std::vector<TimePoint> next_replication_;  ///< for each server, time point
                                             ///< for the next replication.

  std::vector<TimePoint> next_heartbeat_; ///< for each server, time point for
                                          ///< the next heartbeat.

  // Tracks timepoints until a transactions is allowed to be in the replication
  // process.
  ReplicationTimeoutMap replication_timeout_;

  //////////////////////////////////////////////////////////////////////////////
  // persistent state on all servers
  //
  // Persistent data consists of:
  //   - uint64_t current_term -- latest term server has seen.
  //   - uint16_t voted_for    -- candidate_id that received vote in current
  //                              term (null if none).
  //   - uint64_t log_size     -- Number of stored entries within the log.
  //   - vector<LogEntry> log  -- log entries. Each log entry is stored under
  //                              a separate key within KVStore.
  //////////////////////////////////////////////////////////////////////////////

  kvstore::KVStore disk_storage_;

  std::optional<uint16_t> voted_for_;

  std::atomic<uint64_t> current_term_;
  uint64_t log_size_;

  std::map<uint64_t, LogEntry> log_;

  /// Recovers persistent data from disk and stores its in-memory copies
  /// that insure faster read-only operations. This method should be called
  /// on start-up. If parts of persistent data are missing, the method won't
  /// make a copy of that data, i.e. no exception is thrown and the caller
  /// should check whether persistent data actually exists.
  void RecoverPersistentData();

  /// Makes a transition to a new `raft::Mode`.
  ///
  /// throws InvalidTransitionException when transitioning between incompatible
  ///                                   `raft::Mode`s.
  void Transition(const raft::Mode &new_mode);

  /// Tries to advance the commit index on a leader.
  void AdvanceCommitIndex();

  /// Decides whether to send Log Entires or Snapshot to the given peer.
  ///
  /// @param peer_id ID of the peer which receives entries.
  /// @param lock Lock from the peer thread (released while waiting for
  ///             response)
  void SendEntries(uint16_t peer_id, std::unique_lock<std::mutex> *lock);

  /// Sends Log Entries to peer. This function should only be called in leader
  /// mode.
  ///
  /// @param peer_id ID of the peer which receives entries.
  /// @param lock Lock from the peer thread (released while waiting for
  ///             response)
  void SendLogEntries(uint16_t peer_id,
                      std::unique_lock<std::mutex> *lock);

  /// Send Snapshot to peer. This function should only be called in leader
  /// mode.
  ///
  /// @param peer_id ID of the peer which receives entries.
  /// @param lock Lock from the peer thread (released while waiting for
  ///             response)
  void SendSnapshot(uint16_t peer_id, std::unique_lock<std::mutex> *lock);

  /// Main function of the `election_thread_`. It is responsible for
  /// transition to CANDIDATE mode when election timeout elapses.
  void ElectionThreadMain();

  /// Main function of the thread that handles outgoing RPCs towards a
  /// specified node within the Raft cluster.
  ///
  /// @param peer_id - ID of a receiving node in the cluster.
  void PeerThreadMain(uint16_t peer_id);

  /// Main function of the thread that handles issuing heartbeats towards
  /// other peers. At the moment, this function is ignorant about the status
  /// of LogEntry replication. Therefore, it might issue unnecessary
  /// heartbeats, but we can live with that at this point.
  ///
  /// @param peer_id - ID of a receiving node in the cluster.
  void HBThreadMain(uint16_t peer_id);

  /// Issues no-op command when a new leader is elected. This is done to
  /// force the Raft protocol to commit logs from previous terms that
  /// have been replicated on a majority of peers.
  void NoOpIssuerThreadMain();

  /// Sets the `TimePoint` for next election.
  void SetNextElectionTimePoint();

  /// Checks if the current server obtained enough votes to become a leader.
  bool HasMajorityVote();

  /// Returns relevant metadata about the last entry in this server's Raft Log.
  /// More precisely, returns a pair consisting of an index of the last entry
  /// in the log and the term of the last entry in the log.
  ///
  /// @return std::pair<last_log_index, last_log_term>
  std::pair<uint64_t, uint64_t> LastEntryData();

  /// Checks whether Raft log of server A is at least as up-to-date as the Raft
  /// log of server B. This is strictly defined in Raft paper 5.4.
  ///
  /// @param last_log_index_a - Index of server A's last log entry.
  /// @param last_log_term_a  - Term of server A's last log entry.
  /// @param last_log_index_b - Index of server B's last log entry.
  /// @param last_log_term_b  - Term of server B's last log entry.
  bool AtLeastUpToDate(uint64_t last_log_index_a, uint64_t last_log_term_a,
                       uint64_t last_log_index_b, uint64_t last_log_term_b);

  /// Checks whether the current server got a reply from "future", i.e. reply
  /// with a higher term. If so, the current server falls back to follower mode
  /// and updates its current term.
  ///
  /// @param reply_term Term from RPC response.
  /// @return true if the current server's term lags behind.
  bool OutOfSync(uint64_t reply_term);

  /// Retrieves a log entry from the log at a given index.
  ///
  /// @param index Index of the log entry to be retrieved.
  LogEntry GetLogEntry(uint64_t index);

  /// Deletes log entries with indexes that are greater or equal to the given
  /// starting index.
  ///
  /// @param starting_index Smallest index which will be deleted from the Log.
  ///                       Also, a friendly remainder that log entries are
  ///                       1-indexed.
  void DeleteLogSuffix(int starting_index);

  /// Stores log entries with indexes that are greater or equal to the given
  /// starting index into a provided container. If the starting index is
  /// greater than the log size, nothing will be stored in the provided
  /// container.
  ///
  /// @param starting_index Smallest index which will be stored.
  /// @param entries The container which will store the wanted suffix.
  void GetLogSuffix(int starting_index, std::vector<raft::LogEntry> &entries);

  /// Appends new log entries to Raft log. Note that this function is not
  /// smart in any way, i.e. the caller should make sure that it's safe
  /// to call this function. This function also updates this server's commit
  /// index if necessary.
  ///
  /// @param leader_commit_index - Used to update local commit index.
  /// @param starting_index - Index in the log from which we start to append.
  /// @param new_entries - New `LogEntry` instances to be appended in the log.
  void AppendLogEntries(uint64_t leader_commit_index, uint64_t starting_index,
                        const std::vector<LogEntry> &new_entries);

  /// Generates the key under which the `LogEntry` with a given index should
  /// be stored on our disk storage.
  ///
  /// @param index - Index of the `LogEntry` for which we generate the key.
  std::string LogEntryKey(uint64_t index);

  /// Serializes Raft log entry into `std::string`
  std::string SerializeLogEntry(const LogEntry &log_entry);

  /// Deserialized Raft log entry from `std::string`
  LogEntry DeserializeLogEntry(const std::string &serialized_log_entry);

  /// Start a new transaction with a NO-OP StateDelta.
  void NoOpCreate();

  /// Applies the given batch of state deltas that are representing a transacton
  /// to the db.
  void ApplyStateDeltas(const std::vector<database::StateDelta> &deltas);

  std::mutex &WithLock() override;
};
}  // namespace raft
