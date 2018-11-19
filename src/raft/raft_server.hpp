/// @file

#pragma once

#include <mutex>
#include <unordered_map>
#include <vector>

#include "durability/single_node_ha/state_delta.hpp"
#include "raft/config.hpp"
#include "storage/common/kvstore/kvstore.hpp"
#include "transactions/type.hpp"

namespace raft {

// Forward declaration.
class Coordination;

enum class Mode { FOLLOWER, CANDIDATE, LEADER };

/// Class which models the behaviour of a single server within the Raft
/// cluster. The class is responsible for storing both volatile and
/// persistent internal state of the corresponding state machine as well
/// as performing operations that comply with the Raft protocol.
class RaftServer {
 public:
  RaftServer() = delete;

  /**
   * The implementation assumes that server IDs are unique integers between
   * ranging from 1 to cluster_size.
   *
   * @param server_id ID of the current server.
   * @param durbility_dir directory for persisted data.
   * @param config raft configuration.
   * @param coordination Abstraction for coordination between Raft servers.
   */
  RaftServer(uint16_t server_id, const std::string &durability_dir,
             const Config &config, raft::Coordination *coordination);

  void Replicate(const std::vector<database::StateDelta> &log);

  void Emplace(const database::StateDelta &delta);

 private:
  /// Buffers incomplete Raft logs.
  ///
  /// A Raft log is considered to be complete if it ends with a StateDelta
  /// that represents transaction commit;
  /// LogEntryBuffer will be used instead of WriteAheadLog. We don't need to
  /// persist logs until we receive a majority vote from the Raft cluster, and
  /// apply the to our local state machine(storage).
  class LogEntryBuffer final {
   public:
    LogEntryBuffer() = delete;

    explicit LogEntryBuffer(RaftServer *raft_server);

    void Enable();

    /// Disable all future insertions in the buffer.
    ///
    /// Note: this will also clear all existing logs from buffers.
    void Disable();

    /// Insert a new StateDelta in logs.
    ///
    /// If for a state delta, `IsStateDeltaTransactionEnd` returns true, this
    /// marks that this log is complete and the replication can start.
    void Emplace(const database::StateDelta &delta);

   private:
    bool enabled_{false};
    mutable std::mutex lock_;
    std::unordered_map<tx::TransactionId, std::vector<database::StateDelta>>
        logs_;

    RaftServer *raft_server_{nullptr};

    bool IsStateDeltaTransactionEnd(const database::StateDelta &delta);
  };

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on all servers
  //////////////////////////////////////////////////////////////////////////////

  uint16_t server_id_;                   ///< ID of the current server.
  Config config_;                        ///< Raft config.
  Coordination *coordination_{nullptr};  ///< Cluster coordination.

  Mode mode_;              ///< Server's current mode.
  uint64_t commit_index_;  ///< Index of the highest known committed entry.
  uint64_t last_applied_;  ///< Index of the highest applied entry to SM.

  /// Raft log entry buffer.
  ///
  /// LogEntryBuffer buffers Raft logs until a log is complete and ready for
  /// replication. This doesn't have to persist, if something fails before a
  /// log is ready for replication it will be discarded anyway.
  LogEntryBuffer log_entry_buffer_{this};

  //////////////////////////////////////////////////////////////////////////////
  // volatile state on leaders
  //////////////////////////////////////////////////////////////////////////////

  std::vector<uint16_t> next_index_;  ///< for each server, index of the next
                                      ///< log entry to send to that server.

  std::vector<uint16_t> match_index_;  ///< for each server, index of the
                                       ///< highest log entry known to be
                                       ///< replicated on server.

  //////////////////////////////////////////////////////////////////////////////
  // persistent state on all servers
  //
  // Persistent data consists of:
  //   - uint64_t current_term -- latest term server has seen.
  //   - uint16_t voted_for    -- candidate_id that received vote in current
  //                              term (null if none).
  //   - vector<LogEntry> log  -- log entries.
  //////////////////////////////////////////////////////////////////////////////
  storage::KVStore disk_storage_;

  /// Makes a transition to a new `raft::Mode`.
  ///
  /// @throws InvalidTransitionException when transitioning between incompatible
  void Transition(const Mode &new_mode);
};
}  // namespace raft
