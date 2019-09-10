/// @file

#pragma once

#include <mutex>

#include "durability/single_node_ha/state_delta.hpp"
#include "transactions/type.hpp"

namespace raft {

enum class ReplicationStatus { REPLICATED, WAITING, ABORTED, INVALID };

inline std::string ReplicationStatusToString(
    const ReplicationStatus &replication_status) {
  switch (replication_status) {
    case ReplicationStatus::REPLICATED:
      return "REPLICATED";
    case ReplicationStatus::WAITING:
      return "WAITING";
    case ReplicationStatus::ABORTED:
      return "ABORTED";
    case ReplicationStatus::INVALID:
      return "INVALID";
  }
}

/// Structure which describes the status of a newly created LogEntry after the
/// execution of RaftServer's Emplace method.
///
/// It consists of two unsigned 64-bit integers which uniquely describe
/// the emplaced LogEntry:
///   1) Term when the LogEntry was emplaced to the Raft log.
///   2) Index of the entry within the Raft log.
///
/// In the case an entry was not successfully emplaced (e.g. unexpected
/// leadership change), the values will have a std::nullopt value instead.
struct LogEntryStatus {
  uint64_t term_id;
  uint64_t log_index;
};

/// Exposes only functionality that other parts of Memgraph can interact with.
class RaftInterface {
 public:
  /// Emplace a new LogEntry in the raft log and start its replication. This
  /// entry is created from a given batched set of StateDelta objects.
  ///
  /// It is possible that the entry was not successfully emplaced. In that case,
  /// the method returns std::nullopt and the caller is responsible for handling
  /// situation correctly (e.g. aborting the corresponding transaction).
  ///
  /// @returns an optional LogEntryStatus object as result.
  virtual std::optional<LogEntryStatus> Emplace(
      const std::vector<database::StateDelta> &) = 0;

  /// Returns true if the current servers mode is LEADER. False otherwise.
  virtual bool IsLeader() = 0;

  /// Returns the term ID of the current leader.
  virtual uint64_t TermId() = 0;

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
  virtual ReplicationStatus GetReplicationStatus(uint64_t term_id,
                                                 uint64_t log_index) = 0;

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
  virtual bool SafeToCommit(uint64_t term_id, uint64_t log_index) = 0;

  virtual std::mutex &WithLock() = 0;

 protected:
  ~RaftInterface() {}
};

}  // namespace raft
