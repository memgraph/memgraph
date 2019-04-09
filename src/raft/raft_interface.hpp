/// @file

#pragma once

#include <mutex>

#include "durability/single_node_ha/state_delta.hpp"
#include "transactions/type.hpp"

namespace raft {

/// Exposes only functionality that other parts of Memgraph can interact with,
/// emplacing a state delta into the appropriate Raft log entry.
class RaftInterface {
 public:
  /// Add StateDelta to the appropriate Raft log entry.
  ///
  /// @returns true if the Delta is emplaced, false otherwise.
  virtual bool Emplace(const database::StateDelta &) = 0;

  /// Checks if the transaction with the given transaction id can safely be
  /// committed in local storage.
  virtual bool SafeToCommit(const tx::TransactionId &) = 0;

  /// Returns true if the current servers mode is LEADER. False otherwise.
  virtual bool IsLeader() = 0;

  /// Returns the term ID of the current leader.
  virtual uint64_t TermId() = 0;

  virtual std::mutex &WithLock() = 0;

 protected:
  ~RaftInterface() {}
};

}  // namespace raft
