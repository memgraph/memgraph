/// @file
#pragma once

#include <chrono>
#include <mutex>
#include <unordered_map>

#include "transactions/type.hpp"

namespace raft {

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::system_clock::time_point;

/// A wrapper around an unordered_map whose reads/writes are protected with a
/// lock. It's also specialized to serve the sole purpose of tracking
/// replication timeout.
class ReplicationTimeoutMap final {
 public:
  ReplicationTimeoutMap() = delete;

  ReplicationTimeoutMap(const ReplicationTimeoutMap &) = delete;
  ReplicationTimeoutMap(ReplicationTimeoutMap &&) = delete;
  ReplicationTimeoutMap operator=(const ReplicationTimeoutMap &) = delete;
  ReplicationTimeoutMap operator=(ReplicationTimeoutMap &&) = delete;

  explicit ReplicationTimeoutMap(std::chrono::milliseconds replication_timeout)
      : replication_timeout_(replication_timeout) {}

  /// Remove all entries from the map.
  void Clear() {
    std::lock_guard<std::mutex> guard(lock_);
    timeout_.clear();
  }

  /// Remove a single entry from the map.
  void Remove(const tx::TransactionId &tx_id) {
    std::lock_guard<std::mutex> guard(lock_);
    timeout_.erase(tx_id);
  }

  /// Inserts and entry in the map by setting a point in time until it needs to
  /// replicated.
  void Insert(const tx::TransactionId &tx_id) {
    std::lock_guard<std::mutex> guard(lock_);
    timeout_.emplace(tx_id, replication_timeout_ + Clock::now());
  }

  /// Checks if the given entry has timed out.
  /// @returns bool True if it exceeded timeout, false otherwise.
  bool CheckTimeout(const tx::TransactionId &tx_id) {
    std::lock_guard<std::mutex> guard(lock_);
    auto found = timeout_.find(tx_id);
    // If we didn't set the timeout yet, or we already deleted it, we didn't
    // time out.
    if (found == timeout_.end()) return false;
    if (found->second < Clock::now()) {
      return true;
    } else {
      return false;
    }
  }

 private:
  std::chrono::milliseconds replication_timeout_;

  mutable std::mutex lock_;
  std::unordered_map<tx::TransactionId, TimePoint> timeout_;
};

}  // namespace raft
