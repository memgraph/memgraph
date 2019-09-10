/// @file
#pragma once

#include <chrono>
#include <map>
#include <mutex>

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
  void Remove(const uint64_t term_id, const uint64_t log_index) {
    std::lock_guard<std::mutex> guard(lock_);
    timeout_.erase({term_id, log_index});
  }

  /// Inserts and entry in the map by setting a point in time until it needs to
  /// replicated.
  void Insert(const uint64_t term_id, const uint64_t log_index) {
    std::lock_guard<std::mutex> guard(lock_);
    timeout_[{term_id, log_index}] = replication_timeout_ + Clock::now();
  }

  /// Checks if the given entry has timed out.
  /// @returns bool True if it exceeded timeout, false otherwise.
  bool CheckTimeout(const uint64_t term_id, const uint64_t log_index) {
    std::lock_guard<std::mutex> guard(lock_);
    auto found = timeout_.find({term_id, log_index});
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
  // TODO(ipaljak): Consider using unordered_map if we encounter any performance
  //                issues.
  std::map<std::pair<uint64_t, uint64_t>, TimePoint> timeout_;
};

}  // namespace raft
