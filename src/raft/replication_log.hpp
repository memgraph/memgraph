/// @file
#pragma once

#include <atomic>

#include "data_structures/bitset/dynamic_bitset.hpp"
#include "transactions/type.hpp"

namespace raft {

/// Tracks information about replicated and active logs for high availability.
///
/// The main difference between ReplicationLog and CommitLog is that
/// ReplicationLog doesn't throw when looking up garbage collected transaction
/// ids.
class ReplicationLog final {
 public:
  static constexpr int kBitsetBlockSize = 32768;

  ReplicationLog() = default;
  ReplicationLog(const ReplicationLog &) = delete;
  ReplicationLog(ReplicationLog &&) = delete;
  ReplicationLog &operator=(const ReplicationLog &) = delete;
  ReplicationLog &operator=(ReplicationLog &&) = delete;

  bool is_active(tx::TransactionId id) const {
    return fetch_info(id).is_active();
  }

  void set_active(tx::TransactionId id) { log.set(2 * id); }

  bool is_replicated(tx::TransactionId id) const {
    return fetch_info(id).is_replicated();
  }

  void set_replicated(tx::TransactionId id) { log.set(2 * id + 1); }

  // Clears the replication log from bits associated with transactions with an
  // id lower than `id`.
  void garbage_collect_older(tx::TransactionId id) {
    // We keep track of the valid prefix in order to avoid the `CHECK` inside
    // the `DynamicBitset`.
    valid_prefix = 2 * id;
    log.delete_prefix(2 * id);
  }

  class Info final {
   public:
    enum Status {
      UNKNOWN = 0,     // 00
      ACTIVE = 1,      // 01
      REPLICATED = 2,  // 10
    };

    explicit Info(uint8_t flags) {
      if (flags & REPLICATED) {
        flags_ = REPLICATED;
      } else if (flags & ACTIVE) {
        flags_ = ACTIVE;
      } else {
        flags_ = UNKNOWN;
      }
    }

    bool is_active() const { return flags_ & ACTIVE; }

    bool is_replicated() const { return flags_ & REPLICATED; }

    operator uint8_t() const { return flags_; }

   private:
    uint8_t flags_{0};
  };

  Info fetch_info(tx::TransactionId id) const {
    if (valid_prefix > 2 * id) return Info{0};

    return Info{log.at(2 * id, 2)};
  }

 private:
  DynamicBitset<uint8_t, kBitsetBlockSize> log;
  std::atomic<tx::TransactionId> valid_prefix{0};
};

}  // namespace raft
