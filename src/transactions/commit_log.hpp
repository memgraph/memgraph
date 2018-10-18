#pragma once

#include "data_structures/bitset/dynamic_bitset.hpp"
#include "transactions/type.hpp"

namespace tx {

// This class is lock free. There is no need to acquire any lock when accessing
// this class and this class doesn't acquire any lock on method calls.
class CommitLog {
 public:
  static constexpr int kBitsetBlockSize = 32768;
  CommitLog() = default;
  CommitLog(const CommitLog &) = delete;
  CommitLog(CommitLog &&) = delete;
  CommitLog &operator=(const CommitLog &) = delete;
  CommitLog &operator=(CommitLog &&) = delete;

  bool is_active(TransactionId id) const {
    return fetch_info(id).is_active();
  }

  bool is_committed(TransactionId id) const {
    return fetch_info(id).is_committed();
  }

  void set_committed(TransactionId id) { log.set(2 * id); }

  bool is_aborted(TransactionId id) const {
    return fetch_info(id).is_aborted();
  }

  void set_aborted(TransactionId id) { log.set(2 * id + 1); }

  // Clears the commit log from bits associated with transactions with an id
  // lower than `id`.
  void garbage_collect_older(TransactionId id) { log.delete_prefix(2 * id); }

  class Info {
   public:
    Info() {}  // Needed for serialization.
    enum Status {
      ACTIVE = 0,     // 00
      COMMITTED = 1,  // 01
      ABORTED = 2,    // 10
    };

    explicit Info(uint8_t flags) : flags_(flags) {}

    bool is_active() const { return flags_ == ACTIVE; }

    bool is_committed() const { return flags_ & COMMITTED; }

    bool is_aborted() const { return flags_ & ABORTED; }

    operator uint8_t() const { return flags_; }

   private:
    uint8_t flags_{0};
  };

  Info fetch_info(TransactionId id) const { return Info{log.at(2 * id, 2)}; }

 private:
  DynamicBitset<uint8_t, kBitsetBlockSize> log;
};

}  // namespace tx
