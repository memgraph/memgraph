#pragma once

#include "data_structures/bitset/dynamic_bitset.hpp"
#include "type.hpp"

namespace tx {

class CommitLog {
 public:
  struct Info {
    enum Status {
      ACTIVE = 0,     // 00
      COMMITTED = 1,  // 01
      ABORTED = 2,    // 10
    };

    bool is_active() const { return flags == ACTIVE; }

    bool is_committed() const { return flags & COMMITTED; }

    bool is_aborted() const { return flags & ABORTED; }

    operator uint8_t() const { return flags; }

    uint8_t flags;
  };

  CommitLog() = default;
  CommitLog(CommitLog &) = delete;
  CommitLog(CommitLog &&) = delete;

  CommitLog operator=(CommitLog) = delete;

  Info fetch_info(transaction_id_t id) const { return Info{log.at(2 * id, 2)}; }

  bool is_active(transaction_id_t id) const {
    return fetch_info(id).is_active();
  }

  bool is_committed(transaction_id_t id) const {
    return fetch_info(id).is_committed();
  }

  void set_committed(transaction_id_t id) { log.set(2 * id); }

  bool is_aborted(transaction_id_t id) const {
    return fetch_info(id).is_aborted();
  }

  void set_aborted(transaction_id_t id) { log.set(2 * id + 1); }

 private:
  // TODO: Searching the log will take more and more time the more and more
  // transactoins are done. This could be awerted if DynamicBitset is changed
  // to point to largest chunk instead of the smallest.
  DynamicBitset<uint8_t, 32768> log;
};
}
