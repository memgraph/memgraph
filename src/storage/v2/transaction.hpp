#pragma once

#include <atomic>
#include <limits>
#include <list>
#include <memory>

#include "utils/skip_list.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"

namespace storage {

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

struct Transaction {
  Transaction(uint64_t transaction_id, uint64_t start_timestamp)
      : transaction_id(transaction_id), start_timestamp(start_timestamp), command_id(0), must_abort(false) {}

  Transaction(Transaction &&other) noexcept
      : transaction_id(other.transaction_id),
        start_timestamp(other.start_timestamp),
        commit_timestamp(std::move(other.commit_timestamp)),
        command_id(other.command_id),
        deltas(std::move(other.deltas)),
        must_abort(other.must_abort) {}

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() {}

  /// @throw std::bad_alloc if failed to create the `commit_timestamp`
  void EnsureCommitTimestampExists() {
    if (commit_timestamp != nullptr) return;
    commit_timestamp = std::make_unique<std::atomic<uint64_t>>(transaction_id);
  }

  uint64_t transaction_id;
  uint64_t start_timestamp;
  // The `Transaction` object is stack allocated, but the `commit_timestamp`
  // must be heap allocated because `Delta`s have a pointer to it, and that
  // pointer must stay valid after the `Transaction` is moved into
  // `commited_transactions_` list for GC.
  std::unique_ptr<std::atomic<uint64_t>> commit_timestamp;
  uint64_t command_id;
  std::list<Delta> deltas;
  bool must_abort;
};

inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.transaction_id == second.transaction_id;
}
inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.transaction_id < second.transaction_id;
}
inline bool operator==(const Transaction &first, const uint64_t &second) { return first.transaction_id == second; }
inline bool operator<(const Transaction &first, const uint64_t &second) { return first.transaction_id < second; }

}  // namespace storage
