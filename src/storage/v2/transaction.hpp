#pragma once

#include <atomic>
#include <limits>
#include <list>

#include "utils/skip_list.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/view.hpp"

namespace storage {

struct Transaction {
  Transaction(uint64_t transaction_id, uint64_t start_timestamp)
      : transaction_id(transaction_id),
        start_timestamp(start_timestamp),
        commit_timestamp(transaction_id),
        command_id(0),
        is_active(true),
        must_abort(false) {}

  // Default constructor necessary for utils::SkipList.
  Transaction()
      : transaction_id(std::numeric_limits<uint64_t>::max()),
        start_timestamp(std::numeric_limits<uint64_t>::max()),
        commit_timestamp(std::numeric_limits<uint64_t>::max()),
        command_id(std::numeric_limits<uint64_t>::max()),
        is_active(true),
        must_abort(false) {}

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;

  Transaction(Transaction &&other) noexcept
      : transaction_id(other.transaction_id),
        start_timestamp(other.start_timestamp),
        commit_timestamp(other.commit_timestamp.load()),
        command_id(other.command_id),
        deltas(std::move(other.deltas)),
        modified_vertices(std::move(other.modified_vertices)),
        is_active(other.is_active),
        must_abort(other.must_abort) {}

  Transaction &operator=(Transaction &&other) noexcept {
    if (this == &other) return *this;

    transaction_id = other.transaction_id;
    start_timestamp = other.start_timestamp;
    commit_timestamp = other.commit_timestamp.load();
    command_id = other.command_id;
    deltas = std::move(other.deltas);
    modified_vertices = std::move(other.modified_vertices);
    is_active = other.is_active;
    must_abort = other.must_abort;

    return *this;
  }

  ~Transaction() {}

  Delta *CreateDelta(Delta::Action action, uint64_t value) {
    return &deltas.emplace_back(action, value, &commit_timestamp, command_id);
  }

  uint64_t transaction_id;
  uint64_t start_timestamp;
  std::atomic<uint64_t> commit_timestamp;
  uint64_t command_id;
  std::list<Delta> deltas;
  std::list<Vertex *> modified_vertices;
  bool is_active;
  bool must_abort;
};

inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.transaction_id == second.transaction_id;
}
inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.transaction_id < second.transaction_id;
}
inline bool operator==(const Transaction &first, const uint64_t &second) {
  return first.transaction_id == second;
}
inline bool operator<(const Transaction &first, const uint64_t &second) {
  return first.transaction_id < second;
}

}  // namespace storage
