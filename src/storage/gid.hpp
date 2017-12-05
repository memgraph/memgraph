#pragma once

#include <atomic>
#include <cstdint>
#include <experimental/optional>

#include "glog/logging.h"

#include "utils/atomic.hpp"

namespace gid {
/**
 * Global ids are created by taking both the `local` object id, and `worker` id.
 * A global ID has 64 bits. The lower kWorkerIdSize bits contain the worker ID.
 * All the other (upper) bits contain the local ID.
 */
using Gid = uint64_t;

static constexpr std::size_t kWorkerIdSize{10};

/// Returns worker id from global id.
static inline Gid WorkerId(Gid gid) {
  return gid & ((1ULL << kWorkerIdSize) - 1);
}

/// Returns `local` id from global id.
static inline Gid LocalId(Gid gid) { return gid >> kWorkerIdSize; }

/// Returns global id from worker and local ids.
static inline Gid Create(uint64_t worker_id, uint64_t local_id) {
  CHECK(worker_id < (1ULL << kWorkerIdSize));
  CHECK(local_id < (1ULL << (sizeof(Gid) * 8 - kWorkerIdSize)));
  return worker_id | (local_id << kWorkerIdSize);
}

/**
 * @brief - Threadsafe generation of new global ids which belong to the
 * worker_id machine
 */
class GidGenerator {
 public:
  GidGenerator(int worker_id) : worker_id_(worker_id) {}
  /**
   * Returns a new globally unique identifier.
   * @param local_id - force local id instead of generating a new one
   */
  gid::Gid Next(std::experimental::optional<Gid> local_id) {
    auto id = local_id ? *local_id : id_++;
    if (local_id) {
      utils::EnsureAtomicGe(id_, id + 1);
    }
    return gid::Create(worker_id_, id);
  }

 private:
  int worker_id_;
  std::atomic<Gid> id_{0};
};
};  // namespace gid
