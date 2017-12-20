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
 * Threadsafe generation of new global ids which belong to the
 * worker_id machine. Never call SetId after calling Next without an Id you are
 * sure is going to be used for gid, i.e. SetId should only be called before
 * first Next call. We want to make sure that every id that we generate is
 * larger than the id set by SetId, we can ensure that by not allowing calls to
 * SetId after Next which generated new id (incremented internal id counter).
 */
class GidGenerator {
 public:
  GidGenerator(int worker_id) : worker_id_(worker_id) {}
  /**
   * Returns a new globally unique identifier.
   * @param local_id - force local id instead of generating a new one
   */
  gid::Gid Next(std::experimental::optional<Gid> local_id) {
    auto id = local_id ? *local_id : next_local_id_++;
    if (local_id) {
      utils::EnsureAtomicGe(next_local_id_, id + 1);
    } else {
      generated_id_ = true;
    }
    return gid::Create(worker_id_, id);
  }

  /// Returns number of locally generated ids
  uint64_t LocalCount() const { return next_local_id_; };

  // Sets a new id from which every new gid will be generated, should only be
  // set before first Next is called
  void SetId(uint64_t id) {
    DCHECK(!generated_id_)
        << "Id should be set only before first id is generated";
    next_local_id_ = id;
  }

 private:
  bool generated_id_{false};
  int worker_id_;
  std::atomic<uint64_t> next_local_id_{0};
};
}  // namespace gid
