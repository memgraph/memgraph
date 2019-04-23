#pragma once

#include <atomic>
#include <cstdint>
#include <optional>

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

/// Returns `local` id from global id.
static inline uint64_t LocalId(Gid gid) { return gid >> kWorkerIdSize; }

/// Returns id of the worker that created this gid.
static inline int CreatorWorker(Gid gid) {
  return gid & ((1ULL << kWorkerIdSize) - 1);
}

/**
 * Threadsafe generation of new global ids which belong to the
 * worker_id machine. Never call SetId after calling Next without an Id you are
 * sure is going to be used for gid, i.e. SetId should only be called before
 * first Next call. We want to make sure that every id that we generate is
 * larger than the id set by SetId, we can ensure that by not allowing calls to
 * SetId after Next which generated new id (incremented internal id counter).
 */
class Generator {
 public:
  Generator(int worker_id) : worker_id_(worker_id) {}

  /**
   * Returns a globally unique identifier.
   *
   * @param requested_gid - The desired gid. If given, it will be returned and
   * this generator's state updated accordingly.
   */
  gid::Gid Next(std::optional<gid::Gid> requested_gid = std::nullopt) {
    if (requested_gid) {
      if (gid::CreatorWorker(*requested_gid) == worker_id_)
        utils::EnsureAtomicGe(next_local_id_, gid::LocalId(*requested_gid) + 1);
      return *requested_gid;
    } else {
      generated_id_ = true;
      return worker_id_ | next_local_id_++ << kWorkerIdSize;
    }
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
