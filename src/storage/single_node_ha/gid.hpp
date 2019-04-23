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
  /**
   * Returns a globally unique identifier.
   *
   * @param requested_gid - The desired gid. If given, it will be returned and
   * this generator's state updated accordingly.
   */
  gid::Gid Next(std::optional<gid::Gid> requested_gid = std::nullopt) {
    if (requested_gid) {
      utils::EnsureAtomicGe(next_local_id_, *requested_gid + 1);
      return *requested_gid;
    } else {
      return next_local_id_++;
    }
  }

 private:
  std::atomic<uint64_t> next_local_id_{0};
};
}  // namespace gid
