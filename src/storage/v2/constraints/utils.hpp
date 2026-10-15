// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#pragma once

#include <atomic>
#include <optional>
#include <vector>
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {
/// Runs on a worker thread, so nothing may leave it: an exception escaping a thread function terminates the process.
/// Cancellation is reported through `cancelled` and an out-of-memory failure through `oom`; the spawning thread
/// re-raises both once every worker has joined. Anything else `func` might throw would still escape, so keep it to
/// those two.
template <typename ResultType, typename Func, typename... Args>
void do_per_thread_validation(ResultType &result, Func &&func,
                              const std::vector<std::pair<Gid, uint64_t>> &vertex_batches,
                              std::atomic<uint64_t> &batch_counter, const utils::SkipListDb<Vertex>::Accessor &vertices,
                              ProgressCallback const &on_progress, CheckCancelFunction const &cancel_check,
                              std::atomic<bool> &cancelled,
                              utils::Synchronized<std::optional<utils::OutOfMemoryException>, utils::SpinLock> &oom,
                              Args &&...args) {
  while (result.ReadLock()->has_value() && !cancelled.load(std::memory_order_relaxed)) {
    const auto batch_index = batch_counter.fetch_add(1, std::memory_order_acquire);
    if (batch_index >= vertex_batches.size()) {
      return;
    }
    const auto &[gid_start, batch_size] = vertex_batches[batch_index];

    auto vertex_curr = vertices.find(gid_start);
    DMG_ASSERT(vertex_curr != vertices.end(), "No vertex was found with given gid");
    try {
      for (auto i{0U}; i < batch_size; ++i, ++vertex_curr) {
        if (cancel_check()) {
          cancelled.store(true, std::memory_order_relaxed);
          return;
        }
        const auto validation_result = func(*vertex_curr, std::forward<Args>(args)...);
        if (validation_result) [[likely]] {
          if (on_progress) on_progress();
          continue;
        }
        result.WithLock([&validation_result](auto &result) { result = std::unexpected{validation_result.error()}; });
        break;
      }
    } catch (utils::OutOfMemoryException &failure) {
      // Same guard the parallel index population carries: an exception leaving a thread function terminates the
      // process. Report it to the spawning thread, which rethrows once the workers have joined. The blocker keeps
      // handling one OOM from tripping another.
      utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
      *oom.Lock() = std::move(failure);
      return;
    }
  }
}
}  // namespace memgraph::storage
