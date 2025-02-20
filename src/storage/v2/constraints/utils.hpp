// Copyright 2025 Memgraph Ltd.
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

#include <vector>
#include "storage/v2/vertex.hpp"
#include "utils/counter.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {
template <typename ErrorType, typename Func, typename... Args>
void do_per_thread_validation(ErrorType &maybe_error, Func &&func,
                              const std::vector<std::pair<Gid, uint64_t>> &vertex_batches,
                              std::atomic<uint64_t> &batch_counter, const utils::SkipList<Vertex>::Accessor &vertices,
                              std::optional<SnapshotObserverInfo> snapshot_info, Args &&...args) {
  while (!maybe_error.ReadLock()->has_value()) {
    const auto batch_index = batch_counter.fetch_add(1, std::memory_order_acquire);
    if (batch_index >= vertex_batches.size()) {
      return;
    }
    const auto &[gid_start, batch_size] = vertex_batches[batch_index];

    auto vertex_curr = vertices.find(gid_start);
    DMG_ASSERT(vertex_curr != vertices.end(), "No vertex was found with given gid");

    std::optional<utils::ResettableRuntimeCounter> maybe_batch_counter;
    if (snapshot_info) {
      maybe_batch_counter.emplace(utils::ResettableRuntimeCounter{snapshot_info->item_batch_size});
    }
    for (auto i{0U}; i < batch_size; ++i, ++vertex_curr) {
      const auto violation = func(*vertex_curr, std::forward<Args>(args)...);
      if (!violation.has_value()) [[likely]] {
        if (maybe_batch_counter && (*maybe_batch_counter)()) {
          snapshot_info->observer->Update();
        }
        continue;
      }
      maybe_error.WithLock([&violation](auto &maybe_error) { maybe_error = *violation; });
      break;
    }
  }
}
}  // namespace memgraph::storage
