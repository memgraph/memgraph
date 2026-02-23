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

#include "utils/embeddings_memory_counter.hpp"

#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EmbeddingsMemoryCounter embeddings_memory_counter{};

bool EmbeddingsMemoryCounter::CanAllocate(int64_t bytes) const {
  const auto current_embeddings = amount_.load(std::memory_order_relaxed);
  const auto emb_limit = embeddings_limit_.load(std::memory_order_relaxed);

  if (emb_limit > 0 && current_embeddings + bytes > emb_limit) {
    return false;
  }

  const auto ovr_limit = overall_limit_.load(std::memory_order_relaxed);
  if (ovr_limit > 0) {
    const auto tracked_memory = total_memory_tracker.Amount();
    if (tracked_memory + current_embeddings + bytes > ovr_limit) {
      return false;
    }
  }

  return true;
}

}  // namespace memgraph::utils
