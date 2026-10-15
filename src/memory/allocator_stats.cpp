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

#include "allocator_stats.hpp"

#include <cstddef>

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace memgraph::memory {

std::optional<AllocatorStats> GetAllocatorStats() {
#if USE_JEMALLOC
  // The stats are cached inside jemalloc; bumping the epoch refreshes them.
  uint64_t epoch = 1;
  size_t epoch_len = sizeof(epoch);
  if (je_mallctl("epoch", &epoch, &epoch_len, &epoch, sizeof(epoch)) != 0) return std::nullopt;

  AllocatorStats stats;
  auto const read = [](char const *name, uint64_t &out) {
    size_t value = 0;
    size_t len = sizeof(value);
    if (je_mallctl(name, &value, &len, nullptr, 0) != 0) return false;
    out = value;
    return true;
  };
  if (!read("stats.allocated", stats.allocated) || !read("stats.active", stats.active) ||
      !read("stats.metadata", stats.metadata) || !read("stats.resident", stats.resident) ||
      !read("stats.mapped", stats.mapped) || !read("stats.retained", stats.retained)) {
    return std::nullopt;
  }
  return stats;
#else
  return std::nullopt;
#endif
}

}  // namespace memgraph::memory
