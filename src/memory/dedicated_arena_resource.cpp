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

#include "dedicated_arena_resource.hpp"

#include <new>
#include <stdexcept>
#include <string>

#include "global_memory_control.hpp"
#include "utils/logging.hpp"

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace memgraph::memory {

DedicatedArenaResource::DedicatedArenaResource() {
#if USE_JEMALLOC
  unsigned id = 0;
  size_t sz = sizeof(id);
  if (je_mallctl("arenas.create", &id, &sz, nullptr, 0) != 0) {
    throw std::runtime_error("DedicatedArenaResource: failed to create jemalloc arena");
  }
  arena_id_ = id;
  // MALLOCX_TCACHE_NONE is required for arena isolation.
  // jemalloc's tcache is indexed by size-class only, not (arena, size-class).
  // Without it, allocs can return blocks cached from other arenas, and frees
  // can deposit blocks into the tcache where any arena can pick them up —
  // defeating the purpose of a dedicated arena entirely.
  alloc_flags_ = MALLOCX_ARENA(arena_id_) | MALLOCX_TCACHE_NONE;
  InstallTrackingHooksOnArena(arena_id_);
  // Disable automatic purging for this arena: freed pages stay in jemalloc's
  // dirty-page cache and are immediately reused by the next allocation instead
  // of being returned to the OS via madvise/munmap.  This eliminates the rapid
  // munmap/mmap churn that occurs under heavy edge alloc/free workloads
  // (graveyard drain followed immediately by new edge allocations), at the cost
  // of higher steady-state RSS for the light-edge pool.
  ssize_t no_decay = -1;
  const std::string dirty_key = "arena." + std::to_string(arena_id_) + ".dirty_decay_ms";
  const std::string muzzy_key = "arena." + std::to_string(arena_id_) + ".muzzy_decay_ms";
  je_mallctl(dirty_key.c_str(), nullptr, nullptr, &no_decay, sizeof(no_decay));
  je_mallctl(muzzy_key.c_str(), nullptr, nullptr, &no_decay, sizeof(no_decay));
#endif
}

DedicatedArenaResource::~DedicatedArenaResource() {
#if USE_JEMALLOC
  const std::string key = "arena." + std::to_string(arena_id_) + ".destroy";
  // Precondition: all allocations from this arena must already be freed.
  // InMemoryStorage satisfies this: ClearLightEdge() drains the graveyard
  // before this destructor runs.
  if (je_mallctl(key.c_str(), nullptr, nullptr, nullptr, 0) != 0) {
    try {
      spdlog::error("DedicatedArenaResource: failed to destroy arena {}", arena_id_);
    } catch (...) {  // NOLINT
    }
  }
#endif
}

void *DedicatedArenaResource::do_allocate(size_t bytes, size_t alignment) {
#if USE_JEMALLOC
  void *p = je_mallocx(bytes, alloc_flags_ | MALLOCX_ALIGN(alignment));
  if (!p) [[unlikely]]
    throw std::bad_alloc{};
  return p;
#else
  return ::operator new(bytes, std::align_val_t{alignment});
#endif
}

void DedicatedArenaResource::do_deallocate(void *p, [[maybe_unused]] size_t bytes, [[maybe_unused]] size_t alignment) {
  if (!p) return;
#if USE_JEMALLOC
  je_dallocx(p, MALLOCX_TCACHE_NONE);
#else
  ::operator delete(p, bytes, std::align_val_t{alignment});
#endif
}

bool DedicatedArenaResource::do_is_equal(const std::pmr::memory_resource &other) const noexcept {
  return this == &other;
}

}  // namespace memgraph::memory
