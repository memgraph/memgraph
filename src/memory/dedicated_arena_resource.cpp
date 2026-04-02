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

#include "flags/general.hpp"
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
  // Light-edge arena uses a more aggressive reclaim policy than the general
  // jemalloc baseline in order to reduce retained memory under stress.
  ssize_t dirty_decay = 1000;
  ssize_t muzzy_decay = 0;
  const std::string dirty_key = "arena." + std::to_string(arena_id_) + ".dirty_decay_ms";
  const std::string muzzy_key = "arena." + std::to_string(arena_id_) + ".muzzy_decay_ms";
  je_mallctl(dirty_key.c_str(), nullptr, nullptr, &dirty_decay, sizeof(dirty_decay));
  je_mallctl(muzzy_key.c_str(), nullptr, nullptr, &muzzy_decay, sizeof(muzzy_decay));
  spdlog::info(
      "DedicatedArenaResource {} decay configured: dirty={} ms, muzzy={} ms", arena_id_, dirty_decay, muzzy_decay);
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
