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

#include <cstdint>

#include "db_arena.hpp"
#include "global_memory_control.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"

#if USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#else
#include <malloc.h>
#endif

namespace memgraph::memory {

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY_HELPER(x) #x
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY(x) STRINGIFY_HELPER(x)

#if USE_JEMALLOC

// Single DbArenaHooks instance for all startup arenas — they all share the same
// default (base) hooks and feed graph_memory_tracker (which rolls up to total_memory_tracker).
namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DbArenaHooks global_graph_arena_hooks{};
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extent_hooks_t *old_hooks = nullptr;

#endif

void SetHooks() {
#if USE_JEMALLOC

  if (nullptr != old_hooks) {
    return;
  }

  unsigned n_arenas = 0;
  size_t sz = sizeof(n_arenas);
  int err = je_mallctl("opt.narenas", (void *)&n_arenas, &sz, nullptr, 0);
  if (err) {
    LOG_FATAL("Error getting number of jemalloc arenas");
  }

  for (unsigned i = 0; i < n_arenas; i++) {
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    extent_hooks_t *current_old_hooks = nullptr;
    size_t hooks_len = sizeof(extent_hooks_t *);

    err = je_mallctl(func_name.c_str(), (void *)&current_old_hooks, &hooks_len, nullptr, 0);
    if (err) {
      LOG_FATAL("Error getting hooks for jemalloc arena {}", i);
    }

    MG_ASSERT(current_old_hooks);
    MG_ASSERT(current_old_hooks->alloc);
    MG_ASSERT(current_old_hooks->dalloc);
    MG_ASSERT(current_old_hooks->destroy);
    MG_ASSERT(current_old_hooks->commit);
    MG_ASSERT(current_old_hooks->decommit);
    MG_ASSERT(current_old_hooks->purge_forced);
    MG_ASSERT(current_old_hooks->purge_lazy);
    MG_ASSERT(current_old_hooks->split);
    MG_ASSERT(current_old_hooks->merge);

    // First arena: capture old_hooks and initialise the shared hooks struct.
    if (old_hooks == nullptr) {
      old_hooks = current_old_hooks;
      InitDbArenaHooks(global_graph_arena_hooks, &utils::graph_memory_tracker, old_hooks);
    } else {
      MG_ASSERT(old_hooks == current_old_hooks, "Inconsistent jemalloc hooks across arenas");
    }

    // Due to the way jemalloc works, we need first to set their hooks
    // which will trigger creating arena, then we can set our custom hook wrappers
    err = je_mallctl(func_name.c_str(), nullptr, nullptr, (void *)&old_hooks, sizeof(extent_hooks_t *));
    if (err) {
      LOG_FATAL("Error setting jemalloc hooks for jemalloc arena {}", i);
    }

    // Install tracker hooks.
    const extent_hooks_t *new_hooks = &global_graph_arena_hooks.hooks;
    err = je_mallctl(func_name.c_str(), nullptr, nullptr, (void *)&new_hooks, sizeof(extent_hooks_t *));
    if (err) {
      LOG_FATAL("Error setting custom hooks for jemalloc arena {}", i);
    }
  }
#endif
}

void UnsetHooks() {
#if USE_JEMALLOC
  if (nullptr == old_hooks) {
    return;
  }

  MG_ASSERT(old_hooks);
  MG_ASSERT(old_hooks->alloc);
  MG_ASSERT(old_hooks->dalloc);
  MG_ASSERT(old_hooks->destroy);
  MG_ASSERT(old_hooks->commit);
  MG_ASSERT(old_hooks->decommit);
  MG_ASSERT(old_hooks->purge_forced);
  MG_ASSERT(old_hooks->purge_lazy);
  MG_ASSERT(old_hooks->split);
  MG_ASSERT(old_hooks->merge);

  unsigned n_arenas{0};
  size_t sz = sizeof(n_arenas);
  int err = je_mallctl("opt.narenas", (void *)&n_arenas, &sz, nullptr, 0);
  if (err) {
    LOG_FATAL("Error getting number of jemalloc arenas");
  }

  for (unsigned i = 0; i < n_arenas; i++) {
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";
    err = je_mallctl(func_name.c_str(), nullptr, nullptr, (void *)&old_hooks, sizeof(extent_hooks_t *));
    if (err) {
      LOG_FATAL("Error setting default hooks for jemalloc arena {}", i);
    }
  }
#endif
}

void PurgeUnusedMemory() {
#if USE_JEMALLOC
  je_mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#else
  malloc_trim(0);
#endif
}

#undef STRINGIFY
#undef STRINGIFY_HELPER

}  // namespace memgraph::memory
