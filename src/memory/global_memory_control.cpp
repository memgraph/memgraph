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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

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
// default (base) hooks and feed graph_memory_tracker (which parents to total_memory_tracker).
// DB-specific arenas are created later via GlobalArenaPool/ArenaPool as additional explicit
// jemalloc arenas; this startup hook installation does not hand the global arenas
// over to any database.
namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DbArenaHooks global_graph_arena_hooks{};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extent_hooks_t *old_hooks = nullptr;

// Arenas for global graph hooks: automatic arenas [0, opt.narenas) plus the
// sacrificial CPU-coverage arenas lazily created by EnsureCpuArenaCoverage().
// Invariant after startup — cached as magic static.
const std::vector<unsigned> &GlobalHookArenaIds() {
  static const std::vector<unsigned> ids = [] {
    unsigned n_arenas = 0;
    size_t sz = sizeof(n_arenas);
    if (je_mallctl("opt.narenas", static_cast<void *>(&n_arenas), &sz, nullptr, 0)) {
      LOG_FATAL("Error getting number of jemalloc arenas");
    }
    std::vector<unsigned> result(n_arenas);
    std::ranges::iota(result, 0U);
    const auto &coverage = EnsureCpuArenaCoverage();
    result.append_range(coverage);
    return result;
  }();
  return ids;
}
}  // namespace

#endif

void SetHooks() {
#if USE_JEMALLOC

  if (nullptr != old_hooks) {
    return;
  }

  int err = 0;
  // Create the sacrificial CPU-coverage arenas BEFORE installing the global
  // tracking hooks so they are covered too (see GlobalHookArenaIds).
  EnsureCpuArenaCoverage();
  for (const unsigned i : GlobalHookArenaIds()) {
    const auto func_name = fmt::format("arena.{}.extent_hooks", i);

    extent_hooks_t *current_old_hooks = nullptr;
    size_t hooks_len = sizeof(extent_hooks_t *);

    err = je_mallctl(func_name.c_str(), static_cast<void *>(&current_old_hooks), &hooks_len, nullptr, 0);
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
    err = je_mallctl(func_name.c_str(), nullptr, nullptr, static_cast<void *>(&old_hooks), sizeof(extent_hooks_t *));
    if (err) {
      LOG_FATAL("Error setting jemalloc hooks for jemalloc arena {}", i);
    }

    // Install tracker hooks.
    const extent_hooks_t *new_hooks = &global_graph_arena_hooks.hooks;
    err = je_mallctl(func_name.c_str(), nullptr, nullptr, static_cast<void *>(&new_hooks), sizeof(extent_hooks_t *));
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

  for (const unsigned i : GlobalHookArenaIds()) {
    const auto func_name = fmt::format("arena.{}.extent_hooks", i);
    const int err =
        je_mallctl(func_name.c_str(), nullptr, nullptr, static_cast<void *>(&old_hooks), sizeof(extent_hooks_t *));
    if (err) {
      LOG_FATAL("Error setting default hooks for jemalloc arena {}", i);
    }
  }
  old_hooks = nullptr;
#endif
}

void PurgeUnusedMemory() {
#if USE_JEMALLOC
  je_mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#else
  malloc_trim(0);
#endif
}

void EnsureJemallocThreadStateInitialized() {
#if USE_JEMALLOC
  enum class State : uint8_t { kUninitialized, kInitializing, kInitialized };
  // NOLINTNEXTLINE (misc-use-internal-linkage)
  constinit thread_local State state [[gnu::tls_model("initial-exec")]] = State::kUninitialized;

  if (state != State::kUninitialized) {
    return;
  }

  state = State::kInitializing;
  {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker blocker;
    if (void *p = je_malloc(1); p != nullptr) {
      je_free(p);
    }
  }
  state = State::kInitialized;
#endif
}

void EnableBackgroundThreads() {
#if USE_JEMALLOC
  bool enable = true;
  int err = je_mallctl("background_thread", nullptr, nullptr, &enable, sizeof(enable));
  if (err) {
    LOG_FATAL("Failed to enable jemalloc background threads: {} ({})", strerror(err), err);
  }
#endif
}

#undef STRINGIFY
#undef STRINGIFY_HELPER

}  // namespace memgraph::memory
