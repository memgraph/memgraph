// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "memory_control.hpp"
#include <fmt/core.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <ios>
#include <mutex>
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/readable_size.hpp"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include <iostream>
#include <vector>

namespace memgraph::memory {

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY_HELPER(x) #x
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY(x) STRINGIFY_HELPER(x)

#if USE_JEMALLOC
extent_hooks_t *old_hooks = nullptr;
#endif

/*
This is for tracking per query limit
*/

/*

std::vector<int64_t> arena_allocations{};
std::vector<int64_t> arena_upper_limit{};
std::vector<int> tracking_arenas{};
*/

int GetArenaForThread() {
#if USE_JEMALLOC
  // unsigned thread_arena{0};
  // size_t size_thread_arena = sizeof(thread_arena);
  // int err = mallctl("thread.arena", &thread_arena, &size_thread_arena, nullptr, 0);
  // if (err) {
  //   return -1;
  // }
  // return static_cast<int>(thread_arena);
#endif
  return -1;
}

void TrackMemoryForThread(int arena_id, size_t size) {
#if USE_JEMALLOC
  // tracking_arenas[arena_id] = true;
  // arena_upper_limit[arena_id] = arena_allocations[arena_id] + size;
#endif
}

#if USE_JEMALLOC
void *my_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit,
               unsigned arena_ind) {
  // This needs to be before, to throw exception in case of too big alloc
  if (*commit) [[likely]] {
    // arena_allocations[arena_ind] += size;
    // if (tracking_arenas[arena_ind]) {
    //   if (arena_allocations[arena_ind] > arena_upper_limit[arena_ind]) {
    //     throw utils::OutOfMemoryException(
    //         fmt::format("Memory limit exceeded! Attempting to allocate a chunk of {} which would put the current "
    //                     "use to {}, while the maximum allowed size for allocation is set to {}.",
    //                     utils::GetReadableSize(size), utils::GetReadableSize(arena_allocations[arena_ind]),
    //                     utils::GetReadableSize(arena_upper_limit[arena_ind])));
    //   }
    // }
    memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
  }

  auto *ptr = old_hooks->alloc(extent_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr) [[unlikely]] {
    if (*commit) {
      // arena_allocations[arena_ind] -= size;
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    }
    return ptr;
  }

  /*
  TEST OF MEMORY TRACKER
  long page_size = sysconf(_SC_PAGESIZE);
  char *mem = (char*)ptr;
  int i=0;
  do{
    mem[i*page_size]=0;
    i++;
  }while(i*page_size < size);
  */

  return ptr;
}

static bool my_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  auto err = old_hooks->dalloc(extent_hooks, addr, size, committed, arena_ind);

  if (err) [[unlikely]] {
    return err;
  }

  if (committed) [[likely]] {
    memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    // arena_allocations[arena_ind] -= size;
  }

  return false;
}

static void my_destroy(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  if (committed) [[likely]] {
    memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    // arena_allocations[arena_ind] -= size;
  }

  old_hooks->destroy(extent_hooks, addr, size, committed, arena_ind);
}

static bool my_commit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                      unsigned arena_ind) {
  auto err = old_hooks->commit(extent_hooks, addr, size, offset, length, arena_ind);

  if (err) {
    return err;
  }

  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(length));

  return false;
}

static bool my_decommit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
  MG_ASSERT(old_hooks && old_hooks->decommit);
  auto err = old_hooks->decommit(extent_hooks, addr, size, offset, length, arena_ind);

  if (err) {
    return err;
  }

  memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(length));

  return false;
}

static bool my_purge_forced(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                            unsigned arena_ind) {
  MG_ASSERT(old_hooks && old_hooks->purge_forced);
  auto err = old_hooks->purge_forced(extent_hooks, addr, size, offset, length, arena_ind);

  if (err) [[unlikely]] {
    return err;
  }
  memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(length));

  return false;
}

static bool my_purge_lazy(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  // If memory is purged lazily, it will not be cleaned immediatelly if we are not using MADVISE_DONTNEED (muzzy=0 and
  // decay=0)
  // memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(length));
  MG_ASSERT(old_hooks && old_hooks->purge_lazy);
  return old_hooks->purge_lazy(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_split(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t size_a, size_t size_b,
                     bool committed, unsigned arena_ind) {
  MG_ASSERT(old_hooks && old_hooks->split);
  return old_hooks->split(extent_hooks, addr, size, size_a, size_b, committed, arena_ind);
}

static bool my_merge(extent_hooks_t *extent_hooks, void *addr_a, size_t size_a, void *addr_b, size_t size_b,
                     bool committed, unsigned arena_ind) {
  MG_ASSERT(old_hooks && old_hooks->merge);
  return old_hooks->merge(extent_hooks, addr_a, size_a, addr_b, size_b, committed, arena_ind);
}

static constexpr extent_hooks_t custom_hooks = {
    .alloc = &my_alloc,
    .dalloc = &my_dalloc,
    .destroy = &my_destroy,
    .commit = &my_commit,
    .decommit = &my_decommit,
    .purge_lazy = &my_purge_lazy,
    .purge_forced = &my_purge_forced,
    .split = &my_split,
    .merge = &my_merge,
};

static const extent_hooks_t *new_hooks = &custom_hooks;

#endif

// TODO this can be designed if we fail setting hooks to rollback to classic jemalloc tracker
void SetHooks() {
#if USE_JEMALLOC

  uint64_t allocated{0};
  uint64_t sz{sizeof(allocated)};

  sz = sizeof(unsigned);
  unsigned n_arenas{0};
  int err = mallctl("opt.narenas", (void *)&n_arenas, &sz, nullptr, 0);

  if (err) {
    return;
  }

  spdlog::trace("n areanas {}", n_arenas);

  if (nullptr != old_hooks) {
    return;
  }

  for (int i = 0; i < n_arenas; i++) {
    /*

    arena_allocations.emplace_back(0);
    arena_upper_limit.emplace_back(0);
    tracking_arenas.emplace_back(0);
    */
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    size_t hooks_len = sizeof(old_hooks);

    int err = mallctl(func_name.c_str(), &old_hooks, &hooks_len, nullptr, 0);

    if (err) {
      LOG_FATAL("Error getting hooks for jemalloc arena {}", i);
    }

    // Due to the way jemalloc works, we need first to set their hooks
    // which will trigger creating arena, then we can set our custom hook wrappers

    err = mallctl(func_name.c_str(), nullptr, nullptr, &old_hooks, sizeof(old_hooks));

    if (err) {
      LOG_FATAL("Error setting jemalloc hooks for jemalloc arena {}", i);
    }

    err = mallctl(func_name.c_str(), nullptr, nullptr, &new_hooks, sizeof(new_hooks));

    if (err) {
      LOG_FATAL("Error setting custom hooks for jemalloc arena {}", i);
    }
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

#endif
}

// TODO this can be designed if we fail setting hooks to rollback to classic jemalloc tracker
void UnSetHooks() {
#if USE_JEMALLOC

  uint64_t allocated{0};
  uint64_t sz{sizeof(allocated)};

  sz = sizeof(unsigned);
  unsigned n_arenas{0};
  int err = mallctl("opt.narenas", (void *)&n_arenas, &sz, nullptr, 0);

  if (err) {
    return;
  }

  spdlog::trace("n areanas {}", n_arenas);

  for (int i = 0; i < n_arenas; i++) {
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    err = mallctl(func_name.c_str(), nullptr, nullptr, &old_hooks, sizeof(old_hooks));

    if (err) {
      LOG_FATAL("Error setting jemalloc hooks for jemalloc arena {}", i);
    }
  }

#endif
}

void PurgeUnusedMemory() {
#if USE_JEMALLOC
  mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#endif
}

#undef STRINGIFY
#undef STRINGIFY_HELPER

}  // namespace memgraph::memory
