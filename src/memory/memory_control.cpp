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
#include <atomic>
#include <cstdint>
#include <ios>
#include <mutex>
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

static std::vector<extent_hooks_t *> original_hooks_vec;

extent_hooks_t *old_hooks = nullptr;

extent_alloc_t *old_alloc = nullptr;

std::vector<int64_t> arena_allocations{};
std::vector<int64_t> arena_upper_limit{};
std::vector<int> tracking_arenas{};

ExtentHooksStats extent_hook_stats;

int GetArenaForThread() {
#if USE_JEMALLOC
  unsigned thread_arena{0};
  size_t size_thread_arena = sizeof(thread_arena);
  int err = mallctl("thread.arena", &thread_arena, &size_thread_arena, nullptr, 0);
  if (err) {
    return -1;
  }
  return static_cast<int>(thread_arena);
#endif
  return -1;
}

void TrackMemoryForThread(int arena_id, size_t size) {
  tracking_arenas[arena_id] = true;
  arena_upper_limit[arena_id] = arena_allocations[arena_id] + size;
}

void PrintJemallocInternalStats() {
  bool config_stats{false};
  size_t size_of_config_stats = sizeof(config_stats);

  int err = mallctl("config.stats", &config_stats, &size_of_config_stats, nullptr, 0);

  if (err) {
    std::cout << "can't get config.stats" << std::endl;
  }
  std::cout << "CONFIG:STATS: " << std::boolalpha << config_stats << std::endl;

  if (!config_stats) {
    return;
  }

  {
    size_t mapped{0};
    size_t size_of_mapped = sizeof(mapped);
    int err = mallctl("stats.mapped", &mapped, &size_of_mapped, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.mapped" << std::endl;
    }

    std::cout << "STATS:MAPPED: " << utils::GetReadableSize(mapped) << std::endl;
  }

  {
    size_t active{0};
    size_t size_of_active = sizeof(active);
    int err = mallctl("stats.active", &active, &size_of_active, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.active" << std::endl;
    }

    std::cout << "STATS:ACTIVE: " << utils::GetReadableSize(active) << std::endl;
  }

  {
    size_t metadata{0};
    size_t size_of_metadata = sizeof(metadata);
    int err = mallctl("stats.metadata", &metadata, &size_of_metadata, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.metadata" << std::endl;
    }

    std::cout << "STATS:METADATA: " << utils::GetReadableSize(metadata) << std::endl;
  }

  {
    size_t metadata_thp{0};
    size_t size_of_metadata_thp = sizeof(metadata_thp);
    int err = mallctl("stats.metadata", &metadata_thp, &size_of_metadata_thp, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.metadata_thp" << std::endl;
    }

    std::cout << "STATS:metadata_thp: " << utils::GetReadableSize(metadata_thp) << std::endl;
  }

  {
    size_t resident{0};
    size_t size_of_resident = sizeof(resident);
    int err = mallctl("stats.resident", &resident, &size_of_resident, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.resident" << std::endl;
    }

    std::cout << "STATS:resident: " << utils::GetReadableSize(resident) << std::endl;
  }

  {
    size_t retained{0};
    size_t size_of_retained = sizeof(retained);
    int err = mallctl("stats.retained", &retained, &size_of_retained, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.retained" << std::endl;
    }

    std::cout << "STATS:retained: " << utils::GetReadableSize(retained) << std::endl;
  }

  {
    size_t allocated{0};
    size_t size_of_allocated = sizeof(allocated);
    int err = mallctl("stats.allocated", &allocated, &size_of_allocated, nullptr, 0);

    if (err) {
      std::cout << "can't get stats.allocated" << std::endl;
    }

    std::cout << "STATS:allocated: " << utils::GetReadableSize(allocated) << std::endl;
  }
}

void PrintStats() {
  /*

  std::cout << "[TOTAL] RAM:" << utils::GetReadableSize(utils::total_memory_tracker.Amount())
            << ", VIRT: " << utils::GetReadableSize(utils::total_memory_tracker.AmountVirt());

  auto alloc_commited = extent_hook_stats.alloc.commited.load(std::memory_order_relaxed);
  auto alloc_uncommited = extent_hook_stats.alloc.uncommited.load(std::memory_order_relaxed);

  auto dalloc_commited = extent_hook_stats.dalloc.commited.load(std::memory_order_relaxed);
  auto dalloc_uncommited = extent_hook_stats.dalloc.uncommited.load(std::memory_order_relaxed);

  std::cout << "[ALLOC] commited: " << alloc_commited << ", "
            << "uncommited: " << alloc_uncommited << ", "
            << "[DALLOC] commited: " << dalloc_commited << ", "
            << "uncommited: " << dalloc_uncommited << std::endl;

  auto destroy_commited = extent_hook_stats.destroy.commited.load(std::memory_order_relaxed);
  auto destroy_uncommited = extent_hook_stats.destroy.uncommited.load(std::memory_order_relaxed);

  if (destroy_commited || destroy_uncommited) {
    std::cout << "[DESTROY] commited: " << destroy_commited << "uncommited: " << destroy_uncommited << std::endl;
  }

  auto purge_forced = extent_hook_stats.purge_forced.counter.load(std::memory_order_relaxed);
  auto purge_lazy = extent_hook_stats.purge_lazy.counter.load(std::memory_order_relaxed);
  if (purge_forced || purge_lazy) {
    std::cout << "[PURGE] forced: " << purge_forced << ",  lazy " << purge_lazy << std::endl;
  }

  auto commit_cnt = extent_hook_stats.commit.counter.load(std::memory_order_relaxed);
  auto decommit_cnt = extent_hook_stats.decommit.counter.load(std::memory_order_relaxed);

  if (commit_cnt || decommit_cnt) {
    std::cout << "COMMIT: " << commit_cnt << ", DECOMMIT: " << decommit_cnt << std::endl;
  }

  auto split_commited = extent_hook_stats.split.commited.load(std::memory_order_relaxed);
  auto split_uncommited = extent_hook_stats.split.uncommited.load(std::memory_order_relaxed);
  if (split_commited || split_uncommited) {
    std::cout << "[SPLIT] commited: "
              << ", uncommited: " << split_uncommited << std::endl;
  }

  auto merge_commited = extent_hook_stats.merge.commited.load(std::memory_order_relaxed);
  auto merge_uncommited = extent_hook_stats.merge.uncommited.load(std::memory_order_relaxed);

  if (merge_commited || merge_uncommited) {
    std::cout << "[merge]commited: " << merge_commited << ", uncommited: " << merge_uncommited << std::endl;
  }
  */
}

void *my_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit,
               unsigned arena_ind) {
  // dangerous assert, useful for testing
  MG_ASSERT(size % 4096 == 0, "Alloc size not multiple of page size");

  // TODO: THIS CAN ACTUALLY BE REMOVED, only use pointer to old hooks, that is it.
  // You don't have hooks per arena code
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);

  // This needs to be before, to trow exception in case of too big alloc
  if (*commit) {
    arena_allocations[arena_ind] += size;
    if (tracking_arenas[arena_ind]) {
      if (arena_allocations[arena_ind] > arena_upper_limit[arena_ind]) {
        throw utils::OutOfMemoryException(
            fmt::format("Memory limit exceeded! Attempting to allocate a chunk of {} which would put the current "
                        "use to {}, while the maximum allowed size for allocation is set to {}.",
                        utils::GetReadableSize(size), utils::GetReadableSize(arena_allocations[arena_ind]),
                        utils::GetReadableSize(arena_upper_limit[arena_ind])));
      }
    }
    memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
  } else {
    memgraph::utils::total_memory_tracker.AllocVirt(static_cast<int64_t>(size));
  }

  auto *ptr = original_hooks_vec[arena_ind]->alloc(extent_hooks, new_addr, size, alignment, zero, commit, arena_ind);
  if (ptr == nullptr) {
    if (*commit) {
      arena_allocations[arena_ind] -= size;
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    } else {
      memgraph::utils::total_memory_tracker.FreeVirt(static_cast<int64_t>(size));
    }
    return ptr;
  }

  if (*commit) {
    ////spdlog::trace(fmt::format("[ALLOC][RAM] memory pages: {} ,equals to: {}", size / 4096UL,
    /// utils::GetReadableSize(size)));

    extent_hook_stats.alloc.commited.fetch_add(1, std::memory_order_relaxed);

  } else {
    ////spdlog::trace(fmt::format("[ALLOC][VIRT] memory pages: {} ,equals to: {}", size / 4096UL,
    /// utils::GetReadableSize(size)));
    extent_hook_stats.alloc.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  PrintStats();

  return ptr;
}

static bool my_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  // dangerous assert, useful for testing
  MG_ASSERT(size % 4096 == 0, "Dalloc size not multiple of page size!");

  // MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->dalloc);
  auto err = old_hooks->dalloc(extent_hooks, addr, size, committed, arena_ind);

  if (err) {
    extent_hook_stats.dalloc.error.fetch_add(1);
    return err;
  }

  if (committed) {
    memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    arena_allocations[arena_ind] -= size;
    extent_hook_stats.dalloc.commited.fetch_add(1, std::memory_order_relaxed);
    // spdlog::trace(fmt::format("[DALLOC][RAM] memory pages: {} ,equals to: {}", size / 4096UL,
    // utils::GetReadableSize(size)));
  } else {
    memgraph::utils::total_memory_tracker.FreeVirt(static_cast<int64_t>(size));
    extent_hook_stats.dalloc.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  PrintStats();

  return false;
}

static void my_destroy(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  MG_ASSERT(size % 4096 == 0, "Destroy size not multiple of page size!");

  if (committed) {
    // spdlog::trace(fmt::format("[DESTROY][RAM] memory pages: {}, size: {} ", size / 4096UL,
    // utils::GetReadableSize(size)));

    memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    arena_allocations[arena_ind] -= size;
    extent_hook_stats.destroy.commited.fetch_add(1, std::memory_order_relaxed);
  } else {
    // spdlog::trace(fmt::format("[DESTROY][VIRT] memory pages: {}, size: {} ", size / 4096UL,
    // utils::GetReadableSize(size)));
    memgraph::utils::total_memory_tracker.FreeVirt(static_cast<int64_t>(size));
    extent_hook_stats.destroy.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  PrintStats();

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  old_hooks->destroy(extent_hooks, addr, size, committed, arena_ind);
}

static bool my_commit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                      unsigned arena_ind) {
  MG_ASSERT(length % 4096 == 0, "Commit not multiple of page size");

  // TODO change to use old hooks
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->commit);
  auto err = original_hooks_vec[arena_ind]->commit(extent_hooks, addr, size, offset, length, arena_ind);

  if (err) {
    return err;
  }

  extent_hook_stats.commit.counter.fetch_add(1, std::memory_order_relaxed);

  // spdlog::trace(fmt::format("[COMMIT][RAM] memory pages: {}, size: {} ", length / 4096UL,
  // utils::GetReadableSize(length)));
  memgraph::utils::total_memory_tracker.FreeVirt(static_cast<int64_t>(length));
  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(length));

  return false;
}

static bool my_decommit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  auto err = old_hooks->decommit(extent_hooks, addr, size, offset, length, arena_ind);

  extent_hook_stats.decommit.counter.fetch_add(1, std::memory_order_relaxed);
  if (err) {
    return err;
  }

  memgraph::utils::total_memory_tracker.AllocVirt(static_cast<int64_t>(length));
  memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(length));
  // spdlog::trace(fmt::format("[DECOMMIT][RAM] memory pages: {}, size: {} ", length / 4096UL,
  // utils::GetReadableSize(length)));
  //  TODO: check is this correct behavior

  return false;
}

static bool my_purge_forced(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                            unsigned arena_ind) {
  extent_hook_stats.purge_forced.counter.fetch_add(1, std::memory_order_relaxed);

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->purge_forced);
  auto err = original_hooks_vec[arena_ind]->purge_forced(extent_hooks, addr, size, offset, length, arena_ind);

  // if (err) {
  //   return err;
  // }
  // spdlog::trace(fmt::format("[PURGE F][RAM] memory pages: {}, size: {} ", length / 4096UL,
  // utils::GetReadableSize(length)));
  memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(length));

  return false;
}

static bool my_purge_lazy(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  // If memory is purged lazily, it will not be cleaned immediatelly if we are not using MADVISE_DONTNEED (muzzy=0 and
  // decay=0)

  extent_hook_stats.purge_lazy.counter.fetch_add(1, std::memory_order_relaxed);

  // spdlog::trace(fmt::format("[PURGE L][RAM] memory pages: {}, size: {} ", length / 4096UL,
  // utils::GetReadableSize(length)));

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->purge_lazy);
  return original_hooks_vec[arena_ind]->purge_lazy(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_split(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t size_a, size_t size_b,
                     bool committed, unsigned arena_ind) {
  if (committed) {
    extent_hook_stats.split.commited.fetch_add(1, std::memory_order_relaxed);
  } else {
    extent_hook_stats.split.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->split);
  return original_hooks_vec[arena_ind]->split(extent_hooks, addr, size, size_a, size_b, committed, arena_ind);
}

static bool my_merge(extent_hooks_t *extent_hooks, void *addr_a, size_t size_a, void *addr_b, size_t size_b,
                     bool committed, unsigned arena_ind) {
  if (committed) {
    extent_hook_stats.merge.commited.fetch_add(1, std::memory_order_relaxed);
  } else {
    extent_hook_stats.merge.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->merge);
  return original_hooks_vec[arena_ind]->merge(extent_hooks, addr_a, size_a, addr_b, size_b, committed, arena_ind);
}

static extent_hooks_t custom_hooks = {
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

static extent_hooks_t *new_hooks = &custom_hooks;

// TODO this can be designed if we fail setting hooks to rollback to classic jemalloc tracker
void SetHooks() {
#if USE_JEMALLOC

  uint64_t allocated{0};
  uint64_t sz{sizeof(allocated)};

  sz = sizeof(unsigned);
  unsigned narenas{0};
  int err = mallctl("opt.narenas", (void *)&narenas, &sz, nullptr, 0);

  if (err) {
    return;
  }

  std::cout << narenas << " : n arenas" << std::endl;

  if (nullptr != old_hooks) {
    return;
  }

  // get original hooks and update alloc
  original_hooks_vec.reserve(narenas + 5);

  for (int i = 0; i < narenas; i++) {
    arena_allocations.emplace_back(0);
    arena_upper_limit.emplace_back(0);
    tracking_arenas.emplace_back(0);
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    size_t hooks_len = sizeof(old_hooks);
    // int err = mallctlRead<extent_hooks_t *, true>(func_name.c_str(), &old_hooks);
    int err = mallctl(func_name.c_str(), &old_hooks, &hooks_len, nullptr, 0);

    if (err) {
      LOG_FATAL("Error getting hooks for jemalloc arena {}", i);
    }
    original_hooks_vec.emplace_back(old_hooks);

    // Due to the way jemalloc works, we need first to set their hooks
    // which will trigger creating arena, then we can set our custom hook wrappers

    err = mallctl(func_name.c_str(), nullptr, nullptr, &old_hooks, sizeof(old_hooks));
    // mallctlWrite<extent_hooks_t *, true>(func_name.c_str(), &old_hooks)

    if (err) {
      LOG_FATAL("Error setting jemalloc hooks for jemalloc arena {}", i);
    }

    err = mallctl(func_name.c_str(), nullptr, nullptr, &new_hooks, sizeof(new_hooks));

    if (err) {
      LOG_FATAL("Error setting custom hooks for jemalloc arena {}", i);
    }
  }

#endif
}

// TODO this can be designed if we fail setting hooks to rollback to classic jemalloc tracker
void UnSetHooks() {
#if USE_JEMALLOC

  uint64_t allocated{0};
  uint64_t sz{sizeof(allocated)};

  sz = sizeof(unsigned);
  unsigned narenas{0};
  int err = mallctl("opt.narenas", (void *)&narenas, &sz, nullptr, 0);

  if (err) {
    return;
  }

  std::cout << narenas << " : n arenas" << std::endl;

  for (int i = 0; i < narenas; i++) {
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

  std::cout << "PURGE CALLED" << std::endl;
  PrintStats();
}

#undef STRINGIFY
#undef STRINGIFY_HELPER

}  // namespace memgraph::memory
