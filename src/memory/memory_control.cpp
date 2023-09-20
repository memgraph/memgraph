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
#include <mutex>
#include "utils/logging.hpp"
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

// TODO: think how to solve issue of updating memory status if negative, probably we shouldn't care as it will jump to
// positive pretty quickly
// std::mutex m;

struct ExtentHooksStats {
  struct Alloc {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Dalloc {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Destroy {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct PurgeForced {
    std::atomic<uint64_t> counter{0};
  };

  struct PurgeLazy {
    std::atomic<uint64_t> counter{0};
  };

  struct Merge {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Split {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Commit {
    std::atomic<uint64_t> counter{0};
  };

  struct Decommit {
    std::atomic<uint64_t> counter{0};
  };

  Alloc alloc;
  Dalloc dalloc;
  Destroy destroy;
  PurgeForced purge_forced;
  PurgeLazy purge_lazy;
  Merge merge;
  Split split;
  Commit commit;
  Decommit decommit;
};

ExtentHooksStats extent_hook_stats;

enum JemallocLoggingLevel { LOW, HIGH };

JemallocLoggingLevel jemalloc_logging_level = JemallocLoggingLevel::LOW;

void PrintStats() {
  std::cout << "[ALLOC][TOTAL] total memory: "
            << utils::GetReadableSize(allocated_memory.load(std::memory_order_relaxed)) << std::endl;
  std::cout << "[TOTAL][VIRTUAL] total memory: "
            << utils::GetReadableSize(virtual_allocated_memory.load(std::memory_order_relaxed)) << std::endl;

  std::cout << "[ALLOC]commited: " << extent_hook_stats.alloc.commited.load(std::memory_order_relaxed)
            << "[ALLOC] uncommited: " << extent_hook_stats.alloc.uncommited.load(std::memory_order_relaxed)
            << std::endl;
  std::cout << "[DALLOC]commited: " << extent_hook_stats.dalloc.commited.load(std::memory_order_relaxed)
            << "[DALLOC] uncommited: " << extent_hook_stats.dalloc.uncommited.load(std::memory_order_relaxed)
            << std::endl;
  std::cout << "[DESTROY]commited: " << extent_hook_stats.destroy.commited.load(std::memory_order_relaxed)
            << "[DESTROY] uncommited: " << extent_hook_stats.destroy.uncommited.load(std::memory_order_relaxed)
            << std::endl;

  std::cout << "[purge_forced]: " << extent_hook_stats.purge_forced.counter.load(std::memory_order_relaxed)
            << std::endl;
  std::cout << "[purge_lazy]: " << extent_hook_stats.purge_lazy.counter.load(std::memory_order_relaxed) << std::endl;
  std::cout << "[commit]: " << extent_hook_stats.commit.counter.load(std::memory_order_relaxed) << std::endl;
  std::cout << "[decommit]: " << extent_hook_stats.decommit.counter.load(std::memory_order_relaxed) << std::endl;

  std::cout << "[split]commited: " << extent_hook_stats.split.commited.load(std::memory_order_relaxed)
            << "[split] uncommited: " << extent_hook_stats.split.uncommited.load(std::memory_order_relaxed)
            << std::endl;
  std::cout << "[merge]commited: " << extent_hook_stats.merge.commited.load(std::memory_order_relaxed)
            << "[merge] uncommited: " << extent_hook_stats.merge.uncommited.load(std::memory_order_relaxed)
            << std::endl;
}

void *my_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit,
               unsigned arena_ind) {
  MG_ASSERT(size % 4096 == 0, "Size not multiple of page size!");
  if (*commit) {
    if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
      std::cout << "[ALLOC][RAM] memory pages: " << size / 4096UL
                << ", which equals to: " << utils::GetReadableSize(size)
                << ", of allignment: " << utils::GetReadableSize(alignment) << std::endl;
    }

    allocated_memory.fetch_add(static_cast<int64_t>(size), std::memory_order_relaxed);
    extent_hook_stats.alloc.commited.fetch_add(1, std::memory_order_relaxed);

  } else {
    if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
      std::cout << "[ALLOC][VIR] memory pages: " << size / 4096UL << ", of size: " << utils::GetReadableSize(size)
                << ", of allignment: " << utils::GetReadableSize(alignment) << std::endl;
    }

    virtual_allocated_memory.fetch_add(static_cast<int64_t>(size), std::memory_order_relaxed);
    extent_hook_stats.alloc.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
    PrintStats();
  }
  // TODO: THIS CAN ACTUALLY BE REMOVED, only use pointer to old hooks, that is it.
  // You don't have hooks per arena code
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  return original_hooks_vec[arena_ind]->alloc(extent_hooks, new_addr, size, alignment, zero, commit, arena_ind);
}

static bool my_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  MG_ASSERT(size % 4096 == 0, "Dalloc, size not multiple of page size!");
  if (committed) {
    allocated_memory.fetch_sub(static_cast<int64_t>(size));
    extent_hook_stats.dalloc.commited.fetch_add(1, std::memory_order_relaxed);

    if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
      std::cout << "[DALLOC][RAM] memory pages: " << size / 4096UL
                << ", which equals to: " << utils::GetReadableSize(size) << std::endl;
    }

  } else {
    virtual_allocated_memory.fetch_sub(static_cast<int64_t>(size));
    extent_hook_stats.dalloc.uncommited.fetch_add(1, std::memory_order_relaxed);
  }

  if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
    PrintStats();
  }

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  return old_hooks->dalloc(extent_hooks, addr, size, committed, arena_ind);
}

static void my_destroy(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  MG_ASSERT(size % 4096 == 0, "Destroy, size not multiple of page size!");
  if (committed) {
    allocated_memory.fetch_sub(static_cast<int64_t>(size), std::memory_order_relaxed);
    extent_hook_stats.destroy.commited.fetch_add(1, std::memory_order_relaxed);

    if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
      std::cout << "[DESTROY][RAM] memory pages: " << size / 4096UL
                << ", which equals to: " << utils::GetReadableSize(size) << std::endl;
    }

  } else {
    virtual_allocated_memory.fetch_sub(static_cast<int64_t>(size), std::memory_order_relaxed);
    extent_hook_stats.destroy.commited.fetch_add(1, std::memory_order_relaxed);
  }
  if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
    PrintStats();
  }
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  old_hooks->destroy(extent_hooks, addr, size, committed, arena_ind);
}

static bool my_commit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                      unsigned arena_ind) {
  extent_hook_stats.commit.counter.fetch_add(1, std::memory_order_relaxed);

  // TODO: check is this correct behavior
  virtual_allocated_memory.fetch_sub(static_cast<int64_t>(size));
  allocated_memory.fetch_add(static_cast<int64_t>(size), std::memory_order_relaxed);

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  return old_hooks->commit(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_decommit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
  extent_hook_stats.decommit.counter.fetch_add(1, std::memory_order_relaxed);

  // TODO: check is this correct behavior
  virtual_allocated_memory.fetch_add(static_cast<int64_t>(size));
  allocated_memory.fetch_sub(static_cast<int64_t>(size), std::memory_order_relaxed);

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  return old_hooks->decommit(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_purge_forced(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                            unsigned arena_ind) {
  allocated_memory.fetch_sub(static_cast<int64_t>(size), std::memory_order_relaxed);
  extent_hook_stats.purge_forced.counter.fetch_add(1, std::memory_order_relaxed);

  if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
    std::cout << "[PURGE_FORCED][RAM] memory pages: " << size / 4096UL
              << ", which equals to: " << utils::GetReadableSize(size) << std::endl;
    PrintStats();
  }

  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->purge_forced);
  return original_hooks_vec[arena_ind]->purge_forced(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_purge_lazy(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  allocated_memory.fetch_sub(static_cast<int64_t>(size), std::memory_order_relaxed);

  extent_hook_stats.purge_lazy.counter.fetch_add(1, std::memory_order_relaxed);

  if (jemalloc_logging_level == JemallocLoggingLevel::HIGH) {
    std::cout << "[PURGE_LAZY][RAM] memory pages: " << size / 4096UL
              << ", which equals to: " << utils::GetReadableSize(size) << std::endl;
    PrintStats();
  }

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

  for (int i = 0; i <= narenas; i++) {
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    size_t hooks_len = sizeof(old_hooks);
    // int err = mallctlRead<extent_hooks_t *, true>(func_name.c_str(), &old_hooks);
    int err = mallctl(func_name.c_str(), &old_hooks, &hooks_len, nullptr, 0);

    if (err) {
      std::cout << "error getting hooks for arena: " << i << std::endl;
      continue;
    }
    original_hooks_vec.emplace_back(old_hooks);

    // mallctlWrite<extent_hooks_t *, true>(func_name.c_str(), &custom_hooks)
    err = mallctl(func_name.c_str(), nullptr, nullptr, &old_hooks, sizeof(old_hooks));

    if (err) {
      std::cout << "error writing old hooks" << std::endl;
    }

    err = mallctl(func_name.c_str(), nullptr, nullptr, &new_hooks, sizeof(new_hooks));

    if (err) {
      std::cout << "error writing new hooks" << std::endl;
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
