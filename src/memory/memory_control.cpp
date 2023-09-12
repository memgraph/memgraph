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
#include "utils/logging.hpp"

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

void *my_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size, size_t alignment, bool *zero, bool *commit,
               unsigned arena_ind) {
  MG_ASSERT(original_hooks_vec[arena_ind] && original_hooks_vec[arena_ind]->alloc);
  return original_hooks_vec[arena_ind]->alloc(extent_hooks, new_addr, size, alignment, zero, commit, arena_ind);
}

static bool my_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  return old_hooks->dalloc(extent_hooks, addr, size, committed, arena_ind);
}

static void my_destroy(extent_hooks_t *extent_hooks, void *addr, size_t size, bool committed, unsigned arena_ind) {
  old_hooks->destroy(extent_hooks, addr, size, committed, arena_ind);
}

static bool my_commit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                      unsigned arena_ind) {
  return old_hooks->commit(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_decommit(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                        unsigned arena_ind) {
  return old_hooks->decommit(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_purge_forced(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                            unsigned arena_ind) {
  return old_hooks->purge_forced(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_purge_lazy(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  return old_hooks->purge_lazy(extent_hooks, addr, size, offset, length, arena_ind);
}

static bool my_split(extent_hooks_t *extent_hooks, void *addr, size_t size, size_t size_a, size_t size_b,
                     bool committed, unsigned arena_ind) {
  MG_ASSERT(old_hooks && old_hooks->split);
  return old_hooks->split(extent_hooks, addr, size, size_a, size_b, committed, arena_ind);
}

static bool my_merge(extent_hooks_t *extent_hooks, void *addr_a, size_t size_a, void *addr_b, size_t size_b,
                     bool committed, unsigned arena_ind) {
  return old_hooks->merge(extent_hooks, addr_a, size_a, addr_b, size_b, committed, arena_ind);
}

// static extent_hooks_t custom_hooks = {
//     .alloc = &my_alloc,
//     .dalloc = &my_dalloc,
//     .destroy = &my_destroy,
//     .commit = &my_commit,
//     .decommit = &my_decommit,
//     .purge_lazy = &my_purge_lazy,
//     .purge_forced = &my_purge_forced,
//     .split = &my_split,
//     .merge = &my_merge,
// };

// static extent_hooks_t custom_hooks = {
//     .alloc = nullptr,
//     .dalloc = nullptr,
//     .destroy = nullptr,
//     .commit = nullptr,
//     .decommit = nullptr,
//     .purge_lazy =nullptr,
//     .purge_forced = nullptr,
//     .split = nullptr,
//     .merge = nullptr
// };

static extent_hooks_t custom_hooks;

static extent_hooks_t *new_hooks = &custom_hooks;

void PrintStats() {
#if USE_JEMALLOC
  uint64_t allocated{0};
  uint64_t sz{sizeof(allocated)};
  // mallctl("stats.allocated", &allocated, &sz, nullptr, 0);
  // std::cout << "stats allocated:" << allocated << ", sz: " << std::endl;

  // mallctl("stats.arenas." STRINGIFY(MALLCTL_ARENAS_ALL) ".pactive", &allocated, &sz, nullptr, 0);
  // std::cout << "stats allocated:" << allocated << ", sz: " << std::endl;

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
  original_hooks_vec.reserve(narenas);
  for (int i = 0; i < narenas; i++) {
    std::string func_name = "arena." + std::to_string(i) + ".extent_hooks";

    size_t hooks_len = sizeof(old_hooks);
    int err = mallctl(func_name.c_str(), &old_hooks, &hooks_len, nullptr, 0);

    if (err) {
      std::cout << "error getting hooks for arena: " << i << std::endl;
      continue;
    }
    original_hooks_vec.emplace_back(old_hooks);

    custom_hooks = *old_hooks;
    custom_hooks.alloc = &my_alloc;

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

  PrintStats();
}

#undef STRINGIFY
#undef STRINGIFY_HELPER

}  // namespace memgraph::memory

// int err = mallctlRead<extent_hooks_t *, true>(func_name.c_str(), &old_hooks);

//  if (mallctlWrite<extent_hooks_t *, true>(func_name.c_str(), &custom_hooks)) {
//       std::cout << "error writting hook" << std::endl;
//     }

// for(int i=0;i<narenas;i++){
// 	std::string arena_str = "arena."+std::to_string(i) + ".extent_hooks";
// 	extent_hooks_t *ptr = original_hooks_vec[i];
// 	size_t hooks_len = sizeof(ptr);
// 	if (mallctl(arena_str.c_str(), nullptr, nullptr, &new_hooks, hooks_len)) {
// 		std::cout<< "error setting new hook" << std::endl;
// 	}
// }

// my alloc part
// std::cout << fmt::format("In wrapper alloc_hook: new_addr: {}, size: {}, alignment: {}, arena_ind: {}", new_addr,
// size, alignment, arena_ind);
//  printf("In wrapper alloc_hook: new_addr:%p "
//  	"size:%lu(%lu pages) alignment:%lu "
//  	"zero:%s commit:%s arena_ind:%u\n",
//  	new_addr, size, size / 4096, alignment,
//  	(*zero) ? "true" : "false",
//  	(*commit) ? "true" : "false",
//  	arena_ind);
//  Default behavior using original hooks
