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

#pragma once

#include <memory_resource>

namespace memgraph::memory {

/// A std::pmr::memory_resource backed by a dedicated jemalloc arena.
///
/// All allocations come from an arena created solely for this resource, so
/// they are isolated from all other allocations of the same size class.
/// Per-thread caches keep the hot path lock-free; cross-thread frees are
/// handled transparently by jemalloc.
///
/// When jemalloc is disabled (ASAN/TSAN builds) the class falls back to
/// operator new / operator delete, providing the same pmr interface with no
/// pooling.
class DedicatedArenaResource final : public std::pmr::memory_resource {
 public:
  DedicatedArenaResource();
  ~DedicatedArenaResource() override;

  DedicatedArenaResource(const DedicatedArenaResource &) = delete;
  DedicatedArenaResource &operator=(const DedicatedArenaResource &) = delete;
  DedicatedArenaResource(DedicatedArenaResource &&) = delete;
  DedicatedArenaResource &operator=(DedicatedArenaResource &&) = delete;

 protected:
  void *do_allocate(size_t bytes, size_t alignment) override;
  void do_deallocate(void *p, size_t bytes, size_t alignment) override;
  bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override;

 private:
#if USE_JEMALLOC
  unsigned arena_id_{0};
  int alloc_flags_{0};
#endif
};

}  // namespace memgraph::memory
