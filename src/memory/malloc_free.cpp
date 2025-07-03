// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Query memory tracking is supported only with jemalloc. Do not override malloc/free if jemalloc is not used.
#if USE_JEMALLOC

#include <cerrno>
#include <cstddef>
#include <type_traits>
#include <utility>

#include <jemalloc/jemalloc.h>

#include "query_memory_control.hpp"

namespace {
inline auto alloc_tracking(size_t size, int flags = 0) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto actual_size = je_nallocx(size, flags);
    return memgraph::memory::TrackAllocOnCurrentThread(actual_size);
  }
  return true;
}

inline void failed_alloc_tracking(size_t size, int flags = 0) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto actual_size = je_nallocx(size, flags);
    memgraph::memory::TrackFreeOnCurrentThread(actual_size);
  }
}

inline auto realloc_tracking(void *ptr, size_t size, int flags = 0) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? je_sallocx(ptr, flags) : 0;
    const auto next_size = je_nallocx(size, flags);
    if (next_size > prev_size) {  // increasing memory
      const auto actual_size = next_size - prev_size;
      return memgraph::memory::TrackAllocOnCurrentThread(actual_size);
    } else if (next_size < prev_size) {  // decreasing memory
      const auto actual_size = prev_size - next_size;
      memgraph::memory::TrackFreeOnCurrentThread(actual_size);
    }
  }
  return true;
}

inline void failed_realloc_tracking(void *ptr, size_t size, int flags = 0) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? je_sallocx(ptr, flags) : 0;
    const auto next_size = je_nallocx(size, flags);
    if (next_size > prev_size) {  // failed while increasing memory
      const auto actual_size = next_size - prev_size;
      memgraph::memory::TrackFreeOnCurrentThread(actual_size);
    } else if (next_size < prev_size) {  // failed while decreasing memory
      const auto actual_size = prev_size - next_size;
      memgraph::memory::TrackAllocOnCurrentThread(actual_size);  // fail silently
    }
  }
}

inline void free_tracking(void *ptr, int flags = 0) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto actual_size = je_sallocx(ptr, flags);
    memgraph::memory::TrackFreeOnCurrentThread(actual_size);
  }
}
}  // namespace

extern "C" void *malloc(size_t size) {
  if (!alloc_tracking(size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_malloc(size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size);
  }
  return res;
}

extern "C" void *calloc(size_t count, size_t size) {
  if (!alloc_tracking(count * size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_calloc(count, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(count * size);
  }
  return res;
}

extern "C" void *realloc(void *ptr, size_t size) {
  if (!realloc_tracking(ptr, size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_realloc(ptr, size);
  if (res == nullptr) [[unlikely]] {
    failed_realloc_tracking(ptr, size);
  }
  return res;
}

extern "C" void *aligned_alloc(size_t alignment, size_t size) {
  const int flags = MALLOCX_ALIGN(alignment);
  if (!alloc_tracking(size, flags)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_aligned_alloc(alignment, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size, flags);
  }
  return res;
}

extern "C" int posix_memalign(void **p, size_t alignment, size_t size) {
  const int flags = MALLOCX_ALIGN(alignment);
  if (!alloc_tracking(size, flags)) [[unlikely]] {
    return ENOMEM;
  }
  int const res = je_posix_memalign(p, alignment, size);
  if (res != 0) [[unlikely]] {
    failed_alloc_tracking(size, flags);
  }
  return res;
}

extern "C" void *valloc(size_t size) {
  const int flags = MALLOCX_ALIGN(4096);
  if (!alloc_tracking(size, flags)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_valloc(size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size, flags);
  }
  return res;
}

extern "C" void *memalign(size_t alignment, size_t size) {
  const int flags = MALLOCX_ALIGN(alignment);
  if (!alloc_tracking(size, flags)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_memalign(alignment, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size, flags);
  }
  return res;
}

extern "C" void free(void *ptr) {
  if (!ptr) [[unlikely]]
    return;
  free_tracking(ptr);
  je_free(ptr);
}

extern "C" void dallocx(void *ptr, int flags) {
  if (!ptr) [[unlikely]]
    return;
  free_tracking(ptr, flags);
  je_dallocx(ptr, flags);
}

extern "C" void sdallocx(void *ptr, size_t size, int flags) {
  if (!ptr) [[unlikely]]
    return;
  free_tracking(ptr, flags);
  je_sdallocx(ptr, size, flags);
}

extern "C" size_t malloc_usable_size(void *ptr) { return je_malloc_usable_size(ptr); }

#endif  // USE_JEMALLOC
