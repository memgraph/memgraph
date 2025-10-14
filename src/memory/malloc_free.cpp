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

#include "query_memory_control.hpp"
#include "utils/memory_tracker.hpp"

#if USE_JEMALLOC

#include <cerrno>
#include <cstddef>

#include <jemalloc/jemalloc.h>

namespace {
inline auto alloc_tracking(size_t size, int flags = 0) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    auto actual_size = je_nallocx(size, flags);
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

inline void free_tracking(void *ptr, int flags = 0) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    auto actual_size = je_sallocx(ptr, flags);
    memgraph::memory::TrackFreeOnCurrentThread(actual_size);
  }
}

inline auto realloc_tracking(void *ptr, size_t size, int flags = 0) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? je_sallocx(ptr, flags) : 0;
    const auto next_size = je_nallocx(size, flags);
    if (next_size > prev_size) {  // increasing memory
      return memgraph::memory::TrackAllocOnCurrentThread(next_size - prev_size);
    }
    if (next_size < prev_size) {  // decreasing memory
      memgraph::memory::TrackFreeOnCurrentThread(prev_size - next_size);
    }
  }
  return true;
}

inline void failed_realloc_tracking(void *ptr, size_t size, int flags = 0) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? je_sallocx(ptr, flags) : 0;
    const auto next_size = je_nallocx(size, flags);
    if (next_size > prev_size) {  // failed while increasing memory
      memgraph::memory::TrackFreeOnCurrentThread(next_size - prev_size);
    } else if (next_size < prev_size) {                                    // failed while decreasing memory
      memgraph::memory::TrackAllocOnCurrentThread(prev_size - next_size);  // fail silently
    }
  }
}

// Called post malloc/calloc/realloc
inline void *track_alloc(void *ptr) {
  // auto actual_size = je_malloc_usable_size(ptr);
  // if (!memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(actual_size))) [[unlikely]] {
  //   free_tracking(ptr);
  //   je_free(ptr);
  //   return nullptr;
  // }
  return ptr;
}

// Called pre free/dallocx/sdallocx
inline void track_free(void *ptr) {
  // auto actual_size = je_malloc_usable_size(ptr);
  // memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(actual_size));
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

  return track_alloc(res);
}

extern "C" void *calloc(size_t nmemb, size_t size) {
  if (!alloc_tracking(nmemb * size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_calloc(nmemb, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(nmemb * size);
  }

  return track_alloc(res);
}

extern "C" void *realloc(void *ptr, size_t size) {
  if (!realloc_tracking(ptr, size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = je_realloc(ptr, size);
  if (res == nullptr) [[unlikely]] {
    failed_realloc_tracking(ptr, size);
  }

  track_free(ptr);
  return track_alloc(res);
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

  return track_alloc(res);
}

extern "C" int posix_memalign(void **memptr, size_t alignment, size_t size) {
  const int flags = MALLOCX_ALIGN(alignment);
  if (!alloc_tracking(size, flags)) [[unlikely]] {
    return ENOMEM;
  }
  int const res = je_posix_memalign(memptr, alignment, size);
  if (res != 0) [[unlikely]] {
    failed_alloc_tracking(size, flags);
  }

  track_alloc(*memptr);
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

  return track_alloc(res);
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

  return track_alloc(res);
}

extern "C" void free(void *ptr) {
  if (!ptr) [[unlikely]]
    return;
  track_free(ptr);
  free_tracking(ptr);
  je_free(ptr);
}

extern "C" void dallocx(void *ptr, int flags) {
  if (!ptr) [[unlikely]]
    return;
  track_free(ptr);
  free_tracking(ptr, flags);
  je_dallocx(ptr, flags);
}

extern "C" void sdallocx(void *ptr, size_t size, int flags) {
  if (!ptr) [[unlikely]]
    return;
  track_free(ptr);
  free_tracking(ptr, flags);
  je_sdallocx(ptr, size, flags);
}

extern "C" size_t malloc_usable_size(void *ptr) { return je_malloc_usable_size(ptr); }

#else

#include <dlfcn.h>
#include <malloc.h>
#include <cstdlib>
#include <cstring>

namespace {
// Function pointers to the next malloc in the chain
static void *(*next_malloc)(size_t) = nullptr;
static void *(*next_calloc)(size_t, size_t) = nullptr;
static void *(*next_realloc)(void *, size_t) = nullptr;
static void (*next_free)(void *) = nullptr;
static void *(*next_aligned_alloc)(size_t, size_t) = nullptr;
static int (*next_posix_memalign)(void **, size_t, size_t) = nullptr;
static void *(*next_valloc)(size_t) = nullptr;
static void *(*next_memalign)(size_t, size_t) = nullptr;
}  // namespace

namespace {
inline auto alloc_tracking(size_t size) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    return memgraph::memory::TrackAllocOnCurrentThread(size);
  }
  return true;
}

inline void failed_alloc_tracking(size_t size) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(size);
  }
}

inline void free_tracking(size_t size) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(size);
  }
}

inline auto realloc_tracking(void *ptr, size_t size) -> bool {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? malloc_usable_size(ptr) : 0;
    if (size > prev_size) {  // increasing memory
      return memgraph::memory::TrackAllocOnCurrentThread(size - prev_size);
    }
    if (size < prev_size) {  // decreasing memory
      memgraph::memory::TrackFreeOnCurrentThread(prev_size - size);
    }
  }
  return true;
}

inline void failed_realloc_tracking(void *ptr, size_t size) {
  if (memgraph::memory::IsQueryTracked()) [[unlikely]] {
    const auto prev_size = ptr ? malloc_usable_size(ptr) : 0;
    if (size > prev_size) {  // failed while increasing memory
      memgraph::memory::TrackFreeOnCurrentThread(size - prev_size);
    }
    if (size < prev_size) {                                           // failed while decreasing memory
      memgraph::memory::TrackAllocOnCurrentThread(prev_size - size);  // fail silently
    }
  }
}

// Called post malloc/calloc/realloc
inline void *track_alloc(void *ptr) {
  auto actual_size = malloc_usable_size(ptr);
  if (!memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(actual_size))) [[unlikely]] {
    free_tracking(actual_size);
    free(ptr);
    return nullptr;
  }
  return ptr;
}

// Called pre free
inline void track_free(void *ptr) {
  auto actual_size = malloc_usable_size(ptr);
  memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(actual_size));
}
}  // namespace

extern "C" void *malloc(size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_malloc) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_malloc = reinterpret_cast<void *(*)(size_t)>(dlsym(RTLD_NEXT, "malloc"));
    // MG_ASSERT(next_malloc != nullptr, "Failed to find malloc function");
    initializing = false;
  }

  if (!alloc_tracking(size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_malloc(size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size);
  }

  return track_alloc(res);
}

extern "C" void *calloc(size_t nmemb, size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_calloc) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_calloc = reinterpret_cast<void *(*)(size_t, size_t)>(dlsym(RTLD_NEXT, "calloc"));
    // MG_ASSERT(next_calloc != nullptr, "Failed to find calloc function");
    initializing = false;
  }

  const size_t total_size = nmemb * size;
  if (!alloc_tracking(total_size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_calloc(nmemb, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(total_size);
  }

  return track_alloc(res);
}

extern "C" void *realloc(void *ptr, size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_realloc) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_realloc = reinterpret_cast<void *(*)(void *, size_t)>(dlsym(RTLD_NEXT, "realloc"));
    // MG_ASSERT(next_realloc != nullptr, "Failed to find realloc function");
    initializing = false;
  }

  if (!realloc_tracking(ptr, size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_realloc(ptr, size);
  if (res == nullptr) [[unlikely]] {
    failed_realloc_tracking(ptr, size);
  }

  track_free(ptr);
  return track_alloc(res);
}

extern "C" void *aligned_alloc(size_t alignment, size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_aligned_alloc) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_aligned_alloc = reinterpret_cast<void *(*)(size_t, size_t)>(dlsym(RTLD_NEXT, "aligned_alloc"));
    // MG_ASSERT(next_aligned_alloc != nullptr, "Failed to find aligned_alloc function");
    initializing = false;
  }

  if (!alloc_tracking(size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_aligned_alloc(alignment, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size);
  }

  return track_alloc(res);
}

extern "C" int posix_memalign(void **memptr, size_t alignment, size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_posix_memalign) {
    if (initializing) {
      return ENOMEM;
    }
    initializing = true;
    next_posix_memalign = reinterpret_cast<int (*)(void **, size_t, size_t)>(dlsym(RTLD_NEXT, "posix_memalign"));
    // MG_ASSERT(next_posix_memalign != nullptr, "Failed to find posix_memalign function");
    initializing = false;
  }

  if (!alloc_tracking(size)) [[unlikely]] {
    return ENOMEM;
  }
  int const res = next_posix_memalign(memptr, alignment, size);
  if (res != 0) [[unlikely]] {
    failed_alloc_tracking(size);
  }

  track_alloc(*memptr);
  return res;
}

extern "C" void *valloc(size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_valloc) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_valloc = reinterpret_cast<void *(*)(size_t)>(dlsym(RTLD_NEXT, "valloc"));
    // MG_ASSERT(next_valloc != nullptr, "Failed to find valloc function");
    initializing = false;
  }

  if (!alloc_tracking(size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_valloc(size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size);
  }

  return track_alloc(res);
}

extern "C" void *memalign(size_t alignment, size_t size) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_memalign) {
    if (initializing) {
      return nullptr;
    }
    initializing = true;
    next_memalign = reinterpret_cast<void *(*)(size_t, size_t)>(dlsym(RTLD_NEXT, "memalign"));
    // MG_ASSERT(next_memalign != nullptr, "Failed to find memalign function");
    initializing = false;
  }

  if (!alloc_tracking(size)) [[unlikely]] {
    return nullptr;
  }
  void *const res = next_memalign(alignment, size);
  if (res == nullptr) [[unlikely]] {
    failed_alloc_tracking(size);
  }

  return track_alloc(res);
}

extern "C" void free(void *ptr) {
  // Make sure the dlsym call is not recursive
  static std::atomic<bool> initializing = false;
  if (!next_free) {
    if (initializing) {
      return;
    }
    initializing = true;
    next_free = reinterpret_cast<void (*)(void *)>(dlsym(RTLD_NEXT, "free"));
    // MG_ASSERT(next_free != nullptr, "Failed to find free function");
    initializing = false;
  }

  if (!ptr) [[unlikely]]
    return;
  track_free(ptr);
  free_tracking(malloc_usable_size(ptr));
  next_free(ptr);
}

#endif  // USE_JEMALLOC
