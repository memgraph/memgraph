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

#include <new>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#else
#include <malloc.h>
#include <cstdlib>
#endif

#include "utils/memory_tracker.hpp"

extern "C" void dallocx(void *ptr, int flags);
extern "C" void sdallocx(void *ptr, size_t size, int flags);
namespace {
inline void *newImpl(const std::size_t size) {
  auto *ptr = malloc(size);
  if (ptr != nullptr) [[likely]] {
    return ptr;
  }

  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  auto maybe_msg = memgraph::utils::MemoryErrorStatus().msg();
  if (maybe_msg) {
    throw memgraph::utils::OutOfMemoryException{std::move(*maybe_msg)};
  }

  throw std::bad_alloc{};
}

inline void *newImpl(const std::size_t size, const std::align_val_t align) {
  auto *ptr = aligned_alloc(static_cast<std::size_t>(align), size);
  if (ptr != nullptr) [[likely]] {
    return ptr;
  }

  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  auto maybe_msg = memgraph::utils::MemoryErrorStatus().msg();
  if (maybe_msg) {
    throw memgraph::utils::OutOfMemoryException{std::move(*maybe_msg)};
  }

  throw std::bad_alloc{};
}

inline void *newNoExcept(const std::size_t size) noexcept {
  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  return malloc(size);
}

inline void *newNoExcept(const std::size_t size, const std::align_val_t align) noexcept {
  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  return aligned_alloc(size, static_cast<std::size_t>(align));
}

#if USE_JEMALLOC
inline void deleteImpl(void *ptr) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  dallocx(ptr, 0);
}

inline void deleteImpl(void *ptr, const std::align_val_t align) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  dallocx(ptr, MALLOCX_ALIGN(align));  // NOLINT(hicpp-signed-bitwise)
}

inline void deleteSized(void *ptr, const std::size_t size) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }

  sdallocx(ptr, size, 0);
}

inline void deleteSized(void *ptr, const std::size_t size, const std::align_val_t align) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }

  sdallocx(ptr, size, MALLOCX_ALIGN(align));  // NOLINT(hicpp-signed-bitwise)
}

#else
inline void deleteImpl(void *ptr) noexcept { free(ptr); }

inline void deleteImpl(void *ptr, const std::align_val_t /*unused*/) noexcept { free(ptr); }

inline void deleteSized(void *ptr, const std::size_t /*unused*/) noexcept { free(ptr); }

inline void deleteSized(void *ptr, const std::size_t /*unused*/, const std::align_val_t /*unused*/) noexcept {
  free(ptr);
}
#endif

inline void TrackMemory(std::size_t size) {
#if !USE_JEMALLOC
  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
#endif
}

inline void TrackMemory(std::size_t size, const std::align_val_t align) {
#if !USE_JEMALLOC
  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
#endif
}

inline bool TrackMemoryNoExcept(const std::size_t size) {
  try {
    TrackMemory(size);
  } catch (...) {
    return false;
  }

  return true;
}

inline bool TrackMemoryNoExcept(const std::size_t size, const std::align_val_t align) {
  try {
    TrackMemory(size, align);
  } catch (...) {
    return false;
  }

  return true;
}

inline void UntrackMemory([[maybe_unused]] void *ptr, [[maybe_unused]] std::size_t size = 0) noexcept {
  try {
#if !USE_JEMALLOC
    if (size) {
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    } else {
      // Innaccurate because malloc_usable_size() result is greater or equal to allocated size.
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(malloc_usable_size(ptr)));
    }
#endif
  } catch (...) {
  }
}

inline void UntrackMemory(void *ptr, const std::align_val_t align, [[maybe_unused]] std::size_t size = 0) noexcept {
  try {
#if !USE_JEMALLOC
    if (size) {
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(size));
    } else {
      // Innaccurate because malloc_usable_size() result is greater or equal to allocated size.
      memgraph::utils::total_memory_tracker.Free(static_cast<int64_t>(malloc_usable_size(ptr)));
    }
#endif
  } catch (...) {
  }
}

}  // namespace

__attribute__((visibility("default"))) void *operator new(const std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

__attribute__((visibility("default"))) void *operator new[](const std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

__attribute__((visibility("default"))) void *operator new(const std::size_t size, const std::align_val_t align) {
  TrackMemory(size, align);
  return newImpl(size, align);
}

__attribute__((visibility("default"))) void *operator new[](const std::size_t size, const std::align_val_t align) {
  TrackMemory(size, align);
  return newImpl(size, align);
}

__attribute__((visibility("default"))) void *operator new(const std::size_t size,
                                                          const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size)) [[likely]] {
    return newNoExcept(size);
  }
  return nullptr;
}

__attribute__((visibility("default"))) void *operator new[](const std::size_t size,
                                                            const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size)) [[likely]] {
    return newNoExcept(size);
  }
  return nullptr;
}

__attribute__((visibility("default"))) void *operator new(const std::size_t size, const std::align_val_t align,
                                                          const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size, align)) [[likely]] {
    return newNoExcept(size, align);
  }
  return nullptr;
}

__attribute__((visibility("default"))) void *operator new[](const std::size_t size, const std::align_val_t align,
                                                            const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size, align)) [[likely]] {
    return newNoExcept(size, align);
  }
  return nullptr;
}

__attribute__((visibility("default"))) void operator delete(void *ptr) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

__attribute__((visibility("default"))) void operator delete(void *ptr, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

__attribute__((visibility("default"))) void operator delete(void *ptr, const std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr, const std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

__attribute__((visibility("default"))) void operator delete(void *ptr, const std::size_t size,
                                                            const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align, size);
  deleteSized(ptr, size, align);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr, const std::size_t size,
                                                              const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align, size);
  deleteSized(ptr, size, align);
}

__attribute__((visibility("default"))) void operator delete(void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

__attribute__((visibility("default"))) void operator delete(void *ptr, const std::align_val_t align,
                                                            const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

__attribute__((visibility("default"))) void operator delete[](void *ptr, const std::align_val_t align,
                                                              const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}
