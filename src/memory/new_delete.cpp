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

#include "memory/query_memory_control.hpp"
#include "utils/memory_tracker.hpp"

namespace {
inline void *Malloc(std::size_t size) {
#ifdef USE_JEMALLOC
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    const bool ok = memgraph::memory::TrackAllocOnCurrentThread(nallocx(size, 0));
    if (!ok) return nullptr;
  }
#endif
  return malloc(size);
}

inline void *AlignedAlloc(const std::size_t size, const std::align_val_t align) {
#ifdef USE_JEMALLOC
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    const bool ok = memgraph::memory::TrackAllocOnCurrentThread(nallocx(size, MALLOCX_ALIGN(align)));
    if (!ok) return nullptr;
  }
#endif
  return aligned_alloc(static_cast<std::size_t>(align), size);
}

void *newImpl(const std::size_t size) {
  auto *ptr = Malloc(size);
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

void *newImpl(const std::size_t size, const std::align_val_t align) {
  auto *ptr = AlignedAlloc(size, align);
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

void *newNoExcept(const std::size_t size) noexcept {
  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  return Malloc(size);
}
void *newNoExcept(const std::size_t size, const std::align_val_t align) noexcept {
  [[maybe_unused]] auto blocker = memgraph::utils::MemoryTracker::OutOfMemoryExceptionBlocker{};
  return AlignedAlloc(size, align);
}

#if USE_JEMALLOC
void deleteImpl(void *ptr) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(sallocx(ptr, 0));
  }
  dallocx(ptr, 0);
}

void deleteImpl(void *ptr, const std::align_val_t align) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(sallocx(ptr, MALLOCX_ALIGN(align)));
  }
  dallocx(ptr, MALLOCX_ALIGN(align));  // NOLINT(hicpp-signed-bitwise)
}

void deleteSized(void *ptr, const std::size_t size) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(sallocx(ptr, 0));
  }
  sdallocx(ptr, size, 0);
}

void deleteSized(void *ptr, const std::size_t size, const std::align_val_t align) noexcept {
  if (ptr == nullptr) [[unlikely]] {
    return;
  }
  if (memgraph::memory::IsThreadTracked()) [[unlikely]] {
    memgraph::memory::TrackFreeOnCurrentThread(sallocx(ptr, MALLOCX_ALIGN(align)));
  }
  sdallocx(ptr, size, MALLOCX_ALIGN(align));  // NOLINT(hicpp-signed-bitwise)
}

#else
void deleteImpl(void *ptr) noexcept { free(ptr); }

void deleteImpl(void *ptr, const std::align_val_t /*unused*/) noexcept { free(ptr); }

void deleteSized(void *ptr, const std::size_t /*unused*/) noexcept { free(ptr); }

void deleteSized(void *ptr, const std::size_t /*unused*/, const std::align_val_t /*unused*/) noexcept { free(ptr); }
#endif

void TrackMemory(std::size_t size) {
#if !USE_JEMALLOC
  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
#endif
}

void TrackMemory(std::size_t size, const std::align_val_t align) {
#if !USE_JEMALLOC
  memgraph::utils::total_memory_tracker.Alloc(static_cast<int64_t>(size));
#endif
}

bool TrackMemoryNoExcept(const std::size_t size) {
  try {
    TrackMemory(size);
  } catch (...) {
    return false;
  }

  return true;
}

bool TrackMemoryNoExcept(const std::size_t size, const std::align_val_t align) {
  try {
    TrackMemory(size, align);
  } catch (...) {
    return false;
  }

  return true;
}

void UntrackMemory([[maybe_unused]] void *ptr, [[maybe_unused]] std::size_t size = 0) noexcept {
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

void UntrackMemory(void *ptr, const std::align_val_t align, [[maybe_unused]] std::size_t size = 0) noexcept {
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

void *operator new(const std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new[](const std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new(const std::size_t size, const std::align_val_t align) {
  TrackMemory(size, align);
  return newImpl(size, align);
}

void *operator new[](const std::size_t size, const std::align_val_t align) {
  TrackMemory(size, align);
  return newImpl(size, align);
}

void *operator new(const std::size_t size, const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size)) [[likely]] {
    return newNoExcept(size);
  }
  return nullptr;
}

void *operator new[](const std::size_t size, const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size)) [[likely]] {
    return newNoExcept(size);
  }
  return nullptr;
}

void *operator new(const std::size_t size, const std::align_val_t align, const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size, align)) [[likely]] {
    return newNoExcept(size, align);
  }
  return nullptr;
}

void *operator new[](const std::size_t size, const std::align_val_t align, const std::nothrow_t & /*unused*/) noexcept {
  if (TrackMemoryNoExcept(size, align)) [[likely]] {
    return newNoExcept(size, align);
  }
  return nullptr;
}

void operator delete(void *ptr) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete[](void *ptr, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete(void *ptr, const std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete[](void *ptr, const std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete(void *ptr, const std::size_t size, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align, size);
  deleteSized(ptr, size, align);
}

void operator delete[](void *ptr, const std::size_t size, const std::align_val_t align) noexcept {
  UntrackMemory(ptr, align, size);
  deleteSized(ptr, size, align);
}

void operator delete(void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr, const std::align_val_t align, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete[](void *ptr, const std::align_val_t align, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr, align);
  deleteImpl(ptr, align);
}
