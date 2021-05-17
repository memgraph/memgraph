#include <new>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#else
#include <cstdlib>
#endif

#include "utils/likely.hpp"
#include "utils/memory_tracker.hpp"

namespace {
void *newImpl(std::size_t size) {
  auto *ptr = malloc(size);
  if (LIKELY(ptr != nullptr)) {
    return ptr;
  }

  throw std::bad_alloc{};
}

void *newNoExcept(const std::size_t size) noexcept { return malloc(size); }

void deleteImpl(void *ptr) noexcept { free(ptr); }

#if USE_JEMALLOC

void deleteSized(void *ptr, const std::size_t size) noexcept {
  if (UNLIKELY(ptr == nullptr)) {
    return;
  }

  sdallocx(ptr, size, 0);
}

#else

void deleteSized(void *ptr, const std::size_t /*unused*/) noexcept { free(ptr); }

#endif

void TrackMemory(const size_t size) {
  size_t actual_size = size;

#if USE_JEMALLOC
  if (LIKELY(size != 0)) {
    actual_size = nallocx(size, 0);
  }
#endif
  utils::total_memory_tracker.Alloc(actual_size);
}

bool TrackMemoryNoExcept(const size_t size) {
  try {
    TrackMemory(size);
  } catch (...) {
    return false;
  }

  return true;
}

void UntrackMemory([[maybe_unused]] void *ptr, [[maybe_unused]] size_t size = 0) noexcept {
  try {
#if USE_JEMALLOC
    if (LIKELY(ptr != nullptr)) {
      utils::total_memory_tracker.Free(sallocx(ptr, 0));
    }
#else
    if (size) {
      utils::total_memory_tracker.Free(size);
    } else {
      // Innaccurate because malloc_usable_size() result is greater or equal to allocated size.
      utils::total_memory_tracker.Free(malloc_usable_size(ptr));
    }
#endif
  } catch (...) {
  }
}

}  // namespace

void *operator new(std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new[](std::size_t size) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new(std::size_t size, std::align_val_t /*unused*/) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new[](std::size_t size, std::align_val_t /*unused*/) {
  TrackMemory(size);
  return newImpl(size);
}

void *operator new(std::size_t size, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(TrackMemoryNoExcept(size))) {
    return newNoExcept(size);
  }
  return nullptr;
}

void *operator new[](std::size_t size, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(TrackMemoryNoExcept(size))) {
    return newNoExcept(size);
  }
  return nullptr;
}

void *operator new(std::size_t size, std::align_val_t /*unused*/, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(TrackMemoryNoExcept(size))) {
    return newNoExcept(size);
  }
  return nullptr;
}

void *operator new[](std::size_t size, std::align_val_t /*unused*/, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(TrackMemoryNoExcept(size))) {
    return newNoExcept(size);
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

void operator delete(void *ptr, std::align_val_t /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr, std::align_val_t /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr, std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete[](void *ptr, std::size_t size) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete(void *ptr, std::size_t size, std::align_val_t /*unused*/) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete[](void *ptr, std::size_t size, std::align_val_t /*unused*/) noexcept {
  UntrackMemory(ptr, size);
  deleteSized(ptr, size);
}

void operator delete(void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr, std::align_val_t /*unused*/, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr, std::align_val_t /*unused*/, const std::nothrow_t & /*unused*/) noexcept {
  UntrackMemory(ptr);
  deleteImpl(ptr);
}
