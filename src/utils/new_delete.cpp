#include <iostream>
#include <new>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#else
#include <cstdlib>
#endif

#include "utils/likely.hpp"

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

}  // namespace

void *operator new(std::size_t size) { return newImpl(size); }

void *operator new[](std::size_t size) { return newImpl(size); }

void *operator new(std::size_t size, const std::nothrow_t & /*unused*/) noexcept { return newNoExcept(size); }

void *operator new[](std::size_t size, const std::nothrow_t & /*unused*/) noexcept { return newNoExcept(size); }

void operator delete(void *ptr) noexcept { deleteImpl(ptr); }

void operator delete[](void *ptr) noexcept { deleteImpl(ptr); }

void operator delete(void *ptr, std::size_t size) noexcept { deleteSized(ptr, size); }

void operator delete[](void *ptr, std::size_t size) noexcept { deleteSized(ptr, size); }
