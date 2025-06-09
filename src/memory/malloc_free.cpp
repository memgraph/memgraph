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

#include <dlfcn.h>
#include <jemalloc/jemalloc.h>

#include "utils/logging.hpp"

static void *(*next_malloc)(size_t) = nullptr;
static void (*next_free)(void *) = nullptr;
static void *(*next_calloc)(size_t, size_t) = nullptr;
static void *(*next_realloc)(void *, size_t) = nullptr;

static int (*next_posix_memalign)(void **p, size_t alignment, size_t size) = nullptr;
static void *(*next_aligned_alloc)(size_t alignment, size_t size) = nullptr;
static void *(*next_memalign)(size_t alignment, size_t size) = nullptr;
static void *(*next_valloc)(size_t size) = nullptr;
static size_t (*next_malloc_usable_size)(void *p) = nullptr;

// static void __attribute__((constructor)) init(void) {
//   next_malloc = (void *(*)(size_t))dlsym(RTLD_NEXT, "malloc");
//   next_free = (void (*)(void *))dlsym(RTLD_NEXT, "free");
//   next_calloc = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "calloc");
//   next_realloc = (void *(*)(void *, size_t))dlsym(RTLD_NEXT, "realloc");
//   next_aligned_alloc = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "aligned_alloc");
//   next_posix_memalign = (int (*)(void **, size_t, size_t))dlsym(RTLD_NEXT, "posix_memalign");
//   next_valloc = (void *(*)(size_t))dlsym(RTLD_NEXT, "valloc");
//   next_malloc_usable_size = (size_t(*)(void *))dlsym(RTLD_NEXT, "malloc_usable_size");
//   next_memalign = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "memalign");
// }

namespace {
inline void log(auto &&fmt, auto &&...args) {
  // static thread_local char buf[256];
  // int len = snprintf(buf, sizeof(buf), fmt, std::forward<decltype(args)>(args)...);
  // write(STDERR_FILENO, buf, len);
}

static std::atomic<uint64_t> sum{0};

}  // namespace

extern "C" void *malloc(size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_malloc) [[unlikely]] {
      next_malloc = (void *(*)(size_t))dlsym(RTLD_NEXT, "malloc");
      MG_ASSERT(next_malloc, "Failed to find next malloc function using dlsym");
    }
    void *const res = je_malloc(size);
    log("malloc %p %d\n", res, size);
    if (res) sum += je_sallocx(res, 0);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  log("fallback on malloc!\n");
  return next_malloc ? next_malloc(size) : ::malloc(size);
}

extern "C" void free(void *ptr) {
  if (!ptr) return;
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_free) [[unlikely]] {
      next_free = (void (*)(void *))dlsym(RTLD_NEXT, "free");
      MG_ASSERT(next_malloc, "Failed to find next free function using dlsym");
    }
    const auto now = sum.fetch_sub(je_sallocx(ptr, 0));
    je_free(ptr);
    log("free %p now %u\n", ptr, now);
    called = false;
    return;
  }
  // Protect against infinite recursion
  log("fallback on free!\n");
  next_free ? next_free(ptr) : ::free(ptr);
}

extern "C" void *calloc(size_t count, size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_calloc) [[unlikely]] {
      next_calloc = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "calloc");
      MG_ASSERT(next_malloc, "Failed to find next calloc function using dlsym");
    }
    void *const res = je_calloc(count, size);
    if (res) sum += je_sallocx(res, 0);
    log("calloc %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_calloc ? next_calloc(count, size) : ::calloc(count, size);
}

extern "C" void *realloc(void *ptr, size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_realloc) [[unlikely]] {
      next_realloc = (void *(*)(void *, size_t))dlsym(RTLD_NEXT, "realloc");
      MG_ASSERT(next_malloc, "Failed to find next realloc function using dlsym");
    }
    const auto prev_size = ptr ? je_sallocx(ptr, 0) : 0;
    void *const res = je_realloc(ptr, size);
    if (res) sum += je_sallocx(res, 0) - prev_size;
    log("realloc %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_realloc ? next_realloc(ptr, size) : ::realloc(ptr, size);
}

extern "C" void *aligned_alloc(size_t alignment, size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_aligned_alloc) [[unlikely]] {
      next_aligned_alloc = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "aligned_alloc");
      MG_ASSERT(aligned_alloc, "Failed to find next aligned_alloc function using dlsym");
    }
    void *const res = je_aligned_alloc(alignment, size);
    if (res) sum += je_sallocx(res, MALLOCX_ALIGN(alignment));
    log("aligned_alloc %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_aligned_alloc ? next_aligned_alloc(alignment, size) : ::aligned_alloc(alignment, size);
}

extern "C" int posix_memalign(void **p, size_t alignment, size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_posix_memalign) [[unlikely]] {
      next_posix_memalign = (int (*)(void **, size_t, size_t))dlsym(RTLD_NEXT, "posix_memalign");
      MG_ASSERT(next_posix_memalign, "Failed to find next posix_memalign function using dlsym");
    }
    int const res = je_posix_memalign(p, alignment, size);
    if (res == 0) sum += je_sallocx(*p, MALLOCX_ALIGN(alignment));
    log("posix_memalign %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_posix_memalign ? next_posix_memalign(p, alignment, size) : ::posix_memalign(p, alignment, size);
}

extern "C" void *valloc(size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_valloc) [[unlikely]] {
      next_valloc = (void *(*)(size_t))dlsym(RTLD_NEXT, "valloc");
      MG_ASSERT(next_valloc, "Failed to find next valloc function using dlsym");
    }
    void *const res = je_valloc(size);
    if (res) sum += je_sallocx(res, MALLOCX_ALIGN(4096));
    log("valloc %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_valloc ? next_valloc(size) : ::valloc(size);
}

extern "C" size_t malloc_usable_size(void *ptr) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_malloc_usable_size) [[unlikely]] {
      next_malloc_usable_size = (size_t(*)(void *))dlsym(RTLD_NEXT, "malloc_usable_size");
      MG_ASSERT(next_malloc, "Failed to find next malloc_usable_size function using dlsym");
    }
    size_t const res = je_malloc_usable_size(ptr);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_malloc_usable_size ? next_malloc_usable_size(ptr) : ::malloc_usable_size(ptr);
}

extern "C" void *memalign(size_t alignment, size_t size) {
  static thread_local bool called = false;
  if (!called) {
    called = true;
    if (!next_memalign) [[unlikely]] {
      next_memalign = (void *(*)(size_t, size_t))dlsym(RTLD_NEXT, "memalign");
      MG_ASSERT(next_memalign, "Failed to find next memalign function using dlsym");
    }
    void *const res = je_memalign(alignment, size);
    if (res) sum += je_sallocx(res, MALLOCX_ALIGN(alignment));
    log("memalign %p %d\n", res, size);
    called = false;
    return res;
  }
  // Protect against infinite recursion
  return next_memalign ? next_memalign(alignment, size) : ::memalign(alignment, size);
}

void dallocx(void *ptr, int flags) {
  const auto now = ptr ? sum.fetch_sub(je_sallocx(ptr, flags)) : 0;
  je_dallocx(ptr, flags);
  log("dallocx %p now %u\n", ptr, now);
}

void sdallocx(void *ptr, size_t size, int flags) {
  const auto now = ptr ? sum.fetch_sub(je_sallocx(ptr, flags)) : 0;
  je_sdallocx(ptr, size, flags);
  log("sdallocx %p now %u\n", ptr, now);
}
