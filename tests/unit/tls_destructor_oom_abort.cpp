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

// Regression test for QA-2026-07-16-TLS: an uncatchable abort() at the memory ceiling caused by
// glibc's thread-local destructor registration.
//
// Mechanism (verified against the source):
//   * The first touch of a thread_local whose type has a NON-trivial destructor makes the compiler
//     emit a call to __cxa_thread_atexit (libstdc++) -> __cxa_thread_atexit_impl (glibc) so the
//     destructor can be run at thread exit.
//   * __cxa_thread_atexit_impl performs a small internal allocation (calloc) to record the
//     destructor node. Memgraph overrides calloc/malloc (src/memory/malloc_free.cpp) and routes
//     them through the query memory tracker.
//   * If the thread is query-tracked and already at its --memory-limit, that allocation is REFUSED
//     (returns nullptr). glibc cannot propagate the failure (the ABI has no error path here), so it
//     calls __libc_fatal -> abort(): a hard SIGABRT that bypasses Memgraph's graceful
//     OutOfMemoryException path entirely.
//
// The fix (src/query/plan/operator.cpp) holds such per-thread state in a trivially destructible
// wrapper (absl::NoDestructor), so NO __cxa_thread_atexit registration is emitted and the failing
// allocation never happens. This test exercises the general mechanism:
//   * RawThreadLocalWithDtorAbortsUnderPressure  -- reproduces the crash (death test).
//   * NoDestructorWrappedTlsSurvivesUnderPressure -- the fix pattern survives the same pressure.

#include <gtest/gtest.h>

#include <cstddef>
#include <new>
#include <thread>
#include <type_traits>

#include "memory/global_memory_control.hpp"
#include "memory/query_memory_control.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/query_memory_tracker.hpp"

namespace {

using memgraph::utils::MemoryTracker;
using memgraph::utils::QueryMemoryTracker;

// A type whose CONSTRUCTION allocates nothing, but whose user-provided destructor makes it
// non-trivially destructible -- so a thread_local of this type triggers __cxa_thread_atexit on
// first touch. This isolates the registration allocation as the only thing that can fail.
struct NonTrivialDtor {
  // NOLINTNEXTLINE(modernize-use-equals-default) -- must be user-provided to be non-trivial.
  ~NonTrivialDtor() {}

  volatile int x = 0;
};

static_assert(!std::is_trivially_destructible_v<NonTrivialDtor>,
              "test premise: raw type must register a TLS destructor");

// A trivially destructible holder that constructs T in place and never destroys it. This mirrors
// the absl::NoDestructor used by the fix (src/query/plan/operator.cpp): because the holder itself is
// trivially destructible, a thread_local of this type emits NO __cxa_thread_atexit registration.
template <typename T>
struct NeverDestroyed {
  NeverDestroyed() { ::new (static_cast<void *>(&storage_)) T(); }

  T *get() { return std::launder(reinterpret_cast<T *>(&storage_)); }

  alignas(T) unsigned char storage_[sizeof(T)];
};

static_assert(std::is_trivially_destructible_v<NeverDestroyed<NonTrivialDtor>>,
              "fix premise: the never-destroyed holder must NOT register a TLS destructor");

// Prevents the optimizer from proving the thread_local touch is dead and eliding it.
template <typename T>
[[gnu::noinline]] void Escape(T *p) {
  asm volatile("" : : "g"(p) : "memory");
}

[[gnu::noinline]] void TouchRawTlsWithDtor() {
  thread_local NonTrivialDtor tls;  // first touch -> __cxa_thread_atexit -> tracked calloc
  Escape(&tls);
}

[[gnu::noinline]] void TouchNoDestructorWrappedTls() {
  thread_local NeverDestroyed<NonTrivialDtor> tls{};  // trivially destructible -> no registration
  Escape(tls.get());
}

// Runs `fn` on a FRESH thread (so the target thread_local is guaranteed uninitialized) while that
// thread is query-tracked with a 1-byte limit and an OutOfMemoryExceptionEnabler is active -- i.e.
// any tracked allocation of size > 0 is over the limit and will be refused. This is exactly the
// state a query worker is in when it first touches a thread_local at the memory ceiling.
template <typename Fn>
void RunOnFreshThreadUnderPressure(Fn fn) {
  std::thread t([fn]() {
#if USE_JEMALLOC
    memgraph::memory::SetHooks();
    // Warm this thread's jemalloc/tracker TLS BEFORE clamping the limit, so the pressure is
    // isolated to `fn` and does not spuriously fail allocator bootstrap.
    memgraph::memory::EnsureJemallocThreadStateInitialized();
#endif
    QueryMemoryTracker qmt;
    qmt.SetQueryLimit(1);  // set before tracking starts: amount == 0, so the byte limit is 1
    memgraph::memory::StartTrackingCurrentThread(&qmt);
    const MemoryTracker::OutOfMemoryExceptionEnabler enabler;
    fn();
    memgraph::memory::StopTrackingCurrentThread();
  });
  t.join();
}

}  // namespace

// Reproduces the crash: a raw thread_local with a non-trivial destructor, first-touched at the
// ceiling, aborts the process. Run as a death test so the SIGABRT happens in a forked child.
TEST(TlsDestructorOomAbortDeathTest, RawThreadLocalWithDtorAbortsUnderPressure) {
#if USE_JEMALLOC
  // Assert the death is specifically glibc's TLS-destructor registration failure, not an unrelated
  // abort. glibc 2.4x prints "failed to register TLS destructor" before calling abort().
  EXPECT_DEATH({ RunOnFreshThreadUnderPressure(&TouchRawTlsWithDtor); }, "register TLS destructor");
#else
  GTEST_SKIP() << "Query memory tracking (and the malloc/calloc override) require USE_JEMALLOC.";
#endif
}

// The fix pattern: the same per-thread state wrapped in absl::NoDestructor is trivially
// destructible, so no __cxa_thread_atexit registration is emitted and nothing allocates on first
// touch -- the thread survives the identical pressure. (If it did NOT survive, the abort would take
// down the whole test binary, not merely fail this assertion.)
TEST(TlsDestructorOomAbortTest, NoDestructorWrappedTlsSurvivesUnderPressure) {
#if USE_JEMALLOC
  EXPECT_NO_FATAL_FAILURE({ RunOnFreshThreadUnderPressure(&TouchNoDestructorWrappedTls); });
#else
  GTEST_SKIP() << "Query memory tracking (and the malloc/calloc override) require USE_JEMALLOC.";
#endif
}
