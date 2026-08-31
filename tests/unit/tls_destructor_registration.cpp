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

// Registering a thread-local destructor allocates inside glibc, and glibc cannot act on a refusal.
//
// The first touch of a thread_local whose type has a non-trivial destructor emits a call to
// __cxa_thread_atexit -> __cxa_thread_atexit_impl, which callocs a node to record the destructor.
// That calloc is routed through the query memory tracker like any other. Refusing it returns a
// null the ABI has no way to report, so glibc calls __libc_fatal and aborts, bypassing the
// OutOfMemoryException path. It is exempt because it is made outside any RefusalHandledScope.
//
// Both cases run against real glibc rather than the tracker in isolation: one where the
// registration allocation happens and must not be refused, one where a trivially destructible
// holder emits no registration and so never allocates.

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

// Constructing this allocates nothing, but the user-provided destructor makes it non-trivially
// destructible, so the registration allocation is the only one that can fail.
struct NonTrivialDtor {
  // NOLINTNEXTLINE(modernize-use-equals-default) -- must be user-provided to be non-trivial.
  ~NonTrivialDtor() {}

  volatile int x = 0;
};

static_assert(!std::is_trivially_destructible_v<NonTrivialDtor>,
              "test premise: raw type must register a TLS destructor");

// Constructs T in place and never destroys it, so the holder stays trivially destructible and a
// thread_local of this type emits no registration.
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

// Runs `fn` on a fresh thread, so the target thread_local is guaranteed uninitialized, while that
// thread is query-tracked with a 1-byte limit and throwing enabled: every tracked allocation is
// over the limit. This is the state a query worker is in when it first touches a thread_local at
// the memory ceiling.
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

// Were the registration allocation refused, glibc would abort, taking down the whole test binary
// rather than failing this assertion.
TEST(TlsDestructorRegistrationTest, RawThreadLocalWithDtorSurvivesUnderPressure) {
#if USE_JEMALLOC
  EXPECT_NO_FATAL_FAILURE({ RunOnFreshThreadUnderPressure(&TouchRawTlsWithDtor); });
#else
  GTEST_SKIP() << "Query memory tracking (and the malloc/calloc override) require USE_JEMALLOC.";
#endif
}

// A trivially destructible holder emits no registration, so first touch allocates nothing.
TEST(TlsDestructorRegistrationTest, NoDestructorWrappedTlsSurvivesUnderPressure) {
#if USE_JEMALLOC
  EXPECT_NO_FATAL_FAILURE({ RunOnFreshThreadUnderPressure(&TouchNoDestructorWrappedTls); });
#else
  GTEST_SKIP() << "Query memory tracking (and the malloc/calloc override) require USE_JEMALLOC.";
#endif
}
