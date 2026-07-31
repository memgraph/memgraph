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

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <thread>

#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "query/coro_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/coro_task.hpp"
#include "utils/priority_thread_pool.hpp"

// --- Unit 4: AcquireAccessorCoro -- the acquire coroutine at the heart of parkable Prepare ---
// (opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md REVISION 3 §R3.2, REVISION 4
// §R4.3/§R4.6). Exercises the whole park -> wake -> re-acquire cycle on a REAL PriorityThreadPool
// and a REAL InMemoryStorage, with no Bolt/session layer involved at all (that integration is a
// later, separately-gated stage -- see coro_accessor.hpp's doc comment).

namespace {

using memgraph::query::AcquireAccessorCoro;
namespace storage = memgraph::storage;
namespace utils = memgraph::utils;

// Polls `pred` until it becomes true or `timeout` elapses; returns the final value of `pred()`.
// Used instead of a blind fixed sleep so the pass/fail assertions below are as tight as the
// scheduling jitter allows, while still tolerating CI slowness (bounded wait, per the task's
// "latches/atomics, not sleeps where avoidable" guidance -- a bounded poll is the documented
// exception for the wake itself).
template <typename Pred>
bool BoundedWaitUntil(Pred pred, std::chrono::milliseconds timeout,
                      std::chrono::milliseconds poll = std::chrono::milliseconds(2)) {
  const auto wait_deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() >= wait_deadline) return pred();
    std::this_thread::sleep_for(poll);
  }
  return true;
}

// RAII flag flip + cache refresh (mirrors StorageMainLockWakeHookTest in storage_v2.cpp) so a
// test enabling the experimental flag can never leak that state into an unrelated test sharing
// the process.
class ScopedCoroPrepareFlag {
 public:
  explicit ScopedCoroPrepareFlag(bool enabled) : saved_(FLAGS_experimental_coro_prepare_accessor_yield) {
    FLAGS_experimental_coro_prepare_accessor_yield = enabled;
    memgraph::flags::run_time::RefreshCoroPrepareAccessorYieldEnabled();
  }

  ~ScopedCoroPrepareFlag() {
    FLAGS_experimental_coro_prepare_accessor_yield = saved_;
    memgraph::flags::run_time::RefreshCoroPrepareAccessorYieldEnabled();
  }

  ScopedCoroPrepareFlag(const ScopedCoroPrepareFlag &) = delete;
  ScopedCoroPrepareFlag &operator=(const ScopedCoroPrepareFlag &) = delete;

 private:
  bool saved_;
};

// Periodic GC disabled everywhere below: a background GC UNIQUE-release could otherwise release
// main_lock_ (and fire NotifyMainLockReleased()) on its own and make these assertions racy.
storage::Config NoGcConfig() {
  return storage::Config{.gc = {.type = storage::Config::Gc::Type::NONE},
                         .transaction = {.isolation_level = storage::IsolationLevel::SNAPSHOT_ISOLATION}};
}

// Drives (via `.Run()`, inside a pool task so utils::GetCurrentWorkerId() is published)
// AcquireAccessorCoro(...) to completion, storing its result/exception in the caller-owned
// `result`/`eptr` and signalling `done` when finished. Returns the driver Task<void> so the
// caller can keep its coroutine frame alive for as long as the async operation may still be
// running (the frame must outlive any in-flight park -- see the doc comment on `driver` in each
// test below).
//
// Task<void>::Run() is safe to call even when the awaited AcquireAccessorCoro() genuinely parks:
// Promise<void>::TakeValue() (unlike the generic Promise<T>::TakeValue()) does not assert
// completion, only rethrows a stored exception -- so Run() returning after a real suspend deep in
// the awaited chain is completely valid; the driver's body simply resumes later, off a pool
// worker, when the parked coroutine's on_resume eventually fires.
utils::Task<void> MakeDriver(storage::Storage &storage, storage::StorageAccessType rw,
                             std::optional<storage::IsolationLevel> resolved_iso,
                             std::chrono::steady_clock::time_point deadline, utils::PriorityThreadPool &pool,
                             bool is_high_priority, std::optional<std::unique_ptr<storage::Accessor>> &result,
                             std::exception_ptr &eptr, std::atomic<bool> &done) {
  try {
    result = co_await AcquireAccessorCoro(storage, rw, resolved_iso, deadline, pool, is_high_priority);
  } catch (...) {
    eptr = std::current_exception();
  }
  done.store(true, std::memory_order_release);
  done.notify_one();
  co_return;
}

}  // namespace

// (a) PARK -> WAKE -> ACQUIRE: a conflicting UNIQUE request parks while another UNIQUE accessor is
// held, then wakes and completes once that accessor is released.
TEST(CoroAccessor, ParkWakeAcquire) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Kept alive for the whole test: the coroutine frame must outlive the in-flight park (it is
  // resumed later, from a pool worker, via the ParkState's on_resume closure).
  auto driver = MakeDriver(store,
                           storage::UNIQUE,
                           std::nullopt,
                           std::chrono::steady_clock::now() + std::chrono::seconds(5),
                           pool,
                           /*is_high_priority=*/false,
                           result,
                           eptr,
                           done);

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  // Wait until the coroutine has actually registered as a waiter (i.e. genuinely parked) rather
  // than sleeping blindly.
  ASSERT_TRUE(BoundedWaitUntil([&] { return store.main_lock_resume_event().WaitersPending() > 0; },
                               std::chrono::milliseconds(1000)));
  EXPECT_FALSE(done.load()) << "must not complete while the conflicting UNIQUE accessor is still held";

  held->Abort();
  held.reset();  // Releases main_lock_ -> Storage::Accessor::~Accessor() -> NotifyMainLockReleased().

  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "parked coroutine never woke up and completed";

  // The OTHER direction of the post-resume prune, and the one ParkTimeoutLeavesNoWaiterRegistered
  // structurally cannot cover. There, a deadline Sweep claimed the park and erased its own registry
  // entry, leaving the wake-event twin. Here a lock-release NotifyAll claimed it and emptied
  // `waiters_` wholesale, leaving the DEADLINE-REGISTRY twin -- so only AcquireAccessorCoro's own
  // `park_registry().Deregister()` removes it, and a retained entry defeats Sweep's cheap
  // "nothing parked" fast path for the rest of the pool's life (every 100ms tick takes the mutex and
  // walks the list) while holding a fired ParkState alive.
  //
  // Asserted IMMEDIATELY after `done`, and that is load-bearing rather than tidy: Sweep lazy-prunes
  // already-claimed entries regardless of deadline (R4.5), so the next monitor tick would clean up a
  // missing Deregister and mask it. `done` is set by the driver after AcquireAccessorCoro returns,
  // i.e. microseconds after the prune, against a 100ms tick -- but the margin is timing, not logic,
  // so do not move this check further down or add waits before it.
  EXPECT_EQ(pool.park_registry().Size(), 0U)
      << "a park resumed by a lock-release NotifyAll stayed registered in the deadline registry -- "
         "NotifyAll only empties its own waiter list, so nothing else would ever remove this entry";
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U)
      << "the wake event still reports a pending waiter after the park it belonged to completed";

  EXPECT_FALSE(eptr) << "unexpected exception from the parked acquire";
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(*result, nullptr);
  if (result && *result) EXPECT_NO_THROW((*result)->Abort());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (b) PARK -> TIMEOUT: a conflicting UNIQUE request parks against a short absolute deadline and,
// since the held accessor is NEVER released, must be woken by the pool's periodic deadline sweep
// (not by a lock-release notify) and throw UniqueAccessTimeout at ~deadline.
TEST(CoroAccessor, ParkTimeout) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Short deadline (well under the pool monitor's ~100ms sweep period, so this resolves within a
  // couple of ticks) -- keeps the test fast while still exercising the real sched_mon sweep.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
  auto driver =
      MakeDriver(store, storage::UNIQUE, std::nullopt, deadline, pool, /*is_high_priority=*/false, result, eptr, done);

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "deadline-sweep never woke the parked coroutine to time it out";

  ASSERT_FALSE(result.has_value());
  ASSERT_TRUE(eptr) << "expected a UniqueAccessTimeout, got neither a result nor an exception";
  EXPECT_THROW(std::rethrow_exception(eptr), storage::UniqueAccessTimeout);

  // NOTE: this particular assertion is NOT discriminating, and is kept only because it is true and
  // cheap. A UNIQUE campaign holds a real PendingScope, so unwinding the timed-out coroutine
  // destroys it -> unregister_pending -> maybe_notify -> the admit observer -> NotifyAll, which
  // empties `waiters_` as a side effect and would hide a missing prune. WRITE is the case that
  // actually tests it -- see ParkTimeoutLeavesNoWaiterRegistered below.
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);

  held->Abort();
  held.reset();

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (c) FAST PATH: uncontended acquisition succeeds immediately and never parks -- no waiter is
// ever registered on the storage's wake event.
TEST(CoroAccessor, FastPathNeverParks) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto acc = utils::SyncWait(AcquireAccessorCoro(store,
                                                 storage::WRITE,
                                                 std::nullopt,
                                                 std::chrono::steady_clock::now() + std::chrono::seconds(5),
                                                 pool,
                                                 /*is_high_priority=*/false));

  ASSERT_NE(acc, nullptr);
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U) << "uncontended acquire must never register a park";
  EXPECT_NO_THROW(acc->Abort());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (d) HIGH priority bypasses the park path entirely -- even with the flag on and InMemory
// storage, it always takes the ordinary blocking Access()/UniqueAccess()/ReadOnlyAccess() path,
// and still succeeds once a conflicting holder releases (behaviorally identical to today).
TEST(CoroAccessor, HighPriorityUsesBlockingPathAndStillAcquires) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::atomic<bool> releaser_started{false};
  std::thread releaser([&] {
    releaser_started.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    held->Abort();
    held.reset();
  });

  // HIGH priority: this call BLOCKS the calling thread via the ordinary Storage::UniqueAccess()
  // path (never parks, never touches main_lock_resume_event()) until the releaser thread above
  // frees the lock.
  auto acc = utils::SyncWait(AcquireAccessorCoro(store,
                                                 storage::UNIQUE,
                                                 std::nullopt,
                                                 std::chrono::steady_clock::now() + std::chrono::seconds(5),
                                                 pool,
                                                 /*is_high_priority=*/true));

  ASSERT_TRUE(releaser_started.load());
  ASSERT_NE(acc, nullptr);
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U) << "HIGH priority must never register a park";
  EXPECT_NO_THROW(acc->Abort());

  releaser.join();
  pool.ShutDown();
  pool.AwaitShutdown();
}

// (d, continued) Flag OFF also bypasses the park path entirely, same as HIGH priority above.
TEST(CoroAccessor, FlagOffUsesBlockingPath) {
  ScopedCoroPrepareFlag flag_off{false};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto acc = utils::SyncWait(AcquireAccessorCoro(store,
                                                 storage::WRITE,
                                                 std::nullopt,
                                                 std::chrono::steady_clock::now() + std::chrono::seconds(5),
                                                 pool,
                                                 /*is_high_priority=*/false));

  ASSERT_NE(acc, nullptr);
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U);
  EXPECT_NO_THROW(acc->Abort());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (e) PARK -> SHUTDOWN: a genuinely parked acquire must be resumed and unwound by the pool's
// teardown, promptly, rather than left registered forever holding its session alive. This is the
// path the original symptom was on (an unbounded shutdown stall), and the one un-pinning most
// disturbed: pre-F6 the drain's resume was posted to the parking worker, which was guaranteed
// still alive; now it is posted to whichever worker will take it, and the arming side may be what
// delivers it. Nothing else in the suite covers ShutDown() with a park in flight.
TEST(CoroAccessor, ParkThenShutdownUnwindsPromptly) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Deadline far enough out that the deadline sweep cannot be what resolves this -- the only thing
  // that can complete this acquire is the shutdown drain.
  auto driver = MakeDriver(store,
                           storage::UNIQUE,
                           std::nullopt,
                           std::chrono::steady_clock::now() + std::chrono::seconds(600),
                           pool,
                           /*is_high_priority=*/false,
                           result,
                           eptr,
                           done);

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  ASSERT_TRUE(BoundedWaitUntil([&] { return store.main_lock_resume_event().WaitersPending() > 0; },
                               std::chrono::milliseconds(1000)))
      << "coroutine never parked, so this test would not be exercising shutdown-with-a-park";
  ASSERT_FALSE(done.load());

  const auto shutdown_start = std::chrono::steady_clock::now();
  pool.ShutDown();
  pool.AwaitShutdown();
  const auto shutdown_elapsed = std::chrono::steady_clock::now() - shutdown_start;

  // Generous bound: the point is that this does not wait out the 600s deadline (or hang forever),
  // not that it hits any particular latency.
  EXPECT_LT(shutdown_elapsed, std::chrono::seconds(5))
      << "shutdown waited on the parked acquire instead of draining it";

  // The drain must have RESUMED the park, not merely dropped it: the coroutine has to observe
  // IsShuttingDown() and unwind, which is what releases its session/accessor references.
  EXPECT_TRUE(done.load()) << "parked coroutine was never resumed by the shutdown drain -- a leaked "
                              "session and, in production, a shutdown that never finishes";
  EXPECT_FALSE(result.has_value()) << "must not hand back an accessor when the pool is tearing down";
  EXPECT_TRUE(eptr) << "expected the shutdown bail exception";
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U) << "drain must leave no waiter registered";

  held->Abort();
  held.reset();
}

// (g) A timed-out park must leave NOTHING registered behind it, on the path where nothing else
// cleans up for it.
//
// WRITE is load-bearing here. Whichever wake source claims a park removes it from its OWN list only:
// a lock-release NotifyAll empties `waiters_` wholesale, while the deadline Sweep erases its own
// entry and leaves the `waiters_` twin. So a sweep-resumed park needs the coroutine to prune the
// event side -- and for UNIQUE/READ_ONLY that leak is masked, because unwinding the campaign destroys
// its PendingScope, which notifies, which triggers a NotifyAll that empties `waiters_` anyway.
// MakePendingHandle(WRITE) engages no scope at all (inmemory/storage.hpp), so nothing notifies and
// nothing masks it.
//
// Left unpruned, the consequences compound: `waiters_pending_` stays non-zero, which permanently
// defeats NotifyMainLockReleased's "nobody is parked" fast path, so every later admitting transition
// on this storage takes the event mutex AND bumps the epoch -- and a bumped epoch fails concurrent
// RegisterWaiter calls, sending other parkers back around the acquire loop with no backoff. The
// entry also retained the ParkState's on_resume closure, which in production holds a
// shared_ptr<Session> and through it a DatabaseAccess, stalling DROP DATABASE.
TEST(CoroAccessor, ParkTimeoutLeavesNoWaiterRegistered) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  // A held UNIQUE blocks WRITE (can_acquire<WRITE> requires state != UNIQUE), so the WRITE parks.
  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
  auto driver =
      MakeDriver(store, storage::WRITE, std::nullopt, deadline, pool, /*is_high_priority=*/false, result, eptr, done);

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "deadline sweep never woke the parked coroutine to time it out";
  ASSERT_FALSE(result.has_value());
  ASSERT_TRUE(eptr);
  EXPECT_THROW(std::rethrow_exception(eptr), storage::SharedAccessTimeout);

  // The holder is still held, so no lock release has happened and nothing but the coroutine itself
  // could have pruned the wake event.
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U)
      << "a timed-out WRITE park left itself registered on the storage wake event -- it would retain "
         "its Session, defeat the no-waiters fast path, and spin later parkers via epoch bumps";

  held->Abort();
  held.reset();

  pool.ShutDown();
  pool.AwaitShutdown();
}
