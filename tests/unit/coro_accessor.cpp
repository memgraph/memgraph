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

// Drives AcquireAccessorCoro to completion inside a pool task (so GetCurrentWorkerId() is published),
// storing its result/exception and signalling `done`. Returns the driver Task so the caller can keep
// the frame alive -- it MUST outlive any in-flight park.
//
// Run() is safe even when the awaited chain genuinely parks: Promise<void>::TakeValue() only rethrows,
// it does not assert completion, so returning after a real suspend is valid and the body resumes later
// off a pool worker.
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

// Same as MakeDriver, but threads an `on_park_resumed` hook through so a test can observe genuine
// cross-thread resumes rather than inferring them from waiter counts (which cannot distinguish
// "resumed and re-parked" from "never touched").
utils::Task<void> MakeDriverWithResumeHook(storage::Storage &storage, storage::StorageAccessType rw,
                                           std::chrono::steady_clock::time_point deadline,
                                           utils::PriorityThreadPool &pool,
                                           std::optional<std::unique_ptr<storage::Accessor>> &result,
                                           std::exception_ptr &eptr, std::atomic<bool> &done,
                                           std::function<void()> on_park_resumed) {
  try {
    result = co_await AcquireAccessorCoro(
        storage, rw, std::nullopt, deadline, pool, /*is_high_priority=*/false, std::move(on_park_resumed));
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

  // The OTHER direction of the post-resume prune, which ParkTimeoutLeavesNoWaiterRegistered
  // structurally cannot cover: there a Sweep claimed the park and left the wake-event twin, here a
  // NotifyAll claimed it and left the DEADLINE-REGISTRY twin, so only Deregister() removes it.
  //
  // Asserted IMMEDIATELY after `done`, and that is load-bearing: Sweep lazy-prunes already-claimed
  // entries regardless of deadline, so the next monitor tick would mask a missing Deregister. The
  // margin (microseconds vs a 100ms tick) is timing, not logic -- do not add waits before this.
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
// (c) An UNCONTENDED acquire never registers a park -- with the flag ON (it takes the coroutine's own
// fast path, probing successfully before any park attempt) and with it OFF (it takes the ordinary
// blocking path). Same observable outcome via two different code paths, so both are asserted.
class CoroAccessorUncontended : public ::testing::TestWithParam<bool> {};

TEST_P(CoroAccessorUncontended, AcquireRegistersNoPark) {
  ScopedCoroPrepareFlag const flag{GetParam()};

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

INSTANTIATE_TEST_SUITE_P(FlagOnAndOff, CoroAccessorUncontended, ::testing::Bool(),
                         [](const testing::TestParamInfo<bool> &info) { return info.param ? "FlagOn" : "FlagOff"; });

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

// (g) A timed-out park must leave NOTHING registered behind it, on the path where nothing else cleans
// up for it. Consequences of failing to prune: specs/parkable-prepare.md.
//
// WRITE is load-bearing. A sweep-resumed park needs the coroutine to prune the EVENT side, and for
// UNIQUE/READ_ONLY that leak is masked -- unwinding the campaign destroys its PendingScope, which
// notifies, which empties `waiters_` anyway. MakePendingHandle(WRITE) engages no scope at all, so
// nothing masks it.
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

// (h) STORAGE-SIDE DRAIN resumes a park that no lock release will ever wake.
//
// Fills a real gap, not symmetry. The POOL drain is already pinned by ParkThenShutdownUnwindsPromptly.
// The storage-side ones could NOT be covered over Bolt -- park_shutdown.py was measured by mutation to
// guard none of them, because session teardown releases the holder and the ordinary notify path wakes
// the park first. A unit test owns the holder as a local, so it can build what Bolt cannot: a park with
// its conflicting holder still held.
//
// Discriminates RESUMES from merely DROPS: after the drain the resumed coroutine re-probes, still
// cannot acquire, and RE-PARKS, so WaitersPending() returns to 1. A drop leaves it at 0 forever.
TEST(CoroAccessor, StorageSideDrainResumesAParkNoReleaseCanWake) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Far deadline on purpose: the periodic sweep must not be able to be what resolves this park, or the
  // test would pass with the drain removed.
  // Counts genuine cross-thread resumes: AcquireAccessorCoro threads this hook into every ParkState it
  // builds and the posted resume closure fires it right after resuming the handle.
  std::atomic<int> resumes{0};
  auto driver = MakeDriverWithResumeHook(store,
                                         storage::UNIQUE,
                                         std::chrono::steady_clock::now() + std::chrono::seconds(600),
                                         pool,
                                         result,
                                         eptr,
                                         done,
                                         [&resumes] { resumes.fetch_add(1, std::memory_order_release); });

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  ASSERT_TRUE(BoundedWaitUntil([&] { return store.main_lock_resume_event().WaitersPending() > 0; },
                               std::chrono::milliseconds(1000)))
      << "coroutine never parked, so there is nothing for the drain to resume";
  ASSERT_FALSE(done.load());
  ASSERT_EQ(resumes.load(), 0) << "nothing should have resumed this park yet";

  // The holder is STILL HELD here and is never released in this test: no lock-release notify can
  // happen, so the drain below is the only thing in the process that can touch this park.
  store.DrainParkedMainLockWaiters();

  // Observe the RESUME, not the waiter count. Counting waiters cannot discriminate here: the entry is
  // already present before the drain and the re-park puts one back, so `WaitersPending() > 0` is true
  // whether or not the drain did anything -- I wrote that assertion first and the mutation run passed
  // 3/3 with the drain removed. The resume hook fires once per genuine cross-thread resume, so it is
  // the only thing that separates "resumed and re-parked" from "never touched".
  ASSERT_TRUE(BoundedWaitUntil([&] { return resumes.load() > 0; }, std::chrono::milliseconds(2000)))
      << "after DrainParkedMainLockWaiters() the park was never resumed: the drain dropped the waiter "
         "instead of resuming it. In production that is a permanently suspended frame holding its "
         "Session, its DatabaseAccess, and Gatekeeper<Database>::count_ above zero";

  // ...and having been resumed, it must ABANDON rather than re-park. `Drain()` closes the wake event, so
  // no further registration on it can succeed; a coroutine that looped back and re-registered would spin
  // to its deadline (600s here) against a storage that is going away.
  //
  // This assertion used to be EXPECT_FALSE(done) -- i.e. it asserted the re-park -- and that was
  // encoding a defect, not a requirement. Before Drain() was made sticky-closed, the resumed frame DID
  // re-register: Drain deliberately leaves the epoch untouched, so a registration arriving after it
  // succeeded onto a just-emptied list, and only the deadline sweep rescued it. Delaying a DROP DATABASE
  // by a full access timeout was the observable cost.
  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "the resumed coroutine did not finish: it re-parked on a drained (closed) wake event, where no "
         "wake source can ever reach it again, and is now waiting out its full deadline";
  EXPECT_FALSE(result.has_value()) << "must not hand back an accessor -- the conflicting holder is still held";
  EXPECT_TRUE(eptr) << "expected the storage-tearing-down bail exception";
  EXPECT_EQ(store.main_lock_resume_event().WaitersPending(), 0U) << "no waiter may be left registered";

  pool.ShutDown();
  pool.AwaitShutdown();
  held->Abort();
  held.reset();
}
