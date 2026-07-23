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

// AcquireAccessorCoro tests: exercise the whole park -> wake -> re-acquire cycle on a real
// PriorityThreadPool and InMemoryStorage, with no Bolt/session layer involved.

namespace {

using memgraph::query::AcquireAccessorCoro;
namespace storage = memgraph::storage;
namespace utils = memgraph::utils;

// Polls `pred` until true or `timeout` elapses; a bounded wait so assertions stay tight but
// tolerate CI slowness.
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

// RAII flag flip + cache refresh so an enabled experimental flag can't leak into another test.
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

// Periodic GC disabled: a background GC UNIQUE-release could otherwise fire NotifyMainLockReleased()
// on its own and make these assertions racy.
storage::Config NoGcConfig() {
  return storage::Config{.gc = {.type = storage::Config::Gc::Type::NONE},
                         .transaction = {.isolation_level = storage::IsolationLevel::SNAPSHOT_ISOLATION}};
}

// Drives AcquireAccessorCoro(...) to completion (via .Run() inside a pool task so
// GetCurrentWorkerId() is published), storing result/exception and signalling `done`. Returns the
// driver Task<void> so the caller keeps the frame alive across any in-flight park. Run() is safe
// even when the chain parks: Promise<void>::TakeValue() only rethrows, never asserts completion.
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

// (a) PARK -> WAKE -> ACQUIRE: a conflicting UNIQUE request parks while a UNIQUE accessor is held,
// then wakes and completes once it is released.
TEST(CoroAccessor, ParkWakeAcquire) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Kept alive for the whole test: the frame must outlive the in-flight park (resumed later).
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

  // Wait until the coroutine has actually registered as a waiter (genuinely parked).
  ASSERT_TRUE(BoundedWaitUntil([&] { return store.main_lock_resume_event().WaitersPending() > 0; },
                               std::chrono::milliseconds(1000)));
  EXPECT_FALSE(done.load()) << "must not complete while the conflicting UNIQUE accessor is still held";

  held->Abort();
  held.reset();  // Releases main_lock_ -> Storage::Accessor::~Accessor() -> NotifyMainLockReleased().

  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "parked coroutine never woke up and completed";

  EXPECT_FALSE(eptr) << "unexpected exception from the parked acquire";
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(*result, nullptr);
  if (result && *result) EXPECT_NO_THROW((*result)->Abort());

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (b) PARK -> TIMEOUT: with the holder never released, the parked request must be woken by the
// deadline sweep (not a lock-release notify) and throw UniqueAccessTimeout.
TEST(CoroAccessor, ParkTimeout) {
  ScopedCoroPrepareFlag flag_on{true};

  storage::InMemoryStorage store{NoGcConfig()};
  utils::PriorityThreadPool pool{2, 1};

  auto held = store.UniqueAccess();
  ASSERT_TRUE(held);

  std::optional<std::unique_ptr<storage::Accessor>> result;
  std::exception_ptr eptr;
  std::atomic<bool> done{false};

  // Short deadline (a couple of ~100ms monitor sweep periods) -- fast but exercises the real sweep.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(150);
  auto driver =
      MakeDriver(store, storage::UNIQUE, std::nullopt, deadline, pool, /*is_high_priority=*/false, result, eptr, done);

  pool.ScheduledAddTask([&](auto /*priority*/) { driver.Run(); }, utils::Priority::LOW);

  ASSERT_TRUE(BoundedWaitUntil([&] { return done.load(); }, std::chrono::milliseconds(2000)))
      << "deadline-sweep never woke the parked coroutine to time it out";

  ASSERT_FALSE(result.has_value());
  ASSERT_TRUE(eptr) << "expected a UniqueAccessTimeout, got neither a result nor an exception";
  EXPECT_THROW(std::rethrow_exception(eptr), storage::UniqueAccessTimeout);

  held->Abort();
  held.reset();

  pool.ShutDown();
  pool.AwaitShutdown();
}

// (c) FAST PATH: uncontended acquisition succeeds immediately and never registers a waiter.
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

// (d) HIGH priority bypasses the park path (even flag-on + InMemory): it blocks on the ordinary
// path and still acquires once the holder releases.
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

  // HIGH priority: blocks on the ordinary UniqueAccess() path (never parks) until the releaser frees it.
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
