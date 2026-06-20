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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "gtest/gtest.h"

#include "utils/worker_yield_signal.hpp"

using memgraph::utils::WorkerYieldRegistry;

// Each test runs on the gtest main thread; ClearCurrentWorker resets the
// thread-local state so tests do not leak the "current worker" across cases.
namespace {
struct TlsGuard {
  TlsGuard() { WorkerYieldRegistry::ClearCurrentWorker(); }

  ~TlsGuard() { WorkerYieldRegistry::ClearCurrentWorker(); }
};
}  // namespace

TEST(WorkerYieldRegistry, FreshSignalsAreFalse) {
  WorkerYieldRegistry registry(4);
  EXPECT_EQ(registry.MaxWorkers(), 4);
  for (WorkerYieldRegistry::WorkerId i = 0; i < 4; ++i) {
    EXPECT_FALSE(registry.GetSignalForWorker(i)->load(std::memory_order_acquire));
  }
}

TEST(WorkerYieldRegistry, DistinctSignalsPerWorker) {
  WorkerYieldRegistry registry(8);
  // Each worker gets its own distinct, stable signal pointer.
  for (WorkerYieldRegistry::WorkerId i = 0; i < 8; ++i) {
    for (WorkerYieldRegistry::WorkerId j = 0; j < 8; ++j) {
      if (i == j) {
        EXPECT_EQ(registry.GetSignalForWorker(i), registry.GetSignalForWorker(j));  // stable
      } else {
        EXPECT_NE(registry.GetSignalForWorker(i), registry.GetSignalForWorker(j));  // distinct
      }
    }
  }
}

TEST(WorkerYieldRegistry, NoFalseSharingBetweenSignals) {
  // Each signal must sit on its own cache line so workers polling/clearing
  // adjacent flags do not ping-pong cache lines.
  WorkerYieldRegistry registry(4);
  for (WorkerYieldRegistry::WorkerId i = 0; i + 1 < 4; ++i) {
    auto *a = reinterpret_cast<std::byte *>(registry.GetSignalForWorker(i));
    auto *b = reinterpret_cast<std::byte *>(registry.GetSignalForWorker(i + 1));
    const auto distance = static_cast<std::size_t>(b - a);
    EXPECT_GE(distance, 64U) << "adjacent signals share a cache line (false sharing)";
  }
}

TEST(WorkerYieldRegistry, RequestAndClearForWorker) {
  WorkerYieldRegistry registry(2);
  registry.RequestYieldForWorker(1);
  EXPECT_FALSE(registry.GetSignalForWorker(0)->load(std::memory_order_acquire));  // unaffected
  EXPECT_TRUE(registry.GetSignalForWorker(1)->load(std::memory_order_acquire));
  registry.ClearYieldForWorker(1);
  EXPECT_FALSE(registry.GetSignalForWorker(1)->load(std::memory_order_acquire));
}

TEST(WorkerYieldRegistry, OffWorkerCurrentSignalIsNull) {
  // THE behavior-preservation contract: a thread that never called
  // SetCurrentWorker sees a null current signal -> "yield never requested".
  TlsGuard guard;
  EXPECT_EQ(WorkerYieldRegistry::GetCurrentYieldSignal(), nullptr);
  EXPECT_FALSE(WorkerYieldRegistry::GetCurrentWorkerId().has_value());
}

TEST(WorkerYieldRegistry, SetCurrentWorkerPublishesSignal) {
  TlsGuard guard;
  WorkerYieldRegistry registry(4);
  registry.SetCurrentWorker(2);
  EXPECT_EQ(WorkerYieldRegistry::GetCurrentYieldSignal(), registry.GetSignalForWorker(2));
  ASSERT_TRUE(WorkerYieldRegistry::GetCurrentWorkerId().has_value());
  EXPECT_EQ(*WorkerYieldRegistry::GetCurrentWorkerId(), 2);
}

TEST(WorkerYieldRegistry, SetCurrentWorkerClearsStaleFlag) {
  TlsGuard guard;
  WorkerYieldRegistry registry(4);
  registry.RequestYieldForWorker(3);
  EXPECT_TRUE(registry.GetSignalForWorker(3)->load(std::memory_order_acquire));
  // A worker picking up a new task must start with a clean flag.
  registry.SetCurrentWorker(3);
  EXPECT_FALSE(registry.GetSignalForWorker(3)->load(std::memory_order_acquire));
  EXPECT_FALSE(WorkerYieldRegistry::GetCurrentYieldSignal()->load(std::memory_order_acquire));
}

TEST(WorkerYieldRegistry, ClearCurrentWorkerResetsTls) {
  TlsGuard guard;
  WorkerYieldRegistry registry(4);
  registry.SetCurrentWorker(1);
  ASSERT_NE(WorkerYieldRegistry::GetCurrentYieldSignal(), nullptr);
  WorkerYieldRegistry::ClearCurrentWorker();
  EXPECT_EQ(WorkerYieldRegistry::GetCurrentYieldSignal(), nullptr);
  EXPECT_FALSE(WorkerYieldRegistry::GetCurrentWorkerId().has_value());
}

TEST(WorkerYieldRegistry, ClearYieldForCurrentWorker) {
  TlsGuard guard;
  WorkerYieldRegistry registry(4);
  registry.SetCurrentWorker(0);
  // Simulate an external yield request, then the worker clears it after yielding.
  registry.RequestYieldForWorker(0);
  EXPECT_TRUE(WorkerYieldRegistry::GetCurrentYieldSignal()->load(std::memory_order_acquire));
  WorkerYieldRegistry::ClearYieldForCurrentWorker();
  EXPECT_FALSE(registry.GetSignalForWorker(0)->load(std::memory_order_acquire));
}

TEST(WorkerYieldRegistry, TlsIsPerThread) {
  TlsGuard guard;
  WorkerYieldRegistry registry(4);
  registry.SetCurrentWorker(0);  // main thread is "worker 0"

  std::atomic<bool> other_saw_null{false};
  std::atomic<bool> other_saw_own{false};
  std::thread other([&] {
    // A fresh thread must NOT inherit the main thread's current signal.
    other_saw_null.store(WorkerYieldRegistry::GetCurrentYieldSignal() == nullptr, std::memory_order_release);
    registry.SetCurrentWorker(1);
    other_saw_own.store(WorkerYieldRegistry::GetCurrentYieldSignal() == registry.GetSignalForWorker(1),
                        std::memory_order_release);
    WorkerYieldRegistry::ClearCurrentWorker();
  });
  other.join();

  EXPECT_TRUE(other_saw_null.load(std::memory_order_acquire));
  EXPECT_TRUE(other_saw_own.load(std::memory_order_acquire));
  // Main thread's TLS is unaffected by the other thread.
  EXPECT_EQ(WorkerYieldRegistry::GetCurrentYieldSignal(), registry.GetSignalForWorker(0));
}

TEST(WorkerYieldRegistry, CrossThreadYieldVisibility) {
  // External thread sets a worker's flag; the "worker" thread polls its current
  // signal and observes the request (release store / acquire load).
  WorkerYieldRegistry registry(2);
  std::atomic<bool> worker_ready{false};
  std::atomic<bool> worker_observed{false};

  std::thread worker([&] {
    registry.SetCurrentWorker(0);
    worker_ready.store(true, std::memory_order_release);
    auto *signal = WorkerYieldRegistry::GetCurrentYieldSignal();
    while (!signal->load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    worker_observed.store(true, std::memory_order_release);
    WorkerYieldRegistry::ClearCurrentWorker();
  });

  while (!worker_ready.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  registry.RequestYieldForWorker(0);  // external request
  worker.join();
  EXPECT_TRUE(worker_observed.load(std::memory_order_acquire));
}

TEST(WorkerYieldRegistry, SignalPointerStableAfterRequests) {
  // Mutating flags must never move the underlying storage (raw TLS pointers
  // captured by workers must stay valid for the registry's lifetime).
  WorkerYieldRegistry registry(3);
  auto *before = registry.GetSignalForWorker(2);
  registry.RequestYieldForWorker(0);
  registry.RequestYieldForWorker(1);
  registry.RequestYieldForWorker(2);
  registry.ClearYieldForWorker(2);
  EXPECT_EQ(registry.GetSignalForWorker(2), before);
}
