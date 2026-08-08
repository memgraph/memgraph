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
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "communication/v2/session_registry.hpp"

using memgraph::communication::v2::SessionRegistry;
using memgraph::communication::v2::TerminableSession;

namespace {

// Minimal TerminableSession double. The destructor mirrors ~Session: it deregisters itself,
// which is exactly the path that must not deadlock if Find() returned while still holding the
// registry's lock.
struct FakeSession : TerminableSession {
  explicit FakeSession(std::string uuid, std::atomic_bool *destroyed_flag = nullptr)
      : uuid_(std::move(uuid)), destroyed_flag_(destroyed_flag) {}

  ~FakeSession() override {
    SessionRegistry::Instance().Deregister(uuid_, this);
    if (destroyed_flag_ != nullptr) destroyed_flag_->store(true, std::memory_order_release);
  }

  void RequestTermination() override { ++terminations_; }

  std::string uuid_;
  std::atomic_int terminations_{0};
  std::atomic_bool *destroyed_flag_;
};

// SessionRegistry::Instance() is a process-wide singleton shared by every test in this binary;
// derive a fresh uuid per call so tests can never collide with one another's entries.
std::string UniqueUuid(std::string_view label) {
  static std::atomic<uint64_t> counter{0};
  return std::string("session_registry_test.")
      .append(label)
      .append("#")
      .append(std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
}

}  // namespace

// Proves the basic round trip and that an unknown uuid is a clean miss.
TEST(SessionRegistryTest, RegisterFindDeregister) {
  auto &registry = SessionRegistry::Instance();
  const std::string uuid = UniqueUuid("RegisterFindDeregister");
  const std::string unknown_uuid = UniqueUuid("RegisterFindDeregister.unknown");

  auto session = std::make_shared<FakeSession>(uuid);
  registry.Register(uuid, session);

  auto found = registry.Find(uuid);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found.get(), session.get());
  found.reset();

  EXPECT_EQ(registry.Find(unknown_uuid), nullptr);

  session.reset();  // runs ~FakeSession -> Deregister
  EXPECT_EQ(registry.Find(uuid), nullptr);
}

// Proves the registry stores a weak_ptr: dropping the only owning shared_ptr destroys the
// session and makes Find() a miss, with nobody having called Deregister explicitly first.
TEST(SessionRegistryTest, WeakPtrDoesNotExtendLifetime) {
  auto &registry = SessionRegistry::Instance();
  const std::string uuid = UniqueUuid("WeakPtrDoesNotExtendLifetime");
  std::atomic_bool destroyed{false};

  {
    auto session = std::make_shared<FakeSession>(uuid, &destroyed);
    registry.Register(uuid, session);
  }  // `session` goes out of scope; the registry's weak_ptr must not have kept it alive

  EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
  EXPECT_EQ(registry.Find(uuid), nullptr);
}

// A session whose Start() never ran (e.g. Server::OnAccept dropped its shared_ptr immediately)
// is destroyed without ever having registered; Deregister must tolerate that silently.
TEST(SessionRegistryTest, DeregisterUnknownUuidIsNoOp) {
  auto &registry = SessionRegistry::Instance();
  const auto baseline = registry.Size();
  const std::string uuid = UniqueUuid("DeregisterUnknownUuidIsNoOp");

  EXPECT_NO_THROW(registry.Deregister(uuid, nullptr));
  EXPECT_EQ(registry.Size(), baseline);

  {
    FakeSession never_registered(uuid);
  }  // destructor calls Deregister on an unknown uuid
  EXPECT_EQ(registry.Size(), baseline);
}

// A uuid collision (e.g. two rapid connect/disconnect cycles reusing the same generated uuid
// before the first session's destructor runs) leaves the map entry pointing at the second
// session. The first session's destructor must not evict that still-live entry -- if Deregister
// were a bare erase(uuid) instead of the expired-or-owner-equal guard, the first Find() below
// would come back null.
TEST(SessionRegistryTest, DeregisterDoesNotEvictAnotherSessionsEntry) {
  auto &registry = SessionRegistry::Instance();
  const auto baseline = registry.Size();
  const std::string uuid = UniqueUuid("DeregisterDoesNotEvictAnotherSessionsEntry");

  auto s1 = std::make_shared<FakeSession>(uuid);
  registry.Register(uuid, s1);

  auto s2 = std::make_shared<FakeSession>(uuid);
  registry.Register(uuid, s2);  // last writer wins: the map entry now points at s2

  s1.reset();  // ~FakeSession -> Deregister(uuid, s1.get()); stored weak_ptr resolves to s2 != s1

  auto found = registry.Find(uuid);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found.get(), s2.get());
  found.reset();

  s2.reset();  // entry is genuinely stale now; this Deregister must still reclaim it
  EXPECT_EQ(registry.Find(uuid), nullptr);
  EXPECT_EQ(registry.Size(), baseline);
}

// 10k register/destroy cycles must leave the map exactly where it started; deleting the erase
// in Deregister turns this into an unbounded leak.
TEST(SessionRegistryTest, NoLeakAcrossChurn) {
  auto &registry = SessionRegistry::Instance();
  const auto baseline = registry.Size();
  const std::string prefix = UniqueUuid("NoLeakAcrossChurn");

  for (int i = 0; i < 10'000; ++i) {
    const std::string uuid = prefix + "." + std::to_string(i);
    auto session = std::make_shared<FakeSession>(uuid);
    registry.Register(uuid, session);
    ASSERT_NE(registry.Find(uuid), nullptr);
  }  // `session` dies at the end of each iteration -> Deregister runs every time

  EXPECT_EQ(registry.Size(), baseline);
}

// TSan target: 4 threads registering/destroying distinct uuids concurrently with 2 threads
// calling Find(), all bounded by iteration count rather than a timed loop.
TEST(SessionRegistryTest, ConcurrentChurnWhileFinding) {
  auto &registry = SessionRegistry::Instance();
  const auto baseline = registry.Size();
  const std::string prefix = UniqueUuid("ConcurrentChurnWhileFinding");
  constexpr int kChurnThreads = 4;
  constexpr int kChurnIterations = 5'000;
  constexpr int kFindThreads = 2;
  constexpr int kFindIterations = 5'000;

  std::vector<std::thread> threads;
  threads.reserve(kChurnThreads + kFindThreads);

  for (int t = 0; t < kChurnThreads; ++t) {
    threads.emplace_back([&registry, &prefix, t] {
      for (int i = 0; i < kChurnIterations; ++i) {
        const std::string uuid = prefix + "." + std::to_string(t) + "." + std::to_string(i);
        auto session = std::make_shared<FakeSession>(uuid);
        registry.Register(uuid, session);
      }
    });
  }
  for (int t = 0; t < kFindThreads; ++t) {
    threads.emplace_back([&registry, &prefix] {
      for (int i = 0; i < kFindIterations; ++i) {
        auto found = registry.Find(prefix);  // near-always a miss; exercises Find() under churn
        static_cast<void>(found);
      }
    });
  }
  for (auto &thread : threads) thread.join();

  EXPECT_EQ(registry.Size(), baseline);
}

// The load-bearing test: Find() must release the registry's mutex before returning, because the
// returned shared_ptr can be the session's last reference and its destructor re-enters the
// registry via Deregister. If Find() held the lock across its return, this would deadlock on
// the (non-recursive) mutex -- so the drop runs on its own thread with a bounded watchdog
// instead of joining directly, and the test FAILs on timeout rather than hanging the suite.
TEST(SessionRegistryTest, FindDoesNotHoldTheLock) {
  auto &registry = SessionRegistry::Instance();
  const std::string uuid = UniqueUuid("FindDoesNotHoldTheLock");

  auto owner = std::make_shared<FakeSession>(uuid);
  registry.Register(uuid, owner);

  std::shared_ptr<TerminableSession> found = registry.Find(uuid);
  ASSERT_NE(found, nullptr);

  owner.reset();  // `found` is now the only reference to the session

  std::promise<void> dropped_promise;
  auto dropped_future = dropped_promise.get_future();

  std::thread dropper([found = std::move(found), dropped_promise = std::move(dropped_promise)]() mutable {
    found.reset();  // ~FakeSession -> SessionRegistry::Deregister
    dropped_promise.set_value();
  });

  const bool dropped_in_time = dropped_future.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  if (!dropped_in_time) {
    ADD_FAILURE() << "Find() appears to hold the registry lock across its return: destroying the "
                     "last shared_ptr it handed back deadlocked instead of running ~TerminableSession.";
    dropper.detach();  // the thread is stuck inside a self-deadlock; never join it
    return;
  }
  dropper.join();

  EXPECT_EQ(registry.Find(uuid), nullptr);
}

// Pins virtual dispatch through the type-erased TerminableSession base.
TEST(SessionRegistryTest, TerminationIsForwarded) {
  auto &registry = SessionRegistry::Instance();
  const std::string uuid = UniqueUuid("TerminationIsForwarded");

  auto session = std::make_shared<FakeSession>(uuid);
  registry.Register(uuid, session);

  auto found = registry.Find(uuid);
  ASSERT_NE(found, nullptr);
  found->RequestTermination();
  found.reset();

  EXPECT_EQ(session->terminations_.load(), 1);
}
