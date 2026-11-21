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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/durability/retention_floor.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

using memgraph::versioning::Branch;
using memgraph::versioning::BranchCore;
using memgraph::versioning::BranchInfo;
using memgraph::versioning::CoreResult;
using memgraph::versioning::DropResult;
using memgraph::versioning::ForkPoint;
using memgraph::versioning::VersionStore;

namespace {
constexpr uint64_t kNoRetentionFloor = memgraph::storage::durability::kNoRetentionFloor;

// Non-null BranchCore* that is never dereferenced (incomplete type here).
CoreResult MakeCore() {
  auto token = std::make_shared<int>(0);
  return std::shared_ptr<BranchCore>{token, reinterpret_cast<BranchCore *>(token.get())};
}
}  // namespace

class VersionStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::path{std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "/tmp/lima/tmp"} /
           ("mg_version_store_" + memgraph::utils::GenerateUUID());
    kv_ = std::make_unique<memgraph::kvstore::KVStore>(dir_ / "kv");
    root_ = dir_ / "branches";
  }

  void TearDown() override {
    kv_.reset();
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  uint64_t Acquire() {
    const std::scoped_lock lock{live_mutex_};
    const auto ts = next_ts_++;
    live_.push_back(ts);
    return ts;
  }

  void Release(uint64_t ts) {
    const std::scoped_lock lock{live_mutex_};
    const auto it = std::ranges::find(live_, ts);
    ASSERT_NE(it, live_.end()) << "released a pin that is not live";
    live_.erase(it);
  }

  std::unique_ptr<VersionStore> MakeStore() {
    return std::make_unique<VersionStore>(
        [this] {
          const auto ts = Acquire();
          return ForkPoint{.durable_fork_ts = ts,
                           .main_durable_ts = ts - 1,
                           .vertex_gid_floor = ts * 10,
                           .edge_gid_floor = ts * 10 + 1,
                           .counts = {.vertices = 4, .edges = 5}};
        },
        [this](uint64_t ts) { Release(ts); },
        [this](uint64_t floor) { floors_.push_back(floor); },
        Ledger(),
        root_);
  }

  memgraph::versioning::BranchLedger Ledger() const { return memgraph::versioning::BranchLedger{*kv_, uuid_}; }

  // The ledger refuses every write while next_branch_id is below 2, standing in for a failing KVStore.
  static void BreakLedger(VersionStore &store) { store.Restore(1, {}); }

  std::filesystem::path BranchDir(uint64_t id) const { return root_ / std::to_string(id); }

  void MakeDir(uint64_t id) const { std::filesystem::create_directories(BranchDir(id)); }

  std::filesystem::path dir_;
  std::filesystem::path root_;
  std::unique_ptr<memgraph::kvstore::KVStore> kv_;
  memgraph::utils::UUID uuid_;
  uint64_t next_ts_{100};
  std::mutex live_mutex_;
  std::vector<uint64_t> live_;
  std::vector<uint64_t> floors_;
};

TEST_F(VersionStoreTest, IdsStartAtTwoAndAreNeverReused) {
  auto store = MakeStore();
  EXPECT_TRUE(store->Empty());
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a && b);
  EXPECT_EQ(a->id, 2);
  EXPECT_EQ(b->id, 3);
  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  auto c = store->CreateBranch("c");
  ASSERT_TRUE(c);
  EXPECT_EQ(c->id, 4);
  EXPECT_FALSE(store->Empty());
  EXPECT_EQ(store->Get("a")->info.id, 2);
  EXPECT_FALSE(store->Exists("b"));
}

TEST_F(VersionStoreTest, CreateRecordsForkPointAndPersistsIt) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a);
  EXPECT_EQ(store->Get("a")->fork_ts, 100);
  EXPECT_EQ(a->fork.durable_fork_ts, 100);
  EXPECT_EQ(a->fork.main_durable_ts, 99);
  EXPECT_EQ(a->fork.vertex_gid_floor, 1000);
  EXPECT_EQ(a->fork.edge_gid_floor, 1001);
  EXPECT_NE(a->created_at_us, 0);
  EXPECT_EQ(a->fork.counts, (memgraph::versioning::ObjectCounts{4, 5}));
  EXPECT_EQ(live_, std::vector<uint64_t>{100});

  auto read = Ledger().Read();
  ASSERT_TRUE(read && *read);
  EXPECT_EQ((*read)->next_branch_id, 3);
  ASSERT_EQ((*read)->branches.size(), 1);
  const auto &persisted = (*read)->branches[0];
  EXPECT_EQ(persisted.name, "a");
  EXPECT_EQ(persisted.info.id, 2);
  EXPECT_EQ(persisted.info.fork, a->fork);
}

TEST_F(VersionStoreTest, DuplicateNameIsRefusedWithoutAPin) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto dup = store->CreateBranch("a");
  ASSERT_FALSE(dup);
  EXPECT_THAT(dup.error(), testing::HasSubstr("already exists"));
  EXPECT_EQ(live_.size(), 1);
  EXPECT_EQ(store->List().size(), 1);
}

TEST_F(VersionStoreTest, LedgerWriteFailureLeavesNoPinOrBranchAndDropsNothing) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  MakeDir(2);
  BreakLedger(*store);

  EXPECT_FALSE(store->CreateBranch("b"));
  EXPECT_FALSE(store->Exists("b"));
  EXPECT_EQ(live_, std::vector<uint64_t>{100});

  EXPECT_FALSE(store->DropBranch("a"));
  EXPECT_TRUE(store->Exists("a"));
  EXPECT_EQ(live_, std::vector<uint64_t>{100});
  EXPECT_TRUE(std::filesystem::exists(BranchDir(2)));
  EXPECT_TRUE(store->Checkout("a")) << "a failed drop leaves the branch usable";

  store->Restore(3, {});
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b);
  EXPECT_EQ(b->id, 3) << "a failed create consumes no id";
}

TEST_F(VersionStoreTest, FailedCreateRestoresTheIdCounter) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  ASSERT_TRUE(store->CreateBranch("b"));
  store->Restore(3, {});  // the counter now collides with b's id, so the ledger refuses the next write

  EXPECT_FALSE(store->CreateBranch("c"));
  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  auto c = store->CreateBranch("c");
  ASSERT_TRUE(c);
  EXPECT_EQ(c->id, 3) << "the failed create gave its id back";
}

TEST_F(VersionStoreTest, DropUnknownIsNotFound) {
  auto store = MakeStore();
  EXPECT_EQ(store->DropBranch("nope"), DropResult::kNotFound);
}

TEST_F(VersionStoreTest, DropOfIdleBranchReleasesPinAndDirectoryImmediately) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a);
  MakeDir(a->id);
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_TRUE(live_.empty());
  EXPECT_FALSE(std::filesystem::exists(BranchDir(a->id)));
  EXPECT_FALSE(store->Exists("a"));
  auto read = Ledger().Read();
  ASSERT_TRUE(read && *read);
  EXPECT_TRUE((*read)->branches.empty());
}

TEST_F(VersionStoreTest, DropIsRefusedWhileCheckedOutUnlessForced) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto session = store->Checkout("a");
  ASSERT_TRUE(session);

  EXPECT_EQ(store->DropBranch("a"), DropResult::kInUse);
  EXPECT_TRUE(store->Exists("a"));
  EXPECT_FALSE((*session)->dropped());
  EXPECT_EQ(live_.size(), 1);

  EXPECT_EQ(store->Get("a")->sessions, 1);
  EXPECT_EQ(store->DropBranch("a", /*force=*/true), DropResult::kDropped);
  EXPECT_FALSE(store->Exists("a"));
  EXPECT_TRUE((*session)->dropped());
}

TEST_F(VersionStoreTest, DropIsAllowedOnceTheCheckoutIsGone) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  {
    auto session = store->Checkout("a");
    ASSERT_TRUE(session);
    EXPECT_EQ(store->DropBranch("a"), DropResult::kInUse);
  }
  EXPECT_EQ(store->Get("a")->sessions, 0);
  EXPECT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_TRUE(live_.empty());
}

TEST_F(VersionStoreTest, MovedFromCheckoutReleasesNothing) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto s1 = *store->Checkout("a");
  auto s2 = std::move(s1);
  EXPECT_FALSE(s1);
  EXPECT_TRUE(s2);
  s1.reset();
  EXPECT_EQ(store->Get("a")->sessions, 1);
  s2 = memgraph::versioning::BranchCheckout{};
  EXPECT_EQ(store->Get("a")->sessions, 0);
}

TEST_F(VersionStoreTest, ForcedDropKeepsSessionsUsableAndReleasesWithTheLastOne) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a);
  MakeDir(a->id);
  auto s1 = *store->Checkout("a");
  auto s2 = *store->Checkout("a");
  ASSERT_EQ(store->DropBranch("a", true), DropResult::kDropped);

  EXPECT_EQ(s1->info().id, a->id);
  EXPECT_EQ(s1->name(), "a");
  EXPECT_TRUE(s1->GetOrBuildCore([] { return MakeCore(); }));

  s1.reset();
  EXPECT_EQ(live_.size(), 1) << "the pin outlives all but the last session";
  EXPECT_TRUE(std::filesystem::exists(BranchDir(a->id)));
  s2.reset();
  EXPECT_TRUE(live_.empty());
  EXPECT_FALSE(std::filesystem::exists(BranchDir(a->id)));
}

TEST_F(VersionStoreTest, RecreatedNameGetsNewIdAndOldCheckoutIsUnaffected) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a);
  MakeDir(a->id);
  auto old_session = *store->Checkout("a");
  ASSERT_EQ(store->DropBranch("a", true), DropResult::kDropped);

  auto again = store->CreateBranch("a");
  ASSERT_TRUE(again);
  EXPECT_NE(again->id, a->id);
  EXPECT_EQ(old_session->info().id, a->id);
  EXPECT_TRUE(old_session->dropped());
  EXPECT_EQ((*store->Checkout("a"))->info().id, again->id);
  EXPECT_EQ(live_.size(), 2);

  old_session.reset();
  EXPECT_EQ(live_, std::vector<uint64_t>{again->fork.durable_fork_ts});
}

TEST_F(VersionStoreTest, CheckoutRefusesUnknownBranch) {
  auto store = MakeStore();
  auto res = store->Checkout("nope");
  ASSERT_FALSE(res);
  EXPECT_THAT(res.error(), testing::HasSubstr("does not exist"));
}

TEST_F(VersionStoreTest, CheckoutRefusesUnavailableBranchNamingReasonAndDrop) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a);
  store->MarkUnavailable(a->id, "files are corrupt.");
  EXPECT_EQ(store->Get("a")->unavailable_reason, "files are corrupt.");
  auto res = store->Checkout("a");
  ASSERT_FALSE(res);
  EXPECT_THAT(res.error(), testing::HasSubstr("files are corrupt"));
  EXPECT_THAT(res.error(), testing::HasSubstr("DROP BRANCH 'a'"));
  EXPECT_EQ(store->DropBranch("a"), DropResult::kDropped) << "an unavailable branch can still be dropped";
}

TEST_F(VersionStoreTest, ListCountsCheckoutsAndGaugeReturnsToStart) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("b"));
  ASSERT_TRUE(store->CreateBranch("a"));
  auto &sessions = *memgraph::metrics::Metrics().global.versioning_active_sessions;
  const auto before = sessions.Value();

  auto s1 = *store->Checkout("a");
  auto s2 = *store->Checkout("a");
  auto s3 = *store->Checkout("b");
  EXPECT_EQ(sessions.Value() - before, 3.0);
  auto listed = store->List();
  ASSERT_EQ(listed.size(), 2);
  EXPECT_EQ(listed[0].name, "b") << "creation order, not name order";
  EXPECT_EQ(listed[0].sessions, 1);
  EXPECT_EQ(listed[1].name, "a");
  EXPECT_EQ(listed[1].sessions, 2);

  s2.reset();
  EXPECT_EQ(store->List()[1].sessions, 1);
  s1.reset();
  s3.reset();
  EXPECT_EQ(sessions.Value(), before);
  EXPECT_EQ(store->List()[0].sessions, 0);
}

TEST_F(VersionStoreTest, ConcurrentFirstCallersBuildTheCoreOnce) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto session = *store->Checkout("a");
  constexpr size_t kThreads = 8;
  std::atomic<int> builds{0};
  std::latch start{kThreads};
  std::vector<std::shared_ptr<BranchCore>> results(kThreads);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      auto core = session->GetOrBuildCore([&] {
        ++builds;
        for (int y = 0; y < 1000; ++y) std::this_thread::yield();
        return MakeCore();
      });
      if (core) results[i] = *core;
    });
  }
  for (auto &t : threads) t.join();
  EXPECT_EQ(builds.load(), 1);
  ASSERT_NE(results[0], nullptr);
  for (const auto &r : results) EXPECT_EQ(r, results[0]);
}

TEST_F(VersionStoreTest, FailedBuildIsNotCached) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto session = *store->Checkout("a");
  int builds = 0;
  auto failed = session->GetOrBuildCore([&]() -> CoreResult {
    ++builds;
    return std::unexpected(std::string{"boom"});
  });
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), "boom");
  auto retry = session->GetOrBuildCore([&] {
    ++builds;
    return MakeCore();
  });
  ASSERT_TRUE(retry);
  EXPECT_EQ(builds, 2);
  EXPECT_EQ(*session->GetOrBuildCore([&]() -> CoreResult {
    ADD_FAILURE() << "built twice";
    return MakeCore();
  }),
            *retry);
}

TEST_F(VersionStoreTest, SeparateBranchesBuildIndependently) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  ASSERT_TRUE(store->CreateBranch("b"));
  auto a = *store->Checkout("a");
  auto b = *store->Checkout("b");
  int builds = 0;
  auto build = [&] {
    ++builds;
    return MakeCore();
  };
  auto ca = a->GetOrBuildCore(build);
  auto cb = b->GetOrBuildCore(build);
  ASSERT_TRUE(ca && cb);
  EXPECT_EQ(builds, 2);
  EXPECT_NE(*ca, *cb);
}

TEST_F(VersionStoreTest, RestoreInstallsBranchesTakesPinsAndPublishesFloor) {
  auto store = MakeStore();
  const auto ts_a = Acquire();
  const auto ts_b = Acquire();
  const std::vector<VersionStore::RestoredBranch> restored{
      {.branch = {.name = "a", .info = {.id = 4, .fork = {.durable_fork_ts = 50}}}, .runtime_pin = ts_a},
      {.branch = {.name = "b", .info = {.id = 7, .fork = {.durable_fork_ts = 30}}}, .runtime_pin = ts_b}};
  store->Restore(9, restored);
  EXPECT_FALSE(store->Empty());
  EXPECT_EQ(store->Get("a")->info.id, 4);
  EXPECT_EQ(store->Get("b")->info.id, 7);
  EXPECT_EQ(store->Get("a")->fork_ts, ts_a) << "the runtime pin, not durable_fork_ts";
  EXPECT_EQ(store->Get("b")->fork_ts, ts_b);
  EXPECT_EQ(floors_.back(), 29) << "min RecoverableBelow over the restored branches";

  auto fresh = store->CreateBranch("c");
  ASSERT_TRUE(fresh);
  EXPECT_EQ(fresh->id, 9);

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_EQ(std::ranges::count(live_, ts_a), 0) << "a restored pin is released on drop";
  store.reset();
  EXPECT_TRUE(live_.empty()) << "the store releases the pins it still owns";
}

TEST_F(VersionStoreTest, RestoreOfNothingPublishesNoFloorAndStaysEmpty) {
  auto store = MakeStore();
  store->Restore(5, {});
  EXPECT_TRUE(store->Empty());
  ASSERT_FALSE(floors_.empty());
  EXPECT_EQ(floors_.back(), kNoRetentionFloor);
  EXPECT_EQ(store->CreateBranch("a")->id, 5);
}

TEST_F(VersionStoreTest, RetentionFloorIsRepublishedAfterFailedCreateAndAfterDrop) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  ASSERT_TRUE(store->CreateBranch("b"));
  BreakLedger(*store);
  floors_.clear();
  ASSERT_FALSE(store->CreateBranch("c"));
  ASSERT_EQ(floors_.size(), 1);
  EXPECT_EQ(floors_.back(), 99) << "the failed branch's pin does not lower the floor";

  store->Restore(4, {});
  floors_.clear();
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_EQ(floors_.back(), 100) << "only b remains";
  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  EXPECT_EQ(floors_.back(), kNoRetentionFloor);
}

TEST_F(VersionStoreTest, RestartRestoresWhatTheLedgerHolds) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a"));
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b);
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  store.reset();
  ASSERT_TRUE(live_.empty());

  auto read = Ledger().Read();
  ASSERT_TRUE(read && *read);
  const auto fresh_pin = Acquire();
  ASSERT_NE(fresh_pin, b->fork.durable_fork_ts);
  store = MakeStore();
  std::vector<VersionStore::RestoredBranch> restored;
  restored.push_back({.branch = (*read)->branches[0], .runtime_pin = fresh_pin});
  store->Restore((*read)->next_branch_id, restored);

  const auto listed = store->List();
  ASSERT_EQ(listed.size(), 1);
  EXPECT_EQ(listed[0].name, "b");
  EXPECT_EQ(listed[0].info.id, b->id);
  EXPECT_EQ(listed[0].info.fork.durable_fork_ts, b->fork.durable_fork_ts);
  EXPECT_EQ(listed[0].info.created_at_us, b->created_at_us);
  EXPECT_EQ(listed[0].fork_ts, fresh_pin) << "the fresh runtime pin, not durable_fork_ts";

  auto next = store->CreateBranch("c");
  ASSERT_TRUE(next);
  EXPECT_EQ(next->id, 4);
}

TEST_F(VersionStoreTest, ConcurrentCheckoutsAndForcedDropsReleaseEachPinOnce) {
  auto store = MakeStore();
  constexpr int kRounds = 200;
  std::atomic<bool> stop{false};
  std::vector<std::thread> sessions;
  for (int i = 0; i < 4; ++i) {
    sessions.emplace_back([&] {
      while (!stop.load()) {
        if (auto s = store->Checkout("a")) std::this_thread::yield();
      }
    });
  }
  for (int i = 0; i < kRounds; ++i) {
    ASSERT_TRUE(store->CreateBranch("a"));
    std::this_thread::yield();
    ASSERT_EQ(store->DropBranch("a", /*force=*/true), DropResult::kDropped);
  }
  stop = true;
  for (auto &t : sessions) t.join();
  EXPECT_TRUE(live_.empty());
  EXPECT_TRUE(store->Empty());
}
