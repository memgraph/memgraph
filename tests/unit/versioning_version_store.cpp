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

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

using memgraph::versioning::BranchInfo;
using memgraph::versioning::DropResult;
using memgraph::versioning::ForkPoint;
using memgraph::versioning::GidKind;
using memgraph::versioning::kGidReserveChunk;
using memgraph::versioning::LedgerContents;
using memgraph::versioning::VersionStore;

class FakePinRegistry {
 public:
  uint64_t Acquire() {
    const auto ts = next_ts_++;
    live_.push_back(ts);
    return ts;
  }

  void Release(uint64_t fork_ts) {
    released_.push_back(fork_ts);
    auto it = std::find(live_.begin(), live_.end(), fork_ts);
    ASSERT_NE(it, live_.end()) << "Release called for a fork_ts that was never (or already) acquired";
    live_.erase(it);
  }

  uint64_t next_ts_{100};
  std::vector<uint64_t> live_;
  std::vector<uint64_t> released_;
};

class VersionStoreTest : public ::testing::Test {
 protected:
  std::unique_ptr<VersionStore> MakeStore() {
    return std::make_unique<VersionStore>([this] { return ForkPoint{.fork_ts = pins_.Acquire(), .counts = {}}; },
                                          [this](uint64_t ts) { pins_.Release(ts); });
  }

  FakePinRegistry pins_;
};

TEST_F(VersionStoreTest, CreateGetExistsList) {
  auto store = MakeStore();

  EXPECT_FALSE(store->Exists("feature"));
  EXPECT_EQ(store->Get("feature"), std::nullopt);
  EXPECT_TRUE(store->List().empty());
  EXPECT_TRUE(store->Empty());

  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(created->number, 2);
  EXPECT_EQ(created->parent, "main");
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{created->fork_ts});
  EXPECT_TRUE(pins_.released_.empty());

  EXPECT_TRUE(store->Exists("feature"));
  EXPECT_FALSE(store->Empty());
  auto fetched = store->Get("feature");
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->number, created->number);
  EXPECT_EQ(fetched->fork_ts, created->fork_ts);

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 1);
  EXPECT_EQ(listed[0].first, "feature");

  ASSERT_EQ(store->DropBranch("feature"), DropResult::kDropped);
  EXPECT_TRUE(pins_.live_.empty());
  EXPECT_EQ(pins_.released_, std::vector<uint64_t>{created->fork_ts});
}

TEST_F(VersionStoreTest, ListIsOrderedByCreationNotName) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("b").has_value());
  ASSERT_TRUE(store->CreateBranch("a").has_value());

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 2);
  EXPECT_EQ(listed[0].first, "b");
  EXPECT_EQ(listed[1].first, "a");
  EXPECT_LT(listed[0].second.number, listed[1].second.number);
}

TEST_F(VersionStoreTest, MonotonicNumberingNeverReusedAcrossDrop) {
  auto store = MakeStore();

  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->number, 2);

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_TRUE(store->Empty());

  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3);
}

TEST_F(VersionStoreTest, DuplicateNameRejected) {
  auto store = MakeStore();

  ASSERT_TRUE(store->CreateBranch("feature").has_value());
  auto duplicate = store->CreateBranch("feature");
  EXPECT_FALSE(duplicate.has_value());

  EXPECT_EQ(pins_.live_.size(), 1);
}

TEST_F(VersionStoreTest, DropUnknownBranchReportsNotFound) {
  auto store = MakeStore();
  auto live = store->CreateBranch("live");
  ASSERT_TRUE(live.has_value());

  EXPECT_EQ(store->DropBranch("nope"), DropResult::kNotFound);
  EXPECT_EQ(store->ForceDropBranch("nope"), DropResult::kNotFound);
  EXPECT_TRUE(store->Exists("live"));
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{live->fork_ts});
  EXPECT_TRUE(pins_.released_.empty());
}

TEST_F(VersionStoreTest, RestoreUsesNewPinsKeepsRecordedForkAndContinuesNumbering) {
  LedgerContents contents;
  contents.next_number = 5;
  contents.branches.push_back({"a", BranchInfo{.number = 2, .parent = "main", .fork_ts = 10, .recorded_fork_ts = 10}});
  contents.branches.push_back({"c", BranchInfo{.number = 4, .parent = "a", .fork_ts = 30, .recorded_fork_ts = 30}});
  const std::vector<uint64_t> pins{pins_.Acquire(), pins_.Acquire()};
  contents.branches[0].info.fork_ts = pins[0];
  contents.branches[1].info.fork_ts = pins[1];

  auto store = MakeStore();
  std::optional<uint64_t> floor;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> f) { floor = f; });
  store->Restore(contents);

  EXPECT_EQ(floor, 9U) << "the floor follows the recorded fork, not the restart pin";
  ASSERT_EQ(store->List().size(), 2);
  EXPECT_EQ(store->Get("a")->fork_ts, pins[0]);
  EXPECT_EQ(store->Get("a")->recorded_fork_ts, 10U);
  EXPECT_EQ(store->Get("c")->parent, "a") << "a restored parent is kept";

  auto next = store->CreateBranch("d");
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->number, 5);

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_EQ(pins_.released_, std::vector<uint64_t>{pins[0]}) << "drop releases the restart pin";
  EXPECT_EQ(floor, 29U);
}

TEST(VersionStoreStorageTest, PinsAndRetentionFloorFollowTheStorageAndMetricsMove) {
  namespace ms = memgraph::storage;
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  auto storage = std::make_unique<ms::InMemoryStorage>(config);
  auto *mem = static_cast<ms::InMemoryStorage *>(storage.get());
  {
    auto acc = storage->Access(ms::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto &global = memgraph::metrics::Metrics().global;
  const auto created_before = global.versioning_branches_created->Value();
  const auto dropped_before = global.versioning_branches_dropped->Value();
  const auto active_before = global.versioning_active_branches->Value();

  auto store =
      std::make_unique<VersionStore>([mem] { return ForkPoint{.fork_ts = mem->RegisterBranchForkPin(), .counts = {}}; },
                                     [mem](uint64_t fork_ts) { mem->ReleaseForkPin(fork_ts); });
  store->SetRetentionFloorCallback([mem](std::optional<uint64_t> floor) { mem->SetRetentionFloor(floor); });

  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  ASSERT_GT(a->fork_ts, 0U);
  EXPECT_EQ(mem->OldestForkPin(), a->fork_ts);
  EXPECT_EQ(mem->RetentionFloor(), a->fork_ts - 1);
  ASSERT_TRUE(store->CreateBranch("b").has_value());
  EXPECT_EQ(global.versioning_branches_created->Value() - created_before, 2.0);
  EXPECT_EQ(global.versioning_active_branches->Value() - active_before, 2.0);

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_EQ(global.versioning_branches_dropped->Value() - dropped_before, 1.0);
  EXPECT_EQ(global.versioning_active_branches->Value() - active_before, 1.0);
  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  EXPECT_EQ(mem->OldestForkPin(), std::nullopt);
  EXPECT_EQ(mem->RetentionFloor(), std::nullopt);

  ASSERT_TRUE(store->CreateBranch("c").has_value());
  store.reset();
  EXPECT_EQ(global.versioning_active_branches->Value(), active_before) << "a destroyed store leaves the gauge";
}

TEST_F(VersionStoreTest, CheckoutIsCountedAndBlocksDropUntilEverySessionDetached) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());

  auto first = store->TryAcquireCheckout("feature");
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->number, created->number);
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value()) << "a second session may attach to the same branch";
  EXPECT_EQ(store->CheckoutCount("feature"), 2U);

  EXPECT_EQ(store->DropBranch("feature"), DropResult::kInUse);

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 1U);
  EXPECT_EQ(store->DropBranch("feature"), DropResult::kInUse) << "one session is still attached";

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
  EXPECT_EQ(store->DropBranch("feature"), DropResult::kDropped);
  EXPECT_TRUE(pins_.live_.empty()) << "the fork pin is released exactly once, by the drop";

  EXPECT_FALSE(store->TryAcquireCheckout("nope").has_value());
  EXPECT_EQ(store->CheckoutCount("nope"), 0U);
}

TEST_F(VersionStoreTest, ForceDropDropsAnAttachedBranchAndItsStaleAttachIsHarmless) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto sessions0 = global.versioning_active_sessions->Value();

  auto store = MakeStore();
  auto old_branch = store->CreateBranch("feature");
  ASSERT_TRUE(old_branch.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value());

  EXPECT_EQ(store->DropBranch("feature"), DropResult::kInUse);
  ASSERT_EQ(store->ForceDropBranch("feature"), DropResult::kDropped);
  EXPECT_FALSE(store->Exists("feature"));
  EXPECT_TRUE(pins_.live_.empty());
  EXPECT_EQ(global.versioning_active_sessions->Value(), sessions0 + 1) << "the attach outlives the drop";

  auto new_branch = store->CreateBranch("feature");
  ASSERT_TRUE(new_branch.has_value());
  EXPECT_NE(new_branch->number, old_branch->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U) << "the stale attach belongs to the dropped branch";
  EXPECT_EQ(store->DropBranch("feature"), DropResult::kDropped);

  store->ReleaseCheckout(old_branch->number);
  EXPECT_EQ(global.versioning_active_sessions->Value(), sessions0);
}

namespace {
using memgraph::versioning::BranchCore;
using CoreResult = std::expected<std::shared_ptr<BranchCore>, std::string>;

// Non-null BranchCore* that is never dereferenced (incomplete type here); `observer` expires when the last
// owner of the core is released.
CoreResult MakeSentinelCore(std::weak_ptr<int> &observer) {
  auto token = std::make_shared<int>(0);
  observer = token;
  return std::shared_ptr<BranchCore>{token, reinterpret_cast<BranchCore *>(token.get())};
}
}  // namespace

TEST_F(VersionStoreTest, CoreIsBuiltOncePerBranch) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());

  int builds = 0;
  std::weak_ptr<int> observer;
  auto build = [&]() -> CoreResult {
    ++builds;
    return MakeSentinelCore(observer);
  };
  auto first = store->GetOrBuildCore(created->number, build);
  auto second = store->GetOrBuildCore(created->number, build);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_NE(first->get(), nullptr);
  EXPECT_EQ(first->get(), second->get());
  EXPECT_EQ(builds, 1);
}

TEST_F(VersionStoreTest, FailedOrThrowingCoreBuildIsNotCached) {
  auto store = MakeStore();
  auto failing = store->CreateBranch("failing");
  auto throwing = store->CreateBranch("throwing");
  ASSERT_TRUE(failing.has_value() && throwing.has_value());

  auto failed =
      store->GetOrBuildCore(failing->number, []() -> CoreResult { return std::unexpected(std::string{"boom"}); });
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), "boom");
  EXPECT_THROW((void)store->GetOrBuildCore(throwing->number, []() -> CoreResult { throw std::runtime_error{"boom"}; }),
               std::runtime_error);

  for (const auto number : {failing->number, throwing->number}) {
    int builds = 0;
    std::weak_ptr<int> observer;
    auto retry = store->GetOrBuildCore(number, [&]() -> CoreResult {
      ++builds;
      return MakeSentinelCore(observer);
    });
    ASSERT_TRUE(retry.has_value());
    EXPECT_NE(retry->get(), nullptr);
    EXPECT_EQ(builds, 1) << "the failed build must not have been cached";
  }
}

// The builder runs unlocked; a drop that lands meanwhile must not leave the freshly built core to be handed out.
TEST_F(VersionStoreTest, BranchDroppedWhileItsCoreIsBuiltIsRefusedNowAndLater) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value());

  std::weak_ptr<int> observer;
  auto result = store->GetOrBuildCore(created->number, [&] {
    EXPECT_EQ(store->ForceDropBranch("feature"), DropResult::kDropped);
    return MakeSentinelCore(observer);
  });
  EXPECT_FALSE(result.has_value()) << "the core of a branch dropped mid-build must not be handed out";
  EXPECT_TRUE(observer.expired());

  int builds = 0;
  auto later = store->GetOrBuildCore(created->number, [&] {
    ++builds;
    return MakeSentinelCore(observer);
  });
  EXPECT_FALSE(later.has_value());
  EXPECT_EQ(builds, 0);
  store->ReleaseCheckout(created->number);
}

TEST_F(VersionStoreTest, DropRetiresOnlyTheDroppedBranchCore) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());

  std::weak_ptr<int> a_core;
  std::weak_ptr<int> b_core;
  int a_builds = 0;
  auto build_a = [&]() -> CoreResult {
    ++a_builds;
    return MakeSentinelCore(a_core);
  };
  auto build_b = [&]() -> CoreResult { return MakeSentinelCore(b_core); };
  ASSERT_TRUE(store->GetOrBuildCore(a->number, build_a).has_value());  // returned handle dropped: cache is sole owner
  ASSERT_TRUE(store->GetOrBuildCore(b->number, build_b).has_value());
  ASSERT_FALSE(a_core.expired());
  ASSERT_FALSE(b_core.expired());

  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  EXPECT_TRUE(b_core.expired()) << "dropping a branch must release its cached core";
  EXPECT_FALSE(a_core.expired()) << "dropping b must not retire a's core";

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_TRUE(a_core.expired());

  auto a2 = store->CreateBranch("a");
  ASSERT_TRUE(a2.has_value());
  ASSERT_TRUE(store->GetOrBuildCore(a2->number, build_a).has_value());
  EXPECT_EQ(a_builds, 2);
}

// No ledger attached: FreezeAndDropAll still retires the cached cores, so no dropped number keeps (or serves) one.
TEST_F(VersionStoreTest, FreezeAndDropAllRetiresCoresWithoutALedger) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value());
  std::weak_ptr<int> observer;
  ASSERT_TRUE(store->GetOrBuildCore(created->number, [&] { return MakeSentinelCore(observer); }).has_value());
  EXPECT_TRUE(store->IsLive("feature", created->number));
  EXPECT_FALSE(store->IsLive("feature", created->number + 1));

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(observer.expired()) << "the dropped branch's core must be released";
  EXPECT_FALSE(store->IsLive("feature", created->number));
  int builds = 0;
  auto refused = store->GetOrBuildCore(created->number, [&] {
    ++builds;
    return MakeSentinelCore(observer);
  });
  EXPECT_FALSE(refused.has_value()) << "a dropped number must not be served a cached core";
  EXPECT_EQ(builds, 0) << "a dropped branch must not be built";
  store->ReleaseCheckout(created->number);
}

TEST_F(VersionStoreTest, ActiveGaugesSumAcrossStores) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto branches0 = global.versioning_active_branches->Value();
  const auto checkouts0 = global.versioning_active_sessions->Value();

  auto a = MakeStore();
  auto b = MakeStore();
  auto x = a->CreateBranch("x");
  ASSERT_TRUE(x);
  ASSERT_TRUE(b->CreateBranch("y"));
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 2);

  a->ReleaseCheckout(x->number);
  EXPECT_EQ(a->CheckoutCount("x"), 0U);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0) << "a release without an attach changes nothing";

  auto x_attach = a->TryAcquireCheckout("x");
  ASSERT_TRUE(x_attach);
  ASSERT_TRUE(b->TryAcquireCheckout("y"));
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 2);

  a->ReleaseCheckout(x_attach->number);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 1);
  ASSERT_EQ(a->DropBranch("x"), DropResult::kDropped);
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 1);

  b.reset();
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0);

  auto restored = MakeStore();
  restored->Restore(
      {.next_number = 4,
       .branches = {{.name = "r2", .info = {.number = 2, .parent = "main", .fork_ts = 5, .recorded_fork_ts = 5}},
                    {.name = "r3", .info = {.number = 3, .parent = "main", .fork_ts = 6, .recorded_fork_ts = 6}}}});
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 2) << "Restore counts the restored branches";
  restored.reset();
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0);
}

TEST_F(VersionStoreTest, FreezeAndDropAllDropsEverythingIncludingAttachedBranches) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto branches0 = global.versioning_active_branches->Value();
  const auto checkouts0 = global.versioning_active_sessions->Value();

  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("a").has_value());
  ASSERT_EQ(global.versioning_active_branches->Value(), branches0 + 2);
  ASSERT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 1);

  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(store->Frozen());
  EXPECT_TRUE(store->Empty());
  EXPECT_TRUE(pins_.live_.empty());
  EXPECT_EQ(pins_.released_.size(), 2U);
  ASSERT_FALSE(floors.empty());
  EXPECT_EQ(floors.back(), std::nullopt);
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 1) << "the checkout gauge follows checked_out_";

  store->ReleaseCheckout(a->number);
  EXPECT_EQ(store->CheckoutCount("a"), 0U);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0);

  EXPECT_TRUE(store->FreezeAndDropAll().has_value()) << "idempotent";
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0);

  store->Unfreeze();
  auto c = store->CreateBranch("c");
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->number, 4) << "numbers are never reused";
}

TEST_F(VersionStoreTest, FrozenStoreRefusesCreateAndCheckoutUntilUnfrozen) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a").has_value());
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());

  constexpr auto kMessage = "Graph Versioning branches are not available on a REPLICA.";
  auto created = store->CreateBranch("b");
  ASSERT_FALSE(created.has_value());
  EXPECT_EQ(created.error(), kMessage);
  auto checkout = store->TryAcquireCheckout("a");
  ASSERT_FALSE(checkout.has_value());
  EXPECT_EQ(checkout.error(), kMessage);
  EXPECT_TRUE(pins_.live_.empty());

  store->Unfreeze();
  EXPECT_FALSE(store->Frozen());
  ASSERT_TRUE(store->CreateBranch("b").has_value());
  EXPECT_TRUE(store->TryAcquireCheckout("b").has_value());
}

class VersionStoreLedgerTest : public VersionStoreTest {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() / ("mg_version_store_freeze_" + memgraph::utils::GenerateUUID());
    file_ = dir_ / "branches" / "ledger";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::unique_ptr<VersionStore> MakeLedgerStore() {
    auto store = MakeStore();
    store->AttachLedger(file_);
    return store;
  }

  std::filesystem::path dir_;
  std::filesystem::path file_;
};

TEST_F(VersionStoreLedgerTest, ReserveGidsIsInTheLedgerWhenItReturns) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value() && b.has_value());
  ASSERT_TRUE(store->RecordGidFloors(a->number, 10, 20).has_value());

  const auto ceiling = store->ReserveGids(a->number, GidKind::kVertex, 100);
  ASSERT_TRUE(ceiling.has_value());
  ASSERT_TRUE(store->ReserveGids(a->number, GidKind::kEdge, 300).has_value());
  ASSERT_TRUE(store->ReserveGids(b->number, GidKind::kEdge, 5).has_value());

  const auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  const auto &branches = (*read)->branches;
  ASSERT_EQ(branches.size(), 2);
  EXPECT_EQ(branches[0].info.vertex_gid_ceiling, *ceiling);
  EXPECT_EQ(branches[0].info.edge_gid_ceiling, 300 + kGidReserveChunk);
  EXPECT_EQ(branches[0].info.vertex_gid_floor, 10) << "the floors stay";
  EXPECT_EQ(branches[1].info.vertex_gid_ceiling, std::nullopt);
  EXPECT_EQ(branches[1].info.edge_gid_ceiling, 5 + kGidReserveChunk);
}

// Every returned ceiling is above what was asked for and in the ledger; the final ledger ceiling is the highest
// returned.
TEST_F(VersionStoreLedgerTest, ConcurrentReservationsEachFindTheirGidCovered) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  constexpr int kThreads = 8;
  std::vector<uint64_t> got(kThreads, 0);
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (uint64_t i = 0; i < 3; ++i) {
        const uint64_t through = 1 + (i * kThreads + t) * (kGidReserveChunk / 2);
        const auto ceiling = store->ReserveGids(a->number, GidKind::kVertex, through);
        ASSERT_TRUE(ceiling.has_value());
        EXPECT_GE(*ceiling, through);
        const auto read = memgraph::versioning::BranchLedger{file_}.Read();
        ASSERT_TRUE(read.has_value() && read->has_value());
        EXPECT_GE((*read)->branches[0].info.vertex_gid_ceiling.value_or(0), through);
        got[t] = std::max(got[t], *ceiling);
      }
    });
  }
  for (auto &thread : threads) thread.join();
  EXPECT_EQ(store->Get("a")->vertex_gid_ceiling, *std::ranges::max_element(got));
}

TEST_F(VersionStoreLedgerTest, FreezeAndDropAllPersistsEmptyRegistryAndRemovesDirectories) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());
  const auto root = file_.parent_path();
  std::filesystem::create_directories(root / std::to_string(a->number));
  std::filesystem::create_directories(root / std::to_string(b->number));

  auto const dropped = store->FreezeAndDropAll();
  ASSERT_TRUE(dropped.has_value());
  EXPECT_THAT(*dropped, testing::UnorderedElementsAre("a", "b"));

  auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  EXPECT_TRUE((*read)->branches.empty());
  EXPECT_EQ((*read)->next_number, 4);
  EXPECT_FALSE(std::filesystem::exists(root / std::to_string(a->number)));
  EXPECT_FALSE(std::filesystem::exists(root / std::to_string(b->number)));
}

TEST_F(VersionStoreLedgerTest, DirectoryOfDroppedBranchIsKeptWhileACheckoutIsHeld) {
  auto store = MakeLedgerStore();
  auto c = store->CreateBranch("c");
  ASSERT_TRUE(c.has_value());
  const auto kept = file_.parent_path() / std::to_string(c->number);
  std::filesystem::create_directories(kept);

  for (const bool built : {false, true}) {
    auto b = store->CreateBranch(built ? "built" : "unbuilt");
    ASSERT_TRUE(b.has_value());
    const auto directory = file_.parent_path() / std::to_string(b->number);
    std::filesystem::create_directories(directory);

    ASSERT_TRUE(store->TryAcquireCheckout(built ? "built" : "unbuilt").has_value());
    ASSERT_TRUE(store->TryAcquireCheckout(built ? "built" : "unbuilt").has_value());
    std::weak_ptr<int> observer;
    CoreResult core;
    if (built) {
      core = store->GetOrBuildCore(b->number, [&] { return MakeSentinelCore(observer); });
      ASSERT_TRUE(core.has_value());
    }
    ASSERT_EQ(store->ForceDropBranch(built ? "built" : "unbuilt"), DropResult::kDropped);
    EXPECT_TRUE(std::filesystem::exists(directory));

    if (built) {
      core->reset();
      EXPECT_TRUE(observer.expired());
    }
    store->ReleaseCheckout(b->number);
    EXPECT_TRUE(std::filesystem::exists(directory)) << "a checkout is still held";

    store->ReleaseCheckout(b->number);
    EXPECT_FALSE(std::filesystem::exists(directory));
  }

  EXPECT_TRUE(std::filesystem::exists(kept)) << "only the dropped branch's directory goes";
}

// Only a core holder is left: the directory waits for it, whether the attach went before or after the drop.
TEST_F(VersionStoreLedgerTest, DirectoryOfDroppedBranchIsKeptWhileOnlyACoreHolderIsLeft) {
  auto store = MakeLedgerStore();
  for (const bool release_before_drop : {false, true}) {
    const auto name = release_before_drop ? "released_first" : "dropped_first";
    auto b = store->CreateBranch(name);
    ASSERT_TRUE(b.has_value());
    const auto directory = file_.parent_path() / std::to_string(b->number);
    std::filesystem::create_directories(directory);

    ASSERT_TRUE(store->TryAcquireCheckout(name).has_value());
    std::weak_ptr<int> observer;
    auto core = store->GetOrBuildCore(b->number, [&] { return MakeSentinelCore(observer); });
    ASSERT_TRUE(core.has_value());

    if (release_before_drop) store->ReleaseCheckout(b->number);
    ASSERT_EQ(store->ForceDropBranch(name), DropResult::kDropped);
    if (!release_before_drop) store->ReleaseCheckout(b->number);
    EXPECT_EQ(store->CheckoutCount(name), 0U);
    EXPECT_TRUE(std::filesystem::exists(directory)) << "the core is still held";

    core->reset();
    EXPECT_TRUE(observer.expired());
    store->ReleaseCheckout(b->number);  // no attach left: this release removes the queued directory
    EXPECT_FALSE(std::filesystem::exists(directory));
  }
}

TEST_F(VersionStoreLedgerTest, DirectoryOfDroppedBranchSurvivesUntilLastNestedHolderReleases) {
  auto store = MakeLedgerStore();
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());
  const auto directory = file_.parent_path() / std::to_string(b->number);
  std::filesystem::create_directories(directory);

  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  std::weak_ptr<int> observer;
  auto outer = store->GetOrBuildCore(b->number, [&] { return MakeSentinelCore(observer); });
  ASSERT_TRUE(outer.has_value());
  auto nested = *outer;

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(std::filesystem::exists(directory));

  nested.reset();
  store->ReleaseCheckout(b->number);
  EXPECT_TRUE(std::filesystem::exists(directory));

  outer->reset();
  store->ReleaseCheckout(b->number);
  EXPECT_FALSE(std::filesystem::exists(directory));
}

TEST_F(VersionStoreLedgerTest, FreezeAndDropAllLedgerFailureChangesNothingAndDoesNotFreeze) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  const auto live_before = pins_.live_;

  auto tmp = file_;
  tmp += ".tmp";
  std::filesystem::create_directory(tmp);

  EXPECT_FALSE(store->FreezeAndDropAll().has_value());
  EXPECT_FALSE(store->Frozen());
  EXPECT_TRUE(store->Exists("a"));
  EXPECT_EQ(pins_.live_, live_before);
  EXPECT_TRUE(pins_.released_.empty());
  EXPECT_TRUE(store->TryAcquireCheckout("a").has_value());
  store->ReleaseCheckout(a->number);

  std::filesystem::remove(tmp);
  EXPECT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(store->Empty());
}

TEST_F(VersionStoreLedgerTest, LedgerWriteFailureFailsReserveGidsAndRecordGidFloorsAndChangesNothing) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  ASSERT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 100), 100 + kGidReserveChunk);

  auto tmp = file_;
  tmp += ".tmp";
  std::filesystem::create_directory(tmp);

  const auto reserved = store->ReserveGids(a->number, GidKind::kVertex, 200 + kGidReserveChunk);
  EXPECT_FALSE(reserved.has_value());
  EXPECT_EQ(store->Get("a")->vertex_gid_ceiling, 100 + kGidReserveChunk) << "the ceiling stays as it was";
  const auto recorded = store->RecordGidFloors(a->number, 10, 20);
  EXPECT_FALSE(recorded.has_value());
  EXPECT_EQ(store->Get("a")->vertex_gid_floor, std::nullopt);
  EXPECT_EQ(store->Get("a")->edge_gid_floor, std::nullopt);

  std::filesystem::remove(tmp);
  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 200 + kGidReserveChunk), 200 + 2 * kGidReserveChunk);
  EXPECT_TRUE(store->RecordGidFloors(a->number, 10, 20).has_value());
  EXPECT_EQ(store->Get("a")->vertex_gid_floor, 10);
  EXPECT_EQ(store->Get("a")->edge_gid_floor, 20);
}

TEST_F(VersionStoreLedgerTest, FailedCreateRepublishesTheRetentionFloorAndLeavesNoPin) {
  auto store = MakeLedgerStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  auto tmp = file_;
  tmp += ".tmp";
  std::filesystem::create_directory(tmp);
  EXPECT_FALSE(store->CreateBranch("b").has_value());
  EXPECT_FALSE(store->Exists("b"));
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{a->fork_ts});
  ASSERT_EQ(floors.size(), 1U);
  EXPECT_EQ(floors.back(), a->fork_ts - 1);

  std::filesystem::remove(tmp);
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, a->number + 1) << "the failed attempt used no number";
}

// A store with nothing to drop is only frozen: no ledger write (a blocked ledger cannot fail it) and no ledger file.
TEST_F(VersionStoreLedgerTest, FreezeAndDropAllWithNothingToDropOnlyFreezes) {
  auto store = MakeLedgerStore();
  auto const dropped = store->FreezeAndDropAll();
  ASSERT_TRUE(dropped.has_value());
  EXPECT_TRUE(dropped->empty());
  EXPECT_TRUE(store->Frozen());
  EXPECT_FALSE(std::filesystem::exists(file_));
  EXPECT_FALSE(store->CreateBranch("b").has_value());

  store->Unfreeze();
  ASSERT_TRUE(store->CreateBranch("a").has_value());
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  auto tmp = file_;
  tmp += ".tmp";
  std::filesystem::create_directory(tmp);
  EXPECT_TRUE(store->FreezeAndDropAll().has_value()) << "idempotent without touching the ledger";
  EXPECT_TRUE(store->Frozen());
  EXPECT_FALSE(store->TryAcquireCheckout("a").has_value());
  std::filesystem::remove(tmp);
}

namespace {
memgraph::versioning::LedgerContents HiddenContents() {
  using memgraph::versioning::LedgerBranch;
  return {
      .next_number = 5,
      .branches = {
          LedgerBranch{.name = "h2", .info = {.number = 2, .parent = "main", .fork_ts = 40, .recorded_fork_ts = 40}},
          LedgerBranch{.name = "h4", .info = {.number = 4, .parent = "main", .fork_ts = 20, .recorded_fork_ts = 20}}}};
}
}  // namespace

TEST_F(VersionStoreLedgerTest, RestoreHiddenKeepsBranchesOutOfSightButCountsThem) {
  using memgraph::versioning::LedgerBranch;
  auto store = MakeLedgerStore();
  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  // The oldest fork point is neither the first nor the last of them.
  store->RestoreHidden(
      {.next_number = 7,
       .branches = {
           LedgerBranch{.name = "h2", .info = {.number = 2, .parent = "main", .fork_ts = 40, .recorded_fork_ts = 40}},
           LedgerBranch{.name = "h4", .info = {.number = 4, .parent = "main", .fork_ts = 20, .recorded_fork_ts = 20}},
           LedgerBranch{.name = "h6",
                        .info = {.number = 6, .parent = "main", .fork_ts = 60, .recorded_fork_ts = 60}}}});

  EXPECT_TRUE(store->List().empty());
  EXPECT_FALSE(store->Exists("h2"));
  EXPECT_FALSE(store->Get("h4").has_value());
  EXPECT_FALSE(store->TryAcquireCheckout("h2").has_value());
  EXPECT_FALSE(store->Empty());
  ASSERT_FALSE(floors.empty());
  EXPECT_EQ(floors.back(), 19u) << "the floor follows the oldest hidden fork point";
}

TEST_F(VersionStoreLedgerTest, HiddenBranchesStayInTheLedgerAcrossCreateAndOwnTheirNames) {
  auto store = MakeLedgerStore();
  store->RestoreHidden(HiddenContents());

  EXPECT_FALSE(store->CreateBranch("h2").has_value());
  auto created = store->CreateBranch("fresh");
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(created->number, 5u) << "numbering continues past the hidden branches";

  auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  std::vector<std::string> names;
  for (const auto &branch : (*read)->branches) names.push_back(branch.name);
  EXPECT_THAT(names, ::testing::UnorderedElementsAre("h2", "h4", "fresh"));
}

TEST_F(VersionStoreLedgerTest, FreezeAndDropAllDropsHiddenBranchesAndTheirDirectories) {
  auto store = MakeLedgerStore();
  const auto root = file_.parent_path();
  std::filesystem::create_directories(root / "2");
  std::filesystem::create_directories(root / "4");
  store->RestoreHidden(HiddenContents());
  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  auto const dropped = store->FreezeAndDropAll();
  ASSERT_TRUE(dropped.has_value());
  EXPECT_THAT(*dropped, testing::UnorderedElementsAre("h2", "h4"));

  auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  EXPECT_TRUE((*read)->branches.empty());
  EXPECT_EQ((*read)->next_number, 5u);
  EXPECT_FALSE(std::filesystem::exists(root / "2"));
  EXPECT_FALSE(std::filesystem::exists(root / "4"));
  EXPECT_TRUE(store->Empty());
  ASSERT_FALSE(floors.empty());
  EXPECT_FALSE(floors.back().has_value());
}

// Hidden branches are leftovers from while versioning is off; dropping them must still free directory and floor.
TEST_F(VersionStoreLedgerTest, HiddenBranchesCanBeDropped) {
  auto store = MakeLedgerStore();
  const auto root = file_.parent_path();
  std::filesystem::create_directories(root / "2");
  std::filesystem::create_directories(root / "4");
  store->RestoreHidden(HiddenContents());
  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  EXPECT_EQ(store->DropBranch("nope"), DropResult::kNotFound);
  EXPECT_EQ(store->DropBranch("h4"), DropResult::kDropped);
  EXPECT_FALSE(std::filesystem::exists(root / "4"));
  EXPECT_TRUE(std::filesystem::exists(root / "2"));
  ASSERT_FALSE(floors.empty());
  EXPECT_EQ(floors.back(), 39u) << "the floor follows the oldest branch left";
  {
    auto read = memgraph::versioning::BranchLedger{file_}.Read();
    ASSERT_TRUE(read.has_value() && read->has_value());
    ASSERT_EQ((*read)->branches.size(), 1U);
    EXPECT_EQ((*read)->branches[0].name, "h2");
    EXPECT_EQ((*read)->next_number, 5u) << "numbers are never reused";
  }
  EXPECT_FALSE(store->Empty());

  EXPECT_EQ(store->ForceDropBranch("h2"), DropResult::kDropped);
  EXPECT_TRUE(store->Empty());
  EXPECT_FALSE(floors.back().has_value());
  EXPECT_FALSE(std::filesystem::exists(root / "2"));
  EXPECT_TRUE(store->CreateBranch("h4").has_value()) << "a dropped hidden branch gives its name back";
}

namespace {
// Runs `probe` on another thread and reports whether it finished within a bound, without blocking the caller past it:
// the thread is joined later by Join(), so a probe stuck behind a lock the caller holds cannot deadlock the test.
class Probe {
 public:
  bool RunWithin(std::function<void()> probe, std::chrono::milliseconds bound = std::chrono::seconds{2}) {
    std::promise<void> done;
    auto finished = done.get_future();
    threads_.emplace_back([probe = std::move(probe), done = std::move(done)]() mutable {
      probe();
      done.set_value();
    });
    return finished.wait_for(bound) == std::future_status::ready;
  }

  void Join() {
    for (auto &thread : threads_) thread.join();
    threads_.clear();
  }

  ~Probe() { Join(); }

 private:
  std::vector<std::thread> threads_;
};

struct CoreToken {
  explicit CoreToken(std::function<void()> f) : on_destroy{std::move(f)} {}

  CoreToken(const CoreToken &) = delete;
  CoreToken &operator=(const CoreToken &) = delete;
  CoreToken(CoreToken &&) = delete;
  CoreToken &operator=(CoreToken &&) = delete;

  std::function<void()> on_destroy;

  ~CoreToken() {
    if (on_destroy) on_destroy();
  }
};

CoreResult MakeProbedCore(std::function<void()> on_destroy) {
  auto token = std::make_shared<CoreToken>(std::move(on_destroy));
  return std::shared_ptr<BranchCore>{token, reinterpret_cast<BranchCore *>(token.get())};
}
}  // namespace

// The release_pin callback must not run with the registry locked.
TEST_F(VersionStoreTest, ForkPinIsReleasedWithTheRegistryUnlocked) {
  Probe probe;
  std::vector<bool> readable;
  std::unique_ptr<VersionStore> store;
  store = std::make_unique<VersionStore>([this] { return ForkPoint{.fork_ts = pins_.Acquire(), .counts = {}}; },
                                         [&](uint64_t ts) {
                                           pins_.Release(ts);
                                           readable.push_back(probe.RunWithin([&] { (void)store->List(); }));
                                         });
  ASSERT_TRUE(store->CreateBranch("a").has_value());
  ASSERT_TRUE(store->CreateBranch("b").has_value());
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  probe.Join();
  EXPECT_EQ(readable, (std::vector<bool>{true, true}));
}

// Destroying a core tears down a storage engine; it must not block registry mutations.
TEST_F(VersionStoreTest, CoreIsDestroyedWithoutTheLedgerSerialized) {
  Probe probe;
  std::vector<bool> creatable;
  auto store = MakeStore();
  const auto build_probed = [&](const std::string &probe_name) {
    return [&, probe_name] {
      return MakeProbedCore(
          [&, probe_name] { creatable.push_back(probe.RunWithin([&] { (void)store->CreateBranch(probe_name); })); });
    };
  };
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value() && b.has_value());
  ASSERT_TRUE(store->GetOrBuildCore(a->number, build_probed("pa")).has_value());
  ASSERT_TRUE(store->GetOrBuildCore(b->number, build_probed("pb")).has_value());

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  probe.Join();
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  probe.Join();
  EXPECT_EQ(creatable, (std::vector<bool>{true, true}));
}

TEST_F(VersionStoreTest, CoresOfDifferentBranchesBuildConcurrently) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value() && b.has_value());

  std::promise<void> a_building;
  std::promise<void> b_building;
  auto b_started = b_building.get_future();
  bool b_built_during_a = false;
  std::thread first([&] {
    std::weak_ptr<int> observer;
    ASSERT_TRUE(store
                    ->GetOrBuildCore(a->number,
                                     [&] {
                                       a_building.set_value();
                                       b_built_during_a =
                                           b_started.wait_for(std::chrono::seconds{2}) == std::future_status::ready;
                                       return MakeSentinelCore(observer);
                                     })
                    .has_value());
  });
  a_building.get_future().wait();
  std::weak_ptr<int> observer;
  ASSERT_TRUE(store
                  ->GetOrBuildCore(b->number,
                                   [&] {
                                     b_building.set_value();
                                     return MakeSentinelCore(observer);
                                   })
                  .has_value());
  first.join();
  EXPECT_TRUE(b_built_during_a);
}

TEST_F(VersionStoreTest, RacingFirstCheckoutsOfOneBranchShareOneBuild) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());

  constexpr int kSessions = 4;
  std::atomic<int> builds{0};
  std::promise<void> release_build;
  const auto released = release_build.get_future().share();
  std::weak_ptr<int> observer;
  std::vector<CoreResult> results(kSessions);
  std::vector<std::thread> sessions;
  for (int i = 0; i < kSessions; ++i) {
    sessions.emplace_back([&, i] {
      results[i] = store->GetOrBuildCore(a->number, [&] {
        ++builds;
        released.wait();
        return MakeSentinelCore(observer);
      });
    });
  }
  while (builds.load() == 0) std::this_thread::yield();
  release_build.set_value();
  for (auto &session : sessions) session.join();

  EXPECT_EQ(builds.load(), 1);
  for (const auto &result : results) {
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->get(), results[0]->get());
  }
}

// `own_checkout` of an older branch of the same name does not excuse an attach to the re-created one.
TEST_F(VersionStoreTest, DropToleratesOnlyTheCallersOwnCheckouts) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("a").has_value());

  EXPECT_EQ(store->DropBranch("a"), DropResult::kInUse);
  ASSERT_TRUE(store->TryAcquireCheckout("a").has_value()) << "another session";
  EXPECT_EQ(store->DropBranch("a", a->number), DropResult::kInUse);
  store->ReleaseCheckout(a->number);
  EXPECT_EQ(store->DropBranch("a", a->number), DropResult::kDropped);
  EXPECT_EQ(store->DropBranch("a", a->number), DropResult::kNotFound);

  auto again = store->CreateBranch("a");
  ASSERT_TRUE(again.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("a").has_value());
  EXPECT_EQ(store->DropBranch("a", a->number), DropResult::kInUse) << "the caller is on the dropped 'a', not this one";
  EXPECT_EQ(store->DropBranch("a", again->number), DropResult::kDropped);
  store->ReleaseCheckout(a->number);
  store->ReleaseCheckout(again->number);
}

// A DROP resolves the name to a number first; a drop + re-create of the name in between must not make it drop the
// re-created branch.
TEST_F(VersionStoreTest, DropOfAStaleResolvedNumberLeavesARecreatedBranch) {
  auto store = MakeStore();
  auto old_branch = store->CreateBranch("a");
  ASSERT_TRUE(old_branch.has_value());
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);  // the concurrent session's DROP
  auto recreated = store->CreateBranch("a");                // ... and CREATE
  ASSERT_TRUE(recreated.has_value());
  ASSERT_NE(recreated->number, old_branch->number);

  EXPECT_EQ(store->DropBranch("a", std::nullopt, old_branch->number), DropResult::kNotFound);
  EXPECT_EQ(store->ForceDropBranch("a", old_branch->number), DropResult::kNotFound);
  EXPECT_TRUE(store->IsLive("a", recreated->number));

  EXPECT_EQ(store->DropBranch("a", std::nullopt, recreated->number), DropResult::kDropped);
  EXPECT_FALSE(store->Exists("a"));
}

TEST_F(VersionStoreTest, IsLiveMatchesNameAndNumberOfALiveBranchOnly) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_TRUE(store->IsLive("a", a->number));
  EXPECT_FALSE(store->IsLive("a", a->number + 1));
  EXPECT_FALSE(store->IsLive("b", a->number));

  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_FALSE(store->IsLive("a", a->number));
  auto again = store->CreateBranch("a");
  ASSERT_TRUE(again.has_value());
  EXPECT_FALSE(store->IsLive("a", a->number)) << "a re-created name is a different branch";
  EXPECT_TRUE(store->IsLive("a", again->number));

  auto hidden = MakeStore();
  hidden->RestoreHidden(
      {.next_number = 3,
       .branches = {{.name = "h", .info = {.number = 2, .parent = "main", .fork_ts = 5, .recorded_fork_ts = 5}}}});
  EXPECT_FALSE(hidden->IsLive("h", 2));
}

TEST_F(VersionStoreTest, RecordGidFloorsKeepsTheFirstFloorsOfALiveBranch) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value() && b.has_value());
  EXPECT_EQ(a->vertex_gid_floor, std::nullopt);

  ASSERT_TRUE(store->RecordGidFloors(a->number, 7, 9).has_value());
  ASSERT_TRUE(store->RecordGidFloors(a->number, 70, 90).has_value());
  EXPECT_EQ(store->Get("a")->vertex_gid_floor, 7);
  EXPECT_EQ(store->Get("a")->edge_gid_floor, 9);
  EXPECT_EQ(store->Get("b")->vertex_gid_floor, std::nullopt) << "only the given branch";

  EXPECT_TRUE(store->RecordGidFloors(1000, 1, 1).has_value()) << "an unknown number is a no-op";
  ASSERT_EQ(store->DropBranch("b"), DropResult::kDropped);
  EXPECT_TRUE(store->RecordGidFloors(b->number, 1, 1).has_value()) << "a dropped number is a no-op";
  EXPECT_EQ(store->Get("b"), std::nullopt);
}

TEST_F(VersionStoreTest, ReserveGidsRaisesTheCeilingByABlockAndKeepsWhatCoversIt) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(a.has_value() && b.has_value());
  ASSERT_TRUE(store->RecordGidFloors(a->number, 10, 20).has_value());

  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 11), 11 + kGidReserveChunk);
  EXPECT_EQ(store->Get("a")->vertex_gid_ceiling, 11 + kGidReserveChunk);
  EXPECT_EQ(store->Get("a")->edge_gid_ceiling, std::nullopt) << "vertices and edges apiece";
  EXPECT_EQ(store->Get("b")->vertex_gid_ceiling, std::nullopt) << "only the given branch";

  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 12), 11 + kGidReserveChunk) << "covered: nothing to raise";
  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 11 + kGidReserveChunk), 11 + kGidReserveChunk)
      << "the ceiling is exclusive";
  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 12 + kGidReserveChunk), 12 + 2 * kGidReserveChunk);
  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kEdge, 5), 20) << "the floor is held already";
  EXPECT_EQ(store->Get("a")->edge_gid_ceiling, std::nullopt) << "so nothing was written";
}

TEST_F(VersionStoreTest, ReserveGidsOfADroppedOrUnknownBranchHoldsNothing) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  ASSERT_EQ(store->DropBranch("a"), DropResult::kDropped);
  EXPECT_EQ(store->ReserveGids(a->number, GidKind::kVertex, 50), 50);
  EXPECT_EQ(store->ReserveGids(1000, GidKind::kEdge, 60), 60);
}

// Replica promotion racing a demotion: an Unfreeze while FreezeAndDropAll is releasing pins wins, and leaves a usable,
// empty store.
TEST_F(VersionStoreTest, UnfreezeDuringFreezeLeavesAnEmptyUsableStore) {
  Probe probe;
  bool unfrozen = false;
  std::unique_ptr<VersionStore> store;
  store = std::make_unique<VersionStore>([this] { return ForkPoint{.fork_ts = pins_.Acquire(), .counts = {}}; },
                                         [&](uint64_t ts) {
                                           pins_.Release(ts);
                                           unfrozen = probe.RunWithin([&] { store->Unfreeze(); });
                                         });
  ASSERT_TRUE(store->CreateBranch("a").has_value());
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  probe.Join();
  ASSERT_TRUE(unfrozen);
  EXPECT_FALSE(store->Frozen());
  EXPECT_TRUE(store->Empty());
  EXPECT_TRUE(store->CreateBranch("b").has_value());
}

// Readers never observe a half-applied CREATE or DROP: every listed branch is whole (its number, fork pin and name
// agree), lookups agree with the listing, and a dropped number never comes back.
TEST_F(VersionStoreLedgerTest, ConcurrentReadersSeeOnlyWholeRegistryStates) {
  std::atomic<uint64_t> next_pin{100};
  std::atomic<uint64_t> released{0};
  VersionStore store{[&] { return ForkPoint{.fork_ts = next_pin++, .counts = {}}; }, [&](uint64_t) { ++released; }};
  store.AttachLedger(file_);
  // CREATEs are serialized, so branch n forks at pin 98 + n.
  const auto name_of = [](uint64_t number) { return fmt::format("b{}", number); };

  constexpr uint64_t kBranches = 60;
  std::atomic<bool> done{false};
  std::atomic<int> violations{0};
  std::vector<std::thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&] {
      uint64_t highest_dropped = 0;
      while (!done.load()) {
        const auto listed = store.List();
        uint64_t previous = 0;
        for (const auto &[name, info] : listed) {
          if (name != name_of(info.number) || info.fork_ts != 98 + info.number || info.number <= previous ||
              info.number <= highest_dropped) {
            ++violations;
          }
          previous = info.number;
          const auto fetched = store.Get(name);
          if (fetched && fetched->number != info.number) ++violations;
          if (!fetched && store.Exists(name)) ++violations;
        }
        // Drops go in creation order, so everything below the lowest listed number is gone for good.
        if (!listed.empty()) {
          highest_dropped = std::max(highest_dropped, listed.front().second.number - 1);
          if (store.IsLive(name_of(highest_dropped), highest_dropped) && highest_dropped >= 2) ++violations;
        }
      }
    });
  }
  for (uint64_t n = 2; n < 2 + kBranches; ++n) {
    ASSERT_TRUE(store.CreateBranch(name_of(n)).has_value());
    if (n % 3 == 0) {
      for (const auto &[name, info] : store.List()) {
        if (info.number + 2 <= n) ASSERT_EQ(store.DropBranch(name), DropResult::kDropped);
      }
    }
  }
  done = true;
  for (auto &reader : readers) reader.join();
  EXPECT_EQ(violations.load(), 0);

  auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  const auto listed = store.List();
  ASSERT_EQ((*read)->branches.size(), listed.size());
  for (size_t i = 0; i < listed.size(); ++i) EXPECT_EQ((*read)->branches[i].name, listed[i].first);
  EXPECT_EQ(released.load() + listed.size(), kBranches);
}

// Attach/build/hold racing drops and re-creates: each number is built at most once, a held core stays alive, and once
// all let go no dropped core, directory or attach is left.
TEST_F(VersionStoreLedgerTest, CheckoutsAndBuildsRaceForceDrops) {
  std::atomic<uint64_t> next_pin{100};
  VersionStore store{[&] { return ForkPoint{.fork_ts = next_pin++, .counts = {}}; }, [](uint64_t) {}};
  store.AttachLedger(file_);
  auto &sessions_gauge = *memgraph::metrics::Metrics().global.versioning_active_sessions;
  const auto sessions0 = sessions_gauge.Value();
  const auto root = file_.parent_path();

  std::mutex built_mutex;
  std::map<uint64_t, int> builds;
  std::vector<std::weak_ptr<int>> cores;
  const auto build_for = [&](uint64_t number) {
    return [&, number]() -> CoreResult {
      std::filesystem::create_directories(root / std::to_string(number));
      auto token = std::make_shared<int>(static_cast<int>(number));
      std::lock_guard guard{built_mutex};
      ++builds[number];
      cores.emplace_back(token);
      return std::shared_ptr<BranchCore>{token, reinterpret_cast<BranchCore *>(token.get())};
    };
  };

  ASSERT_TRUE(store.CreateBranch("b").has_value());
  std::atomic<bool> done{false};
  std::atomic<int> violations{0};
  std::vector<std::thread> sessions;
  for (int s = 0; s < 4; ++s) {
    sessions.emplace_back([&] {
      while (!done.load()) {
        auto info = store.TryAcquireCheckout("b");
        if (!info) {
          std::this_thread::yield();
          continue;
        }
        if (auto core = store.GetOrBuildCore(info->number, build_for(info->number))) {
          if (*reinterpret_cast<int *>(core->get()) != static_cast<int>(info->number)) ++violations;
          std::this_thread::yield();
          if (*reinterpret_cast<int *>(core->get()) != static_cast<int>(info->number)) ++violations;
        }
        store.ReleaseCheckout(info->number);
      }
    });
  }
  sessions.emplace_back([&] {
    while (!done.load()) {
      if (!store.DropBranch("b").has_value()) ++violations;
      std::this_thread::sleep_for(std::chrono::microseconds{300});
    }
  });
  for (int round = 0; round < 50; ++round) {
    std::this_thread::sleep_for(std::chrono::microseconds{200});
    const auto dropped = store.ForceDropBranch("b");
    ASSERT_TRUE(dropped == DropResult::kDropped || dropped == DropResult::kNotFound);
    ASSERT_TRUE(store.CreateBranch("b").has_value());
  }
  done = true;
  for (auto &session : sessions) session.join();
  EXPECT_EQ(violations.load(), 0);

  ASSERT_TRUE(store.ForceDropBranch("b").has_value());
  for (const auto &[number, count] : builds) EXPECT_EQ(count, 1) << "branch " << number << " built twice";
  for (const auto &core : cores) EXPECT_TRUE(core.expired());
  EXPECT_EQ(sessions_gauge.Value(), sessions0);
  for (const auto &entry : std::filesystem::directory_iterator(root)) {
    EXPECT_FALSE(entry.is_directory()) << "left behind: " << entry.path();
  }
}

namespace {
struct StoreWithAnUnavailableBranch {
  std::unique_ptr<VersionStore> store;
  uint64_t ok{0};
  uint64_t bad{0};
};
}  // namespace

class VersionStoreUnavailableTest : public VersionStoreLedgerTest {
 protected:
  StoreWithAnUnavailableBranch Make() {
    auto store = MakeLedgerStore();
    auto ok = store->CreateBranch("ok");
    auto bad = store->CreateBranch("bad");
    EXPECT_TRUE(ok.has_value() && bad.has_value());
    auto contents = memgraph::versioning::BranchLedger{file_}.Read();
    EXPECT_TRUE(contents.has_value() && contents->has_value());
    auto restarted = MakeLedgerStore();
    restarted->Restore(**contents);
    restarted->MarkUnavailable(bad->number, "the write-ahead log directory is missing");
    return {.store = std::move(restarted), .ok = ok->number, .bad = bad->number};
  }
};

// The branch keeps its pin and number, so a repair plus a restart brings it back.
TEST_F(VersionStoreUnavailableTest, AnUnavailableBranchStaysRegisteredAndReportsItsReason) {
  const auto [store, ok, bad] = Make();
  ASSERT_EQ(store->List().size(), 2U);
  EXPECT_EQ(store->Get("bad")->unavailable_reason, "the write-ahead log directory is missing");
  EXPECT_FALSE(store->Get("ok")->unavailable_reason.has_value());
  EXPECT_FALSE(store->Empty());
  EXPECT_TRUE(store->IsLive("bad", bad));
}

// Using it fails with a message that names the branch, the reason, where its files are and the two ways out; a
// healthy branch is not affected, and a failed attach leaves no count behind.
TEST_F(VersionStoreUnavailableTest, CheckingOutAnUnavailableBranchSaysWhyAndWhatToDo) {
  const auto [store, ok, bad] = Make();
  const auto refused = store->TryAcquireCheckout("bad");
  ASSERT_FALSE(refused.has_value());
  EXPECT_THAT(refused.error(), testing::HasSubstr("'bad'"));
  EXPECT_THAT(refused.error(), testing::HasSubstr("unavailable"));
  EXPECT_THAT(refused.error(), testing::HasSubstr("the write-ahead log directory is missing"));
  EXPECT_THAT(refused.error(), testing::HasSubstr((file_.parent_path() / std::to_string(bad)).string()));
  EXPECT_THAT(refused.error(), testing::HasSubstr("restart"));
  EXPECT_THAT(refused.error(), testing::HasSubstr("DROP BRANCH 'bad'"));
  EXPECT_EQ(store->CheckoutCount("bad"), 0U);

  EXPECT_TRUE(store->TryAcquireCheckout("ok").has_value());

  store->MarkUnavailable(ok, "WAL damaged...");
  const auto dotted = store->TryAcquireCheckout("ok");
  ASSERT_FALSE(dotted.has_value());
  EXPECT_THAT(dotted.error(), testing::HasSubstr("unavailable: WAL damaged. Its files"));
  EXPECT_THAT(dotted.error(), testing::Not(testing::HasSubstr("..")));
}

// Creating a branch under its name is refused, creating another one keeps its ledger entry as it was, and DROP
// removes the entry, the pin and the directory without FORCE.
TEST_F(VersionStoreUnavailableTest, AnUnavailableBranchBlocksItsNameKeepsItsLedgerEntryAndDropsWithoutForce) {
  const auto [store, ok, bad] = Make();
  const auto bad_directory = file_.parent_path() / std::to_string(bad);
  std::filesystem::create_directories(bad_directory / "wal");
  const auto live_before = pins_.live_.size();

  EXPECT_FALSE(store->CreateBranch("bad").has_value());
  ASSERT_TRUE(store->CreateBranch("fresh").has_value());
  auto ledger = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(ledger.has_value() && ledger->has_value());
  const auto entry = std::ranges::find((*ledger)->branches, "bad", &memgraph::versioning::LedgerBranch::name);
  ASSERT_NE(entry, (*ledger)->branches.end()) << "the ledger must keep the unavailable branch";
  EXPECT_FALSE(entry->info.unavailable_reason.has_value()) << "unavailability is runtime state, never persisted";

  EXPECT_EQ(store->DropBranch("bad"), DropResult::kDropped);
  EXPECT_FALSE(store->Exists("bad"));
  EXPECT_EQ(pins_.live_.size(), live_before) << "the drop released the fork pin (the new branch took one)";
  EXPECT_FALSE(std::filesystem::exists(bad_directory));
  ASSERT_TRUE(store->CreateBranch("bad").has_value()) << "the name is free again";
}

// A ledger that could not be read leaves the registry unknown: nothing may be created, dropped or used, which also
// keeps the next write from replacing the file; the message is the one the caller was given.
TEST_F(VersionStoreUnavailableTest, AnUnreadableLedgerRefusesEveryBranchOperationWithItsMessage) {
  auto store = MakeLedgerStore();
  store->RestoreLedgerUnreadable("the ledger is damaged, restore it");
  EXPECT_EQ(store->LedgerUnreadable(), "the ledger is damaged, restore it");
  EXPECT_FALSE(store->Empty()) << "storage-global operations must not run over branches nobody can list";

  const auto expect_refused = [](const auto &result) {
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), "the ledger is damaged, restore it");
  };
  expect_refused(store->CreateBranch("a"));
  expect_refused(store->DropBranch("a"));
  expect_refused(store->TryAcquireCheckout("a"));
  EXPECT_FALSE(std::filesystem::exists(file_)) << "the unreadable ledger is never replaced";
}
