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

  EXPECT_TRUE(store->Exists("feature"));
  EXPECT_FALSE(store->Empty());
  auto fetched = store->Get("feature");
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->number, created->number);
  EXPECT_EQ(fetched->fork_ts, created->fork_ts);

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 1);
  EXPECT_EQ(listed[0].first, "feature");
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
  EXPECT_TRUE(store->Exists("live"));
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{live->fork_ts});
  EXPECT_TRUE(pins_.released_.empty());
}

TEST_F(VersionStoreTest, PinAcquiredOnCreateAndReleasedOnDrop) {
  auto store = MakeStore();

  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());

  ASSERT_EQ(pins_.live_.size(), 1);
  EXPECT_EQ(pins_.live_.front(), created->fork_ts);
  EXPECT_TRUE(pins_.released_.empty());

  ASSERT_EQ(store->DropBranch("feature"), DropResult::kDropped);
  EXPECT_TRUE(pins_.live_.empty());
  ASSERT_EQ(pins_.released_.size(), 1);
  EXPECT_EQ(pins_.released_.front(), created->fork_ts);
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

// The registry wired to the storage the way Database wires it: pins clamp GC and the floor keeps the fork recoverable.
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
  EXPECT_EQ(store->DropBranch("feature", created->number), DropResult::kInUse) << "one attach beyond the caller's own";

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 1U);
  EXPECT_EQ(store->DropBranch("feature"), DropResult::kInUse) << "one session is still attached";

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
  EXPECT_EQ(store->DropBranch("feature"), DropResult::kDropped);
  EXPECT_TRUE(pins_.live_.empty()) << "the fork pin is released exactly once, by the drop";
}

TEST_F(VersionStoreTest, CheckoutOfUnknownBranchIsRefused) {
  auto store = MakeStore();
  EXPECT_FALSE(store->TryAcquireCheckout("nope").has_value());
  EXPECT_EQ(store->CheckoutCount("nope"), 0U);
}

TEST_F(VersionStoreTest, ReleaseCheckoutWithoutAttachIsANoOp) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
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

TEST_F(VersionStoreTest, ForceDropOfUnknownBranchReportsNotDropped) {
  auto store = MakeStore();
  EXPECT_EQ(store->ForceDropBranch("nope"), DropResult::kNotFound);
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

TEST_F(VersionStoreTest, FailedCoreBuildIsNotCached) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());

  auto failing = []() -> CoreResult { return std::unexpected(std::string{"boom"}); };
  auto failed = store->GetOrBuildCore(created->number, failing);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), "boom");

  int builds = 0;
  std::weak_ptr<int> observer;
  auto build = [&]() -> CoreResult {
    ++builds;
    return MakeSentinelCore(observer);
  };
  auto retry = store->GetOrBuildCore(created->number, build);
  ASSERT_TRUE(retry.has_value());
  EXPECT_NE(retry->get(), nullptr);
  EXPECT_EQ(builds, 1) << "the failed build must not have been cached";
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

TEST_F(VersionStoreTest, GetOrBuildCoreRefusesADroppedBranchNumber) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  EXPECT_TRUE(store->IsLive("feature", created->number));
  EXPECT_FALSE(store->IsLive("feature", created->number + 1));

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_FALSE(store->IsLive("feature", created->number));

  int builds = 0;
  auto build = [&]() -> CoreResult {
    ++builds;
    return std::unexpected(std::string("must not build"));
  };
  auto refused = store->GetOrBuildCore(created->number, build);
  EXPECT_FALSE(refused.has_value());
  EXPECT_EQ(builds, 0) << "a dropped branch must not be built";
}

// No ledger attached: FreezeAndDropAll still retires the cached cores, so no dropped number keeps (or serves) one.
TEST_F(VersionStoreTest, FreezeAndDropAllRetiresCoresWithoutALedger) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature");
  ASSERT_TRUE(created.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value());
  std::weak_ptr<int> observer;
  ASSERT_TRUE(store->GetOrBuildCore(created->number, [&] { return MakeSentinelCore(observer); }).has_value());

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(observer.expired()) << "the dropped branch's core must be released";
  auto refused = store->GetOrBuildCore(created->number, [&] { return MakeSentinelCore(observer); });
  EXPECT_FALSE(refused.has_value()) << "a dropped number must not be served a cached core";
  store->ReleaseCheckout(created->number);
}

TEST_F(VersionStoreTest, ActiveGaugesSumAcrossStores) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto branches0 = global.versioning_active_branches->Value();
  const auto checkouts0 = global.versioning_active_sessions->Value();

  auto a = MakeStore();
  auto b = MakeStore();
  ASSERT_TRUE(a->CreateBranch("x"));
  ASSERT_TRUE(b->CreateBranch("y"));
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 2);

  auto x_attach = a->TryAcquireCheckout("x");
  ASSERT_TRUE(x_attach);
  ASSERT_TRUE(b->TryAcquireCheckout("y"));
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 2);

  a->ReleaseCheckout(x_attach->number);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 1);
  ASSERT_EQ(a->DropBranch("x"), DropResult::kDropped);
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 1);

  // Destroying a store that still holds a branch and a checkout withdraws exactly its share.
  b.reset();
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0);
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
  auto b = store->CreateBranch("b");
  auto c = store->CreateBranch("c");
  ASSERT_TRUE(b.has_value());
  ASSERT_TRUE(c.has_value());
  const auto directory = file_.parent_path() / std::to_string(b->number);
  const auto kept = file_.parent_path() / std::to_string(c->number);
  std::filesystem::create_directories(directory);
  std::filesystem::create_directories(kept);

  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  ASSERT_EQ(store->ForceDropBranch("b"), DropResult::kDropped);
  EXPECT_TRUE(std::filesystem::exists(directory));

  store->ReleaseCheckout(b->number);
  EXPECT_TRUE(std::filesystem::exists(directory));

  store->ReleaseCheckout(b->number);
  EXPECT_FALSE(std::filesystem::exists(directory));

  EXPECT_TRUE(std::filesystem::exists(kept)) << "only the dropped branch's directory goes";
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

TEST_F(VersionStoreLedgerTest, DirectoryOfDroppedBranchIsKeptWhileACheckoutIsHeldEvenIfCoreIsGone) {
  auto store = MakeLedgerStore();
  auto b = store->CreateBranch("b");
  ASSERT_TRUE(b.has_value());
  const auto directory = file_.parent_path() / std::to_string(b->number);
  std::filesystem::create_directories(directory);

  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("b").has_value());
  std::weak_ptr<int> observer;
  auto core = store->GetOrBuildCore(b->number, [&] { return MakeSentinelCore(observer); });
  ASSERT_TRUE(core.has_value());

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());

  core->reset();
  EXPECT_TRUE(observer.expired());
  store->ReleaseCheckout(b->number);
  EXPECT_TRUE(std::filesystem::exists(directory));

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
  auto store = MakeLedgerStore();
  std::vector<std::optional<uint64_t>> floors;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> floor) { floors.push_back(floor); });

  store->RestoreHidden(HiddenContents());

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

// A core whose destruction runs `on_destroy`.
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

// The fork-pin release takes the storage's pin lock; it must not run with the registry locked.
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

// Building one branch's core must not hold up the first checkout of another.
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

TEST_F(VersionStoreTest, ThrowingBuildReachesEveryWaiterAndIsNotCached) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_THROW((void)store->GetOrBuildCore(a->number, []() -> CoreResult { throw std::runtime_error{"boom"}; }),
               std::runtime_error);
  std::weak_ptr<int> observer;
  EXPECT_TRUE(store->GetOrBuildCore(a->number, [&] { return MakeSentinelCore(observer); }).has_value());
}

// The caller's own attach does not make a branch in use, so a session can drop the branch it is on without a racy count
// check first; an attach to an older branch of the same name does not count.
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

// Sessions attaching, building and holding cores while a branch is dropped (forced and not) and re-created: each number
// is built at most once, a session's core stays alive while it holds it, and once everyone let go no dropped core,
// directory or attach is left.
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
