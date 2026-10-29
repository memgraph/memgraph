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

#include <gtest/gtest.h>
#include <algorithm>
#include <expected>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "metrics/prometheus_metrics.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

using memgraph::versioning::BranchInfo;
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
    return std::make_unique<VersionStore>([this] { return pins_.Acquire(); },
                                          [this](uint64_t ts) { pins_.Release(ts); });
  }

  FakePinRegistry pins_;
};

TEST_F(VersionStoreTest, CreateGetExistsList) {
  auto store = MakeStore();

  EXPECT_FALSE(store->Exists("feature"));
  EXPECT_EQ(store->Get("feature"), std::nullopt);
  EXPECT_TRUE(store->List().empty());

  auto created = store->CreateBranch("feature", "main");
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(created->number, 2);
  EXPECT_EQ(created->parent, "main");

  EXPECT_TRUE(store->Exists("feature"));
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
  ASSERT_TRUE(store->CreateBranch("b", "main").has_value());
  ASSERT_TRUE(store->CreateBranch("a", "main").has_value());

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 2);
  EXPECT_EQ(listed[0].first, "b");
  EXPECT_EQ(listed[1].first, "a");
  EXPECT_LT(listed[0].second.number, listed[1].second.number);
}

TEST_F(VersionStoreTest, BranchOfBranchRecordsParent) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a", "main").has_value());
  auto b = store->CreateBranch("b", "a");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->parent, "a");
  EXPECT_EQ(store->Get("b")->parent, "a");
}

TEST_F(VersionStoreTest, MonotonicNumberingNeverReusedAcrossDrop) {
  auto store = MakeStore();

  auto a = store->CreateBranch("a", "main");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->number, 2);

  ASSERT_TRUE(store->DropBranch("a").value_or(false));

  auto b = store->CreateBranch("b", "main");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3);
}

TEST_F(VersionStoreTest, DuplicateNameRejected) {
  auto store = MakeStore();

  ASSERT_TRUE(store->CreateBranch("feature", "main").has_value());
  auto duplicate = store->CreateBranch("feature", "main");
  EXPECT_FALSE(duplicate.has_value());

  EXPECT_EQ(pins_.live_.size(), 1);
}

TEST_F(VersionStoreTest, UnknownParentRejected) {
  auto store = MakeStore();

  auto result = store->CreateBranch("feature", "does-not-exist");
  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(pins_.live_.empty());
}

TEST_F(VersionStoreTest, DropUnknownBranchReturnsFalse) {
  auto store = MakeStore();
  auto live = store->CreateBranch("live", "main");
  ASSERT_TRUE(live.has_value());

  EXPECT_FALSE(store->DropBranch("nope").value_or(false));
  EXPECT_TRUE(store->Exists("live"));
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{live->fork_ts});
  EXPECT_TRUE(pins_.released_.empty());
}

TEST_F(VersionStoreTest, PinAcquiredOnCreateAndReleasedOnDrop) {
  auto store = MakeStore();

  auto created = store->CreateBranch("feature", "main");
  ASSERT_TRUE(created.has_value());

  ASSERT_EQ(pins_.live_.size(), 1);
  EXPECT_EQ(pins_.live_.front(), created->fork_ts);
  EXPECT_TRUE(pins_.released_.empty());

  ASSERT_TRUE(store->DropBranch("feature").value_or(false));
  EXPECT_TRUE(pins_.live_.empty());
  ASSERT_EQ(pins_.released_.size(), 1);
  EXPECT_EQ(pins_.released_.front(), created->fork_ts);
}

TEST_F(VersionStoreTest, CheckoutIsCountedAndBlocksDropUntilEverySessionDetached) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main");
  ASSERT_TRUE(created.has_value());

  auto first = store->TryAcquireCheckout("feature");
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->number, created->number);
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value()) << "a second session may attach to the same branch";
  EXPECT_EQ(store->CheckoutCount("feature"), 2U);

  EXPECT_FALSE(store->DropBranch("feature").value_or(false));

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 1U);
  EXPECT_FALSE(store->DropBranch("feature").value_or(false)) << "one session is still attached";

  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
  EXPECT_TRUE(store->DropBranch("feature").value_or(false));
  EXPECT_TRUE(pins_.live_.empty()) << "the fork pin is released exactly once, by the drop";
}

TEST_F(VersionStoreTest, CheckoutOfUnknownBranchIsRefused) {
  auto store = MakeStore();
  EXPECT_FALSE(store->TryAcquireCheckout("nope").has_value());
  EXPECT_EQ(store->CheckoutCount("nope"), 0U);
}

TEST_F(VersionStoreTest, ReleaseCheckoutWithoutAttachIsANoOp) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main");
  ASSERT_TRUE(created.has_value());
  store->ReleaseCheckout(created->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
}

TEST_F(VersionStoreTest, ForceDropDropsAnAttachedBranchAndItsStaleAttachIsHarmless) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto sessions0 = global.versioning_active_sessions->Value();

  auto store = MakeStore();
  auto old_branch = store->CreateBranch("feature", "main");
  ASSERT_TRUE(old_branch.has_value());
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value());

  EXPECT_FALSE(store->DropBranch("feature").value_or(false));
  ASSERT_TRUE(store->ForceDropBranch("feature").value_or(false));
  EXPECT_FALSE(store->Exists("feature"));
  EXPECT_TRUE(pins_.live_.empty());
  EXPECT_EQ(global.versioning_active_sessions->Value(), sessions0 + 1) << "the attach outlives the drop";

  auto new_branch = store->CreateBranch("feature", "main");
  ASSERT_TRUE(new_branch.has_value());
  EXPECT_NE(new_branch->number, old_branch->number);
  EXPECT_EQ(store->CheckoutCount("feature"), 0U) << "the stale attach belongs to the dropped branch";
  EXPECT_TRUE(store->DropBranch("feature").value_or(false));

  store->ReleaseCheckout(old_branch->number);
  EXPECT_EQ(global.versioning_active_sessions->Value(), sessions0);
}

TEST_F(VersionStoreTest, ForceDropOfUnknownBranchReportsNotDropped) {
  auto store = MakeStore();
  EXPECT_FALSE(store->ForceDropBranch("nope").value_or(true));
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
  auto created = store->CreateBranch("feature", "main");
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
  auto created = store->CreateBranch("feature", "main");
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
  auto a = store->CreateBranch("a", "main");
  auto b = store->CreateBranch("b", "main");
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

  ASSERT_TRUE(store->DropBranch("b").value_or(false));
  EXPECT_TRUE(b_core.expired()) << "dropping a branch must release its cached core";
  EXPECT_FALSE(a_core.expired()) << "dropping b must not retire a's core";

  ASSERT_TRUE(store->DropBranch("a").value_or(false));
  EXPECT_TRUE(a_core.expired());

  auto a2 = store->CreateBranch("a", "main");
  ASSERT_TRUE(a2.has_value());
  ASSERT_TRUE(store->GetOrBuildCore(a2->number, build_a).has_value());
  EXPECT_EQ(a_builds, 2);
}

TEST_F(VersionStoreTest, GetOrBuildCoreRefusesADroppedBranchNumber) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main");
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

TEST_F(VersionStoreTest, ActiveGaugesSumAcrossStores) {
  auto &global = memgraph::metrics::Metrics().global;
  const auto branches0 = global.versioning_active_branches->Value();
  const auto checkouts0 = global.versioning_active_sessions->Value();

  auto a = MakeStore();
  auto b = MakeStore();
  ASSERT_TRUE(a->CreateBranch("x", "main"));
  ASSERT_TRUE(b->CreateBranch("y", "main"));
  EXPECT_EQ(global.versioning_active_branches->Value(), branches0 + 2);

  auto x_attach = a->TryAcquireCheckout("x");
  ASSERT_TRUE(x_attach);
  ASSERT_TRUE(b->TryAcquireCheckout("y"));
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 2);

  a->ReleaseCheckout(x_attach->number);
  EXPECT_EQ(global.versioning_active_sessions->Value(), checkouts0 + 1);
  ASSERT_TRUE(a->DropBranch("x", /*remove_files=*/false).value());
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
  auto a = store->CreateBranch("a", "main");
  auto b = store->CreateBranch("b", "a");
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
  auto c = store->CreateBranch("c", "main");
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->number, 4) << "numbers are never reused";
}

TEST_F(VersionStoreTest, FrozenStoreRefusesCreateAndCheckoutUntilUnfrozen) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a", "main").has_value());
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());

  constexpr auto kMessage = "Graph Versioning branches are not available on a REPLICA.";
  auto created = store->CreateBranch("b", "main");
  ASSERT_FALSE(created.has_value());
  EXPECT_EQ(created.error(), kMessage);
  auto checkout = store->TryAcquireCheckout("a");
  ASSERT_FALSE(checkout.has_value());
  EXPECT_EQ(checkout.error(), kMessage);
  EXPECT_TRUE(pins_.live_.empty());

  store->Unfreeze();
  EXPECT_FALSE(store->Frozen());
  ASSERT_TRUE(store->CreateBranch("b", "main").has_value());
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
  auto a = store->CreateBranch("a", "main");
  auto b = store->CreateBranch("b", "main");
  ASSERT_TRUE(a.has_value());
  ASSERT_TRUE(b.has_value());
  const auto root = file_.parent_path();
  std::filesystem::create_directories(root / std::to_string(a->number));
  std::filesystem::create_directories(root / std::to_string(b->number));

  ASSERT_TRUE(store->FreezeAndDropAll().has_value());

  auto read = memgraph::versioning::BranchLedger{file_}.Read();
  ASSERT_TRUE(read.has_value() && read->has_value());
  EXPECT_TRUE((*read)->branches.empty());
  EXPECT_EQ((*read)->next_number, 4);
  EXPECT_FALSE(std::filesystem::exists(root / std::to_string(a->number)));
  EXPECT_FALSE(std::filesystem::exists(root / std::to_string(b->number)));
}

TEST_F(VersionStoreLedgerTest, DirectoryOfDroppedBranchSurvivesUntilLastNestedHolderReleases) {
  auto store = MakeLedgerStore();
  auto b = store->CreateBranch("b", "main");
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
  auto b = store->CreateBranch("b", "main");
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
  auto a = store->CreateBranch("a", "main");
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

TEST_F(VersionStoreLedgerTest, FreezeAndDropAllLedgerFailureOnFrozenStoreKeepsItFrozen) {
  auto store = MakeLedgerStore();
  ASSERT_TRUE(store->CreateBranch("a", "main").has_value());
  ASSERT_TRUE(store->FreezeAndDropAll().has_value());
  ASSERT_TRUE(store->Frozen());

  auto tmp = file_;
  tmp += ".tmp";
  std::filesystem::create_directory(tmp);

  EXPECT_FALSE(store->FreezeAndDropAll().has_value());
  EXPECT_TRUE(store->Frozen());
  EXPECT_FALSE(store->CreateBranch("b", "main").has_value());
  EXPECT_FALSE(store->TryAcquireCheckout("a").has_value());

  std::filesystem::remove(tmp);
}
