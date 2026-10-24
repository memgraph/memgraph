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
#include <memory>
#include <string>
#include <vector>

#include "versioning/version_store.hpp"

using memgraph::versioning::BranchInfo;
using memgraph::versioning::VersionStore;

// Fake GC fork-pin: records every acquire/release so tests can assert the VersionStore drives the
// pin lifecycle correctly, without any real storage::InMemoryStorage.
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

  auto created = store->CreateBranch("feature", "main", "my feature branch");
  ASSERT_TRUE(created.has_value());
  EXPECT_EQ(created->number, 2);
  EXPECT_EQ(created->parent, "main");
  EXPECT_EQ(created->description, "my feature branch");

  EXPECT_TRUE(store->Exists("feature"));
  auto fetched = store->Get("feature");
  ASSERT_TRUE(fetched.has_value());
  EXPECT_EQ(fetched->number, created->number);
  EXPECT_EQ(fetched->fork_ts, created->fork_ts);

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 1);
  EXPECT_EQ(listed[0].first, "feature");
}

// List() is ordered by branch number (creation order), not by name.
TEST_F(VersionStoreTest, ListIsOrderedByCreationNotName) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("b", "main", std::nullopt).has_value());
  ASSERT_TRUE(store->CreateBranch("a", "main", std::nullopt).has_value());

  auto listed = store->List();
  ASSERT_EQ(listed.size(), 2);
  EXPECT_EQ(listed[0].first, "b");
  EXPECT_EQ(listed[1].first, "a");
  EXPECT_LT(listed[0].second.number, listed[1].second.number);
}

TEST_F(VersionStoreTest, BranchOfBranchRecordsParent) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("a", "main", std::nullopt).has_value());
  auto b = store->CreateBranch("b", "a", std::nullopt);
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->parent, "a");
  EXPECT_EQ(store->Get("b")->parent, "a");
}

TEST_F(VersionStoreTest, MonotonicNumberingNeverReusedAcrossDrop) {
  auto store = MakeStore();

  auto a = store->CreateBranch("a", "main", std::nullopt);
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->number, 2);

  ASSERT_TRUE(store->DropBranch("a"));

  auto b = store->CreateBranch("b", "main", std::nullopt);
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3);  // never reuses 2, even though 'a' was dropped
}

TEST_F(VersionStoreTest, DuplicateNameRejected) {
  auto store = MakeStore();

  ASSERT_TRUE(store->CreateBranch("feature", "main", std::nullopt).has_value());
  auto duplicate = store->CreateBranch("feature", "main", std::nullopt);
  EXPECT_FALSE(duplicate.has_value());

  // Rejected before any pin was taken for the duplicate attempt.
  EXPECT_EQ(pins_.live_.size(), 1);
}

TEST_F(VersionStoreTest, UnknownParentRejected) {
  auto store = MakeStore();

  auto result = store->CreateBranch("feature", "does-not-exist", std::nullopt);
  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(pins_.live_.empty());  // no pin leaked for the rejected create
}

TEST_F(VersionStoreTest, DropUnknownBranchReturnsFalse) {
  auto store = MakeStore();
  auto live = store->CreateBranch("live", "main", std::nullopt);
  ASSERT_TRUE(live.has_value());

  EXPECT_FALSE(store->DropBranch("nope"));
  EXPECT_TRUE(store->Exists("live"));
  EXPECT_EQ(pins_.live_, std::vector<uint64_t>{live->fork_ts});
  EXPECT_TRUE(pins_.released_.empty());
}

TEST_F(VersionStoreTest, PinAcquiredOnCreateAndReleasedOnDrop) {
  auto store = MakeStore();

  auto created = store->CreateBranch("feature", "main", std::nullopt);
  ASSERT_TRUE(created.has_value());

  // The fork_ts returned to the caller is exactly what the pin callback produced.
  ASSERT_EQ(pins_.live_.size(), 1);
  EXPECT_EQ(pins_.live_.front(), created->fork_ts);
  EXPECT_TRUE(pins_.released_.empty());

  ASSERT_TRUE(store->DropBranch("feature"));
  EXPECT_TRUE(pins_.live_.empty());
  ASSERT_EQ(pins_.released_.size(), 1);
  EXPECT_EQ(pins_.released_.front(), created->fork_ts);
}

// Any number of sessions may attach to one branch; the branch cannot be dropped until every one detached.
TEST_F(VersionStoreTest, CheckoutIsCountedAndBlocksDropUntilEverySessionDetached) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main", std::nullopt);
  ASSERT_TRUE(created.has_value());

  auto first = store->TryAcquireCheckout("feature");
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->number, created->number);
  ASSERT_TRUE(store->TryAcquireCheckout("feature").has_value()) << "a second session may attach to the same branch";
  EXPECT_EQ(store->CheckoutCount("feature"), 2U);

  EXPECT_FALSE(store->DropBranch("feature"));

  store->ReleaseCheckout("feature");
  EXPECT_EQ(store->CheckoutCount("feature"), 1U);
  EXPECT_FALSE(store->DropBranch("feature")) << "one session is still attached";

  store->ReleaseCheckout("feature");
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
  EXPECT_TRUE(store->DropBranch("feature"));
  EXPECT_TRUE(pins_.live_.empty()) << "the fork pin is released exactly once, by the drop";
}

TEST_F(VersionStoreTest, CheckoutOfUnknownBranchIsRefused) {
  auto store = MakeStore();
  EXPECT_FALSE(store->TryAcquireCheckout("nope").has_value());
  EXPECT_EQ(store->CheckoutCount("nope"), 0U);
}

TEST_F(VersionStoreTest, ReleaseCheckoutWithoutAttachIsANoOp) {
  auto store = MakeStore();
  ASSERT_TRUE(store->CreateBranch("feature", "main", std::nullopt).has_value());
  store->ReleaseCheckout("feature");
  EXPECT_EQ(store->CheckoutCount("feature"), 0U);
}

namespace {
using memgraph::versioning::BranchCore;
using CoreResult = std::expected<std::shared_ptr<BranchCore>, std::string>;

// A non-null BranchCore* that is never dereferenced (BranchCore is incomplete here); the lifetime is observable
// through `observer`, which expires once the last owner of the core is released.
CoreResult MakeSentinelCore(std::weak_ptr<int> &observer) {
  auto token = std::make_shared<int>(0);
  observer = token;
  return std::shared_ptr<BranchCore>{token, reinterpret_cast<BranchCore *>(token.get())};
}
}  // namespace

// The shared core registry builds once per branch number, even across detach/re-attach.
TEST_F(VersionStoreTest, CoreIsBuiltOncePerBranch) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main", std::nullopt);
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
  auto created = store->CreateBranch("feature", "main", std::nullopt);
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

// Dropping a branch retires exactly its own core.
TEST_F(VersionStoreTest, DropRetiresOnlyTheDroppedBranchCore) {
  auto store = MakeStore();
  auto a = store->CreateBranch("a", "main", std::nullopt);
  auto b = store->CreateBranch("b", "main", std::nullopt);
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

  ASSERT_TRUE(store->DropBranch("b"));
  EXPECT_TRUE(b_core.expired()) << "dropping a branch must release its cached core";
  EXPECT_FALSE(a_core.expired()) << "dropping b must not retire a's core";

  ASSERT_TRUE(store->DropBranch("a"));
  EXPECT_TRUE(a_core.expired());

  // A recreated branch gets a fresh number and builds a fresh core.
  auto a2 = store->CreateBranch("a", "main", std::nullopt);
  ASSERT_TRUE(a2.has_value());
  ASSERT_TRUE(store->GetOrBuildCore(a2->number, build_a).has_value());
  EXPECT_EQ(a_builds, 2);
}
