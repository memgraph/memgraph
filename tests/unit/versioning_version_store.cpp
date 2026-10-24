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
    return std::make_unique<VersionStore>(
        [this] { return pins_.Acquire(); }, [this](uint64_t ts) { pins_.Release(ts); });
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
  EXPECT_FALSE(store->DropBranch("nope"));
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

// The shared core registry builds once per branch number, even across detach/re-attach.
TEST_F(VersionStoreTest, CoreIsBuiltOncePerBranch) {
  auto store = MakeStore();
  auto created = store->CreateBranch("feature", "main", std::nullopt);
  ASSERT_TRUE(created.has_value());

  int builds = 0;
  auto build = [&builds]() -> std::expected<std::shared_ptr<memgraph::versioning::BranchCore>, std::string> {
    ++builds;
    return std::shared_ptr<memgraph::versioning::BranchCore>{};
  };
  ASSERT_TRUE(store->GetOrBuildCore(created->number, build).has_value());
  ASSERT_TRUE(store->GetOrBuildCore(created->number, build).has_value());
  EXPECT_EQ(builds, 1);

  auto failing = []() -> std::expected<std::shared_ptr<memgraph::versioning::BranchCore>, std::string> {
    return std::unexpected(std::string{"boom"});
  };
  auto failed = store->GetOrBuildCore(created->number + 1, failing);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error(), "boom");
}
