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
#include <filesystem>
#include <memory>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "versioning/version_store.hpp"

using memgraph::versioning::BranchInfo;
using memgraph::versioning::VersionStore;

namespace {
constexpr auto kTestSuite = "versioning_version_store";
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / kTestSuite};
}  // namespace

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
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }

  std::unique_ptr<VersionStore> MakeStore(std::filesystem::path dir = kStorageDirectory) {
    return std::make_unique<VersionStore>(
        std::move(dir), [this] { return pins_.Acquire(); }, [this](uint64_t ts) { pins_.Release(ts); });
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

TEST_F(VersionStoreTest, NestedParentOffABranch) {
  auto store = MakeStore();

  auto parent = store->CreateBranch("parent", "main", std::nullopt);
  ASSERT_TRUE(parent.has_value());

  auto child = store->CreateBranch("child", "parent", std::nullopt);
  ASSERT_TRUE(child.has_value());
  EXPECT_EQ(child->parent, "parent");
  EXPECT_EQ(child->number, 3);
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

TEST_F(VersionStoreTest, DropWithChildrenRejected) {
  auto store = MakeStore();

  ASSERT_TRUE(store->CreateBranch("parent", "main", std::nullopt).has_value());
  ASSERT_TRUE(store->CreateBranch("child", "parent", std::nullopt).has_value());

  EXPECT_FALSE(store->DropBranch("parent"));
  EXPECT_TRUE(store->Exists("parent"));  // untouched

  // Drop the child first, then the parent succeeds.
  EXPECT_TRUE(store->DropBranch("child"));
  EXPECT_TRUE(store->DropBranch("parent"));
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

TEST_F(VersionStoreTest, DurabilityRoundTrip) {
  uint64_t fork_ts_a{};
  uint64_t fork_ts_b{};
  {
    auto store = MakeStore();
    auto a = store->CreateBranch("a", "main", "branch a");
    ASSERT_TRUE(a.has_value());
    fork_ts_a = a->fork_ts;

    auto b = store->CreateBranch("b", "a", std::nullopt);
    ASSERT_TRUE(b.has_value());
    fork_ts_b = b->fork_ts;
    // store destroyed here (end of scope) -- durable data lives on in kStorageDirectory.
  }

  // Reset the fake pin registry bookkeeping: a fresh VersionStore reconstruction must NOT
  // re-acquire pins for the branches it loads from disk.
  pins_ = FakePinRegistry{};

  auto reloaded = MakeStore();
  EXPECT_TRUE(pins_.live_.empty());  // nothing re-acquired purely from loading

  EXPECT_TRUE(reloaded->Exists("a"));
  EXPECT_TRUE(reloaded->Exists("b"));

  auto a = reloaded->Get("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->number, 2);
  EXPECT_EQ(a->parent, "main");
  EXPECT_EQ(a->fork_ts, fork_ts_a);
  EXPECT_EQ(a->description, "branch a");

  auto b = reloaded->Get("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3);
  EXPECT_EQ(b->parent, "a");
  EXPECT_EQ(b->fork_ts, fork_ts_b);

  // next_number_ correctly restored: a new branch continues from 4, not reusing 2/3.
  auto c = reloaded->CreateBranch("c", "main", std::nullopt);
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->number, 4);

  // The persisted '.next_number' counter itself (not just observed behavior) reflects the create
  // above. Close `reloaded` first -- kvstore forbids two live instances on the same directory.
  reloaded.reset();
  {
    memgraph::kvstore::KVStore raw_store{kStorageDirectory};
    auto persisted_next = raw_store.Get(".next_number");
    ASSERT_TRUE(persisted_next.has_value());
    EXPECT_EQ(*persisted_next, "5");
  }
}

// Reproduces the HIGH bug found by logic-verifier: dropping the highest-numbered branch and then
// restarting the VersionStore (reconstructing from the same durable directory) must NOT hand the
// dropped branch's number back out. Root cause was deriving next_number_ purely from the max
// number among SURVIVING branch records -- since DropBranch deletes the branch's own record, the
// dropped (highest) number becomes invisible to that derivation after a restart.
TEST_F(VersionStoreTest, ReloadAfterDroppingHighestNumberedBranchDoesNotReuseNumber) {
  {
    auto store = MakeStore();
    auto a = store->CreateBranch("a", "main", std::nullopt);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->number, 2);

    ASSERT_TRUE(store->DropBranch("a"));
    // store destroyed at end of scope. The kvstore no longer has ANY branch record -- if the
    // counter were derived from surviving records, a reload would see nothing and restart
    // numbering from 2.
  }

  pins_ = FakePinRegistry{};  // a fresh registry: reload must not re-acquire any pin

  auto reloaded = MakeStore();
  EXPECT_FALSE(reloaded->Exists("a"));  // the record itself really is gone
  EXPECT_TRUE(pins_.live_.empty());     // and nothing was re-acquired purely from loading

  auto b = reloaded->CreateBranch("b", "main", std::nullopt);
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->number, 3);  // MUST NOT reuse 2, even though 'a' (the only branch) was dropped
}
