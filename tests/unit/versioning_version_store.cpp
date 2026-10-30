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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"

using memgraph::versioning::BranchInfo;
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
  EXPECT_TRUE(store->Empty());

  auto created = store->CreateBranch("feature", "main");
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
  EXPECT_TRUE(store->Empty());

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

TEST_F(VersionStoreTest, RestoreUsesNewPinsKeepsRecordedForkAndContinuesNumbering) {
  LedgerContents contents;
  contents.next_number = 5;
  contents.branches.push_back({"a", BranchInfo{.number = 2, .parent = "main", .fork_ts = 10, .recorded_fork_ts = 10}});
  contents.branches.push_back({"c", BranchInfo{.number = 4, .parent = "a", .fork_ts = 30, .recorded_fork_ts = 30}});
  const std::vector<uint64_t> pins{pins_.Acquire(), pins_.Acquire()};

  auto store = MakeStore();
  std::optional<uint64_t> floor;
  store->SetRetentionFloorCallback([&](std::optional<uint64_t> f) { floor = f; });
  store->Restore(contents, pins);

  EXPECT_EQ(floor, 9U) << "the floor follows the recorded fork, not the restart pin";
  ASSERT_EQ(store->List().size(), 2);
  EXPECT_EQ(store->Get("a")->fork_ts, pins[0]);
  EXPECT_EQ(store->Get("a")->recorded_fork_ts, 10U);
  EXPECT_EQ(store->Get("c")->parent, "a");

  auto next = store->CreateBranch("d", "c");
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next->number, 5);

  ASSERT_TRUE(store->DropBranch("a").value_or(false));
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

  auto store = std::make_unique<VersionStore>([mem] { return mem->RegisterBranchForkPin(); },
                                              [mem](uint64_t fork_ts) { mem->ReleaseForkPin(fork_ts); });
  store->SetRetentionFloorCallback([mem](std::optional<uint64_t> floor) { mem->SetRetentionFloor(floor); });

  auto a = store->CreateBranch("a", "main");
  ASSERT_TRUE(a.has_value());
  ASSERT_GT(a->fork_ts, 0U);
  EXPECT_EQ(mem->OldestForkPin(), a->fork_ts);
  EXPECT_EQ(mem->RetentionFloor(), a->fork_ts - 1);
  ASSERT_TRUE(store->CreateBranch("b", "a").has_value());
  EXPECT_EQ(global.versioning_branches_created->Value() - created_before, 2.0);
  EXPECT_EQ(global.versioning_active_branches->Value() - active_before, 2.0);

  ASSERT_TRUE(store->DropBranch("a").value_or(false));
  EXPECT_EQ(global.versioning_branches_dropped->Value() - dropped_before, 1.0);
  EXPECT_EQ(global.versioning_active_branches->Value() - active_before, 1.0);
  ASSERT_TRUE(store->DropBranch("b").value_or(false));
  EXPECT_EQ(mem->OldestForkPin(), std::nullopt);
  EXPECT_EQ(mem->RetentionFloor(), std::nullopt);

  ASSERT_TRUE(store->CreateBranch("c", "main").has_value());
  store.reset();
  EXPECT_EQ(global.versioning_active_branches->Value(), active_before) << "a destroyed store leaves the gauge";
}
