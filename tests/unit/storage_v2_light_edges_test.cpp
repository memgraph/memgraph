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

/**
 * Unit tests for light edges (storage_light_edge): edges stored only in vertex
 * lists, no global edge skiplist. Focus on MVCC correctness: visibility (OLD/NEW),
 * concurrent read/write, commit/abort, FindEdge by GID, and delete/release.
 *
 * Storage mode and isolation coverage:
 * - LightEdgesTestWithModes runs core tests under all 6 combinations:
 *   Storage modes: IN_MEMORY_TRANSACTIONAL, IN_MEMORY_ANALYTICAL (InMemoryStorage only; ON_DISK_TRANSACTIONAL uses
 * DiskStorage). Isolation levels: SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED.
 * - LightEdgesTest uses default (transactional + snapshot isolation); some tests explicitly use READ_UNCOMMITTED (e.g.
 * ReadUncommittedSeesEdgeThenAbortDeferred).
 * - Concurrent writer tests run only in transactional mode (analytical allows only one writer at a time).
 *
 * RAII-like lifecycle (release only where we remove refs or tear down):
 * - Abort: undo create removes edge from vertex vectors -> ReleaseLight (CreateEdgeAbort, AbortThenCreateCommitDelete,
 * RepeatedAbortThenSingleCommit).
 * - Commit (delete): apply remove removes edge from vertex vectors -> ReleaseLight (DeleteEdgeCommit,
 * GcAfterCreateThenDeleteAll).
 * - GC: REMOVE deltas hold only Gid; slab drop runs no destructors -> no release at GC (MultipleEdgesAndGc,
 * GcAfterCreateThenDeleteAll).
 * - Teardown: ~InMemoryStorage releases all vertex edge refs when storage_light_edge (every test; TearDown calls
 * FreeMemory then reset).
 */

#include <atomic>
#include <random>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>

#include "flags/general.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

static ms::Config LightEdgeConfig() {
  ms::Config config{};
  config.gc.type = ms::Config::Gc::Type::NONE;  // Control GC manually
  config.salient.items.properties_on_edges = true;
  config.salient.items.storage_light_edge = true;
  return config;
}

static ms::Config LightEdgeConfigWithMode(ms::IsolationLevel isolation_level) {
  auto config = LightEdgeConfig();
  config.transaction.isolation_level = isolation_level;
  return config;
}

// Parameterized tests: run core light-edge scenarios under each (storage mode, isolation level).
// Storage modes: IN_MEMORY_TRANSACTIONAL, IN_MEMORY_ANALYTICAL (this test file uses InMemoryStorage only).
// Isolation levels: SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED.
struct LightEdgesModeParam {
  ms::StorageMode storage_mode;
  ms::IsolationLevel isolation_level;
};

static std::string LightEdgesModeParamName(const testing::TestParamInfo<LightEdgesModeParam> &info) {
  const auto &p = info.param;
  std::string mode_str = (p.storage_mode == ms::StorageMode::IN_MEMORY_ANALYTICAL)
                             ? "Analytical"
                             : (p.storage_mode == ms::StorageMode::IN_MEMORY_TRANSACTIONAL ? "Transactional" : "Disk");
  std::string iso_str =
      (p.isolation_level == ms::IsolationLevel::SNAPSHOT_ISOLATION)
          ? "SnapshotIso"
          : (p.isolation_level == ms::IsolationLevel::READ_COMMITTED ? "ReadCommitted" : "ReadUncommitted");
  return mode_str + "_" + iso_str;
}

class LightEdgesTestWithModes : public ::testing::TestWithParam<LightEdgesModeParam> {
 protected:
  void SetUp() override {
    FLAGS_storage_light_edge = true;
    const auto &p = GetParam();
    auto config = LightEdgeConfigWithMode(p.isolation_level);
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    static_cast<ms::InMemoryStorage *>(storage_.get())->SetStorageMode(p.storage_mode);
  }

  void TearDown() override {
    if (storage_) {
      storage_->FreeMemory();
    }
    storage_.reset();
  }

  bool IsAnalytical() const { return GetParam().storage_mode == ms::StorageMode::IN_MEMORY_ANALYTICAL; }

  std::unique_ptr<ms::Storage> storage_;
};

class LightEdgesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_storage_light_edge = true;  // Match LightEdgeConfig(); needed for EdgeRef == Gid
    storage_ = std::make_unique<ms::InMemoryStorage>(LightEdgeConfig());
  }

  void TearDown() override {
    if (storage_) {
      storage_->FreeMemory();
    }
    storage_.reset();
  }

  std::unique_ptr<ms::Storage> storage_;
};

// --- Parameterized: run core scenarios under all (storage mode, isolation level) combinations ---

TEST_P(LightEdgesTestWithModes, CreateEdgeCommitThenRead) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value()) << "CreateEdge failed";
    edge_gid = res->Gid();
    // OLD view: SI/RC do not see own uncommitted edge; READ_UNCOMMITTED and analytical may show it
    if (!IsAnalytical() && GetParam().isolation_level != ms::IsolationLevel::READ_UNCOMMITTED) {
      ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    }
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 1U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 1U);
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 1U);
    ASSERT_EQ(v_to->InEdges(ms::View::OLD)->edges.size(), 1U);
    ASSERT_EQ(v_to->InEdges(ms::View::NEW)->edges.size(), 1U);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(maybe_edge.has_value()) << "FindEdge(gid) should see committed edge";
    acc->Abort();
  }
}

TEST_P(LightEdgesTestWithModes, CreateEdgeAbort) {
  if (IsAnalytical()) {
    GTEST_SKIP() << "Abort visibility semantics differ in analytical mode";
  }
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    acc->Abort();
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 0U);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_FALSE(maybe_edge.has_value()) << "FindEdge(gid) must not see aborted edge";
    acc->Abort();
  }
}

TEST_P(LightEdgesTestWithModes, FindEdgeByGid) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto by_gid = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(by_gid.has_value());
    EXPECT_EQ(by_gid->Gid(), edge_gid);
    auto by_gid_and_from = acc->FindEdge(edge_gid, gid_from, ms::View::OLD);
    ASSERT_TRUE(by_gid_and_from.has_value());
    EXPECT_EQ(by_gid_and_from->Gid(), edge_gid);
    acc->Abort();
  }
}

TEST_P(LightEdgesTestWithModes, DeleteEdgeCommit) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&v_from, &v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(maybe_edge.has_value());
    auto del_res = acc->DeleteEdge(&*maybe_edge);
    ASSERT_TRUE(del_res.has_value());
    ASSERT_TRUE(*del_res);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    ASSERT_FALSE(acc->FindEdge(edge_gid, ms::View::OLD).has_value());
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_to->InEdges(ms::View::OLD)->edges.size(), 0U);
    acc->Abort();
  }
}

TEST_P(LightEdgesTestWithModes, MultipleEdgesAndGc) {
  ms::Gid gid_v0{};
  ms::Gid gid_v1{};
  std::vector<ms::Gid> edge_gids;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v0 = acc->CreateVertex();
    auto v1 = acc->CreateVertex();
    gid_v0 = v0.Gid();
    gid_v1 = v1.Gid();
    auto et = acc->NameToEdgeType("E");
    for (int i = 0; i < 5; ++i) {
      auto res = acc->CreateEdge(&v0, &v1, et);
      ASSERT_TRUE(res.has_value());
      edge_gids.push_back(res->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v0 = acc->FindVertex(gid_v0, ms::View::NEW);
    auto v1 = acc->FindVertex(gid_v1, ms::View::NEW);
    ASSERT_TRUE(v0.has_value());
    ASSERT_TRUE(v1.has_value());
    ASSERT_EQ(v0->OutEdges(ms::View::OLD)->edges.size(), 5U);
    ASSERT_EQ(v1->InEdges(ms::View::OLD)->edges.size(), 5U);
    for (const auto &eg : edge_gids) {
      auto maybe = acc->FindEdge(eg, ms::View::OLD);
      ASSERT_TRUE(maybe.has_value()) << "FindEdge(" << eg.AsUint() << ") after GC";
    }
    acc->Abort();
  }
}

// Concurrent writer + reader: only in transactional mode (analytical allows single writer only).
TEST_P(LightEdgesTestWithModes, ConcurrentEdgeCreationAndCount) {
  if (IsAnalytical()) {
    GTEST_SKIP() << "Analytical mode allows only one writer at a time";
  }
  constexpr int kWriterIterations = 100;
  constexpr int kEdgesPerBatch = 20;
  std::atomic<bool> writer_done{false};
  std::atomic<uint64_t> reader_count_ops{0};

  {
    auto acc = storage_->Access(ms::WRITE);
    for (int i = 0; i < 4; ++i) {
      acc->CreateVertex();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::thread writer([this, &writer_done] {
    for (int iter = 0; iter < kWriterIterations; ++iter) {
      auto acc = storage_->Access(ms::WRITE);
      auto v_from = acc->CreateVertex();
      auto v_to = acc->CreateVertex();
      auto et = acc->NameToEdgeType("E");
      for (int e = 0; e < kEdgesPerBatch; ++e) {
        auto res = acc->CreateEdge(&v_from, &v_to, et);
        ASSERT_TRUE(res.has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    writer_done.store(true, std::memory_order_release);
  });

  std::thread reader([this, &writer_done, &reader_count_ops] {
    while (!writer_done.load(std::memory_order_acquire)) {
      auto acc = storage_->Access(ms::READ);
      uint64_t edge_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        const auto &v = *it;
        auto out = v.OutEdges(ms::View::OLD);
        if (out.has_value()) {
          edge_count += out->edges.size();
        }
      }
      (void)edge_count;
      reader_count_ops.fetch_add(1, std::memory_order_relaxed);
    }
    for (int i = 0; i < 20; ++i) {
      auto acc = storage_->Access(ms::READ);
      uint64_t edge_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        const auto &v = *it;
        auto out = v.OutEdges(ms::View::OLD);
        if (out.has_value()) {
          edge_count += out->edges.size();
        }
      }
      (void)edge_count;
      reader_count_ops.fetch_add(1, std::memory_order_relaxed);
    }
  });

  writer.join();
  reader.join();
  EXPECT_GT(reader_count_ops.load(std::memory_order_relaxed), 0U);
}

static const std::vector<LightEdgesModeParam> kLightEdgesModeParams = {
    {ms::StorageMode::IN_MEMORY_TRANSACTIONAL, ms::IsolationLevel::SNAPSHOT_ISOLATION},
    {ms::StorageMode::IN_MEMORY_TRANSACTIONAL, ms::IsolationLevel::READ_COMMITTED},
    {ms::StorageMode::IN_MEMORY_TRANSACTIONAL, ms::IsolationLevel::READ_UNCOMMITTED},
    {ms::StorageMode::IN_MEMORY_ANALYTICAL, ms::IsolationLevel::SNAPSHOT_ISOLATION},
    {ms::StorageMode::IN_MEMORY_ANALYTICAL, ms::IsolationLevel::READ_COMMITTED},
    {ms::StorageMode::IN_MEMORY_ANALYTICAL, ms::IsolationLevel::READ_UNCOMMITTED},
};

INSTANTIATE_TEST_SUITE_P(AllStorageAndIsolationModes, LightEdgesTestWithModes,
                         ::testing::ValuesIn(kLightEdgesModeParams), LightEdgesModeParamName);

// --- Create + commit: edge visible in new transaction with OLD and NEW view ---
TEST_F(LightEdgesTest, CreateEdgeCommitThenRead) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value()) << "CreateEdge failed";
    edge_gid = res->Gid();
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 1U);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 1U);
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 1U);
    ASSERT_EQ(v_to->InEdges(ms::View::OLD)->edges.size(), 1U);
    ASSERT_EQ(v_to->InEdges(ms::View::NEW)->edges.size(), 1U);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(maybe_edge.has_value()) << "FindEdge(gid) should see committed edge";
    acc->Abort();
  }
}

// --- Create + abort: edge not visible; RAII release on abort (remove from vertex vectors) ---
TEST_F(LightEdgesTest, CreateEdgeAbort) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    acc->Abort();
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    ASSERT_EQ(v_from->OutEdges(ms::View::NEW)->edges.size(), 0U);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_FALSE(maybe_edge.has_value()) << "FindEdge(gid) must not see aborted edge";
    acc->Abort();
  }
}

// --- Two transactions: Tx1 reads OLD (no edge) before Tx0 commits; after commit Tx1 sees edge in NEW ---
TEST_F(LightEdgesTest, TwoTxVisibilityOldNew) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::unique_ptr<ms::Storage::Accessor> tx0;
  std::unique_ptr<ms::Storage::Accessor> tx1;
  tx0 = storage_->Access(ms::WRITE);
  tx1 = storage_->Access(ms::WRITE);

  auto v_from0 = tx0->FindVertex(gid_from, ms::View::NEW);
  auto v_to0 = tx0->FindVertex(gid_to, ms::View::NEW);
  ASSERT_TRUE(v_from0.has_value());
  ASSERT_TRUE(v_to0.has_value());
  auto et = tx0->NameToEdgeType("E");
  auto res = tx0->CreateEdge(&*v_from0, &*v_to0, et);
  ASSERT_TRUE(res.has_value());

  // Tx1: OLD view must not see uncommitted edge
  auto v_from1 = tx1->FindVertex(gid_from, ms::View::OLD);
  ASSERT_TRUE(v_from1.has_value());
  ASSERT_EQ(v_from1->OutEdges(ms::View::OLD)->edges.size(), 0U);

  tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  tx0.reset();

  // Tx1: With snapshot isolation, Tx1 started before Tx0 committed so its snapshot may not
  // include Tx0's edge. OLD is 0; NEW view is implementation-defined (may be 0 or 1).
  // A new transaction will see the edge.
  auto out_new = v_from1->OutEdges(ms::View::NEW);
  ASSERT_TRUE(out_new.has_value());
  tx1->Abort();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->FindVertex(gid_from, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_EQ(v->OutEdges(ms::View::OLD)->edges.size(), 1U) << "New tx must see committed edge";
    acc->Abort();
  }
}

// --- FindEdge by GID after commit ---
TEST_F(LightEdgesTest, FindEdgeByGid) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto by_gid = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(by_gid.has_value());
    EXPECT_EQ(by_gid->Gid(), edge_gid);
    auto by_gid_and_from = acc->FindEdge(edge_gid, gid_from, ms::View::OLD);
    ASSERT_TRUE(by_gid_and_from.has_value());
    EXPECT_EQ(by_gid_and_from->Gid(), edge_gid);
    acc->Abort();
  }
}

// --- Delete edge, commit: RAII release on commit (remove from vertex vectors); then GC ---
TEST_F(LightEdgesTest, DeleteEdgeCommit) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&v_from, &v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(maybe_edge.has_value());
    auto del_res = acc->DeleteEdge(&*maybe_edge);
    ASSERT_TRUE(del_res.has_value());
    ASSERT_TRUE(*del_res);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    ASSERT_FALSE(acc->FindEdge(edge_gid, ms::View::OLD).has_value());
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_to->InEdges(ms::View::OLD)->edges.size(), 0U);
    acc->Abort();
  }
}

// --- Delete edge, abort: edge still visible after abort ---
TEST_F(LightEdgesTest, DeleteEdgeAbort) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&v_from, &v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(maybe_edge.has_value());
    auto del_res = acc->DeleteEdge(&*maybe_edge);
    ASSERT_TRUE(del_res.has_value());
    ASSERT_TRUE(*del_res);
    acc->Abort();
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(maybe_edge.has_value()) << "Edge must still exist after delete-abort";
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 1U);
    acc->Abort();
  }
}

// --- RAII: abort releases refs; then create+commit+delete+commit; GC drops deltas (Gid only) ---
TEST_F(LightEdgesTest, AbortThenCreateCommitDelete) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et).has_value());
    acc->Abort();
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto maybe_edge = acc->FindEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(maybe_edge.has_value());
    auto del_res = acc->DeleteEdge(&*maybe_edge);
    ASSERT_TRUE(del_res.has_value());
    ASSERT_TRUE(*del_res);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    ASSERT_FALSE(acc->FindEdge(edge_gid, ms::View::OLD).has_value());
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    acc->Abort();
  }
}

// --- RAII: repeated abort then single commit; GC after commit ---
TEST_F(LightEdgesTest, RepeatedAbortThenSingleCommit) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  for (int i = 0; i < 5; ++i) {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et).has_value());
    acc->Abort();
  }

  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto maybe = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(maybe.has_value());
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 1U);
    acc->Abort();
  }
}

// --- RAII: create -> commit -> GC; delete all -> commit -> GC; create again ---
TEST_F(LightEdgesTest, GcAfterCreateThenDeleteAll) {
  constexpr int kNumEdges = 10;
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  std::vector<ms::Gid> edge_gids;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    auto et = acc->NameToEdgeType("E");
    for (int i = 0; i < kNumEdges; ++i) {
      auto res = acc->CreateEdge(&v_from, &v_to, et);
      ASSERT_TRUE(res.has_value());
      edge_gids.push_back(res->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), static_cast<size_t>(kNumEdges));
    for (const auto &eg : edge_gids) {
      auto maybe = acc->FindEdge(eg, ms::View::NEW);
      ASSERT_TRUE(maybe.has_value());
      auto del_res = acc->DeleteEdge(&*maybe);
      ASSERT_TRUE(del_res.has_value());
      ASSERT_TRUE(*del_res);
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    ASSERT_EQ(v_to->InEdges(ms::View::OLD)->edges.size(), 0U);
    for (const auto &eg : edge_gids) {
      ASSERT_FALSE(acc->FindEdge(eg, ms::View::OLD).has_value());
    }
    auto et = acc->NameToEdgeType("E");
    for (int i = 0; i < 5; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 5U);
    acc->Abort();
  }
}

// --- Multiple edges, commit, GC (deltas hold only Gid; no release at GC), read ---
TEST_F(LightEdgesTest, MultipleEdgesAndGc) {
  ms::Gid gid_v0{};
  ms::Gid gid_v1{};
  std::vector<ms::Gid> edge_gids;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v0 = acc->CreateVertex();
    auto v1 = acc->CreateVertex();
    gid_v0 = v0.Gid();
    gid_v1 = v1.Gid();
    auto et = acc->NameToEdgeType("E");
    for (int i = 0; i < 5; ++i) {
      auto res = acc->CreateEdge(&v0, &v1, et);
      ASSERT_TRUE(res.has_value());
      edge_gids.push_back(res->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v0 = acc->FindVertex(gid_v0, ms::View::NEW);
    auto v1 = acc->FindVertex(gid_v1, ms::View::NEW);
    ASSERT_TRUE(v0.has_value());
    ASSERT_TRUE(v1.has_value());
    ASSERT_EQ(v0->OutEdges(ms::View::OLD)->edges.size(), 5U);
    ASSERT_EQ(v1->InEdges(ms::View::OLD)->edges.size(), 5U);
    for (const auto &eg : edge_gids) {
      auto maybe = acc->FindEdge(eg, ms::View::OLD);
      ASSERT_TRUE(maybe.has_value()) << "FindEdge(" << eg.AsUint() << ") after GC";
    }
    acc->Abort();
  }
}

// --- Reuse pattern from storage_v2_edge_inmemory EdgeCreateFromSmallerCommit with light edge config ---
TEST_F(LightEdgesTest, EdgeCreateFromSmallerCommit) {
  ms::Gid gid_from = ms::Gid::FromUint(std::numeric_limits<uint64_t>::max());
  ms::Gid gid_to = ms::Gid::FromUint(std::numeric_limits<uint64_t>::max());

  {
    auto acc = storage_->Access(ms::WRITE);
    auto vertex_from = acc->CreateVertex();
    auto vertex_to = acc->CreateVertex();
    gid_from = vertex_from.Gid();
    gid_to = vertex_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto vertex_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);

    auto et = acc->NameToEdgeType("et5");
    auto res = acc->CreateEdge(&*vertex_from, &*vertex_to, et);
    ASSERT_TRUE(res.has_value());
    auto edge = res.value();
    ASSERT_EQ(edge.EdgeType(), et);

    ASSERT_EQ(vertex_from->OutEdges(ms::View::OLD)->edges.size(), 0);
    ASSERT_EQ(vertex_from->OutEdges(ms::View::NEW)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(ms::View::OLD)->edges.size(), 0);
    ASSERT_EQ(vertex_to->InEdges(ms::View::NEW)->edges.size(), 1);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto vertex_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto vertex_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(vertex_from);
    ASSERT_TRUE(vertex_to);
    auto et = acc->NameToEdgeType("et5");
    ASSERT_EQ(vertex_from->OutEdges(ms::View::OLD)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(ms::View::NEW)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(ms::View::OLD)->edges.size(), 1);
    ASSERT_EQ(vertex_to->InEdges(ms::View::NEW)->edges.size(), 1);
    ASSERT_EQ(vertex_from->OutEdges(ms::View::OLD)->edges[0].EdgeType(), et);
    acc->Abort();
  }
}

// --- Concurrent-like: two writers create edges on same vertices, commit in order ---
TEST_F(LightEdgesTest, TwoWritersSequentialCommit) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et1 = acc->NameToEdgeType("E1");
    ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et1).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et2 = acc->NameToEdgeType("E2");
    ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et2).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  storage_->FreeMemory();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    auto out = v_from->OutEdges(ms::View::OLD);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 2U);
    acc->Abort();
  }
}

// --- Concurrent: one thread creates batches of edges, another counts (MVCC view: scan vertices + out_edges) ---
// Reproduces the scenario where one client creates edges and another runs a count-like query (ScanAll -> Expand).
// Run with --gtest_repeat=N to increase chance of hitting timing-dependent races.
TEST_F(LightEdgesTest, ConcurrentEdgeCreationAndCount) {
  constexpr int kWriterIterations = 500;
  constexpr int kEdgesPerBatch = 30;
  std::atomic<bool> writer_done{false};
  std::atomic<uint64_t> reader_count_ops{0};

  // Seed a few vertices so the reader has something to scan from the start
  {
    auto acc = storage_->Access(ms::WRITE);
    for (int i = 0; i < 4; ++i) {
      acc->CreateVertex();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::thread writer([this, &writer_done] {
    for (int iter = 0; iter < kWriterIterations; ++iter) {
      auto acc = storage_->Access(ms::WRITE);
      auto v_from = acc->CreateVertex();
      auto v_to = acc->CreateVertex();
      auto et = acc->NameToEdgeType("E");
      for (int e = 0; e < kEdgesPerBatch; ++e) {
        auto res = acc->CreateEdge(&v_from, &v_to, et);
        ASSERT_TRUE(res.has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    writer_done.store(true, std::memory_order_release);
  });

  std::thread reader([this, &writer_done, &reader_count_ops] {
    while (!writer_done.load(std::memory_order_acquire)) {
      auto acc = storage_->Access(ms::READ);
      uint64_t edge_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        const auto &v = *it;
        auto out = v.OutEdges(ms::View::OLD);
        if (out.has_value()) {
          edge_count += out->edges.size();
        }
      }
      (void)edge_count;
      reader_count_ops.fetch_add(1, std::memory_order_relaxed);
    }
    // Drain more reads after writer finishes to catch post-commit races
    for (int i = 0; i < 50; ++i) {
      auto acc = storage_->Access(ms::READ);
      uint64_t edge_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        const auto &v = *it;
        auto out = v.OutEdges(ms::View::OLD);
        if (out.has_value()) {
          edge_count += out->edges.size();
        }
      }
      (void)edge_count;
      reader_count_ops.fetch_add(1, std::memory_order_relaxed);
    }
  });

  writer.join();
  reader.join();

  EXPECT_GT(reader_count_ops.load(std::memory_order_relaxed), 0U);
}

// --- Dataset-style: mirrors tools/setup_memgraph_dataset.py + "MATCH p=()--() RETURN count(p)" ---
// Script: 6× edges vs nodes, nodes have one label (Label_0..Label_{n-1}) and property "id", edges have type TYPE_0..
// Query: scan all vertices and expand edges (count paths of length 1). One thread creates edge batches, another counts.
TEST_F(LightEdgesTest, ConcurrentDatasetStyleEdgeBatchesAndPathCount) {
  constexpr int kNumNodes = 2000;
  constexpr int kNumLabels = 8;
  constexpr int kNumEdgeTypes = 6;
  constexpr int kEdgesPerNode = 6;
  constexpr int kTotalEdges = kNumNodes * kEdgesPerNode;  // 12000
  constexpr int kNodeBatchSize = 500;
  constexpr int kEdgeBatchSize = 500;
  constexpr int kWriterEdgeBatches = 80;  // writer adds 80 * kEdgeBatchSize = 40000 more edges
  std::atomic<bool> writer_done{false};
  std::atomic<uint64_t> reader_ops{0};

  // 1) Create nodes in batches: label Label_{i % 8}, property id = double(i)
  std::vector<ms::Gid> node_gids;
  node_gids.reserve(kNumNodes);
  {
    for (int start = 0; start < kNumNodes; start += kNodeBatchSize) {
      int end = std::min(start + kNodeBatchSize, kNumNodes);
      auto acc = storage_->Access(ms::WRITE);
      for (int i = start; i < end; ++i) {
        auto v = acc->CreateVertex();
        std::string label_name = "Label_" + std::to_string(i % kNumLabels);
        ASSERT_TRUE(v.AddLabel(acc->NameToLabel(label_name)).has_value());
        ASSERT_TRUE(v.SetProperty(acc->NameToProperty("id"), ms::PropertyValue(static_cast<double>(i))).has_value());
        node_gids.push_back(v.Gid());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
  ASSERT_EQ(node_gids.size(), static_cast<size_t>(kNumNodes));

  // 2) Create initial 6*N edges in batches (random src, tgt, edge type)
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> node_dist(0, kNumNodes - 1);
  int edges_created = 0;
  while (edges_created < kTotalEdges) {
    int batch = std::min(kEdgeBatchSize, kTotalEdges - edges_created);
    auto acc = storage_->Access(ms::WRITE);
    for (int e = 0; e < batch; ++e) {
      int src_idx = node_dist(rng);
      int tgt_idx = node_dist(rng);
      int type_idx = (edges_created + e) % kNumEdgeTypes;
      std::string type_name = "TYPE_" + std::to_string(type_idx);
      auto v_from = acc->FindVertex(node_gids[src_idx], ms::View::NEW);
      auto v_to = acc->FindVertex(node_gids[tgt_idx], ms::View::NEW);
      ASSERT_TRUE(v_from.has_value());
      ASSERT_TRUE(v_to.has_value());
      auto et = acc->NameToEdgeType(type_name);
      auto res = acc->CreateEdge(&*v_from, &*v_to, et);
      ASSERT_TRUE(res.has_value());
    }
    edges_created += batch;
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // 3) Writer: add more edge batches. Reader: count paths ()--() (sum of out-degrees)
  std::thread writer([this, &node_gids, &writer_done] {
    std::mt19937 writer_rng(123);
    std::uniform_int_distribution<int> node_dist(0, kNumNodes - 1);
    for (int b = 0; b < kWriterEdgeBatches; ++b) {
      auto acc = storage_->Access(ms::WRITE);
      for (int e = 0; e < kEdgeBatchSize; ++e) {
        int src_idx = node_dist(writer_rng);
        int tgt_idx = node_dist(writer_rng);
        int type_idx = (b * kEdgeBatchSize + e) % kNumEdgeTypes;
        std::string type_name = "TYPE_" + std::to_string(type_idx);
        auto v_from = acc->FindVertex(node_gids[src_idx], ms::View::NEW);
        auto v_to = acc->FindVertex(node_gids[tgt_idx], ms::View::NEW);
        ASSERT_TRUE(v_from.has_value());
        ASSERT_TRUE(v_to.has_value());
        auto et = acc->NameToEdgeType(type_name);
        auto res = acc->CreateEdge(&*v_from, &*v_to, et);
        ASSERT_TRUE(res.has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    writer_done.store(true, std::memory_order_release);
  });

  std::thread reader([this, &writer_done, &reader_ops] {
    while (!writer_done.load(std::memory_order_acquire)) {
      auto acc = storage_->Access(ms::READ);
      uint64_t path_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        auto out = (*it).OutEdges(ms::View::OLD);
        if (out.has_value()) {
          path_count += out->edges.size();
        }
      }
      (void)path_count;
      reader_ops.fetch_add(1, std::memory_order_relaxed);
    }
    for (int i = 0; i < 30; ++i) {
      auto acc = storage_->Access(ms::READ);
      uint64_t path_count = 0;
      auto vertices = acc->Vertices(ms::View::OLD);
      for (auto it = vertices.begin(); it != vertices.end(); ++it) {
        auto out = (*it).OutEdges(ms::View::OLD);
        if (out.has_value()) {
          path_count += out->edges.size();
        }
      }
      (void)path_count;
      reader_ops.fetch_add(1, std::memory_order_relaxed);
    }
  });

  writer.join();
  reader.join();

  EXPECT_GT(reader_ops.load(std::memory_order_relaxed), 0U);
}

// --- Deferred light edge delete: READ_UNCOMMITTED reader can hold refs while writer aborts ---
TEST_F(LightEdgesTest, ReadUncommittedSeesEdgeThenAbortDeferred) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Writer creates edge (uncommitted)
  {
    auto writer = storage_->Access(ms::WRITE);
    auto v_from = writer->FindVertex(gid_from, ms::View::NEW);
    auto v_to = writer->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = writer->NameToEdgeType("E");
    auto res = writer->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());

    // READ_UNCOMMITTED reader sees the uncommitted edge
    {
      auto reader = storage_->Access(ms::WRITE, std::optional{ms::IsolationLevel::READ_UNCOMMITTED}, std::nullopt);
      auto v = reader->FindVertex(gid_from, ms::View::NEW);
      ASSERT_TRUE(v.has_value());
      auto out = v->OutEdges(ms::View::NEW);
      ASSERT_TRUE(out.has_value());
      ASSERT_EQ(out->edges.size(), 1U);  // sees writer's uncommitted edge
      // reader holds EdgeAccessors here
    }
    // Reader destroyed; ru_or_analytical_accessors_ is 0

    writer->Abort();  // edge removed from vertices, ref deferred (not freed)
  }

  // GC can run and drain deferred list (no RU accessor active)
  storage_->FreeMemory();

  // New transaction: edge should not exist
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 0U);
    acc->Abort();
  }
}

// --- Deferred list drained when no READ_UNCOMMITTED/analytical accessors; then normal ops ---
TEST_F(LightEdgesTest, DeferredDrainedOnGcWhenNoRuAccessors) {
  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->CreateVertex();
    auto v_to = acc->CreateVertex();
    gid_from = v_from.Gid();
    gid_to = v_to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Create edge and abort -> deferred
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    ASSERT_TRUE(acc->CreateEdge(&*v_from, &*v_to, et).has_value());
    acc->Abort();
  }

  // No READ_UNCOMMITTED accessor; GC drains deferred list
  storage_->FreeMemory();

  // Create and commit a new edge
  ms::Gid edge_gid{};
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    auto v_to = acc->FindVertex(gid_to, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_TRUE(v_to.has_value());
    auto et = acc->NameToEdgeType("E");
    auto res = acc->CreateEdge(&*v_from, &*v_to, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage_->Access(ms::WRITE);
    auto maybe = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(maybe.has_value());
    auto v_from = acc->FindVertex(gid_from, ms::View::NEW);
    ASSERT_TRUE(v_from.has_value());
    ASSERT_EQ(v_from->OutEdges(ms::View::OLD)->edges.size(), 1U);
    acc->Abort();
  }
}
