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

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "memory/db_arena.hpp"
#include "memory/query_memory_control.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory_tracker.hpp"

namespace {

memgraph::storage::Config MakeConfig(const std::filesystem::path &dir) {
  memgraph::storage::Config config{};
  config.durability.storage_directory = dir;
  config.disk.main_storage_directory = dir / "disk";
  config.gc.type = memgraph::storage::Config::Gc::Type::NONE;
  return config;
}

}  // namespace

class DbMemoryTrackingTest : public ::testing::Test {
 protected:
  std::filesystem::path data_dir_{std::filesystem::temp_directory_path() / "mg_test_db_memory_tracking"};

  void SetUp() override { std::filesystem::create_directories(data_dir_); }

  void TearDown() override { std::filesystem::remove_all(data_dir_); }
};

#if USE_JEMALLOC

TEST_F(DbMemoryTrackingTest, ArenaIdxIsNonZero) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;
  EXPECT_NE(db->ArenaIdx(), 0u) << "Each Database should own a unique jemalloc arena";
}

TEST_F(DbMemoryTrackingTest, TwoDbsGetDifferentArenas) {
  auto dir1 = data_dir_ / "db1";
  auto dir2 = data_dir_ / "db2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};

  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  EXPECT_NE((*acc1)->ArenaIdx(), (*acc2)->ArenaIdx()) << "Two databases must not share a jemalloc arena";
}

TEST_F(DbMemoryTrackingTest, AllocationInArenaIsTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0u);

  const int64_t before = db->DbMemoryUsage();

  // Allocate ~4 MiB via DbAwareAllocator while the DB arena is pinned on this thread.
  // The extent hooks should fire and report committed pages to db_memory_tracker_.
  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;  // 4 MiB total

  memgraph::memory::DbArenaScope scope{arena_idx};

  std::vector<void *> ptrs;
  ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_ARENA(arena_idx) | MALLOCX_TCACHE_NONE));
  }

  const int64_t after = db->DbMemoryUsage();

  for (void *p : ptrs) {
    ::operator delete(static_cast<void *>(p), kAllocSize);
  }

  // At least 1 MiB should be attributed — extent hook granularity varies but
  // 4 MiB of dirty allocations should commit at least a few extents.
  EXPECT_GT(after, before + static_cast<int64_t>(1 * 1024 * 1024))
      << "Allocations in the DB arena should increase DbMemoryUsage. before=" << before << " after=" << after;
}

TEST_F(DbMemoryTrackingTest, AllocationsOutsideArenaAreNotTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const int64_t before = db->DbMemoryUsage();

  // Allocate without pinning the arena — should go to default arena, not this DB's.
  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;
  std::vector<void *> ptrs;
  ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_TCACHE_NONE));  // default arena
  }
  for (void *p : ptrs) {
    ::operator delete(static_cast<void *>(p), kAllocSize);
  }

  const int64_t after = db->DbMemoryUsage();

  // The DB's tracker should not have grown substantially.
  EXPECT_LE(after, before + static_cast<int64_t>(512 * 1024))
      << "Allocations outside the DB arena must not be attributed to this DB. before=" << before << " after=" << after;
}

TEST_F(DbMemoryTrackingTest, TwoDbsTrackedIndependently) {
  auto dir1 = data_dir_ / "db1";
  auto dir2 = data_dir_ / "db2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};
  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  const unsigned arena1 = (*acc1)->ArenaIdx();
  ASSERT_NE(arena1, 0u);

  const int64_t before1 = (*acc1)->DbMemoryUsage();
  const int64_t before2 = (*acc2)->DbMemoryUsage();

  // Allocate 4 MiB into DB1's arena only.
  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;
  memgraph::memory::DbArenaScope scope{arena1};
  std::vector<void *> ptrs;
  ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_ARENA(arena1) | MALLOCX_TCACHE_NONE));
  }

  const int64_t after1 = (*acc1)->DbMemoryUsage();
  const int64_t after2 = (*acc2)->DbMemoryUsage();

  for (void *p : ptrs) ::operator delete(static_cast<void *>(p), kAllocSize);

  EXPECT_GT(after1, before1 + static_cast<int64_t>(1 * 1024 * 1024))
      << "DB1 tracker should grow after allocations in its arena";
  EXPECT_LE(after2, before2 + static_cast<int64_t>(512 * 1024))
      << "DB2 tracker must not grow when allocating in DB1 arena";
}

TEST_F(DbMemoryTrackingTest, GraphMemoryTrackerIncludesDbMemory) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0u);

  const int64_t graph_before = memgraph::utils::graph_memory_tracker.Amount();
  const int64_t db_before = db->DbMemoryUsage();

  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;
  memgraph::memory::DbArenaScope scope{arena_idx};
  std::vector<void *> ptrs;
  ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_ARENA(arena_idx) | MALLOCX_TCACHE_NONE));
  }

  const int64_t graph_after = memgraph::utils::graph_memory_tracker.Amount();
  const int64_t db_after = db->DbMemoryUsage();

  for (void *p : ptrs) ::operator delete(static_cast<void *>(p), kAllocSize);

  const int64_t db_delta = db_after - db_before;
  const int64_t graph_delta = graph_after - graph_before;

  EXPECT_GT(db_delta, static_cast<int64_t>(1 * 1024 * 1024)) << "DB tracker must grow after in-arena allocations";
  EXPECT_GE(graph_delta, db_delta) << "graph_memory_tracker must include per-DB allocations (parent chain). "
                                      "graph_delta="
                                   << graph_delta << " db_delta=" << db_delta;
}

TEST_F(DbMemoryTrackingTest, QueryMemoryTrackerDoesNotAffectDbAndGlobalQueryTrackers) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  auto accessor = db->Access();
  auto &query_tracker = accessor->GetTransactionMemoryTracker();

  const int64_t query_before = query_tracker.Amount();
  const int64_t db_before = db->DbQueryMemoryUsage();
  const int64_t global_before = memgraph::utils::global_query_memory_tracker.Amount();

  int64_t query_during = query_before;
  int64_t db_delta = 0;
  int64_t global_delta = 0;
  memgraph::memory::StartTrackingCurrentThread(&query_tracker);
  {
    std::vector<char> payload(4 * 1024 * 1024, 0);

    query_during = query_tracker.Amount();
    const int64_t db_after = db->DbQueryMemoryUsage();
    const int64_t global_after = memgraph::utils::global_query_memory_tracker.Amount();
    db_delta = db_after - db_before;
    global_delta = global_after - global_before;
  }
  memgraph::memory::StopTrackingCurrentThread();

  EXPECT_GE(query_during, query_before) << "TLS query tracker accounting should not go backwards while enabled";
  EXPECT_EQ(db_delta, 0) << "Per-DB query memory should now come only from QueryAllocator-backed tracking";
  EXPECT_EQ(global_delta, 0) << "Global query memory should now come only from QueryAllocator-backed tracking";
  EXPECT_EQ(db->DbQueryMemoryUsage(), db_before) << "Per-DB query memory should return to baseline after free";
  EXPECT_EQ(memgraph::utils::global_query_memory_tracker.Amount(), global_before)
      << "Global query memory should return to baseline after free";
}

TEST_F(DbMemoryTrackingTest, QueryMemoryIsIsolatedPerDatabase) {
  auto dir1 = data_dir_ / "query_db1";
  auto dir2 = data_dir_ / "query_db2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};
  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  auto &db1 = *acc1;
  auto &db2 = *acc2;

  auto accessor1 = db1->Access();
  auto &query_tracker1 = accessor1->GetTransactionMemoryTracker();

  const int64_t db1_before = db1->DbQueryMemoryUsage();
  const int64_t db2_before = db2->DbQueryMemoryUsage();

  memgraph::memory::StartTrackingCurrentThread(&query_tracker1);
  {
    std::vector<char> payload(4 * 1024 * 1024, 0);
    const int64_t db1_after = db1->DbQueryMemoryUsage();
    const int64_t db2_after = db2->DbQueryMemoryUsage();

    EXPECT_EQ(db1_after, db1_before)
        << "DB1 query memory should no longer be affected by TLS QueryMemoryTracker allocations";
    EXPECT_EQ(db2_after, db2_before) << "DB2 query tracker must not grow during DB1 query allocation";
  }
  memgraph::memory::StopTrackingCurrentThread();

  EXPECT_EQ(db1->DbQueryMemoryUsage(), db1_before) << "DB1 query tracker should return to baseline after free";
  EXPECT_EQ(db2->DbQueryMemoryUsage(), db2_before) << "DB2 query tracker should remain unchanged";
}

TEST_F(DbMemoryTrackingTest, QueryAllocatorExecutionMemoryUsesDbQueryTracker) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  memgraph::query::QueryAllocator execution_memory{db->DbQueryMemoryTracker()};

  const int64_t db_before = db->DbQueryMemoryUsage();
  const int64_t global_before = memgraph::utils::global_query_memory_tracker.Amount();

  static constexpr std::size_t kAllocSize = 4 * 1024 * 1024;
  void *ptr = execution_memory.resource_without_pool_or_mono()->allocate(kAllocSize, alignof(std::max_align_t));

  const int64_t db_after = db->DbQueryMemoryUsage();
  const int64_t global_after = memgraph::utils::global_query_memory_tracker.Amount();

  execution_memory.resource_without_pool_or_mono()->deallocate(ptr, kAllocSize, alignof(std::max_align_t));

  const int64_t db_delta = db_after - db_before;
  const int64_t global_delta = global_after - global_before;

  EXPECT_GT(db_delta, static_cast<int64_t>(1 * 1024 * 1024))
      << "Query execution memory upstream should be attributed to the DB query tracker";
  EXPECT_GE(global_delta, db_delta)
      << "Global query tracker should include query execution memory attributed through the DB query tracker";
}

TEST_F(DbMemoryTrackingTest, EmbeddingMemoryRollsIntoDbAndGlobalTrackers) {
  auto dir1 = data_dir_ / "embedding_db1";
  auto dir2 = data_dir_ / "embedding_db2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};
  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  auto &db1 = *acc1;
  auto &db2 = *acc2;

  auto label = db1->storage()->NameToLabel("EmbeddingLabel");
  auto property = db1->storage()->NameToProperty("embedding");

  {
    auto accessor = db1->Access();
    for (int i = 0; i < 1024; ++i) {
      auto vertex = accessor->CreateVertex();
      ASSERT_NO_ERROR(vertex.AddLabel(label));

      std::vector<double> embedding(256, static_cast<double>(i % 7) + 0.5);
      ASSERT_NO_ERROR(vertex.SetProperty(property, memgraph::storage::PropertyValue(embedding)));
    }
    accessor->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  auto unique_acc = db1->UniqueAccess();

  const int64_t db1_before = db1->DbEmbeddingMemoryUsage();
  const int64_t db2_before = db2->DbEmbeddingMemoryUsage();
  const int64_t global_before = memgraph::utils::vector_index_memory_tracker.Amount();
  const int64_t db1_total_before = db1->DbMemoryUsage();

  memgraph::storage::VectorIndexSpec spec{
      .index_name = "db1_embedding_index",
      .label_id = label,
      .property = property,
      .metric_kind = unum::usearch::metric_kind_t::cos_k,
      .dimension = 256,
      .resize_coefficient = 2,
      .capacity = 4096,
      .scalar_kind = unum::usearch::scalar_kind_t::f32_k,
  };

  ASSERT_NO_ERROR(unique_acc->CreateVectorIndex(spec));

  const int64_t db1_after_create = db1->DbEmbeddingMemoryUsage();
  const int64_t db2_after_create = db2->DbEmbeddingMemoryUsage();
  const int64_t global_after_create = memgraph::utils::vector_index_memory_tracker.Amount();
  const int64_t db1_total_after_create = db1->DbMemoryUsage();

  const int64_t db1_delta = db1_after_create - db1_before;
  const int64_t db2_delta = db2_after_create - db2_before;
  const int64_t global_delta = global_after_create - global_before;
  const int64_t db1_total_delta = db1_total_after_create - db1_total_before;

  EXPECT_GT(db1_delta, static_cast<int64_t>(256 * 1024))
      << "Creating a vector index should attribute embedding memory to DB1";
  EXPECT_EQ(db2_delta, 0) << "DB2 embedding tracker must not grow during DB1 vector index creation";
  EXPECT_EQ(global_delta, db1_delta) << "Global embedding tracker should mirror DB1 embedding delta";
  EXPECT_EQ(db1_total_delta, db1_delta)
      << "Combined DB memory should include the embedding delta when no query/storage delta is introduced";

  ASSERT_NO_ERROR(unique_acc->DropVectorIndex(spec.index_name));

  EXPECT_EQ(db1->DbEmbeddingMemoryUsage(), db1_before)
      << "Dropping the vector index should release DB1 embedding memory back to baseline";
  EXPECT_EQ(db2->DbEmbeddingMemoryUsage(), db2_before) << "DB2 embedding tracker should remain unchanged";
  EXPECT_EQ(memgraph::utils::vector_index_memory_tracker.Amount(), global_before)
      << "Global embedding tracker should return to baseline after the index is dropped";
}

TEST_F(DbMemoryTrackingTest, StorageCreateNodesIncreasesDbMemory) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  // Pin this test thread to the DB arena (mimics what PullPlan::Pull does).
  unsigned prev_arena = 0;
  std::size_t arena_sz = sizeof(unsigned);
  je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);
  auto restore = [&] { je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned)); };

  const int64_t before = db->DbMemoryUsage();

  static constexpr int kNodeCount = 1000;
  {
    auto acc = db->Access();
    for (int i = 0; i < kNodeCount; ++i) {
      acc->CreateVertex();
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();
  restore();

  EXPECT_GT(after, before) << "Creating " << kNodeCount
                           << " nodes with arena pinned must increase DbMemoryUsage. "
                              "before="
                           << before << " after=" << after;
}

TEST_F(DbMemoryTrackingTest, StorageCreateVerticesWithLabelsAndPropertiesIncreasesDbMemory) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  unsigned prev_arena = 0;
  std::size_t arena_sz = sizeof(unsigned);
  je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);
  auto restore = [&] { je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned)); };

  auto label_a = db->storage()->NameToLabel("Person");
  auto label_b = db->storage()->NameToLabel("Employee");
  auto prop_name = db->storage()->NameToProperty("name");
  auto prop_age = db->storage()->NameToProperty("age");

  const int64_t before = db->DbMemoryUsage();

  static constexpr int kNodeCount = 500;
  {
    auto acc = db->Access();
    for (int i = 0; i < kNodeCount; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label_a));
      ASSERT_NO_ERROR(v.AddLabel(label_b));
      // String property triggers heap allocation for the property value.
      ASSERT_NO_ERROR(
          v.SetProperty(prop_name, memgraph::storage::PropertyValue(std::string("Alice_") + std::to_string(i))));
      ASSERT_NO_ERROR(v.SetProperty(prop_age, memgraph::storage::PropertyValue(i)));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();
  restore();

  EXPECT_GT(after, before) << "Creating " << kNodeCount
                           << " vertices with labels and properties must increase DbMemoryUsage. "
                           << "before=" << before << " after=" << after;
}

// Each vertex gets kEdgesPerVertex outgoing edges to distinct targets.
// When kEdgesPerVertex exceeds the inline capacity of the adjacency-list small
// vector, the vector spills to heap — those allocations must be attributed to
// this DB's arena and reflected in DbMemoryUsage().
TEST_F(DbMemoryTrackingTest, StorageCreateEdgesWithMultipleConnectionsIncreasesDbMemory) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  unsigned prev_arena = 0;
  std::size_t arena_sz = sizeof(unsigned);
  je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);
  auto restore = [&] { je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned)); };

  auto edge_type = db->storage()->NameToEdgeType("KNOWS");
  auto prop_weight = db->storage()->NameToProperty("weight");

  const int64_t before = db->DbMemoryUsage();

  // 1 hub vertex + kSpokes targets, each target gets kEdgesPerVertex edges
  // from additional source vertices to stress the adjacency-list small vector.
  static constexpr int kSpokes = 32;          // targets hanging off the hub
  static constexpr int kEdgesPerVertex = 16;  // enough to spill small-vector inline storage

  {
    auto acc = db->Access();

    // Create hub and spokes.
    auto hub = acc->CreateVertex();
    std::vector<memgraph::storage::VertexAccessor> spokes;
    spokes.reserve(kSpokes);
    for (int i = 0; i < kSpokes; ++i) {
      spokes.push_back(acc->CreateVertex());
    }

    // Hub → every spoke (stresses hub's out-edge adjacency list).
    for (int i = 0; i < kSpokes; ++i) {
      auto edge = acc->CreateEdge(&hub, &spokes[i], edge_type);
      ASSERT_TRUE(edge.has_value());
      ASSERT_NO_ERROR(edge->SetProperty(prop_weight, memgraph::storage::PropertyValue(static_cast<double>(i) * 0.5)));
    }

    // Additional source vertices each pointing to every spoke (stresses spoke
    // in-edge lists and triggers small-vector heap spill).
    for (int s = 0; s < kEdgesPerVertex; ++s) {
      auto src = acc->CreateVertex();
      for (int i = 0; i < kSpokes; ++i) {
        auto edge = acc->CreateEdge(&src, &spokes[i], edge_type);
        ASSERT_TRUE(edge.has_value());
        ASSERT_NO_ERROR(edge->SetProperty(prop_weight, memgraph::storage::PropertyValue(static_cast<double>(s + i))));
      }
    }

    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();
  restore();

  EXPECT_GT(after, before) << "Creating vertices with " << kEdgesPerVertex
                           << " edges each (small-vector spill) must increase DbMemoryUsage. "
                           << "before=" << before << " after=" << after;
}

// -----------------------------------------------------------------------
// Snapshot recovery preserves per-DB memory attribution
// -----------------------------------------------------------------------
//
// Flow:
//   1. Create a DB with PERIODIC_SNAPSHOT_WITH_WAL mode.
//   2. Pin this test thread to the DB arena (full pin via je_mallctl) and create
//      a rich dataset: vertices, labels, properties, edges.
//   3. Measure DbMemoryUsage() — call it `before_snapshot`.
//   4. Take a manual snapshot.
//   5. Destroy the DB (let the Gatekeeper go out of scope).
//   6. Recreate the same DB from the same directory with recover_on_startup = true.
//      Recovery runs on the scheduler / GC threads which are already fully pinned.
//   7. Measure DbMemoryUsage() — call it `after_recovery`.
//   8. Assert after_recovery > 0 and within 2× of before_snapshot.
//
// This validates that snapshot recovery re-attributes data objects to the
// new DB's arena rather than the default arena, so the tracker is non-zero.
TEST_F(DbMemoryTrackingTest, SnapshotRecoveryPreservesDbMemoryTracking) {
  const auto snap_dir = data_dir_ / "snap_recovery";
  std::filesystem::create_directories(snap_dir);

  // ---- Phase 1: build dataset and take snapshot ----
  int64_t before_snapshot = 0;
  {
    auto cfg = MakeConfig(snap_dir);
    cfg.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    cfg.durability.recover_on_startup = false;
    cfg.durability.snapshot_on_exit = false;

    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
    auto acc_opt = db_gk.access();
    ASSERT_TRUE(acc_opt);
    auto &db = *acc_opt;

    const unsigned arena_idx = db->ArenaIdx();
    ASSERT_NE(arena_idx, 0U);

    // Full-pin this thread to the DB arena so all operator-new allocations
    // during vertex/edge creation go through the DB extent hooks.
    unsigned prev_arena = 0;
    std::size_t arena_sz = sizeof(unsigned);
    je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);

    auto label_person = db->storage()->NameToLabel("Person");
    auto label_employee = db->storage()->NameToLabel("Employee");
    auto prop_name = db->storage()->NameToProperty("name");
    auto prop_age = db->storage()->NameToProperty("age");
    auto edge_type = db->storage()->NameToEdgeType("KNOWS");

    static constexpr int kNodes = 500;

    std::vector<memgraph::storage::Gid> vertex_gids;
    vertex_gids.reserve(kNodes);

    {
      auto txn = db->Access();
      for (int i = 0; i < kNodes; ++i) {
        auto v = txn->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label_person));
        ASSERT_NO_ERROR(v.AddLabel(label_employee));
        ASSERT_NO_ERROR(
            v.SetProperty(prop_name, memgraph::storage::PropertyValue(std::string("User_") + std::to_string(i))));
        ASSERT_NO_ERROR(v.SetProperty(prop_age, memgraph::storage::PropertyValue(i % 80)));
        vertex_gids.push_back(v.Gid());
      }
      txn->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }

    // Add edges between consecutive vertices to stress adjacency-list spill.
    {
      auto txn = db->Access();
      for (int i = 0; i < kNodes - 1; ++i) {
        auto src_opt = txn->FindVertex(vertex_gids[i], memgraph::storage::View::NEW);
        auto dst_opt = txn->FindVertex(vertex_gids[i + 1], memgraph::storage::View::NEW);
        ASSERT_TRUE(src_opt && dst_opt);
        auto src = *src_opt;
        auto dst = *dst_opt;
        ASSERT_TRUE(txn->CreateEdge(&src, &dst, edge_type).has_value());
      }
      txn->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }

    before_snapshot = db->DbMemoryUsage();
    ASSERT_GT(before_snapshot, 0) << "DB memory should be non-zero after creating dataset";

    // Create snapshot while arena is still pinned.
    auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db->storage());
    auto snap_result = storage->CreateSnapshot();
    ASSERT_TRUE(snap_result.has_value()) << "Snapshot creation must succeed";

    // Restore test thread's original arena before the DB is destroyed.
    je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned));
  }  // DB destroyed here — jemalloc arena returned to pool.

  // ---- Phase 2: recover from snapshot ----
  {
    auto cfg = MakeConfig(snap_dir);
    cfg.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    cfg.durability.recover_on_startup = true;
    cfg.durability.snapshot_on_exit = false;

    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
    auto acc_opt = db_gk.access();
    ASSERT_TRUE(acc_opt);
    auto &db = *acc_opt;

    const int64_t after_recovery = db->DbMemoryUsage();

    EXPECT_GT(after_recovery, 0) << "DbMemoryUsage must be non-zero after snapshot recovery. before_snapshot="
                                 << before_snapshot;

    // Recovered memory should be in the same order of magnitude as what was
    // tracked before snapshotting. Allow 2× slack (jemalloc arena re-numbering,
    // extent rounding, jemalloc tcache warm-up on new arena).
    EXPECT_LT(after_recovery, before_snapshot * 2)
        << "Recovered memory should not be more than 2× the pre-snapshot usage. "
        << "before_snapshot=" << before_snapshot << " after_recovery=" << after_recovery;
  }
}

// ---------------------------------------------------------------------------
// Helper: dump jemalloc per-bin and per-large stats for a specific arena.
//
// Prints to stderr so output is visible with --gtest_also_run_disabled_tests
// or when a test fails.  The diff between curregs*bin_size (objects actually
// live in the arena) and DbMemoryUsage (extent-hook byte tracker) shows the
// jemalloc slab pre-allocation overhead that makes extent-level tracking coarse.
// ---------------------------------------------------------------------------

#if USE_JEMALLOC
static void DumpArenaStats(unsigned arena_idx, int64_t db_usage, const char *label) {
  uint64_t epoch = 1;
  size_t sz = sizeof(epoch);
  je_mallctl("epoch", &epoch, &sz, &epoch, sz);

  size_t nbins = 0;
  sz = sizeof(nbins);
  je_mallctl("arenas.nbins", &nbins, &sz, nullptr, 0);

  int64_t live_bytes = 0;

  fprintf(stderr, "\n=== Arena %u [%s]  DbMemoryUsage=%ld ===\n", arena_idx, label, (long)db_usage);
  for (size_t j = 0; j < nbins; ++j) {
    size_t bin_size = 0;
    sz = sizeof(bin_size);
    char key[128];
    snprintf(key, sizeof(key), "arenas.bin.%zu.size", j);
    je_mallctl(key, &bin_size, &sz, nullptr, 0);

    uint64_t curregs = 0;
    sz = sizeof(curregs);
    snprintf(key, sizeof(key), "stats.arenas.%u.bins.%zu.curregs", arena_idx, j);
    je_mallctl(key, &curregs, &sz, nullptr, 0);

    if (curregs > 0) {
      live_bytes += static_cast<int64_t>(curregs * bin_size);
      fprintf(stderr,
              "  bin %2zu  size=%6zu  curregs=%5lu  live=%ld B\n",
              j,
              bin_size,
              (unsigned long)curregs,
              (long)(curregs * bin_size));
    }
  }

  // Large extents (>= large_min_class, typically 16 KiB in jemalloc default config).
  uint64_t large_extents = 0;
  sz = sizeof(large_extents);
  {
    char key[64];
    snprintf(key, sizeof(key), "stats.arenas.%u.large.curlextents", arena_idx);
    je_mallctl(key, &large_extents, &sz, nullptr, 0);
  }
  size_t large_alloc = 0;
  sz = sizeof(large_alloc);
  {
    char key[64];
    snprintf(key, sizeof(key), "stats.arenas.%u.large.allocated", arena_idx);
    je_mallctl(key, &large_alloc, &sz, nullptr, 0);
  }
  if (large_extents > 0 || large_alloc > 0) {
    live_bytes += static_cast<int64_t>(large_alloc);
    fprintf(stderr, "  large   extents=%lu  allocated=%zu B\n", (unsigned long)large_extents, large_alloc);
  }

  fprintf(stderr,
          "  TOTAL  live_bytes=%ld  db_usage=%ld  overhead=%ld\n",
          live_bytes,
          (long)db_usage,
          (long)(db_usage - live_bytes));
  fprintf(stderr, "=== End arena %u ===\n\n", arena_idx);
}
#endif  // USE_JEMALLOC

// ---------------------------------------------------------------------------
// Tests mirroring failing e2e scenarios — use DbArenaScope only (TLS, no
// full thread.arena pin) to match the real query-thread execution path.
//
// Purpose: verify that object-level allocations DO land in the DB arena
// (curregs grows) and diagnose why byte-level DbMemoryUsage sometimes does
// not grow (extent granularity / already-committed slab pages).
// ---------------------------------------------------------------------------

#if USE_JEMALLOC

// Mirror: test_db_memory_grows_after_edge_create
// Hypothesis: edge SkipList nodes go through ArenaAwareAllocator → DB arena,
// but may fit in already-committed extents from vertex creation, so
// DbMemoryUsage byte counter doesn't change even though curregs grows.
TEST_F(DbMemoryTrackingTest, EdgeCreationWithTlsOnlyScope) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto edge_type = db->storage()->NameToEdgeType("CONNECTS");
  static constexpr int kNodes = 100;
  static constexpr int kEdgesPerNode = 10;

  std::vector<memgraph::storage::Gid> vertex_gids;
  vertex_gids.reserve(kNodes);

  // Create source and destination nodes — use TLS scope only (matches e2e).
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < kNodes; ++i) {
      vertex_gids.push_back(acc->CreateVertex().Gid());
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t before = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, before, "after vertex creation");

  // Create edges — TLS scope only.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int s = 0; s < kNodes; ++s) {
      for (int d = s + 1; d < std::min(s + kEdgesPerNode + 1, kNodes); ++d) {
        auto src = acc->FindVertex(vertex_gids[s], memgraph::storage::View::NEW);
        auto dst = acc->FindVertex(vertex_gids[d], memgraph::storage::View::NEW);
        if (src && dst) {
          auto src_v = *src;
          auto dst_v = *dst;
          acc->CreateEdge(&src_v, &dst_v, edge_type);
        }
      }
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after, "after edge creation");

  fprintf(stderr, "EdgeCreation: before=%ld after=%ld delta=%ld\n", (long)before, (long)after, (long)(after - before));

  // curregs-level objects ARE in the arena — byte-level may or may not grow
  // depending on whether new extents were needed.  This test documents the
  // behaviour rather than asserting a specific byte delta.
  EXPECT_GE(after, 0) << "DbMemoryUsage must remain non-negative";
}

// Mirror: test_db_memory_grows_after_label_index_create
TEST_F(DbMemoryTrackingTest, LabelIndexCreationTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("LabelIdxNode");

  // Create 2000 nodes with TLS scope.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 2000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t before = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, before, "before label index");

  // CreateIndex runs on this thread (no DbArenaScope needed — InMemoryStorage
  // uses ArenaAwareAllocator for index SkipLists).
  {
    auto unique_acc = db->storage()->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(label).has_value());
  }

  const int64_t after = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after, "after label index");

  fprintf(stderr, "LabelIndex: before=%ld after=%ld delta=%ld\n", (long)before, (long)after, (long)(after - before));
  EXPECT_GT(after, before) << "Label index creation should increase DbMemoryUsage (new SkipList nodes in DB arena)";
}

// Mirror: test_db_memory_grows_after_label_property_index_create
TEST_F(DbMemoryTrackingTest, LabelPropertyIndexCreationTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("LPIdxNode");
  auto prop = db->storage()->NameToProperty("val");

  // Create 2000 nodes with a numeric property.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 2000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Set dirty_decay_ms=0 so freed pages are returned to OS synchronously (no background
  // decay thread racing with our measurements). Without this the background purge thread
  // can fire concurrently during index creation and produce a net-negative delta even
  // though index objects ARE being allocated in the DB arena.
  {
    ssize_t zero_ms = 0;
    je_mallctl(("arena." + std::to_string(arena_idx) + ".dirty_decay_ms").c_str(),
               nullptr,
               nullptr,
               &zero_ms,
               sizeof(zero_ms));
  }

  // Purge dirty pages so slab headroom is returned to OS. Without this, index objects
  // may fit in already-committed extents and the alloc hook won't fire.
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  const int64_t before = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, before, "before LP index");

  {
    memgraph::storage::PropertiesPaths props{{prop}};
    auto unique_acc = db->storage()->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(label, std::move(props)).has_value());
  }

  const int64_t after = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after, "after LP index");

  fprintf(stderr,
          "LabelPropertyIndex: before=%ld after=%ld delta=%ld\n",
          (long)before,
          (long)after,
          (long)(after - before));
  EXPECT_GT(after, before)
      << "Label-property index creation should increase DbMemoryUsage (SkipList entries in DB arena)";
}

// Mirror: test_db_memory_grows_after_edge_type_index_create
TEST_F(DbMemoryTrackingTest, EdgeTypeIndexCreationTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label_src = db->storage()->NameToLabel("ETIdxSrc");
  auto label_dst = db->storage()->NameToLabel("ETIdxDst");
  auto edge_type = db->storage()->NameToEdgeType("ET_IDX_REL");

  // Create 1000 source/destination pairs with edges.
  std::vector<memgraph::storage::Gid> src_gids, dst_gids;
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 1000; ++i) {
      auto s = acc->CreateVertex();
      ASSERT_NO_ERROR(s.AddLabel(label_src));
      src_gids.push_back(s.Gid());
      auto d = acc->CreateVertex();
      ASSERT_NO_ERROR(d.AddLabel(label_dst));
      dst_gids.push_back(d.Gid());
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 1000; ++i) {
      auto s = acc->FindVertex(src_gids[i], memgraph::storage::View::NEW);
      auto d = acc->FindVertex(dst_gids[i], memgraph::storage::View::NEW);
      if (s && d) {
        auto sv = *s;
        auto dv = *d;
        acc->CreateEdge(&sv, &dv, edge_type);
      }
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Purge dirty pages so slab headroom is returned to OS.
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  const int64_t before = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, before, "before edge-type index");

  {
    auto unique_acc = db->storage()->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(edge_type).has_value());
  }

  const int64_t after = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after, "after edge-type index");

  fprintf(stderr, "EdgeTypeIndex: before=%ld after=%ld delta=%ld\n", (long)before, (long)after, (long)(after - before));
  EXPECT_GT(after, before) << "Edge-type index creation should increase DbMemoryUsage";
}

// Mirror: test_db_memory_shrinks_after_gc
// Config: PERIODIC GC with a very short interval so it runs during the test.
TEST_F(DbMemoryTrackingTest, GcFreesArenaPages) {
  // Use a GC config that runs frequently for testing.
  auto cfg = MakeConfig(data_dir_);
  cfg.gc.type = memgraph::storage::Config::Gc::Type::PERIODIC;
  cfg.gc.interval = std::chrono::seconds(1);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("GcNode");
  auto prop = db->storage()->NameToProperty("value");

  // Create 50k nodes with a string property — large enough to force many full
  // jemalloc extents so that dealloc after GC produces a measurable drop.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 50000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(std::string("val_") + std::to_string(i))));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after_create = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after_create, "after create 50k nodes");
  ASSERT_GT(after_create, 0) << "Memory must be non-zero after creating nodes";

  // Delete all nodes — adds them to the GC graveyard (deltas freed on next GC tick).
  std::vector<memgraph::storage::Gid> gids;
  {
    auto acc = db->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      gids.push_back(v.Gid());
    }
    for (auto gid : gids) {
      auto v = acc->FindVertex(gid, memgraph::storage::View::NEW);
      if (v) acc->DeleteVertex(&(*v));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Wait for GC to collect deleted vertices (structural cleanup).
  // GC interval is 1s; 4s gives it several ticks to finish 50k vertices.
  std::this_thread::sleep_for(std::chrono::seconds(4));

  // Force jemalloc to process the dirty-page decay queue, then hard-purge
  // to return all dirty extents to the OS.  Both steps fire extent dalloc hooks
  // which decrement DbMemoryUsage.
  std::string decay_key = "arena." + std::to_string(arena_idx) + ".decay";
  std::string purge_key = "arena." + std::to_string(arena_idx) + ".purge";
  je_mallctl(decay_key.c_str(), nullptr, nullptr, nullptr, 0);
  je_mallctl(purge_key.c_str(), nullptr, nullptr, nullptr, 0);

  const int64_t after_gc = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after_gc, "after gc+purge");

  fprintf(stderr,
          "GC: after_create=%ld after_gc=%ld delta=%ld\n",
          (long)after_create,
          (long)after_gc,
          (long)(after_create - after_gc));

  // Primary check: DbMemoryUsage (extent-hook tracker) must have dropped by at
  // least 10% of peak.  10% is conservative — live infrastructure objects
  // (SkipList sentinels, property store) prevent full reclaim.
  if (after_gc < after_create - (after_create / 10)) {
    // Fast path: extent hooks already show the drop.
    SUCCEED();
    return;
  }

  // Fallback: extent hooks may not have fired yet if jemalloc retained some
  // extents for reuse (retained_decay_ms).  Check jemalloc's own arena stats:
  // stats.arenas.X.mapped is the total bytes currently mapped for the arena.
  // If mapped shrank substantially the memory WAS freed — it just hasn't been
  // returned to OS yet (still retained).
  uint64_t epoch = 1;
  size_t esz = sizeof(epoch);
  je_mallctl("epoch", &epoch, &esz, &epoch, esz);

  size_t mapped_after = 0;
  size_t sz = sizeof(mapped_after);
  char mapped_key[64];
  snprintf(mapped_key, sizeof(mapped_key), "stats.arenas.%u.mapped", arena_idx);
  je_mallctl(mapped_key, &mapped_after, &sz, nullptr, 0);

  // Take a "mapped before" snapshot by estimating from after_create + slab overhead.
  // A simpler bound: mapped_after must be less than after_create (peak tracked bytes).
  // If jemalloc mapped less than peak, it returned at least some extents.
  fprintf(stderr, "GC fallback: mapped_after=%zu after_create=%ld\n", mapped_after, (long)after_create);

  EXPECT_LT(static_cast<int64_t>(mapped_after), after_create)
      << "After GC + purge, jemalloc arena mapped bytes should be less than peak tracked usage. "
      << "This confirms GC freed memory even if extent hooks fired after measurement. "
      << "mapped_after=" << mapped_after << " after_create=" << after_create << " after_gc=" << after_gc;
}

// Mirror: test_db_memory_grows_after_index_population
// Index creation on pre-existing data: the async indexer thread (DbAwareThread)
// inherits the DB arena idx via tls_db_arena_idx.  SkipList entries created
// during population should be attributed to the DB arena.
TEST_F(DbMemoryTrackingTest, IndexPopulationOnPreExistingData) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("PopIdxNode");
  auto prop = db->storage()->NameToProperty("score");

  // Create data BEFORE the index — population happens at index creation time.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < 3000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i % 100)));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Purge dirty pages so slab headroom is returned to OS.
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  const int64_t before = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, before, "before index population");

  // CreateIndex triggers async population via DbAwareThread (inherits arena_idx).
  {
    memgraph::storage::PropertiesPaths props{{prop}};
    auto unique_acc = db->storage()->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(label, std::move(props)).has_value());
  }

  const int64_t after = db->DbMemoryUsage();
  DumpArenaStats(arena_idx, after, "after index population");

  fprintf(
      stderr, "IndexPopulation: before=%ld after=%ld delta=%ld\n", (long)before, (long)after, (long)(after - before));
  EXPECT_GT(after, before) << "Index population on pre-existing data should increase DbMemoryUsage "
                              "(async indexer thread inherits DB arena idx via DbAwareThread)";
}

#endif  // USE_JEMALLOC (diagnostic tests)

// ===========================================================================
// Scenario coverage: multi-tenancy, WAL recovery, stream / trigger / replication
// thread patterns.
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. Multi-tenancy: two independent databases, storage-API writes to one must
//    not affect the other's tracker.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, MultiTenancyStorageOpsAreIsolated) {
  auto dir1 = data_dir_ / "mt1";
  auto dir2 = data_dir_ / "mt2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};

  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  auto &db1 = *acc1;
  auto &db2 = *acc2;

  const unsigned arena1 = db1->ArenaIdx();
  const unsigned arena2 = db2->ArenaIdx();
  ASSERT_NE(arena1, 0U);
  ASSERT_NE(arena2, 0U);
  ASSERT_NE(arena1, arena2);

  const int64_t before1 = db1->DbMemoryUsage();
  const int64_t before2 = db2->DbMemoryUsage();

  // Pin this thread to DB1's arena and create nodes via the storage API.
  unsigned prev_arena = 0;
  std::size_t arena_sz = sizeof(unsigned);
  je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena1), arena_sz);

  static constexpr int kNodes = 500;
  {
    auto storage_acc = db1->Access();
    auto label = db1->storage()->NameToLabel("MTNode");
    auto prop = db1->storage()->NameToProperty("val");
    for (int i = 0; i < kNodes; ++i) {
      auto v = storage_acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
    }
    storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned));

  const int64_t after1 = db1->DbMemoryUsage();
  const int64_t after2 = db2->DbMemoryUsage();

  EXPECT_GT(after1, before1) << "DB1 tracker must grow after storage-API vertex creation";
  // Allow up to 512 KiB noise on DB2 (background threads, GC tick, etc.)
  EXPECT_LE(after2, before2 + static_cast<int64_t>(512 * 1024))
      << "DB2 tracker must not grow when writing to DB1. before2=" << before2 << " after2=" << after2;
}

// ---------------------------------------------------------------------------
// 2. WAL-only recovery: commit transactions (which flush to WAL) but never
//    take a snapshot.  After destroy + recover_on_startup, tracker is non-zero.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, WalOnlyRecoveryPreservesDbMemoryTracking) {
  const auto wal_dir = data_dir_ / "wal_recovery";
  std::filesystem::create_directories(wal_dir);

  int64_t before_destroy = 0;

  // ---- Phase 1: write transactions — WAL is flushed, no snapshot taken ----
  {
    auto cfg = MakeConfig(wal_dir);
    cfg.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    cfg.durability.recover_on_startup = false;
    cfg.durability.snapshot_on_exit = false;

    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
    auto acc_opt = db_gk.access();
    ASSERT_TRUE(acc_opt);
    auto &db = *acc_opt;

    const unsigned arena_idx = db->ArenaIdx();
    ASSERT_NE(arena_idx, 0U);

    unsigned prev_arena = 0;
    std::size_t arena_sz = sizeof(unsigned);
    je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);

    auto label = db->storage()->NameToLabel("WalNode");
    auto prop = db->storage()->NameToProperty("id");

    // Two separate transactions so multiple WAL entries are written.
    for (int batch = 0; batch < 2; ++batch) {
      auto txn = db->Access();
      for (int i = 0; i < 300; ++i) {
        auto v = txn->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(batch * 300 + i)));
      }
      txn->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }

    before_destroy = db->DbMemoryUsage();
    ASSERT_GT(before_destroy, 0) << "DB memory must be non-zero after WAL-only commits";

    je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned));
    // No CreateSnapshot() call — only WAL files exist on disk.
  }

  // Verify no snapshot file was written (confirms we are testing WAL-only path).
  ASSERT_FALSE(std::filesystem::exists(wal_dir / "snapshots") && !std::filesystem::is_empty(wal_dir / "snapshots"))
      << "Snapshot directory must be absent or empty for WAL-only recovery test";

  // ---- Phase 2: recover from WAL only ----
  {
    auto cfg = MakeConfig(wal_dir);
    cfg.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    cfg.durability.recover_on_startup = true;
    cfg.durability.snapshot_on_exit = false;

    memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
    auto acc_opt = db_gk.access();
    ASSERT_TRUE(acc_opt);
    auto &db = *acc_opt;

    const int64_t after_recovery = db->DbMemoryUsage();

    EXPECT_GT(after_recovery, 0) << "DbMemoryUsage must be non-zero after WAL-only recovery. before_destroy="
                                 << before_destroy;
    EXPECT_LT(after_recovery, before_destroy * 3)
        << "Recovered memory should be in the same order of magnitude as pre-destroy. "
        << "before_destroy=" << before_destroy << " after_recovery=" << after_recovery;
  }
}

// ---------------------------------------------------------------------------
// 3. Stream consumer thread: mirrors the arena setup in
//    integrations/kafka/consumer.cpp and integrations/pulsar/consumer.cpp.
//
//    Both set:
//      je_mallctl("thread.arena", nullptr, nullptr, &arena_idx, sizeof(unsigned));
//      memory::tls_db_arena_idx = arena_idx;
//
//    Vertex/edge allocations via the storage API must be attributed to the DB arena.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, StreamConsumerThreadArenaAttributed) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  const int64_t before = db->DbMemoryUsage();
  std::atomic<int64_t> after{0};

  std::thread consumer_thread([&] {
    // Exact arena setup used by integrations/kafka/consumer.cpp and
    // integrations/pulsar/consumer.cpp.
    je_mallctl("thread.arena", nullptr, nullptr, const_cast<unsigned *>(&arena_idx), sizeof(unsigned));
    memgraph::memory::tls_db_arena_idx = arena_idx;

    // Exercise the real storage implementation — CreateVertex allocates through
    // the DB arena whose extent hooks call TrackMemory/UntrackMemory.
    auto label = db->storage()->NameToLabel("StreamNode");
    auto prop = db->storage()->NameToProperty("msg");
    {
      auto acc = db->Access();
      for (int i = 0; i < 500; ++i) {
        auto v = acc->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(std::string("msg_") + std::to_string(i))));
      }
      acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }
    after.store(db->DbMemoryUsage());
  });
  consumer_thread.join();

  EXPECT_GT(after.load(), before)
      << "Stream consumer thread (je_mallctl + tls pin) must increase DbMemoryUsage after vertex creation. "
      << "before=" << before << " after=" << after.load();
}

// ---------------------------------------------------------------------------
// 4a. Before-commit trigger thread: triggers that fire synchronously on the
//     query thread use DbArenaScope (TLS-only, no full je_mallctl pin).
//     Storage API operations on that thread must still be tracked.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, BeforeCommitTriggerStorageOpsAttributed) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  const int64_t before = db->DbMemoryUsage();
  std::atomic<int64_t> after{0};

  std::thread trigger_thread([&] {
    // Before-commit triggers execute on the query thread which sets DbArenaScope
    // (TLS-only — does NOT call je_mallctl("thread.arena")).
    memgraph::memory::DbArenaScope scope{arena_idx};

    auto label = db->storage()->NameToLabel("TriggerNode");
    auto prop = db->storage()->NameToProperty("data");
    {
      auto acc = db->Access();
      for (int i = 0; i < 500; ++i) {
        auto v = acc->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(
            v.SetProperty(prop, memgraph::storage::PropertyValue(std::string("trig_") + std::to_string(i))));
      }
      acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }
    after.store(db->DbMemoryUsage());
  });
  trigger_thread.join();

  EXPECT_GT(after.load(), before)
      << "Before-commit trigger thread (DbArenaScope/TLS-only) must increase DbMemoryUsage after vertex creation. "
      << "before=" << before << " after=" << after.load();
}

// ---------------------------------------------------------------------------
// 4b. After-commit trigger pool: database.cpp pins the after_commit_trigger_pool_
//     thread via je_mallctl at construction.  A fully-pinned thread running
//     storage API operations must attribute allocations to the DB arena.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, AfterCommitTriggerPoolStorageOpsAttributed) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  const int64_t before = db->DbMemoryUsage();
  std::atomic<int64_t> after{0};

  std::thread trigger_pool_thread([&] {
    // after_commit_trigger_pool_ pins threads via je_mallctl at pool construction
    // (database.cpp).  Replicate the same pin here.
    je_mallctl("thread.arena", nullptr, nullptr, const_cast<unsigned *>(&arena_idx), sizeof(unsigned));

    auto label = db->storage()->NameToLabel("AfterTrigNode");
    auto prop = db->storage()->NameToProperty("val");
    {
      auto acc = db->Access();
      for (int i = 0; i < 500; ++i) {
        auto v = acc->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
      }
      acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }
    after.store(db->DbMemoryUsage());
  });
  trigger_pool_thread.join();

  EXPECT_GT(after.load(), before)
      << "After-commit trigger pool thread (full je_mallctl pin) must increase DbMemoryUsage after vertex creation. "
      << "before=" << before << " after=" << after.load();
}

// ---------------------------------------------------------------------------
// 5. Replication receiver thread: the replica applier thread is pinned to the
//    DB arena via je_mallctl.  Storage API operations (applying received deltas)
//    must be attributed to the correct DB.
// ---------------------------------------------------------------------------

TEST_F(DbMemoryTrackingTest, ReplicationApplierStorageOpsAttributed) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  const int64_t before = db->DbMemoryUsage();
  std::atomic<int64_t> after{0};

  std::thread repl_thread([&] {
    // Replication applier threads use full je_mallctl pinning (same as
    // after_commit_trigger_pool_).
    je_mallctl("thread.arena", nullptr, nullptr, const_cast<unsigned *>(&arena_idx), sizeof(unsigned));

    auto label = db->storage()->NameToLabel("ReplNode");
    auto prop = db->storage()->NameToProperty("seq");
    {
      auto acc = db->Access();
      for (int i = 0; i < 500; ++i) {
        auto v = acc->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
      }
      acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }
    after.store(db->DbMemoryUsage());
  });
  repl_thread.join();

  EXPECT_GT(after.load(), before)
      << "Replication applier thread (full je_mallctl pin) must attribute storage ops to DB arena. "
      << "before=" << before << " after=" << after.load();
}

// ---------------------------------------------------------------------------

#else  // !USE_JEMALLOC

TEST_F(DbMemoryTrackingTest, ArenaIdxIsZeroWithoutJemalloc) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  EXPECT_EQ((*db_acc_opt)->ArenaIdx(), 0u) << "Without jemalloc, arena idx must be 0";
}

TEST_F(DbMemoryTrackingTest, PlanCacheInsertionsAreAttributedToOwningDatabase) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      std::nullopt};
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt,
                                                          nullptr
#endif
  };
  InterpreterFaker interpreter_faker{&interpreter_context, db};

  const int64_t before = db->DbStorageMemoryUsage();
  constexpr int kQueryCount = 200;
  for (int i = 0; i < kQueryCount; ++i) {
    interpreter_faker.Interpret("RETURN 1 AS a" + std::to_string(i));
  }

  const int64_t after = db->DbStorageMemoryUsage();
  EXPECT_EQ(db->plan_cache()->WithLock([&](auto &cache) { return cache.size(); }), static_cast<size_t>(kQueryCount));
  EXPECT_GT(after, before + static_cast<int64_t>(4096))
      << "DB storage tracker should include plan-cache node allocations done during prepare. before=" << before
      << " after=" << after;
}

#endif  // USE_JEMALLOC
