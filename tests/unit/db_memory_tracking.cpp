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
#include <thread>
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

// ---------------------------------------------------------------------------
// 1. Arena basics: each DB gets a unique non-zero arena index.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ArenaBasics) {
  auto dir1 = data_dir_ / "db1";
  auto dir2 = data_dir_ / "db2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};
  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);

  EXPECT_NE((*acc1)->ArenaIdx(), 0u) << "Each Database should own a unique jemalloc arena";
  EXPECT_NE((*acc2)->ArenaIdx(), 0u);
  EXPECT_NE((*acc1)->ArenaIdx(), (*acc2)->ArenaIdx()) << "Two databases must not share a jemalloc arena";
}

// ---------------------------------------------------------------------------
// 2. Arena isolation: allocating in DB1's arena grows DB1's tracker,
//    doesn't affect DB2, and propagates to total_memory_tracker.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ArenaIsolationAndParentPropagation) {
  auto dir1 = data_dir_ / "iso_db1";
  auto dir2 = data_dir_ / "iso_db2";
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
  ASSERT_NE(arena1, 0u);

  const int64_t before1 = db1->DbMemoryUsage();
  const int64_t before2 = db2->DbMemoryUsage();
  const int64_t total_before = memgraph::utils::total_memory_tracker.Amount();

  // Allocate 4 MiB into DB1's arena.
  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;
  memgraph::memory::DbArenaScope scope{arena1};
  std::vector<void *> ptrs;
  ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_ARENA(arena1) | MALLOCX_TCACHE_NONE));
  }

  const int64_t after1 = db1->DbMemoryUsage();
  const int64_t after2 = db2->DbMemoryUsage();
  const int64_t total_after = memgraph::utils::total_memory_tracker.Amount();

  for (void *p : ptrs) ::operator delete(static_cast<void *>(p), kAllocSize);

  const int64_t db1_delta = after1 - before1;
  const int64_t total_delta = total_after - total_before;

  EXPECT_GT(db1_delta, static_cast<int64_t>(1024 * 1024)) << "DB1 tracker must grow after arena allocations";
  EXPECT_LE(after2, before2 + static_cast<int64_t>(512 * 1024)) << "DB2 tracker must not grow when allocating in DB1";
  EXPECT_GE(total_delta, db1_delta) << "total_memory_tracker must include per-DB allocations via parent chain";

  // Allocations outside any DB arena should not affect either DB.
  const int64_t db1_mid = db1->DbMemoryUsage();
  std::vector<void *> default_ptrs;
  default_ptrs.reserve(kAllocCount);
  for (std::size_t i = 0; i < kAllocCount; ++i) {
    default_ptrs.push_back(je_mallocx(kAllocSize, MALLOCX_TCACHE_NONE));
  }
  EXPECT_LE(db1->DbMemoryUsage(), db1_mid + static_cast<int64_t>(512 * 1024))
      << "Default-arena allocations must not be attributed to any DB";
  for (void *p : default_ptrs) ::operator delete(static_cast<void *>(p), kAllocSize);
}

// ---------------------------------------------------------------------------
// 3. Query memory: TLS QueryMemoryTracker doesn't affect per-DB query tracker,
//    and QueryAllocator PMR does.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, QueryMemoryTracking) {
  auto dir1 = data_dir_ / "qdb1";
  auto dir2 = data_dir_ / "qdb2";
  std::filesystem::create_directories(dir1);
  std::filesystem::create_directories(dir2);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk1{MakeConfig(dir1)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk2{MakeConfig(dir2)};
  auto acc1 = db_gk1.access();
  auto acc2 = db_gk2.access();
  ASSERT_TRUE(acc1 && acc2);
  auto &db1 = *acc1;
  auto &db2 = *acc2;

  // TLS query tracking should NOT affect per-DB query tracker.
  auto accessor = db1->Access();
  auto &query_tracker = accessor->GetTransactionMemoryTracker();

  const int64_t db1_q_before = db1->DbQueryMemoryUsage();
  const int64_t db2_q_before = db2->DbQueryMemoryUsage();

  memgraph::memory::StartTrackingCurrentThread(&query_tracker);
  {
    std::vector<char> payload(4 * 1024 * 1024, 0);
  }
  memgraph::memory::StopTrackingCurrentThread();

  EXPECT_EQ(db1->DbQueryMemoryUsage(), db1_q_before)
      << "TLS query tracker allocations should not affect per-DB query tracker";
  EXPECT_EQ(db2->DbQueryMemoryUsage(), db2_q_before) << "DB2 query tracker must remain unchanged";

  // QueryAllocator PMR SHOULD affect per-DB query tracker.
  memgraph::query::QueryAllocator execution_memory{db1->DbQueryMemoryTracker()};
  const int64_t pmr_before = db1->DbQueryMemoryUsage();
  const int64_t total_before = memgraph::utils::total_memory_tracker.Amount();

  static constexpr std::size_t kAllocSize = 4 * 1024 * 1024;
  void *ptr = execution_memory.resource_without_pool_or_mono()->allocate(kAllocSize, alignof(std::max_align_t));

  const int64_t pmr_after = db1->DbQueryMemoryUsage();
  const int64_t total_after = memgraph::utils::total_memory_tracker.Amount();

  execution_memory.resource_without_pool_or_mono()->deallocate(ptr, kAllocSize, alignof(std::max_align_t));

  EXPECT_GT(pmr_after - pmr_before, static_cast<int64_t>(1024 * 1024))
      << "QueryAllocator PMR should attribute memory to per-DB query tracker";
  EXPECT_GE(total_after - total_before, pmr_after - pmr_before)
      << "total_memory_tracker should include query memory via parent chain";
}

// ---------------------------------------------------------------------------
// 4. Embedding memory: TrackedVectorAllocator feeds db_embedding_memory_tracker_
//    and propagates to total. Per-DB isolation between two databases.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, EmbeddingMemoryTracking) {
  auto dir1 = data_dir_ / "emb_db1";
  auto dir2 = data_dir_ / "emb_db2";
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

  const int64_t db1_delta = db1->DbEmbeddingMemoryUsage() - db1_before;
  const int64_t db1_total_delta = db1->DbMemoryUsage() - db1_total_before;

  EXPECT_GT(db1_delta, static_cast<int64_t>(256 * 1024)) << "Vector index should attribute embedding memory to DB1";
  EXPECT_EQ(db2->DbEmbeddingMemoryUsage(), db2_before) << "DB2 embedding tracker must not grow";
  EXPECT_EQ(db1_total_delta, db1_delta) << "db_total should include embedding delta";

  ASSERT_NO_ERROR(unique_acc->DropVectorIndex(spec.index_name));
  EXPECT_EQ(db1->DbEmbeddingMemoryUsage(), db1_before) << "Dropping index should release embedding memory";
}

// ---------------------------------------------------------------------------
// 5. Storage ops: creating vertices with labels, properties, and edges
//    increases DbMemoryUsage.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, StorageOpsIncreaseDbMemory) {
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

  auto label = db->storage()->NameToLabel("Person");
  auto prop_name = db->storage()->NameToProperty("name");
  auto edge_type = db->storage()->NameToEdgeType("KNOWS");

  const int64_t before = db->DbMemoryUsage();

  // Create vertices with labels + properties, then connect them with edges.
  {
    auto acc = db->Access();
    std::vector<memgraph::storage::VertexAccessor> vertices;
    vertices.reserve(500);
    for (int i = 0; i < 500; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(
          v.SetProperty(prop_name, memgraph::storage::PropertyValue(std::string("Person_") + std::to_string(i))));
      vertices.push_back(v);
    }
    // Create 128 edges per vertex to force small_vector heap spill.
    for (int i = 0; i < 500; ++i) {
      for (int j = 1; j <= 128 && i + j < 500; ++j) {
        auto edge = acc->CreateEdge(&vertices[i], &vertices[(i + j) % 500], edge_type);
        ASSERT_TRUE(edge.has_value());
      }
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();
  restore();

  EXPECT_GT(after, before + static_cast<int64_t>(1024 * 1024))
      << "Creating vertices with labels, properties, and edges must increase DbMemoryUsage";
}

// ---------------------------------------------------------------------------
// 6. Index creation: label, label-property, and edge-type indices all
//    increase DbMemoryUsage when populated.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, IndexCreationTracked) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("IdxNode");
  auto prop = db->storage()->NameToProperty("val");
  auto edge_type = db->storage()->NameToEdgeType("IDX_REL");

  // Create 2000 nodes with label+property, and 1000 edges.
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    std::vector<memgraph::storage::VertexAccessor> verts;
    verts.reserve(2000);
    for (int i = 0; i < 2000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
      verts.push_back(v);
    }
    for (int i = 0; i < 1000; ++i) {
      acc->CreateEdge(&verts[i], &verts[i + 1000], edge_type);
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Purge so baseline is stable.
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  // Label index
  const int64_t before_label = db->DbMemoryUsage();
  {
    auto ua = db->storage()->UniqueAccess();
    ASSERT_TRUE(ua->CreateIndex(label).has_value());
  }
  EXPECT_GT(db->DbMemoryUsage(), before_label) << "Label index creation should increase DbMemoryUsage";

  // Label-property index
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);
  const int64_t before_lp = db->DbMemoryUsage();
  {
    memgraph::storage::PropertiesPaths props{{prop}};
    auto ua = db->storage()->UniqueAccess();
    ASSERT_TRUE(ua->CreateIndex(label, std::move(props)).has_value());
  }
  EXPECT_GT(db->DbMemoryUsage(), before_lp) << "Label-property index creation should increase DbMemoryUsage";

  // Edge-type index
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);
  const int64_t before_et = db->DbMemoryUsage();
  {
    auto ua = db->storage()->UniqueAccess();
    ASSERT_TRUE(ua->CreateIndex(edge_type).has_value());
  }
  EXPECT_GT(db->DbMemoryUsage(), before_et) << "Edge-type index creation should increase DbMemoryUsage";
}

// ---------------------------------------------------------------------------
// 7. Thread pinning patterns: different arena-pinning methods all attribute
//    allocations to the correct DB.
//    - je_mallctl("thread.arena") + tls  (stream consumer / replication applier)
//    - DbArenaScope (before-commit trigger / query thread)
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ThreadPinningPatternsAttributed) {
  memgraph::memory::ArenaPool::Instance().Drain();
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->ArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto create_vertices = [&](const char *label_name) {
    auto lbl = db->storage()->NameToLabel(label_name);
    auto prp = db->storage()->NameToProperty("data");
    auto acc = db->Access();
    for (int i = 0; i < 5000; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(lbl));
      ASSERT_NO_ERROR(v.SetProperty(prp, memgraph::storage::PropertyValue(std::string("v_") + std::to_string(i))));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  };

  // Full pin: je_mallctl("thread.arena") + tls_db_arena_idx
  // (mirrors stream consumer, replication applier, after-commit trigger pool)
  {
    const int64_t before = db->DbMemoryUsage();
    std::atomic<int64_t> after{0};
    std::thread t([&] {
      je_mallctl("thread.arena", nullptr, nullptr, const_cast<unsigned *>(&arena_idx), sizeof(unsigned));
      memgraph::memory::tls_db_arena_idx = arena_idx;
      create_vertices("FullPinNode");
      after.store(db->DbMemoryUsage());
    });
    t.join();
    EXPECT_GT(after.load(), before) << "Full je_mallctl+tls pin must attribute allocations to DB";
  }

  // TLS-only pin: DbArenaScope (mirrors before-commit trigger / query thread)
  {
    const int64_t before = db->DbMemoryUsage();
    std::atomic<int64_t> after{0};
    std::thread t([&] {
      memgraph::memory::DbArenaScope scope{arena_idx};
      create_vertices("TlsPinNode");
      after.store(db->DbMemoryUsage());
    });
    t.join();
    EXPECT_GT(after.load(), before) << "DbArenaScope (TLS-only) pin must attribute allocations to DB";
  }
}

// ---------------------------------------------------------------------------
// 8. Snapshot recovery preserves per-DB memory attribution.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, SnapshotRecoveryPreservesDbMemoryTracking) {
  const auto snap_dir = data_dir_ / "snap_recovery";
  std::filesystem::create_directories(snap_dir);

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

    unsigned prev_arena = 0;
    std::size_t arena_sz = sizeof(unsigned);
    je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);

    auto label = db->storage()->NameToLabel("SnapNode");
    auto prop = db->storage()->NameToProperty("id");
    auto edge_type = db->storage()->NameToEdgeType("SNAP_EDGE");

    {
      auto txn = db->Access();
      for (int i = 0; i < 500; ++i) {
        auto v = txn->CreateVertex();
        ASSERT_NO_ERROR(v.AddLabel(label));
        ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(i)));
      }
      txn->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }

    {
      auto acc = db->Access();
      auto vertices_iterable = acc->Vertices(memgraph::storage::View::NEW);
      auto it = vertices_iterable.begin();
      for (int i = 0; i < 100 && it != vertices_iterable.end(); ++i) {
        auto src = *it;
        ++it;
        if (it == vertices_iterable.end()) break;
        auto dst = *it;
        acc->CreateEdge(&src, &dst, edge_type);
      }
      acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    }

    before_snapshot = db->DbMemoryUsage();
    ASSERT_GT(before_snapshot, 0);

    auto *storage = dynamic_cast<memgraph::storage::InMemoryStorage *>(db->storage());
    ASSERT_NE(storage, nullptr);
    storage->CreateSnapshot(true);

    je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned));
  }

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
    EXPECT_GT(after_recovery, 0) << "DbMemoryUsage must be non-zero after snapshot recovery";
    EXPECT_LT(after_recovery, before_snapshot * 3)
        << "Recovered memory should be same order of magnitude as pre-snapshot";
  }
}

// ---------------------------------------------------------------------------
// 9. WAL-only recovery preserves per-DB memory attribution.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, WalOnlyRecoveryPreservesDbMemoryTracking) {
  const auto wal_dir = data_dir_ / "wal_recovery";
  std::filesystem::create_directories(wal_dir);

  int64_t before_destroy = 0;
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
    unsigned prev_arena = 0;
    std::size_t arena_sz = sizeof(unsigned);
    je_mallctl("thread.arena", &prev_arena, &arena_sz, const_cast<unsigned *>(&arena_idx), arena_sz);

    auto label = db->storage()->NameToLabel("WalNode");
    auto prop = db->storage()->NameToProperty("id");

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
    ASSERT_GT(before_destroy, 0);
    je_mallctl("thread.arena", nullptr, nullptr, &prev_arena, sizeof(unsigned));
  }

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
    EXPECT_GT(after_recovery, 0) << "DbMemoryUsage must be non-zero after WAL-only recovery";
    EXPECT_LT(after_recovery, before_destroy * 3) << "Recovered memory should be same order of magnitude";
  }
}

// ---------------------------------------------------------------------------
// 10. GC frees arena pages: after deleting all nodes and running GC + purge,
//     DbMemoryUsage must drop substantially.
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, GcFreesArenaPages) {
  // Drain recycled arena indices so this test gets a fresh jemalloc arena.
  memgraph::memory::ArenaPool::Instance().Drain();

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

  const int64_t baseline = db->DbMemoryUsage();

  // Create 100k nodes with string properties to force many committed extents.
  static constexpr int kGcNodes = 100000;
  {
    memgraph::memory::DbArenaScope scope{arena_idx};
    auto acc = db->Access();
    for (int i = 0; i < kGcNodes; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_NO_ERROR(v.AddLabel(label));
      ASSERT_NO_ERROR(v.SetProperty(prop, memgraph::storage::PropertyValue(std::string("val_") + std::to_string(i))));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after_create = db->DbMemoryUsage();
  ASSERT_GT(after_create, baseline + static_cast<int64_t>(1024 * 1024));

  // Delete all nodes.
  {
    auto acc = db->Access();
    std::vector<memgraph::storage::Gid> gids;
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) gids.push_back(v.Gid());
    for (auto gid : gids) {
      auto v = acc->FindVertex(gid, memgraph::storage::View::NEW);
      if (v) acc->DeleteVertex(&(*v));
    }
    acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  // Wait for GC (interval=1s, give it 4 ticks).
  std::this_thread::sleep_for(std::chrono::seconds(4));

  je_mallctl(("arena." + std::to_string(arena_idx) + ".decay").c_str(), nullptr, nullptr, nullptr, 0);
  je_mallctl(("arena." + std::to_string(arena_idx) + ".purge").c_str(), nullptr, nullptr, nullptr, 0);

  const int64_t after_gc = db->DbMemoryUsage();
  const int64_t growth = after_create - baseline;

  // After GC + purge, at most 25% of growth should remain (slab fragmentation).
  static constexpr double kMaxResidualFraction = 0.25;
  const auto max_after_gc = baseline + static_cast<int64_t>(static_cast<double>(growth) * kMaxResidualFraction);

  EXPECT_LT(after_gc, max_after_gc) << "After GC + purge, at most " << (kMaxResidualFraction * 100)
                                    << "% of growth should remain. "
                                    << "baseline=" << baseline << " after_create=" << after_create
                                    << " after_gc=" << after_gc;
}

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
                                                          system_state,
                                                          nullptr
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
