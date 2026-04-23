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
#include <cstring>
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
#include "storage/v2/indices/vector_edge_index.hpp"
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

#if USE_JEMALLOC
unsigned JemallocArenaCount() {
  unsigned count = 0;
  size_t count_size = sizeof(count);
  EXPECT_EQ(je_mallctl("arenas.narenas", &count, &count_size, nullptr, 0), 0);
  return count;
}
#endif

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

  EXPECT_NE((*acc1)->BaseArenaIdx(), 0u) << "Each Database should own a unique jemalloc arena";
  EXPECT_NE((*acc2)->BaseArenaIdx(), 0u);
  EXPECT_NE((*acc1)->BaseArenaIdx(), (*acc2)->BaseArenaIdx()) << "Two databases must not share a jemalloc arena";
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

  const unsigned arena1 = db1->BaseArenaIdx();
  ASSERT_NE(arena1, 0u);

  const int64_t before1 = db1->DbMemoryUsage();
  const int64_t before2 = db2->DbMemoryUsage();
  const int64_t total_before = memgraph::utils::total_memory_tracker.Amount();

  // Allocate 4 MiB into DB1's arena.
  static constexpr std::size_t kAllocCount = 4096;
  static constexpr std::size_t kAllocSize = 1024;
  memgraph::memory::DbArenaScope scope{&db1->Arena()};
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
  // NOTE: db_query_memory_tracker_ intentionally does NOT have graph_memory_tracker as parent
  // to avoid double-counting (query PMR bytes were being counted twice: once via
  // TrackingMemoryResource::Alloc and once via arena hooks). Query memory only rolls up to
  // db_total_memory_tracker_ for tenant limit enforcement, not to global domain trackers.
  memgraph::query::QueryAllocator execution_memory{db1->DbQueryMemoryTracker()};
  const int64_t pmr_before = db1->DbQueryMemoryUsage();
  const int64_t db_total_before = db1->DbMemoryUsage();

  static constexpr std::size_t kAllocSize = 4 * 1024 * 1024;
  void *ptr = execution_memory.resource_without_pool_or_mono()->allocate(kAllocSize, alignof(std::max_align_t));

  const int64_t pmr_after = db1->DbQueryMemoryUsage();
  const int64_t db_total_after = db1->DbMemoryUsage();

  execution_memory.resource_without_pool_or_mono()->deallocate(ptr, kAllocSize, alignof(std::max_align_t));

  EXPECT_GT(pmr_after - pmr_before, static_cast<int64_t>(1024 * 1024))
      << "QueryAllocator PMR should attribute memory to per-DB query tracker";
  EXPECT_GE(db_total_after - db_total_before, pmr_after - pmr_before)
      << "db_total_memory_tracker should include query memory via parent chain (for tenant limits)";
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

  {
    memgraph::memory::DbArenaScope db_arena_scope{&db1->Arena()};
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
    EXPECT_GE(db1_total_delta, db1_delta) << "db_total should include embedding delta";

    ASSERT_NO_ERROR(unique_acc->DropVectorIndex(spec.index_name));
    EXPECT_EQ(db1->DbEmbeddingMemoryUsage(), db1_before) << "Dropping index should release embedding memory";
  }

  auto edge_type = db1->storage()->NameToEdgeType("EMBEDDED_EDGE");
  auto edge_property = db1->storage()->NameToProperty("edge_embedding");
  {
    memgraph::memory::DbArenaScope scope{&db1->Arena()};
    auto accessor = db1->Access();
    std::vector<memgraph::storage::VertexAccessor> vertices;
    vertices.reserve(1025);
    for (int i = 0; i < 1025; ++i) {
      vertices.push_back(accessor->CreateVertex());
    }
    for (int i = 0; i < 1024; ++i) {
      auto edge = accessor->CreateEdge(&vertices[i], &vertices[i + 1], edge_type);
      ASSERT_NO_ERROR(edge);
      std::vector<double> embedding(256, static_cast<double>(i % 11) + 0.25);
      ASSERT_NO_ERROR(edge->SetProperty(edge_property, memgraph::storage::PropertyValue(embedding)));
    }
    accessor->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t db1_edge_before = db1->DbEmbeddingMemoryUsage();
  const int64_t db2_edge_before = db2->DbEmbeddingMemoryUsage();
  const int64_t db1_edge_total_before = db1->DbMemoryUsage();

  memgraph::storage::VectorEdgeIndexSpec edge_spec{
      .index_name = "db1_edge_embedding_index",
      .edge_type_id = edge_type,
      .property = edge_property,
      .metric_kind = unum::usearch::metric_kind_t::cos_k,
      .dimension = 256,
      .resize_coefficient = 2,
      .capacity = 4096,
      .scalar_kind = unum::usearch::scalar_kind_t::f32_k,
  };

  {
    memgraph::memory::DbArenaScope db_arena_scope{&db1->Arena()};
    auto unique_acc = db1->UniqueAccess();
    ASSERT_NO_ERROR(unique_acc->CreateVectorEdgeIndex(edge_spec));
  }

  const int64_t db1_edge_delta = db1->DbEmbeddingMemoryUsage() - db1_edge_before;
  const int64_t db1_edge_total_delta = db1->DbMemoryUsage() - db1_edge_total_before;

  EXPECT_GT(db1_edge_delta, static_cast<int64_t>(256 * 1024))
      << "Vector edge index should attribute embedding memory to DB1";
  EXPECT_EQ(db2->DbEmbeddingMemoryUsage(), db2_edge_before) << "DB2 embedding tracker must not grow";
  EXPECT_GE(db1_edge_total_delta, db1_edge_delta) << "db_total should include edge embedding delta";

  {
    memgraph::memory::DbArenaScope db_arena_scope{&db1->Arena()};
    auto unique_acc = db1->UniqueAccess();
    ASSERT_NO_ERROR(unique_acc->DropVectorIndex(edge_spec.index_name));
  }
  EXPECT_EQ(db1->DbEmbeddingMemoryUsage(), db1_edge_before) << "Dropping edge index should release embedding memory";
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

  const unsigned arena_idx = db->BaseArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  memgraph::memory::DbArenaScope scope{&db->Arena()};

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

  const unsigned arena_idx = db->BaseArenaIdx();
  ASSERT_NE(arena_idx, 0U);
  memgraph::memory::DbArenaScope scope{&db->Arena()};

  auto label = db->storage()->NameToLabel("IdxNode");
  auto prop = db->storage()->NameToProperty("val");
  auto edge_type = db->storage()->NameToEdgeType("IDX_REL");

  // Create 2000 nodes with label+property, and 1000 edges.
  {
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
//    - DbArenaScope (query, trigger, stream, replication, GC, snapshot, TTL)
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ThreadPinningPatternsAttributed) {
  memgraph::memory::GlobalArenaPool::Instance().Drain();
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->BaseArenaIdx();
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

  // DbArenaScope pins DbAwareAllocator TLS.
  {
    const int64_t before = db->DbMemoryUsage();
    std::atomic<int64_t> after{0};
    std::thread t([&] {
      memgraph::memory::DbArenaScope scope{&db->Arena()};
      create_vertices("FullPinNode");
      after.store(db->DbMemoryUsage());
    });
    t.join();
    EXPECT_GT(after.load(), before) << "DbArenaScope must attribute allocations to DB";
  }

  // DbArenaScope restores previous state and can be reused by another thread.
  {
    const int64_t before = db->DbMemoryUsage();
    std::atomic<int64_t> after{0};
    std::thread t([&] {
      memgraph::memory::DbArenaScope scope{&db->Arena()};
      create_vertices("TlsPinNode");
      after.store(db->DbMemoryUsage());
    });
    t.join();
    EXPECT_GT(after.load(), before) << "DbArenaScope must attribute allocations to DB";
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

    const unsigned arena_idx = db->BaseArenaIdx();
    ASSERT_NE(arena_idx, 0U);

    memgraph::memory::DbArenaScope scope{&db->Arena()};

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

    memgraph::memory::DbArenaScope scope{&db->Arena()};

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
  memgraph::memory::GlobalArenaPool::Instance().Drain();

  auto cfg = MakeConfig(data_dir_);
  cfg.gc.type = memgraph::storage::Config::Gc::Type::PERIODIC;
  cfg.gc.interval = std::chrono::seconds(1);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{cfg};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  auto &db = *db_acc_opt;

  const unsigned arena_idx = db->BaseArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  auto label = db->storage()->NameToLabel("GcNode");
  auto prop = db->storage()->NameToProperty("value");

  const int64_t baseline = db->DbMemoryUsage();

  // Create 100k nodes with string properties to force many committed extents.
  static constexpr int kGcNodes = 100000;
  {
    memgraph::memory::DbArenaScope scope{&db->Arena()};
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

// ---------------------------------------------------------------------------
// 14. ArenaPool API: Base arena vs Acquire/Release semantics
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ArenaPool_BaseArenaIdxVsAcquireRelease) {
  auto dir = data_dir_ / "db_arena_reg";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  // BaseArenaIdx returns the DB's stable base arena.
  unsigned base_arena = db->Arena().idx();
  EXPECT_NE(base_arena, 0u) << "BaseArenaIdx should return a valid arena";
  unsigned base_arena2 = db->Arena().idx();
  EXPECT_EQ(base_arena, base_arena2) << "BaseArenaIdx should be stable across calls";

  // Acquire returns a valid arena for DB-owned thread work.
  unsigned thread_arena = db->Arena().Acquire();
  EXPECT_NE(thread_arena, 0u) << "Acquire should return a valid arena";

  // Acquire does not keep a thread-id map; each call obtains an arena owned by
  // this DB arena pool unless Release returned one to the free list.
  unsigned thread_arena2 = db->Arena().Acquire();
  EXPECT_NE(thread_arena, thread_arena2) << "Acquire should not be idempotent on same thread";

  db->Arena().Release(thread_arena);
  unsigned reused_arena = db->Arena().Acquire();
  EXPECT_EQ(thread_arena, reused_arena) << "Release should return arenas to the pool free list";
  db->Arena().Release(thread_arena2);
  db->Arena().Release(reused_arena);

  // The constructor-created base arena participates in the same free/in-use
  // split as every other arena, so Acquire may return it.
  EXPECT_EQ(base_arena, thread_arena) << "The base arena should be available through Acquire";
}

// ---------------------------------------------------------------------------
// 15. ArenaPool failure paths
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ArenaPool_ConstructorFailureReleasesPendingArena) {
  memgraph::memory::GlobalArenaPool::Instance().Drain();

  memgraph::utils::MemoryTracker tracker1;
  memgraph::utils::MemoryTracker tracker2;

  const auto arena_count_before = JemallocArenaCount();
  memgraph::memory::testing::SetArenaPoolFailureInjection(
      memgraph::memory::testing::ArenaPoolFailureInjection::ConstructorPublish);
  EXPECT_THROW((memgraph::memory::ArenaPool{&tracker1}), std::runtime_error);

  const auto arena_count_after_failure = JemallocArenaCount();
  EXPECT_EQ(arena_count_after_failure, arena_count_before + 1);

  {
    memgraph::memory::ArenaPool arena{&tracker2};
    EXPECT_EQ(JemallocArenaCount(), arena_count_after_failure)
        << "Successful construction should reuse the arena released by failed construction";
  }
}

TEST_F(DbMemoryTrackingTest, ArenaPool_SharedFirstArenaFallbackNotFreedUntilLastRelease) {
  auto dir = data_dir_ / "db_arena_shared_first";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  const auto base_arena = db->Arena().idx();
  const auto first_arena = db->Arena().Acquire();
  ASSERT_EQ(first_arena, base_arena);

  memgraph::memory::testing::SetArenaPoolFailureInjection(
      memgraph::memory::testing::ArenaPoolFailureInjection::AcquireArenaCreate);
  const auto fallback_arena = db->Arena().Acquire();
  ASSERT_EQ(fallback_arena, base_arena);

  db->Arena().Release(fallback_arena);
  const auto next_arena = db->Arena().Acquire();
  EXPECT_NE(next_arena, base_arena) << "The first arena should stay in use until all shared users release it";

  db->Arena().Release(next_arena);
  db->Arena().Release(first_arena);
}

TEST_F(DbMemoryTrackingTest, ArenaPoolScope_NestedSamePoolBorrowsExistingArena) {
  memgraph::utils::MemoryTracker tracker;
  memgraph::memory::ArenaPool arena{&tracker};

  const memgraph::memory::DbArenaScope outer_scope{&arena};
  const auto outer_arena_idx = memgraph::memory::tls_db_arena_state.arena;
  ASSERT_NE(outer_arena_idx, 0U);

  {
    const memgraph::memory::DbArenaScope inner_scope{&arena};
    EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, outer_arena_idx)
        << "Nested scopes from the same ArenaPool should borrow the existing TLS arena";
  }

  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, outer_arena_idx);
}

TEST_F(DbMemoryTrackingTest, ArenaPoolScope_NestedSamePoolDoesNotReleaseOnInnerDestruction) {
  memgraph::utils::MemoryTracker tracker;
  memgraph::memory::ArenaPool pool{&tracker};

  const memgraph::memory::DbArenaScope outer{&pool};
  const auto arena_after_outer = memgraph::memory::tls_db_arena_state.arena;
  ASSERT_NE(arena_after_outer, 0U);

  {
    const memgraph::memory::DbArenaScope inner{&pool};
    EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, arena_after_outer);
    // inner scope destructs here — must NOT release the arena back to the pool
  }

  // TLS still valid; outer scope still holds the arena
  EXPECT_EQ(memgraph::memory::tls_db_arena_state.arena, arena_after_outer);
  // Outer scope destructs here; only one Release() should follow
}

#ifndef NDEBUG
TEST_F(DbMemoryTrackingTest, ArenaPoolScope_CrossPoolNestingAborts) {
  memgraph::utils::MemoryTracker tracker1;
  memgraph::utils::MemoryTracker tracker2;
  memgraph::memory::ArenaPool pool1{&tracker1};
  memgraph::memory::ArenaPool pool2{&tracker2};

  ASSERT_DEATH(
      {
        const memgraph::memory::DbArenaScope outer{&pool1};
        const memgraph::memory::DbArenaScope inner{&pool2};  // different pool → assert
      },
      "");
}
#endif  // !NDEBUG

// ---------------------------------------------------------------------------
// 16. Arena reuse restores hooks before returning the arena index to the pool
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, GlobalArenaPool_ReusedArenaUsesNewTrackerHooks) {
  memgraph::memory::GlobalArenaPool::Instance().Drain();

  memgraph::utils::MemoryTracker tracker1;
  memgraph::utils::MemoryTracker tracker2;

  auto allocate_and_touch = [](unsigned arena_idx) {
    static constexpr std::size_t kAllocCount = 32;
    static constexpr std::size_t kAllocSize = 1024 * 1024;

    std::vector<void *> ptrs;
    ptrs.reserve(kAllocCount);
    for (std::size_t i = 0; i < kAllocCount; ++i) {
      auto *ptr = memgraph::memory::DbAllocateBytes(kAllocSize, arena_idx, alignof(std::max_align_t));
      std::memset(ptr, 0xA5, kAllocSize);
      ptrs.push_back(ptr);
    }
    return ptrs;
  };

  auto deallocate_all = [](std::vector<void *> &ptrs) {
    static constexpr std::size_t kAllocSize = 1024 * 1024;
    for (auto *ptr : ptrs) {
      memgraph::memory::DbDeallocateBytes(ptr, kAllocSize, alignof(std::max_align_t));
    }
    ptrs.clear();
  };

  unsigned released_arena_idx = 0;
  {
    memgraph::memory::ArenaPool arena{&tracker1};
    released_arena_idx = arena.idx();
    ASSERT_NE(released_arena_idx, 0u);

    const auto before = tracker1.Amount();
    auto ptrs = allocate_and_touch(released_arena_idx);
    EXPECT_GT(tracker1.Amount() - before, static_cast<int64_t>(1024 * 1024));
    deallocate_all(ptrs);
  }

  const auto tracker1_after_release = tracker1.Amount();
  {
    memgraph::memory::ArenaPool arena{&tracker2};
    ASSERT_EQ(arena.idx(), released_arena_idx) << "GlobalArenaPool should recycle the just-released arena index";

    const auto tracker2_before = tracker2.Amount();
    auto ptrs = allocate_and_touch(arena.idx());
    EXPECT_GT(tracker2.Amount() - tracker2_before, static_cast<int64_t>(1024 * 1024));
    EXPECT_EQ(tracker1.Amount(), tracker1_after_release)
        << "Reused arena must not keep charging the previous DB tracker";
    deallocate_all(ptrs);
  }
}

// ---------------------------------------------------------------------------
// 17. Concurrent Acquire: All threads get unique arenas
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, ConcurrentAcquire_AllUniqueArenas) {
  auto dir = data_dir_ / "db_concurrent";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  constexpr int kNumThreads = 20;
  std::vector<unsigned> arenas(kNumThreads);
  std::vector<std::thread> threads;
  std::atomic<int> ready_count{0};
  std::atomic<bool> start{false};

  // Launch threads that all wait for a signal, then simultaneously call Acquire.
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      ready_count.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      arenas[i] = db->Arena().Acquire();
    });
  }

  // Wait for all threads to be ready, then signal start
  while (ready_count.load() < kNumThreads) {
    std::this_thread::yield();
  }
  start.store(true);

  for (auto &t : threads) {
    t.join();
  }

  // All arenas should be valid and non-zero
  for (int i = 0; i < kNumThreads; ++i) {
    EXPECT_NE(arenas[i], 0u) << "Thread " << i << " should have a valid arena";
  }

  // All arenas should be unique (per-thread isolation)
  std::set<unsigned> unique_arenas(arenas.begin(), arenas.end());
  EXPECT_EQ(unique_arenas.size(), kNumThreads)
      << "All " << kNumThreads << " threads should have unique arenas, got " << unique_arenas.size();
  for (const auto arena : arenas) {
    db->Arena().Release(arena);
  }
}

// ---------------------------------------------------------------------------
// 18. Concurrent threads get different arenas (guaranteed invariant)
// ---------------------------------------------------------------------------
// Note: We test CONCURRENT threads, not sequential threads. Sequential threads
// may get the same arena if the OS reuses thread IDs (which is OS-dependent).
// The implementation does not use thread-id mappings; it creates or reuses
// arenas through the pool free list.
TEST_F(DbMemoryTrackingTest, ConcurrentThreads_GetDifferentArenas) {
  auto dir = data_dir_ / "db_concurrent_arenas";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  // The guaranteed invariant: concurrent threads get different arenas.
  // Thread ID reuse after death is OS-dependent and NOT guaranteed to produce
  // different IDs, so we test concurrent threads (where different IDs are certain).
  unsigned arena1 = 0, arena2 = 0;
  std::atomic<bool> start{false};

  std::thread t1([&]() {
    while (!start.load()) std::this_thread::yield();
    arena1 = db->Arena().Acquire();
  });
  std::thread t2([&]() {
    while (!start.load()) std::this_thread::yield();
    arena2 = db->Arena().Acquire();
  });

  start.store(true);
  t1.join();
  t2.join();

  ASSERT_NE(arena1, 0u) << "Thread 1 should have a valid arena";
  ASSERT_NE(arena2, 0u) << "Thread 2 should have a valid arena";
  EXPECT_NE(arena1, arena2) << "Concurrent threads should get different arenas";
  db->Arena().Release(arena1);
  db->Arena().Release(arena2);
}

// ---------------------------------------------------------------------------
// 19. Background thread (GC simulation) acquires per-thread arena
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, BackgroundThread_AcquiresPerThreadArena) {
  auto dir = data_dir_ / "db_bg_thread";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  unsigned base_arena = db->Arena().idx();
  ASSERT_NE(base_arena, 0u);

  unsigned bg_thread_arena = 0;
  std::thread bg_thread([&]() {
    // Simulate what GC/snapshot/TTL threads do — use pool-backed scope
    const memgraph::memory::DbArenaScope scope{&db->Arena()};
    bg_thread_arena = memgraph::memory::tls_db_arena_state.arena;
  });
  bg_thread.join();

  EXPECT_NE(bg_thread_arena, 0u) << "Background thread should acquire an arena";
}

// ---------------------------------------------------------------------------
// 21. Mixed allocation paths: explicit DB scope + DbAwareAllocator consistency
// ---------------------------------------------------------------------------
TEST_F(DbMemoryTrackingTest, MixedAllocators_ConsistentAttribution) {
  auto dir = data_dir_ / "db_mixed_alloc";
  std::filesystem::create_directories(dir);

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(dir)};
  auto acc = db_gk.access();
  ASSERT_TRUE(acc);
  auto *db = acc->get();

  const unsigned arena_idx = db->BaseArenaIdx();
  ASSERT_NE(arena_idx, 0U);

  const int64_t before = db->DbMemoryUsage();

  // Create vertices under an explicit DB scope and set properties using
  // DbAwareAllocator (via std::string)
  {
    memgraph::memory::DbArenaScope scope{&db->Arena()};
    auto txn = db->Access();

    // Create 1000 vertices with string properties
    for (int i = 0; i < 1000; ++i) {
      auto v = txn->CreateVertex();
      // Vertex creation is attributed through the DB scope in the storage layer
      ASSERT_NO_ERROR(v.AddLabel(db->storage()->NameToLabel("MixedNode")));
      // Property string uses DbAwareAllocator (via std::string allocation)
      ASSERT_NO_ERROR(v.SetProperty(db->storage()->NameToProperty("data"),
                                    memgraph::storage::PropertyValue(std::string(1024, 'x'))));  // 1KB string
    }
    txn->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  }

  const int64_t after = db->DbMemoryUsage();

  // Both allocators should attribute to the same DB
  EXPECT_GT(after, before + static_cast<int64_t>(1024 * 1024))
      << "Mixed allocator usage should consistently attribute to DB tracker. "
      << "before=" << before << " after=" << after;
}

#endif  // USE_JEMALLOC
