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

#include <filesystem>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/config.hpp"
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
    je_free(p);
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
    je_free(p);
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

  for (void *p : ptrs) je_free(p);

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

  for (void *p : ptrs) je_free(p);

  const int64_t db_delta = db_after - db_before;
  const int64_t graph_delta = graph_after - graph_before;

  EXPECT_GT(db_delta, static_cast<int64_t>(1 * 1024 * 1024)) << "DB tracker must grow after in-arena allocations";
  EXPECT_GE(graph_delta, db_delta) << "graph_memory_tracker must include per-DB allocations (parent chain). "
                                      "graph_delta="
                                   << graph_delta << " db_delta=" << db_delta;
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

#else  // !USE_JEMALLOC

TEST_F(DbMemoryTrackingTest, ArenaIdxIsZeroWithoutJemalloc) {
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{MakeConfig(data_dir_)};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt);
  EXPECT_EQ((*db_acc_opt)->ArenaIdx(), 0u) << "Without jemalloc, arena idx must be 0";
}

#endif  // USE_JEMALLOC
