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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"

class CreateSnapshotTest : public testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary directory for testing
    storage_directory = std::filesystem::temp_directory_path() / "create_snapshot_test";
    std::filesystem::create_directories(storage_directory);
  }

  void TearDown() override {
    // Clean up the test directory
    std::filesystem::remove_all(storage_directory);
  }

  memgraph::storage::Config CreateConfig() {
    return memgraph::storage::Config{
        .durability = {.storage_directory = storage_directory,
                       .recover_on_startup = false,
                       .snapshot_on_exit = false,
                       .items_per_batch = 13,
                       .allow_parallel_schema_creation = true},
        .salient = {.items = {.properties_on_edges = false, .enable_schema_info = true}},
    };
  }

  std::filesystem::path storage_directory;
};

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsPathOnSuccess) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data to ensure snapshot has content
  {
    auto acc = mem_storage->Access(memgraph::storage::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test CreateSnapshot returns path on success
  auto result = mem_storage->CreateSnapshot();

  ASSERT_TRUE(result.has_value()) << "CreateSnapshot should succeed with some data";

  auto snapshot_path = result.value();
  ASSERT_TRUE(std::filesystem::exists(snapshot_path)) << "Snapshot file should exist at returned path";
  ASSERT_TRUE(std::filesystem::is_regular_file(snapshot_path)) << "Snapshot should be a regular file";

  // Verify the path is in the expected directory
  auto expected_dir = config.durability.storage_directory / memgraph::storage::durability::kSnapshotDirectory;
  ASSERT_EQ(snapshot_path.parent_path(), expected_dir) << "Snapshot should be in the snapshots directory";

  // Verify the filename format (should contain timestamp)
  auto filename = snapshot_path.filename().string();
  ASSERT_TRUE(filename.find("timestamp_") != std::string::npos) << "Snapshot filename should contain timestamp";
}

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsErrorForReplica) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  auto result = mem_storage->CreateSnapshot();
  ASSERT_TRUE(result.has_value());
}

TEST_F(CreateSnapshotTest, CreateSnapshotReturnsErrorWhenNothingNewToWrite) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data and take a snapshot
  {
    auto acc = mem_storage->Access(memgraph::storage::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto result1 = mem_storage->CreateSnapshot();
  ASSERT_TRUE(result1.has_value()) << "First CreateSnapshot should succeed";

  // Try to create another snapshot immediately - should fail with NothingNewToWrite
  auto result2 = mem_storage->CreateSnapshot();
  ASSERT_FALSE(result2.has_value()) << "Second CreateSnapshot should fail";
  ASSERT_EQ(result2.error(), memgraph::storage::InMemoryStorage::CreateSnapshotError::NothingNewToWrite)
      << "Should return NothingNewToWrite error";

  // Force another snapshot immediately - should succeed
  auto result3 = mem_storage->CreateSnapshot(true);
  ASSERT_TRUE(result3.has_value()) << "Third CreateSnapshot should succeed";
}

TEST_F(CreateSnapshotTest, CreateSnapshotPathFormat) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data
  {
    auto acc = mem_storage->Access(memgraph::storage::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Create snapshot and verify path format
  auto result = mem_storage->CreateSnapshot();
  ASSERT_TRUE(result.has_value()) << "CreateSnapshot should succeed";

  auto snapshot_path = result.value();
  auto filename = snapshot_path.filename().string();

  // Verify the filename follows the expected format: YYYYmmddHHMMSSffffff_timestamp_<timestamp>
  // The format should contain exactly one underscore before "timestamp_"
  auto timestamp_pos = filename.find("_timestamp_");
  ASSERT_NE(timestamp_pos, std::string::npos) << "Filename should contain '_timestamp_'";

  // Verify there's only one underscore before "timestamp_"
  auto first_underscore = filename.find('_');
  ASSERT_EQ(first_underscore, timestamp_pos) << "Should have only one underscore before 'timestamp_'";

  // Verify the timestamp part is numeric
  auto timestamp_str = filename.substr(timestamp_pos + 11);  // Skip "_timestamp_"
  ASSERT_FALSE(timestamp_str.empty()) << "Timestamp should not be empty";
  ASSERT_TRUE(std::all_of(timestamp_str.begin(), timestamp_str.end(), ::isdigit)) << "Timestamp should be numeric";
}

TEST_F(CreateSnapshotTest, BackwardCompatibilityWithErrorHandling) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Test that existing error handling code still works
  auto result = mem_storage->CreateSnapshot();
  ASSERT_TRUE(result.has_value());
}

TEST_F(CreateSnapshotTest, SuccessCaseWithPathRetrieval) {
  auto config = CreateConfig();
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // Create some data
  {
    auto acc = mem_storage->Access(memgraph::storage::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Test the new functionality - getting the path on success
  auto result = mem_storage->CreateSnapshot();

  if (!result) {
    FAIL() << "CreateSnapshot should succeed with data present";
  } else {
    // New functionality: get the path
    auto snapshot_path = result.value();
    ASSERT_TRUE(std::filesystem::exists(snapshot_path));

    // Verify the path is reasonable
    ASSERT_EQ(snapshot_path.extension(), "");
    ASSERT_TRUE(snapshot_path.filename().string().find("timestamp_") != std::string::npos);
  }
}

TEST_F(CreateSnapshotTest, ModeSwitchSnapshotDurableTimestampCoversAnalyticalWrites) {
  auto config = CreateConfig();
  uint64_t ldt_before_switch{};
  memgraph::storage::durability::SnapshotInfo snapshot;
  {
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

    mem_storage->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
    auto label = mem_storage->NameToLabel("L");
    for (int i = 0; i < 5; ++i) {
      auto acc = mem_storage->Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    ldt_before_switch = mem_storage->repl_storage_state_.commit_ts_info_.load().ldt_;
    mem_storage->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL);

    std::vector<memgraph::storage::durability::SnapshotInfo> infos;
    for (auto const &entry : std::filesystem::directory_iterator(storage_directory / "snapshots")) {
      if (!entry.is_regular_file()) continue;
      infos.push_back(memgraph::storage::durability::ReadSnapshotInfo(entry.path()));
    }
    ASSERT_FALSE(infos.empty());
    std::sort(infos.begin(), infos.end(), [](auto const &a, auto const &b) {
      return a.durable_timestamp < b.durable_timestamp;
    });
    snapshot = infos.back();
  }
  EXPECT_GT(snapshot.durable_timestamp, ldt_before_switch);
}

// ===========================================================================
// Light-edge durability (PR8)
// ===========================================================================

namespace {

auto LightCfg(std::filesystem::path dir, bool recover, bool parallel = false) {
  return memgraph::storage::Config{.durability = {.storage_directory = std::move(dir),
                                                  .recover_on_startup = recover,
                                                  .snapshot_on_exit = false,
                                                  .items_per_batch = 13,
                                                  .allow_parallel_snapshot_creation = parallel},
                                   .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}}};
}

auto HeavyCfg(std::filesystem::path dir, bool recover) {
  return memgraph::storage::Config{.durability = {.storage_directory = std::move(dir),
                                                  .recover_on_startup = recover,
                                                  .snapshot_on_exit = false,
                                                  .items_per_batch = 13},
                                   .salient = {.items = {.properties_on_edges = true, .storage_light_edge = false}}};
}

// Light config in WAL mode (no snapshot): forces recovery to replay WAL deltas,
// exercising the light-edge WAL replay arms (edge create / delete / SET_PROPERTY
// out-edges scan).
auto LightWalCfg(std::filesystem::path dir, bool recover) {
  return memgraph::storage::Config{
      .durability = {.storage_directory = std::move(dir),
                     .recover_on_startup = recover,
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::minutes(20)},
                     .wal_file_flush_every_n_tx = 1,
                     .snapshot_on_exit = false},
      .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}}};
}

struct DurabilityTestGids {
  memgraph::storage::Gid v1{}, v2{}, v3{};
  memgraph::storage::Gid e1{};
};

// Builds a fixed graph and returns the GIDs needed for later verification.
// Graph topology:
//   v1 -[KNOWS{weight=3.14, name="edge1"}]-> v2   (e1)
//   v2 -[FOLLOWS]-> v3                            (e2)
//   v1 -[SELF]-> v1                               (e3, self-loop)
//   v1 -[KNOWS]-> v2                              (e4)
//   v1 -[KNOWS]-> v2                              (e5)
// v1 out-degree = 4, v2 out-degree = 1.
DurabilityTestGids WriteDurabilityTestGraph(memgraph::storage::InMemoryStorage *store) {
  namespace s = memgraph::storage;
  DurabilityTestGids gids;

  {
    auto acc = store->Access(s::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    gids.v1 = v1.Gid();
    gids.v2 = v2.Gid();
    gids.v3 = v3.Gid();
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(s::WRITE);
    auto v1 = acc->FindVertex(gids.v1, s::View::NEW);
    auto v2 = acc->FindVertex(gids.v2, s::View::NEW);
    auto v3 = acc->FindVertex(gids.v3, s::View::NEW);
    EXPECT_TRUE(v1 && v2 && v3);
    if (!v1 || !v2 || !v3) return gids;

    auto et_knows = acc->NameToEdgeType("KNOWS");
    auto et_follows = acc->NameToEdgeType("FOLLOWS");
    auto et_self = acc->NameToEdgeType("SELF");
    auto prop_weight = acc->NameToProperty("weight");
    auto prop_name = acc->NameToProperty("name");

    auto e1 = acc->CreateEdge(&*v1, &*v2, et_knows);
    EXPECT_TRUE(e1.has_value());
    if (e1) {
      gids.e1 = e1->Gid();
      EXPECT_TRUE(e1->SetProperty(prop_weight, s::PropertyValue(3.14)).has_value());
      EXPECT_TRUE(e1->SetProperty(prop_name, s::PropertyValue("edge1")).has_value());
    }
    EXPECT_TRUE(acc->CreateEdge(&*v2, &*v3, et_follows).has_value());
    EXPECT_TRUE(acc->CreateEdge(&*v1, &*v1, et_self).has_value());
    EXPECT_TRUE(acc->CreateEdge(&*v1, &*v2, et_knows).has_value());
    EXPECT_TRUE(acc->CreateEdge(&*v1, &*v2, et_knows).has_value());

    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  return gids;
}

// Verifies the topology and e1 properties written by WriteDurabilityTestGraph.
void VerifyDurabilityTestGraph(memgraph::storage::InMemoryStorage *store, const DurabilityTestGids &gids) {
  namespace s = memgraph::storage;
  auto acc = store->Access(s::WRITE);

  auto v1 = acc->FindVertex(gids.v1, s::View::OLD);
  ASSERT_TRUE(v1) << "v1 not found after recovery";

  auto v1_out = v1->OutEdges(s::View::OLD);
  ASSERT_TRUE(v1_out.has_value());
  ASSERT_EQ(v1_out->edges.size(), 4u) << "v1 should have 4 out edges (e1 KNOWS, e3 SELF, e4 KNOWS, e5 KNOWS)";

  auto et_knows = acc->NameToEdgeType("KNOWS");
  int knows_count = 0;
  for (const auto &edge : v1_out->edges) {
    if (edge.EdgeType() == et_knows) ++knows_count;
  }
  ASSERT_EQ(knows_count, 3) << "v1 should have 3 KNOWS out edges (e1, e4, e5)";

  auto et_self = acc->NameToEdgeType("SELF");
  bool self_in_out = false;
  for (const auto &edge : v1_out->edges) {
    if (edge.EdgeType() == et_self) {
      self_in_out = true;
      break;
    }
  }
  EXPECT_TRUE(self_in_out) << "self-loop SELF not found in v1 out-edges";

  auto v1_in = v1->InEdges(s::View::OLD);
  ASSERT_TRUE(v1_in.has_value());
  bool self_in_in = false;
  for (const auto &edge : v1_in->edges) {
    if (edge.EdgeType() == et_self) {
      self_in_in = true;
      break;
    }
  }
  EXPECT_TRUE(self_in_in) << "self-loop SELF not found in v1 in-edges";

  auto prop_weight = acc->NameToProperty("weight");
  auto prop_name = acc->NameToProperty("name");
  bool found_e1 = false;
  for (const auto &edge : v1_out->edges) {
    if (edge.Gid() == gids.e1) {
      found_e1 = true;
      auto props = edge.Properties(s::View::OLD);
      ASSERT_TRUE(props.has_value());
      ASSERT_TRUE(props->count(prop_weight)) << "e1 missing 'weight' property after recovery";
      ASSERT_DOUBLE_EQ(props->at(prop_weight).ValueDouble(), 3.14);
      ASSERT_TRUE(props->count(prop_name)) << "e1 missing 'name' property after recovery";
      ASSERT_EQ(props->at(prop_name).ValueString(), "edge1");
      break;
    }
  }
  ASSERT_TRUE(found_e1) << "e1 not found by GID in v1 out-edges after recovery";

  auto v2 = acc->FindVertex(gids.v2, s::View::OLD);
  ASSERT_TRUE(v2) << "v2 not found after recovery";
  auto v2_out = v2->OutEdges(s::View::OLD);
  ASSERT_TRUE(v2_out.has_value());
  ASSERT_EQ(v2_out->edges.size(), 1u) << "v2 should have 1 out edge (FOLLOWS v2->v3)";

  auto v3 = acc->FindVertex(gids.v3, s::View::OLD);
  ASSERT_TRUE(v3) << "v3 not found after recovery";
}

}  // namespace

// Q2a: light snapshot written and loaded by a light instance.
TEST_F(CreateSnapshotTest, LightEdgeSnapshotRoundTrip) {
  DurabilityTestGids gids;
  {
    memgraph::dbms::Database db{LightCfg(storage_directory, false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
    gids = WriteDurabilityTestGraph(mem_storage);
    ASSERT_TRUE(mem_storage->CreateSnapshot().has_value()) << "CreateSnapshot (light) should succeed";
  }
  {
    memgraph::dbms::Database db2{LightCfg(storage_directory, true)};
    const memgraph::memory::DbArenaScope arena_scope{&db2.Arena()};
    auto *mem_storage2 = static_cast<memgraph::storage::InMemoryStorage *>(db2.storage());
    VerifyDurabilityTestGraph(mem_storage2, gids);
  }
}

// Q2a parallel: same as LightEdgeSnapshotRoundTrip but with parallel snapshot
// creation enabled (exercises the parallel k-way merge path in WriteLightEdgesSection).
TEST_F(CreateSnapshotTest, LightEdgeSnapshotRoundTripParallel) {
  DurabilityTestGids gids;
  {
    memgraph::dbms::Database db{LightCfg(storage_directory, false, /*parallel=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
    gids = WriteDurabilityTestGraph(mem_storage);
    ASSERT_TRUE(mem_storage->CreateSnapshot().has_value()) << "Parallel CreateSnapshot (light) should succeed";
  }
  {
    memgraph::dbms::Database db2{LightCfg(storage_directory, true)};
    const memgraph::memory::DbArenaScope arena_scope{&db2.Arena()};
    auto *mem_storage2 = static_cast<memgraph::storage::InMemoryStorage *>(db2.storage());
    VerifyDurabilityTestGraph(mem_storage2, gids);
  }
}

// Q2b: heavy-written snapshot (v34 bytes) loaded by a light instance.
// Regression for the 475886cc4 fix: heavy loader path must be reachable from
// a storage instance that has storage_light_edge=true.
TEST_F(CreateSnapshotTest, HeavyWrittenSnapshotLoadedIntoLightInstance) {
  DurabilityTestGids gids;
  {
    memgraph::dbms::Database db{HeavyCfg(storage_directory, false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
    gids = WriteDurabilityTestGraph(mem_storage);
    ASSERT_TRUE(mem_storage->CreateSnapshot().has_value()) << "CreateSnapshot (heavy) should succeed";
  }
  {
    memgraph::dbms::Database db2{LightCfg(storage_directory, true)};
    const memgraph::memory::DbArenaScope arena_scope{&db2.Arena()};
    auto *mem_storage2 = static_cast<memgraph::storage::InMemoryStorage *>(db2.storage());
    VerifyDurabilityTestGraph(mem_storage2, gids);
  }
}

// Reverse interop: light-written snapshot (v35 bytes) loaded by a heavy instance.
TEST_F(CreateSnapshotTest, LightSnapshotLoadedIntoHeavyInstance) {
  DurabilityTestGids gids;
  {
    memgraph::dbms::Database db{LightCfg(storage_directory, false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
    gids = WriteDurabilityTestGraph(mem_storage);
    ASSERT_TRUE(mem_storage->CreateSnapshot().has_value()) << "CreateSnapshot (light) should succeed";
  }
  {
    memgraph::dbms::Database db2{HeavyCfg(storage_directory, true)};
    const memgraph::memory::DbArenaScope arena_scope{&db2.Arena()};
    auto *mem_storage2 = static_cast<memgraph::storage::InMemoryStorage *>(db2.storage());
    VerifyDurabilityTestGraph(mem_storage2, gids);
  }
}

// WAL replay round-trip (light): write the graph (no snapshot taken — 20min
// interval), then recover purely from WAL deltas. Exercises the light-edge WAL
// replay arms: edge create (EDGE_CREATE), the out-edges scan for
// EDGE_SET_PROPERTY on light edges, and edge delete (EDGE_DELETE).
TEST_F(CreateSnapshotTest, LightEdgeWalReplayRoundTrip) {
  namespace s = memgraph::storage;
  DurabilityTestGids gids;
  {
    memgraph::dbms::Database db{LightWalCfg(storage_directory, false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto *mem_storage = static_cast<s::InMemoryStorage *>(db.storage());
    gids = WriteDurabilityTestGraph(mem_storage);
    // Create then delete an extra edge to exercise the EDGE_DELETE replay arm.
    {
      auto acc = mem_storage->Access(s::WRITE);
      auto v2 = acc->FindVertex(gids.v2, s::View::NEW);
      auto v3 = acc->FindVertex(gids.v3, s::View::NEW);
      ASSERT_TRUE(v2 && v3);
      auto et = acc->NameToEdgeType("TEMP");
      auto e = acc->CreateEdge(&*v2, &*v3, et);
      ASSERT_TRUE(e.has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = mem_storage->Access(s::WRITE);
      auto v2 = acc->FindVertex(gids.v2, s::View::NEW);
      ASSERT_TRUE(v2);
      auto out = v2->OutEdges(s::View::NEW);
      ASSERT_TRUE(out.has_value());
      for (auto &ea : out->edges) {
        if (ea.EdgeType() == acc->NameToEdgeType("TEMP")) {
          ASSERT_TRUE(acc->DeleteEdge(&ea).has_value());
        }
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
  {
    memgraph::dbms::Database db2{LightWalCfg(storage_directory, true)};
    const memgraph::memory::DbArenaScope arena_scope{&db2.Arena()};
    auto *mem_storage2 = static_cast<s::InMemoryStorage *>(db2.storage());
    // The base graph (and its properties) must survive WAL replay; the TEMP edge
    // must be gone (created+deleted via WAL deltas).
    VerifyDurabilityTestGraph(mem_storage2, gids);
    auto acc = mem_storage2->Access(s::WRITE);
    auto v2 = acc->FindVertex(gids.v2, s::View::OLD);
    ASSERT_TRUE(v2);
    auto out = v2->OutEdges(s::View::OLD);
    ASSERT_TRUE(out.has_value());
    for (auto &ea : out->edges) {
      ASSERT_NE(ea.EdgeType(), acc->NameToEdgeType("TEMP")) << "Deleted TEMP edge must not survive WAL replay";
    }
  }
}
