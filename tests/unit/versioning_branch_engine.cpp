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

// BranchCore tests against a real InMemoryStorage main plus RegisterForkPin; no interpreter wiring (this file does
// not link mg-query). The diff engine shares main's mapper, so one label/property id is valid on both sides.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <prometheus/gauge.h>

#include "memory/db_arena.hpp"
#include "metrics/metric_handles.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/versioning_stub_codec.hpp"
#include "utils/exceptions.hpp"
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/index_key_parts.hpp"
#include "versioning/stub_state.hpp"

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {

class VersioningBranchCoreTest : public testing::Test {
 protected:
  // Runs on main just before the fork pin, for a test that must change main after its initial setup.
  virtual void BeforeFork() {}

  virtual ms::Config MainConfig() const {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    return config;
  }

  void SetUp() override {
    pool_ = std::make_unique<memgraph::memory::ArenaPool>(&db_tracker_);
    scope_ = std::make_unique<memgraph::memory::DbArenaScope>(pool_.get());
    memgraph::metrics::DatabaseMetricHandles handles{};
    handles.unreleased_delta_objects = {&delta_gauge_};
    storage_ = std::make_unique<ms::InMemoryStorage>(
        MainConfig(), std::nullopt, std::make_unique<ms::PlanInvalidatorDefault>(), handles, nullptr, pool_.get());
    {
      auto unique = storage_->UniqueAccess();
      const std::vector<std::string> colors{"red", "green"};
      const std::vector<std::string> sizes{"s", "m", "l"};
      ASSERT_TRUE(unique->CreateEnum("Color", colors).has_value());
      ASSERT_TRUE(unique->CreateEnum("Size", sizes).has_value());
      ASSERT_TRUE(unique->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->Access(ms::WRITE);
      auto v = acc->CreateVertex();
      auto a = acc->CreateVertex();
      ASSERT_TRUE(acc->CreateEdge(&v, &a, storage_->NameToEdgeType("E")).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    BeforeFork();
    fork_ts_ = storage_->RegisterForkPin();
    auto built = mv::BranchCore::Build(*storage_, fork_ts_);
    ASSERT_TRUE(built.has_value());
    core_ = std::move(*built);
  }

  void TearDown() override {
    core_.reset();
    storage_->ReleaseForkPin(fork_ts_);
    storage_.reset();
    scope_.reset();
    pool_.reset();
  }

  ms::InMemoryStorage &Diff() { return core_->diff_engine(); }

  prometheus::Gauge delta_gauge_;
  memgraph::utils::MemoryTracker db_tracker_;
  std::unique_ptr<memgraph::memory::ArenaPool> pool_;
  std::unique_ptr<memgraph::memory::DbArenaScope> scope_;
  std::unique_ptr<ms::InMemoryStorage> storage_;
  std::shared_ptr<mv::BranchCore> core_;
  uint64_t fork_ts_{};
};

// Main is durable, so a diff engine that inherited main's durability settings would leave files on disk.
class VersioningBranchCoreDurableMainTest : public VersioningBranchCoreTest {
 protected:
  VersioningBranchCoreDurableMainTest()
      : main_dir_(std::filesystem::temp_directory_path() /
                  ("versioning_branch_core_" + memgraph::utils::GenerateUUID())) {}

  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.durability.storage_directory = main_dir_;
    config.durability.root_data_directory = main_dir_;
    config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.durability.snapshot_on_exit = true;
    return config;
  }

  void TearDown() override {
    VersioningBranchCoreTest::TearDown();
    std::error_code ec;
    std::filesystem::remove_all(main_dir_, ec);
  }

  std::filesystem::path main_dir_;
};

class VersioningBranchCorePeriodicGcTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(20)};
    return config;
  }
};

// Main started in analytical mode and was switched to transactional before the fork, so its startup config says
// analytical while it runs transactional.
class VersioningBranchCoreSwitchedModeTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.storage_mode = ms::StorageMode::IN_MEMORY_ANALYTICAL;
    return config;
  }

  void BeforeFork() override { storage_->SetStorageMode(ms::StorageMode::IN_MEMORY_TRANSACTIONAL); }
};

class VersioningBranchCoreAutoIndexTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.items.enable_label_index_auto_creation = true;
    config.salient.items.enable_edge_type_index_auto_creation = true;
    config.salient.items.enable_schema_info = true;
    return config;
  }
};

class VersioningBranchCoreNoEdgePropertiesTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.items.properties_on_edges = false;
    return config;
  }
};

class VersioningBranchCoreSchemaMetadataTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.items.enable_schema_metadata = true;
    return config;
  }
};

class VersioningBranchCoreLightEdgeTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.items.properties_on_edges = true;
    config.salient.items.storage_light_edge = true;
    return config;
  }
};

// Main holds one of every definition a branch must not mirror into its diff engine.
class VersioningBranchCoreDefinitionsTest : public VersioningBranchCoreTest {
 protected:
  void BeforeFork() override {
    const auto label = storage_->NameToLabel("Indexed");
    const auto property = storage_->NameToProperty("p");
    const auto vector_property = storage_->NameToProperty("vec");
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateIndex(label).has_value());
      ASSERT_TRUE(acc->CreateIndex(label, {property}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateExistenceConstraint(label, property).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateUniqueConstraint(label, {property}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateVectorIndex(ms::VectorIndexSpec{.index_name = "v",
                                                             .label_filter = {ms::VectorMatchMode::SINGLE, {label}},
                                                             .property = vector_property,
                                                             .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                                             .dimension = 2,
                                                             .resize_coefficient = 2,
                                                             .capacity = 10,
                                                             .scalar_kind = unum::usearch::scalar_kind_t::f32_k})
                      .has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateTextIndex(ms::TextIndexSpec{"t", label, {property}}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }
};

class VersioningBranchCoreNarrowingTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.experimental_commit_lock_narrowing = true;
    return config;
  }
};

TEST_F(VersioningBranchCoreNarrowingTest, DiffEngineNeverNarrowsTheCommitLock) {
  ASSERT_TRUE(storage_->config_.experimental_commit_lock_narrowing);
  EXPECT_FALSE(Diff().config_.experimental_commit_lock_narrowing);
  auto acc = Diff().Access(ms::WRITE);
  EXPECT_FALSE(acc->GetTransaction()->commit_lock_narrowing);
}

TEST_F(VersioningBranchCoreTest, DiffEngineSharesMainsArenaDeltaGaugeAndGcConfig) {
  EXPECT_EQ(Diff().DbArenaPool(), pool_.get());
  EXPECT_EQ(Diff().metric_handles_.unreleased_delta_objects.gauge, &delta_gauge_);
  EXPECT_TRUE(Diff().config_.gc == storage_->config_.gc);
  EXPECT_EQ(Diff().config_.durability.snapshot_wal_mode, ms::Config::Durability::SnapshotWalMode::DISABLED);
  EXPECT_NE(Diff().uuid(), storage_->uuid());
}

TEST_F(VersioningBranchCorePeriodicGcTest, DiffEngineInheritsNonDefaultGcInterval) {
  EXPECT_EQ(storage_->config_.gc.interval, std::chrono::milliseconds(20));
  EXPECT_EQ(Diff().config_.gc.type, ms::Config::Gc::Type::PERIODIC);
  EXPECT_EQ(Diff().config_.gc.interval, storage_->config_.gc.interval);
}

TEST_F(VersioningBranchCoreTest, DiffEngineSharesMainsNameIdMapperAndReportsMainsName) {
  EXPECT_EQ(Diff().GetSharedNameIdMapper(), storage_->GetSharedNameIdMapper());
  const auto label = Diff().NameToLabel("CreatedOnTheBranch");
  EXPECT_EQ(storage_->LabelToName(label), "CreatedOnTheBranch");
  EXPECT_EQ(storage_->NameToLabel("CreatedOnTheBranch"), label);
  EXPECT_EQ(Diff().NameToProperty("p"), storage_->NameToProperty("p"));
  EXPECT_EQ(Diff().NameToEdgeType("E"), storage_->NameToEdgeType("E"));
  EXPECT_EQ(Diff().LabelToName(storage_->NameToLabel("FromMain")), "FromMain");

  EXPECT_EQ(Diff().name(), storage_->name());
  const auto before = storage_->name();
  storage_->config_.salient.name = "renamed";  // what Database::Rename does to main
  EXPECT_EQ(Diff().name(), "renamed") << "a rename of main must be visible through the diff engine";
  EXPECT_EQ(*Diff().name_view(), "renamed");
  EXPECT_EQ(Diff().config_.salient.name.str(), before) << "the copied config goes stale; name() must not read it";
}

// What a user types on a branch resolves on main's mapper (BranchCore::NameTo*); a name that spells a marker is just
// the user's own label.
TEST_F(VersioningBranchCoreTest, UserNamesSpellingMarkersStayUserNamesWhenResolvedThroughTheCore) {
  for (const char *name : {"__mg_removed_x", "__mg_deleted", "__mg_lit_x", "__mg_"}) {
    SCOPED_TRACE(name);
    const auto label = core_->NameToLabel(name);
    EXPECT_FALSE(ms::IsReservedId(label));
    EXPECT_EQ(label, storage_->NameToLabel(name));
    EXPECT_EQ(storage_->LabelToName(label), name) << "main knows the text as written";
    EXPECT_EQ(core_->NameToProperty(name), storage_->NameToProperty(name));
    EXPECT_FALSE(ms::IsReservedId(core_->NameToProperty(name)));
    EXPECT_FALSE(ms::IsReservedId(core_->NameToEdgeType(name).AsUint()));
  }
  const auto removed_x = core_->NameToLabel("__mg_removed_x");
  const auto x = core_->NameToLabel("x");
  EXPECT_NE(removed_x, x);
  EXPECT_NE(removed_x, mv::stub::Mark(x));
  EXPECT_EQ(Diff().NameToLabel("__mg_removed_x"), removed_x) << "the diff engine reads the text as the user's label";
  EXPECT_EQ(Diff().LabelToName(removed_x), "__mg_removed_x") << "and writes it to the branch WAL as written";

  auto acc = Diff().Access(ms::WRITE);
  auto v = acc->CreateVertex();
  ASSERT_TRUE(v.AddLabel(removed_x).has_value());
  auto labels = v.Labels(ms::View::NEW);
  ASSERT_TRUE(labels.has_value());
  ASSERT_EQ(labels->size(), 1U);
  EXPECT_EQ((*labels)[0], removed_x);
  auto const state = mv::stub::Decode(v, ms::View::NEW);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state->added_labels, std::vector<ms::LabelId>{removed_x});
  EXPECT_TRUE(state->removed_labels.empty());
  acc->Abort();
}

TEST_F(VersioningBranchCoreSwitchedModeTest, DiffEngineRunsInMainsCurrentStorageModeNotItsStartupConfig) {
  ASSERT_EQ(storage_->GetStorageMode(), ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage_->config_.salient.storage_mode, ms::StorageMode::IN_MEMORY_ANALYTICAL);
  EXPECT_EQ(Diff().GetStorageMode(), ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  auto acc = Diff().Access(ms::WRITE);
  EXPECT_EQ(acc->GetTransaction()->storage_mode, ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
}

TEST_F(VersioningBranchCoreAutoIndexTest, DiffEngineNeverAutoCreatesIndexes) {
  ASSERT_TRUE(storage_->config_.salient.items.enable_label_index_auto_creation);
  ASSERT_TRUE(storage_->config_.salient.items.enable_edge_type_index_auto_creation);
  EXPECT_FALSE(Diff().config_.salient.items.enable_label_index_auto_creation);
  EXPECT_FALSE(Diff().config_.salient.items.enable_edge_type_index_auto_creation);

  {
    auto acc = Diff().Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    ASSERT_TRUE(from.AddLabel(core_->NameToLabel("Fresh")).has_value());
    // A marker label is exactly what the auto-index tracker must never be handed.
    ASSERT_TRUE(from.AddLabel(mv::stub::Mark(core_->NameToLabel("Fresh"))).has_value());
    ASSERT_TRUE(acc->CreateEdge(&from, &to, core_->NameToEdgeType("FRESH_EDGE")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Diff().GetAsyncIndexer().CompleteRemaining();
  const auto indices = Diff().Access(ms::READ)->ListAllIndices();
  const auto fresh = core_->NameToLabel("Fresh");
  EXPECT_FALSE(std::ranges::contains(indices.label, fresh))
      << "a branch write must not auto-create a label index on the diff engine";
  EXPECT_FALSE(std::ranges::contains(indices.label, mv::stub::Mark(fresh)))
      << "a branch write must not auto-create a label index on the marker label";
  EXPECT_FALSE(std::ranges::contains(indices.edge_type, core_->NameToEdgeType("FRESH_EDGE")))
      << "a branch write must not auto-create an edge-type index";
}

TEST_F(VersioningBranchCoreAutoIndexTest, DiffEngineNeverTracksSchemaInfo) {
  ASSERT_TRUE(storage_->config_.salient.items.enable_schema_info);
  EXPECT_FALSE(Diff().config_.salient.items.enable_schema_info);
  {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(mv::stub::Mark(core_->NameToLabel("Fresh"))).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

TEST_F(VersioningBranchCoreDefinitionsTest, DiffEngineMirrorsNoConstraintVectorOrTextDefinitions) {
  {
    const auto main_indices = storage_->Access(ms::READ)->ListAllIndices();
    ASSERT_FALSE(main_indices.label.empty());
    ASSERT_FALSE(main_indices.label_properties.empty());
    ASSERT_FALSE(main_indices.vector_indices_spec.empty());
    ASSERT_FALSE(main_indices.text_indices.empty());
    const auto main_constraints = storage_->Access(ms::READ)->ListAllConstraints();
    ASSERT_FALSE(main_constraints.existence.empty());
    ASSERT_FALSE(main_constraints.unique.empty());
  }
  auto acc = Diff().Access(ms::READ);
  const auto indices = acc->ListAllIndices();
  EXPECT_TRUE(indices.label.empty());
  EXPECT_TRUE(indices.label_properties.empty());
  EXPECT_TRUE(indices.vertex_property.empty());
  EXPECT_TRUE(indices.edge_type.empty());
  EXPECT_TRUE(indices.edge_type_property.empty());
  EXPECT_TRUE(indices.edge_property.empty());
  EXPECT_TRUE(indices.text_indices.empty());
  EXPECT_TRUE(indices.text_edge_indices.empty());
  EXPECT_TRUE(indices.point_label_property.empty());
  EXPECT_TRUE(indices.vector_indices_spec.empty());
  EXPECT_TRUE(indices.vector_edge_indices_spec.empty());
  const auto constraints = acc->ListAllConstraints();
  EXPECT_TRUE(constraints.existence.empty());
  EXPECT_TRUE(constraints.unique.empty());
  EXPECT_TRUE(constraints.type.empty());
}

TEST_F(VersioningBranchCoreSchemaMetadataTest, DiffEngineKeepsSchemaMetadataAndHoldsReservedIdsInTheLabelList) {
  ASSERT_TRUE(storage_->config_.salient.items.enable_schema_metadata);
  EXPECT_TRUE(Diff().config_.salient.items.enable_schema_metadata) << "inherited from main, not forced off";
  const auto label = core_->NameToLabel("Native");
  {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(label).has_value());
    ASSERT_TRUE(v.AddLabel(mv::stub::Mark(label)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  auto labels = Diff().ListAllPossiblyPresentVertexLabels();
  EXPECT_TRUE(std::ranges::count(labels, label) == 1) << "branch-native labels are listed";
  EXPECT_TRUE(std::ranges::count(labels, mv::stub::Mark(label)) == 1)
      << "a marker reaches the list: the single SHOW consumer must filter ms::IsReservedId";
  std::erase_if(labels, [](ms::LabelId l) { return ms::IsReservedId(l); });
  EXPECT_EQ(labels, std::vector<ms::LabelId>{label});
}

TEST_F(VersioningBranchCoreNoEdgePropertiesTest, DiffEngineForcesPropertiesOnEdgesWhenMainHasNone) {
  ASSERT_FALSE(storage_->config_.salient.items.properties_on_edges);
  EXPECT_TRUE(Diff().config_.salient.items.properties_on_edges);
  EXPECT_FALSE(core_->main().config_.salient.items.properties_on_edges)
      << "EdgeRef interpretation of main's edges must read main's config";

  auto acc = Diff().Access(ms::WRITE);
  auto *from = core_->stub_index().GetOrCreateStub(0, nullptr);
  auto *to = core_->stub_index().GetOrCreateStub(1, nullptr);
  auto *edge = core_->edge_stub_index().GetOrCreate(0);
  auto accessor = ms::EdgeAccessor::Create(
      ms::EdgeRef{edge}, core_->NameToEdgeType("E"), from, to, &Diff(), acc->GetTransaction(), ms::View::NEW);
  ASSERT_TRUE(accessor.has_value());
  const auto set = accessor->SetProperty(core_->NameToProperty("p"), ms::PropertyValue{1});
  EXPECT_TRUE(set.has_value()) << "an edge stub must accept a property even when main has none";
  acc->Abort();
}

// Nothing ever snapshots a diff engine: the snapshot thread belongs to whoever installs a snapshot handler, and a
// branch never does. Main's own thread is the control.
TEST_F(VersioningBranchCoreTest, DiffEngineStartsNoSnapshotThread) {
  const auto snapshot_threads = [] {
    size_t count = 0;
    for (const auto &entry : std::filesystem::directory_iterator("/proc/self/task")) {
      std::ifstream comm{entry.path() / "comm"};
      std::string name;
      std::getline(comm, name);
      count += name == "Snapshot" ? 1 : 0;
    }
    return count;
  };
  ASSERT_EQ(snapshot_threads(), 0U);
  storage_->CreateSnapshotHandler(
      [](std::string_view) -> std::expected<void, ms::InMemoryStorage::CreateSnapshotError> { return {}; });
  // The runner names its thread once it is scheduled, a moment after Run returns.
  for (int i = 0; i < 200 && snapshot_threads() == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_EQ(snapshot_threads(), 1U) << "main's handler starts main's runner";

  auto another = mv::BranchCore::Build(*storage_, fork_ts_);
  ASSERT_TRUE(another.has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(snapshot_threads(), 1U) << "building a diff engine must not add a snapshot runner";
  EXPECT_FALSE(static_cast<ms::Storage &>(Diff()).GetInfo().durability_snapshot_enabled);
}

TEST_F(VersioningBranchCoreTest, BuildMirrorsMainsEnumsWithTheSameIds) {
  auto main_acc = storage_->Access(ms::READ);
  auto diff_acc = Diff().Access(ms::READ);
  for (const auto &[type, value] : {std::pair{"Color", "green"}, std::pair{"Size", "l"}}) {
    auto in_main = main_acc->GetEnumValue(type, value);
    auto in_diff = diff_acc->GetEnumValue(type, value);
    ASSERT_TRUE(in_main.has_value());
    ASSERT_TRUE(in_diff.has_value()) << type << "::" << value;
    EXPECT_EQ(*in_main, *in_diff) << type << "::" << value;
  }
}

TEST_F(VersioningBranchCoreTest, GidFloorsAreMainsCountersAtBuildOrTheRecordedOnes) {
  EXPECT_EQ(core_->fork_ts(), fork_ts_);
  EXPECT_EQ(&core_->main(), storage_.get());
  EXPECT_EQ(core_->vertex_gid_floor(), 2U);
  EXPECT_EQ(core_->edge_gid_floor(), 1U);
  {
    auto acc = storage_->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(core_->vertex_gid_floor(), 2U) << "main's later creates must not move the floor";

  auto later = mv::BranchCore::Build(*storage_, fork_ts_);
  ASSERT_TRUE(later.has_value());
  EXPECT_EQ((*later)->vertex_gid_floor(), 3U) << "an unrecorded build reads main's counter at build time";

  auto recorded = mv::BranchCore::Build(
      *storage_, fork_ts_, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 2, .edge_gid_floor = 1});
  ASSERT_TRUE(recorded.has_value());
  EXPECT_EQ((*recorded)->vertex_gid_floor(), 2U);
  EXPECT_EQ((*recorded)->edge_gid_floor(), 1U);
}

// A stub keeps its fork gid, and the diff engine's index scans skip entries at or above the engine's own counter, so
// the counters start at the floors: a stub is then always below them, whether or not a native object exists yet.
TEST_F(VersioningBranchCoreTest, TheDiffEnginesCountersStartAtTheGidFloors) {
  EXPECT_EQ(Diff().vertex_id_.load(), core_->vertex_gid_floor());
  EXPECT_EQ(Diff().edge_id_.load(), core_->edge_gid_floor());
  auto recorded = mv::BranchCore::Build(
      *storage_, fork_ts_, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 7, .edge_gid_floor = 5});
  ASSERT_TRUE(recorded.has_value());
  EXPECT_EQ((*recorded)->diff_engine().vertex_id_.load(), 7U);
  EXPECT_EQ((*recorded)->diff_engine().edge_id_.load(), 5U);
}

TEST_F(VersioningBranchCoreTest, ReserveNativeGidAsksTheReserverOnlyAtOrAboveTheKnownCeiling) {
  using mv::GidKind;
  const auto v_floor = core_->vertex_gid_floor();
  const auto e_floor = core_->edge_gid_floor();

  EXPECT_NO_THROW(core_->ReserveNativeGid(GidKind::kVertex, v_floor)) << "no reserver: nothing to make durable";

  using Call = std::pair<GidKind, uint64_t>;
  std::vector<Call> calls;
  core_->SetGidReserver([&calls](GidKind kind, uint64_t through) -> std::expected<uint64_t, std::string> {
    calls.emplace_back(kind, through);
    return through + 9;
  });

  core_->ReserveNativeGid(GidKind::kVertex, v_floor);
  core_->ReserveNativeGid(GidKind::kVertex, v_floor + 1);
  core_->ReserveNativeGid(GidKind::kVertex, v_floor + 9);
  EXPECT_EQ(calls, (std::vector<Call>{{GidKind::kVertex, v_floor + 1}}))
      << "below the returned ceiling: not asked again";

  core_->ReserveNativeGid(GidKind::kVertex, v_floor + 10);
  core_->ReserveNativeGid(GidKind::kEdge, e_floor);
  const std::vector<Call> expected{
      {GidKind::kVertex, v_floor + 1}, {GidKind::kVertex, v_floor + 11}, {GidKind::kEdge, e_floor + 1}};
  EXPECT_EQ(calls, expected) << "a gid at the ceiling asks again; the edge ceiling is separate";
}

TEST_F(VersioningBranchCoreTest, ReserveNativeGidThrowsOnAReserverFailureAndKeepsTheCeiling) {
  using mv::GidKind;
  const auto v_floor = core_->vertex_gid_floor();
  std::vector<std::pair<GidKind, uint64_t>> calls;
  core_->SetGidReserver([&calls](GidKind kind, uint64_t through) -> std::expected<uint64_t, std::string> {
    calls.emplace_back(kind, through);
    return std::unexpected("ledger down");
  });

  for (int attempt = 0; attempt < 2; ++attempt) {
    try {
      core_->ReserveNativeGid(GidKind::kVertex, v_floor);
      ADD_FAILURE() << "a failed reservation must throw";
    } catch (const memgraph::utils::BasicException &e) {
      EXPECT_NE(std::string{e.what()}.find("ledger down"), std::string::npos) << e.what();
    }
  }
  const std::pair<GidKind, uint64_t> asked{GidKind::kVertex, v_floor + 1};
  EXPECT_EQ(calls, (std::vector<std::pair<GidKind, uint64_t>>{asked, asked})) << "the ceiling did not move";
}

TEST(VersioningIndexKeyParts, EachPartKnowsTheKeysItBelongsTo) {
  auto const a = ms::LabelId::FromUint(1), b = ms::LabelId::FromUint(2);
  auto const x = ms::PropertyId::FromUint(1), y = ms::PropertyId::FromUint(2), z = ms::PropertyId::FromUint(3);
  std::vector<ms::LabelPropertyIndexEntry> entries;
  entries.push_back({.label = a, .properties = {ms::PropertyPath{x}}, .order = ms::IndexOrder::ASC});
  entries.push_back({.label = a, .properties = {ms::PropertyPath{x}}, .order = ms::IndexOrder::DESC});
  entries.push_back({.label = a, .properties = {ms::PropertyPath{x}, ms::PropertyPath{y, z}, ms::PropertyPath{y}}});
  entries.push_back({.label = b, .properties = {ms::PropertyPath{y, z}}});
  mv::IndexKeyParts const parts{entries};
  ASSERT_EQ(parts.keys().size(), 3U) << "one key under both orders; a nested path counts as its root";
  EXPECT_EQ(parts.keys()[1].properties, (std::vector<ms::PropertyId>{x, y})) << "roots in index order, distinct";

  auto const keys_of_label = [&](ms::LabelId label) {
    std::vector<size_t> sizes;
    parts.ForEachKeyOfLabel(label, [&](const mv::IndexKeyParts::Key &key) { sizes.push_back(key.properties.size()); });
    return sizes;
  };
  auto const keys_of_property = [&](ms::PropertyId property) {
    std::vector<ms::LabelId> labels;
    parts.ForEachKeyOfProperty(property, [&](const mv::IndexKeyParts::Key &key) { labels.push_back(key.label); });
    return labels;
  };
  EXPECT_EQ(keys_of_label(a), (std::vector<size_t>{1, 2}));
  EXPECT_EQ(keys_of_label(b), (std::vector<size_t>{1}));
  EXPECT_TRUE(keys_of_label(ms::LabelId::FromUint(9)).empty());
  EXPECT_EQ(keys_of_property(x), (std::vector<ms::LabelId>{a, a}));
  EXPECT_EQ(keys_of_property(y), (std::vector<ms::LabelId>{a, b}));
  EXPECT_EQ(keys_of_property(z), (std::vector<ms::LabelId>{}));
  EXPECT_TRUE(mv::IndexKeyParts{}.empty());
}

TEST_F(VersioningBranchCoreTest, TouchedGatesStartClearAndStaySetOnceRecorded) {
  const auto label = storage_->NameToLabel("L");
  const auto pid = storage_->NameToProperty("p");

  EXPECT_FALSE(core_->MayHaveLabelTouched(label));
  EXPECT_FALSE(core_->MayHavePropertyTouched(pid));

  core_->RecordLabelTouched(label);
  EXPECT_TRUE(core_->MayHaveLabelTouched(label));
  const auto other = storage_->NameToLabel("Other");
  ASSERT_NE(label.AsUint() % 64, other.AsUint() % 64);
  EXPECT_FALSE(core_->MayHaveLabelTouched(other)) << "another label's bit stays clear";
  EXPECT_FALSE(core_->MayHavePropertyTouched(pid)) << "the two gates are independent";
  core_->RecordLabelTouched(label);
  EXPECT_TRUE(core_->MayHaveLabelTouched(label));

  core_->RecordPropertyTouched(pid);
  EXPECT_TRUE(core_->MayHavePropertyTouched(pid));

  const auto other_pid = storage_->NameToProperty("other_p");
  ASSERT_NE(pid.AsUint() % 64, other_pid.AsUint() % 64);
  EXPECT_FALSE(core_->MayHavePropertyTouched(other_pid)) << "another property's bit stays clear";

  EXPECT_FALSE(core_->MayHaveAnyVertexDeleted());
  core_->RecordVertexDeleted(ms::Gid::FromUint(1));
  EXPECT_TRUE(core_->MayHaveVertexDeleted(ms::Gid::FromUint(1)));
  EXPECT_TRUE(core_->MayHaveAnyVertexDeleted());
}

TEST_F(VersioningBranchCoreDurableMainTest, DiffEngineWritesNothingToDiskWhenMainIsDurable) {
  ASSERT_EQ(storage_->config_.durability.snapshot_wal_mode,
            ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL);
  EXPECT_EQ(Diff().config_.durability.snapshot_wal_mode, ms::Config::Durability::SnapshotWalMode::DISABLED);
  EXPECT_FALSE(Diff().config_.durability.snapshot_on_exit);

  {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(storage_->NameToLabel("Durable")).has_value());
    ASSERT_TRUE(v.SetProperty(storage_->NameToProperty("p"), ms::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  core_.reset();

  const auto diff_root = main_dir_ / "versioning_diff_engine";
  std::error_code ec;
  if (std::filesystem::exists(diff_root, ec)) {
    for (const auto &entry : std::filesystem::recursive_directory_iterator(diff_root, ec)) {
      EXPECT_FALSE(entry.is_regular_file()) << "diff engine wrote to disk: " << entry.path();
      if (entry.is_regular_file()) break;
    }
  }
}

TEST_F(VersioningBranchCoreTest, ChangeFilterForwardersReachTheCoresFilters) {
  const auto pid = storage_->NameToProperty("p");
  const auto gid = ms::Gid::FromUint(0);
  const auto edge = ms::Gid::FromUint(0);

  core_->RecordLabelChange(gid);
  core_->RecordPropertyChange(gid);
  core_->RecordPropertyFieldChange(gid, pid);
  core_->RecordEdgeChange(gid);
  core_->RecordForkEdgeProperty(edge, pid);
  core_->RecordForkEdgeDeleted(edge);
  EXPECT_TRUE(core_->MayHaveLabelChange(gid));
  EXPECT_TRUE(core_->MayHavePropertyChange(gid));
  EXPECT_TRUE(core_->MayHavePropertyFieldChange(gid, pid));
  EXPECT_TRUE(core_->MayHaveEdgeChange(gid));
  EXPECT_TRUE(core_->MayHaveForkEdgePropertyChange(edge));
  EXPECT_TRUE(core_->MayHaveForkEdgePropertyFieldChange(edge, pid));
  EXPECT_TRUE(core_->MayHaveForkEdgeDeleted(edge));
  EXPECT_FALSE(core_->MayHaveVertexDeleted(gid));
  EXPECT_FALSE(core_->MayHaveAnyVertexDeleted());
}

TEST_F(VersioningBranchCoreTest, BuildRejectsAnUnpinnedForkTimestamp) {
  const uint64_t never_pinned_ts = fork_ts_ + 1000;
  auto result = mv::BranchCore::Build(*storage_, never_pinned_ts);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message.find(std::to_string(never_pinned_ts)), std::string::npos) << result.error().message;
  EXPECT_EQ(storage_->OldestForkPin(), std::optional<uint64_t>{fork_ts_}) << "a failed build must leak no pin";
}

// Commits through stubs, so their deltas stay linked into the stubs for teardown to free.
void WriteThroughStubs(mv::BranchCore &core, ms::InMemoryStorage &diff, bool also_edge_stub) {
  auto acc = diff.Access(ms::WRITE);
  const auto label = core.NameToLabel("L");
  const auto pid = core.NameToProperty("p");
  auto *vertex_stub = core.stub_index().GetOrCreateStub(0, nullptr);
  auto *other_stub = core.stub_index().GetOrCreateStub(1, nullptr);
  ms::VertexAccessor va{vertex_stub, &diff, acc->GetTransaction()};
  ASSERT_TRUE(va.AddLabel(label).has_value());
  ASSERT_TRUE(va.AddLabel(mv::stub::Mark(label)).has_value());
  ASSERT_TRUE(va.SetProperty(pid, ms::PropertyValue{1}).has_value());
  if (also_edge_stub) {
    auto *edge = core.edge_stub_index().GetOrCreate(0);
    auto accessor = ms::EdgeAccessor::Create(ms::EdgeRef{edge},
                                             core.NameToEdgeType("E"),
                                             vertex_stub,
                                             other_stub,
                                             &diff,
                                             acc->GetTransaction(),
                                             ms::View::NEW);
    ASSERT_TRUE(accessor.has_value());
    ASSERT_TRUE(accessor->SetProperty(pid, ms::PropertyValue{2}).has_value());
  }
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// The teardown tests assert no crash; a use-after-free or leak in the destruction order shows only under ASan.
TEST_F(VersioningBranchCoreTest, TeardownFreesStubsWhoseDeltasAreStillLinked) {
  WriteThroughStubs(*core_, Diff(), true);
  // A second transaction aborts mid-way, leaving its deltas linked to the stubs at teardown time.
  {
    auto acc = Diff().Access(ms::WRITE);
    ms::VertexAccessor va{core_->stub_index().FindStub(0), &Diff(), acc->GetTransaction()};
    ASSERT_TRUE(va.SetProperty(core_->NameToProperty("q"), ms::PropertyValue{3}).has_value());
    acc->Abort();
  }
  core_.reset();
}

TEST_F(VersioningBranchCoreTest, TeardownOnAnotherThreadStillFreesStubsIntoMainsArena) {
  WriteThroughStubs(*core_, Diff(), true);
  std::shared_ptr<mv::BranchCore> last = std::move(core_);
  std::thread{[core = std::move(last)]() mutable { core.reset(); }}.join();
}

TEST_F(VersioningBranchCoreLightEdgeTest, TeardownWithEdgeStubsAndLightEdges) {
  ASSERT_TRUE(Diff().config_.salient.items.storage_light_edge == storage_->config_.salient.items.storage_light_edge);
  WriteThroughStubs(*core_, Diff(), true);
  core_.reset();
}

}  // namespace
