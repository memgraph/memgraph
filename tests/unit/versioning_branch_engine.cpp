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
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <prometheus/gauge.h>
#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "memory/db_arena.hpp"
#include "metrics/metric_handles.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/index_key_parts.hpp"
#include "versioning/stub_merge.hpp"
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
    storage_ = std::make_unique<ms::InMemoryStorage>(MainConfig(),
                                                     std::nullopt,
                                                     std::make_unique<ms::PlanInvalidatorDefault>(),
                                                     handles,
                                                     nullptr,
                                                     pool_.get(),
                                                     &embedding_tracker_);
    {
      auto unique = storage_->UniqueAccess();
      const std::vector<std::string> colors{"red", "green"};
      const std::vector<std::string> sizes{"s", "m", "l"};
      ASSERT_TRUE(unique->CreateEnum("Color", colors).has_value());
      ASSERT_TRUE(unique->CreateEnum("Size", sizes).has_value());
      ASSERT_TRUE(unique->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    label_ = storage_->NameToLabel("L");
    prop_x_ = storage_->NameToProperty("x");
    {
      auto acc = storage_->Access(ms::WRITE);
      auto v = acc->CreateVertex();
      v_gid_ = v.Gid();
      ASSERT_TRUE(v.AddLabel(label_).has_value());
      ASSERT_TRUE(v.SetProperty(prop_x_, ms::PropertyValue(0)).has_value());
      auto a = acc->CreateVertex();
      a_gid_ = a.Gid();
      auto edge = acc->CreateEdge(&v, &a, storage_->NameToEdgeType("E"));
      ASSERT_TRUE(edge.has_value());
      edge_gid_ = edge->Gid();
      if (storage_->config_.salient.items.properties_on_edges) {
        ASSERT_TRUE(edge->SetProperty(prop_x_, ms::PropertyValue(1)).has_value());
      }
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
  memgraph::utils::MemoryTracker embedding_tracker_;
  std::unique_ptr<memgraph::memory::ArenaPool> pool_;
  std::unique_ptr<memgraph::memory::DbArenaScope> scope_;
  std::unique_ptr<ms::InMemoryStorage> storage_;
  std::shared_ptr<mv::BranchCore> core_;
  uint64_t fork_ts_{};
  // Main's data at the fork: v (label L, x=0) -E{x=1}-> a.
  ms::LabelId label_;
  ms::PropertyId prop_x_;
  ms::Gid v_gid_, a_gid_, edge_gid_;
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

TEST_F(VersioningBranchCoreTest, DiffEngineSharesMainsArenaTrackersAndGcConfig) {
  EXPECT_EQ(Diff().DbArenaPool(), pool_.get());
  EXPECT_EQ(Diff().DbEmbeddingMemoryTracker(), &embedding_tracker_);
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
  storage_->config_.salient.name = "renamed";
  EXPECT_EQ(Diff().name(), "renamed") << "a rename of main must be visible through the diff engine";
  EXPECT_EQ(*Diff().name_view(), "renamed");
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

  // On a vertex, the user label survives next to a real marker and decodes back as a user label.
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

// The name is read through name_source_, not copied: main's rename reaches Database::Rename as exactly this write,
// and the diff engine's own copy of the config goes stale the moment it happens.
TEST_F(VersioningBranchCoreTest, DiffEngineNameFollowsMainAfterARenameWhileItsCopiedConfigGoesStale) {
  const auto before = storage_->name();
  storage_->config_.salient.name = "renamed";
  EXPECT_EQ(Diff().name(), storage_->name());
  EXPECT_EQ(Diff().name(), "renamed");
  EXPECT_EQ(Diff().config_.salient.name.str(), before)
      << "the copied config keeps the old name; name() must not read it";
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
  EXPECT_TRUE(indices.label.empty()) << "a branch write must not auto-create a label index on the diff engine";
  EXPECT_TRUE(indices.edge_type.empty()) << "a branch write must not auto-create an edge-type index";
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

TEST_F(VersioningBranchCoreDefinitionsTest, DiffEngineMirrorsOnlyTheVertexIndexDefinitionsNoConstraintVectorOrText) {
  const auto main_indices = storage_->Access(ms::READ)->ListAllIndices();
  ASSERT_FALSE(main_indices.label.empty());
  ASSERT_FALSE(main_indices.label_properties.empty());
  ASSERT_FALSE(main_indices.vector_indices_spec.empty());
  ASSERT_FALSE(main_indices.text_indices.empty());
  {
    const auto main_constraints = storage_->Access(ms::READ)->ListAllConstraints();
    ASSERT_FALSE(main_constraints.existence.empty());
    ASSERT_FALSE(main_constraints.unique.empty());
  }
  auto acc = Diff().Access(ms::READ);
  const auto indices = acc->ListAllIndices();
  EXPECT_EQ(indices.label, main_indices.label);
  ASSERT_EQ(indices.label_properties.size(), main_indices.label_properties.size());
  EXPECT_EQ(indices.vertex_property, main_indices.vertex_property);
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
      << "a marker reaches the list: the single SHOW consumer must filter ms::IsReservedId (S30a)";
  std::erase_if(labels, [](ms::LabelId l) { return ms::IsReservedId(l); });
  EXPECT_EQ(labels, std::vector<ms::LabelId>{label});
}

TEST_F(VersioningBranchCoreTest, DiffEngineAlwaysHasPropertiesOnEdgesAndMainsConfigIsReadFromMain) {
  ASSERT_TRUE(storage_->config_.salient.items.properties_on_edges);
  EXPECT_TRUE(Diff().config_.salient.items.properties_on_edges);
  EXPECT_EQ(&core_->main(), storage_.get());
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
      *storage_, fork_ts_, std::nullopt, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 2, .edge_gid_floor = 1});
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
      *storage_, fork_ts_, std::nullopt, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 7, .edge_gid_floor = 5});
  ASSERT_TRUE(recorded.has_value());
  EXPECT_EQ((*recorded)->diff_engine().vertex_id_.load(), 7U);
  EXPECT_EQ((*recorded)->diff_engine().edge_id_.load(), 5U);
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

TEST_F(VersioningBranchCoreTest, TouchedSnapshotsAreCachedUntilTheNextInsertAndIncludeDeletes) {
  const auto label = storage_->NameToLabel("L");
  const auto other_label = storage_->NameToLabel("Other");
  const auto pid = storage_->NameToProperty("p");
  const auto gid = [](uint64_t g) { return ms::Gid::FromUint(g); };

  EXPECT_FALSE(core_->MayHaveLabelTouched(label));
  EXPECT_FALSE(core_->MayHavePropertyTouched(pid));
  EXPECT_TRUE(core_->LabelTouchedSnapshot(label)->empty());

  core_->RecordLabelTouched(label, gid(0));
  EXPECT_TRUE(core_->MayHaveLabelTouched(label));
  auto first = core_->LabelTouchedSnapshot(label);
  EXPECT_EQ(*first, mv::BranchCore::LabelTouchedSet{0});
  EXPECT_EQ(core_->LabelTouchedSnapshot(label), first) << "no insert since: the cached set is shared";
  core_->RecordLabelTouched(label, gid(0));
  EXPECT_EQ(core_->LabelTouchedSnapshot(label), first) << "a repeated insert changes nothing";

  core_->RecordVertexDeleted(gid(1));
  EXPECT_TRUE(core_->MayHaveVertexDeleted(gid(1)));
  EXPECT_TRUE(core_->MayHaveAnyVertexDeleted());
  EXPECT_EQ(*core_->LabelTouchedSnapshot(label), (mv::BranchCore::LabelTouchedSet{0, 1}));
  EXPECT_EQ(*core_->LabelTouchedSnapshot(other_label), mv::BranchCore::LabelTouchedSet{1})
      << "every label scan must resolve a deleted gid";
  EXPECT_EQ(*first, mv::BranchCore::LabelTouchedSet{0}) << "a handed-out snapshot is immutable";

  core_->RecordPropertyTouched(pid, gid(0));
  EXPECT_TRUE(core_->MayHavePropertyTouched(pid));
  EXPECT_EQ(*core_->PropertyTouchedSnapshot(pid), mv::BranchCore::LabelTouchedSet{0});

  core_->RecordEdgePropertyTouched(pid, gid(0), gid(1));
  core_->RecordEdgePropertyTouched(pid, gid(0), gid(5));
  EXPECT_EQ(*core_->EdgePropertyTouchedSnapshot(pid), (mv::BranchCore::EdgeTouchedMap{{0, 1}}))
      << "the first FROM gid recorded for an edge is kept";
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

// Stub deltas stay linked into the stubs after commit; the diff engine and the indexes must go in the order that keeps
// every delta's owner alive, and give everything back to main's arenas.
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

TEST_F(VersioningBranchCoreTest, TeardownFreesStubsWhoseDeltasAreStillLinked) {
  WriteThroughStubs(*core_, Diff(), true);
  // A second transaction aborts mid-way and a third one is still pinned by a reader at teardown time.
  {
    auto acc = Diff().Access(ms::WRITE);
    ms::VertexAccessor va{core_->stub_index().FindStub(0), &Diff(), acc->GetTransaction()};
    ASSERT_TRUE(va.SetProperty(core_->NameToProperty("q"), ms::PropertyValue{3}).has_value());
    acc->Abort();
  }
  core_.reset();  // the deltas, the stubs and the edge stubs must go without a crash or a leak
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

#if USE_JEMALLOC
// The touched sets grow with the number of changes a branch makes, so they are the tenant's memory.
TEST_F(VersioningBranchCoreTest, TouchedSetsAreChargedToMainsArenaPool) {
  constexpr uint64_t kGids = 200'000;
  const auto label = storage_->NameToLabel("L");
  const auto pid = storage_->NameToProperty("p");
  const auto before = db_tracker_.Amount();
  for (uint64_t gid = 0; gid < kGids; ++gid) {
    core_->RecordLabelTouched(label, ms::Gid::FromUint(gid));
    core_->RecordPropertyTouched(pid, ms::Gid::FromUint(gid));
    core_->RecordVertexDeleted(ms::Gid::FromUint(gid + kGids));
    core_->RecordEdgePropertyTouched(pid, ms::Gid::FromUint(gid), ms::Gid::FromUint(0));
  }
  // At least the 8 key bytes of each of the four sets' entries.
  EXPECT_GT(db_tracker_.Amount() - before, static_cast<int64_t>(4 * kGids * sizeof(uint64_t)));
}
#endif

///// BRANCH READS: THE STUBS MERGED OVER THE FORK STATE

using memgraph::versioning::StubState;
namespace stub = mv::stub;

ms::PropertyValue Int(int64_t v) { return ms::PropertyValue{v}; }

class VersioningBranchEngineTest : public testing::Test {
 protected:
  virtual bool LightEdge() const { return false; }

  virtual bool PropertiesOnEdges() const { return true; }

  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.salient.items.storage_light_edge = LightEdge();
    config.salient.items.properties_on_edges = PropertiesOnEdges();
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
  }

  void TearDown() override {
    branch_context_.reset();
    if (fork_ts_.has_value()) storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  uint64_t ForkNow() {
    fork_ts_ = storage_->RegisterForkPin();
    return *fork_ts_;
  }

  void Fork() {
    auto result = mv::BranchContext::BuildFromFork(*storage_, ForkNow());
    ASSERT_TRUE(result.has_value());
    branch_context_ = std::move(*result);
  }

  void Begin(ms::Storage::Accessor *diff_acc) { ASSERT_TRUE(branch_context_->BeginTransaction(diff_acc).has_value()); }

  static void Commit(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  mv::BranchContext &Ctx() { return *branch_context_; }

  mv::BranchCore &Core() { return *branch_context_->core(); }

  ms::InMemoryStorage &Diff() { return branch_context_->diff_engine(); }

  ms::LabelId Label(const char *name) { return storage_->NameToLabel(name); }

  ms::PropertyId Prop(const char *name) { return storage_->NameToProperty(name); }

  ms::Gid CreateMainVertex(std::initializer_list<ms::LabelId> labels,
                           const std::map<ms::PropertyId, ms::PropertyValue> &properties) {
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    for (auto const label : labels) {
      EXPECT_TRUE(v.AddLabel(label).has_value());
    }
    for (auto const &[pid, value] : properties) {
      EXPECT_TRUE(v.SetProperty(pid, value).has_value());
    }
    auto const gid = v.Gid();
    Commit(*acc);
    return gid;
  }

  struct MainEdge {
    ms::Gid gid;
    ms::Gid from;
    ms::Gid to;
    ms::EdgeTypeId type;
  };

  MainEdge CreateMainEdge(ms::Gid from, ms::Gid to, const char *type,
                          const std::map<ms::PropertyId, ms::PropertyValue> &properties = {}) {
    auto acc = storage_->Access(ms::WRITE);
    auto from_v = acc->FindVertex(from, ms::View::NEW);
    auto to_v = acc->FindVertex(to, ms::View::NEW);
    auto edge = acc->CreateEdge(&*from_v, &*to_v, storage_->NameToEdgeType(type));
    EXPECT_TRUE(edge.has_value());
    if (PropertiesOnEdges()) {
      for (auto const &[pid, value] : properties) {
        EXPECT_TRUE(edge->SetProperty(pid, value).has_value());
      }
    }
    MainEdge created{edge->Gid(), from, to, edge->EdgeType()};
    Commit(*acc);
    return created;
  }

  ms::Vertex *MainVertex(ms::Gid gid) {
    auto acc = storage_->Access(ms::READ);
    auto v = acc->FindVertex(gid, ms::View::OLD);
    EXPECT_TRUE(v.has_value());
    return v->vertex_;
  }

  // Main deletes `edge` and collects: the fork pin keeps the Edge, but the live adjacency no longer links it.
  void MainDeleteEdge(const MainEdge &edge) {
    {
      auto acc = storage_->Access(ms::WRITE);
      auto from = acc->FindVertex(edge.from, ms::View::NEW);
      auto out = from->OutEdges(ms::View::NEW);
      ASSERT_TRUE(out.has_value());
      auto const found = std::ranges::find_if(out->edges, [&](const auto &e) { return e.Gid() == edge.gid; });
      ASSERT_NE(found, out->edges.end());
      auto victim = *found;
      ASSERT_TRUE(acc->DeleteEdge(&victim).has_value());
      Commit(*acc);
    }
    storage_->FreeMemory({}, false);
    storage_->FreeMemory({}, false);
  }

  // Fork edges main deleted after the fork are found at their fork state, by id and in the adjacency, whether or not
  // the branch changed them; so are the fork edges of a main without edge objects.
  void ForkEdgesMainDeletedSinceAreFound() {
    auto const x = Prop("x");
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const changed = CreateMainEdge(a, b, "E", {{x, Int(1)}});
    auto const untouched = CreateMainEdge(a, b, "E", {{x, Int(5)}});
    auto const kept = CreateMainEdge(a, b, "E", {{x, Int(6)}});
    Fork();
    if (PropertiesOnEdges()) SeedEdge(changed, [&](StubState &s) { s.SetProperty(x, Int(10)); });
    MainDeleteEdge(changed);
    MainDeleteEdge(untouched);

    auto diff_acc = Diff().Access(ms::READ);
    Begin(diff_acc.get());
    auto &ctx = Ctx();
    for (auto const view : {ms::View::OLD, ms::View::NEW}) {
      for (auto const &edge : {changed, untouched, kept}) {
        auto const found = ctx.FindForkEdge(edge.gid, a);
        ASSERT_TRUE(found.has_value()) << edge.gid.AsUint();
        EXPECT_EQ(found->Gid(), edge.gid);
        EXPECT_EQ(found->EdgeType(), edge.type);
        auto const resolved = ctx.ResolveEdge(edge.gid, a, view);
        ASSERT_TRUE(resolved.has_value()) << edge.gid.AsUint();
        EXPECT_EQ(resolved->storage_, storage_.get());
      }
      if (PropertiesOnEdges()) {
        EXPECT_EQ(*ctx.GetProperty(*ctx.FindForkEdge(changed.gid, a), x, view), Int(10)) << "the stub merges over it";
        EXPECT_EQ(*ctx.GetProperty(*ctx.FindForkEdge(untouched.gid, a), x, view), Int(5)) << "the fork's properties";
      }
      for (auto const direction : {ms::EdgeDirection::OUT, ms::EdgeDirection::IN}) {
        std::set<uint64_t> gids;
        for (auto const &edge : ctx.ResolveEdges(direction == ms::EdgeDirection::OUT ? a : b, direction, view)) {
          gids.insert(edge.Gid().AsUint());
        }
        EXPECT_EQ(gids, (std::set<uint64_t>{changed.gid.AsUint(), untouched.gid.AsUint(), kept.gid.AsUint()}));
      }
    }
    EXPECT_FALSE(ctx.FindForkEdge(changed.gid, b).has_value()) << "B has no such out-edge";
  }

  // The stubs' marker entries decide which fork edges an expansion drops: by direction, by view, for a deleted edge,
  // a deleted endpoint and a self-loop, and never as an edge of their own.
  void MarkersHideForkEdgesFromTheAdjacency() {
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const c = CreateMainVertex({}, {});
    auto const e1 = CreateMainEdge(a, b, "E");
    auto const e2 = CreateMainEdge(a, b, "E");
    auto const e3 = CreateMainEdge(a, c, "F");
    auto const loop = CreateMainEdge(a, a, "E");
    Fork();
    auto const deleted = [](StubState &s) { s.MarkDeleted(); };
    SeedEdge(e2, deleted);
    SeedEdge(loop, deleted);

    auto const expand = [&](mv::BranchContext &ctx, ms::Gid vertex, ms::EdgeDirection direction, ms::View view) {
      std::set<uint64_t> gids;
      for (auto const &edge : ctx.ResolveEdges(vertex, direction, view)) {
        EXPECT_FALSE(ms::IsReservedId(edge.EdgeType())) << "a marker is no edge";
        gids.insert(edge.Gid().AsUint());
      }
      return gids;
    };
    auto const gids = [](std::initializer_list<MainEdge> edges) {
      std::set<uint64_t> result;
      for (auto const &edge : edges) result.insert(edge.gid.AsUint());
      return result;
    };
    constexpr auto kOut = ms::EdgeDirection::OUT;
    constexpr auto kIn = ms::EdgeDirection::IN;
    constexpr auto kNew = ms::View::NEW;
    constexpr auto kOld = ms::View::OLD;

    auto read_acc = Diff().Access(ms::READ);
    Begin(read_acc.get());
    for (auto const view : {kOld, kNew}) {
      EXPECT_EQ(expand(Ctx(), a, kOut, view), gids({e1, e3}));
      EXPECT_EQ(expand(Ctx(), a, kIn, view), gids({}));
      EXPECT_EQ(expand(Ctx(), b, kIn, view), gids({e1}));
      EXPECT_EQ(expand(Ctx(), c, kIn, view), gids({e3}));
    }
    Ctx().EndTransaction();

    // An uncommitted delete hides the edge from its own transaction only, and not at OLD before the command advances.
    mv::BranchContext other{Ctx().core()};
    auto other_acc = Diff().Access(ms::READ);
    ASSERT_TRUE(other.BeginTransaction(other_acc.get()).has_value());
    {
      auto writer = Diff().Access(ms::WRITE);
      ASSERT_FALSE(WriteEdgeStub(*writer, e1, deleted).has_value());
      Begin(writer.get());
      EXPECT_EQ(expand(Ctx(), a, kOut, kNew), gids({e3}));
      EXPECT_EQ(expand(Ctx(), b, kIn, kNew), gids({}));
      EXPECT_EQ(expand(Ctx(), a, kOut, kOld), gids({e1, e3}));
      writer->AdvanceCommand();
      EXPECT_EQ(expand(Ctx(), a, kOut, kOld), gids({e3}));
      EXPECT_EQ(expand(other, a, kOut, kNew), gids({e1, e3}));
      Ctx().EndTransaction();
      writer->Abort();
    }
    EXPECT_EQ(expand(other, a, kOut, kNew), gids({e1, e3}));
    other.EndTransaction();
    auto after_abort = Diff().Access(ms::READ);
    Begin(after_abort.get());
    EXPECT_EQ(expand(Ctx(), a, kOut, kNew), gids({e1, e3})) << "the abort took the markers away";
    EXPECT_EQ(expand(Ctx(), b, kIn, kNew), gids({e1}));
    Ctx().EndTransaction();

    // A native edge is listed beside the markers, and a lookup of it passes over them.
    ms::Gid native_gid;
    {
      auto writer = Diff().Access(ms::WRITE);
      Begin(writer.get());
      MarkBranched(MainVertex(a));
      ms::VertexAccessor stub_a{
          Core().stub_index().GetOrCreateStub(a.AsUint(), MainVertex(a)), &Diff(), writer->GetTransaction()};
      auto native = CreateNative(writer.get());
      auto const edge = CreateNativeEdge(writer.get(), &stub_a, &native);
      native_gid = edge.Gid();
      EXPECT_EQ(expand(Ctx(), a, kOut, kNew),
                (std::set<uint64_t>{e1.gid.AsUint(), e3.gid.AsUint(), native_gid.AsUint()}));
      EXPECT_TRUE(Ctx().FindDiffEdge(native_gid, a, kNew).has_value());
      EXPECT_FALSE(Ctx().FindDiffEdge(e2.gid, a, kNew).has_value()) << "a marked fork edge is no diff edge";
      EXPECT_FALSE(Ctx().FindDiffEdge(e2.gid, kNew).has_value());
      Commit(*writer);
      Ctx().EndTransaction();
    }
  }

  // A hub: more deleted edges than the linear search, and a type nobody deleted.
  void ManyMarkersHideTheirEdgesOnAHub() {
    auto const hub = CreateMainVertex({}, {});
    std::vector<MainEdge> edges;
    for (int i = 0; i < 40; ++i) {
      edges.push_back(CreateMainEdge(hub, CreateMainVertex({}, {}), i % 2 == 0 ? "E" : "F"));
    }
    Fork();
    std::set<uint64_t> kept;
    for (size_t i = 0; i < edges.size(); ++i) {
      if (i % 3 == 0) {
        kept.insert(edges[i].gid.AsUint());
      } else {
        SeedEdge(edges[i], [](StubState &s) { s.MarkDeleted(); });
      }
    }
    auto acc = Diff().Access(ms::READ);
    Begin(acc.get());
    std::set<uint64_t> seen;
    for (auto const &edge : Ctx().ResolveEdges(hub, ms::EdgeDirection::OUT, ms::View::NEW)) {
      seen.insert(edge.Gid().AsUint());
    }
    EXPECT_EQ(seen, kept);
    for (size_t i = 0; i < edges.size(); ++i) {
      auto const far = Ctx().ResolveEdges(edges[i].to, ms::EdgeDirection::IN, ms::View::NEW);
      EXPECT_EQ(far.size(), i % 3 == 0 ? 1U : 0U) << i;
    }
  }

  // What the write path does before its first stub write of a fork vertex.
  static void MarkBranched(ms::Vertex *vertex) {
    std::unique_lock guard{vertex->lock};
    vertex->SetBranched(true);
  }

  void RecordVertexWrite(ms::Gid gid, const StubState &before, const StubState &after) {
    Core().RecordLabelChange(gid);
    Core().RecordPropertyChange(gid);
    for (auto const *state : {&before, &after}) {
      for (auto const label : state->added_labels) Core().RecordLabelTouched(label, gid);
      for (auto const label : state->removed_labels) Core().RecordLabelTouched(label, gid);
      for (auto const &[pid, value] : state->properties.Properties()) {
        Core().RecordPropertyFieldChange(gid, pid);
        Core().RecordPropertyTouched(pid, gid);
      }
      for (auto const pid : state->removed_pids) {
        Core().RecordPropertyFieldChange(gid, pid);
        Core().RecordPropertyTouched(pid, gid);
      }
    }
    if (after.deleted) Core().RecordVertexDeleted(gid);
  }

  // Edits the stub of fork vertex `gid` in the diff transaction `acc` the way the branch write path will: mark main's
  // vertex, record the change filters, then single-key stock writes. Returns the first write's error.
  template <typename Edit>
  std::optional<ms::Error> WriteVertexStub(ms::Storage::Accessor &acc, ms::Gid gid, Edit &&edit) {
    auto *fork = MainVertex(gid);
    MarkBranched(fork);
    ms::VertexAccessor stub_acc{Core().stub_index().GetOrCreateStub(gid.AsUint(), fork), &Diff(), acc.GetTransaction()};
    auto before = stub::Decode(stub_acc, ms::View::NEW);
    EXPECT_TRUE(before.has_value());
    auto after = before->Copy();
    edit(after);
    RecordVertexWrite(gid, *before, after);
    // A vertex delete hides each of the vertex's visible edges before its own marker, as the write path does.
    if (after.deleted && !before->deleted) {
      for (auto const &edge : VisibleForkEdgesOf(gid)) {
        if (auto error = HideEdge(acc, edge)) return error;
      }
    }
    return stub::Apply(stub_acc, *before, after);
  }

  template <typename Edit>
  void SeedVertex(ms::Gid gid, Edit &&edit) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_FALSE(WriteVertexStub(*acc, gid, edit).has_value());
    Commit(*acc);
  }

  template <typename Edit>
  std::optional<ms::Error> WriteEdgeStub(ms::Storage::Accessor &acc, const MainEdge &edge, Edit &&edit) {
    auto *from = MainVertex(edge.from);
    auto *to = MainVertex(edge.to);
    auto const fork_edge = MarkEdgeBranched(edge);
    ms::EdgeAccessor stub_acc{ms::EdgeRef{Core().edge_stub_index().GetOrCreate(edge.gid.AsUint(), fork_edge)},
                              edge.type,
                              from,
                              to,
                              &Diff(),
                              acc.GetTransaction()};
    auto before = stub::DecodeEdge(stub_acc, ms::View::NEW);
    EXPECT_TRUE(before.has_value());
    auto after = before->Copy();
    edit(after);
    for (auto const *state : {&before.value(), &after}) {
      for (auto const &[pid, value] : state->properties.Properties()) Core().RecordForkEdgeProperty(edge.gid, pid);
      for (auto const pid : state->removed_pids) Core().RecordForkEdgeProperty(edge.gid, pid);
    }
    if (after.deleted) {
      Core().RecordForkEdgeDeleted(edge.gid);
      if (!before->deleted && hidden_edges_.insert(edge.gid.AsUint()).second) {
        if (auto error = HideEdge(acc, edge)) return error;
      }
    }
    return stub::ApplyEdge(stub_acc, *before, after);
  }

  // Marks main's `edge` branched, as the write path does before it writes a stub, and returns it as main holds it.
  ms::EdgeRef MarkEdgeBranched(const MainEdge &edge) {
    auto main_acc = storage_->Access(ms::READ);
    auto from = main_acc->FindVertex(edge.from, ms::View::OLD);
    EXPECT_TRUE(from.has_value());
    auto out = from->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    std::optional<ms::EdgeRef> fork_edge;
    for (auto const &candidate : out->edges) {
      if (candidate.Gid() == edge.gid) fork_edge = candidate.edge_;
    }
    EXPECT_TRUE(fork_edge.has_value());
    // Without properties_on_edges the fork edge is a bare gid, and there is no object to mark.
    if (PropertiesOnEdges()) {
      std::unique_lock guard{fork_edge->ptr->lock};
      fork_edge->ptr->SetBranched(true);
    }
    return *fork_edge;
  }

  // Links the markers of fork edge `edge` into both endpoint stubs, as the write path hides a deleted fork edge.
  std::optional<ms::Error> HideEdge(ms::Storage::Accessor &acc, const MainEdge &edge) {
    auto *from = MainVertex(edge.from);
    auto *to = MainVertex(edge.to);
    MarkBranched(from);
    MarkBranched(to);
    Core().RecordEdgeChange(edge.from);
    Core().RecordEdgeChange(edge.to);
    auto *from_stub = Core().stub_index().GetOrCreateStub(edge.from.AsUint(), from);
    auto *to_stub = Core().stub_index().GetOrCreateStub(edge.to.AsUint(), to);
    auto *edge_stub = Core().edge_stub_index().GetOrCreate(edge.gid.AsUint(), MarkEdgeBranched(edge));
    auto *transaction = acc.GetTransaction();
    if (!stub::LinkEdgeMarker(transaction, Diff(), from_stub, ms::EdgeDirection::OUT, edge.type, to_stub, edge_stub) ||
        !stub::LinkEdgeMarker(transaction, Diff(), to_stub, ms::EdgeDirection::IN, edge.type, from_stub, edge_stub)) {
      return ms::Error::SERIALIZATION_ERROR;
    }
    return std::nullopt;
  }

  // The edges of fork vertex `gid` a delete of it hides: its fork-state edges not yet hidden (a self-loop once).
  std::vector<MainEdge> VisibleForkEdgesOf(ms::Gid gid) {
    std::vector<MainEdge> edges;
    auto acc = storage_->Access(ms::READ);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    EXPECT_TRUE(vertex.has_value());
    for (auto const direction : {ms::EdgeDirection::OUT, ms::EdgeDirection::IN}) {
      auto expanded =
          direction == ms::EdgeDirection::OUT ? vertex->OutEdges(ms::View::OLD) : vertex->InEdges(ms::View::OLD);
      EXPECT_TRUE(expanded.has_value());
      for (auto const &edge : expanded->edges) {
        if (direction == ms::EdgeDirection::IN && edge.IsCycle()) continue;
        if (!hidden_edges_.insert(edge.Gid().AsUint()).second) continue;
        edges.push_back({edge.Gid(), edge.FromVertex().Gid(), edge.ToVertex().Gid(), edge.EdgeType()});
      }
    }
    return edges;
  }

  // The fork edges the fixture has hidden by markers, so that none is hidden twice.
  std::set<uint64_t> hidden_edges_;

  template <typename Edit>
  void SeedEdge(const MainEdge &edge, Edit &&edit) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_FALSE(WriteEdgeStub(*acc, edge, edit).has_value());
    Commit(*acc);
  }

  // A branch-native vertex at a gid from main's counter, as the write path allocates it.
  ms::VertexAccessor CreateNative(ms::Storage::Accessor *diff_acc) {
    auto const gid = ms::Gid::FromUint(storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel));
    return *static_cast<ms::ReplicationAccessor *>(diff_acc)->CreateVertexEx(gid);
  }

  ms::Result<ms::EdgeAccessor> CreateNativeEdgeResult(ms::Storage::Accessor *diff_acc, ms::VertexAccessor *from,
                                                      ms::VertexAccessor *to) {
    auto const gid = ms::Gid::FromUint(storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel));
    return static_cast<ms::ReplicationAccessor *>(diff_acc)->CreateEdgeEx(from, to, storage_->NameToEdgeType("N"), gid);
  }

  ms::EdgeAccessor CreateNativeEdge(ms::Storage::Accessor *diff_acc, ms::VertexAccessor *from, ms::VertexAccessor *to) {
    return *CreateNativeEdgeResult(diff_acc, from, to);
  }

  std::unique_ptr<ms::InMemoryStorage> storage_;
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;
};

class VersioningBranchEngineNoEdgePropertiesTest : public VersioningBranchEngineTest {
 protected:
  bool PropertiesOnEdges() const override { return false; }
};

class VersioningBranchEngineLightEdgeTest : public VersioningBranchEngineTest {
 protected:
  bool LightEdge() const override { return true; }
};

std::set<ms::LabelId> LabelSet(const ms::Result<ms::VertexKey> &labels) {
  EXPECT_TRUE(labels.has_value());
  return labels ? std::set<ms::LabelId>(labels->begin(), labels->end()) : std::set<ms::LabelId>{};
}

TEST_F(VersioningBranchEngineTest, ForkEdgesMainDeletedSinceAreFound) { ForkEdgesMainDeletedSinceAreFound(); }

TEST_F(VersioningBranchEngineNoEdgePropertiesTest, ForkEdgesMainDeletedSinceAreFound) {
  ForkEdgesMainDeletedSinceAreFound();
}

TEST_F(VersioningBranchEngineLightEdgeTest, ForkEdgesMainDeletedSinceAreFound) { ForkEdgesMainDeletedSinceAreFound(); }

#define ENGINE_EDGE_TEST(Name)                                         \
  TEST_F(VersioningBranchEngineTest, Name) { Name(); }                 \
  TEST_F(VersioningBranchEngineNoEdgePropertiesTest, Name) { Name(); } \
  TEST_F(VersioningBranchEngineLightEdgeTest, Name) { Name(); }

ENGINE_EDGE_TEST(MarkersHideForkEdgesFromTheAdjacency)
ENGINE_EDGE_TEST(ManyMarkersHideTheirEdgesOnAHub)

TEST_F(VersioningBranchEngineTest, ForkReadsMergeTheStubOverTheForkState) {
  auto const l1 = Label("L1"), l2 = Label("L2"), l3 = Label("L3"), l4 = Label("L4");
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const a = CreateMainVertex({l1, l3}, {{x, Int(1)}, {y, Int(2)}});
  auto const b = CreateMainVertex({l1}, {});
  auto const c = CreateMainVertex({l1}, {{x, Int(7)}});
  auto const d = CreateMainVertex({l1}, {{x, Int(8)}});
  Fork();
  SeedVertex(a, [&](StubState &s) {
    s.RemoveLabel(l1);
    s.AddLabel(l2);
    s.SetProperty(x, Int(10));
    s.RemoveProperty(y);
    s.SetProperty(z, Int(3));
  });
  SeedVertex(b, [&](StubState &s) { s.RemoveLabel(l1); });
  SeedVertex(d, [](StubState &s) { s.MarkDeleted(); });

  auto diff_acc = Diff().Access(ms::READ);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  for (auto const view : {ms::View::OLD, ms::View::NEW}) {
    auto fa = ctx.ResolveVertex(a, view);
    ASSERT_TRUE(fa.has_value());
    EXPECT_EQ(fa->storage_, storage_.get()) << "a fork vertex is returned as main's object, never as its stub";
    EXPECT_EQ(LabelSet(ctx.Labels(*fa, view)), (std::set<ms::LabelId>{l2, l3}));
    EXPECT_EQ(*ctx.HasLabel(*fa, l1, view), false);
    EXPECT_EQ(*ctx.HasLabel(*fa, l2, view), true);
    EXPECT_EQ(*ctx.HasLabel(*fa, l3, view), true);
    EXPECT_EQ(*ctx.HasLabel(*fa, l4, view), false);
    EXPECT_TRUE(*ctx.HasAnyLabel(*fa, view));
    auto const props = ctx.Properties(*fa, view);
    ASSERT_TRUE(props.has_value());
    EXPECT_EQ(*props, (std::map<ms::PropertyId, ms::PropertyValue>{{x, Int(10)}, {z, Int(3)}}));
    EXPECT_EQ(*ctx.GetProperty(*fa, x, view), Int(10));
    EXPECT_TRUE(ctx.GetProperty(*fa, y, view)->IsNull());
    EXPECT_EQ(*ctx.GetProperty(*fa, z, view), Int(3));
    EXPECT_GT(*ctx.GetPropertySize(*fa, x, view), 0U);
    EXPECT_EQ(*ctx.GetPropertySize(*fa, y, view), 0U);

    auto fb = ctx.ResolveVertex(b, view);
    ASSERT_TRUE(fb.has_value());
    EXPECT_FALSE(*ctx.HasAnyLabel(*fb, view)) << "the only label is hidden";
    EXPECT_TRUE(LabelSet(ctx.Labels(*fb, view)).empty());

    auto fc = ctx.ResolveVertex(c, view);
    ASSERT_TRUE(fc.has_value());
    EXPECT_FALSE(fc->vertex_->branched());
    EXPECT_EQ(*ctx.HasLabel(*fc, l1, view), true);
    EXPECT_EQ(*ctx.GetProperty(*fc, x, view), Int(7));
    EXPECT_FALSE(ctx.HasVertexStub(c));

    EXPECT_FALSE(ctx.ResolveVertex(d, view).has_value()) << "deleted on the branch";
    auto fd = ctx.FindForkVertex(d);
    ASSERT_TRUE(fd.has_value()) << "the fork state itself is unchanged";
    EXPECT_TRUE(ctx.IsForkVertexDeleted(*fd, view));
    EXPECT_EQ(ctx.Labels(*fd, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.HasLabel(*fd, l1, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.HasAnyLabel(*fd, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.Properties(*fd, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.GetProperty(*fd, x, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.GetPropertySize(*fd, x, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.OutEdges(*fd, view).error(), ms::Error::DELETED_OBJECT);
  }
  EXPECT_TRUE(ctx.HasVertexStub(a));
  EXPECT_FALSE(ctx.HasVertexStub(ms::Gid::FromUint(Core().vertex_gid_floor()))) << "a native gid has no stub";
  EXPECT_TRUE(Ctx().IsForkCandidateVertex(a));
}

TEST_F(VersioningBranchEngineTest, ForkEdgeReadsMergeTheEdgeStubOverTheForkEdge) {
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const v = CreateMainVertex({}, {});
  auto const w = CreateMainVertex({}, {});
  auto const e1 = CreateMainEdge(v, w, "E", {{x, Int(1)}, {y, Int(2)}});
  auto const e2 = CreateMainEdge(v, w, "E", {{x, Int(5)}});
  auto const e3 = CreateMainEdge(v, w, "E", {{x, Int(6)}});
  Fork();
  SeedEdge(e1, [&](StubState &s) {
    s.SetProperty(x, Int(10));
    s.RemoveProperty(y);
    s.SetProperty(z, Int(3));
  });
  SeedEdge(e2, [](StubState &s) { s.MarkDeleted(); });

  auto diff_acc = Diff().Access(ms::READ);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  auto fe1 = ctx.FindForkEdge(e1.gid, v);
  auto fe2 = ctx.FindForkEdge(e2.gid, v);
  auto fe3 = ctx.FindForkEdge(e3.gid, v);
  ASSERT_TRUE(fe1 && fe2 && fe3);
  for (auto const view : {ms::View::OLD, ms::View::NEW}) {
    EXPECT_EQ(*ctx.Properties(*fe1, view), (std::map<ms::PropertyId, ms::PropertyValue>{{x, Int(10)}, {z, Int(3)}}));
    EXPECT_EQ(*ctx.GetProperty(*fe1, x, view), Int(10));
    EXPECT_TRUE(ctx.GetProperty(*fe1, y, view)->IsNull());
    EXPECT_GT(*ctx.GetPropertySize(*fe1, x, view), 0U);
    EXPECT_EQ(*ctx.GetPropertySize(*fe1, y, view), 0U);

    EXPECT_TRUE(ctx.IsForkEdgeHidden(*fe2, view));
    EXPECT_EQ(ctx.Properties(*fe2, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.GetProperty(*fe2, x, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(ctx.GetPropertySize(*fe2, x, view).error(), ms::Error::DELETED_OBJECT);
    EXPECT_FALSE(ctx.ResolveEdge(e2.gid, v, view).has_value());

    EXPECT_FALSE(ctx.IsForkEdgeHidden(*fe3, view));
    EXPECT_EQ(*ctx.GetProperty(*fe3, x, view), Int(6)) << "an edge the branch never wrote reads as the fork's";
    auto resolved = ctx.ResolveEdge(e1.gid, v, view);
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->storage_, storage_.get()) << "a fork edge is returned as main's, never as its stub";
  }
}

TEST_F(VersioningBranchEngineTest, ForkStateReadsHideBranchDeletesAndUnionNativeEdges) {
  auto const v = CreateMainVertex({}, {});
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  auto const va = CreateMainEdge(v, a, "E");
  auto const vb = CreateMainEdge(v, b, "E");
  Fork();
  SeedVertex(b, [](StubState &s) { s.MarkDeleted(); });
  SeedEdge(va, [](StubState &s) { s.MarkDeleted(); });
  SeedVertex(v, [](StubState &) {});  // the anchor of a native edge

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  EXPECT_FALSE(ctx.IsVertexDeleted(a, ms::View::NEW));
  EXPECT_TRUE(ctx.IsVertexDeleted(b, ms::View::NEW));
  EXPECT_TRUE(ctx.HasVertexStub(b));
  EXPECT_TRUE(ctx.HasVertexStub(a)) << "hiding its in-edge gave it a stub, though its own state is unchanged";
  EXPECT_FALSE(ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto stub_v = ctx.FindStub(v, ms::View::NEW);
  ASSERT_TRUE(stub_v.has_value());
  EXPECT_TRUE(ctx.FindStub(a, ms::View::NEW).has_value()) << "a stub that holds only a marker entry";
  EXPECT_FALSE(ctx.FindStub(ms::Gid::FromUint(Core().vertex_gid_floor()), ms::View::NEW).has_value());
  auto native = CreateNative(diff_acc.get());
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &*stub_v, &native);
  EXPECT_TRUE(ctx.FindDiffEdge(native_edge.Gid(), ms::View::NEW).has_value())
      << "an edge out of a stub is found through the stubs";
  EXPECT_TRUE(ctx.FindDiffEdge(native_edge.Gid(), v, ms::View::NEW).has_value());
  EXPECT_FALSE(ctx.FindDiffEdge(va.gid, ms::View::NEW).has_value()) << "fork edges are not diff edges";
  EXPECT_FALSE(ctx.FindDiffEdge(va.gid, v, ms::View::NEW).has_value());
  EXPECT_TRUE(ctx.ResolveEdge(native_edge.Gid(), v, ms::View::NEW).has_value());

  auto fva = ctx.FindForkEdge(va.gid, v);
  auto fvb = ctx.FindForkEdge(vb.gid, v);
  ASSERT_TRUE(fva && fvb);
  EXPECT_TRUE(ctx.IsForkEdgeHidden(*fva, ms::View::NEW)) << "deleted on the branch";
  EXPECT_TRUE(ctx.IsForkEdgeHidden(*fvb, ms::View::NEW)) << "its endpoint B is deleted";
  EXPECT_FALSE(ctx.IsForkEdgeHidden(native_edge, ms::View::NEW)) << "a native edge is never fork-hidden";

  auto out = ctx.ResolveEdges(v, ms::EdgeDirection::OUT, ms::View::NEW);
  ASSERT_EQ(out.size(), 1u) << "only the native edge survives V's out-adjacency";
  EXPECT_EQ(out[0].Gid(), native_edge.Gid());
  auto fv = ctx.ResolveVertex(v, ms::View::NEW);
  ASSERT_TRUE(fv.has_value());
  auto out_of_accessor = ctx.OutEdges(*fv, ms::View::NEW);
  ASSERT_TRUE(out_of_accessor.has_value());
  ASSERT_EQ(out_of_accessor->size(), 1u);
  EXPECT_TRUE(ctx.ResolveEdges(a, ms::EdgeDirection::IN, ms::View::NEW).empty());
  bool b_deleted = false;
  EXPECT_TRUE(ctx.ResolveEdges(b, ms::EdgeDirection::IN, ms::View::NEW, &b_deleted).empty());
  EXPECT_TRUE(b_deleted);

  std::vector<uint64_t> scanned;
  for (auto vertex : ctx.Vertices(ms::View::NEW)) {
    scanned.push_back(vertex.Gid().AsUint());
    EXPECT_EQ(vertex.storage_, vertex.Gid() == native.Gid() ? static_cast<ms::Storage *>(&Diff()) : storage_.get());
  }
  EXPECT_EQ(scanned, (std::vector<uint64_t>{v.AsUint(), a.AsUint(), native.Gid().AsUint()}))
      << "the deleted fork vertex is skipped; the stub is not a second V";
}

TEST_F(VersioningBranchEngineTest, NativeEdgeLookupsNeverThrowOrReturnAnEdgeStub) {
  auto const v = CreateMainVertex({}, {});
  auto const w = CreateMainVertex({}, {});
  auto const e = CreateMainEdge(v, w, "E", {{Prop("x"), Int(1)}});
  Fork();
  SeedEdge(e, [&](StubState &s) { s.SetProperty(Prop("x"), Int(2)); });
  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  auto &ctx = Ctx();

  EXPECT_NE(Core().edge_stub_index().Find(e.gid.AsUint()), nullptr);
  EXPECT_FALSE(ctx.FindDiffEdge(e.gid, ms::View::NEW).has_value());
  EXPECT_FALSE(ctx.FindDiffEdge(e.gid, v, ms::View::NEW).has_value());
  auto resolved = ctx.ResolveEdge(e.gid, v, ms::View::NEW);
  ASSERT_TRUE(resolved.has_value());
  EXPECT_EQ(resolved->storage_, storage_.get());

  // A native edge between native vertices, looked up with a fork FROM that has no stub: not found, and no throw (the
  // stock lookup by FROM gid throws for a vertex the diff engine does not hold).
  auto n = CreateNative(diff_acc.get());
  auto m = CreateNative(diff_acc.get());
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &n, &m);
  EXPECT_TRUE(ctx.FindDiffEdge(native_edge.Gid(), n.Gid(), ms::View::NEW).has_value());
  EXPECT_FALSE(ctx.FindDiffEdge(native_edge.Gid(), v, ms::View::NEW).has_value());
}

TEST_F(VersioningBranchEngineTest, ANativeVertexDeletedInThisTransactionReportsDeletedWhenItsEdgesAreRead) {
  Fork();
  auto setup = Diff().Access(ms::WRITE);
  auto isolated = CreateNative(setup.get());
  auto const gid = isolated.Gid();
  Commit(*setup);

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  auto native = ctx.ResolveVertex(gid, ms::View::NEW);
  ASSERT_TRUE(native.has_value());
  EXPECT_EQ(native->storage_, static_cast<ms::Storage *>(&Diff()));
  ASSERT_TRUE(ctx.OutEdges(*native, ms::View::NEW).has_value());
  ASSERT_TRUE(diff_acc->DeleteVertex(&*native).has_value());

  EXPECT_EQ(ctx.OutEdges(*native, ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(ctx.InEdges(*native, ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
  EXPECT_TRUE(ctx.OutEdges(*native, ms::View::OLD).has_value()) << "the delete is not visible to OLD yet";
  // By gid alone the deleted vertex is not found at all, so the call cannot tell: that is what the accessor form is
  // for.
  bool deleted = true;
  EXPECT_TRUE(ctx.ResolveEdges(gid, ms::EdgeDirection::OUT, ms::View::NEW, &deleted).empty());
  EXPECT_FALSE(deleted);
}

// A delete of a fork vertex and the creation of a native edge to it cannot both commit: both write the vertex's stub,
// so stock first-writer-wins decides. This is why a native edge never has a deleted fork endpoint to filter.
TEST_F(VersioningBranchEngineTest, ADeleteOfAForkVertexAndANativeEdgeToItConflictOnItsStub) {
  auto const v = CreateMainVertex({}, {});
  auto const w = CreateMainVertex({}, {});
  Fork();
  SeedVertex(v, [](StubState &) {});
  SeedVertex(w, [](StubState &) {});
  for (bool edge_first : {true, false}) {
    SCOPED_TRACE(edge_first);
    auto attach = Diff().Access(ms::WRITE);
    auto remove = Diff().Access(ms::WRITE);
    ms::VertexAccessor stub_v{Core().stub_index().FindStub(v.AsUint()), &Diff(), attach->GetTransaction()};
    ms::VertexAccessor stub_w{Core().stub_index().FindStub(w.AsUint()), &Diff(), attach->GetTransaction()};
    std::optional<ms::Error> attach_error, remove_error;
    auto const do_attach = [&] {
      auto edge = CreateNativeEdgeResult(attach.get(), &stub_v, &stub_w);
      if (!edge) attach_error = edge.error();
    };
    auto const do_remove = [&] { remove_error = WriteVertexStub(*remove, w, [](StubState &s) { s.MarkDeleted(); }); };
    if (edge_first) {
      do_attach();
      do_remove();
      EXPECT_FALSE(attach_error.has_value());
      EXPECT_EQ(remove_error, std::optional{ms::Error::SERIALIZATION_ERROR});
    } else {
      do_remove();
      do_attach();
      EXPECT_FALSE(remove_error.has_value());
      EXPECT_EQ(attach_error, std::optional{ms::Error::SERIALIZATION_ERROR});
    }
    attach->Abort();
    remove->Abort();
  }
}

TEST_F(VersioningBranchEngineTest, TheTransactionStateFollowsOpenHistoricalSetDiffTxnAndEnd) {
  Fork();
  auto &ctx = Ctx();
  EXPECT_FALSE(ctx.in_transaction());
  EXPECT_EQ(ctx.current_diff_txn(), nullptr);
  EXPECT_DEATH((void)ctx.historical(), "no branch transaction is open");

  ASSERT_TRUE(ctx.OpenHistorical().has_value());
  EXPECT_FALSE(ctx.in_transaction()) << "half open: the diff accessor does not exist yet";
  EXPECT_EQ(ctx.current_diff_txn(), nullptr);
  (void)ctx.historical();
  EXPECT_DEATH((void)ctx.ResolveVertex(ms::Gid::FromUint(0), ms::View::NEW), "no branch transaction is open");
  EXPECT_DEATH(ctx.SetCurrentDiffTxn(nullptr), "diff accessor is null");

  auto diff_acc = Diff().Access(ms::READ);
  ctx.SetCurrentDiffTxn(diff_acc.get());
  EXPECT_TRUE(ctx.in_transaction());
  EXPECT_EQ(ctx.current_diff_txn(), diff_acc.get());
  EXPECT_DEATH(ctx.SetCurrentDiffTxn(diff_acc.get()), "OpenHistorical must run first");

  ctx.EndTransaction();
  ctx.EndTransaction();
  EXPECT_FALSE(ctx.in_transaction());
  EXPECT_DEATH(ctx.SetCurrentDiffTxn(diff_acc.get()), "OpenHistorical must run first");

  // Opening again drops whatever was open before.
  Begin(diff_acc.get());
  ASSERT_TRUE(ctx.OpenHistorical().has_value());
  EXPECT_FALSE(ctx.in_transaction());
}

// A branch transaction opens only while main still pins the fork.
TEST_F(VersioningBranchEngineTest, OpenHistoricalFailsOnceTheForkIsNoLongerPinned) {
  Fork();
  ASSERT_TRUE(Ctx().OpenHistorical().has_value());
  Ctx().EndTransaction();

  storage_->ReleaseForkPin(*fork_ts_);
  fork_ts_.reset();
  auto reopened = Ctx().OpenHistorical();
  ASSERT_FALSE(reopened.has_value());
  EXPECT_NE(reopened.error().message.find("no longer retained"), std::string::npos) << reopened.error().message;
  EXPECT_FALSE(Ctx().in_transaction());
}

TEST_F(VersioningBranchEngineTest, OpenHistoricalNamesTheStorageModeWhenMainLeftTransactionalMode) {
  Fork();
  storage_->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  auto reopened = Ctx().OpenHistorical();
  storage_->SetStorageMode(ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_NE(reopened.error().message.find("IN_MEMORY_TRANSACTIONAL"), std::string::npos) << reopened.error().message;
  EXPECT_EQ(reopened.error().message.find("no longer retained"), std::string::npos) << reopened.error().message;
}

// ---- Randomized: the lazy single-key reads against a model of the merged object.

struct ModelVertex {
  std::set<ms::LabelId> fork_labels, labels;
  std::map<ms::PropertyId, int64_t> fork_props, props;
  bool deleted{false};
};

TEST_F(VersioningBranchEngineTest, LazyStubReadsMatchAModelOfTheMergedObject) {
  constexpr int kVertices = 30;
  constexpr int kRounds = 8;
  constexpr uint32_t kIds = 6;
  std::mt19937 rng{12345};
  auto const label = [&](uint32_t i) { return Label(("L" + std::to_string(i)).c_str()); };
  auto const prop = [&](uint32_t i) { return Prop(("p" + std::to_string(i)).c_str()); };

  std::vector<ms::Gid> gids;
  std::vector<ModelVertex> model(kVertices);
  for (int i = 0; i < kVertices; ++i) {
    std::vector<ms::LabelId> labels;
    std::map<ms::PropertyId, ms::PropertyValue> props;
    for (uint32_t id = 0; id < kIds; ++id) {
      if (rng() % 2 == 0) {
        model[i].fork_labels.insert(label(id));
        labels.push_back(label(id));
      }
      if (rng() % 2 == 0) {
        model[i].fork_props[prop(id)] = 100 + id;
        props.emplace(prop(id), Int(100 + id));
      }
    }
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    for (auto const l : labels) ASSERT_TRUE(v.AddLabel(l).has_value());
    for (auto const &[pid, value] : props) ASSERT_TRUE(v.SetProperty(pid, value).has_value());
    gids.push_back(v.Gid());
    Commit(*acc);
    model[i].labels = model[i].fork_labels;
    model[i].props = model[i].fork_props;
  }
  Fork();

  for (int round = 0; round < kRounds; ++round) {
    SCOPED_TRACE(round);
    auto writer = Diff().Access(ms::WRITE);
    for (int i = 0; i < kVertices; ++i) {
      if (model[i].deleted || rng() % 3 != 0) continue;
      auto &m = model[i];
      ASSERT_FALSE(WriteVertexStub(*writer, gids[i], [&](StubState &s) {
                     if (rng() % 12 == 0) {
                       s.MarkDeleted();
                       m.deleted = true;
                       return;
                     }
                     for (int edit = 0; edit < 3; ++edit) {
                       auto const id = static_cast<uint32_t>(rng() % kIds);
                       if (rng() % 2 == 0) {
                         auto const l = label(id);
                         bool const in_fork = m.fork_labels.contains(l);
                         if (m.labels.contains(l)) {
                           in_fork ? s.RemoveLabel(l) : s.DropAddedLabel(l);
                           m.labels.erase(l);
                         } else {
                           in_fork ? s.UnhideLabel(l) : s.AddLabel(l);
                           m.labels.insert(l);
                         }
                       } else {
                         auto const p = prop(id);
                         if (rng() % 3 == 0) {
                           if (!m.props.contains(p)) continue;
                           if (m.fork_props.contains(p)) {
                             s.RemoveProperty(p);
                           } else {
                             s.properties.SetProperty(p, ms::PropertyValue{});
                           }
                           m.props.erase(p);
                         } else {
                           auto const value = static_cast<int64_t>(rng() % 1000);
                           s.SetProperty(p, Int(value));
                           m.props[p] = value;
                         }
                       }
                     }
                   }).has_value());
    }
    Commit(*writer);

    auto reader = Diff().Access(ms::READ);
    Begin(reader.get());
    auto &ctx = Ctx();
    for (int i = 0; i < kVertices; ++i) {
      SCOPED_TRACE(i);
      auto const &m = model[i];
      auto fork = ctx.FindForkVertex(gids[i]);
      ASSERT_TRUE(fork.has_value());
      EXPECT_EQ(ctx.ResolveVertex(gids[i], ms::View::NEW).has_value(), !m.deleted);
      for (auto const view : {ms::View::OLD, ms::View::NEW}) {
        if (m.deleted) {
          EXPECT_EQ(ctx.Labels(*fork, view).error(), ms::Error::DELETED_OBJECT);
          EXPECT_EQ(ctx.HasAnyLabel(*fork, view).error(), ms::Error::DELETED_OBJECT);
          EXPECT_EQ(ctx.Properties(*fork, view).error(), ms::Error::DELETED_OBJECT);
          EXPECT_EQ(ctx.GetProperty(*fork, prop(0), view).error(), ms::Error::DELETED_OBJECT);
          EXPECT_EQ(ctx.HasLabel(*fork, label(0), view).error(), ms::Error::DELETED_OBJECT);
          continue;
        }
        EXPECT_EQ(LabelSet(ctx.Labels(*fork, view)), m.labels);
        EXPECT_EQ(*ctx.HasAnyLabel(*fork, view), !m.labels.empty());
        std::map<ms::PropertyId, ms::PropertyValue> expected;
        for (auto const &[pid, value] : m.props) expected.emplace(pid, Int(value));
        EXPECT_EQ(*ctx.Properties(*fork, view), expected);
        for (uint32_t id = 0; id < kIds; ++id) {
          EXPECT_EQ(*ctx.HasLabel(*fork, label(id), view), m.labels.contains(label(id)));
          auto const it = m.props.find(prop(id));
          EXPECT_EQ(*ctx.GetProperty(*fork, prop(id), view),
                    it == m.props.end() ? ms::PropertyValue{} : Int(it->second));
          EXPECT_EQ(*ctx.GetPropertySize(*fork, prop(id), view) != 0, it != m.props.end());
        }
      }
    }
    ctx.EndTransaction();
  }
}

// ---- Isolation: a stub commit is a diff-engine commit, read under the reader's own isolation level.

TEST_F(VersioningBranchEngineTest, SnapshotAndReadCommittedBranchReadersSeeAStubCommitAsTheirIsolationAllows) {
  auto const l1 = Label("L1");
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  auto const b = CreateMainVertex({}, {{x, Int(2)}});
  Fork();
  SeedVertex(b, [](StubState &) {});  // a stub that exists but changes nothing yet

  mv::BranchContext snapshot_ctx{branch_context_->core()};
  mv::BranchContext committed_ctx{branch_context_->core()};
  auto snapshot_acc = Diff().Access(ms::READ, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
  auto committed_acc = Diff().Access(ms::READ, ms::IsolationLevel::READ_COMMITTED, std::nullopt);
  ASSERT_TRUE(snapshot_ctx.BeginTransaction(snapshot_acc.get()).has_value());
  ASSERT_TRUE(committed_ctx.BeginTransaction(committed_acc.get()).has_value());

  SeedVertex(a, [&](StubState &s) {
    s.SetProperty(x, Int(10));
    s.AddLabel(l1);
  });
  SeedVertex(b, [](StubState &s) { s.MarkDeleted(); });

  auto const read = [&](mv::BranchContext &ctx, ms::Gid gid) { return *ctx.FindForkVertex(gid); };
  auto const old_a = read(snapshot_ctx, a);
  EXPECT_EQ(*snapshot_ctx.GetProperty(old_a, x, ms::View::NEW), Int(1));
  EXPECT_FALSE(*snapshot_ctx.HasLabel(old_a, l1, ms::View::NEW));
  EXPECT_FALSE(snapshot_ctx.IsForkVertexDeleted(read(snapshot_ctx, b), ms::View::NEW));
  EXPECT_TRUE(snapshot_ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto const new_a = read(committed_ctx, a);
  EXPECT_EQ(*committed_ctx.GetProperty(new_a, x, ms::View::NEW), Int(10));
  EXPECT_TRUE(*committed_ctx.HasLabel(new_a, l1, ms::View::NEW));
  EXPECT_TRUE(committed_ctx.IsForkVertexDeleted(read(committed_ctx, b), ms::View::NEW));
  EXPECT_FALSE(committed_ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto later = Diff().Access(ms::READ);
  Begin(later.get());
  EXPECT_EQ(*Ctx().GetProperty(read(Ctx(), a), x, ms::View::OLD), Int(10));
  Ctx().EndTransaction();
  snapshot_ctx.EndTransaction();
  committed_ctx.EndTransaction();
}

// A branch write is invisible to View::OLD of its own transaction until the command advances: the diff transaction's
// own command id, which the branch reads rest on.
TEST_F(VersioningBranchEngineTest, ABranchWriteIsInvisibleToOldUntilTheCommandAdvances) {
  auto const x = Prop("x");
  auto const l = Label("L");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto fork = *Ctx().FindForkVertex(a);
  ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value());
  ASSERT_TRUE(Ctx().AddLabel(fork, l).has_value());
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(2));
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::OLD), Int(1));
  EXPECT_TRUE(*Ctx().HasLabel(fork, l, ms::View::NEW));
  EXPECT_FALSE(*Ctx().HasLabel(fork, l, ms::View::OLD));
  ASSERT_TRUE(*Ctx().DeleteForkVertex(a));
  EXPECT_TRUE(Ctx().IsVertexDeleted(a, ms::View::NEW));
  EXPECT_FALSE(Ctx().IsVertexDeleted(a, ms::View::OLD));
  acc->AdvanceCommand();
  EXPECT_EQ(Ctx().GetProperty(fork, x, ms::View::OLD).error(), ms::Error::DELETED_OBJECT);
  EXPECT_TRUE(Ctx().IsVertexDeleted(a, ms::View::OLD));
  Ctx().EndTransaction();
  acc->Abort();
}

// ---- Scans and concurrency

// Fork vertices' x on the branch at `view`; native vertices under their gid too.
std::map<uint64_t, int64_t> BranchValues(mv::BranchContext &ctx, ms::PropertyId prop, ms::View view) {
  std::map<uint64_t, int64_t> out;
  for (auto v : ctx.Vertices(view)) {
    auto value = ctx.GetProperty(v, prop, view);
    if (!value) {
      ADD_FAILURE() << "property read failed";
      continue;
    }
    out.emplace(v.Gid().AsUint(), value->IsNull() ? -1 : value->ValueInt());
  }
  return out;
}

TEST_F(VersioningBranchEngineTest, ResolveAndUnionScanMergeStubsAndIncludeNatives) {
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  auto const b = CreateMainVertex({}, {{x, Int(9)}});
  Fork();
  SeedVertex(a, [&](StubState &s) { s.SetProperty(x, Int(2)); });

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  auto c = CreateNative(diff_acc.get());
  auto d = CreateNative(diff_acc.get());
  ASSERT_TRUE(c.SetProperty(x, Int(7)).has_value());
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &c, &d);
  EXPECT_FALSE(Ctx().IsForkCandidateVertex(c.Gid())) << "a native vertex must not reuse a fork-range gid";
  EXPECT_FALSE(Ctx().IsForkCandidateEdge(native_edge.Gid())) << "a native edge must not reuse a fork-range gid";

  auto resolved_a = Ctx().ResolveVertex(a, ms::View::NEW);
  ASSERT_TRUE(resolved_a.has_value());
  EXPECT_EQ(*Ctx().GetProperty(*resolved_a, x, ms::View::NEW), Int(2));
  EXPECT_EQ(*Ctx().GetProperty(*Ctx().ResolveVertex(b, ms::View::NEW), x, ms::View::NEW), Int(9)) << "B is unchanged";
  EXPECT_EQ(*Ctx().GetProperty(*Ctx().ResolveVertex(c.Gid(), ms::View::NEW), x, ms::View::NEW), Int(7));

  std::vector<uint64_t> order;
  for (auto v : Ctx().Vertices(ms::View::NEW)) order.push_back(v.Gid().AsUint());
  EXPECT_EQ(order.size(), 4u) << "A, B, C, D must each be yielded exactly once";
  EXPECT_TRUE(std::ranges::is_sorted(order)) << "the union merge must be gid-ordered";
  auto const values = BranchValues(Ctx(), x, ms::View::NEW);
  EXPECT_EQ(values.at(a.AsUint()), 2) << "A must yield the stub's value, not the fork's stale 1";
  EXPECT_EQ(values.at(b.AsUint()), 9);
  EXPECT_EQ(values.at(c.Gid().AsUint()), 7);
}

TEST_F(VersioningBranchEngineTest, ManyReadsThenAWriteInOneBranchTransaction) {
  auto const x = Prop("x");
  std::vector<ms::Gid> gids;
  for (int i = 0; i < 8; ++i) gids.push_back(CreateMainVertex({}, {{x, Int(i)}}));
  Fork();
  SeedVertex(gids[0], [&](StubState &s) { s.SetProperty(x, Int(100)); });
  SeedVertex(gids[1], [](StubState &s) { s.MarkDeleted(); });

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  std::map<uint64_t, int64_t> expected;
  for (size_t i = 2; i < gids.size(); ++i) expected.emplace(gids[i].AsUint(), static_cast<int64_t>(i));
  expected.emplace(gids[0].AsUint(), 100);

  {  // main moves on after the fork; the branch never sees it
    auto acc = storage_->Access(ms::WRITE);
    ASSERT_TRUE(acc->FindVertex(gids[2], ms::View::OLD)->SetProperty(x, Int(999)).has_value());
    acc->CreateVertex();
    Commit(*acc);
  }
  for (int round = 0; round < 50; ++round) {
    ASSERT_EQ(BranchValues(Ctx(), x, ms::View::NEW), expected);
    ASSERT_EQ(BranchValues(Ctx(), x, ms::View::OLD), expected);
    ASSERT_FALSE(Ctx().ResolveVertex(gids[1], ms::View::NEW).has_value());
  }

  auto native = CreateNative(diff_acc.get());
  ASSERT_TRUE(native.SetProperty(x, Int(42)).has_value());
  EXPECT_EQ(BranchValues(Ctx(), x, ms::View::OLD), expected) << "OLD: the write is not visible yet";
  auto with_native = expected;
  with_native.emplace(native.Gid().AsUint(), 42);
  EXPECT_EQ(BranchValues(Ctx(), x, ms::View::NEW), with_native);
  diff_acc->AdvanceCommand();
  EXPECT_EQ(BranchValues(Ctx(), x, ms::View::OLD), with_native) << "visible to OLD after the command";
  EXPECT_TRUE(Ctx().ResolveVertex(native.Gid(), ms::View::OLD).has_value());
  Ctx().EndTransaction();
}

// One stub writer (every fork vertex in each commit), one native writer (two vertices per commit) and main writes
// against branch readers on their own contexts: each snapshot reader sees one whole snapshot on every scan, and a
// read-committed reader only ever sees values some commit wrote.
TEST_F(VersioningBranchEngineTest, ConcurrentBranchReadersAndWritersSeeStableSnapshots) {
  constexpr int kForkVertices = 8;
  constexpr int kRounds = 1000;
  auto const x = Prop("x");
  std::vector<ms::Gid> gids;
  for (int i = 0; i < kForkVertices; ++i) gids.push_back(CreateMainVertex({}, {{x, Int(0)}}));
  Fork();
  for (auto const gid : gids) SeedVertex(gid, [](StubState &) {});
  auto &diff = Diff();
  std::atomic<int> writers_left{3};
  std::atomic<int> snapshots{0};

  std::vector<std::jthread> threads;
  auto writer = [&](auto body) {
    threads.emplace_back([&writers_left, body] {
      body();
      --writers_left;
    });
  };
  writer([&] {
    for (int round = 1; round <= kRounds; ++round) {
      auto acc = diff.Access(ms::WRITE);
      for (auto const gid : gids) {
        ASSERT_FALSE(WriteVertexStub(*acc, gid, [&](StubState &s) { s.SetProperty(x, Int(round)); }).has_value());
      }
      Commit(*acc);
    }
  });
  writer([&] {
    for (int round = 0; round < kRounds; ++round) {
      auto acc = diff.Access(ms::WRITE);
      for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(CreateNative(acc.get()).SetProperty(x, Int(-2)).has_value());
      }
      Commit(*acc);
    }
  });
  writer([&] {
    for (int round = 0; round < kRounds; ++round) {
      auto acc = storage_->Access(ms::WRITE);
      ASSERT_TRUE(acc->FindVertex(gids[round % kForkVertices], ms::View::OLD)->SetProperty(x, Int(-round)).has_value());
      Commit(*acc);
    }
  });
  for (int reader = 0; reader < 3; ++reader) {
    threads.emplace_back([&, reader] {
      mv::BranchContext ctx{branch_context_->core()};
      int64_t last_round = 0;
      do {
        if (reader == 2) {  // read committed: each value is some commit's, and never goes back
          auto acc = diff.Access(ms::READ, ms::IsolationLevel::READ_COMMITTED, std::nullopt);
          ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
          for (auto const gid : gids) {
            auto v = ctx.FindForkVertex(gid);
            auto const value = ctx.GetProperty(*v, x, ms::View::NEW)->ValueInt();
            ASSERT_GE(value, 0);
            ASSERT_LE(value, kRounds);
          }
          ctx.EndTransaction();
          ++snapshots;
          continue;
        }
        auto acc = diff.Access(ms::READ);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto const first = BranchValues(ctx, x, ms::View::OLD);
        ASSERT_EQ(first.size() % 2, kForkVertices % 2u) << "native vertices are committed in pairs";
        auto const round = first.at(gids[0].AsUint());
        for (auto const gid : gids) ASSERT_EQ(first.at(gid.AsUint()), round) << "a stub commit is all-or-nothing";
        ASSERT_GE(round, last_round);
        last_round = round;
        for (int i = 0; i < 3; ++i) ASSERT_EQ(BranchValues(ctx, x, ms::View::NEW), first);
        ctx.EndTransaction();
        ++snapshots;
      } while (writers_left.load() > 0);
    });
  }
  threads.clear();
  EXPECT_GT(snapshots.load(), 3);
  auto acc = diff.Access(ms::READ);
  Begin(acc.get());
  auto const final_values = BranchValues(Ctx(), x, ms::View::OLD);
  EXPECT_EQ(final_values.size(), kForkVertices + (2 * kRounds));
  for (auto const gid : gids) EXPECT_EQ(final_values.at(gid.AsUint()), kRounds);
  Ctx().EndTransaction();
}

// A property flipped between the branch's value and a removal, over a fork value neither of which is: whatever the
// isolation level, a branch read is one of the two, never the fork's. (stub_merge.hpp, versioning_stub_mvcc.cpp.)
TEST_F(VersioningBranchEngineTest, ABranchReadOfAFlippedPropertyNeverShowsTheForkValue) {
  constexpr int kFlips = 1500;
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(100)}});
  Fork();
  SeedVertex(a, [&](StubState &s) { s.SetProperty(x, Int(5)); });
  std::atomic<bool> stop{false};
  std::atomic<int> wrong{0};
  std::vector<std::thread> readers;
  for (auto const level : {ms::IsolationLevel::READ_COMMITTED, ms::IsolationLevel::SNAPSHOT_ISOLATION}) {
    readers.emplace_back([&, level] {
      mv::BranchContext ctx{branch_context_->core()};
      while (!stop.load(std::memory_order_acquire)) {
        auto acc = Diff().Access(ms::READ, level, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto fork = *ctx.FindForkVertex(a);
        auto const value = ctx.GetProperty(fork, x, ms::View::NEW);
        auto const props = ctx.Properties(fork, ms::View::NEW);
        if (!value || (!value->IsNull() && *value != Int(5))) wrong.fetch_add(1);
        if (!props || (props->contains(x) && props->at(x) != Int(5))) wrong.fetch_add(1);
        ctx.EndTransaction();
      }
    });
  }
  bool removed = false;
  for (int i = 0; i < kFlips; ++i) {
    SeedVertex(a, [&](StubState &s) {
      if (removed) {
        s.SetProperty(x, Int(5));
      } else {
        s.RemoveProperty(x);
      }
    });
    removed = !removed;
  }
  stop.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(wrong.load(), 0);
}

// ---- Gid floors: main objects created after fork_ts but before the lazy Build are below the floor and invisible.

class VersioningGidFloorTest : public VersioningBranchEngineTest {
 protected:
  void Run();
};

class VersioningGidFloorNoEdgePropertiesTest : public VersioningGidFloorTest {
 protected:
  bool PropertiesOnEdges() const override { return false; }
};

void VersioningGidFloorTest::Run() {
  auto &storage = *storage_;
  auto const v1 = CreateMainVertex({}, {});
  auto const w1 = CreateMainVertex({}, {});
  auto const e1 = CreateMainEdge(v1, w1, "E");
  // Transactions open (gids allocated) at the fork pin, committing later.
  auto inflight = storage.Access(ms::WRITE);
  auto inflight_x = inflight->CreateVertex();
  auto const inflight_x_gid = inflight_x.Gid();
  auto inflight_v1 = inflight->FindVertex(v1, ms::View::NEW);
  auto const inflight_e_gid = inflight->CreateEdge(&*inflight_v1, &inflight_x, storage.NameToEdgeType("E"))->Gid();
  auto late = storage.Access(ms::WRITE);
  auto const late_y = late->CreateVertex().Gid();

  ForkNow();
  auto const v2 = CreateMainVertex({}, {});
  auto const w2 = CreateMainVertex({}, {});
  auto const e2 = CreateMainEdge(v2, w2, "E");
  auto const e3 = CreateMainEdge(v1, v2, "E");  // fork vertex -> post-fork vertex
  Commit(*inflight);
  inflight.reset();

  // Lazy first build, after all of the above.
  auto built = mv::BranchContext::BuildFromFork(storage, *fork_ts_);
  ASSERT_TRUE(built.has_value());
  branch_context_ = std::move(*built);
  auto &ctx = Ctx();
  auto const vertex_floor = Core().vertex_gid_floor();
  auto const edge_floor = Core().edge_gid_floor();
  EXPECT_TRUE(Diff().config_.salient.items.properties_on_edges) << "the diff engine always has them";

  std::vector<ms::Gid> const invisible_vertices{v2, w2, inflight_x_gid, late_y};
  std::vector<std::pair<ms::Gid, ms::Gid>> const invisible_edges{{e2.gid, v2}, {e3.gid, v1}, {inflight_e_gid, v1}};
  // Claim: every post-fork object has a gid below the floor (the floor is read at Build, not at the fork).
  for (auto gid : invisible_vertices) EXPECT_LT(gid.AsUint(), vertex_floor);
  for (auto const &[gid, from] : invisible_edges) EXPECT_LT(gid.AsUint(), edge_floor);

  auto const check_invisible = [&](bool with_natives) {
    for (auto gid : invisible_vertices) {
      EXPECT_TRUE(ctx.IsForkCandidateVertex(gid)) << gid.AsUint();
      for (auto view : {ms::View::OLD, ms::View::NEW}) {
        EXPECT_FALSE(ctx.ResolveVertex(gid, view).has_value()) << gid.AsUint();
        EXPECT_FALSE(ctx.FindStub(gid, view).has_value()) << gid.AsUint();
      }
      EXPECT_FALSE(ctx.FindForkVertex(gid).has_value()) << gid.AsUint();
      EXPECT_FALSE(ctx.HasVertexStub(gid));
      EXPECT_FALSE(ctx.IsVertexDeleted(gid, ms::View::NEW));
      EXPECT_TRUE(ctx.ResolveEdges(gid, ms::EdgeDirection::OUT, ms::View::NEW).empty()) << gid.AsUint();
      EXPECT_TRUE(ctx.ResolveEdges(gid, ms::EdgeDirection::IN, ms::View::NEW).empty()) << gid.AsUint();
    }
    for (auto const &[gid, from] : invisible_edges) {
      EXPECT_TRUE(ctx.IsForkCandidateEdge(gid)) << gid.AsUint();
      EXPECT_FALSE(ctx.FindDiffEdge(gid, ms::View::NEW).has_value());
      EXPECT_FALSE(ctx.ResolveEdge(gid, from, ms::View::NEW).has_value()) << gid.AsUint();
    }
    EXPECT_FALSE(ctx.FindForkEdge(e2.gid, v2).has_value());
    EXPECT_FALSE(ctx.FindForkEdge(e3.gid, v1).has_value());
    EXPECT_FALSE(ctx.FindForkEdge(inflight_e_gid, v1).has_value());
    // The fork vertex does not expand to the post-fork object, in either direction.
    std::vector<uint64_t> out_edges;
    for (auto e : ctx.ResolveEdges(v1, ms::EdgeDirection::OUT, ms::View::NEW)) out_edges.push_back(e.Gid().AsUint());
    EXPECT_EQ(std::ranges::count(out_edges, e1.gid.AsUint()), 1);
    EXPECT_EQ(out_edges.size(), with_natives ? 2u : 1u)
        << "e1, plus the native edge off the v1 anchor; never e3 / inflight_e";
    EXPECT_TRUE(ctx.ResolveEdges(v1, ms::EdgeDirection::IN, ms::View::NEW).empty());
    std::vector<uint64_t> scanned;
    for (auto v : ctx.Vertices(ms::View::NEW)) scanned.push_back(v.Gid().AsUint());
    EXPECT_EQ(scanned.size(), with_natives ? 4u : 2u);
    for (auto gid : invisible_vertices) EXPECT_EQ(std::ranges::count(scanned, gid.AsUint()), 0);
  };

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  check_invisible(false);

  // Native creates are at or above the floor and collide with nothing, including the post-fork main gids.
  auto n1 = CreateNative(diff_acc.get());
  auto n2 = CreateNative(diff_acc.get());
  EXPECT_GE(n1.Gid().AsUint(), vertex_floor);
  EXPECT_FALSE(ctx.IsForkCandidateVertex(n1.Gid()));
  for (auto gid : invisible_vertices) EXPECT_NE(n1.Gid(), gid);
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &n1, &n2);
  EXPECT_GE(native_edge.Gid().AsUint(), edge_floor);
  EXPECT_FALSE(ctx.IsForkCandidateEdge(native_edge.Gid()));
  for (auto const &[gid, from] : invisible_edges) EXPECT_NE(native_edge.Gid(), gid);
  EXPECT_TRUE(ctx.ResolveVertex(n1.Gid(), ms::View::NEW).has_value());
  EXPECT_TRUE(ctx.ResolveEdge(native_edge.Gid(), n1.Gid(), ms::View::NEW).has_value());
  // A native edge off a fork vertex's anchor.
  ASSERT_FALSE(WriteVertexStub(*diff_acc, v1, [](StubState &) {}).has_value());
  auto anchor = *ctx.FindStub(v1, ms::View::NEW);
  auto const anchored = CreateNativeEdge(diff_acc.get(), &anchor, &n2);
  EXPECT_GE(anchored.Gid().AsUint(), edge_floor);
  check_invisible(true);  // unchanged by the branch's own writes (v1 now has an anchor)
  Commit(*diff_acc);
  Ctx().EndTransaction();

  // Main's transaction that was open at the pin commits only now, after Build: still invisible.
  Commit(*late);
  late.reset();
  auto diff2 = Diff().Access(ms::READ);
  Begin(diff2.get());
  check_invisible(true);
  Ctx().EndTransaction();
}

TEST_F(VersioningGidFloorTest, PostForkPreBuildMainObjectsAreForkCandidatesButInvisible) { Run(); }

TEST_F(VersioningGidFloorNoEdgePropertiesTest, PostForkPreBuildMainObjectsAreForkCandidatesButInvisible) { Run(); }

// ---- Vertex writes

class VersioningBranchWriteTest : public VersioningBranchEngineTest {
 protected:
  using Props = std::map<ms::PropertyId, ms::PropertyValue>;

  // What the branch stored for fork vertex `gid`, as `ctx`'s transaction sees it.
  static StubState StubOf(mv::BranchContext &ctx, ms::Gid gid, ms::View view = ms::View::NEW) {
    auto stub = ctx.FindStub(gid, view);
    EXPECT_TRUE(stub.has_value());
    if (!stub) return {};
    auto decoded = stub::Decode(*stub, view);
    EXPECT_TRUE(decoded.has_value());
    return decoded ? std::move(*decoded) : StubState{};
  }

  static bool StubIsEmpty(const StubState &state) {
    return !state.deleted && state.added_labels.empty() && state.removed_labels.empty() &&
           state.properties.Properties().empty() && state.removed_pids.empty();
  }

  size_t StubCount() {
    size_t count = 0;
    Core().stub_index().ForEachStub([&](ms::Vertex *) {
      ++count;
      return true;
    });
    return count;
  }

  // The property ids of `stub`'s SET_PROPERTY deltas, newest write first.
  static std::vector<ms::PropertyId> PropertyWriteOrder(ms::Vertex *stub) {
    std::vector<ms::PropertyId> keys;
    std::shared_lock guard{stub->lock};
    for (const ms::Delta *delta = stub->delta(); delta != nullptr; delta = delta->next.load()) {
      if (delta->action == ms::Delta::Action::SET_PROPERTY) keys.push_back(delta->property.key);
    }
    return keys;
  }

  // Runs `body` in a committed branch write transaction.
  template <typename Body>
  void Write(Body &&body) {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    body();
    Commit(*acc);
    Ctx().EndTransaction();
  }

  // Runs `body` in a branch read transaction and leaves it open for the caller's checks.
  template <typename Body>
  void Read(Body &&body) {
    auto acc = Diff().Access(ms::READ);
    Begin(acc.get());
    body();
    Ctx().EndTransaction();
  }
};

TEST_F(VersioningBranchWriteTest, EnsureDiffVertexMaterializesAStubOnlyForTheTouchedVertex) {
  auto const la = Label("A");
  auto const x = Prop("x");
  auto const a = CreateMainVertex({la}, {{x, Int(1)}});
  auto const b = CreateMainVertex({}, {});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto const fork_a = *Ctx().FindForkVertex(a);
  EXPECT_EQ(StubCount(), 0u);
  EXPECT_FALSE(fork_a.vertex_->branched());

  auto stub = Ctx().EnsureDiffVertex(a);
  ASSERT_TRUE(stub.has_value());
  EXPECT_EQ(stub->Gid(), a);
  EXPECT_TRUE(fork_a.vertex_->branched()) << "main's hint bit is set before any stub write";
  EXPECT_EQ(StubCount(), 1u);
  EXPECT_FALSE(*stub->HasLabel(la, ms::View::NEW)) << "a fresh stub holds nothing of the fork state";
  EXPECT_TRUE(stub->GetProperty(x, ms::View::NEW)->IsNull());
  size_t engine_vertices = 0;
  for (auto v : acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++engine_vertices;
  }
  EXPECT_EQ(engine_vertices, 0u) << "stubs are not entries of the engine's vertices_";
  EXPECT_FALSE(Ctx().FindStub(b, ms::View::NEW).has_value()) << "an untouched vertex gets no stub";

  auto again = Ctx().EnsureDiffVertex(a);
  ASSERT_TRUE(again.has_value());
  EXPECT_EQ(again->vertex_, stub->vertex_) << "the same stub, not a second one";
  EXPECT_EQ(StubCount(), 1u);
  Ctx().EndTransaction();
  acc->Abort();
}

TEST_F(VersioningBranchWriteTest, EnsureDiffVertexRefusesWhatTheBranchCannotWrite) {
  auto const a = CreateMainVertex({}, {});
  ForkNow();
  auto const after_fork = CreateMainVertex({}, {});  // below the floor, but main@fork_ts does not have it
  auto built = mv::BranchContext::BuildFromFork(*storage_, *fork_ts_);
  ASSERT_TRUE(built.has_value());
  branch_context_ = std::move(*built);

  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto missing = Ctx().EnsureDiffVertex(after_fork);
  ASSERT_FALSE(missing.has_value());
  EXPECT_NE(missing.error().message.find("not in the branch's fork state"), std::string::npos)
      << missing.error().message;

  ASSERT_TRUE(*Ctx().DeleteForkVertex(a));
  auto deleted_fork = Ctx().EnsureDiffVertex(a);
  ASSERT_FALSE(deleted_fork.has_value());
  EXPECT_EQ(deleted_fork.error().message,
            "Cannot modify vertex " + std::to_string(a.AsUint()) + ": it was deleted on this branch.");

  auto native = Ctx().CreateNativeVertex();
  auto const native_gid = native.Gid();
  auto live = Ctx().EnsureDiffVertex(native_gid);
  ASSERT_TRUE(live.has_value());
  EXPECT_EQ(live->storage_, &Diff());
  EXPECT_FALSE(Ctx().IsForkCandidateVertex(live->Gid()));
  ASSERT_TRUE(acc->DeleteVertex(&native).has_value());
  auto deleted_native = Ctx().EnsureDiffVertex(native_gid);
  ASSERT_FALSE(deleted_native.has_value());
  EXPECT_EQ(deleted_native.error().message,
            "Cannot modify vertex " + std::to_string(native_gid.AsUint()) + ": it was deleted on this branch.");
  Ctx().EndTransaction();
  acc->Abort();
}

// What a stub holds is the difference to the fork and nothing the fork already implies; each write is a direct
// mutator, and whole-set writes never wipe the removal markers.
TEST_F(VersioningBranchWriteTest, VertexWritesMergeOverTheForkStateAndStoreOnlyTheDifference) {
  auto const la = Label("A"), lb = Label("B");
  auto const x = Prop("x"), y = Prop("y");
  auto const v = CreateMainVertex({la}, {{x, Int(1)}});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto fork = *Ctx().FindForkVertex(v);
  auto const labels = [&] { return LabelSet(Ctx().Labels(fork, ms::View::NEW)); };
  auto const props = [&] { return *Ctx().Properties(fork, ms::View::NEW); };
  auto const stub_of = [&] { return StubOf(Ctx(), v); };

  EXPECT_FALSE(*Ctx().AddLabel(fork, la)) << "the fork vertex already has A";
  EXPECT_TRUE(StubIsEmpty(stub_of())) << "no label the fork implies is recorded";
  EXPECT_TRUE(*Ctx().RemoveLabel(fork, la));
  EXPECT_EQ(stub_of().removed_labels, std::vector<ms::LabelId>{la});
  EXPECT_FALSE(*Ctx().RemoveLabel(fork, la)) << "already hidden";
  EXPECT_TRUE(*Ctx().AddLabel(fork, lb));
  EXPECT_EQ(stub_of().added_labels, std::vector<ms::LabelId>{lb});
  EXPECT_EQ(labels(), (std::set<ms::LabelId>{lb}));
  EXPECT_TRUE(*Ctx().AddLabel(fork, la)) << "un-hides the fork label";
  EXPECT_TRUE(*Ctx().RemoveLabel(fork, lb)) << "drops the added label";
  EXPECT_FALSE(*Ctx().RemoveLabel(fork, lb));
  EXPECT_EQ(labels(), (std::set<ms::LabelId>{la}));
  EXPECT_TRUE(StubIsEmpty(stub_of())) << "a full toggle leaves no trace";

  EXPECT_EQ(*Ctx().SetProperty(fork, x, Int(2)), Int(1)) << "the old value is the fork's";
  EXPECT_EQ(*Ctx().SetProperty(fork, x, ms::PropertyValue{}), Int(2)) << "then the stub's";
  EXPECT_TRUE(props().empty()) << "a null write removes the fork property";
  EXPECT_EQ(stub_of().removed_pids, std::vector<ms::PropertyId>{x});
  EXPECT_TRUE(stub_of().properties.Properties().empty());
  EXPECT_TRUE(Ctx().SetProperty(fork, y, ms::PropertyValue{})->IsNull());
  EXPECT_EQ(stub_of().removed_pids, std::vector<ms::PropertyId>{x}) << "no marker for a property the fork lacks";

  Props init{{y, Int(5)}};
  EXPECT_TRUE(*Ctx().InitProperties(fork, init));
  EXPECT_FALSE(*Ctx().InitProperties(fork, init)) << "init needs an empty vertex";
  Props update{{x, Int(3)}, {y, Int(6)}};
  auto changes = *Ctx().UpdateProperties(fork, update);
  ASSERT_EQ(changes.size(), 2u);
  for (auto const &[pid, old_value, new_value] : changes) {
    EXPECT_EQ(old_value, pid == x ? ms::PropertyValue{} : Int(5));
    EXPECT_EQ(new_value, pid == x ? Int(3) : Int(6));
  }
  Props const updated{{x, Int(3)}, {y, Int(6)}};
  EXPECT_EQ(props(), updated);
  EXPECT_TRUE(stub_of().removed_pids.empty()) << "setting x again took its marker off";

  Props more{{y, Int(7)}};
  Props const expected_more{{x, Int(3)}, {y, Int(7)}};
  ASSERT_TRUE(Ctx().UpdateProperties(fork, more).has_value());
  EXPECT_EQ(more, expected_more) << "the properties the update left alone come back in the caller's map";

  EXPECT_EQ(*Ctx().ClearProperties(fork), expected_more);
  EXPECT_TRUE(props().empty());
  EXPECT_EQ(stub_of().removed_pids, std::vector<ms::PropertyId>{x}) << "a whole-set write never wipes the markers";
  EXPECT_TRUE(Ctx().ClearProperties(fork)->empty());

  auto x_fork = Ctx().historical().FindVertex(v, ms::View::OLD)->GetProperty(x, ms::View::OLD);
  EXPECT_EQ(*x_fork, Int(1)) << "the fork state itself is unchanged";
  Ctx().EndTransaction();
  acc->Abort();
}

// Per key the marker goes on before the value is dropped and off after the value is set, so a READ_COMMITTED reader
// holding a prefix of the commit reads the old or the new value. The stub's delta chain, newest write first, shows it.
TEST_F(VersioningBranchWriteTest, DirectMutatorsKeepThePerKeyMarkerOrder) {
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  auto const marker = stub::Mark(x);
  auto const write_order = [&](const ms::PropertyValue &value) {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(Ctx().SetProperty(fork, x, value).has_value());
    auto order = PropertyWriteOrder(Core().stub_index().FindStub(v.AsUint()));
    Commit(*acc);
    Ctx().EndTransaction();
    return order;
  };
  EXPECT_EQ(write_order(Int(2)), std::vector<ms::PropertyId>{x}) << "a plain set is one write";
  EXPECT_EQ(write_order(ms::PropertyValue{}), (std::vector<ms::PropertyId>{x, marker}))
      << "remove: marker on, then the value dropped";
  EXPECT_EQ(write_order(Int(3)), (std::vector<ms::PropertyId>{marker, x}))
      << "set over a removal: the value first, then the marker off";
}

TEST_F(VersioningBranchWriteTest, ALabelAndAPropertyCanBeRemovedAndReaddedAcrossTransactions) {
  auto const la = Label("A"), lb = Label("B");
  auto const x = Prop("x"), y = Prop("y");
  auto const v = CreateMainVertex({la}, {{x, Int(1)}});
  Fork();
  auto const state = [&] {
    auto fork = *Ctx().FindForkVertex(v);
    return std::tuple{LabelSet(Ctx().Labels(fork, ms::View::NEW)), *Ctx().Properties(fork, ms::View::NEW)};
  };
  using Labels = std::set<ms::LabelId>;

  Write([&] {  // remove the fork label and property
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().RemoveLabel(fork, la));
    ASSERT_TRUE(Ctx().SetProperty(fork, x, ms::PropertyValue{}).has_value());
  });
  Read([&] { EXPECT_EQ(state(), (std::tuple{Labels{}, Props{}})); });
  Write([&] {  // read them
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().AddLabel(fork, la));
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(3)).has_value());
  });
  Read([&] {
    EXPECT_EQ(state(), (std::tuple{Labels{la}, Props{{x, Int(3)}}}));
    auto const stub = StubOf(Ctx(), v);
    EXPECT_TRUE(stub.removed_labels.empty() && stub.removed_pids.empty()) << "both marks are off";
  });
  Write([&] {  // a label and a property the fork lacks: add, remove, add; set, remove, set
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().AddLabel(fork, lb));
    ASSERT_TRUE(Ctx().SetProperty(fork, y, Int(1)).has_value());
  });
  Write([&] {
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().RemoveLabel(fork, lb));
    ASSERT_TRUE(Ctx().SetProperty(fork, y, ms::PropertyValue{}).has_value());
  });
  Read([&] {
    EXPECT_EQ(state(), (std::tuple{Labels{la}, Props{{x, Int(3)}}}));
    auto const stub = StubOf(Ctx(), v);
    EXPECT_TRUE(stub.added_labels.empty() && stub.removed_labels.empty() && stub.removed_pids.empty())
        << "a label or property the fork lacks leaves no marker when removed";
  });
  Write([&] {
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().AddLabel(fork, lb));
    ASSERT_TRUE(Ctx().SetProperty(fork, y, Int(2)).has_value());
  });
  Read([&] { EXPECT_EQ(state(), (std::tuple{Labels{la, lb}, Props{{x, Int(3)}, {y, Int(2)}}})); });
}

TEST_F(VersioningBranchWriteTest, ADeletedForkVertexRefusesEveryWriteAndASecondDelete) {
  auto const la = Label("A");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({la}, {{x, Int(1)}});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto fork = *Ctx().FindForkVertex(v);
  EXPECT_TRUE(*Ctx().DeleteForkVertex(v));
  EXPECT_FALSE(*Ctx().DeleteForkVertex(v)) << "a repeated delete is a no-op";

  Props props{{x, Int(2)}};
  EXPECT_EQ(Ctx().AddLabel(fork, Label("B")).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().RemoveLabel(fork, la).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().SetProperty(fork, x, Int(2)).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().InitProperties(fork, props).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().UpdateProperties(fork, props).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().ClearProperties(fork).error(), ms::Error::DELETED_OBJECT);
  EXPECT_TRUE(StubOf(Ctx(), v).deleted);
  EXPECT_TRUE(*Ctx().historical().FindVertex(v, ms::View::OLD)->HasLabel(la, ms::View::OLD))
      << "the fork state itself is unchanged";
  EXPECT_EQ(Ctx().DeleteForkVertex(ms::Gid::FromUint(Core().vertex_gid_floor() + 5)).error(),
            ms::Error::NONEXISTENT_OBJECT);
  Ctx().EndTransaction();
  acc->Abort();
}

TEST_F(VersioningBranchWriteTest, AbortedBranchWritesLeaveTheStubContentUnchangedAndBounded) {
  auto const la = Label("A"), lb = Label("B");
  auto const x = Prop("x"), y = Prop("y");
  auto const v = CreateMainVertex({la}, {{x, Int(1)}, {y, Int(2)}});
  auto const w = CreateMainVertex({la}, {});
  Fork();
  Write([&] {
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(*Ctx().AddLabel(fork, lb));
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(5)).has_value());
    ASSERT_TRUE(Ctx().SetProperty(fork, y, ms::PropertyValue{}).has_value());
  });
  StubState committed;
  Read([&] { committed = StubOf(Ctx(), v); });

  for (int round = 0; round < 50; ++round) {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto fork = *Ctx().FindForkVertex(v);
    Props update{{x, Int(round)}, {y, Int(round)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, update).has_value());
    ASSERT_TRUE(*Ctx().RemoveLabel(fork, la));
    ASSERT_TRUE(*Ctx().RemoveLabel(fork, lb));
    ASSERT_TRUE(Ctx().ClearProperties(fork).has_value());
    auto other = *Ctx().FindForkVertex(w);
    ASSERT_TRUE(*Ctx().RemoveLabel(other, la));
    ASSERT_TRUE(*Ctx().DeleteForkVertex(w));
    ASSERT_TRUE(*Ctx().DeleteForkVertex(v));
    Ctx().EndTransaction();
    acc->Abort();
  }

  EXPECT_EQ(StubCount(), 2u) << "aborts leave one (empty) stub per touched vertex, however often";
  Read([&] {
    auto const after = StubOf(Ctx(), v);
    EXPECT_EQ(after.added_labels, committed.added_labels);
    EXPECT_EQ(after.removed_labels, committed.removed_labels);
    EXPECT_EQ(after.removed_pids, committed.removed_pids);
    EXPECT_EQ(after.properties.Properties(), committed.properties.Properties());
    EXPECT_FALSE(after.deleted);
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(LabelSet(Ctx().Labels(fork, ms::View::NEW)), (std::set<ms::LabelId>{la, lb}));
    EXPECT_EQ(*Ctx().Properties(fork, ms::View::NEW), (Props{{x, Int(5)}}));
    auto other = *Ctx().FindForkVertex(w);
    EXPECT_TRUE(StubIsEmpty(StubOf(Ctx(), w))) << "the stub an aborted first touch left is empty";
    EXPECT_EQ(LabelSet(Ctx().Labels(other, ms::View::NEW)), (std::set<ms::LabelId>{la}));
    EXPECT_TRUE(Ctx().ResolveVertex(w, ms::View::NEW).has_value());
  });
}

// The stock mutators conflict before they find that there is nothing to do; so must a branch write that changes
// nothing, outcome for outcome as on main.
TEST_F(VersioningBranchWriteTest, WritesThatChangeNothingStillConflictAsOnMain) {
  auto const la = Label("A"), lc = Label("C");
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const a = CreateMainVertex({la}, {{x, Int(1)}});
  auto const b = CreateMainVertex({}, {});
  Fork();

  using Errors = std::vector<std::optional<ms::Error>>;
  auto const error_of = [](auto &&result) {
    return result ? std::optional<ms::Error>{} : std::optional<ms::Error>{result.error()};
  };

  Errors on_main;
  {
    auto t1 = storage_->Access(ms::WRITE);
    ASSERT_TRUE(t1->FindVertex(a, ms::View::OLD)->SetProperty(z, Int(1)).has_value());
    ASSERT_TRUE(t1->FindVertex(b, ms::View::OLD)->SetProperty(z, Int(1)).has_value());
    auto t2 = storage_->Access(ms::WRITE);
    auto va = *t2->FindVertex(a, ms::View::OLD);
    auto vb = *t2->FindVertex(b, ms::View::OLD);
    Props init{{y, Int(1)}};
    on_main = {error_of(va.AddLabel(la)),
               error_of(va.RemoveLabel(lc)),
               error_of(va.SetProperty(y, ms::PropertyValue{})),
               error_of(va.InitProperties(init)),
               error_of(vb.ClearProperties())};
    t1->Abort();
    t2->Abort();
  }

  Errors on_branch;
  {
    mv::BranchContext second{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(second.BeginTransaction(t2.get()).has_value());
    auto fork_a = *Ctx().FindForkVertex(a);
    auto fork_b = *Ctx().FindForkVertex(b);
    ASSERT_TRUE(Ctx().SetProperty(fork_a, z, Int(1)).has_value());
    ASSERT_TRUE(Ctx().SetProperty(fork_b, z, Int(1)).has_value());
    auto second_a = *second.FindForkVertex(a);
    auto second_b = *second.FindForkVertex(b);
    Props init{{y, Int(1)}};
    on_branch = {error_of(second.AddLabel(second_a, la)),
                 error_of(second.RemoveLabel(second_a, lc)),
                 error_of(second.SetProperty(second_a, y, ms::PropertyValue{})),
                 error_of(second.InitProperties(second_a, init)),
                 error_of(second.ClearProperties(second_b))};
    second.EndTransaction();
    Ctx().EndTransaction();
    t1->Abort();
    t2->Abort();
  }
  EXPECT_EQ(on_branch, on_main);
  for (auto const &error : on_branch) EXPECT_EQ(error, std::optional<ms::Error>{ms::Error::SERIALIZATION_ERROR});
}

// A write that throws leaves the diff transaction poisoned: the stub may be half-written, so it must be aborted.
TEST_F(VersioningBranchWriteTest, AWriteThatThrowsPoisonsTheTransactionAndLeavesNothingAfterAbort) {
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto fork = *Ctx().FindForkVertex(v);
    acc->GetTransaction()->edge_import_mode_active = true;  // makes every stock vertex write throw
    EXPECT_ANY_THROW((void)Ctx().SetProperty(fork, x, ms::PropertyValue{}));
    EXPECT_TRUE(acc->TransactionHasSerializationError());
    acc->GetTransaction()->edge_import_mode_active = false;
    Ctx().EndTransaction();
    acc->Abort();
  }
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(1));
    EXPECT_TRUE(StubIsEmpty(StubOf(Ctx(), v)));
  });
}

TEST_F(VersioningBranchWriteTest, ReadsBeforeAndAfterAWriteInOneTransactionSeeTheWrite) {
  auto const x = Prop("x");
  auto const l = Label("L");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto fork = *Ctx().FindForkVertex(a);
  for (int i = 0; i < 2; ++i) EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(1));
  EXPECT_FALSE(fork.vertex_->branched()) << "an unwritten fork vertex is read as is";

  ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value());
  ASSERT_TRUE(*Ctx().AddLabel(fork, l));
  EXPECT_TRUE(fork.vertex_->branched());
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(2));
    EXPECT_EQ(*Ctx().Properties(fork, ms::View::NEW), (Props{{x, Int(2)}}));
    EXPECT_TRUE(*Ctx().HasLabel(fork, l, ms::View::NEW));
    EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::OLD), Int(1));
  }
  acc->AdvanceCommand();
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::OLD), Int(2));
  Commit(*acc);
  Ctx().EndTransaction();
  Read([&] {
    auto later = *Ctx().FindForkVertex(a);
    EXPECT_EQ(*Ctx().GetProperty(later, x, ms::View::OLD), Int(2));
    EXPECT_TRUE(*Ctx().HasLabel(later, l, ms::View::OLD));
  });
}

// Main deletes a vertex the branch has a stub for, and GC runs: the fork Vertex* the stub index holds stays valid
// because main's GC horizon is clamped to the oldest fork pin.
TEST_F(VersioningBranchWriteTest, AForkVertexMainDeletedAfterTheForkStaysReadableAndWritableThroughItsStub) {
  auto const la = Label("A");
  auto const x = Prop("x");
  auto const a = CreateMainVertex({la}, {{x, Int(1)}});
  Fork();
  Write([&] {
    auto fork = *Ctx().FindForkVertex(a);
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value());
  });
  {
    auto acc = storage_->Access(ms::WRITE);
    auto vertex = *acc->FindVertex(a, ms::View::NEW);
    ASSERT_TRUE(acc->DeleteVertex(&vertex).has_value());
    Commit(*acc);
  }
  storage_->FreeMemory({}, false);
  storage_->FreeMemory({}, false);
  Write([&] {
    auto fork = Ctx().FindForkVertex(a);
    ASSERT_TRUE(fork.has_value()) << "main@fork_ts still has the vertex";
    EXPECT_TRUE(*Ctx().HasLabel(*fork, la, ms::View::NEW));
    EXPECT_EQ(*Ctx().GetProperty(*fork, x, ms::View::NEW), Int(2));
    ASSERT_TRUE(Ctx().SetProperty(*fork, x, Int(3)).has_value());
  });
  Read([&] { EXPECT_EQ(*Ctx().GetProperty(*Ctx().FindForkVertex(a), x, ms::View::NEW), Int(3)); });
}

// Branch-native vertex gids come from main's counter: at or above the fork floor, never reused by main or another
// branch.
TEST_F(VersioningBranchWriteTest, NativeGidsComeFromMainCountersAndNeverCollide) {
  ms::Gid fork_vertex_gid = CreateMainVertex({}, {});
  Fork();
  auto second = mv::BranchContext::BuildFromFork(*storage_, *fork_ts_);
  ASSERT_TRUE(second.has_value());
  EXPECT_TRUE(Ctx().IsForkCandidateVertex(fork_vertex_gid));
  auto const vertex_floor = Core().vertex_gid_floor();

  auto diff1 = Diff().Access(ms::WRITE);
  Begin(diff1.get());
  auto diff2 = (*second)->diff_engine().Access(ms::WRITE);
  ASSERT_TRUE((*second)->BeginTransaction(diff2.get()).has_value());

  auto v1a = Ctx().CreateNativeVertex();
  auto v1b = Ctx().CreateNativeVertex();
  auto v2a = (*second)->CreateNativeVertex();
  auto v2b = (*second)->CreateNativeVertex();
  auto const main_vertex_gid = CreateMainVertex({}, {});

  std::vector<ms::Gid> const vertex_gids{v1a.Gid(), v1b.Gid(), v2a.Gid(), v2b.Gid(), main_vertex_gid};
  for (auto const gid : vertex_gids) {
    EXPECT_GE(gid.AsUint(), vertex_floor);
    EXPECT_EQ(std::ranges::count(vertex_gids, gid), 1) << "vertex gid " << gid.AsUint() << " allocated twice";
  }
  EXPECT_FALSE(Ctx().IsForkCandidateVertex(v1a.Gid()));
  EXPECT_TRUE(diff1->FindVertex(v1a.Gid(), ms::View::NEW).has_value());
  EXPECT_FALSE(diff1->FindVertex(v2a.Gid(), ms::View::NEW).has_value()) << "branches must not see each other";
  EXPECT_TRUE(diff2->FindVertex(v2a.Gid(), ms::View::NEW).has_value());
  EXPECT_FALSE(diff2->FindVertex(main_vertex_gid, ms::View::NEW).has_value())
      << "main's later create is not in a branch";
  {
    auto acc = storage_->Access(ms::READ);
    EXPECT_FALSE(acc->FindVertex(v1a.Gid(), ms::View::NEW).has_value()) << "branch creates are not in main";
  }
  (*second)->EndTransaction();
  Ctx().EndTransaction();
  diff1->Abort();
  diff2->Abort();
}

// Two sessions on one branch write fork vertices while both are open, as two transactions on main do: other
// vertices both succeed; the same vertex conflicts whatever the keys (the unit of conflict is the object), also a
// second write of another property; and once the first writer aborts a new transaction may write.
TEST_F(VersioningBranchWriteTest, ConcurrentSessionsWritingForkVerticesFollowMainsConflictRule) {
  auto const x = Prop("x"), y = Prop("y");
  auto const label = Label("L");
  auto const a_gid = CreateMainVertex({}, {});
  auto const b_gid = CreateMainVertex({}, {});
  Fork();

  struct Outcome {
    bool other_vertex_ok;
    std::optional<ms::Error> same_vertex_label;
    std::optional<ms::Error> same_vertex_other_property;
    bool poisoned;
    bool after_abort_ok;
    bool operator==(const Outcome &) const = default;
  };

  auto const value = Int(1);
  auto const error_of = [](auto &&result) {
    return result ? std::optional<ms::Error>{} : std::optional<ms::Error>{result.error()};
  };

  Outcome on_main{};
  {
    auto t1 = storage_->Access(ms::WRITE);
    auto t2 = storage_->Access(ms::WRITE);
    ASSERT_TRUE(t1->FindVertex(a_gid, ms::View::OLD)->SetProperty(x, value).has_value());
    on_main.other_vertex_ok = t2->FindVertex(b_gid, ms::View::OLD)->SetProperty(x, value).has_value();
    on_main.same_vertex_label = error_of(t2->FindVertex(a_gid, ms::View::OLD)->AddLabel(label));
    on_main.same_vertex_other_property = error_of(t2->FindVertex(a_gid, ms::View::OLD)->SetProperty(y, value));
    on_main.poisoned = t2->GetTransaction()->has_serialization_error;
    t1->Abort();
    t2->Abort();
    auto t3 = storage_->Access(ms::WRITE);
    on_main.after_abort_ok = t3->FindVertex(a_gid, ms::View::OLD)->SetProperty(x, value).has_value();
    t3->Abort();
  }

  Outcome on_branch{};
  {
    mv::BranchContext s2{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
    auto s1_a = *Ctx().FindForkVertex(a_gid);
    auto s2_a = *s2.FindForkVertex(a_gid);
    auto s2_b = *s2.FindForkVertex(b_gid);
    ASSERT_TRUE(Ctx().SetProperty(s1_a, x, value).has_value());
    on_branch.other_vertex_ok = s2.SetProperty(s2_b, x, value).has_value();
    on_branch.same_vertex_label = error_of(s2.AddLabel(s2_a, label));
    on_branch.same_vertex_other_property = error_of(s2.SetProperty(s2_a, y, value));
    on_branch.poisoned = t2->GetTransaction()->has_serialization_error;
    EXPECT_EQ(s2.DeleteForkVertex(a_gid).error_or(ms::Error::PROPERTIES_DISABLED), ms::Error::SERIALIZATION_ERROR);
    Ctx().EndTransaction();
    s2.EndTransaction();
    t1->Abort();
    t2->Abort();
    auto t3 = Diff().Access(ms::WRITE);
    Begin(t3.get());
    auto s3_a = *Ctx().FindForkVertex(a_gid);
    on_branch.after_abort_ok = Ctx().SetProperty(s3_a, x, value).has_value();
    Ctx().EndTransaction();
    t3->Abort();
  }

  EXPECT_EQ(on_main, (Outcome{true, ms::Error::SERIALIZATION_ERROR, ms::Error::SERIALIZATION_ERROR, true, true}));
  EXPECT_EQ(on_branch, on_main);
}

// A write that committed after this snapshot started conflicts, one that committed before does not: as on main, per
// vertex, whichever property it wrote.
TEST_F(VersioningBranchWriteTest, AWriteConflictsWithAStubCommittedAfterItsSnapshotStarted) {
  auto const x = Prop("x"), y = Prop("y");
  auto const a = CreateMainVertex({}, {});
  Fork();
  mv::BranchContext early{Ctx().core()};
  auto t_early = Diff().Access(ms::WRITE);
  ASSERT_TRUE(early.BeginTransaction(t_early.get()).has_value());
  Write([&] {
    auto fork = *Ctx().FindForkVertex(a);
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(1)).has_value());
  });
  auto fork = *early.FindForkVertex(a);
  EXPECT_EQ(early.SetProperty(fork, y, Int(2)).error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t_early->TransactionHasSerializationError());
  early.EndTransaction();
  t_early->Abort();
  Write([&] {  // a transaction that starts after the commit may write
    auto later = *Ctx().FindForkVertex(a);
    ASSERT_TRUE(Ctx().SetProperty(later, y, Int(2)).has_value());
  });
}

// T2 then T3 attach an edge to fork vertex V; T3 commits, T1 begins, then T2 commits. T2's delta is buried under T3's
// head (committed before T1's snapshot), so T1's delete of V conflicts only if CanDeleteAnchor walks the whole chain.
// Whatever the failed delete wrote is gone once T1 aborts.
TEST_F(VersioningBranchWriteTest, DeleteForkVertexConflictsWithEdgeBuriedUnderOlderCommittedHead) {
  auto const v_gid = CreateMainVertex({}, {});
  Fork();
  mv::BranchContext &deleter = Ctx();
  mv::BranchContext creator_early{deleter.core()};
  mv::BranchContext creator_late{deleter.core()};

  auto const attach_edge = [&](mv::BranchContext &ctx, ms::Storage::Accessor *diff_acc) {
    auto anchor = ctx.EnsureDiffVertex(v_gid);
    ASSERT_TRUE(anchor.has_value());
    auto native = ctx.CreateNativeVertex();
    CreateNativeEdge(diff_acc, &*anchor, &native);
  };

  auto t2 = Diff().Access(ms::WRITE);
  ASSERT_TRUE(creator_early.BeginTransaction(t2.get()).has_value());
  attach_edge(creator_early, t2.get());
  auto t3 = Diff().Access(ms::WRITE);
  ASSERT_TRUE(creator_late.BeginTransaction(t3.get()).has_value());
  attach_edge(creator_late, t3.get());
  Commit(*t3);
  creator_late.EndTransaction();

  auto t1 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  Commit(*t2);
  creator_early.EndTransaction();

  auto deleted = deleter.DeleteForkVertex(v_gid);
  ASSERT_FALSE(deleted.has_value()) << "the delete must not pass over an edge committed after its snapshot";
  EXPECT_EQ(deleted.error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t1->TransactionHasSerializationError());
  deleter.EndTransaction();
  t1->Abort();

  Read([&] {
    EXPECT_FALSE(Ctx().IsVertexDeleted(v_gid, ms::View::NEW)) << "the marker the failed delete wrote is gone";
    EXPECT_FALSE(StubOf(Ctx(), v_gid).deleted);
  });
}

// A vertex delete hides each of the vertex's fork edges from their other endpoints too, by the markers it writes
// there: per direction and view, and not for a reader that cannot see the delete; a rollback leaves nothing.
TEST_F(VersioningBranchWriteTest, DeleteForkVertexHidesItsEdgesFromTheirOtherEndpoints) {
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  auto const c = CreateMainVertex({}, {});
  auto const d = CreateMainVertex({}, {});
  auto const e1 = CreateMainEdge(a, b, "E");
  auto const e2 = CreateMainEdge(a, c, "E");
  auto const e3 = CreateMainEdge(c, a, "E");
  auto const loop = CreateMainEdge(a, a, "E");
  auto const other = CreateMainEdge(c, d, "E");
  Fork();
  auto const gids = [&](mv::BranchContext &ctx, ms::Gid vertex, ms::EdgeDirection direction, ms::View view) {
    std::set<uint64_t> found;
    for (auto const &edge : ctx.ResolveEdges(vertex, direction, view)) found.insert(edge.Gid().AsUint());
    return found;
  };
  auto const of = [](std::initializer_list<MainEdge> edges) {
    std::set<uint64_t> found;
    for (auto const &edge : edges) found.insert(edge.gid.AsUint());
    return found;
  };
  constexpr auto kOut = ms::EdgeDirection::OUT;
  constexpr auto kIn = ms::EdgeDirection::IN;

  mv::BranchContext snapshot{Ctx().core()};
  auto snapshot_acc = Diff().Access(ms::READ);
  ASSERT_TRUE(snapshot.BeginTransaction(snapshot_acc.get()).has_value());
  {
    auto writer = Diff().Access(ms::WRITE);
    Begin(writer.get());
    EXPECT_FALSE(Ctx().HasVertexStub(b));
    ASSERT_TRUE(*Ctx().DeleteForkVertex(a));
    for (auto const gid : {a, b, c}) {
      EXPECT_TRUE(Ctx().HasVertexStub(gid)) << "a stub holds the markers: " << gid.AsUint();
      EXPECT_TRUE(Core().MayHaveEdgeChange(gid));
    }
    EXPECT_FALSE(Ctx().HasVertexStub(d)) << "an edge of the neighbour that the delete does not hide";
    EXPECT_EQ(gids(Ctx(), b, kIn, ms::View::NEW), of({}));
    EXPECT_EQ(gids(Ctx(), c, kIn, ms::View::NEW), of({}));
    EXPECT_EQ(gids(Ctx(), c, kOut, ms::View::NEW), of({other}));
    EXPECT_EQ(gids(Ctx(), b, kIn, ms::View::OLD), of({e1})) << "not before the command advances";
    EXPECT_EQ(gids(snapshot, b, kIn, ms::View::NEW), of({e1})) << "nor for another transaction";
    writer->AdvanceCommand();
    EXPECT_EQ(gids(Ctx(), b, kIn, ms::View::OLD), of({}));
    Ctx().EndTransaction();
    writer->Abort();
  }
  EXPECT_EQ(gids(snapshot, b, kIn, ms::View::NEW), of({e1}));
  snapshot.EndTransaction();
  Read([&] {
    EXPECT_EQ(gids(Ctx(), b, kIn, ms::View::NEW), of({e1})) << "the rollback left no marker";
    EXPECT_EQ(gids(Ctx(), c, kIn, ms::View::NEW), of({e2}));
    EXPECT_EQ(gids(Ctx(), c, kOut, ms::View::NEW), of({e3, other}));
  });

  Write([&] { ASSERT_TRUE(*Ctx().DeleteForkVertex(a)); });
  Read([&] {
    EXPECT_EQ(gids(Ctx(), b, kIn, ms::View::NEW), of({}));
    EXPECT_EQ(gids(Ctx(), c, kIn, ms::View::NEW), of({}));
    EXPECT_EQ(gids(Ctx(), c, kOut, ms::View::NEW), of({other}));
    bool deleted = false;
    EXPECT_TRUE(Ctx().ResolveEdges(a, kOut, ms::View::NEW, &deleted).empty());
    EXPECT_TRUE(deleted);
  });
  (void)loop;
}

// A neighbour's write conflicts with the delete of a vertex it shares an edge with, whichever comes first.
TEST_F(VersioningBranchWriteTest, DeleteForkVertexConflictsWithAWriteOfANeighbourInEitherOrder) {
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  CreateMainEdge(a, b, "E");
  Fork();
  for (bool delete_first : {true, false}) {
    SCOPED_TRACE(delete_first);
    mv::BranchContext s2{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
    auto neighbour = *s2.FindForkVertex(b);
    std::optional<ms::Error> error;
    if (delete_first) {
      ASSERT_TRUE(*Ctx().DeleteForkVertex(a));
      if (auto written = s2.SetProperty(neighbour, x, Int(1)); !written) error = written.error();
    } else {
      ASSERT_TRUE(s2.SetProperty(neighbour, x, Int(1)).has_value());
      if (auto deleted = Ctx().DeleteForkVertex(a); !deleted) error = deleted.error();
    }
    EXPECT_EQ(error, std::optional{ms::Error::SERIALIZATION_ERROR});
    Ctx().EndTransaction();
    s2.EndTransaction();
    t1->Abort();
    t2->Abort();
  }
}

// A fork-vertex delete is a stub write: hidden at View::NEW but not View::OLD within the deleting command, and an abort
// leaves no marker.
TEST_F(VersioningBranchWriteTest, DeleteForkVertexMarkerFollowsMvcc) {
  auto const x = Prop("x");
  auto const a_gid = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  {
    auto diff_acc = Diff().Access(ms::WRITE);
    Begin(diff_acc.get());
    ASSERT_FALSE(Ctx().IsVertexDeleted(a_gid, ms::View::NEW));
    EXPECT_FALSE(Core().MayHaveVertexDeleted(a_gid));
    auto deleted = Ctx().DeleteForkVertex(a_gid);
    ASSERT_TRUE(deleted.has_value() && *deleted);
    EXPECT_TRUE(Core().MayHaveVertexDeleted(a_gid));
    EXPECT_TRUE(Ctx().FindForkVertex(a_gid)->vertex_->branched());
    EXPECT_TRUE(Ctx().IsVertexDeleted(a_gid, ms::View::NEW));
    EXPECT_FALSE(Ctx().IsVertexDeleted(a_gid, ms::View::OLD));
    EXPECT_FALSE(Ctx().ResolveVertex(a_gid, ms::View::NEW).has_value());
    EXPECT_TRUE(Ctx().ResolveVertex(a_gid, ms::View::OLD).has_value());
    diff_acc->Abort();
    Ctx().EndTransaction();
  }
  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  EXPECT_FALSE(Ctx().IsVertexDeleted(a_gid, ms::View::NEW)) << "an aborted delete must leave no marker";
  ASSERT_TRUE(Ctx().ResolveVertex(a_gid, ms::View::NEW).has_value());
  EXPECT_EQ(*Ctx().GetProperty(*Ctx().FindForkVertex(a_gid), x, ms::View::NEW), Int(1));
  std::vector<ms::Gid> scanned;
  for (auto v : Ctx().Vertices(ms::View::NEW)) scanned.push_back(v.Gid());
  EXPECT_EQ(scanned, std::vector<ms::Gid>{a_gid}) << "the union scan must see the vertex again post-abort";
  Ctx().EndTransaction();
  diff_acc->Abort();
}

// Stubs are not transactional: concurrent first touches that all roll back leave exactly one stub per touched gid,
// none of them in the diff engine's vertices_, and lookups of untouched gids fall back to the fork state.
TEST_F(VersioningBranchWriteTest, StubIndexRecordsTouchedGidsUnderConcurrentFirstTouchAndRollback) {
  constexpr size_t kVertices = 300;  // spans several 16-gid pages
  constexpr size_t kThreads = 4;
  std::vector<ms::Gid> gids;
  {
    auto acc = storage_->Access(ms::WRITE);
    for (size_t i = 0; i < kVertices; ++i) gids.push_back(acc->CreateVertex().Gid());
    Commit(*acc);
  }
  Fork();
  auto core = Ctx().core();

  // Even indexes are touched by every thread (contended); a touched odd index by one thread; the rest stay untouched.
  std::vector<bool> is_touched(kVertices, false);
  for (size_t i = 0; i < kVertices; ++i) is_touched[i] = i % 2 == 0 || i % 3 != 0;

  std::vector<std::thread> threads;
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      auto ctx = std::make_unique<mv::BranchContext>(core);
      auto diff_acc = core->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE(ctx->BeginTransaction(diff_acc.get()).has_value());
      for (size_t i = 0; i < kVertices; ++i) {
        if (!is_touched[i]) continue;
        if (i % 2 == 1 && (i / 2) % kThreads != t) continue;
        EXPECT_TRUE(ctx->EnsureDiffVertex(gids[i]).has_value());
      }
      ctx->EndTransaction();
      diff_acc->Abort();
    });
  }
  for (auto &th : threads) th.join();

  auto diff_acc = Diff().Access(ms::READ);
  Begin(diff_acc.get());
  size_t diff_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++diff_count;
  }
  EXPECT_EQ(diff_count, 0u) << "stubs must not be entries of the diff engine's vertices_";
  size_t expected_count = 0;
  for (size_t i = 0; i < kVertices; ++i) expected_count += is_touched[i] ? 1 : 0;
  EXPECT_EQ(StubCount(), expected_count) << "exactly one stub per touched gid";

  auto &index = core->stub_index();
  for (size_t i = 0; i < kVertices; ++i) {
    auto const gid = gids[i];
    auto diff_vertex = Ctx().FindStub(gid, ms::View::NEW);
    auto hist_vertex = Ctx().historical().FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(hist_vertex.has_value());
    if (!is_touched[i]) {
      EXPECT_FALSE(diff_vertex.has_value());
      EXPECT_EQ(index.FindStub(gid.AsUint()), nullptr);
      EXPECT_EQ(index.FindFork(gid.AsUint()), nullptr);
      EXPECT_EQ(Ctx().FindForkVertex(gid)->vertex_, hist_vertex->vertex_) << "miss falls back";
      continue;
    }
    ASSERT_TRUE(diff_vertex.has_value()) << "a rolled-back first touch must leave the stub";
    EXPECT_EQ(index.FindStub(gid.AsUint()), diff_vertex->vertex_);
    EXPECT_EQ(index.FindFork(gid.AsUint()), hist_vertex->vertex_);
    auto fork_via_index = Ctx().FindForkVertex(gid);
    ASSERT_TRUE(fork_via_index.has_value());
    EXPECT_TRUE(*fork_via_index == *hist_vertex);
  }
  Ctx().EndTransaction();
}

// Writers move value between fork vertices (read two, write two, commit; retried after a conflict) while snapshot
// readers sum them: the total is constant at every snapshot, whatever the interleaving of first touches, stub writes
// and commits.
TEST_F(VersioningBranchWriteTest, ConcurrentTransfersBetweenForkVerticesKeepAConstantTotal) {
  constexpr int kVertices = 6;
  constexpr int kWriters = 4;
  constexpr int kTransfers = 120;
  constexpr int64_t kInitial = 100;
  auto const x = Prop("x");
  std::vector<ms::Gid> gids;
  for (int i = 0; i < kVertices; ++i) gids.push_back(CreateMainVertex({}, {{x, Int(kInitial)}}));
  Fork();
  auto core = Ctx().core();
  std::atomic<int> writers_left{kWriters};
  std::atomic<int> snapshots{0};
  std::atomic<int> conflicts{0};

  auto const total = [&](mv::BranchContext &ctx, ms::View view) {
    int64_t sum = 0;
    for (auto const gid : gids) sum += ctx.GetProperty(*ctx.FindForkVertex(gid), x, view)->ValueInt();
    return sum;
  };

  std::vector<std::jthread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      mv::BranchContext ctx{core};
      std::mt19937 rng(static_cast<uint32_t>(w) + 1);
      for (int done = 0; done < kTransfers;) {
        auto const from = rng() % kVertices;
        auto const to = (from + 1 + rng() % (kVertices - 1)) % kVertices;
        auto acc = Diff().Access(ms::WRITE, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto fork_from = *ctx.FindForkVertex(gids[from]);
        auto fork_to = *ctx.FindForkVertex(gids[to]);
        auto const from_value = ctx.GetProperty(fork_from, x, ms::View::NEW)->ValueInt();
        auto const to_value = ctx.GetProperty(fork_to, x, ms::View::NEW)->ValueInt();
        bool ok = ctx.SetProperty(fork_from, x, Int(from_value - 1)).has_value();
        ok = ok && ctx.SetProperty(fork_to, x, Int(to_value + 1)).has_value();
        ctx.EndTransaction();
        if (ok && !acc->TransactionHasSerializationError()) {
          Commit(*acc);
          ++done;
        } else {
          acc->Abort();
          ++conflicts;
        }
      }
      --writers_left;
    });
  }
  for (int reader = 0; reader < 2; ++reader) {
    threads.emplace_back([&] {
      mv::BranchContext ctx{core};
      do {
        auto acc = Diff().Access(ms::READ, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        ASSERT_EQ(total(ctx, ms::View::OLD), kVertices * kInitial);
        ASSERT_EQ(total(ctx, ms::View::NEW), kVertices * kInitial);
        ctx.EndTransaction();
        ++snapshots;
      } while (writers_left.load() > 0);
    });
  }
  threads.clear();
  EXPECT_GT(snapshots.load(), 3);
  EXPECT_GT(conflicts.load(), 0) << "the writers must actually collide";
  Read([&] { EXPECT_EQ(total(Ctx(), ms::View::OLD), kVertices * kInitial); });
}

// A property flipped between the branch's value and a removal through the direct mutators, over a fork value neither
// of which is: whatever the isolation level, a read is one of the two, never the fork's.
TEST_F(VersioningBranchWriteTest, AReadOfAPropertyFlippedByDirectWritesNeverShowsTheForkValue) {
  constexpr int kFlips = 600;
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(100)}});
  Fork();
  Write([&] {
    auto fork = *Ctx().FindForkVertex(a);
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(5)).has_value());
  });
  auto core = Ctx().core();
  std::atomic<bool> stop{false};
  std::atomic<int> wrong{0};
  std::vector<std::thread> readers;
  for (auto const level : {ms::IsolationLevel::READ_COMMITTED, ms::IsolationLevel::SNAPSHOT_ISOLATION}) {
    readers.emplace_back([&, level] {
      mv::BranchContext ctx{core};
      while (!stop.load(std::memory_order_acquire)) {
        auto acc = Diff().Access(ms::READ, level, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto fork = *ctx.FindForkVertex(a);
        auto const value = ctx.GetProperty(fork, x, ms::View::NEW);
        auto const props = ctx.Properties(fork, ms::View::NEW);
        if (!value || (!value->IsNull() && *value != Int(5))) wrong.fetch_add(1);
        if (!props || (props->contains(x) && props->at(x) != Int(5))) wrong.fetch_add(1);
        ctx.EndTransaction();
      }
    });
  }
  for (int i = 0; i < kFlips; ++i) {
    Write([&] {
      auto fork = *Ctx().FindForkVertex(a);
      ASSERT_TRUE(Ctx().SetProperty(fork, x, i % 2 == 0 ? ms::PropertyValue{} : Int(5)).has_value());
    });
  }
  stop.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(wrong.load(), 0);
}

///// KEY-PART COPIES: A STUB UNDER A MIRRORED INDEX KEY HOLDS EVERY PART OF THAT KEY

// The diff engine mirrors main's label+property indexes, and its stock index insert reads the stub's own labels and
// properties. A stub holds only what the branch changed, so a write that moves one part of a key copies the other
// parts the fork has onto the stub as base state (no delta) first; the insert then finds the whole key.
class VersioningBranchKeyCopyTest : public VersioningBranchWriteTest {
 protected:
  void MainIndex(ms::LabelId label, std::vector<ms::PropertyPath> properties) {
    auto acc = storage_->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(label, std::move(properties)).has_value());
    Commit(*acc);
  }

  // The gids the diff engine's own label+properties index yields, as a fresh branch reader sees them.
  std::set<uint64_t> IndexedStubs(ms::LabelId label, const std::vector<ms::PropertyPath> &properties) {
    auto acc = Diff().Access(ms::READ);
    std::set<uint64_t> gids;
    for (auto v : acc->Vertices(label, properties, ms::View::NEW)) gids.insert(v.Gid().AsUint());
    return gids;
  }

  // What each entry of the diff engine's label+properties index yields, as gid -> the key's values read off the stub
  // itself: a yielded entry is validated against the stub's current state, so the stub holds the whole key.
  using Keys = std::map<uint64_t, std::vector<ms::PropertyValue>>;

  Keys IndexedKeys(ms::LabelId label, const std::vector<ms::PropertyId> &properties) {
    auto acc = Diff().Access(ms::READ);
    std::vector<ms::PropertyPath> paths(properties.begin(), properties.end());
    Keys keys;
    for (auto v : acc->Vertices(label, paths, ms::View::NEW)) {
      auto &key = keys[v.Gid().AsUint()];
      for (auto const property : properties) key.push_back(*v.GetProperty(property, ms::View::NEW));
    }
    return keys;
  }

  ms::Vertex *RawStub(ms::Gid gid) { return Core().stub_index().FindStub(gid.AsUint()); }

  static bool HoldsLabel(ms::Vertex *stub, ms::LabelId label) {
    std::shared_lock guard{stub->lock};
    return std::ranges::contains(stub->labels, label);
  }

  static ms::PropertyValue HeldProperty(ms::Vertex *stub, ms::PropertyId key) {
    std::shared_lock guard{stub->lock};
    return stub->properties.GetProperty(key);
  }

  // The actions of the stub's deltas, newest first.
  static std::vector<ms::Delta::Action> Actions(ms::Vertex *stub) {
    std::vector<ms::Delta::Action> actions;
    std::shared_lock guard{stub->lock};
    for (const ms::Delta *delta = stub->delta(); delta != nullptr; delta = delta->next.load()) {
      actions.push_back(delta->action);
    }
    return actions;
  }

  template <typename Body>
  void OnFork(ms::Gid gid, Body &&body) {
    Fork();
    Writing(gid, std::forward<Body>(body));
  }

  template <typename Body>
  void Writing(ms::Gid gid, Body &&body) {
    Write([&] {
      auto fork = *Ctx().FindForkVertex(gid);
      body(fork);
    });
  }
};

TEST_F(VersioningBranchKeyCopyTest, SettingALabelCopiesTheIndexedPropertyTheForkHas) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {{x, Int(1)}});
  MainIndex(l, {x});
  std::vector<ms::Delta::Action> actions;
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().AddLabel(fork, l).has_value());
    actions = Actions(RawStub(v));
  });
  EXPECT_EQ(IndexedStubs(l, {x}), std::set<uint64_t>{v.AsUint()}) << "the stub is under (L, x) in the diff index";
  EXPECT_EQ(HeldProperty(RawStub(v), x), Int(1)) << "x was copied from the fork";
  EXPECT_EQ(actions, std::vector<ms::Delta::Action>{ms::Delta::Action::REMOVE_LABEL})
      << "the copy left no delta: only the label write has one";
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(1));
    EXPECT_EQ(LabelSet(Ctx().Labels(fork, ms::View::NEW)), std::set<ms::LabelId>{l});
  });
}

TEST_F(VersioningBranchKeyCopyTest, SettingAPropertyCopiesTheForkLabelOfTheKey) {
  auto const l = Label("L"), other = Label("Other");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l, other}, {});
  MainIndex(l, {x});
  std::vector<ms::Delta::Action> actions;
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(5)).has_value());
    actions = Actions(RawStub(v));
  });
  EXPECT_EQ(IndexedStubs(l, {x}), std::set<uint64_t>{v.AsUint()});
  auto *const stub = RawStub(v);
  EXPECT_TRUE(HoldsLabel(stub, l)) << "L was copied";
  EXPECT_FALSE(HoldsLabel(stub, other)) << "a label no index key uses is not";
  EXPECT_EQ(actions, std::vector<ms::Delta::Action>{ms::Delta::Action::SET_PROPERTY});
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(LabelSet(Ctx().Labels(fork, ms::View::NEW)), (std::set<ms::LabelId>{l, other}));
    EXPECT_TRUE(*Ctx().HasLabel(fork, l, ms::View::NEW));
  });
}

TEST_F(VersioningBranchKeyCopyTest, ACompositeKeyGetsEveryPartTheForkHas) {
  auto const l = Label("L");
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}, {y, Int(2)}, {z, Int(3)}});
  MainIndex(l, {x, y});
  OnFork(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, y, Int(20)).has_value()); });
  EXPECT_EQ(IndexedStubs(l, {x, y}), std::set<uint64_t>{v.AsUint()});
  auto *const stub = RawStub(v);
  EXPECT_TRUE(HoldsLabel(stub, l));
  EXPECT_EQ(HeldProperty(stub, x), Int(1)) << "the partner part was copied";
  EXPECT_TRUE(HeldProperty(stub, z).IsNull()) << "a property outside the key was not";
  EXPECT_EQ(HeldProperty(stub, y), Int(20));
}

TEST_F(VersioningBranchKeyCopyTest, RemovingOnePartOfACompositeKeyCopiesTheRest) {
  auto const l = Label("L");
  auto const x = Prop("x"), y = Prop("y");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}, {y, Int(2)}});
  MainIndex(l, {x, y});
  OnFork(v,
         [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, x, ms::PropertyValue{}).has_value()); });
  auto *const stub = RawStub(v);
  EXPECT_TRUE(HoldsLabel(stub, l));
  EXPECT_EQ(HeldProperty(stub, y), Int(2)) << "the rest of the key, which stays non-null";
  EXPECT_TRUE(HeldProperty(stub, x).IsNull());
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(Ctx().GetProperty(fork, x, ms::View::NEW)->IsNull());
    EXPECT_EQ(*Ctx().GetProperty(fork, y, ms::View::NEW), Int(2));
  });
}

TEST_F(VersioningBranchKeyCopyTest, WritesThatCannotMoveAKeyCopyNothing) {
  auto const l = Label("L"), other = Label("Other");
  auto const x = Prop("x"), unrelated = Prop("u");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}, {unrelated, Int(9)}});
  auto const w = CreateMainVertex({l}, {{x, Int(2)}});
  auto const u = CreateMainVertex({l}, {{x, Int(3)}});
  MainIndex(l, {x});
  Fork();
  Writing(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, unrelated, Int(10)).has_value());
    ASSERT_TRUE(Ctx().AddLabel(fork, other).has_value()) << "a label no key uses";
  });
  auto *const stub = RawStub(v);
  EXPECT_FALSE(HoldsLabel(stub, l));
  EXPECT_TRUE(HeldProperty(stub, x).IsNull());
  Writing(w, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().RemoveLabel(fork, l).has_value()); });
  EXPECT_TRUE(HeldProperty(RawStub(w), x).IsNull()) << "a label removal copies nothing";
  Writing(u, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().ClearProperties(fork).has_value()); });
  EXPECT_FALSE(HoldsLabel(RawStub(u), l)) << "neither does a ClearProperties";
  EXPECT_TRUE(IndexedStubs(l, {x}).empty());
}

TEST_F(VersioningBranchKeyCopyTest, SinglePartIndexesNeverCopy) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}});
  {
    auto acc = storage_->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(l).has_value());
    Commit(*acc);
  }
  {
    auto acc = storage_->UniqueAccess();
    ASSERT_TRUE(acc->CreateGlobalVertexIndex(x).has_value());
    Commit(*acc);
  }
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value());
    ASSERT_TRUE(Ctx().AddLabel(fork, Label("M")).has_value());
  });
  EXPECT_FALSE(HoldsLabel(RawStub(v), l));
}

TEST_F(VersioningBranchKeyCopyTest, ACopyNeverReplacesWhatTheBranchAlreadyHolds) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {{x, Int(1)}});
  MainIndex(l, {x});
  OnFork(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(7)).has_value()); });
  Writing(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().AddLabel(fork, l).has_value()); });
  EXPECT_EQ(HeldProperty(RawStub(v), x), Int(7)) << "the branch's own value stays";
  EXPECT_EQ(IndexedStubs(l, {x}), std::set<uint64_t>{v.AsUint()});
}

TEST_F(VersioningBranchKeyCopyTest, ARemovedForkLabelIsNotCopiedAndReaddingItIndexesTheStub) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}});
  MainIndex(l, {x});
  OnFork(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().RemoveLabel(fork, l).has_value()); });
  Writing(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value()); });
  EXPECT_FALSE(HoldsLabel(RawStub(v), l)) << "the label is removed on the branch, so it is not copied";
  EXPECT_TRUE(IndexedStubs(l, {x}).empty()) << "and no entry exists while the vertex lacks L";
  Writing(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().AddLabel(fork, l).has_value()); });
  EXPECT_EQ(IndexedStubs(l, {x}), std::set<uint64_t>{v.AsUint()}) << "re-adding L puts the stub under (L, 2)";
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(*Ctx().HasLabel(fork, l, ms::View::NEW));
    EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(2));
  });
}

TEST_F(VersioningBranchKeyCopyTest, ACopiedLabelThatIsThenRemovedReadsAsRemoved) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l}, {});
  MainIndex(l, {x});
  OnFork(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value()); });
  ASSERT_TRUE(HoldsLabel(RawStub(v), l));
  Writing(v, [&](ms::VertexAccessor &fork) { EXPECT_TRUE(*Ctx().RemoveLabel(fork, l)); });
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_FALSE(*Ctx().HasLabel(fork, l, ms::View::NEW));
    EXPECT_TRUE(Ctx().Labels(fork, ms::View::NEW)->empty());
    EXPECT_FALSE(*Ctx().HasAnyLabel(fork, ms::View::NEW));
  });
  Writing(v, [&](ms::VertexAccessor &fork) { EXPECT_TRUE(*Ctx().AddLabel(fork, l)); });
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(*Ctx().HasLabel(fork, l, ms::View::NEW));
    EXPECT_EQ(LabelSet(Ctx().Labels(fork, ms::View::NEW)), std::set<ms::LabelId>{l});
  });
}

TEST_F(VersioningBranchKeyCopyTest, AnAbortedKeyChangingWriteLeavesNoVisibleEntry) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l}, {});
  MainIndex(l, {x});
  Fork();
  {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto fork = *Ctx().FindForkVertex(v);
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(5)).has_value());
    Ctx().EndTransaction();
    acc->Abort();
  }
  EXPECT_TRUE(IndexedStubs(l, {x}).empty()) << "the stale entry is hidden by validation";
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(Ctx().GetProperty(fork, x, ms::View::NEW)->IsNull());
    EXPECT_EQ(LabelSet(Ctx().Labels(fork, ms::View::NEW)), std::set<ms::LabelId>{l}) << "a copy changes no read";
  });
}

TEST_F(VersioningBranchKeyCopyTest, CopiesAddNoDeltaSoTheStubHistoryIsTheUserWritesOnly) {
  auto const l = Label("L");
  auto const x = Prop("x"), y = Prop("y");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}});
  MainIndex(l, {x, y});
  std::vector<ms::Delta::Action> actions;
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, y, Int(2)).has_value());
    ASSERT_TRUE(Ctx().SetProperty(fork, x, ms::PropertyValue{}).has_value());
    actions = Actions(RawStub(v));
  });
  using Action = ms::Delta::Action;
  EXPECT_EQ(actions, (std::vector<Action>{Action::SET_PROPERTY, Action::SET_PROPERTY, Action::SET_PROPERTY}))
      << "the write of y, then x's removal marker and its drop: no delta for L or for a copied part";
}

TEST_F(VersioningBranchKeyCopyTest, WithoutAnIndexAWriteStoresOnlyTheDifference) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({l}, {{x, Int(1)}});
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, x, Int(2)).has_value());
    ASSERT_TRUE(Ctx().AddLabel(fork, Label("M")).has_value());
  });
  EXPECT_FALSE(HoldsLabel(RawStub(v), l));
}

TEST_F(VersioningBranchKeyCopyTest, UpdatingPropertiesCopiesThePartsOfEveryKeyTheMapMoves) {
  auto const l = Label("L"), m = Label("M");
  auto const a = Prop("a"), b = Prop("b"), c = Prop("c"), d = Prop("d"), z = Prop("z");
  auto const v = CreateMainVertex({l, m}, {{a, Int(1)}, {b, Int(2)}, {c, Int(3)}, {d, Int(4)}, {z, Int(5)}});
  MainIndex(l, {a, b});
  MainIndex(m, {c, d});
  OnFork(v, [&](ms::VertexAccessor &fork) {
    Props map{{a, Int(10)}, {c, Int(30)}, {z, Int(50)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value());
  });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{v.AsUint(), {Int(10), Int(2)}}}));
  EXPECT_EQ(IndexedKeys(m, {c, d}), (Keys{{v.AsUint(), {Int(30), Int(4)}}}));
  auto *const stub = RawStub(v);
  EXPECT_TRUE(HoldsLabel(stub, l));
  EXPECT_TRUE(HoldsLabel(stub, m));
  EXPECT_EQ(HeldProperty(stub, b), Int(2));
  EXPECT_EQ(HeldProperty(stub, d), Int(4));
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(*Ctx().Properties(fork, ms::View::NEW),
              (Props{{a, Int(10)}, {b, Int(2)}, {c, Int(30)}, {d, Int(4)}, {z, Int(50)}}));
  });
}

TEST_F(VersioningBranchKeyCopyTest, InitialisingPropertiesCopiesTheForkLabelOfTheKey) {
  auto const l = Label("L");
  auto const a = Prop("a"), b = Prop("b");
  auto const v = CreateMainVertex({l}, {});
  MainIndex(l, {a, b});
  OnFork(v, [&](ms::VertexAccessor &fork) {
    Props map{{a, Int(1)}, {b, Int(2)}};
    ASSERT_TRUE(*Ctx().InitProperties(fork, map));
  });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{v.AsUint(), {Int(1), Int(2)}}}));
  EXPECT_TRUE(HoldsLabel(RawStub(v), l));
}

// SET n = {...} clears the properties, then updates the map in.
TEST_F(VersioningBranchKeyCopyTest, ReplacingPropertiesIndexesOnlyTheKeysTheMapCompletes) {
  auto const l = Label("L");
  auto const a = Prop("a"), b = Prop("b");
  auto const whole = CreateMainVertex({l}, {{a, Int(1)}, {b, Int(2)}});
  auto const half = CreateMainVertex({l}, {{a, Int(1)}, {b, Int(2)}});
  MainIndex(l, {a, b});
  Fork();
  Writing(whole, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().ClearProperties(fork).has_value());
    EXPECT_FALSE(HoldsLabel(RawStub(whole), l)) << "the clear copies nothing";
    Props map{{a, Int(5)}, {b, Int(6)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value());
  });
  Writing(half, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().ClearProperties(fork).has_value());
    Props map{{a, Int(7)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value());
  });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{whole.AsUint(), {Int(5), Int(6)}}}))
      << "the vertex the replace left without b is under no key";
  EXPECT_TRUE(HeldProperty(RawStub(half), b).IsNull()) << "the cleared b is not copied back";
  Read([&] {
    auto fork = *Ctx().FindForkVertex(half);
    EXPECT_EQ(*Ctx().Properties(fork, ms::View::NEW), (Props{{a, Int(7)}}));
  });
}

TEST_F(VersioningBranchKeyCopyTest, SettingAPropertyBackToTheForkValueKeepsTheStubUnderTheKey) {
  auto const l = Label("L");
  auto const a = Prop("a"), b = Prop("b");
  auto const v = CreateMainVertex({l}, {{a, Int(1)}, {b, Int(2)}});
  MainIndex(l, {a, b});
  OnFork(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, a, Int(9)).has_value()); });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{v.AsUint(), {Int(9), Int(2)}}}));
  Writing(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, a, Int(1)).has_value()); });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{v.AsUint(), {Int(1), Int(2)}}}))
      << "the fork's own key again, found on the branch as well";
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_EQ(*Ctx().GetProperty(fork, a, ms::View::NEW), Int(1));
  });
}

TEST_F(VersioningBranchKeyCopyTest, SettingSeveralLabelsCopiesThePartsOfEachKey) {
  auto const l1 = Label("L1"), l2 = Label("L2");
  auto const a = Prop("a"), b = Prop("b");
  auto const v = CreateMainVertex({}, {{a, Int(1)}, {b, Int(2)}});
  MainIndex(l1, {a});
  MainIndex(l2, {a, b});
  OnFork(v, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(*Ctx().AddLabel(fork, l1));
    ASSERT_TRUE(*Ctx().AddLabel(fork, l2));
  });
  EXPECT_EQ(IndexedKeys(l1, {a}), (Keys{{v.AsUint(), {Int(1)}}}));
  EXPECT_EQ(IndexedKeys(l2, {a, b}), (Keys{{v.AsUint(), {Int(1), Int(2)}}}));
}

TEST_F(VersioningBranchKeyCopyTest, RemovalsDeletesAndNoOpsCopyNothing) {
  auto const l = Label("L"), m = Label("M");
  auto const s = Prop("s"), x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const removed = CreateMainVertex({l}, {{s, Int(1)}});
  auto const deleted = CreateMainVertex({m}, {{x, Int(1)}, {y, Int(2)}});
  auto const absent = CreateMainVertex({m}, {{x, Int(1)}});
  auto const unrelated = CreateMainVertex({m}, {{x, Int(1)}, {y, Int(2)}});
  MainIndex(l, {s});
  MainIndex(m, {x, y});
  Fork();
  Writing(removed, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, s, ms::PropertyValue{}).has_value()) << "REMOVE of a single-part key";
  });
  EXPECT_FALSE(HoldsLabel(RawStub(removed), l));
  Write([&] { ASSERT_TRUE(*Ctx().DeleteForkVertex(deleted)); });
  EXPECT_FALSE(HoldsLabel(RawStub(deleted), m)) << "a delete copies nothing";
  EXPECT_TRUE(HeldProperty(RawStub(deleted), x).IsNull());
  Writing(absent, [&](ms::VertexAccessor &fork) {
    ASSERT_TRUE(Ctx().SetProperty(fork, y, ms::PropertyValue{}).has_value()) << "REMOVE of a property nobody holds";
  });
  EXPECT_FALSE(HoldsLabel(RawStub(absent), m));
  EXPECT_TRUE(HeldProperty(RawStub(absent), x).IsNull());
  Writing(unrelated, [&](ms::VertexAccessor &fork) {
    Props map{{z, Int(1)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value()) << "a map that names no key part";
  });
  EXPECT_FALSE(HoldsLabel(RawStub(unrelated), m));
  EXPECT_TRUE(HeldProperty(RawStub(unrelated), x).IsNull());
  EXPECT_TRUE(IndexedKeys(l, {s}).empty());
  EXPECT_TRUE(IndexedKeys(m, {x, y}).empty());
}

TEST_F(VersioningBranchKeyCopyTest, AddingALabelAfterRemovingAPartNeverBringsThePartBack) {
  auto const l = Label("L");
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {{x, Int(1)}});
  MainIndex(l, {x});
  OnFork(v,
         [&](ms::VertexAccessor &fork) { ASSERT_TRUE(Ctx().SetProperty(fork, x, ms::PropertyValue{}).has_value()); });
  Writing(v, [&](ms::VertexAccessor &fork) { ASSERT_TRUE(*Ctx().AddLabel(fork, l)); });
  EXPECT_TRUE(HeldProperty(RawStub(v), x).IsNull()) << "the branch removed x: the label copy does not restore it";
  EXPECT_TRUE(IndexedKeys(l, {x}).empty());
  Read([&] {
    auto fork = *Ctx().FindForkVertex(v);
    EXPECT_TRUE(Ctx().GetProperty(fork, x, ms::View::NEW)->IsNull());
  });
}

TEST_F(VersioningBranchKeyCopyTest, AnAbortedUpdateDoesNotKeepTheNextWriteOutOfTheIndex) {
  auto const l = Label("L");
  auto const a = Prop("a"), b = Prop("b");
  auto const v = CreateMainVertex({l}, {{a, Int(1)}, {b, Int(2)}});
  MainIndex(l, {a, b});
  Fork();
  {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto fork = *Ctx().FindForkVertex(v);
    Props map{{a, Int(5)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value());
    Ctx().EndTransaction();
    acc->Abort();
  }
  EXPECT_TRUE(IndexedKeys(l, {a, b}).empty());
  Writing(v, [&](ms::VertexAccessor &fork) {
    Props map{{a, Int(7)}};
    ASSERT_TRUE(Ctx().UpdateProperties(fork, map).has_value());
  });
  EXPECT_EQ(IndexedKeys(l, {a, b}), (Keys{{v.AsUint(), {Int(7), Int(2)}}}));
}

///// BRANCH EDGE WRITES: THE EDGE STUBS, THE DELETE MARKER AND THE COMMIT'S COUNTS

// A fork edge the branch changes gets a stub: an Edge in the EdgeStubIndex built on main's own endpoint vertices and
// never linked into an adjacency list. The shared bodies run on both heavy and light edges.
class VersioningBranchEdgeWriteTest : public VersioningBranchWriteTest {
 protected:
  struct Graph {
    ms::Gid a, b, c;
    MainEdge e1, e2, loop;
  };

  // On main: a -e1{x=1,y=2}-> b, a -e2{x=5}-> b and c -loop{x=7}-> c.
  Graph MakeGraph() {
    auto const x = Prop("x"), y = Prop("y");
    Graph g{};
    g.a = CreateMainVertex({}, {});
    g.b = CreateMainVertex({}, {});
    g.c = CreateMainVertex({}, {});
    g.e1 = CreateMainEdge(g.a, g.b, "E", {{x, Int(1)}, {y, Int(2)}});
    g.e2 = CreateMainEdge(g.a, g.b, "E", {{x, Int(5)}});
    g.loop = CreateMainEdge(g.c, g.c, "E", {{x, Int(7)}});
    return g;
  }

  // The fork edge, in the open branch transaction.
  ms::EdgeAccessor ForkEdge(const MainEdge &edge) {
    auto found = Ctx().FindForkEdge(edge.gid, edge.from);
    EXPECT_TRUE(found.has_value());
    return *found;
  }

  // Runs `body` in a branch write transaction committed through BranchContext::CommitTransaction.
  template <typename Body>
  void WriteCounted(Body &&body) {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    body();
    ASSERT_TRUE(Ctx().CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    Ctx().EndTransaction();
  }

  // Runs `body` in a branch write transaction that is then aborted.
  template <typename Body>
  void WriteAborted(Body &&body) {
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    body();
    Ctx().EndTransaction();
    acc->Abort();
  }

  // What the branch stored for fork edge `edge`, as `acc`'s transaction sees it.
  StubState EdgeStubOf(ms::Storage::Accessor &acc, const MainEdge &edge, ms::View view = ms::View::NEW) {
    auto *stub = Core().edge_stub_index().Find(edge.gid.AsUint());
    EXPECT_NE(stub, nullptr);
    if (stub == nullptr) return {};
    ms::EdgeAccessor accessor{
        ms::EdgeRef{stub}, edge.type, MainVertex(edge.from), MainVertex(edge.to), &Diff(), acc.GetTransaction()};
    auto decoded = stub::DecodeEdge(accessor, view);
    EXPECT_TRUE(decoded.has_value());
    return decoded ? std::move(*decoded) : StubState{};
  }

  StubState CommittedEdgeStubOf(const MainEdge &edge) {
    auto acc = Diff().Access(ms::READ);
    return EdgeStubOf(*acc, edge);
  }

  size_t EdgeStubCount() {
    size_t count = 0;
    Core().edge_stub_index().ForEach([&](uint64_t, ms::Edge *) {
      ++count;
      return true;
    });
    return count;
  }

  static bool IsEmpty(const StubState &state) {
    return !state.deleted && state.properties.Properties().empty() && state.removed_pids.empty();
  }

  static Props PropsOf(std::initializer_list<std::pair<const ms::PropertyId, ms::PropertyValue>> list) {
    return Props{list};
  }

  void PropertyWritesMergeOverTheForkEdge() {
    auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
    auto const g = MakeGraph();
    Fork();
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto e1 = ForkEdge(g.e1);
    EXPECT_FALSE(e1.MaybeBranchedHint());
    EXPECT_EQ(*Ctx().SetProperty(e1, x, Int(10)), Int(1));
    EXPECT_TRUE(e1.MaybeBranchedHint()) << "main's own Edge is marked before the stub is written";
    EXPECT_EQ(*Ctx().SetProperty(e1, y, ms::PropertyValue{}), Int(2)) << "removing returns the fork's value";
    EXPECT_TRUE(Ctx().SetProperty(e1, z, Int(3))->IsNull());
    EXPECT_EQ(*Ctx().SetProperty(e1, x, Int(11)), Int(10)) << "the old value is the branch's own";
    EXPECT_EQ(*Ctx().Properties(e1, ms::View::NEW), PropsOf({{x, Int(11)}, {z, Int(3)}}));
    EXPECT_EQ(*Ctx().GetProperty(e1, y, ms::View::NEW), ms::PropertyValue{});
    EXPECT_EQ(*Ctx().Properties(e1, ms::View::OLD), PropsOf({{x, Int(1)}, {y, Int(2)}}))
        << "the command's own writes are invisible at OLD";
    acc->AdvanceCommand();
    EXPECT_EQ(*Ctx().Properties(e1, ms::View::OLD), PropsOf({{x, Int(11)}, {z, Int(3)}}));

    auto const stub = EdgeStubOf(*acc, g.e1);
    EXPECT_EQ(stub.properties.GetProperty(x), Int(11));
    EXPECT_EQ(stub.properties.GetProperty(z), Int(3));
    EXPECT_EQ(stub.removed_pids, std::vector<ms::PropertyId>{y});
    EXPECT_EQ(*e1.GetProperty(x, ms::View::OLD), Int(1)) << "main's fork edge is untouched";
    // A removed property can come back; the marker goes with it.
    EXPECT_TRUE(Ctx().SetProperty(e1, y, Int(4))->IsNull());
    EXPECT_TRUE(EdgeStubOf(*acc, g.e1).removed_pids.empty());
    EXPECT_EQ(*Ctx().GetProperty(e1, y, ms::View::NEW), Int(4));
    Ctx().EndTransaction();
    acc->Abort();
  }

  void WholeSetWritesDecomposeIntoPerKeyWrites() {
    auto const x = Prop("x"), y = Prop("y"), w = Prop("w"), k = Prop("k");
    auto const g = MakeGraph();
    auto const bare = CreateMainEdge(g.a, g.c, "E");
    Fork();
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    auto bare_edge = ForkEdge(bare);
    Props init{{k, Int(1)}};
    EXPECT_TRUE(*Ctx().InitProperties(bare_edge, init));
    Props again{{w, Int(2)}};
    EXPECT_FALSE(*Ctx().InitProperties(bare_edge, again)) << "Init only fills an empty property set";
    EXPECT_EQ(*Ctx().Properties(bare_edge, ms::View::NEW), PropsOf({{k, Int(1)}}));

    auto e1 = ForkEdge(g.e1);
    Props update{{x, Int(20)}, {w, Int(4)}};
    auto changes = Ctx().UpdateProperties(e1, update);
    ASSERT_TRUE(changes.has_value());
    EXPECT_EQ(changes->size(), 2u);
    for (auto const &[key, old_value, new_value] : *changes) {
      EXPECT_EQ(old_value, key == x ? Int(1) : ms::PropertyValue{});
      EXPECT_EQ(new_value, key == x ? Int(20) : Int(4));
    }
    EXPECT_EQ(update, PropsOf({{x, Int(20)}, {y, Int(2)}, {w, Int(4)}})) << "the untouched old property comes back";
    EXPECT_EQ(*Ctx().Properties(e1, ms::View::NEW), PropsOf({{x, Int(20)}, {y, Int(2)}, {w, Int(4)}}));

    auto cleared = Ctx().ClearProperties(e1);
    ASSERT_TRUE(cleared.has_value());
    EXPECT_EQ(*cleared, PropsOf({{x, Int(20)}, {y, Int(2)}, {w, Int(4)}}));
    EXPECT_TRUE(Ctx().Properties(e1, ms::View::NEW)->empty());
    auto const stub = EdgeStubOf(*acc, g.e1);
    EXPECT_TRUE(stub.properties.Properties().empty()) << "clearing leaves markers, not values";
    EXPECT_EQ(stub.removed_pids, (std::vector<ms::PropertyId>{x, y}));
    EXPECT_TRUE(Ctx().ClearProperties(e1)->empty()) << "a second clear finds nothing";
    Ctx().EndTransaction();
    acc->Abort();
  }

  // An edge stub is no vertex and no edge of the branch: its marker entries sit in the stubs of the endpoints of a
  // deleted edge only, and the reads come out in fork identity.
  void AnEdgeStubCreatesNothingElseAndIsNeverAnEdgeOfTheBranch() {
    auto const x = Prop("x");
    auto const g = MakeGraph();
    Fork();
    Write([&] {
      auto e1 = ForkEdge(g.e1);
      ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(9)).has_value());
      ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e2)));
    });
    EXPECT_EQ(EdgeStubCount(), 2u);
    EXPECT_EQ(StubCount(), 2u)
        << "the endpoints of the deleted e2 hold its marker entries; the property write needs none";
    EXPECT_EQ(Core().stub_index().FindStub(g.c.AsUint()), nullptr);
    Read([&] {
      EXPECT_FALSE(Ctx().FindDiffEdge(g.e1.gid, ms::View::NEW).has_value());
      EXPECT_FALSE(Ctx().FindDiffEdge(g.e2.gid, g.a, ms::View::NEW).has_value());
      auto out = Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, ms::View::NEW);
      ASSERT_EQ(out.size(), 1u) << "e2 is hidden, e1 stays";
      EXPECT_EQ(out[0].storage_, storage_.get()) << "a fork edge is main's, never its stub";
      EXPECT_EQ(out[0].Gid(), g.e1.gid);
      EXPECT_EQ(*Ctx().GetProperty(out[0], x, ms::View::NEW), Int(9));
      auto diff_acc = Diff().Access(ms::READ);
      size_t native = 0;
      for ([[maybe_unused]] auto v : diff_acc->Vertices(ms::View::NEW)) ++native;
      EXPECT_EQ(native, 0u);
    });
  }

  void DeletingAForkEdgeWritesOnlyItsMarkersAndHidesItPerViewThenCommitted() {
    auto const x = Prop("x");
    auto const g = MakeGraph();
    Fork();
    // The stub already holds properties and removal markers: the delete must not read or rewrite them, and writes only
    // its own markers.
    Write([&] {
      auto e1 = ForkEdge(g.e1);
      ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(2)).has_value());
      ASSERT_TRUE(Ctx().SetProperty(e1, Prop("y"), ms::PropertyValue{}).has_value());
      ASSERT_TRUE(Ctx().SetProperty(e1, Prop("z"), Int(3)).has_value());
    });
    {
      auto acc = Diff().Access(ms::WRITE);
      Begin(acc.get());
      auto e1 = ForkEdge(g.e1);
      auto const deltas_before = acc->GetTransaction()->deltas.size();
      ASSERT_EQ(deltas_before, 0u);
      auto deleted = Ctx().DeleteForkEdge(e1);
      ASSERT_TRUE(deleted.has_value());
      EXPECT_TRUE(*deleted);
      EXPECT_EQ(acc->GetTransaction()->deltas.size(), 3u) << "the edge stub's marker and one marker entry per endpoint";
      EXPECT_TRUE(Ctx().IsForkEdgeHidden(e1, ms::View::NEW));
      EXPECT_FALSE(Ctx().IsForkEdgeHidden(e1, ms::View::OLD))
          << "the deleting command's own delete is invisible at OLD";
      EXPECT_TRUE(Core().MayHaveForkEdgeDeleted(g.e1.gid));
      EXPECT_TRUE(Core().MayHaveEdgeChange(g.a) && Core().MayHaveEdgeChange(g.b));
      EXPECT_EQ(Ctx().Properties(e1, ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(Ctx().SetProperty(e1, x, Int(5)).error(), ms::Error::DELETED_OBJECT);
      EXPECT_FALSE(*Ctx().DeleteForkEdge(e1)) << "a repeated delete is a no-op";
      EXPECT_FALSE(Ctx().ResolveEdge(g.e1.gid, g.a, ms::View::NEW).has_value());
      EXPECT_TRUE(CommittedEdgeStubOf(g.e1).properties.GetProperty(x) == Int(2)) << "committed state, not the delete";
      ASSERT_TRUE(Ctx().CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
      Ctx().EndTransaction();
    }
    EXPECT_TRUE(CommittedEdgeStubOf(g.e1).deleted);
    EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 1}));
    Read([&] {
      for (auto const view : {ms::View::NEW, ms::View::OLD}) {
        EXPECT_TRUE(Ctx().IsForkEdgeHidden(ForkEdge(g.e1), view));
        EXPECT_EQ(Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, view).size(), 1u) << "e2 stays";
        EXPECT_EQ(Ctx().ResolveEdges(g.b, ms::EdgeDirection::IN, view).size(), 1u);
        EXPECT_FALSE(Ctx().IsForkEdgeHidden(ForkEdge(g.e2), view));
      }
    });
  }

  void AnAbortedEdgeWriteLeavesTheForkEdgeReadableAndAnEmptyStub() {
    auto const x = Prop("x"), y = Prop("y");
    auto const g = MakeGraph();
    Fork();
    for (int round = 0; round < 50; ++round) {
      WriteAborted([&] {
        auto e1 = ForkEdge(g.e1);
        ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(100 + round)).has_value());
        ASSERT_TRUE(Ctx().SetProperty(e1, y, ms::PropertyValue{}).has_value());
        ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e2)));
        ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
      });
    }
    EXPECT_EQ(EdgeStubCount(), 2u) << "one stub per touched edge however often the writes abort";
    EXPECT_TRUE(IsEmpty(CommittedEdgeStubOf(g.e1)));
    EXPECT_TRUE(IsEmpty(CommittedEdgeStubOf(g.e2)));
    EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 0}));
    Read([&] {
      for (auto const &edge : {g.e1, g.e2}) {
        EXPECT_FALSE(Ctx().IsForkEdgeHidden(ForkEdge(edge), ms::View::NEW));
      }
      EXPECT_EQ(*Ctx().Properties(ForkEdge(g.e1), ms::View::NEW), PropsOf({{x, Int(1)}, {y, Int(2)}}));
      EXPECT_EQ(Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, ms::View::NEW).size(), 2u);
    });
    Write([&] {
      auto e1 = ForkEdge(g.e1);
      ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(3)).has_value());
    });
  }
};

class VersioningBranchEdgeWriteLightEdgeTest : public VersioningBranchEdgeWriteTest {
 protected:
  bool LightEdge() const override { return true; }
};

class VersioningBranchEdgeWriteNoEdgePropertiesTest : public VersioningBranchEdgeWriteTest {
 protected:
  bool PropertiesOnEdges() const override { return false; }
};

#define EDGE_WRITE_TEST(Name)                             \
  TEST_F(VersioningBranchEdgeWriteTest, Name) { Name(); } \
  TEST_F(VersioningBranchEdgeWriteLightEdgeTest, Name) { Name(); }

EDGE_WRITE_TEST(PropertyWritesMergeOverTheForkEdge)
EDGE_WRITE_TEST(WholeSetWritesDecomposeIntoPerKeyWrites)
EDGE_WRITE_TEST(AnEdgeStubCreatesNothingElseAndIsNeverAnEdgeOfTheBranch)
EDGE_WRITE_TEST(DeletingAForkEdgeWritesOnlyItsMarkersAndHidesItPerViewThenCommitted)
EDGE_WRITE_TEST(AnAbortedEdgeWriteLeavesTheForkEdgeReadableAndAnEmptyStub)

// A native edge is the diff engine's own object: the resolver hands it to the stock mutators and creates no stub.
TEST_F(VersioningBranchEdgeWriteTest, ANativeEdgeTakesTheStockMutators) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  Write([&] {
    auto from = Ctx().CreateNativeVertex();
    auto to = Ctx().CreateNativeVertex();
    auto edge = Ctx().CreateNativeEdge(from, to, storage_->NameToEdgeType("N"));
    ASSERT_TRUE(edge.has_value());
    EXPECT_EQ(edge->storage_, &Diff());
    EXPECT_TRUE(Ctx().SetProperty(*edge, x, Int(1))->IsNull());
    Props init{{Prop("y"), Int(2)}};
    EXPECT_FALSE(*Ctx().InitProperties(*edge, init)) << "the stock Init: the edge already has x";
    Props update{{x, Int(3)}};
    EXPECT_TRUE(Ctx().UpdateProperties(*edge, update).has_value());
    EXPECT_EQ(*Ctx().GetProperty(*edge, x, ms::View::NEW), Int(3));
    EXPECT_EQ(Ctx().ClearProperties(*edge)->size(), 1u);
    EXPECT_TRUE(Ctx().Properties(*edge, ms::View::NEW)->empty());
  });
  EXPECT_EQ(EdgeStubCount(), 0u);
  EXPECT_EQ(StubCount(), 0u);
  (void)g;
}

// With main's edge properties off, the branch's fork edges still delete (a marker is a property of the diff engine's
// own Edge), but their properties cannot be written, as on main.
TEST_F(VersioningBranchEdgeWriteNoEdgePropertiesTest, ADeleteWorksAndAPropertyWriteIsRefused) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  EXPECT_FALSE(storage_->config_.salient.items.properties_on_edges);
  EXPECT_TRUE(Diff().config_.salient.items.properties_on_edges);
  Write([&] {
    auto e1 = ForkEdge(g.e1);
    EXPECT_EQ(Ctx().SetProperty(e1, x, Int(1)).error(), ms::Error::PROPERTIES_DISABLED);
    Props props{{x, Int(1)}};
    EXPECT_EQ(Ctx().InitProperties(e1, props).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_EQ(Ctx().UpdateProperties(e1, props).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_EQ(Ctx().ClearProperties(e1).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_EQ(EdgeStubCount(), 0u) << "a refused write creates no stub";
    EXPECT_FALSE(e1.MaybeBranchedHint());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(e1));
    EXPECT_TRUE(Ctx().IsForkEdgeHidden(e1, ms::View::NEW));
    EXPECT_FALSE(*Ctx().DeleteForkEdge(e1));
  });
  EXPECT_TRUE(CommittedEdgeStubOf(g.e1).deleted);
  Read([&] {
    EXPECT_TRUE(Ctx().IsForkEdgeHidden(ForkEdge(g.e1), ms::View::NEW));
    EXPECT_FALSE(Ctx().ResolveEdge(g.e1.gid, g.a, ms::View::NEW).has_value());
    auto out = Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, ms::View::NEW);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].Gid(), g.e2.gid);
    EXPECT_FALSE(Ctx().IsForkEdgeHidden(ForkEdge(g.e2), ms::View::NEW));
  });
}

// A branch-native edge is the diff engine's own object, which always has properties: it must refuse them exactly when
// main would.
TEST_F(VersioningBranchEdgeWriteNoEdgePropertiesTest, ANativeEdgeRefusesPropertyWritesToo) {
  auto const x = Prop("x");
  MakeGraph();
  Fork();
  Write([&] {
    auto from = Ctx().CreateNativeVertex();
    auto to = Ctx().CreateNativeVertex();
    auto edge = Ctx().CreateNativeEdge(from, to, storage_->NameToEdgeType("N"));
    ASSERT_TRUE(edge.has_value());
    EXPECT_EQ(edge->storage_, &Diff());
    EXPECT_EQ(Ctx().SetProperty(*edge, x, Int(1)).error(), ms::Error::PROPERTIES_DISABLED);
    Props props{{x, Int(1)}};
    EXPECT_EQ(Ctx().InitProperties(*edge, props).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_EQ(Ctx().UpdateProperties(*edge, props).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_EQ(Ctx().ClearProperties(*edge).error(), ms::Error::PROPERTIES_DISABLED);
    EXPECT_TRUE(Ctx().Properties(*edge, ms::View::NEW)->empty());
  });
}

// Two transactions writing one fork edge conflict however different their keys, a transaction that started before a
// commit conflicts with it, one that starts after does not: the stub's MVCC is the edge's.
TEST_F(VersioningBranchEdgeWriteTest, EdgeStubWritersFollowFirstWriterWins) {
  auto const x = Prop("x"), y = Prop("y");
  auto const g = MakeGraph();
  Fork();
  mv::BranchContext s2{Ctx().core()};
  auto t1 = Diff().Access(ms::WRITE);
  auto t2 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
  auto e1_t1 = ForkEdge(g.e1);
  auto e1_t2 = *s2.FindForkEdge(g.e1.gid, g.a);
  auto e2_t2 = *s2.FindForkEdge(g.e2.gid, g.a);
  ASSERT_TRUE(Ctx().SetProperty(e1_t1, x, Int(10)).has_value());
  EXPECT_TRUE(s2.SetProperty(e2_t2, x, Int(6)).has_value()) << "another edge is another object";
  EXPECT_EQ(s2.SetProperty(e1_t2, y, Int(1)).error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t2->TransactionHasSerializationError());
  EXPECT_EQ(s2.DeleteForkEdge(e1_t2).error(), ms::Error::SERIALIZATION_ERROR);
  Props props{{y, Int(1)}};
  EXPECT_EQ(s2.UpdateProperties(e1_t2, props).error(), ms::Error::SERIALIZATION_ERROR);
  s2.EndTransaction();
  t2->Abort();

  // A transaction that began before the commit conflicts with it; the one that begins after does not.
  mv::BranchContext s3{Ctx().core()};
  auto t3 = Diff().Access(ms::WRITE);
  ASSERT_TRUE(s3.BeginTransaction(t3.get()).has_value());
  Commit(*t1);
  Ctx().EndTransaction();
  auto e1_t3 = *s3.FindForkEdge(g.e1.gid, g.a);
  EXPECT_EQ(s3.SetProperty(e1_t3, y, Int(2)).error(), ms::Error::SERIALIZATION_ERROR);
  s3.EndTransaction();
  t3->Abort();
  Write([&] {
    auto later = ForkEdge(g.e1);
    EXPECT_TRUE(Ctx().SetProperty(later, y, Int(3)).has_value());
  });
}

// A write that changes nothing still conflicts, as the stock edge mutators do.
TEST_F(VersioningBranchEdgeWriteTest, EdgeWritesThatChangeNothingStillConflictAsOnMain) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  mv::BranchContext s2{Ctx().core()};
  auto t1 = Diff().Access(ms::WRITE);
  auto t2 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
  auto e1_t1 = ForkEdge(g.e1);
  ASSERT_TRUE(Ctx().SetProperty(e1_t1, x, Int(10)).has_value());
  auto e1_t2 = *s2.FindForkEdge(g.e1.gid, g.a);
  Props nothing;
  auto e2_t2 = *s2.FindForkEdge(g.e2.gid, g.a);
  EXPECT_TRUE(s2.ClearProperties(e2_t2).has_value()) << "another edge: no conflict";
  EXPECT_TRUE(s2.UpdateProperties(e1_t2, nothing).has_value()) << "an update of no key writes nothing";
  Props init{{x, Int(1)}};
  EXPECT_EQ(s2.InitProperties(e1_t2, init).error(), ms::Error::SERIALIZATION_ERROR)
      << "Init of a non-empty edge finds nothing to do but conflicts first";
  s2.EndTransaction();
  t2->Abort();
  Ctx().EndTransaction();
  t1->Abort();
}

// Many threads touching one fork edge for the first time: the stub is created once, exactly one writer wins.
TEST_F(VersioningBranchEdgeWriteTest, ConcurrentFirstTouchOfOneForkEdgeLetsExactlyOneWriterWin) {
  constexpr int kThreads = 6;
  constexpr int kRounds = 60;
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  std::vector<MainEdge> edges;
  for (int i = 0; i < kRounds; ++i) edges.push_back(CreateMainEdge(a, b, "E"));
  std::vector<ms::PropertyId> keys;
  for (int t = 0; t < kThreads; ++t) keys.push_back(Prop(("k" + std::to_string(t)).c_str()));
  Fork();
  auto core = Ctx().core();
  std::barrier sync(kThreads);
  std::array<std::atomic<int>, kRounds> winners{};
  std::vector<std::jthread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      mv::BranchContext ctx{core};
      for (int round = 0; round < kRounds; ++round) {
        auto acc = Diff().Access(ms::WRITE, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        sync.arrive_and_wait();
        auto edge = *ctx.FindForkEdge(edges[round].gid, a);
        auto const written = ctx.SetProperty(edge, keys[t], Int(t));
        if (written) {
          ++winners[round];
        } else {
          EXPECT_EQ(written.error(), ms::Error::SERIALIZATION_ERROR);
        }
        ctx.EndTransaction();
        if (written) {
          Commit(*acc);
        } else {
          acc->Abort();
        }
        sync.arrive_and_wait();
      }
    });
  }
  threads.clear();
  for (int round = 0; round < kRounds; ++round) EXPECT_EQ(winners[round].load(), 1) << "round " << round;
  EXPECT_EQ(EdgeStubCount(), static_cast<size_t>(kRounds)) << "one stub per edge";
  Read([&] {
    for (auto const &edge : edges) {
      EXPECT_EQ(Ctx().Properties(ForkEdge(edge), ms::View::NEW)->size(), 1u) << "only the winner's key";
    }
  });
}

// One transaction sets a property of a fork edge and then deletes it: the delete wins and the edge counts once; the
// other order refuses the write.
TEST_F(VersioningBranchEdgeWriteTest, ASetThenADeleteOfOneEdgeInOneTransaction) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  WriteCounted([&] {
    auto e1 = ForkEdge(g.e1);
    ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(8)).has_value());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(e1));
    EXPECT_EQ(Ctx().SetProperty(e1, x, Int(9)).error(), ms::Error::DELETED_OBJECT);
    Props props{{x, Int(9)}};
    EXPECT_EQ(Ctx().UpdateProperties(e1, props).error(), ms::Error::DELETED_OBJECT);
    EXPECT_EQ(Ctx().ClearProperties(e1).error(), ms::Error::DELETED_OBJECT);
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 1}));
  Read([&] {
    auto e1 = ForkEdge(g.e1);
    for (auto const view : {ms::View::NEW, ms::View::OLD}) EXPECT_TRUE(Ctx().IsForkEdgeHidden(e1, view));
    EXPECT_EQ(Ctx().GetProperty(e1, x, ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
  });
}

///// The commit's counts

// Counts reach the core when the transaction commits: an abort publishes nothing, and an edge hidden by its deleted
// endpoint counts with the vertex, once however it was deleted.
TEST_F(VersioningBranchEdgeWriteTest, ForkDeletesArePublishedByTheCommitAndCountEachHiddenEdgeOnce) {
  auto const g = MakeGraph();
  Fork();
  WriteAborted([&] {
    ASSERT_TRUE(*Ctx().DeleteForkVertex(g.a));
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.loop)));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 0}));

  // The edge goes first, then its endpoint: e1 counts once, e2 with the vertex.
  WriteCounted([&] {
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
    ASSERT_TRUE(*Ctx().DeleteForkVertex(g.a));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{1, 2}));

  // Both edges are already hidden by a, so b hides none; the self-loop is one edge.
  WriteCounted([&] {
    ASSERT_TRUE(*Ctx().DeleteForkVertex(g.b));
    ASSERT_TRUE(*Ctx().DeleteForkVertex(g.c));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{3, 3}));
}

TEST_F(VersioningBranchEdgeWriteTest, ADeletedEndpointThenItsEdgeCountsTheEdgeOnce) {
  auto const g = MakeGraph();
  Fork();
  WriteCounted([&] {
    ASSERT_TRUE(*Ctx().DeleteForkVertex(g.a));
    EXPECT_FALSE(*Ctx().DeleteForkEdge(ForkEdge(g.e1))) << "hidden by its deleted endpoint";
    EXPECT_FALSE(*Ctx().DeleteForkVertex(g.a));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{1, 2}));
}

TEST_F(VersioningBranchEdgeWriteTest, AHubDeleteCountsEveryEdgeAndAPropertyWriteCountsNone) {
  auto const x = Prop("x");
  auto const hub = CreateMainVertex({}, {});
  constexpr int kSpokes = 100;
  std::vector<MainEdge> spokes;
  for (int i = 0; i < kSpokes; ++i) {
    auto const leaf = CreateMainVertex({}, {});
    spokes.push_back(i % 2 == 0 ? CreateMainEdge(hub, leaf, "E", {{x, Int(i)}}) : CreateMainEdge(leaf, hub, "E"));
  }
  Fork();
  WriteCounted([&] {
    auto e = ForkEdge(spokes[0]);
    ASSERT_TRUE(Ctx().SetProperty(e, x, Int(-1)).has_value());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(spokes[2])));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 1}));
  WriteCounted([&] { ASSERT_TRUE(*Ctx().DeleteForkVertex(hub)); });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{1, 1 + kSpokes - 1}));
}

// Each periodic batch publishes only what it deleted, and a batch that never commits publishes nothing.
TEST_F(VersioningBranchEdgeWriteTest, PeriodicCommitPublishesEachBatchsForkDeletesOnce) {
  auto const g = MakeGraph();
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
  ASSERT_TRUE(Ctx().PeriodicCommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 1}));
  ASSERT_TRUE(Ctx().in_transaction());
  ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e2)));
  ASSERT_TRUE(Ctx().PeriodicCommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 2})) << "the first batch is not published again";
  ASSERT_TRUE(*Ctx().DeleteForkVertex(g.c));
  ASSERT_TRUE(Ctx().CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{1, 3}));
  Ctx().EndTransaction();

  auto aborted = Diff().Access(ms::WRITE);
  Begin(aborted.get());
  ASSERT_TRUE(*Ctx().DeleteForkVertex(g.b));
  Ctx().EndTransaction();
  aborted->Abort();
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{1, 3}));
}

///// A delete of a vertex against a delete of one of its edges (BL-0239)

// Whichever delete is written second finds the other and fails, so the edge is hidden by exactly one committed delete.
TEST_F(VersioningBranchEdgeWriteTest, ADeleteOfAVertexAndOfOneOfItsEdgesConflictInEitherOrder) {
  auto const g = MakeGraph();
  Fork();
  for (bool edge_first : {true, false}) {
    SCOPED_TRACE(edge_first);
    mv::BranchContext s2{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
    std::optional<ms::Error> edge_error, vertex_error;
    auto const delete_edge = [&] {
      auto const deleted = Ctx().DeleteForkEdge(ForkEdge(g.e1));
      if (!deleted) edge_error = deleted.error();
    };
    auto const delete_vertex = [&] {
      auto const deleted = s2.DeleteForkVertex(g.a);
      if (!deleted) vertex_error = deleted.error();
    };
    if (edge_first) {
      delete_edge();
      delete_vertex();
      EXPECT_FALSE(edge_error.has_value());
      EXPECT_EQ(vertex_error, std::optional{ms::Error::SERIALIZATION_ERROR});
      EXPECT_TRUE(t2->TransactionHasSerializationError());
    } else {
      delete_vertex();
      delete_edge();
      EXPECT_FALSE(vertex_error.has_value());
      EXPECT_EQ(edge_error, std::optional{ms::Error::SERIALIZATION_ERROR});
      EXPECT_TRUE(t1->TransactionHasSerializationError());
    }
    Ctx().EndTransaction();
    s2.EndTransaction();
    t1->Abort();
    t2->Abort();
  }
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 0}));
}

// The race the two orders above cannot show: the delete of vertex a and the delete of its edge e run at once, many
// times. At most one commits, and the edge is counted once per round.
TEST_F(VersioningBranchEdgeWriteTest, ADeleteOfAVertexAndOfItsEdgeNeverBothCommit) {
  constexpr int kRounds = 1500;
  std::vector<MainEdge> edges;
  for (int round = 0; round < kRounds; ++round) {
    auto const from = CreateMainVertex({}, {});
    auto const to = CreateMainVertex({}, {});
    edges.push_back(CreateMainEdge(from, to, "E"));
  }
  Fork();
  auto core = Ctx().core();
  std::barrier sync(2);
  std::vector<char> edge_committed(kRounds, 0), vertex_committed(kRounds, 0);
  auto const run = [&](bool delete_edge) {
    mv::BranchContext ctx{core};
    for (int round = 0; round < kRounds; ++round) {
      auto acc = Diff().Access(ms::WRITE, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
      ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
      auto const &edge = edges[round];
      auto const fork_edge = delete_edge ? ctx.FindForkEdge(edge.gid, edge.from) : std::nullopt;
      sync.arrive_and_wait();
      bool const ok = delete_edge ? ctx.DeleteForkEdge(*fork_edge).value_or(false)
                                  : ctx.DeleteForkVertex(edge.from).value_or(false);
      bool const committed = ok && !acc->TransactionHasSerializationError() &&
                             ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value();
      ctx.EndTransaction();
      if (!committed) acc->Abort();
      (delete_edge ? edge_committed : vertex_committed)[round] = committed ? 1 : 0;
      sync.arrive_and_wait();
    }
  };
  {
    std::jthread vertex_deleter{[&] { run(false); }};
    run(true);
  }
  uint64_t edge_rounds = 0, vertex_rounds = 0, rounds_with_a_commit = 0;
  for (int round = 0; round < kRounds; ++round) {
    EXPECT_FALSE(edge_committed[round] && vertex_committed[round]) << "both deletes of round " << round << " committed";
    edge_rounds += edge_committed[round];
    vertex_rounds += vertex_committed[round];
    rounds_with_a_commit += edge_committed[round] || vertex_committed[round];
  }
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{vertex_rounds, rounds_with_a_commit}))
      << "each round hides its one edge once (edge deletes committed: " << edge_rounds << ")";
}

// A fork edge written by a transaction in flight blocks a DETACH DELETE of its endpoint, whatever the write was.
TEST_F(VersioningBranchEdgeWriteTest, AnEdgeWriteInFlightBlocksTheDeleteOfItsEndpoint) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  mv::BranchContext s2{Ctx().core()};
  auto t1 = Diff().Access(ms::WRITE);
  auto t2 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
  auto e1 = ForkEdge(g.e1);
  ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(2)).has_value());
  EXPECT_FALSE(t1->TransactionHasSerializationError());
  EXPECT_EQ(s2.DeleteForkVertex(g.a).error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t2->TransactionHasSerializationError());
  Ctx().EndTransaction();
  s2.EndTransaction();
  t1->Abort();
  t2->Abort();
}

// The other order (BL-0249): an edge write after the delete of its endpoint, the delete in flight or committed after
// this transaction started, conflicts too. Otherwise the branch WAL would hold [VertexDelete][EdgeSetProperty] and
// not replay.
TEST_F(VersioningBranchEdgeWriteTest, AnEdgeWriteAfterTheDeleteOfItsEndpointConflicts) {
  auto const x = Prop("x");
  enum class Write { kSet, kInit, kUpdate, kClear, kDelete };

  struct Case {
    Write write;
    bool delete_from, committed;
    Graph g;
    MainEdge bare;
  };

  std::vector<Case> cases;
  for (auto const write : {Write::kSet, Write::kInit, Write::kUpdate, Write::kClear, Write::kDelete}) {
    for (bool delete_from : {true, false}) {
      for (bool committed : {true, false}) {
        auto const g = MakeGraph();
        cases.push_back({write, delete_from, committed, g, CreateMainEdge(g.a, g.b, "E")});
      }
    }
  }
  Fork();
  for (auto const &[write, delete_from, committed, g, bare] : cases) {
    {
      {
        SCOPED_TRACE(testing::Message() << "write " << static_cast<int>(write) << " delete_from " << delete_from
                                        << " committed " << committed);
        mv::BranchContext s2{Ctx().core()};
        auto t1 = Diff().Access(ms::WRITE);
        auto t2 = Diff().Access(ms::WRITE);
        Begin(t1.get());
        ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
        ASSERT_TRUE(*s2.DeleteForkVertex(delete_from ? g.a : g.b));
        if (committed) Commit(*t2);
        auto edge = ForkEdge(write == Write::kInit ? bare : g.e1);
        Props map{{x, Int(9)}};
        std::optional<ms::Error> error;
        switch (write) {
          case Write::kSet:
            error = Ctx().SetProperty(edge, x, Int(2)).error_or(ms::Error::DELETED_OBJECT);
            break;
          case Write::kInit:
            error = Ctx().InitProperties(edge, map).error_or(ms::Error::DELETED_OBJECT);
            break;
          case Write::kUpdate:
            error = Ctx().UpdateProperties(edge, map).error_or(ms::Error::DELETED_OBJECT);
            break;
          case Write::kClear:
            error = Ctx().ClearProperties(edge).error_or(ms::Error::DELETED_OBJECT);
            break;
          case Write::kDelete:
            error = Ctx().DeleteForkEdge(edge).error_or(ms::Error::DELETED_OBJECT);
            break;
        }
        EXPECT_EQ(error, std::optional{ms::Error::SERIALIZATION_ERROR});
        EXPECT_TRUE(t1->TransactionHasSerializationError());
        Ctx().EndTransaction();
        s2.EndTransaction();
        t1->Abort();
        if (!committed) t2->Abort();
      }
    }
  }
}

// Two DETACH DELETEs of the two endpoints of one edge (BL-0243): the edge is hidden by exactly one committed delete,
// so one of the two fails whichever is written second.
TEST_F(VersioningBranchEdgeWriteTest, TheDeletesOfBothEndpointsOfAnEdgeConflict) {
  auto const g = MakeGraph();
  Fork();
  mv::BranchContext s2{Ctx().core()};
  auto t1 = Diff().Access(ms::WRITE);
  auto t2 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
  EXPECT_TRUE(Ctx().DeleteForkVertex(g.a).has_value());
  auto const second = s2.DeleteForkVertex(g.b);
  EXPECT_EQ(second.error_or(ms::Error::DELETED_OBJECT), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t2->TransactionHasSerializationError());
  Ctx().EndTransaction();
  s2.EndTransaction();
  t1->Abort();
  t2->Abort();
}

TEST_F(VersioningBranchEdgeWriteTest, TheDeletesOfBothEndpointsOfAnEdgeNeverBothCommit) {
  constexpr int kRounds = 1500;
  std::vector<MainEdge> edges;
  for (int round = 0; round < kRounds; ++round) {
    auto const from = CreateMainVertex({}, {});
    auto const to = CreateMainVertex({}, {});
    edges.push_back(CreateMainEdge(from, to, "E"));
  }
  Fork();
  auto core = Ctx().core();
  std::barrier sync(2);
  std::vector<char> from_committed(kRounds, 0), to_committed(kRounds, 0);
  auto const run = [&](bool delete_to) {
    mv::BranchContext ctx{core};
    for (int round = 0; round < kRounds; ++round) {
      auto acc = Diff().Access(ms::WRITE, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
      ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
      auto const &edge = edges[round];
      sync.arrive_and_wait();
      bool const ok = ctx.DeleteForkVertex(delete_to ? edge.to : edge.from).value_or(false);
      bool const committed = ok && !acc->TransactionHasSerializationError() &&
                             ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value();
      ctx.EndTransaction();
      if (!committed) acc->Abort();
      (delete_to ? to_committed : from_committed)[round] = committed ? 1 : 0;
      sync.arrive_and_wait();
    }
  };
  {
    std::jthread to_deleter{[&] { run(true); }};
    run(false);
  }
  uint64_t rounds_with_a_commit = 0, vertices = 0;
  for (int round = 0; round < kRounds; ++round) {
    EXPECT_FALSE(from_committed[round] && to_committed[round]) << "both deletes of round " << round << " committed";
    rounds_with_a_commit += from_committed[round] || to_committed[round];
    vertices += from_committed[round] + to_committed[round];
  }
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{vertices, rounds_with_a_commit}));
}

// A write of an endpoint's property is no delete of it: it does not conflict with a write of the edge's properties.
TEST_F(VersioningBranchEdgeWriteTest, AnEdgeWriteDoesNotConflictWithAnEndpointPropertyWrite) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  mv::BranchContext s2{Ctx().core()};
  auto t1 = Diff().Access(ms::WRITE);
  auto t2 = Diff().Access(ms::WRITE);
  Begin(t1.get());
  ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
  auto a = *s2.FindForkVertex(g.a);
  ASSERT_TRUE(s2.SetProperty(a, x, Int(3)).has_value());
  auto e1 = ForkEdge(g.e1);
  EXPECT_TRUE(Ctx().SetProperty(e1, x, Int(2)).has_value());
  EXPECT_FALSE(t1->TransactionHasSerializationError());
  Commit(*t1);
  Ctx().EndTransaction();
  Commit(*t2);
  s2.EndTransaction();
}

// A fork edge delete writes the stubs of both its endpoints, as main's delete writes both vertices: two deletes of
// edges that share an endpoint conflict, whichever endpoint they share.
TEST_F(VersioningBranchEdgeWriteTest, TwoDeletesOfForkEdgesSharingAnEndpointConflict) {
  auto const g = MakeGraph();
  auto const extra = CreateMainEdge(g.b, g.c, "E", {});
  Fork();

  struct Case {
    const MainEdge &second;
    const char *shared;
  };

  for (auto const &[second, shared] : {Case{g.e2, "both endpoints"}, Case{extra, "the head of the first"}}) {
    SCOPED_TRACE(shared);
    mv::BranchContext s2{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
    auto const fork_edge = s2.FindForkEdge(second.gid, second.from);
    ASSERT_TRUE(fork_edge.has_value());
    auto const deleted = s2.DeleteForkEdge(*fork_edge);
    ASSERT_FALSE(deleted.has_value());
    EXPECT_EQ(deleted.error(), ms::Error::SERIALIZATION_ERROR);
    EXPECT_TRUE(t2->TransactionHasSerializationError());
    Ctx().EndTransaction();
    s2.EndTransaction();
    t1->Abort();
    t2->Abort();
  }
}

// A fork edge delete conflicts with a write of either endpoint, in either order.
TEST_F(VersioningBranchEdgeWriteTest, ADeleteOfAForkEdgeConflictsWithAWriteOfItsEndpointInEitherOrder) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  for (auto const endpoint : {g.a, g.b}) {
    for (bool delete_first : {true, false}) {
      SCOPED_TRACE(std::to_string(endpoint.AsUint()) + (delete_first ? " delete first" : " write first"));
      mv::BranchContext s2{Ctx().core()};
      auto t1 = Diff().Access(ms::WRITE);
      auto t2 = Diff().Access(ms::WRITE);
      Begin(t1.get());
      ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
      auto vertex = *s2.FindForkVertex(endpoint);
      std::optional<ms::Error> error;
      if (delete_first) {
        ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
        if (auto written = s2.SetProperty(vertex, x, Int(3)); !written) error = written.error();
        EXPECT_TRUE(t2->TransactionHasSerializationError());
      } else {
        ASSERT_TRUE(s2.SetProperty(vertex, x, Int(3)).has_value());
        auto deleted = Ctx().DeleteForkEdge(ForkEdge(g.e1));
        if (!deleted) error = deleted.error();
        EXPECT_TRUE(t1->TransactionHasSerializationError());
      }
      EXPECT_EQ(error, std::optional{ms::Error::SERIALIZATION_ERROR});
      Ctx().EndTransaction();
      s2.EndTransaction();
      t1->Abort();
      t2->Abort();
    }
  }
}

// A native edge created on an endpoint conflicts with a fork edge delete there, as main's create fails over an
// in-flight delete, in either order.
TEST_F(VersioningBranchEdgeWriteTest, ANativeEdgeOnAnEndpointAndAForkEdgeDeleteConflictInEitherOrder) {
  auto const g = MakeGraph();
  Fork();
  auto const type = storage_->NameToEdgeType("N");
  for (bool delete_first : {true, false}) {
    SCOPED_TRACE(delete_first ? "delete first" : "create first");
    mv::BranchContext s2{Ctx().core()};
    auto t1 = Diff().Access(ms::WRITE);
    auto t2 = Diff().Access(ms::WRITE);
    Begin(t1.get());
    ASSERT_TRUE(s2.BeginTransaction(t2.get()).has_value());
    auto const create = [&](mv::BranchContext &ctx) -> std::optional<ms::Error> {
      auto from = ctx.EnsureDiffVertex(g.a);
      auto to = ctx.EnsureDiffVertex(g.c);
      if (!from || !to) return ms::Error::SERIALIZATION_ERROR;
      auto edge = ctx.CreateNativeEdge(*from, *to, type);
      return edge ? std::nullopt : std::optional{edge.error()};
    };
    std::optional<ms::Error> error;
    if (delete_first) {
      ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
      error = create(s2);
    } else {
      ASSERT_FALSE(create(s2).has_value());
      auto deleted = Ctx().DeleteForkEdge(ForkEdge(g.e1));
      if (!deleted) error = deleted.error();
    }
    EXPECT_EQ(error, std::optional{ms::Error::SERIALIZATION_ERROR});
    Ctx().EndTransaction();
    s2.EndTransaction();
    t1->Abort();
    t2->Abort();
  }
}

// Random edge and vertex deletes, committed and rolled back: for every vertex and direction the adjacency drops
// exactly the fork edges the point lookups hide, for a snapshot reader and a read-committed one.
TEST_F(VersioningBranchEdgeWriteTest, TheAdjacencyHidesExactlyWhatThePointLookupsHideUnderRandomDeletesAndRollbacks) {
  constexpr int kVertices = 7;
  constexpr int kEdges = 22;
  constexpr int kRounds = 60;
  std::mt19937 rng{20261107};
  std::vector<ms::Gid> vertices;
  for (int i = 0; i < kVertices; ++i) vertices.push_back(CreateMainVertex({}, {}));
  std::vector<MainEdge> edges;
  for (int i = 0; i < kEdges; ++i) {
    auto const from = vertices[rng() % kVertices];
    auto const to = i % 5 == 0 ? from : vertices[rng() % kVertices];
    edges.push_back(CreateMainEdge(from, to, i % 2 == 0 ? "E" : "F"));
  }
  Fork();

  mv::BranchContext rc{Ctx().core()};
  auto rc_acc = Diff().Access(ms::READ, ms::IsolationLevel::READ_COMMITTED, std::nullopt);
  ASSERT_TRUE(rc.BeginTransaction(rc_acc.get()).has_value());

  auto const check = [&](mv::BranchContext &ctx, ms::View view, int round) {
    for (auto const vertex : vertices) {
      for (auto const direction : {ms::EdgeDirection::OUT, ms::EdgeDirection::IN}) {
        bool deleted = false;
        std::set<uint64_t> expected, actual;
        for (auto const &edge : edges) {
          if ((direction == ms::EdgeDirection::OUT ? edge.from : edge.to) != vertex) continue;
          auto const fork_edge = ctx.FindForkEdge(edge.gid, edge.from);
          ASSERT_TRUE(fork_edge.has_value());
          if (!ctx.IsForkEdgeHidden(*fork_edge, view)) expected.insert(edge.gid.AsUint());
        }
        for (auto const &edge : ctx.ResolveEdges(vertex, direction, view, &deleted)) actual.insert(edge.Gid().AsUint());
        if (deleted) {
          EXPECT_TRUE(actual.empty());
          continue;
        }
        EXPECT_EQ(actual, expected) << "round " << round << ", vertex " << vertex.AsUint() << ", "
                                    << (direction == ms::EdgeDirection::OUT ? "out" : "in");
      }
    }
  };

  for (int round = 0; round < kRounds; ++round) {
    bool const commit = rng() % 3 != 0;
    auto acc = Diff().Access(ms::WRITE);
    Begin(acc.get());
    for (int step = 0, steps = 1 + static_cast<int>(rng() % 3); step < steps; ++step) {
      if (rng() % 3 == 0) {
        auto const deleted = Ctx().DeleteForkVertex(vertices[rng() % kVertices]);
        ASSERT_TRUE(deleted.has_value());
      } else {
        auto const &edge = edges[rng() % kEdges];
        auto const deleted = Ctx().DeleteForkEdge(ForkEdge(edge));
        ASSERT_TRUE(deleted.has_value());
      }
    }
    if (commit) {
      Commit(*acc);
    }
    Ctx().EndTransaction();
    if (!commit) acc->Abort();

    auto reader = Diff().Access(ms::READ);
    Begin(reader.get());
    check(Ctx(), ms::View::NEW, round);
    check(Ctx(), ms::View::OLD, round);
    Ctx().EndTransaction();
    check(rc, ms::View::NEW, round);
  }
  rc.EndTransaction();
}

// The teardown frees the light edges a native edge hangs on a stub, and must leave alone the edge stubs the marker
// entries of deleted fork edges hold: the stub index owns those.
TEST_F(VersioningBranchEdgeWriteLightEdgeTest, TeardownLeavesTheEdgeStubsOfMarkerEntriesToTheStubIndex) {
  auto const g = MakeGraph();
  Fork();
  auto const type = storage_->NameToEdgeType("N");
  WriteCounted([&] {
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e1)));
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.loop)));
    auto from = Ctx().EnsureDiffVertex(g.a);
    auto to = Ctx().EnsureDiffVertex(g.c);
    ASSERT_TRUE(from.has_value() && to.has_value());
    ASSERT_TRUE(Ctx().CreateNativeEdge(*from, *to, type).has_value());
  });
  EXPECT_EQ(EdgeStubCount(), 2U);
}

///// Native edges on fork endpoints

// The stock CreateEdge refuses a deleted vertex by its deleted bit, which a stub does not carry: a fork vertex deleted
// earlier in the transaction must be refused by the resolver.
TEST_F(VersioningBranchEdgeWriteTest, ANativeEdgeCannotBeAttachedToAVertexDeletedInTheSameTransaction) {
  auto const g = MakeGraph();
  Fork();
  auto const type = storage_->NameToEdgeType("N");
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  auto stub_a = Ctx().EnsureDiffVertex(g.a);
  auto stub_b = Ctx().EnsureDiffVertex(g.b);
  ASSERT_TRUE(stub_a.has_value() && stub_b.has_value());
  auto native = Ctx().CreateNativeVertex();
  auto attached = Ctx().CreateNativeEdge(*stub_a, native, type);
  ASSERT_TRUE(attached.has_value()) << "an endpoint that is not deleted takes the edge";
  EXPECT_EQ(attached->storage_, &Diff());
  EXPECT_TRUE(Core().MayHaveEdgeChange(g.a));
  EXPECT_TRUE(Ctx().FindDiffEdge(attached->Gid(), g.a, ms::View::NEW).has_value());
  ASSERT_TRUE(acc->DeleteEdge(&*attached).has_value());

  ASSERT_TRUE(*Ctx().DeleteForkVertex(g.a));
  EXPECT_EQ(Ctx().CreateNativeEdge(*stub_a, native, type).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(Ctx().CreateNativeEdge(native, *stub_a, type).error(), ms::Error::DELETED_OBJECT);
  EXPECT_FALSE(Ctx().EnsureDiffVertex(g.a).has_value());
  EXPECT_FALSE(acc->TransactionHasSerializationError()) << "a refusal is not a conflict";
  EXPECT_TRUE(Ctx().CreateNativeEdge(*stub_b, native, type).has_value()) << "another vertex is unaffected";
  ASSERT_TRUE(Ctx().CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  Ctx().EndTransaction();
  Read([&] {
    auto edges = Ctx().ResolveEdges(g.b, ms::EdgeDirection::OUT, ms::View::NEW);
    ASSERT_EQ(edges.size(), 1u);
    EXPECT_EQ(edges[0].storage_, &Diff()) << "b's one visible edge is the native one (e1, e2 end at b)";
    EXPECT_TRUE(Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  });
}

// An edge between two fork vertices is native, hangs on their stubs and lives with the fork edges in one listing.
TEST_F(VersioningBranchEdgeWriteTest, ANativeEdgeBetweenForkVerticesListsBesideTheForkEdges) {
  auto const g = MakeGraph();
  Fork();
  auto const type = storage_->NameToEdgeType("N");
  ms::Gid native_gid;
  Write([&] {
    auto stub_a = Ctx().EnsureDiffVertex(g.a);
    auto stub_c = Ctx().EnsureDiffVertex(g.c);
    ASSERT_TRUE(stub_a.has_value() && stub_c.has_value());
    auto edge = Ctx().CreateNativeEdge(*stub_a, *stub_c, type);
    ASSERT_TRUE(edge.has_value());
    native_gid = edge->Gid();
  });
  EXPECT_FALSE(Ctx().IsForkCandidateEdge(native_gid));
  Read([&] {
    auto out = Ctx().ResolveEdges(g.a, ms::EdgeDirection::OUT, ms::View::NEW);
    EXPECT_EQ(out.size(), 3u) << "e1, e2 and the native edge";
    auto in = Ctx().ResolveEdges(g.c, ms::EdgeDirection::IN, ms::View::NEW);
    ASSERT_EQ(in.size(), 2u) << "the self-loop and the native edge";
    EXPECT_TRUE(Ctx().FindDiffEdge(native_gid, g.a, ms::View::NEW).has_value());
    EXPECT_TRUE(Ctx().ResolveEdge(native_gid, g.a, ms::View::NEW).has_value());
  });
}

// What the fork pin keeps alive: main's endpoint vertices of an edge stub, after main deleted them and collected.
TEST_F(VersioningBranchEdgeWriteTest, AForkEdgeMainDeletedAfterTheForkStaysWritableThroughItsStub) {
  auto const x = Prop("x");
  auto const g = MakeGraph();
  Fork();
  Write([&] {
    auto e1 = ForkEdge(g.e1);
    ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(2)).has_value());
  });
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = *acc->FindVertex(g.a, ms::View::NEW);
    auto b = *acc->FindVertex(g.b, ms::View::NEW);
    ASSERT_TRUE(acc->DetachDelete({&a, &b}, {}, true).has_value());
    Commit(*acc);
  }
  storage_->FreeMemory({}, false);
  storage_->FreeMemory({}, false);
  WriteAborted([&] {
    auto e1 = ForkEdge(g.e1);
    EXPECT_EQ(*Ctx().GetProperty(e1, x, ms::View::NEW), Int(2));
    ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(3)).has_value());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e2)));
  });
  Diff().FreeMemory({}, false);
  WriteCounted([&] {
    auto e1 = ForkEdge(g.e1);
    ASSERT_TRUE(Ctx().SetProperty(e1, x, Int(4)).has_value());
    ASSERT_TRUE(*Ctx().DeleteForkEdge(ForkEdge(g.e2)));
  });
  Diff().FreeMemory({}, false);
  Diff().FreeMemory({}, false);
  Read([&] {
    EXPECT_EQ(*Ctx().GetProperty(ForkEdge(g.e1), x, ms::View::NEW), Int(4));
    EXPECT_TRUE(Ctx().IsForkEdgeHidden(ForkEdge(g.e2), ms::View::NEW));
  });
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, 1}));
}

///// The hammer: hot fork edges under concurrent SET, REMOVE and DELETE

TEST_F(VersioningBranchEdgeWriteTest, HotForkEdgesUnderConcurrentSetRemoveAndDeleteStayConsistent) {
  constexpr int kEdges = 4;
  constexpr int kThreads = 8;
  constexpr int kOps = 300;
  auto const x = Prop("x"), y = Prop("y");
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  std::vector<MainEdge> edges;
  for (int i = 0; i < kEdges; ++i) edges.push_back(CreateMainEdge(a, b, "E", {{x, Int(-1)}, {y, Int(-2)}}));
  Fork();
  auto core = Ctx().core();
  std::array<std::atomic<int>, kEdges> deleted{};
  std::mutex mutex;
  std::array<std::set<int64_t>, kEdges> committed_values;

  std::vector<std::jthread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      mv::BranchContext ctx{core};
      std::mt19937 rng(static_cast<uint32_t>(t) + 17);
      for (int op = 0; op < kOps; ++op) {
        auto const which = static_cast<int>(rng() % kEdges);
        auto const kind = rng() % 4;
        auto acc = Diff().Access(ms::WRITE, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto edge = *ctx.FindForkEdge(edges[which].gid, a);
        int64_t const value = (static_cast<int64_t>(t + 1) << 32) | op;
        bool ok = false, deleted_it = false;
        if (kind == 0 || kind == 1) {
          ok = ctx.SetProperty(edge, kind == 0 ? x : y, Int(value)).has_value();
        } else if (kind == 2) {
          ok = ctx.SetProperty(edge, x, ms::PropertyValue{}).has_value();
        } else {
          auto const result = ctx.DeleteForkEdge(edge);
          ok = result.has_value();
          deleted_it = ok && *result;
        }
        bool const committed = ok && !acc->TransactionHasSerializationError() &&
                               ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value();
        ctx.EndTransaction();
        if (!committed) {
          acc->Abort();
          continue;
        }
        if (deleted_it) ++deleted[which];
        if (kind == 0 || kind == 1) {
          std::lock_guard lock{mutex};
          committed_values[which].insert(value);
        }
      }
    });
  }
  threads.clear();

  int total_deleted = 0;
  for (int i = 0; i < kEdges; ++i) {
    EXPECT_LE(deleted[i].load(), 1) << "an edge is deleted by one committed transaction";
    total_deleted += deleted[i].load();
  }
  EXPECT_EQ(Core().ForkDeletes(), (mv::ObjectCounts{0, static_cast<uint64_t>(total_deleted)}));
  EXPECT_LE(EdgeStubCount(), static_cast<size_t>(kEdges));
  Read([&] {
    for (int i = 0; i < kEdges; ++i) {
      auto edge = ForkEdge(edges[i]);
      EXPECT_EQ(Ctx().IsForkEdgeHidden(edge, ms::View::NEW), deleted[i].load() == 1);
      if (deleted[i].load() == 1) continue;
      auto const props = Ctx().Properties(edge, ms::View::NEW);
      ASSERT_TRUE(props.has_value());
      for (auto const &[key, value] : *props) {
        EXPECT_FALSE(ms::IsReservedId(key)) << "a removal marker leaked into the branch's properties";
        EXPECT_TRUE(value.IsInt());
        EXPECT_TRUE(value.ValueInt() < 0 || committed_values[i].contains(value.ValueInt()))
            << "a value no committed transaction wrote";
      }
      EXPECT_EQ(*Ctx().GetProperty(edge, x, ms::View::NEW), props->contains(x) ? props->at(x) : ms::PropertyValue{});
    }
  });
}

///// BRANCH COMMIT, ABORT AND GC: THE DIFF ENGINE'S OWN, OVER THE STUBS

// Branch transactions over the core fixture's tenant wiring. Every write is a stub write committed by the diff engine,
// so these check the stock commit and GC over stubs. Every test starts after a collect, with the unreleased-delta
// gauge at `baseline_`.
class VersioningBranchGcTest : public VersioningBranchCoreTest {
 protected:
  void SetUp() override {
    VersioningBranchCoreTest::SetUp();
    if (HasFatalFailure()) return;
    ctx_ = std::make_unique<mv::BranchContext>(core_);
    Collect();
    baseline_ = Deltas();
  }

  void TearDown() override {
    ctx_.reset();
    VersioningBranchCoreTest::TearDown();
  }

  int64_t Deltas() const { return static_cast<int64_t>(delta_gauge_.Value()); }

  // Unlinking and freeing can take separate passes.
  void Collect() {
    for (int i = 0; i < 3; ++i) Diff().FreeMemory({}, false);
  }

  static ms::EdgeAccessor ForkEdgeOf(mv::BranchContext &ctx, ms::Gid from_gid, ms::Gid edge_gid) {
    return *ctx.FindForkEdge(edge_gid, from_gid);
  }

  ms::EdgeAccessor ForkEdge(mv::BranchContext &ctx) { return ForkEdgeOf(ctx, v_gid_, edge_gid_); }

  // Sets x on the fork vertex and on the fork edge in `ctx`'s open transaction.
  void WriteBoth(mv::BranchContext &ctx, int64_t value) {
    auto v = *ctx.FindForkVertex(v_gid_);
    ASSERT_TRUE(ctx.SetProperty(v, prop_x_, ms::PropertyValue(value)).has_value());
    auto e = ForkEdge(ctx);
    ASSERT_TRUE(ctx.SetProperty(e, prop_x_, ms::PropertyValue(value)).has_value());
  }

  // One committed transaction through BranchContext::CommitTransaction.
  void CommitBoth(int64_t value) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    WriteBoth(*ctx_, value);
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }

  // x of the fork vertex and of the fork edge as `ctx`'s open transaction sees them at `view`.
  std::pair<int64_t, int64_t> Values(mv::BranchContext &ctx, ms::View view) {
    auto value = [&](const auto &object) {
      auto read = ctx.GetProperty(object, prop_x_, view);
      return !read.has_value() ? int64_t{-1} : read->IsNull() ? int64_t{0} : read->ValueInt();
    };
    return {value(*ctx.FindForkVertex(v_gid_)), value(ForkEdge(ctx))};
  }

  std::unique_ptr<mv::BranchContext> ctx_;
  int64_t baseline_{};
};

class VersioningBranchPeriodicGcTest : public VersioningBranchGcTest {
 protected:
  ms::Config MainConfig() const override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(20)};
    return config;
  }
};

// Another session's open transaction defeats fast-discard, so every committed branch update keeps its deltas until GC.
TEST_F(VersioningBranchGcTest, DeltasRetainedBehindAnOpenTransactionAreFreedByGc) {
  CommitBoth(1);
  constexpr int64_t kCommits = 1000;
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 2; i <= kCommits; ++i) CommitBoth(i);
    Collect();
    EXPECT_GE(Deltas() - baseline_, 2 * (kCommits - 1)) << "an older open transaction must pin the committed deltas";
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{kCommits, kCommits}));
  ctx_->EndTransaction();
}

TEST_F(VersioningBranchGcTest, RolledBackBigTransactionLeavesNoDeltasAfterGc) {
  constexpr int64_t kWrites = 1000;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    for (int64_t i = 0; i < kWrites; ++i) {
      acc->CreateVertex();
      WriteBoth(*ctx_, i);
    }
    EXPECT_GE(Deltas() - baseline_, 3 * kWrites)
        << "a native create and a write of the vertex stub and of the edge stub";
    acc->Abort();
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);

  auto acc = Diff().Access(ms::READ);
  size_t natives = 0;
  for ([[maybe_unused]] auto v : acc->Vertices(ms::View::NEW)) ++natives;
  EXPECT_EQ(natives, 0u);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{0, 1}));
  ctx_->EndTransaction();
}

// A stub outlives an aborted transaction and GC without being an entry of the diff engine: the abort undoes only its
// deltas, GC never retires the stub, and the branch still finds it and reads the fork state through it.
TEST_F(VersioningBranchGcTest, AbortedStubWritesThenGcKeepTheStubsOutsideTheDiffEngineAndTheForkReadable) {
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    WriteBoth(*ctx_, 7);
    acc->Abort();
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(acc->ApproximateVertexCount(), 0U) << "the stubs are not diff-engine vertices";
  EXPECT_EQ(acc->ApproximateEdgeCount(), 0U) << "nor diff-engine edges";
  auto stub = ctx_->FindStub(v_gid_, ms::View::NEW);
  ASSERT_TRUE(stub.has_value());
  EXPECT_TRUE(stub->GetProperty(prop_x_, ms::View::NEW)->IsNull()) << "the aborted write must be undone on the stub";
  EXPECT_NE(core_->edge_stub_index().Find(edge_gid_.AsUint()), nullptr);
  EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{0, 1}));
  EXPECT_TRUE(ctx_->HasLabel(*ctx_->FindForkVertex(v_gid_), label_, ms::View::NEW).value_or(false));
  ctx_->EndTransaction();
}

// A deleted fork edge stays hidden once GC has freed the deltas that recorded the delete, and refuses further writes.
TEST_F(VersioningBranchGcTest, ACollectedDeletedForkEdgeStaysHidden) {
  CommitBoth(7);
  CommitBoth(8);
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto deleted = ctx_->DeleteForkEdge(ForkEdge(*ctx_));
    ASSERT_TRUE(deleted.has_value());
    EXPECT_TRUE(*deleted);
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8) << "the committed deltas are gone";

  auto acc = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  auto fork_edge = ForkEdge(*ctx_);
  EXPECT_TRUE(ctx_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
  EXPECT_TRUE(ctx_->ResolveEdges(v_gid_, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_EQ(ctx_->SetProperty(fork_edge, prop_x_, ms::PropertyValue(1)).error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(core_->ForkDeletes(), (mv::ObjectCounts{0, 1}));
  acc->Abort();
  ctx_->EndTransaction();
}

// BL-0222: the GC horizon is one clock. A reader older than a write or a delete keeps reading its snapshot through the
// stub while GC runs, before and after its branch transaction opens, and the delta is collected only once it is gone.
TEST_F(VersioningBranchGcTest, GcKeepsWhatAnOlderSnapshotReadsThroughStubs) {
  CommitBoth(1);
  auto reader = Diff().Access(ms::READ);
  CommitBoth(2);
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(*ctx_->DeleteForkEdge(ForkEdge(*ctx_)));
    ASSERT_TRUE(*ctx_->DeleteForkVertex(a_gid_));
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_GT(Deltas() - baseline_, 0) << "the reader pins them";

  mv::BranchContext reading{core_};
  ASSERT_TRUE(reading.BeginTransaction(reader.get()).has_value());
  EXPECT_EQ(Values(reading, ms::View::OLD), (std::pair<int64_t, int64_t>{1, 1}));
  EXPECT_FALSE(reading.IsForkEdgeHidden(ForkEdge(reading), ms::View::NEW));
  EXPECT_FALSE(reading.IsVertexDeleted(a_gid_, ms::View::NEW));
  reading.EndTransaction();
  reader.reset();
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_TRUE(ctx_->IsForkEdgeHidden(ForkEdge(*ctx_), ms::View::NEW));
  EXPECT_TRUE(ctx_->IsVertexDeleted(a_gid_, ms::View::NEW));
  EXPECT_EQ(*ctx_->GetProperty(*ctx_->FindForkVertex(v_gid_), prop_x_, ms::View::NEW), ms::PropertyValue(2));
  ctx_->EndTransaction();
}

// A mixed transaction (stub writes + native creates, vertex and edge) commits or aborts all of it together.
TEST_F(VersioningBranchGcTest, AMixedTransactionCommitsAndAbortsStubsAndNativeObjectsTogether) {
  auto const type = storage_->NameToEdgeType("N");
  ms::Gid native, aborted_native;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    WriteBoth(*ctx_, 5);
    auto from = ctx_->CreateNativeVertex();
    aborted_native = from.Gid();
    auto stub = ctx_->EnsureDiffVertex(v_gid_);
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(ctx_->CreateNativeEdge(from, *stub, type).has_value());
    acc->Abort();
    ctx_->EndTransaction();
  }
  {
    auto acc = Diff().Access(ms::READ);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{0, 1})) << "an aborted stub write is gone";
    EXPECT_FALSE(ctx_->ResolveVertex(aborted_native, ms::View::NEW).has_value()) << "so is an aborted native create";
    EXPECT_EQ(ctx_->ResolveEdges(v_gid_, ms::EdgeDirection::IN, ms::View::NEW).size(), 0u);
    ctx_->EndTransaction();
  }
  ms::Gid native_edge;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    WriteBoth(*ctx_, 6);
    auto from = ctx_->CreateNativeVertex();
    native = from.Gid();
    auto stub = ctx_->EnsureDiffVertex(v_gid_);
    ASSERT_TRUE(stub.has_value());
    auto edge = ctx_->CreateNativeEdge(from, *stub, type);
    ASSERT_TRUE(edge.has_value());
    native_edge = edge->Gid();
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{6, 6}));
  EXPECT_TRUE(ctx_->ResolveVertex(native, ms::View::NEW).has_value());
  EXPECT_TRUE(ctx_->ResolveEdge(native_edge, native, ms::View::NEW).has_value());
  EXPECT_EQ(ctx_->ResolveEdges(v_gid_, ms::EdgeDirection::IN, ms::View::NEW).size(), 1u);
  ctx_->EndTransaction();
}

// Two branch transactions writing one fork vertex or fork edge conflict, the loser's diff transaction is poisoned like
// a storage serialization error, and a snapshot reader older than the winner's commit is unaffected.
TEST_F(VersioningBranchGcTest, ConcurrentStubWritersConflictAndReadersKeepTheirSnapshot) {
  mv::BranchContext other{core_};
  auto first = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(first.get()).has_value());
  auto second = Diff().Access(ms::WRITE);
  ASSERT_TRUE(other.BeginTransaction(second.get()).has_value());
  auto reader = Diff().Access(ms::READ);
  mv::BranchContext reading{core_};
  ASSERT_TRUE(reading.BeginTransaction(reader.get()).has_value());

  WriteBoth(*ctx_, 1);
  auto other_vertex = *other.FindForkVertex(v_gid_);
  auto const vertex_conflict = other.SetProperty(other_vertex, prop_x_, ms::PropertyValue(2));
  ASSERT_FALSE(vertex_conflict.has_value());
  EXPECT_EQ(vertex_conflict.error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(second->TransactionHasSerializationError());
  auto other_edge = ForkEdge(other);
  EXPECT_EQ(other.SetProperty(other_edge, prop_x_, ms::PropertyValue(2)).error(), ms::Error::SERIALIZATION_ERROR);

  ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_EQ(Values(reading, ms::View::NEW), (std::pair<int64_t, int64_t>{0, 1}))
      << "a snapshot older than the commit must not see it";
  other.EndTransaction();
  reading.EndTransaction();
  ctx_->EndTransaction();
}

// A fork edge deleted by a transaction in flight fails a DETACH DELETE of its endpoint, on the edge's stub.
TEST_F(VersioningBranchGcTest, DeleteForkVertexConflictsWithAnInFlightEdgeDelete) {
  mv::BranchContext other{core_};
  auto first = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(first.get()).has_value());
  auto second = Diff().Access(ms::WRITE);
  ASSERT_TRUE(other.BeginTransaction(second.get()).has_value());

  auto deleted = ctx_->DeleteForkEdge(ForkEdge(*ctx_));
  ASSERT_TRUE(deleted.has_value() && *deleted);
  EXPECT_FALSE(first->TransactionHasSerializationError());
  EXPECT_EQ(other.DeleteForkVertex(v_gid_).error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(second->TransactionHasSerializationError());

  ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  other.EndTransaction();
  ctx_->EndTransaction();
}

// A periodic commit commits each batch's stub and native writes and continues at a fresh snapshot; a reader older than
// every batch sees none of them while GC runs.
TEST_F(VersioningBranchGcTest, PeriodicCommitCommitsEachBatchAndGcKeepsPinnedSnapshots) {
  auto reader = Diff().Access(ms::READ);
  mv::BranchContext reading{core_};
  ASSERT_TRUE(reading.BeginTransaction(reader.get()).has_value());

  ms::Gid native;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    for (int64_t batch = 1; batch <= 3; ++batch) {
      WriteBoth(*ctx_, batch);
      if (batch == 2) native = ctx_->CreateNativeVertex().Gid();
      ASSERT_TRUE(ctx_->PeriodicCommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
      EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{batch, batch}));
    }
    WriteBoth(*ctx_, 4);
    acc->Abort();
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_GT(Deltas() - baseline_, 0) << "the reader's snapshot predates every batch";
  EXPECT_EQ(Values(reading, ms::View::NEW), (std::pair<int64_t, int64_t>{0, 1}));
  reading.EndTransaction();
  reader.reset();

  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);
  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(Values(*ctx_, ms::View::NEW), (std::pair<int64_t, int64_t>{3, 3})) << "the aborted batch is gone";
  EXPECT_TRUE(ctx_->ResolveVertex(native, ms::View::NEW).has_value()) << "the mixed batch's native create committed";
  ctx_->EndTransaction();
}

// A write to an object another transaction committed after this one started conflicts, a write to another object does
// not, and a transaction started after the commit writes freely; main gives the same outcomes.
TEST_F(VersioningBranchGcTest, CommittedConcurrentWritesConflictAsOnMain) {
  struct Outcome {
    bool other_object_ok;
    std::optional<ms::Error> same_vertex_error;
    std::optional<ms::Error> same_edge_error;
    bool later_txn_ok;
    bool operator==(const Outcome &) const = default;
  };

  auto const value = ms::PropertyValue(7);
  auto const error_of = [](auto &&result) {
    return result ? std::optional<ms::Error>{} : std::optional<ms::Error>{result.error()};
  };
  auto const main_edge = [&](ms::Storage::Accessor &acc) {
    return acc.FindVertex(v_gid_, ms::View::OLD)->OutEdges(ms::View::OLD)->edges[0];
  };

  Outcome on_main{};
  {
    auto t1 = storage_->Access(ms::WRITE);
    auto t2 = storage_->Access(ms::WRITE);
    auto edge1 = main_edge(*t1);
    ASSERT_TRUE(t1->FindVertex(v_gid_, ms::View::OLD)->SetProperty(prop_x_, value).has_value());
    ASSERT_TRUE(edge1.SetProperty(prop_x_, value).has_value());
    ASSERT_TRUE(t1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    on_main.other_object_ok = t2->FindVertex(a_gid_, ms::View::OLD)->SetProperty(prop_x_, value).has_value();
    on_main.same_vertex_error = error_of(t2->FindVertex(v_gid_, ms::View::OLD)->SetProperty(prop_x_, value));
    auto edge2 = main_edge(*t2);
    on_main.same_edge_error = error_of(edge2.SetProperty(prop_x_, value));
    t2->Abort();
    auto t3 = storage_->Access(ms::WRITE);
    auto edge3 = main_edge(*t3);
    on_main.later_txn_ok = t3->FindVertex(v_gid_, ms::View::OLD)->SetProperty(prop_x_, value).has_value() &&
                           edge3.SetProperty(prop_x_, value).has_value();
  }

  Outcome on_branch{};
  {
    mv::BranchContext other{core_};
    auto t1 = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(t1.get()).has_value());
    auto t2 = Diff().Access(ms::WRITE);
    ASSERT_TRUE(other.BeginTransaction(t2.get()).has_value());
    WriteBoth(*ctx_, 7);
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
    auto other_a = *other.FindForkVertex(a_gid_);
    on_branch.other_object_ok = other.SetProperty(other_a, prop_x_, value).has_value();
    auto other_v = *other.FindForkVertex(v_gid_);
    on_branch.same_vertex_error = error_of(other.SetProperty(other_v, prop_x_, value));
    auto other_e = ForkEdge(other);
    on_branch.same_edge_error = error_of(other.SetProperty(other_e, prop_x_, value));
    EXPECT_TRUE(t2->TransactionHasSerializationError());
    other.EndTransaction();
    t2->Abort();
    auto t3 = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(t3.get()).has_value());
    auto later_v = *ctx_->FindForkVertex(v_gid_);
    auto later_e = ForkEdge(*ctx_);
    on_branch.later_txn_ok = ctx_->SetProperty(later_v, prop_x_, value).has_value() &&
                             ctx_->SetProperty(later_e, prop_x_, value).has_value();
    ctx_->EndTransaction();
  }

  EXPECT_EQ(on_main, (Outcome{true, ms::Error::SERIALIZATION_ERROR, ms::Error::SERIALIZATION_ERROR, true}));
  EXPECT_EQ(on_branch, on_main);
}

// Two sessions write the fork vertex and the fork vertex's neighbour in every transaction (same keys: they conflict and
// retry), a third writes the fork edge only (never conflicts); readers hold snapshots across commits and GC. Every read
// of a reader transaction returns the same values, and both vertices always carry one transaction's value.
TEST_F(VersioningBranchGcTest, ConcurrentSessionsWriteAndReadersStayStableAcrossCommitsAndGc) {
  constexpr int kCommitsPerWriter = 600;
  std::atomic<int> writers_left{3};
  std::atomic<int> same_key_conflicts{0};
  std::atomic<int> snapshots{0};

  // Runs one transaction on `ctx`: true if it committed, false on a serialization error (aborted).
  auto run_txn = [&](mv::BranchContext &ctx, auto &&write) {
    if (!ctx.OpenHistorical()) {
      ADD_FAILURE() << "OpenHistorical failed";
      return false;
    }
    auto acc = Diff().Access(ms::WRITE);
    ctx.SetCurrentDiffTxn(acc.get());
    bool const wrote = write(ctx);
    bool const committed = wrote && ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value();
    ctx.EndTransaction();
    if (!committed) acc->Abort();
    return committed;
  };
  auto vertex_value = [&](mv::BranchContext &ctx, ms::Gid gid) {
    auto value = ctx.GetProperty(*ctx.FindForkVertex(gid), prop_x_, ms::View::OLD);
    return !value.has_value() ? -1 : value->IsNull() ? 0 : value->ValueInt();
  };
  auto edge_value = [&](mv::BranchContext &ctx) {
    auto value = ctx.GetProperty(ForkEdge(ctx), prop_x_, ms::View::OLD);
    return !value.has_value() ? -1 : value->IsNull() ? 0 : value->ValueInt();
  };

  std::vector<std::jthread> threads;
  for (int writer = 1; writer <= 2; ++writer) {
    threads.emplace_back([&, writer] {
      const memgraph::memory::DbArenaScope scope{pool_.get()};
      mv::BranchContext ctx{core_};
      for (int round = 1; round <= kCommitsPerWriter;) {
        auto const value = ms::PropertyValue((writer * 100000) + round);
        bool const committed = run_txn(ctx, [&](mv::BranchContext &c) {
          auto v = *c.FindForkVertex(v_gid_);
          auto a = *c.FindForkVertex(a_gid_);
          return c.SetProperty(v, prop_x_, value).has_value() && c.SetProperty(a, prop_x_, value).has_value();
        });
        if (committed) {
          ++round;
        } else {
          ++same_key_conflicts;
        }
      }
      --writers_left;
    });
  }
  threads.emplace_back([&] {
    const memgraph::memory::DbArenaScope scope{pool_.get()};
    mv::BranchContext ctx{core_};
    for (int round = 1; round <= kCommitsPerWriter; ++round) {
      EXPECT_TRUE(run_txn(ctx, [&](mv::BranchContext &c) {
        auto edge = ForkEdge(c);
        return c.SetProperty(edge, prop_x_, ms::PropertyValue(round)).has_value();
      })) << "a write to an object no one else writes never conflicts";
    }
    --writers_left;
  });
  threads.emplace_back([&] {
    const memgraph::memory::DbArenaScope scope{pool_.get()};
    while (writers_left.load() > 0) Diff().FreeMemory({}, false);
  });
  for (int reader = 0; reader < 2; ++reader) {
    threads.emplace_back([&] {
      const memgraph::memory::DbArenaScope scope{pool_.get()};
      mv::BranchContext ctx{core_};
      int64_t last_edge = 0;
      do {
        auto acc = Diff().Access(ms::READ);
        ASSERT_TRUE(ctx.BeginTransaction(acc.get()).has_value());
        auto const v = vertex_value(ctx, v_gid_);
        auto const e = edge_value(ctx);
        ASSERT_EQ(vertex_value(ctx, a_gid_), v) << "both vertices carry one transaction's value";
        ASSERT_GE(e, last_edge);
        last_edge = e;
        for (int i = 0; i < 4; ++i) {
          std::this_thread::yield();
          ASSERT_EQ(vertex_value(ctx, v_gid_), v);
          ASSERT_EQ(vertex_value(ctx, a_gid_), v);
          ASSERT_EQ(edge_value(ctx), e);
        }
        ctx.EndTransaction();
        ++snapshots;
      } while (writers_left.load() > 0);
    });
  }
  threads.clear();

  EXPECT_GT(snapshots.load(), 2);
  Collect();
  EXPECT_LE(Deltas() - baseline_, 8);
  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(vertex_value(*ctx_, v_gid_) % 100000, kCommitsPerWriter) << "the last committed writer finished its rounds";
  EXPECT_EQ(vertex_value(*ctx_, a_gid_), vertex_value(*ctx_, v_gid_));
  EXPECT_EQ(edge_value(*ctx_), kCommitsPerWriter);
  ctx_->EndTransaction();
}

TEST_F(VersioningBranchGcTest, BranchWritesAreChargedToMainsMemoryTracker) {
#if !USE_JEMALLOC
  GTEST_SKIP() << "per-DB arena tracking needs jemalloc";
#endif
  Collect();
  const auto before = db_tracker_.Amount();
  // A fresh thread starts outside any arena scope; entering the diff engine's pool means a null or foreign pool in
  // Build would leave main's tracker flat.
  std::thread writer([&] {
    const memgraph::memory::DbArenaScope scope{Diff().DbArenaPool()};
    auto acc = Diff().Access(ms::WRITE);
    for (int i = 0; i < 40000; ++i) {
      auto v = acc->CreateVertex();
      if (!v.SetProperty(prop_x_, ms::PropertyValue(std::string(64, 'a'))).has_value()) {
        ADD_FAILURE() << "SetProperty failed";
        break;
      }
    }
    EXPECT_GT(db_tracker_.Amount() - before, 1024 * 1024);
    acc->Abort();
  });
  writer.join();
}

TEST_F(VersioningBranchPeriodicGcTest, PeriodicGcThreadFreesDeltasWithoutExplicitCollect) {
  CommitBoth(1);
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 2; i <= 300; ++i) CommitBoth(i);
    EXPECT_GE(Deltas() - baseline_, 300);
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (Deltas() - baseline_ > 8 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_LE(Deltas() - baseline_, 8);
}

// A native edge hung on a stub is linked from the stub, which the diff engine does not own: with light edges that
// edge is a pool object, and the core must free it at teardown. A second identical round must not grow the process's
// allocations by what the first left behind.
#if USE_JEMALLOC
size_t AllocatedBytes() {
  uint64_t epoch = 1;
  size_t size = sizeof(epoch);
  EXPECT_EQ(je_mallctl("epoch", &epoch, &size, &epoch, size), 0);
  size_t allocated = 0;
  size = sizeof(allocated);
  EXPECT_EQ(je_mallctl("stats.allocated", &allocated, &size, nullptr, 0), 0);
  return allocated;
}

TEST_F(VersioningBranchCoreLightEdgeTest, TeardownFreesLightEdgesLinkedFromStubs) {
  core_.reset();
  constexpr int kEdges = 60'000;
  auto const round = [&] {
    auto built = mv::BranchCore::Build(*storage_, fork_ts_);
    ASSERT_TRUE(built.has_value());
    auto core = std::move(*built);
    auto &diff = core->diff_engine();
    auto acc = diff.Access(ms::WRITE);
    ms::VertexAccessor from{core->stub_index().GetOrCreateStub(v_gid_.AsUint(), nullptr), &diff, acc->GetTransaction()};
    ms::VertexAccessor to{core->stub_index().GetOrCreateStub(a_gid_.AsUint(), nullptr), &diff, acc->GetTransaction()};
    auto &replication = static_cast<ms::ReplicationAccessor &>(*acc);
    auto const type = core->NameToEdgeType("N");
    for (int i = 0; i < kEdges; ++i) {
      ASSERT_TRUE(replication.CreateEdgeEx(&from, &to, type, ms::Gid::FromUint(1'000'000 + i)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  round();
  auto const after_first = AllocatedBytes();
  round();
  EXPECT_LT(AllocatedBytes(), after_first + kEdges * sizeof(ms::Edge) / 2)
      << "the light edges linked from the stubs leaked";
}
#endif

}  // namespace
