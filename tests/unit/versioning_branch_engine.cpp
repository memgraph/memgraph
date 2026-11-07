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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <prometheus/gauge.h>

#include "memory/db_arena.hpp"
#include "metrics/metric_handles.hpp"
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
  memgraph::utils::MemoryTracker embedding_tracker_;
  std::unique_ptr<memgraph::memory::ArenaPool> pool_;
  std::unique_ptr<memgraph::memory::DbArenaScope> scope_;
  std::unique_ptr<ms::InMemoryStorage> storage_;
  std::shared_ptr<mv::BranchCore> core_;
  uint64_t fork_ts_{};
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
      *storage_, fork_ts_, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 2, .edge_gid_floor = 1});
  ASSERT_TRUE(recorded.has_value());
  EXPECT_EQ((*recorded)->vertex_gid_floor(), 2U);
  EXPECT_EQ((*recorded)->edge_gid_floor(), 1U);
}

// A stub keeps its fork gid, and the diff engine's index scans skip entries at or above the engine's own counter, so the
// counters start at the floors: a stub is then always below them, whether or not a native object exists yet.
TEST_F(VersioningBranchCoreTest, TheDiffEnginesCountersStartAtTheGidFloors) {
  EXPECT_EQ(Diff().vertex_id_.load(), core_->vertex_gid_floor());
  EXPECT_EQ(Diff().edge_id_.load(), core_->edge_gid_floor());
  auto recorded = mv::BranchCore::Build(
      *storage_, fork_ts_, mv::BranchCore::RecordedFloors{.vertex_gid_floor = 7, .edge_gid_floor = 5});
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
    {
      auto main_acc = storage_->Access(ms::READ);
      auto fork_edge = main_acc->FindEdge(edge.gid, ms::View::OLD);
      EXPECT_TRUE(fork_edge.has_value());
      std::unique_lock guard{fork_edge->edge_.ptr->lock};
      fork_edge->edge_.ptr->SetBranched(true);
    }
    ms::EdgeAccessor stub_acc{ms::EdgeRef{Core().edge_stub_index().GetOrCreate(edge.gid.AsUint())},
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
      Core().RecordEdgeChange(edge.from);
      Core().RecordEdgeChange(edge.to);
    }
    return stub::ApplyEdge(stub_acc, *before, after);
  }

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
  EXPECT_FALSE(ctx.HasVertexStub(a));
  EXPECT_FALSE(ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto stub_v = ctx.FindStub(v, ms::View::NEW);
  ASSERT_TRUE(stub_v.has_value());
  EXPECT_FALSE(ctx.FindStub(a, ms::View::NEW).has_value()) << "a fork vertex without a stub";
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

// A stub write is invisible to View::OLD of its own transaction until the command advances; stock, but the branch
// reads depend on it.
TEST_F(VersioningBranchEngineTest, AStubWriteIsInvisibleToOldUntilTheCommandAdvances) {
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  auto acc = Diff().Access(ms::WRITE);
  Begin(acc.get());
  ASSERT_FALSE(WriteVertexStub(*acc, a, [&](StubState &s) { s.SetProperty(x, Int(2)); }).has_value());
  auto fork = *Ctx().FindForkVertex(a);
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::NEW), Int(2));
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::OLD), Int(1));
  acc->AdvanceCommand();
  EXPECT_EQ(*Ctx().GetProperty(fork, x, ms::View::OLD), Int(2));
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

}  // namespace
