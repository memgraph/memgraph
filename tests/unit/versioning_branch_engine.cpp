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
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
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
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/overlay_mark_deleted.hpp"
#include "versioning/overlay_merge.hpp"
#include "versioning/overlay_state.hpp"

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
    fork_.vertex_gid_floor = storage_->vertex_id_.load(std::memory_order_acquire);
    fork_.edge_gid_floor = storage_->edge_id_.load(std::memory_order_acquire);
    auto built = mv::BranchCore::Build(*storage_, fork_, fork_ts_);
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
  mv::ForkPoint fork_;
  uint64_t fork_ts_{};
};

// Every setting the diff engine must override, set the opposite way on a light-edge main.
class VersioningBranchCoreConfigTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(20)};
    config.experimental_commit_lock_narrowing = true;
    config.salient.items.enable_label_index_auto_creation = true;
    config.salient.items.enable_edge_type_index_auto_creation = true;
    config.salient.items.enable_schema_info = true;
    config.salient.items.properties_on_edges = true;
    config.salient.items.storage_light_edge = true;
    return config;
  }
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

class VersioningBranchCoreNoEdgePropertiesTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    auto config = VersioningBranchCoreTest::MainConfig();
    config.salient.items.properties_on_edges = false;
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
    const auto in_unique_txn = [this](auto &&define) {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(define(*acc).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    };
    in_unique_txn([&](auto &acc) { return acc.CreateIndex(label); });
    in_unique_txn([&](auto &acc) { return acc.CreateIndex(label, {property}); });
    in_unique_txn([&](auto &acc) { return acc.CreateExistenceConstraint(label, property); });
    in_unique_txn([&](auto &acc) { return acc.CreateUniqueConstraint(label, {property}); });
    in_unique_txn([&](auto &acc) {
      return acc.CreateVectorIndex(ms::VectorIndexSpec{.index_name = "v",
                                                       .label_filter = {ms::VectorMatchMode::SINGLE, {label}},
                                                       .property = vector_property,
                                                       .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                                                       .dimension = 2,
                                                       .resize_coefficient = 2,
                                                       .capacity = 10,
                                                       .scalar_kind = unum::usearch::scalar_kind_t::f32_k});
    });
    in_unique_txn([&](auto &acc) { return acc.CreateTextIndex(ms::TextIndexSpec{"t", label, {property}}); });
  }
};

TEST(VersioningDiffEngineConfig, OverridesOnlyDurabilityAutoCreationAndPerCommitExtras) {
  ms::Config main;
  main.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(77)};
  main.experimental_commit_lock_narrowing = true;
  main.track_label_counts = true;
  main.salient.storage_mode = ms::StorageMode::IN_MEMORY_ANALYTICAL;
  main.salient.items.enable_label_index_auto_creation = true;
  main.salient.items.enable_edge_type_index_auto_creation = true;
  main.salient.items.enable_schema_info = true;
  main.salient.items.enable_schema_metadata = true;
  main.salient.items.properties_on_edges = false;
  main.salient.items.storage_light_edge = true;
  main.durability.storage_directory = "/main/dir";
  main.durability.root_data_directory = "/main/root";
  main.durability.recover_on_startup = true;
  main.durability.snapshot_on_exit = true;
  main.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;

  const auto cfg = mv::DiffEngineConfig(main, ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  EXPECT_EQ(cfg.salient.storage_mode, ms::StorageMode::IN_MEMORY_TRANSACTIONAL) << "the live mode, not the startup one";
  EXPECT_NE(cfg.salient.uuid, main.salient.uuid);
  EXPECT_NE(mv::DiffEngineConfig(main, ms::StorageMode::IN_MEMORY_TRANSACTIONAL).salient.uuid, cfg.salient.uuid);
  EXPECT_FALSE(cfg.salient.items.enable_label_index_auto_creation);
  EXPECT_FALSE(cfg.salient.items.enable_edge_type_index_auto_creation);
  EXPECT_FALSE(cfg.salient.items.enable_schema_info);
  EXPECT_TRUE(cfg.salient.items.properties_on_edges);
  EXPECT_FALSE(cfg.salient.items.storage_light_edge);
  EXPECT_FALSE(cfg.experimental_commit_lock_narrowing);
  EXPECT_FALSE(cfg.track_label_counts);
  EXPECT_EQ(cfg.durability.snapshot_wal_mode, ms::Config::Durability::SnapshotWalMode::DISABLED);
  EXPECT_FALSE(cfg.durability.recover_on_startup);
  EXPECT_FALSE(cfg.durability.snapshot_on_exit);
  EXPECT_TRUE(cfg.durability.storage_directory.string().starts_with("/main/dir/versioning_diff_engine/"));
  EXPECT_EQ(cfg.durability.root_data_directory, cfg.durability.storage_directory);
  EXPECT_TRUE(cfg.gc == main.gc) << "the diff engine needs main's GC settings";
  EXPECT_TRUE(cfg.salient.items.enable_schema_metadata) << "inherited from main, not forced off";
}

// What only a live engine shows: the arena, the gauge, and settings that take effect at construction.
TEST_F(VersioningBranchCoreConfigTest, DiffEngineSharesMainsArenaAndGaugeAndDefaultsEverythingElseOff) {
  EXPECT_EQ(Diff().DbArenaPool(), pool_.get());
  EXPECT_EQ(Diff().metric_handles_.unreleased_delta_objects.gauge, &delta_gauge_);
  EXPECT_TRUE(storage_->config_.salient.items.storage_light_edge);
  EXPECT_FALSE(Diff().config_.salient.items.storage_light_edge) << "heavy while main is light";
  EXPECT_FALSE(Diff().Access(ms::WRITE)->GetTransaction()->commit_lock_narrowing);
  EXPECT_FALSE(static_cast<ms::Storage &>(Diff()).GetInfo().durability_snapshot_enabled);
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

TEST_F(VersioningBranchCoreSwitchedModeTest, DiffEngineRunsInMainsCurrentStorageModeNotItsStartupConfig) {
  ASSERT_EQ(storage_->GetStorageMode(), ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_EQ(storage_->config_.salient.storage_mode, ms::StorageMode::IN_MEMORY_ANALYTICAL);
  EXPECT_EQ(Diff().GetStorageMode(), ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  auto acc = Diff().Access(ms::WRITE);
  EXPECT_EQ(acc->GetTransaction()->storage_mode, ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
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

TEST_F(VersioningBranchCoreNoEdgePropertiesTest, DiffEngineForcesPropertiesOnEdgesWhenMainHasNone) {
  ASSERT_FALSE(storage_->config_.salient.items.properties_on_edges);
  EXPECT_TRUE(Diff().config_.salient.items.properties_on_edges);
  EXPECT_FALSE(core_->main().config_.salient.items.properties_on_edges)
      << "EdgeRef interpretation of main's edges must read main's config";

  auto acc = Diff().Access(ms::WRITE);
  auto *from = core_->overlay_index().GetOrCreateOverlay(ms::Gid::FromUint(0), nullptr, {});
  auto *to = core_->overlay_index().GetOrCreateOverlay(ms::Gid::FromUint(1), nullptr, {});
  auto *edge = core_->edge_overlay_index().GetOrCreate(ms::Gid::FromUint(0), ms::EdgeRef{ms::Gid::FromUint(0)});
  auto accessor = ms::EdgeAccessor::Create(
      ms::EdgeRef{edge}, core_->NameToEdgeType("E"), from, to, &Diff(), acc->GetTransaction(), ms::View::NEW);
  ASSERT_TRUE(accessor.has_value());
  const auto set = accessor->SetProperty(core_->NameToProperty("p"), ms::PropertyValue{1});
  EXPECT_TRUE(set.has_value()) << "an edge overlay must accept a property even when main has none";
  acc->Abort();
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

// An overlay keeps its base gid, and the diff engine's index scans skip entries at or above the engine's own counter,
// so the counters start at the fork's floors, which also bound what the engine's GC never retires.
TEST_F(VersioningBranchCoreTest, GidFloorsComeFromTheForkPointAndSeedTheDiffEngine) {
  EXPECT_EQ(core_->fork_ts(), fork_ts_);
  EXPECT_EQ(&core_->main(), storage_.get());
  EXPECT_EQ(core_->vertex_gid_floor(), 2U);
  EXPECT_EQ(core_->edge_gid_floor(), 1U);
  EXPECT_EQ(Diff().vertex_id_.load(), core_->vertex_gid_floor());
  EXPECT_EQ(Diff().edge_id_.load(), core_->edge_gid_floor());
  EXPECT_EQ(Diff().retained_gid_floors().vertex, 2U);
  EXPECT_EQ(Diff().retained_gid_floors().edge, 1U);
  {
    auto acc = storage_->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(core_->vertex_gid_floor(), 2U) << "main's later creates must not move the floor";

  auto other = mv::BranchCore::Build(*storage_, {.vertex_gid_floor = 7, .edge_gid_floor = 5}, fork_ts_);
  ASSERT_TRUE(other.has_value());
  EXPECT_EQ((*other)->vertex_gid_floor(), 7U);
  EXPECT_EQ((*other)->edge_gid_floor(), 5U);
  EXPECT_EQ((*other)->diff_engine().vertex_id_.load(), 7U);
  EXPECT_EQ((*other)->diff_engine().edge_id_.load(), 5U);
}

TEST_F(VersioningBranchCoreDurableMainTest, DiffEngineWritesNothingToDiskWhenMainIsDurable) {
  ASSERT_EQ(storage_->config_.durability.snapshot_wal_mode,
            ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL);
  {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(storage_->NameToLabel("Durable")).has_value());
    ASSERT_TRUE(v.SetProperty(storage_->NameToProperty("p"), ms::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  core_.reset();
  EXPECT_FALSE(std::filesystem::exists(main_dir_ / "versioning_diff_engine"));
}

// The filters themselves are covered by versioning_branch_change_filter; this checks the core reaches them.
TEST_F(VersioningBranchCoreTest, ChangeFilterForwardersReachTheCoresFilters) {
  const auto pid = storage_->NameToProperty("p");
  const auto label = storage_->NameToLabel("L");
  const auto gid = ms::Gid::FromUint(0);

  EXPECT_FALSE(core_->MayHaveLabelChange(gid));
  EXPECT_FALSE(core_->MayHaveLabelTouched(label));
  EXPECT_FALSE(core_->MayHavePropertyTouched(pid));
  EXPECT_FALSE(core_->MayHavePropertyFieldChange(gid, pid));
  EXPECT_FALSE(core_->MayHaveVertexDeleted(gid));
  EXPECT_FALSE(core_->MayHaveAnyVertexDeleted());
  EXPECT_FALSE(core_->MayHaveBaseEdgePropertyChange(gid));
  EXPECT_FALSE(core_->MayHaveBaseEdgePropertyFieldChange(gid, pid));
  EXPECT_FALSE(core_->MayHaveBaseEdgeDeleted(gid));

  core_->RecordLabelChange(gid);
  core_->RecordPropertyChange(gid);
  core_->RecordEdgeChange(gid, ms::EdgeDirection::OUT);
  EXPECT_TRUE(core_->MayHaveLabelChange(gid));
  EXPECT_TRUE(core_->MayHavePropertyChange(gid));
  EXPECT_TRUE(core_->MayHaveEdgeChange(gid, ms::EdgeDirection::OUT));
  EXPECT_FALSE(core_->MayHaveEdgeChange(gid, ms::EdgeDirection::IN)) << "an OUT record is no IN record";

  core_->RecordLabelTouched(label);
  core_->RecordPropertyFieldChange(gid, pid);
  core_->RecordVertexDeleted(gid);
  core_->RecordBaseEdgeProperty(gid, pid);
  core_->RecordBaseEdgeDeleted(gid);
  EXPECT_TRUE(core_->MayHaveLabelTouched(label));
  EXPECT_TRUE(core_->MayHavePropertyTouched(pid));
  EXPECT_TRUE(core_->MayHavePropertyFieldChange(gid, pid));
  EXPECT_TRUE(core_->MayHaveVertexDeleted(gid));
  EXPECT_TRUE(core_->MayHaveAnyVertexDeleted());
  EXPECT_TRUE(core_->MayHaveBaseEdgePropertyChange(gid));
  EXPECT_TRUE(core_->MayHaveBaseEdgePropertyFieldChange(gid, pid));
  EXPECT_TRUE(core_->MayHaveBaseEdgeDeleted(gid));
}

TEST_F(VersioningBranchCoreTest, BuildRejectsAnUnpinnedForkTimestamp) {
  const uint64_t never_pinned_ts = fork_ts_ + 1000;
  auto result = mv::BranchCore::Build(*storage_, fork_, never_pinned_ts);
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message.find(std::to_string(never_pinned_ts)), std::string::npos) << result.error().message;
  EXPECT_EQ(storage_->OldestForkPin(), std::optional<uint64_t>{fork_ts_}) << "a failed build must leak no pin";
}

// Commits through vertex and edge overlays, so their deltas stay linked into the overlays for teardown to free.
void WriteThroughOverlays(mv::BranchCore &core, ms::InMemoryStorage &diff) {
  auto acc = diff.Access(ms::WRITE);
  const auto label = core.NameToLabel("L");
  const auto pid = core.NameToProperty("p");
  auto *vertex_overlay = core.overlay_index().GetOrCreateOverlay(ms::Gid::FromUint(0), nullptr, {});
  auto *other_overlay = core.overlay_index().GetOrCreateOverlay(ms::Gid::FromUint(1), nullptr, {});
  ms::VertexAccessor va{vertex_overlay, &diff, acc->GetTransaction()};
  ASSERT_TRUE(va.AddLabel(label).has_value());
  ASSERT_TRUE(va.SetProperty(pid, ms::PropertyValue{1}).has_value());
  auto accessor = ms::EdgeAccessor::Create(
      ms::EdgeRef{core.edge_overlay_index().GetOrCreate(ms::Gid::FromUint(0), ms::EdgeRef{ms::Gid::FromUint(0)})},
      core.NameToEdgeType("E"),
      vertex_overlay,
      other_overlay,
      &diff,
      acc->GetTransaction(),
      ms::View::NEW);
  ASSERT_TRUE(accessor.has_value());
  ASSERT_TRUE(accessor->SetProperty(pid, ms::PropertyValue{2}).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// {cross_thread, abort_mid_txn}. These assert no crash: a use-after-free or leak in the destruction order shows only
// under ASan.
class VersioningBranchCoreTeardownTest : public VersioningBranchCoreTest,
                                         public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(VersioningBranchCoreTeardownTest, FreesOverlaysWhoseDeltasAreStillLinked) {
  const auto [cross_thread, abort_mid_txn] = GetParam();
  WriteThroughOverlays(*core_, Diff());
  if (abort_mid_txn) {
    // Aborts mid-way, leaving its deltas linked to the overlays at teardown time.
    auto acc = Diff().Access(ms::WRITE);
    ms::VertexAccessor va{core_->overlay_index().Find(ms::Gid::FromUint(0)).overlay, &Diff(), acc->GetTransaction()};
    ASSERT_TRUE(va.SetProperty(core_->NameToProperty("q"), ms::PropertyValue{3}).has_value());
    acc->Abort();
  }
  if (cross_thread) {
    std::thread{[core = std::move(core_)]() mutable { core.reset(); }}.join();
  } else {
    core_.reset();
  }
}

INSTANTIATE_TEST_SUITE_P(Variants, VersioningBranchCoreTeardownTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

///// BRANCH READS: THE OVERLAYS MERGED OVER THE BASE STATE

namespace overlay = mv::overlay;

// One overlay write as a test describes it; the fixture applies it to the overlays. Labels come
// first, then properties, then the delete.
struct OverlayEdit {
  std::vector<std::pair<ms::PropertyId, ms::PropertyValue>> properties;  // a Null value is a removal
  std::vector<ms::LabelId> added_labels;
  std::vector<ms::LabelId> removed_labels;
  bool deleted{false};

  void SetProperty(ms::PropertyId pid, ms::PropertyValue value) { properties.emplace_back(pid, std::move(value)); }

  void RemoveProperty(ms::PropertyId pid) { properties.emplace_back(pid, ms::PropertyValue{}); }

  void AddLabel(ms::LabelId label) { added_labels.push_back(label); }

  void RemoveLabel(ms::LabelId label) { removed_labels.push_back(label); }

  void MarkDeleted() { deleted = true; }
};

ms::PropertyValue Int(int64_t v) { return ms::PropertyValue{v}; }

// Base vertices' x on the branch at `view`; native vertices under their gid too.
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

  // Base edges main deleted after the fork are found at their base state, by id and in the adjacency, whether or not
  // the branch changed them; so are the base edges of a main without edge objects.
  void BaseEdgesMainDeletedSinceAreFound() {
    auto const x = Prop("x");
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const changed = CreateMainEdge(a, b, "E", {{x, Int(1)}});
    auto const untouched = CreateMainEdge(a, b, "E", {{x, Int(5)}});
    auto const kept = CreateMainEdge(a, b, "E", {{x, Int(6)}});
    Fork();
    if (PropertiesOnEdges()) SeedEdge(changed, [&](OverlayEdit &s) { s.SetProperty(x, Int(10)); });
    MainDeleteEdge(changed);
    MainDeleteEdge(untouched);

    auto diff_acc = Diff().Access(ms::READ);
    Begin(diff_acc.get());
    auto &ctx = Ctx();
    for (auto const view : {ms::View::OLD, ms::View::NEW}) {
      for (auto const &edge : {changed, untouched, kept}) {
        auto const found = ctx.FindBaseEdge(edge.gid, a);
        ASSERT_TRUE(found.has_value()) << edge.gid.AsUint();
        EXPECT_EQ(found->Gid(), edge.gid);
        EXPECT_EQ(found->EdgeType(), edge.type);
        auto const resolved = ctx.ResolveEdge(edge.gid, a, view);
        ASSERT_TRUE(resolved.has_value()) << edge.gid.AsUint();
        EXPECT_EQ(resolved->storage_, storage_.get());
      }
      if (PropertiesOnEdges()) {
        EXPECT_EQ(*ctx.GetProperty(*ctx.FindBaseEdge(changed.gid, a), x, view), Int(10))
            << "the overlay merges over it";
        EXPECT_EQ(*ctx.GetProperty(*ctx.FindBaseEdge(untouched.gid, a), x, view), Int(5)) << "the base's properties";
      }
      for (auto const direction : {ms::EdgeDirection::OUT, ms::EdgeDirection::IN}) {
        std::set<uint64_t> gids;
        for (auto const &edge : ctx.ResolveEdges(direction == ms::EdgeDirection::OUT ? a : b, direction, view)) {
          gids.insert(edge.Gid().AsUint());
        }
        EXPECT_EQ(gids, (std::set<uint64_t>{changed.gid.AsUint(), untouched.gid.AsUint(), kept.gid.AsUint()}));
      }
    }
    EXPECT_FALSE(ctx.FindBaseEdge(changed.gid, b).has_value()) << "B has no such out-edge";
  }

  // The overlays' delete candidates decide which base edges an expansion drops: by direction, by view, for a deleted
  // edge, a deleted endpoint and a self-loop, and never as an edge of their own.
  void DeletedCandidatesHideBaseEdgesFromTheAdjacency() {
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const c = CreateMainVertex({}, {});
    auto const e1 = CreateMainEdge(a, b, "E");
    auto const e2 = CreateMainEdge(a, b, "E");
    auto const e3 = CreateMainEdge(a, c, "F");
    auto const loop = CreateMainEdge(a, a, "E");
    Fork();
    auto const deleted = [](OverlayEdit &s) { s.MarkDeleted(); };
    SeedEdge(e2, deleted);
    SeedEdge(loop, deleted);

    auto const expand = [&](mv::BranchContext &ctx, ms::Gid vertex, ms::EdgeDirection direction, ms::View view) {
      std::set<uint64_t> gids;
      for (auto const &edge : ctx.ResolveEdges(vertex, direction, view)) {
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
      ASSERT_FALSE(WriteEdgeOverlay(*writer, e1, deleted).has_value());
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
    EXPECT_EQ(expand(Ctx(), a, kOut, kNew), gids({e1, e3})) << "the abort un-hid the edge";
    EXPECT_EQ(expand(Ctx(), b, kIn, kNew), gids({e1}));
    Ctx().EndTransaction();

    // A native edge is read beside the candidates, and a lookup of it passes over them.
    ms::Gid native_gid;
    {
      auto writer = Diff().Access(ms::WRITE);
      Begin(writer.get());
      ms::VertexAccessor overlay_a{OverlayFor(a), &Diff(), writer->GetTransaction()};
      auto native = CreateNative(writer.get());
      auto const edge = CreateNativeEdge(writer.get(), &overlay_a, &native);
      native_gid = edge.Gid();
      EXPECT_EQ(expand(Ctx(), a, kOut, kNew),
                (std::set<uint64_t>{e1.gid.AsUint(), e3.gid.AsUint(), native_gid.AsUint()}));
      EXPECT_TRUE(Ctx().FindDiffEdge(native_gid, a, kNew).has_value());
      EXPECT_FALSE(Ctx().FindDiffEdge(e2.gid, a, kNew).has_value()) << "a deleted base edge is no diff edge";
      EXPECT_FALSE(Ctx().FindDiffEdge(e2.gid, kNew).has_value());
      Commit(*writer);
      Ctx().EndTransaction();
    }
  }

  // A hub: more deleted edges than the linear search, across two edge types that each keep some edges.
  void ManyDeletedCandidatesHideTheirEdgesOnAHub() {
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
        SeedEdge(edges[i], [](OverlayEdit &s) { s.MarkDeleted(); });
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

  // What the write path does before its first overlay write of a base vertex.
  static void MarkBranched(ms::Vertex *vertex) {
    std::unique_lock guard{vertex->lock};
    vertex->SetBranched();
  }

  // The overlay of base vertex `gid`, created as the write path does: main's vertex marked branched, the overlay
  // holding the base's labels at fork_ts.
  ms::Vertex *OverlayFor(ms::Gid gid) {
    if (auto *existing = Core().overlay_index().Find(gid).overlay) return existing;
    auto *base_vertex = MainVertex(gid);
    MarkBranched(base_vertex);
    auto historical = storage_->HistoricalAccess(*fork_ts_);
    EXPECT_TRUE(historical.has_value());
    auto const labels = (*historical)->FindVertex(gid, ms::View::OLD)->Labels(ms::View::OLD);
    EXPECT_TRUE(labels.has_value());
    return Core().overlay_index().GetOrCreateOverlay(
        gid, base_vertex, std::span<const ms::LabelId>{labels->data(), labels->size()});
  }

  // Applies `edit` to the overlay of base vertex `gid` in the diff transaction `acc`: record the change filters, then
  // stock writes. Returns the first write's error.
  template <typename Edit>
  std::optional<ms::Error> WriteVertexOverlay(ms::Storage::Accessor &acc, ms::Gid gid, Edit &&edit) {
    OverlayEdit e;
    edit(e);
    auto *overlay_vertex = OverlayFor(gid);
    auto *transaction = acc.GetTransaction();
    ms::VertexAccessor overlay_acc{overlay_vertex, &Diff(), transaction};
    if (record_attribute_changes_) {
      Core().RecordLabelChange(gid);
      Core().RecordPropertyChange(gid);
      for (auto const label : e.added_labels) Core().RecordLabelTouched(label);
      for (auto const label : e.removed_labels) Core().RecordLabelTouched(label);
      for (auto const &[pid, value] : e.properties) Core().RecordPropertyFieldChange(gid, pid);
    }
    for (auto const label : e.added_labels) {
      if (auto added = overlay_acc.AddLabel(label); !added) return added.error();
    }
    for (auto const label : e.removed_labels) {
      if (auto removed = overlay_acc.RemoveLabel(label); !removed) return removed.error();
    }
    for (auto const &[pid, value] : e.properties) {
      if (auto written = overlay::WriteProperty(transaction, Diff(), overlay_vertex, pid, value); !written) {
        return written.error();
      }
    }
    if (e.deleted) {
      Core().RecordVertexDeleted(gid);
      // A vertex delete hides each of the vertex's visible edges before its own delete mark, as the write path does.
      for (auto const &edge : VisibleBaseEdgesOf(gid)) {
        if (auto error = HideEdge(acc, edge)) return error;
      }
      if (auto deleted = acc.DeleteVertex(&overlay_acc); !deleted) return deleted.error();
    }
    return std::nullopt;
  }

  template <typename Edit>
  void SeedVertex(ms::Gid gid, Edit &&edit) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_FALSE(WriteVertexOverlay(*acc, gid, edit).has_value());
    Commit(*acc);
  }

  template <typename Edit>
  std::optional<ms::Error> WriteEdgeOverlay(ms::Storage::Accessor &acc, const MainEdge &edge, Edit &&edit) {
    OverlayEdit e;
    edit(e);
    auto *transaction = acc.GetTransaction();
    auto *edge_overlay = Core().edge_overlay_index().GetOrCreate(edge.gid, MarkEdgeBranched(edge));
    for (auto const &[pid, value] : e.properties) {
      // Only a property write gives the endpoints overlays; a delete lists the edge and nothing more.
      auto *from_overlay = OverlayFor(edge.from);
      auto *to_overlay = OverlayFor(edge.to);
      Core().RecordBaseEdgeProperty(edge.gid, pid);
      if (auto written = overlay::WriteProperty(
              transaction, Diff(), edge_overlay, from_overlay, to_overlay, edge.type, pid, value);
          !written) {
        return written.error();
      }
    }
    if (e.deleted) {
      Core().RecordBaseEdgeDeleted(edge.gid);
      Core().RecordEdgeChange(edge.from, ms::EdgeDirection::OUT);
      Core().RecordEdgeChange(edge.to, ms::EdgeDirection::IN);
      auto const deleted = DeleteBaseEdgeOverlay(transaction, edge, edge_overlay);
      if (!deleted) return deleted.error();
    }
    return std::nullopt;
  }

  // Marks main's `edge` branched, as the write path does before it writes an overlay, and returns it as main holds it.
  ms::EdgeRef MarkEdgeBranched(const MainEdge &edge) {
    auto main_acc = storage_->Access(ms::READ);
    auto from = main_acc->FindVertex(edge.from, ms::View::OLD);
    EXPECT_TRUE(from.has_value());
    auto out = from->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    std::optional<ms::EdgeRef> base_edge;
    for (auto const &candidate : out->edges) {
      if (candidate.Gid() == edge.gid) base_edge = candidate.edge_;
    }
    EXPECT_TRUE(base_edge.has_value());
    // Without properties_on_edges the base edge is a bare gid, and there is no object to mark.
    if (PropertiesOnEdges()) {
      std::unique_lock guard{base_edge->ptr->lock};
      base_edge->ptr->SetBranched();
    }
    return *base_edge;
  }

  // Lists the edge overlay as a delete candidate of both endpoints, then marks it deleted (the F1 order). Already
  // deleted by an earlier vertex delete of the same transaction: false.
  ms::Result<bool> DeleteBaseEdgeOverlay(ms::Transaction *transaction, const MainEdge &edge, ms::Edge *edge_overlay) {
    MarkBranched(MainVertex(edge.from));
    MarkBranched(MainVertex(edge.to));
    mv::ListBaseEdgeDelete(Core().overlay_index(), edge.from, edge.to, edge_overlay);
    return overlay::MarkEdgeDeleted(transaction, edge_overlay);
  }

  // Hides base edge `edge` as the write path does for an edge of a deleted vertex.
  std::optional<ms::Error> HideEdge(ms::Storage::Accessor &acc, const MainEdge &edge) {
    Core().RecordEdgeChange(edge.from, ms::EdgeDirection::OUT);
    Core().RecordEdgeChange(edge.to, ms::EdgeDirection::IN);
    auto *edge_overlay = Core().edge_overlay_index().GetOrCreate(edge.gid, MarkEdgeBranched(edge));
    if (auto hidden = DeleteBaseEdgeOverlay(acc.GetTransaction(), edge, edge_overlay); !hidden) {
      return hidden.error();
    }
    return std::nullopt;
  }

  // The edges of base vertex `gid` a delete of it hides: its base-state edges not yet hidden (a self-loop once).
  std::vector<MainEdge> VisibleBaseEdgesOf(ms::Gid gid) {
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

  // The base edges a vertex delete of the fixture has hidden, so that none is hidden twice.
  std::set<uint64_t> hidden_edges_;

  // False: a vertex write records only its delete in the change filters, not its label or property changes.
  bool record_attribute_changes_{true};

  template <typename Edit>
  void SeedEdge(const MainEdge &edge, Edit &&edit) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_FALSE(WriteEdgeOverlay(*acc, edge, edit).has_value());
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

  bool HasOverlay(ms::Gid gid) { return Core().overlay_index().Find(gid).overlay != nullptr; }

  // An overlay or a deleted-edge entry: the branch touched the vertex.
  bool HasEntry(ms::Gid gid) { return Core().overlay_index().HasEntry(gid); }

  mv::CandidateCopy Candidates(ms::Gid gid, ms::EdgeDirection direction) {
    return Core().overlay_index().FindForExpansion(gid, direction, true).candidates;
  }

  // The overlay of base vertex `gid` as an accessor of the open diff transaction.
  std::optional<ms::VertexAccessor> OverlayOf(ms::Gid gid, ms::View view) {
    auto *overlay = Core().overlay_index().Find(gid).overlay;
    if (overlay == nullptr) return std::nullopt;
    return ms::VertexAccessor::Create(overlay, &Diff(), Ctx().current_diff_txn()->GetTransaction(), view);
  }

  void BaseEdgeReadsMergeTheEdgeOverlayOverTheBaseEdge() {
    auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
    auto const v = CreateMainVertex({}, {});
    auto const w = CreateMainVertex({}, {});
    auto const e1 = CreateMainEdge(v, w, "E", {{x, Int(1)}, {y, Int(2)}});
    auto const e2 = CreateMainEdge(v, w, "E", {{x, Int(5)}});
    auto const e3 = CreateMainEdge(v, w, "E", {{x, Int(6)}});
    Fork();
    SeedEdge(e1, [&](OverlayEdit &s) {
      s.SetProperty(x, Int(10));
      s.RemoveProperty(y);
      s.SetProperty(z, Int(3));
    });
    SeedEdge(e2, [](OverlayEdit &s) { s.MarkDeleted(); });

    auto diff_acc = Diff().Access(ms::READ);
    Begin(diff_acc.get());
    auto &ctx = Ctx();
    auto fe1 = ctx.FindBaseEdge(e1.gid, v);
    auto fe2 = ctx.FindBaseEdge(e2.gid, v);
    auto fe3 = ctx.FindBaseEdge(e3.gid, v);
    ASSERT_TRUE(fe1 && fe2 && fe3);
    for (auto const view : {ms::View::OLD, ms::View::NEW}) {
      EXPECT_EQ(*ctx.Properties(*fe1, view), (std::map<ms::PropertyId, ms::PropertyValue>{{x, Int(10)}, {z, Int(3)}}));
      EXPECT_EQ(*ctx.GetProperty(*fe1, x, view), Int(10));
      EXPECT_TRUE(ctx.GetProperty(*fe1, y, view)->IsNull());
      EXPECT_GT(*ctx.GetPropertySize(*fe1, x, view), 0U);
      EXPECT_EQ(*ctx.GetPropertySize(*fe1, y, view), 0U);

      EXPECT_TRUE(ctx.IsBaseEdgeHidden(*fe2, view));
      EXPECT_EQ(ctx.Properties(*fe2, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.GetProperty(*fe2, x, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.GetPropertySize(*fe2, x, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_FALSE(ctx.ResolveEdge(e2.gid, v, view).has_value());

      EXPECT_FALSE(ctx.IsBaseEdgeHidden(*fe3, view));
      EXPECT_EQ(*ctx.GetProperty(*fe3, x, view), Int(6)) << "an edge the branch never wrote reads as the base's";
      auto resolved = ctx.ResolveEdge(e1.gid, v, view);
      ASSERT_TRUE(resolved.has_value());
      EXPECT_EQ(resolved->storage_, storage_.get()) << "a base edge is returned as main's, never as its overlay";
    }
  }

  void ResolveAndUnionScanMergeOverlaysAndIncludeNatives() {
    auto const x = Prop("x");
    auto const a = CreateMainVertex({}, {{x, Int(1)}});
    auto const b = CreateMainVertex({}, {{x, Int(9)}});
    Fork();
    SeedVertex(a, [&](OverlayEdit &s) { s.SetProperty(x, Int(2)); });
    auto setup = Diff().Access(ms::WRITE);
    auto const lone = CreateNative(setup.get()).Gid();
    Commit(*setup);

    auto diff_acc = Diff().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto &ctx = Ctx();
    auto c = CreateNative(diff_acc.get());
    auto d = CreateNative(diff_acc.get());
    ASSERT_TRUE(c.SetProperty(x, Int(7)).has_value());
    auto const native_edge = CreateNativeEdge(diff_acc.get(), &c, &d);
    EXPECT_FALSE(ctx.IsBaseCandidateVertex(c.Gid())) << "a native vertex must not reuse a base-range gid";
    EXPECT_FALSE(ctx.IsBaseCandidateEdge(native_edge.Gid())) << "a native edge must not reuse a base-range gid";

    EXPECT_EQ(*ctx.GetProperty(*ctx.ResolveVertex(a, ms::View::NEW), x, ms::View::NEW), Int(2));
    EXPECT_EQ(*ctx.GetProperty(*ctx.ResolveVertex(b, ms::View::NEW), x, ms::View::NEW), Int(9)) << "B is unchanged";
    auto const resolved_c = ctx.ResolveVertex(c.Gid(), ms::View::NEW);
    ASSERT_TRUE(resolved_c.has_value());
    EXPECT_EQ(*ctx.GetProperty(*resolved_c, x, ms::View::NEW), Int(7));
    auto const c_out = ctx.OutEdges(*resolved_c, ms::View::NEW);
    ASSERT_TRUE(c_out.has_value());
    ASSERT_EQ(c_out->size(), 1U) << "the native edge is kept";
    EXPECT_EQ(c_out->front().Gid(), native_edge.Gid());

    std::vector<uint64_t> order;
    for (auto v : ctx.Vertices(ms::View::NEW)) order.push_back(v.Gid().AsUint());
    EXPECT_EQ(order.size(), 5U) << "A, B, the committed native, C and D, each exactly once";
    EXPECT_TRUE(std::ranges::is_sorted(order)) << "the union merge must be gid-ordered";
    auto const values = BranchValues(ctx, x, ms::View::NEW);
    EXPECT_EQ(values.at(a.AsUint()), 2) << "A must yield the overlay's value, not the base's stale 1";
    EXPECT_EQ(values.at(b.AsUint()), 9);
    EXPECT_EQ(values.at(c.Gid().AsUint()), 7);

    // A native vertex this transaction deleted reports DELETED_OBJECT through its accessor; by gid it is not found.
    auto lone_v = ctx.ResolveVertex(lone, ms::View::NEW);
    ASSERT_TRUE(lone_v.has_value());
    ASSERT_TRUE(diff_acc->DeleteVertex(&*lone_v).has_value());
    EXPECT_EQ(ctx.OutEdges(*lone_v, ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
    EXPECT_TRUE(ctx.OutEdges(*lone_v, ms::View::OLD).has_value()) << "the delete is not visible to OLD yet";
    bool deleted = true;
    EXPECT_TRUE(ctx.ResolveEdges(lone, ms::EdgeDirection::OUT, ms::View::NEW, &deleted).empty());
    EXPECT_FALSE(deleted);
  }

  // An untouched base vertex (branched bit clear) expands to main's edges; a branch delete of one of them sets the bit
  // and hides it.
  void BaseExpansionFollowsTheBranchedBit() {
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const e1 = CreateMainEdge(a, b, "E");
    auto const e2 = CreateMainEdge(a, b, "F");
    Fork();
    auto const out_gids = [&] {
      auto acc = Diff().Access(ms::READ);
      Begin(acc.get());
      auto const base_a = *Ctx().FindBaseVertex(a);
      EXPECT_EQ(base_a.vertex_->branched(), HasEntry(a));
      std::set<uint64_t> gids;
      for (auto const &edge : *Ctx().OutEdges(base_a, ms::View::NEW)) gids.insert(edge.Gid().AsUint());
      Ctx().EndTransaction();
      return gids;
    };
    EXPECT_EQ(out_gids(), (std::set<uint64_t>{e1.gid.AsUint(), e2.gid.AsUint()}));
    EXPECT_FALSE(HasOverlay(a));
    SeedEdge(e1, [](OverlayEdit &s) { s.MarkDeleted(); });
    EXPECT_TRUE(MainVertex(a)->branched());
    EXPECT_FALSE(HasOverlay(a));
    EXPECT_EQ(out_gids(), (std::set<uint64_t>{e2.gid.AsUint()}));
  }

  // An aborted delete leaves its edge overlay listed as a candidate; that stale candidate hides nothing, and a later
  // committed delete of the same edge hides it without listing it again.
  void StaleDeleteCandidateIsHiddenOnlyOnceTheDeleteCommits() {
    auto const a = CreateMainVertex({}, {});
    auto const b = CreateMainVertex({}, {});
    auto const e = CreateMainEdge(a, b, "E");
    Fork();
    auto const deleted = [](OverlayEdit &s) { s.MarkDeleted(); };
    auto const out_gids = [&](ms::View view) {
      auto acc = Diff().Access(ms::READ);
      Begin(acc.get());
      std::set<uint64_t> gids;
      for (auto const &edge : Ctx().ResolveEdges(a, ms::EdgeDirection::OUT, view)) gids.insert(edge.Gid().AsUint());
      Ctx().EndTransaction();
      return gids;
    };
    auto const candidates = [&](ms::EdgeDirection direction, ms::Gid vertex) {
      return Candidates(vertex, direction).size();
    };
    {
      auto writer = Diff().Access(ms::WRITE);
      ASSERT_FALSE(WriteEdgeOverlay(*writer, e, deleted).has_value());
      writer->Abort();
    }
    EXPECT_EQ(candidates(ms::EdgeDirection::OUT, a), 1U);
    EXPECT_EQ(candidates(ms::EdgeDirection::IN, b), 1U);
    EXPECT_EQ(out_gids(ms::View::NEW), (std::set<uint64_t>{e.gid.AsUint()})) << "an aborted delete hides nothing";

    SeedEdge(e, deleted);
    EXPECT_EQ(candidates(ms::EdgeDirection::OUT, a), 1U) << "the edge overlay is listed once";
    EXPECT_EQ(candidates(ms::EdgeDirection::IN, b), 1U);
    EXPECT_TRUE(out_gids(ms::View::NEW).empty());
  }

  // A snapshot that began before an overlay write reads the overlay as it was: (c) a committed delete of an absent key
  // leaves the base's value, (d) a committed overwrite of an explicit null leaves the key removed. `read` is the
  // snapshot context's value of the key; `write` commits an edit on the object.
  template <typename Read, typename Write>
  void OldSnapshotsReadTheOverlayPropertyBeforeAWrite(ms::PropertyId x, Read &&read, Write &&write) {
    {  // (c): the overlay holds no entry for x, the base has 1.
      mv::BranchContext snapshot{Ctx().core()};
      auto snapshot_acc = Diff().Access(ms::READ);
      ASSERT_TRUE(snapshot.BeginTransaction(snapshot_acc.get()).has_value());
      write([&](OverlayEdit &s) { s.RemoveProperty(x); });
      EXPECT_EQ(read(snapshot, ms::View::NEW), Int(1)) << "the snapshot predates the delete";
      snapshot.EndTransaction();
      mv::BranchContext after{Ctx().core()};
      auto after_acc = Diff().Access(ms::READ);
      ASSERT_TRUE(after.BeginTransaction(after_acc.get()).has_value());
      EXPECT_TRUE(read(after, ms::View::NEW).IsNull()) << "the delete removed the base's value";
      after.EndTransaction();
    }
    {  // (d): the overlay holds an explicit null for x.
      mv::BranchContext snapshot{Ctx().core()};
      auto snapshot_acc = Diff().Access(ms::READ);
      ASSERT_TRUE(snapshot.BeginTransaction(snapshot_acc.get()).has_value());
      write([&](OverlayEdit &s) { s.SetProperty(x, Int(7)); });
      EXPECT_TRUE(read(snapshot, ms::View::NEW).IsNull()) << "the snapshot predates the overwrite";
      snapshot.EndTransaction();
      mv::BranchContext after{Ctx().core()};
      auto after_acc = Diff().Access(ms::READ);
      ASSERT_TRUE(after.BeginTransaction(after_acc.get()).has_value());
      EXPECT_EQ(read(after, ms::View::NEW), Int(7));
      after.EndTransaction();
    }
  }

  void OldSnapshotsReadTheVertexOverlayBeforeAWrite() {
    auto const x = Prop("x");
    auto const a = CreateMainVertex({}, {{x, Int(1)}});
    Fork();
    OldSnapshotsReadTheOverlayPropertyBeforeAWrite(
        x,
        [&](mv::BranchContext &ctx, ms::View view) { return *ctx.GetProperty(*ctx.ResolveVertex(a, view), x, view); },
        [&](auto edit) { SeedVertex(a, edit); });
  }

  void OldSnapshotsReadTheEdgeOverlayBeforeAWrite() {
    auto const x = Prop("x");
    auto const v = CreateMainVertex({}, {});
    auto const w = CreateMainVertex({}, {});
    auto const e = CreateMainEdge(v, w, "E", {{x, Int(1)}});
    Fork();
    OldSnapshotsReadTheOverlayPropertyBeforeAWrite(
        x,
        [&](mv::BranchContext &ctx, ms::View view) {
          auto const edge = ctx.FindBaseEdge(e.gid, v);
          EXPECT_TRUE(edge.has_value());
          return *ctx.GetProperty(*edge, x, view);
        },
        [&](auto edit) { SeedEdge(e, edit); });
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

TEST_F(VersioningBranchEngineTest, BaseEdgesMainDeletedSinceAreFound) { BaseEdgesMainDeletedSinceAreFound(); }

TEST_F(VersioningBranchEngineNoEdgePropertiesTest, BaseEdgesMainDeletedSinceAreFound) {
  BaseEdgesMainDeletedSinceAreFound();
}

TEST_F(VersioningBranchEngineLightEdgeTest, BaseEdgesMainDeletedSinceAreFound) { BaseEdgesMainDeletedSinceAreFound(); }

#define ENGINE_EDGE_TEST(Name)                                         \
  TEST_F(VersioningBranchEngineTest, Name) { Name(); }                 \
  TEST_F(VersioningBranchEngineNoEdgePropertiesTest, Name) { Name(); } \
  TEST_F(VersioningBranchEngineLightEdgeTest, Name) { Name(); }

ENGINE_EDGE_TEST(DeletedCandidatesHideBaseEdgesFromTheAdjacency)
ENGINE_EDGE_TEST(ManyDeletedCandidatesHideTheirEdgesOnAHub)
ENGINE_EDGE_TEST(StaleDeleteCandidateIsHiddenOnlyOnceTheDeleteCommits)

TEST_F(VersioningBranchEngineTest, OldSnapshotsReadTheVertexOverlayBeforeAWrite) {
  OldSnapshotsReadTheVertexOverlayBeforeAWrite();
}

TEST_F(VersioningBranchEngineTest, OldSnapshotsReadTheEdgeOverlayBeforeAWrite) {
  OldSnapshotsReadTheEdgeOverlayBeforeAWrite();
}

TEST_F(VersioningBranchEngineLightEdgeTest, OldSnapshotsReadTheEdgeOverlayBeforeAWrite) {
  OldSnapshotsReadTheEdgeOverlayBeforeAWrite();
}

TEST_F(VersioningBranchEngineTest, BaseReadsMergeTheOverlayOverTheBaseState) {
  auto const l1 = Label("L1"), l2 = Label("L2"), l3 = Label("L3");
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z");
  auto const a = CreateMainVertex({l1, l3}, {{x, Int(1)}, {y, Int(2)}});
  auto const b = CreateMainVertex({l1}, {});
  auto const c = CreateMainVertex({l1}, {{x, Int(7)}});
  auto const d = CreateMainVertex({l1}, {{x, Int(8)}});
  auto const e = CreateMainVertex({l1}, {{x, Int(9)}});
  auto const g = CreateMainVertex({l1}, {});
  auto const h = CreateMainVertex({}, {});
  Fork();
  SeedVertex(a, [&](OverlayEdit &s) {
    s.RemoveLabel(l1);
    s.AddLabel(l2);
    s.SetProperty(x, Int(10));
    s.RemoveProperty(y);
    s.SetProperty(z, Int(3));
  });
  SeedVertex(b, [&](OverlayEdit &s) { s.RemoveLabel(l1); });
  SeedVertex(d, [](OverlayEdit &s) { s.MarkDeleted(); });
  SeedVertex(g, [&](OverlayEdit &s) { s.SetProperty(x, Int(1)); });
  SeedVertex(h, [&](OverlayEdit &s) { s.SetProperty(x, Int(1)); });
  record_attribute_changes_ = false;
  SeedVertex(e, [](OverlayEdit &s) { s.MarkDeleted(); });
  record_attribute_changes_ = true;
  EXPECT_TRUE(Core().MayHaveVertexDeleted(e));
  EXPECT_FALSE(Core().MayHaveLabelChange(e));

  auto diff_acc = Diff().Access(ms::READ);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  for (auto const view : {ms::View::OLD, ms::View::NEW}) {
    auto fa = ctx.ResolveVertex(a, view);
    ASSERT_TRUE(fa.has_value());
    EXPECT_EQ(fa->storage_, storage_.get()) << "a base vertex is returned as main's object, never as its overlay";
    EXPECT_EQ(LabelSet(ctx.Labels(*fa, view)), (std::set<ms::LabelId>{l2, l3}));
    EXPECT_EQ(*ctx.HasLabel(*fa, l1, view), false);
    EXPECT_EQ(*ctx.HasLabel(*fa, l2, view), true);
    EXPECT_EQ(*ctx.HasLabel(*fa, l3, view), true);
    EXPECT_TRUE(*ctx.HasAnyLabel(*fa, view));
    EXPECT_EQ(*ctx.Properties(*fa, view), (std::map<ms::PropertyId, ms::PropertyValue>{{x, Int(10)}, {z, Int(3)}}));
    EXPECT_EQ(*ctx.GetProperty(*fa, x, view), Int(10));
    EXPECT_TRUE(ctx.GetProperty(*fa, y, view)->IsNull());
    EXPECT_GT(*ctx.GetPropertySize(*fa, x, view), 0U);
    EXPECT_EQ(*ctx.GetPropertySize(*fa, y, view), 0U);

    auto fb = ctx.ResolveVertex(b, view);
    ASSERT_TRUE(fb.has_value());
    EXPECT_FALSE(*ctx.HasAnyLabel(*fb, view)) << "the only label is hidden";
    EXPECT_TRUE(LabelSet(ctx.Labels(*fb, view)).empty());

    EXPECT_TRUE(*ctx.HasAnyLabel(*ctx.ResolveVertex(g, view), view)) << "the overlay holds the base's labels";
    EXPECT_FALSE(*ctx.HasAnyLabel(*ctx.ResolveVertex(h, view), view));

    auto fc = ctx.ResolveVertex(c, view);
    ASSERT_TRUE(fc.has_value());
    EXPECT_FALSE(fc->vertex_->branched());
    EXPECT_EQ(*ctx.HasLabel(*fc, l1, view), true);
    EXPECT_EQ(*ctx.GetProperty(*fc, x, view), Int(7));
    EXPECT_FALSE(HasOverlay(c));

    for (auto const gone : {d, e}) {
      SCOPED_TRACE(gone.AsUint());
      EXPECT_FALSE(ctx.ResolveVertex(gone, view).has_value()) << "deleted on the branch";
      auto fd = ctx.FindBaseVertex(gone);
      ASSERT_TRUE(fd.has_value()) << "the base state itself is unchanged";
      EXPECT_TRUE(ctx.IsBaseVertexDeleted(*fd, view));
      EXPECT_EQ(ctx.Labels(*fd, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.HasAnyLabel(*fd, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.Properties(*fd, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.GetProperty(*fd, x, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.OutEdges(*fd, view).error(), ms::Error::DELETED_OBJECT);
    }
  }
}

TEST_F(VersioningBranchEngineTest, BaseEdgeReadsMergeTheEdgeOverlayOverTheBaseEdge) {
  BaseEdgeReadsMergeTheEdgeOverlayOverTheBaseEdge();
}

TEST_F(VersioningBranchEngineLightEdgeTest, BaseEdgeReadsMergeTheEdgeOverlayOverTheBaseEdge) {
  BaseEdgeReadsMergeTheEdgeOverlayOverTheBaseEdge();
}

// Without edge properties a base edge carries no branched bit to consult, so its hidden state is derived from its
// endpoints on every read.
TEST_F(VersioningBranchEngineNoEdgePropertiesTest, EdgeReadsOfAnEdgeWithADeletedEndpointReportDeleted) {
  auto const x = Prop("x");
  auto const v = CreateMainVertex({}, {});
  auto const w = CreateMainVertex({}, {});
  auto const u = CreateMainVertex({}, {});
  auto const to_deleted = CreateMainEdge(v, w, "E");
  auto const from_deleted = CreateMainEdge(w, u, "E");
  auto const kept = CreateMainEdge(v, u, "E");
  Fork();
  SeedVertex(w, [](OverlayEdit &s) { s.MarkDeleted(); });

  auto diff_acc = Diff().Access(ms::READ);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  auto fto = ctx.FindBaseEdge(to_deleted.gid, v);
  auto ffrom = ctx.FindBaseEdge(from_deleted.gid, w);
  auto fkept = ctx.FindBaseEdge(kept.gid, v);
  ASSERT_TRUE(fto && ffrom && fkept);
  EXPECT_FALSE(fto->MaybeBranchedHint());
  for (auto const view : {ms::View::OLD, ms::View::NEW}) {
    for (auto const *edge : {&*fto, &*ffrom}) {
      EXPECT_TRUE(ctx.IsBaseEdgeHidden(*edge, view));
      EXPECT_EQ(ctx.Properties(*edge, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.GetProperty(*edge, x, view).error(), ms::Error::DELETED_OBJECT);
      EXPECT_EQ(ctx.GetPropertySize(*edge, x, view).error(), ms::Error::DELETED_OBJECT);
    }
    EXPECT_FALSE(ctx.IsBaseEdgeHidden(*fkept, view));
  }
}

TEST_F(VersioningBranchEngineTest, BaseStateReadsHideBranchDeletesAndUnionNativeEdges) {
  auto const v = CreateMainVertex({}, {});
  auto const a = CreateMainVertex({}, {});
  auto const b = CreateMainVertex({}, {});
  auto const f = CreateMainVertex({}, {});
  auto const u = CreateMainVertex({}, {});
  auto const va = CreateMainEdge(v, a, "E");
  auto const vb = CreateMainEdge(v, b, "E");
  auto const av = CreateMainEdge(a, v, "E");
  auto const fa = CreateMainEdge(f, a, "E");
  Fork();
  SeedVertex(b, [](OverlayEdit &s) { s.MarkDeleted(); });
  SeedVertex(f, [](OverlayEdit &s) { s.MarkDeleted(); });
  SeedEdge(va, [](OverlayEdit &s) { s.MarkDeleted(); });
  SeedVertex(v, [](OverlayEdit &) {});  // the anchor of a native edge

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  auto &ctx = Ctx();
  EXPECT_FALSE(ctx.IsBaseVertexDeleted(*ctx.FindBaseVertex(a), ms::View::NEW));
  EXPECT_TRUE(ctx.IsBaseVertexDeleted(*ctx.FindBaseVertex(b), ms::View::NEW));
  EXPECT_TRUE(HasEntry(a));
  EXPECT_FALSE(HasOverlay(a)) << "hiding its in-edge listed it, nothing more";
  EXPECT_FALSE(ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto overlay_v = OverlayOf(v, ms::View::NEW);
  ASSERT_TRUE(overlay_v.has_value());
  auto native = CreateNative(diff_acc.get());
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &*overlay_v, &native);
  auto const native_in_edge = CreateNativeEdge(diff_acc.get(), &native, &*overlay_v);
  EXPECT_TRUE(ctx.FindDiffEdge(native_edge.Gid(), ms::View::NEW).has_value())
      << "an edge out of an overlay is found through the overlays";
  EXPECT_TRUE(ctx.FindDiffEdge(native_edge.Gid(), v, ms::View::NEW).has_value());
  EXPECT_FALSE(ctx.FindDiffEdge(native_edge.Gid(), u, ms::View::NEW).has_value()) << "u has no overlay, and no throw";
  EXPECT_FALSE(ctx.FindDiffEdge(va.gid, v, ms::View::NEW).has_value()) << "base edges are not diff edges";
  EXPECT_TRUE(ctx.ResolveEdge(native_edge.Gid(), v, ms::View::NEW).has_value());

  auto fva = ctx.FindBaseEdge(va.gid, v);
  auto fvb = ctx.FindBaseEdge(vb.gid, v);
  auto ffa = ctx.FindBaseEdge(fa.gid, f);
  auto fav = ctx.FindBaseEdge(av.gid, a);
  ASSERT_TRUE(fva && fvb && ffa && fav);
  EXPECT_TRUE(ctx.IsBaseEdgeHidden(*fva, ms::View::NEW)) << "deleted on the branch";
  EXPECT_TRUE(ctx.IsBaseEdgeHidden(*fvb, ms::View::NEW)) << "its endpoint B is deleted";
  EXPECT_TRUE(ctx.IsBaseEdgeHidden(*ffa, ms::View::NEW)) << "its endpoint F is deleted, and F is the FROM";
  EXPECT_FALSE(ctx.IsBaseEdgeHidden(*fav, ms::View::NEW));
  EXPECT_FALSE(ctx.IsBaseEdgeHidden(native_edge, ms::View::NEW)) << "a native edge is never base-hidden";

  auto fv = *ctx.ResolveVertex(v, ms::View::NEW);
  auto const out = ctx.OutEdges(fv, ms::View::NEW);
  ASSERT_TRUE(out.has_value());
  ASSERT_EQ(out->size(), 1U) << "only the native edge survives V's out-adjacency";
  EXPECT_EQ(out->front().Gid(), native_edge.Gid());
  std::set<uint64_t> in_gids;
  for (auto const &edge : *ctx.InEdges(fv, ms::View::NEW)) in_gids.insert(edge.Gid().AsUint());
  EXPECT_EQ(in_gids, (std::set<uint64_t>{av.gid.AsUint(), native_in_edge.Gid().AsUint()}));
  EXPECT_TRUE(ctx.InEdges(*ctx.ResolveVertex(a, ms::View::NEW), ms::View::NEW)->empty())
      << "both base in-edges of A are hidden, one deleted and one by its deleted FROM";
  EXPECT_EQ(ctx.InEdges(*ctx.FindBaseVertex(b), ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
  bool b_deleted = false;
  EXPECT_TRUE(ctx.ResolveEdges(b, ms::EdgeDirection::IN, ms::View::NEW, &b_deleted).empty());
  EXPECT_TRUE(b_deleted);

  std::vector<uint64_t> scanned;
  for (auto vertex : ctx.Vertices(ms::View::NEW)) {
    scanned.push_back(vertex.Gid().AsUint());
    EXPECT_EQ(vertex.storage_, vertex.Gid() == native.Gid() ? static_cast<ms::Storage *>(&Diff()) : storage_.get());
  }
  EXPECT_EQ(scanned, (std::vector<uint64_t>{v.AsUint(), a.AsUint(), u.AsUint(), native.Gid().AsUint()}))
      << "the deleted base vertices are skipped; the overlay is not a second V";
}

// A vertex the branch only listed a deleted edge on has no overlay: it reads as the base does, except that the deleted
// edge is hidden once the delete is visible.
TEST_F(VersioningBranchEngineTest, ADeletesOnlyVertexReadsAsTheBaseExceptForItsHiddenEdge) {
  auto const l1 = Label("L1");
  auto const x = Prop("x");
  auto const a = CreateMainVertex({l1}, {{x, Int(1)}});
  auto const b = CreateMainVertex({l1}, {{x, Int(2)}});
  auto const e1 = CreateMainEdge(a, b, "E");
  auto const e2 = CreateMainEdge(a, b, "F");
  Fork();

  mv::BranchContext before{Ctx().core()};
  auto before_acc = Diff().Access(ms::READ, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
  ASSERT_TRUE(before.BeginTransaction(before_acc.get()).has_value());

  SeedEdge(e1, [](OverlayEdit &s) { s.MarkDeleted(); });
  for (auto const gid : {a, b}) {
    EXPECT_TRUE(HasEntry(gid));
    EXPECT_FALSE(HasOverlay(gid));
    EXPECT_TRUE(MainVertex(gid)->branched());
  }
  EXPECT_EQ(Candidates(a, ms::EdgeDirection::OUT).size(), 1U);
  EXPECT_EQ(Candidates(b, ms::EdgeDirection::IN).size(), 1U);

  auto const gids = [](mv::BranchContext &ctx, ms::Gid vertex, ms::EdgeDirection direction, ms::View view) {
    std::set<uint64_t> out;
    for (auto const &edge : ctx.ResolveEdges(vertex, direction, view)) out.insert(edge.Gid().AsUint());
    return out;
  };
  auto const both = std::set<uint64_t>{e1.gid.AsUint(), e2.gid.AsUint()};
  auto const kept = std::set<uint64_t>{e2.gid.AsUint()};

  auto acc = Diff().Access(ms::READ);
  Begin(acc.get());
  auto &ctx = Ctx();
  for (auto const [gid, value] : {std::pair{a, 1}, std::pair{b, 2}}) {
    SCOPED_TRACE(gid.AsUint());
    for (auto const view : {ms::View::OLD, ms::View::NEW}) {
      auto const resolved = ctx.ResolveVertex(gid, view);
      ASSERT_TRUE(resolved.has_value());
      EXPECT_EQ(resolved->storage_, storage_.get()) << "read from the base, not an overlay";
      EXPECT_EQ(LabelSet(ctx.Labels(*resolved, view)), (std::set<ms::LabelId>{l1}));
      EXPECT_TRUE(*ctx.HasAnyLabel(*resolved, view));
      EXPECT_EQ(*ctx.GetProperty(*resolved, x, view), Int(value));
    }
  }
  EXPECT_EQ(gids(ctx, a, ms::EdgeDirection::OUT, ms::View::NEW), kept);
  EXPECT_EQ(gids(ctx, b, ms::EdgeDirection::IN, ms::View::NEW), kept);
  // The delete committed before this transaction began, so the edge is hidden at OLD too.
  EXPECT_EQ(gids(ctx, a, ms::EdgeDirection::OUT, ms::View::OLD), kept);
  EXPECT_EQ(gids(ctx, b, ms::EdgeDirection::IN, ms::View::OLD), kept);
  EXPECT_EQ(gids(before, a, ms::EdgeDirection::OUT, ms::View::NEW), both) << "the delete is after its snapshot";
  EXPECT_EQ(gids(before, b, ms::EdgeDirection::IN, ms::View::NEW), both);
  EXPECT_FALSE(HasOverlay(a));
  EXPECT_FALSE(HasOverlay(b));
  ctx.EndTransaction();
  before.EndTransaction();
}

TEST_F(VersioningBranchEngineTest, TheTransactionStateFollowsBeginAndEnd) {
  Fork();
  auto &ctx = Ctx();
  EXPECT_FALSE(ctx.in_transaction());
  EXPECT_DEATH((void)ctx.historical(), "no branch transaction is open");
  auto historical = ctx.OpenHistorical();
  ASSERT_TRUE(historical.has_value());
  EXPECT_FALSE(ctx.in_transaction()) << "opening main alone starts no branch transaction";
  auto diff_acc = Diff().Access(ms::READ);
  ctx.BeginTransaction(std::move(*historical), *diff_acc);
  EXPECT_TRUE(ctx.in_transaction());
  EXPECT_EQ(ctx.current_diff_txn(), diff_acc.get());
  ctx.EndTransaction();
  ctx.EndTransaction();
  EXPECT_FALSE(ctx.in_transaction());
}

TEST_F(VersioningBranchEngineTest, OpenHistoricalSaysWhyMainWouldNotOpenTheForkPoint) {
  Fork();
  storage_->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  auto const mode = Ctx().OpenHistorical();
  storage_->SetStorageMode(ms::StorageMode::IN_MEMORY_TRANSACTIONAL);
  ASSERT_FALSE(mode.has_value());
  EXPECT_NE(mode.error().message.find("IN_MEMORY_TRANSACTIONAL"), std::string::npos) << mode.error().message;

  storage_->ReleaseForkPin(*fork_ts_);
  fork_ts_.reset();
  auto const unpinned = Ctx().OpenHistorical();
  ASSERT_FALSE(unpinned.has_value());
  EXPECT_NE(unpinned.error().message.find("no longer retained"), std::string::npos) << unpinned.error().message;
  EXPECT_FALSE(Ctx().in_transaction());
}

// An overlay commit is a diff-engine commit, read under the reader's own isolation level.
TEST_F(VersioningBranchEngineTest, SnapshotAndReadCommittedBranchReadersSeeAnOverlayCommitAsTheirIsolationAllows) {
  auto const l1 = Label("L1");
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  auto const b = CreateMainVertex({}, {{x, Int(2)}});
  Fork();
  SeedVertex(b, [](OverlayEdit &) {});  // an overlay that exists but changes nothing yet

  mv::BranchContext snapshot_ctx{branch_context_->core()};
  mv::BranchContext committed_ctx{branch_context_->core()};
  auto snapshot_acc = Diff().Access(ms::READ, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
  auto committed_acc = Diff().Access(ms::READ, ms::IsolationLevel::READ_COMMITTED, std::nullopt);
  ASSERT_TRUE(snapshot_ctx.BeginTransaction(snapshot_acc.get()).has_value());
  ASSERT_TRUE(committed_ctx.BeginTransaction(committed_acc.get()).has_value());

  SeedVertex(a, [&](OverlayEdit &s) {
    s.SetProperty(x, Int(10));
    s.AddLabel(l1);
  });
  SeedVertex(b, [](OverlayEdit &s) { s.MarkDeleted(); });

  auto const base_a = *snapshot_ctx.FindBaseVertex(a);
  EXPECT_EQ(*snapshot_ctx.GetProperty(base_a, x, ms::View::NEW), Int(1));
  EXPECT_FALSE(*snapshot_ctx.HasLabel(base_a, l1, ms::View::NEW));
  EXPECT_TRUE(snapshot_ctx.ResolveVertex(b, ms::View::NEW).has_value());

  auto const committed_a = *committed_ctx.FindBaseVertex(a);
  EXPECT_EQ(*committed_ctx.GetProperty(committed_a, x, ms::View::NEW), Int(10));
  EXPECT_TRUE(*committed_ctx.HasLabel(committed_a, l1, ms::View::NEW));
  EXPECT_FALSE(committed_ctx.ResolveVertex(b, ms::View::NEW).has_value());
  snapshot_ctx.EndTransaction();
  committed_ctx.EndTransaction();
}

TEST_F(VersioningBranchEngineTest, ResolveAndUnionScanMergeOverlaysAndIncludeNatives) {
  ResolveAndUnionScanMergeOverlaysAndIncludeNatives();
}

TEST_F(VersioningBranchEngineLightEdgeTest, ResolveAndUnionScanMergeOverlaysAndIncludeNatives) {
  ResolveAndUnionScanMergeOverlaysAndIncludeNatives();
}

TEST_F(VersioningBranchEngineTest, BaseExpansionFollowsTheBranchedBit) { BaseExpansionFollowsTheBranchedBit(); }

TEST_F(VersioningBranchEngineLightEdgeTest, BaseExpansionFollowsTheBranchedBit) {
  BaseExpansionFollowsTheBranchedBit();
}

// One overlay writer (every base vertex in each commit), one native writer (two vertices per commit) and main writes
// against branch readers on their own contexts: each snapshot reader sees one whole snapshot on every scan, and a
// read-committed reader only ever sees values some commit wrote.
TEST_F(VersioningBranchEngineTest, ConcurrentBranchReadersAndWritersSeeStableSnapshots) {
  constexpr int kBaseVertices = 8;
  constexpr int kRounds = 1000;
  auto const x = Prop("x");
  std::vector<ms::Gid> gids;
  for (int i = 0; i < kBaseVertices; ++i) gids.push_back(CreateMainVertex({}, {{x, Int(0)}}));
  Fork();
  for (auto const gid : gids) SeedVertex(gid, [](OverlayEdit &) {});
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
        ASSERT_FALSE(WriteVertexOverlay(*acc, gid, [&](OverlayEdit &s) { s.SetProperty(x, Int(round)); }).has_value());
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
      ASSERT_TRUE(acc->FindVertex(gids[round % kBaseVertices], ms::View::OLD)->SetProperty(x, Int(-round)).has_value());
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
            auto v = ctx.FindBaseVertex(gid);
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
        ASSERT_EQ(first.size() % 2, kBaseVertices % 2u) << "native vertices are committed in pairs";
        auto const round = first.at(gids[0].AsUint());
        for (auto const gid : gids) ASSERT_EQ(first.at(gid.AsUint()), round) << "an overlay commit is all-or-nothing";
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
  EXPECT_EQ(final_values.size(), kBaseVertices + (2 * kRounds));
  for (auto const gid : gids) EXPECT_EQ(final_values.at(gid.AsUint()), kRounds);
  Ctx().EndTransaction();
}

// ---- The overlay encoding as the branch reads it: full labels, value / explicit null / absent, delete mark, older
// snapshots.

// A branch reader at snapshot isolation; its diff accessor outlives its context.
struct SnapshotReader {
  std::unique_ptr<ms::Storage::Accessor> acc;
  std::unique_ptr<mv::BranchContext> ctx;
};

SnapshotReader OpenSnapshotReader(mv::BranchContext &like) {
  SnapshotReader reader{
      .acc = like.diff_engine().Access(ms::READ, ms::IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt),
      .ctx = std::make_unique<mv::BranchContext>(like.core())};
  EXPECT_TRUE(reader.ctx->BeginTransaction(reader.acc.get()).has_value());
  return reader;
}

TEST_F(VersioningBranchEngineTest, AnOverlayHoldsTheBaseLabelsFromCreationAndReadsThemAtTheReadersSnapshot) {
  auto const l1 = Label("L1"), l2 = Label("L2"), l3 = Label("L3");
  auto const a = CreateMainVertex({l1, l2}, {});
  auto const untouched = CreateMainVertex({l1}, {});
  Fork();
  auto const before = OpenSnapshotReader(Ctx());
  SeedVertex(a, [&](OverlayEdit &s) {
    s.RemoveLabel(l1);
    s.AddLabel(l3);
  });
  auto const after = OpenSnapshotReader(Ctx());

  auto const labels_of = [&](const SnapshotReader &reader, ms::Gid gid) {
    auto const vertex = reader.ctx->ResolveVertex(gid, ms::View::NEW);
    EXPECT_TRUE(vertex.has_value());
    return LabelSet(reader.ctx->Labels(*vertex, ms::View::NEW));
  };
  EXPECT_EQ(labels_of(after, a), (std::set<ms::LabelId>{l2, l3})) << "the removal and the addition";
  EXPECT_EQ(labels_of(before, a), (std::set<ms::LabelId>{l1, l2})) << "the commit is after this snapshot";
  EXPECT_EQ(labels_of(after, untouched), (std::set<ms::LabelId>{l1}));
  for (auto const &[reader, has_l1, has_l3] : {std::tuple{&before, true, false}, std::tuple{&after, false, true}}) {
    auto const vertex = *reader->ctx->ResolveVertex(a, ms::View::NEW);
    EXPECT_EQ(*reader->ctx->HasLabel(vertex, l1, ms::View::NEW), has_l1);
    EXPECT_EQ(*reader->ctx->HasLabel(vertex, l3, ms::View::NEW), has_l3);
    EXPECT_TRUE(*reader->ctx->HasLabel(vertex, l2, ms::View::NEW));
    EXPECT_TRUE(*reader->ctx->HasAnyLabel(vertex, ms::View::NEW));
  }
}

TEST_F(VersioningBranchEngineTest, AKeyIsTheOverlaysValueAnExplicitNullOrTheBasesValue) {
  auto const x = Prop("x"), y = Prop("y"), z = Prop("z"), w = Prop("w");
  auto const a = CreateMainVertex({}, {{x, Int(1)}, {y, Int(2)}, {z, Int(3)}});
  Fork();
  SeedVertex(a, [&](OverlayEdit &s) {
    s.SetProperty(x, Int(10));
    s.RemoveProperty(y);
    s.SetProperty(w, Int(4));
  });
  auto acc = Diff().Access(ms::READ);
  Begin(acc.get());
  auto &ctx = Ctx();
  auto const vertex = *ctx.ResolveVertex(a, ms::View::NEW);
  for (auto const view : {ms::View::OLD, ms::View::NEW}) {
    EXPECT_EQ(*ctx.GetProperty(vertex, x, view), Int(10)) << "the overlay's value over the base's";
    EXPECT_TRUE(ctx.GetProperty(vertex, y, view)->IsNull()) << "an explicit null hides the base's value";
    EXPECT_EQ(*ctx.GetProperty(vertex, z, view), Int(3)) << "an absent key is the base's";
    EXPECT_EQ(*ctx.GetProperty(vertex, w, view), Int(4)) << "a key only the overlay has";
    EXPECT_EQ(*ctx.GetPropertySize(vertex, y, view), 0U);
    EXPECT_GT(*ctx.GetPropertySize(vertex, z, view), 0U);
    EXPECT_EQ(*ctx.Properties(vertex, view),
              (std::map<ms::PropertyId, ms::PropertyValue>{{x, Int(10)}, {z, Int(3)}, {w, Int(4)}}));
  }
}

// The explicit-null before-image is what a stock reader loses: at an older snapshot it would read as an absent key and
// bring the base's value back.
TEST_F(VersioningBranchEngineTest, OlderSnapshotsReadTheOverlaysPreWriteStateIncludingAnExplicitNull) {
  auto const y = Prop("y");
  auto const a = CreateMainVertex({}, {{y, Int(2)}});
  Fork();
  auto const absent = OpenSnapshotReader(Ctx());
  SeedVertex(a, [&](OverlayEdit &s) { s.RemoveProperty(y); });
  auto const explicit_null = OpenSnapshotReader(Ctx());
  SeedVertex(a, [&](OverlayEdit &s) { s.SetProperty(y, Int(5)); });
  auto const value = OpenSnapshotReader(Ctx());
  SeedVertex(a, [&](OverlayEdit &s) { s.RemoveProperty(y); });
  auto const latest = OpenSnapshotReader(Ctx());

  auto const expect_state = [&](const SnapshotReader &reader, std::optional<int64_t> expected, const char *what) {
    SCOPED_TRACE(what);
    auto const vertex = *reader.ctx->ResolveVertex(a, ms::View::NEW);
    auto const got = reader.ctx->GetProperty(vertex, y, ms::View::NEW);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->IsNull() ? std::nullopt : std::optional<int64_t>{got->ValueInt()}, expected);
    auto const all = reader.ctx->Properties(vertex, ms::View::NEW);
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(all->contains(y) ? std::optional<int64_t>{all->at(y).ValueInt()} : std::nullopt, expected);
    EXPECT_EQ(*reader.ctx->GetPropertySize(vertex, y, ms::View::NEW) != 0, expected.has_value());
  };
  expect_state(absent, 2, "before the overlay: the base's value");
  expect_state(explicit_null, std::nullopt, "an explicit null, not the base's value");
  expect_state(value, 5, "the overlay's value");
  expect_state(latest, std::nullopt, "removed again");

  // An aborted write over an explicit null puts the explicit null back.
  {
    auto writer = Diff().Access(ms::WRITE);
    ASSERT_FALSE(WriteVertexOverlay(*writer, a, [&](OverlayEdit &s) { s.SetProperty(y, Int(9)); }).has_value());
    writer->Abort();
  }
  expect_state(OpenSnapshotReader(Ctx()), std::nullopt, "after an aborted overwrite");
}

TEST_F(VersioningBranchEngineTest, AMarkedDeletedOverlayHidesTheVertexFromNewReadsOnlyOnceCommitted) {
  auto const x = Prop("x");
  auto const a = CreateMainVertex({}, {{x, Int(1)}});
  Fork();
  auto const before = OpenSnapshotReader(Ctx());
  {
    auto writer = Diff().Access(ms::WRITE);
    Begin(writer.get());
    ASSERT_FALSE(WriteVertexOverlay(*writer, a, [](OverlayEdit &s) { s.MarkDeleted(); }).has_value());
    EXPECT_FALSE(Ctx().ResolveVertex(a, ms::View::NEW).has_value()) << "the writer's own delete";
    EXPECT_TRUE(Ctx().ResolveVertex(a, ms::View::OLD).has_value()) << "not yet visible to OLD";
    Ctx().EndTransaction();
    Commit(*writer);
  }
  auto const after = OpenSnapshotReader(Ctx());
  EXPECT_FALSE(after.ctx->ResolveVertex(a, ms::View::NEW).has_value());
  EXPECT_EQ(after.ctx->Properties(*after.ctx->FindBaseVertex(a), ms::View::NEW).error(), ms::Error::DELETED_OBJECT);
  auto const old = before.ctx->ResolveVertex(a, ms::View::NEW);
  ASSERT_TRUE(old.has_value()) << "the snapshot predates the delete mark";
  EXPECT_EQ(*before.ctx->GetProperty(*old, x, ms::View::NEW), Int(1));
}

// ---- Gid floors: main objects created after fork_ts but before the lazy Build are below the floor and invisible.

TEST_F(VersioningBranchEngineTest, PostForkPreBuildMainObjectsAreBaseCandidatesButInvisible) {
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
  auto const e3 = CreateMainEdge(v1, v2, "E");  // base vertex -> post-fork vertex
  Commit(*inflight);
  inflight.reset();

  // Lazy first build, after all of the above.
  auto built = mv::BranchContext::BuildFromFork(storage, *fork_ts_);
  ASSERT_TRUE(built.has_value());
  branch_context_ = std::move(*built);
  auto &ctx = Ctx();
  auto const vertex_floor = Core().vertex_gid_floor();
  auto const edge_floor = Core().edge_gid_floor();

  std::vector<ms::Gid> const invisible_vertices{v2, w2, inflight_x_gid, late_y};
  std::vector<std::pair<ms::Gid, ms::Gid>> const invisible_edges{{e2.gid, v2}, {e3.gid, v1}, {inflight_e_gid, v1}};
  for (auto gid : invisible_vertices) EXPECT_LT(gid.AsUint(), vertex_floor) << "the floor is read at Build";
  for (auto const &[gid, from] : invisible_edges) EXPECT_LT(gid.AsUint(), edge_floor);

  auto const check_invisible = [&](bool with_natives) {
    for (auto gid : invisible_vertices) {
      EXPECT_TRUE(ctx.IsBaseCandidateVertex(gid)) << gid.AsUint();
      for (auto view : {ms::View::OLD, ms::View::NEW}) EXPECT_FALSE(ctx.ResolveVertex(gid, view).has_value());
      EXPECT_FALSE(ctx.FindBaseVertex(gid).has_value()) << gid.AsUint();
      EXPECT_FALSE(HasOverlay(gid));
      EXPECT_TRUE(ctx.ResolveEdges(gid, ms::EdgeDirection::OUT, ms::View::NEW).empty()) << gid.AsUint();
    }
    for (auto const &[gid, from] : invisible_edges) {
      EXPECT_TRUE(ctx.IsBaseCandidateEdge(gid)) << gid.AsUint();
      EXPECT_FALSE(ctx.FindBaseEdge(gid, from).has_value());
      EXPECT_FALSE(ctx.FindDiffEdge(gid, ms::View::NEW).has_value());
      EXPECT_FALSE(ctx.ResolveEdge(gid, from, ms::View::NEW).has_value()) << gid.AsUint();
    }
    // The base vertex does not expand to the post-fork object.
    std::vector<uint64_t> out_edges;
    for (auto e : ctx.ResolveEdges(v1, ms::EdgeDirection::OUT, ms::View::NEW)) out_edges.push_back(e.Gid().AsUint());
    EXPECT_EQ(std::ranges::count(out_edges, e1.gid.AsUint()), 1);
    EXPECT_EQ(out_edges.size(), with_natives ? 2U : 1U) << "e1, plus the native edge off the v1 anchor";
    std::vector<uint64_t> scanned;
    for (auto v : ctx.Vertices(ms::View::NEW)) scanned.push_back(v.Gid().AsUint());
    EXPECT_EQ(scanned.size(), with_natives ? 4U : 2U);
    for (auto gid : invisible_vertices) EXPECT_EQ(std::ranges::count(scanned, gid.AsUint()), 0);
  };

  auto diff_acc = Diff().Access(ms::WRITE);
  Begin(diff_acc.get());
  check_invisible(false);

  // Native creates are at or above the floor and collide with nothing, including the post-fork main gids.
  auto n1 = CreateNative(diff_acc.get());
  auto n2 = CreateNative(diff_acc.get());
  auto const native_edge = CreateNativeEdge(diff_acc.get(), &n1, &n2);
  EXPECT_GE(n1.Gid().AsUint(), vertex_floor);
  EXPECT_GE(native_edge.Gid().AsUint(), edge_floor);
  EXPECT_FALSE(ctx.IsBaseCandidateVertex(n1.Gid()));
  EXPECT_FALSE(ctx.IsBaseCandidateEdge(native_edge.Gid()));
  for (auto gid : invisible_vertices) EXPECT_NE(n1.Gid(), gid);
  for (auto const &[gid, from] : invisible_edges) EXPECT_NE(native_edge.Gid(), gid);
  EXPECT_TRUE(ctx.ResolveEdge(native_edge.Gid(), n1.Gid(), ms::View::NEW).has_value());
  // A native edge off a base vertex's overlay.
  ASSERT_FALSE(WriteVertexOverlay(*diff_acc, v1, [](OverlayEdit &) {}).has_value());
  auto anchor = *OverlayOf(v1, ms::View::NEW);
  EXPECT_GE(CreateNativeEdge(diff_acc.get(), &anchor, &n2).Gid().AsUint(), edge_floor);
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

}  // namespace
