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
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
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
#include "utils/uuid.hpp"
#include "versioning/branch_engine.hpp"

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

TEST_F(VersioningBranchCoreConfigTest, DiffEngineNeverAutoCreatesIndexes) {
  {
    auto acc = Diff().Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    ASSERT_TRUE(from.AddLabel(core_->NameToLabel("Fresh")).has_value());
    ASSERT_TRUE(acc->CreateEdge(&from, &to, core_->NameToEdgeType("FRESH_EDGE")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Diff().GetAsyncIndexer().CompleteRemaining();
  const auto indices = Diff().Access(ms::READ)->ListAllIndices();
  const auto fresh = core_->NameToLabel("Fresh");
  EXPECT_FALSE(std::ranges::contains(indices.label, fresh));
  EXPECT_FALSE(std::ranges::contains(indices.edge_type, core_->NameToEdgeType("FRESH_EDGE")));
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
  core_->RecordEdgeChange(gid);
  EXPECT_TRUE(core_->MayHaveLabelChange(gid));
  EXPECT_TRUE(core_->MayHavePropertyChange(gid));
  EXPECT_TRUE(core_->MayHaveEdgeChange(gid));
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
    ms::VertexAccessor va{core_->overlay_index().FindOverlay(ms::Gid::FromUint(0)), &Diff(), acc->GetTransaction()};
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

}  // namespace
