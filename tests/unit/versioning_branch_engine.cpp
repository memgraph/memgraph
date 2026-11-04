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
// not link mg-query). The diff engine shares main's NameIdMapper, so one label/property id is valid on both sides.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <prometheus/gauge.h>

#include "memory/db_arena.hpp"
#include "metrics/metric_handles.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/vertex_overlay.hpp"

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {

class VersioningBranchCoreTest : public testing::Test {
 protected:
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
      auto e = acc->CreateEdge(&v, &a, storage_->NameToEdgeType("E"));
      ASSERT_TRUE(e.has_value());
      edge_gid_ = e->Gid();
      ASSERT_TRUE(e->SetProperty(prop_x_, ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
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
  ms::LabelId label_;
  ms::PropertyId prop_x_;
  ms::Gid v_gid_, edge_gid_;
};

class VersioningBranchCorePeriodicGcTest : public VersioningBranchCoreTest {
 protected:
  ms::Config MainConfig() const override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(20)};
    return config;
  }
};

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

  EXPECT_EQ(Diff().name(), storage_->name());
  storage_->config_.salient.name = "renamed";
  EXPECT_EQ(Diff().name(), "renamed") << "a rename of main must be visible through the diff engine";
  EXPECT_EQ(*Diff().name_view(), "renamed");
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

  core_->RecordVertexChange(gid, mv::BranchChangeKind::kLabel);
  core_->RecordVertexChange(gid, mv::BranchChangeKind::kProperty);
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

class VersioningBranchEngineTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.salient.items.storage_light_edge = LightEdge();
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    branch_context_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  virtual bool LightEdge() const { return false; }

  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  void Fork() {
    auto result = mv::BranchContext::BuildFromFork(*mem_storage_, ForkNow());
    ASSERT_TRUE(result.has_value());
    branch_context_ = std::move(*result);
  }

  void Begin(ms::Storage::Accessor *diff_acc) { ASSERT_TRUE(branch_context_->BeginTransaction(diff_acc).has_value()); }

  mv::BranchCore &Core() { return *branch_context_->core(); }

  // Commits one overlay version of `key` in its own transaction, before any later diff transaction starts.
  template <typename Mutator>
  void SeedOverlay(uint64_t key, Mutator &&mutate) {
    auto &diff = branch_context_->diff_engine();
    auto seed = diff.Access(ms::WRITE);
    auto const *txn = seed->GetTransaction();
    mv::OverlayTransaction overlay_txn(txn->start_timestamp, txn->transaction_id, txn->isolation_level);
    auto written = Core().overlay_store().Write(key, overlay_txn, [&](mv::OverlayState &state) {
      mutate(state);
      return true;
    });
    ASSERT_TRUE(written.value_or(false));
    uint64_t commit_ts = 0;
    {
      std::lock_guard guard{diff.engine_lock_};
      commit_ts = diff.timestamp_++;
    }
    Core().overlay_store().Commit(overlay_txn, commit_ts);
  }

  // Seeds a committed branch delete of fork vertex `gid`, as the write path records it.
  void SeedVertexDelete(ms::Gid gid) {
    Core().RecordVertexDeleted(gid);
    {
      auto acc = storage_->Access(ms::READ);
      auto *main_vertex = acc->FindVertex(gid, ms::View::OLD)->vertex_;
      auto guard = std::unique_lock{main_vertex->lock};
      main_vertex->SetBranched(true);
    }
    SeedOverlay(gid.AsUint(), [](mv::OverlayState &state) { state.MarkDeleted(); });
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;
};

TEST_F(VersioningBranchEngineTest, ResolveAndUnionScanMergeOverlayAndIncludeNatives) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    auto b = acc->CreateVertex();
    b_gid = b.Gid();
    ASSERT_TRUE(b.SetProperty(prop_x, ms::PropertyValue(9)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Fork();
  SeedOverlay(a_gid.AsUint(), [&](mv::OverlayState &state) { state.SetProperty(prop_x, ms::PropertyValue(2)); });

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff_acc.get());

  auto c = branch_context_->CreateNativeVertex();
  auto d = branch_context_->CreateNativeVertex();
  const auto c_gid = c.Gid();
  ASSERT_TRUE(c.SetProperty(prop_x, ms::PropertyValue(7)).has_value());
  const auto native_edge = *branch_context_->CreateNativeEdge(&c, &d, storage_->NameToEdgeType("N"));
  EXPECT_FALSE(branch_context_->IsForkVertexGid(c_gid)) << "a native vertex must not reuse a fork-range gid";
  EXPECT_FALSE(branch_context_->IsForkEdgeGid(native_edge.Gid())) << "a native edge must not reuse a fork-range gid";

  auto branch_value = [&](const ms::VertexAccessor &fork) {
    auto const *overlay = branch_context_->ReadOverlayVersion(fork.Gid(), ms::View::NEW);
    EXPECT_NE(overlay, nullptr);
    return mv::MergeGetProperty(*overlay, fork, prop_x);
  };
  auto resolved_a = branch_context_->ResolveVertex(a_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_a.has_value());
  auto a_val = branch_value(*resolved_a);
  ASSERT_TRUE(a_val.has_value());
  EXPECT_EQ(*a_val, ms::PropertyValue(2));
  EXPECT_EQ(branch_context_->ReadOverlayVersion(b_gid, ms::View::NEW), nullptr) << "B was never changed";
  auto resolved_b = branch_context_->ResolveVertex(b_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_b.has_value());
  auto b_val = resolved_b->GetProperty(prop_x, ms::View::OLD);
  ASSERT_TRUE(b_val.has_value());
  EXPECT_EQ(*b_val, ms::PropertyValue(9));

  std::map<uint64_t, int64_t> seen;
  size_t yielded = 0;
  std::vector<uint64_t> order;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) {
    ++yielded;
    order.push_back(v.Gid().AsUint());
    auto val = v.Gid() == a_gid ? branch_value(v) : v.GetProperty(prop_x, ms::View::NEW);
    ASSERT_TRUE(val.has_value());
    seen.emplace(v.Gid().AsUint(), val->IsNull() ? -1 : val->ValueInt());
  }
  EXPECT_EQ(yielded, 4u) << "A, B, C, D must each be yielded exactly once";
  ASSERT_EQ(seen.size(), 4u);
  EXPECT_TRUE(std::ranges::is_sorted(order)) << "the union merge must be gid-ordered";
  EXPECT_EQ(seen.at(a_gid.AsUint()), 2) << "A must yield the overlay value, not the fork's stale 1";
  EXPECT_EQ(seen.at(b_gid.AsUint()), 9) << "B must yield the fork's untouched value";
  EXPECT_EQ(seen.at(c_gid.AsUint()), 7) << "C (branch-native) must be yielded too";
}

// Fork state with committed branch deletes (vertex B, fork edge V->A) and a native edge on V's anchor stub.
TEST_F(VersioningBranchEngineTest, ForkStateReadsHideBranchDeletesAndUnionNativeEdges) {
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid v_gid, a_gid, b_gid, va_gid, vb_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    v_gid = v.Gid();
    a_gid = a.Gid();
    b_gid = b.Gid();
    va_gid = acc->CreateEdge(&v, &a, edge_type)->Gid();
    vb_gid = acc->CreateEdge(&v, &b, edge_type)->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Fork();
  SeedVertexDelete(b_gid);
  Core().RecordForkEdgeDeleted(va_gid);
  Core().RecordEdgeChange(v_gid);
  Core().RecordEdgeChange(a_gid);
  SeedOverlay(mv::EdgeKey(va_gid.AsUint()), [](mv::OverlayState &state) { state.MarkDeleted(); });
  Core().stub_index().GetOrCreateStub(v_gid.AsUint(), nullptr);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff_acc.get());

  EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));
  EXPECT_TRUE(branch_context_->IsVertexDeleted(b_gid, ms::View::NEW));
  EXPECT_TRUE(branch_context_->HasVertexOverlay(b_gid));
  EXPECT_FALSE(branch_context_->HasVertexOverlay(a_gid));
  EXPECT_FALSE(branch_context_->ResolveVertex(b_gid, ms::View::NEW).has_value());
  EXPECT_TRUE(branch_context_->FindForkVertex(b_gid).has_value()) << "the fork state itself is unchanged";

  auto stub = branch_context_->FindStub(v_gid, ms::View::NEW);
  ASSERT_TRUE(stub.has_value());
  EXPECT_FALSE(branch_context_->FindStub(a_gid, ms::View::NEW).has_value()) << "a fork gid without an anchor";
  auto native = branch_context_->CreateNativeVertex();
  const auto native_edge = *branch_context_->CreateNativeEdge(&*stub, &native, storage_->NameToEdgeType("N"));
  EXPECT_TRUE(branch_context_->FindDiffEdge(native_edge.Gid(), ms::View::NEW).has_value())
      << "an edge out of a stub is found through the stub index";
  EXPECT_TRUE(branch_context_->FindDiffEdge(native_edge.Gid(), v_gid, ms::View::NEW).has_value());
  EXPECT_FALSE(branch_context_->FindDiffEdge(va_gid, ms::View::NEW).has_value()) << "fork edges are not diff edges";

  auto va = branch_context_->FindForkEdge(va_gid, v_gid);
  auto vb = branch_context_->FindForkEdge(vb_gid, v_gid);
  ASSERT_TRUE(va.has_value());
  ASSERT_TRUE(vb.has_value());
  EXPECT_TRUE(branch_context_->IsForkEdgeHidden(*va, ms::View::NEW)) << "deleted on the branch";
  EXPECT_TRUE(branch_context_->IsForkEdgeHidden(*vb, ms::View::NEW)) << "its endpoint B is deleted";
  EXPECT_FALSE(branch_context_->IsForkEdgeHidden(native_edge, ms::View::NEW)) << "a native edge is never fork-hidden";
  auto const *va_version = branch_context_->ReadEdgeOverlayVersion(va_gid, ms::View::NEW);
  ASSERT_NE(va_version, nullptr);
  EXPECT_TRUE(va_version->deleted());

  auto out = branch_context_->ResolveEdges(v_gid, ms::EdgeDirection::OUT, ms::View::NEW);
  ASSERT_EQ(out.size(), 1u) << "only the native edge survives V's out-adjacency";
  EXPECT_EQ(out[0].Gid(), native_edge.Gid());
  EXPECT_TRUE(branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
  bool b_deleted = false;
  EXPECT_TRUE(branch_context_->ResolveEdges(b_gid, ms::EdgeDirection::IN, ms::View::NEW, &b_deleted).empty());
  EXPECT_TRUE(b_deleted);

  std::vector<uint64_t> scanned;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) scanned.push_back(v.Gid().AsUint());
  EXPECT_EQ(scanned, (std::vector<uint64_t>{v_gid.AsUint(), a_gid.AsUint(), native.Gid().AsUint()}))
      << "the deleted fork vertex is skipped; the stub is not a second V";
}

// A branch transaction opens only while main still pins the fork.
TEST_F(VersioningBranchEngineTest, OpenHistoricalFailsOnceTheForkIsNoLongerPinned) {
  Fork();
  ASSERT_TRUE(branch_context_->OpenHistorical().has_value());
  branch_context_->EndTransaction();
  EXPECT_EQ(branch_context_->current_diff_txn(), nullptr);

  mem_storage_->ReleaseForkPin(*fork_ts_);
  fork_ts_.reset();
  auto reopened = branch_context_->OpenHistorical();
  ASSERT_FALSE(reopened.has_value());
  EXPECT_NE(reopened.error().message.find("no longer retained"), std::string::npos) << reopened.error().message;
}

// CowVertex materializes an empty stub for ONLY the touched vertex; untouched vertices never appear in the diff engine.
TEST_F(VersioningBranchEngineTest, CowVertexMaterializesEmptyStubOnlyForTouchedVertex) {
  const auto label_a = storage_->NameToLabel("A");
  const auto label_b = storage_->NameToLabel("B");
  const auto prop_x = storage_->NameToProperty("x");

  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    auto b = acc->CreateVertex();
    b_gid = b.Gid();
    ASSERT_TRUE(b.AddLabel(label_b).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Fork();

  auto count_diff_vertices = [](ms::Storage::Accessor &acc) {
    size_t n = 0;
    for (auto v : acc.Vertices(ms::View::NEW)) {
      (void)v;
      ++n;
    }
    return n;
  };

  auto count_stubs = [&] {
    size_t n = 0;
    Core().stub_index().ForEachStub([&](ms::Vertex *) { ++n; });
    return n;
  };

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff_acc.get());

  ASSERT_EQ(count_diff_vertices(*diff_acc), 0u);
  ASSERT_EQ(count_stubs(), 0u);

  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  EXPECT_EQ(cowed->Gid(), a_gid);

  EXPECT_FALSE(cowed->HasLabel(label_a, ms::View::NEW).value_or(true));
  auto x_val = cowed->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_TRUE(x_val->IsNull());

  auto fork_a = branch_context_->FindForkVertex(a_gid);
  ASSERT_TRUE(fork_a.has_value());
  EXPECT_TRUE(fork_a->HasLabel(label_a, ms::View::OLD).value_or(false));
  EXPECT_TRUE(fork_a->vertex_->branched()) << "the CoW sets main's hint bit";
  EXPECT_TRUE(branch_context_->ForkIdentity(*cowed) == *fork_a) << "a stub surfaces as its fork vertex";
  EXPECT_TRUE(branch_context_->ForkIdentity(*fork_a) == *fork_a);

  EXPECT_EQ(count_diff_vertices(*diff_acc), 0u) << "stubs are owned by the stub index, not the diff engine's vertices_";
  EXPECT_EQ(count_stubs(), 1u);
  EXPECT_NE(Core().stub_index().FindStub(a_gid.AsUint()), nullptr);
  EXPECT_FALSE(branch_context_->FindStub(b_gid, ms::View::NEW).has_value()) << "untouched vertex must not be copied";

  ASSERT_TRUE(branch_context_->OverlaySetProperty(a_gid, prop_x, ms::PropertyValue(42)).has_value());
  EXPECT_EQ(branch_context_->OverlayWrittenGids(), std::vector<uint64_t>{a_gid.AsUint()});
  auto second = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->vertex_, cowed->vertex_) << "second CowVertex must resolve to the SAME anchor";
  auto x_again = second->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_again.has_value());
  EXPECT_TRUE(x_again->IsNull()) << "an anchor carries no overlay state";
  auto const *overlay = branch_context_->ReadOverlayVersion(a_gid, ms::View::NEW);
  ASSERT_NE(overlay, nullptr);
  EXPECT_EQ(overlay->properties().GetProperty(prop_x), ms::PropertyValue(42));
  EXPECT_EQ(count_stubs(), 1u) << "idempotent COW must not create a duplicate";

  auto missing = branch_context_->CowVertex(ms::Gid::FromUint(1'000'000));
  ASSERT_FALSE(missing.has_value());
  EXPECT_NE(missing.error().message.find("not found"), std::string::npos) << missing.error().message;
  EXPECT_EQ(count_stubs(), 1u);

  auto deleted_a = branch_context_->OverlayMarkDeleted(a_gid);
  ASSERT_TRUE(deleted_a.has_value() && *deleted_a);
  auto refused = branch_context_->CowVertex(a_gid);
  ASSERT_FALSE(refused.has_value());
  EXPECT_NE(refused.error().message.find("already deleted"), std::string::npos) << refused.error().message;
  EXPECT_FALSE(branch_context_->PrepareForkWrite(a_gid).has_value());

  ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Label and property writes on a fork vertex land in the overlay, merged over the untouched fork state; a deleted
// vertex refuses them.
TEST_F(VersioningBranchEngineTest, OverlayLabelAndPropertyWritesMergeOverTheForkState) {
  const auto label_a = storage_->NameToLabel("A");
  const auto label_b = storage_->NameToLabel("B");
  const auto prop_x = storage_->NameToProperty("x");
  const auto prop_y = storage_->NameToProperty("y");
  ms::Gid v_gid, w_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(v.AddLabel(label_a).has_value());
    ASSERT_TRUE(v.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    w_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Fork();
  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff_acc.get());

  ASSERT_TRUE(branch_context_->PrepareForkWrite(v_gid).has_value());
  auto fork = *branch_context_->FindForkVertex(v_gid);
  auto labels = [&] {
    return mv::MergeLabels(*branch_context_->ReadOverlayVersion(v_gid, ms::View::NEW), *fork.Labels(ms::View::OLD));
  };
  auto props = [&] { return *mv::MergeProperties(*branch_context_->ReadOverlayVersion(v_gid, ms::View::NEW), fork); };
  using Props = std::map<ms::PropertyId, ms::PropertyValue>;

  EXPECT_FALSE(*branch_context_->OverlayAddLabel(v_gid, label_a)) << "the fork vertex already has A";
  EXPECT_TRUE(*branch_context_->OverlayRemoveLabel(v_gid, label_a));
  EXPECT_TRUE(*branch_context_->OverlayAddLabel(v_gid, label_b));
  EXPECT_EQ(labels(), (ms::VertexKey{label_b}));
  EXPECT_TRUE(*branch_context_->OverlayAddLabel(v_gid, label_a)) << "un-hides the fork label";
  EXPECT_TRUE(*branch_context_->OverlayRemoveLabel(v_gid, label_b)) << "drops the added label";
  EXPECT_FALSE(*branch_context_->OverlayRemoveLabel(v_gid, label_b));
  EXPECT_EQ(labels(), (ms::VertexKey{label_a}));
  auto const *version = branch_context_->ReadOverlayVersion(v_gid, ms::View::NEW);
  EXPECT_TRUE(version->added_labels().empty() && version->removed_labels().empty()) << "a full toggle leaves no trace";

  EXPECT_EQ(*branch_context_->OverlaySetProperty(v_gid, prop_x, ms::PropertyValue(2)), ms::PropertyValue(1));
  EXPECT_EQ(*branch_context_->OverlaySetProperty(v_gid, prop_x, ms::PropertyValue()), ms::PropertyValue(2));
  EXPECT_TRUE(props().empty()) << "a null write removes the fork property";
  Props init{{prop_y, ms::PropertyValue(5)}};
  EXPECT_TRUE(*branch_context_->OverlayInitProperties(v_gid, init));
  EXPECT_FALSE(*branch_context_->OverlayInitProperties(v_gid, init)) << "init needs an empty vertex";
  Props update{{prop_x, ms::PropertyValue(3)}, {prop_y, ms::PropertyValue(6)}};
  auto changes = *branch_context_->OverlayUpdateProperties(v_gid, update);
  ASSERT_EQ(changes.size(), 2u);
  EXPECT_TRUE(std::get<1>(changes[0]).IsNull()) << "x was removed before the update";
  EXPECT_EQ(std::get<1>(changes[1]), ms::PropertyValue(5));
  const Props updated{{prop_x, ms::PropertyValue(3)}, {prop_y, ms::PropertyValue(6)}};
  EXPECT_EQ(props(), updated);
  EXPECT_EQ(*branch_context_->OverlayClearProperties(v_gid), updated);
  EXPECT_TRUE(props().empty());
  auto x_fork = fork.GetProperty(prop_x, ms::View::OLD);
  EXPECT_EQ(*x_fork, ms::PropertyValue(1)) << "the fork state itself is unchanged";

  EXPECT_FALSE(
      branch_context_->OverlaySetProperty(ms::Gid::FromUint(1'000'000), prop_x, ms::PropertyValue(1)).has_value());
  ASSERT_TRUE(*branch_context_->OverlayMarkDeleted(w_gid));
  auto refused = branch_context_->OverlayAddLabel(w_gid, label_a);
  ASSERT_FALSE(refused.has_value());
  EXPECT_EQ(refused.error(), ms::Error::DELETED_OBJECT);
  EXPECT_EQ(branch_context_->OverlaySetProperty(w_gid, prop_x, ms::PropertyValue(1)).error(),
            ms::Error::DELETED_OBJECT);
}

// Branch-native vertex and edge gids come from main's counters: at or above the fork floors, never reused by main or
// another branch.
TEST_F(VersioningBranchEngineTest, NativeGidsComeFromMainCountersAndNeverCollide) {
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid fork_vertex_gid, fork_edge_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    fork_vertex_gid = a.Gid();
    auto e = acc->CreateEdge(&a, &b, edge_type);
    ASSERT_TRUE(e.has_value());
    fork_edge_gid = e->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Fork();
  auto second = mv::BranchContext::BuildFromFork(*mem_storage_, *fork_ts_);
  ASSERT_TRUE(second.has_value());

  EXPECT_TRUE(branch_context_->IsForkVertexGid(fork_vertex_gid));
  EXPECT_TRUE(branch_context_->IsForkEdgeGid(fork_edge_gid));
  const auto vertex_floor = Core().vertex_gid_floor();
  const auto edge_floor = Core().edge_gid_floor();

  auto diff1 = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff1.get());
  auto diff2 = (*second)->diff_engine().Access(ms::WRITE);
  ASSERT_TRUE((*second)->BeginTransaction(diff2.get()).has_value());

  auto v1a = branch_context_->CreateNativeVertex();
  auto v1b = branch_context_->CreateNativeVertex();
  auto e1 = branch_context_->CreateNativeEdge(&v1a, &v1b, edge_type);
  ASSERT_TRUE(e1.has_value());
  auto v2a = (*second)->CreateNativeVertex();
  auto v2b = (*second)->CreateNativeVertex();
  auto e2 = (*second)->CreateNativeEdge(&v2a, &v2b, edge_type);
  ASSERT_TRUE(e2.has_value());

  ms::Gid main_vertex_gid, main_edge_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    main_vertex_gid = a.Gid();
    auto e = acc->CreateEdge(&a, &b, edge_type);
    ASSERT_TRUE(e.has_value());
    main_edge_gid = e->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const std::vector<ms::Gid> vertex_gids{v1a.Gid(), v1b.Gid(), v2a.Gid(), v2b.Gid(), main_vertex_gid};
  const std::vector<ms::Gid> edge_gids{e1->Gid(), e2->Gid(), main_edge_gid};
  for (const auto gid : vertex_gids) {
    EXPECT_GE(gid.AsUint(), vertex_floor);
    EXPECT_EQ(std::ranges::count(vertex_gids, gid), 1) << "vertex gid " << gid.AsUint() << " allocated twice";
  }
  for (const auto gid : edge_gids) {
    EXPECT_GE(gid.AsUint(), edge_floor);
    EXPECT_EQ(std::ranges::count(edge_gids, gid), 1) << "edge gid " << gid.AsUint() << " allocated twice";
  }
  EXPECT_FALSE(branch_context_->IsForkVertexGid(v1a.Gid()));
  EXPECT_FALSE(branch_context_->IsForkEdgeGid(e1->Gid()));

  EXPECT_TRUE(diff1->FindVertex(v1a.Gid(), ms::View::NEW).has_value());
  EXPECT_FALSE(diff1->FindVertex(v2a.Gid(), ms::View::NEW).has_value()) << "branches must not see each other";
  EXPECT_TRUE(diff2->FindVertex(v2a.Gid(), ms::View::NEW).has_value());
  EXPECT_FALSE(diff2->FindVertex(main_vertex_gid, ms::View::NEW).has_value())
      << "main's later create is not in a branch";
  {
    auto acc = storage_->Access(ms::READ);
    EXPECT_TRUE(acc->FindVertex(main_vertex_gid, ms::View::NEW).has_value());
    EXPECT_FALSE(acc->FindVertex(v1a.Gid(), ms::View::NEW).has_value()) << "branch creates are not in main";
  }

  ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  ASSERT_TRUE((*second)->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  (*second)->EndTransaction();
}

// T2 then T3 attach an edge to fork vertex V; T3 commits, T1 begins, then T2 commits. T2's delta is buried under T3's
// head (committed before T1's snapshot), so T1's delete of V conflicts only if CanDeleteAnchor walks the whole chain.
TEST_F(VersioningBranchEngineTest, DeleteForkVertexConflictsWithEdgeBuriedUnderOlderCommittedHead) {
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid v_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    v_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Fork();
  mv::BranchContext &deleter = *branch_context_;
  mv::BranchContext creator_early{deleter.core()};
  mv::BranchContext creator_late{deleter.core()};

  auto attach_edge = [&](mv::BranchContext &ctx) {
    auto anchor = ctx.CowVertex(v_gid);
    ASSERT_TRUE(anchor.has_value());
    auto native = ctx.CreateNativeVertex();
    ASSERT_TRUE(ctx.CreateNativeEdge(&*anchor, &native, edge_type).has_value());
  };

  auto t2 = deleter.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(creator_early.BeginTransaction(t2.get()).has_value());
  attach_edge(creator_early);

  auto t3 = deleter.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(creator_late.BeginTransaction(t3.get()).has_value());
  attach_edge(creator_late);
  ASSERT_TRUE(creator_late.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  creator_late.EndTransaction();

  auto t1 = deleter.diff_engine().Access(ms::WRITE);
  Begin(t1.get());

  ASSERT_TRUE(creator_early.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  creator_early.EndTransaction();

  auto deleted = deleter.OverlayMarkDeleted(v_gid);
  ASSERT_FALSE(deleted.has_value()) << "the delete must not pass over an edge committed after its snapshot";
  EXPECT_EQ(deleted.error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(t1->TransactionHasSerializationError());
  t1->Abort();
  deleter.EndTransaction();
}

// A fork-vertex delete is an overlay write: hidden at View::NEW but not View::OLD within the deleting command; an
// abort leaves no marker; a committed delete stays hidden to later transactions.
TEST_F(VersioningBranchEngineTest, DeleteForkVertexMarkerFollowsMvcc) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    b_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Fork();

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    ASSERT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));

    auto deleted = branch_context_->OverlayMarkDeleted(a_gid);
    ASSERT_TRUE(deleted.has_value() && *deleted);
    EXPECT_TRUE(Core().MayHaveVertexDeleted(a_gid));
    EXPECT_TRUE(Core().MayHaveEdgeChange(a_gid));

    EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));
    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::OLD));
    EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
    EXPECT_TRUE(branch_context_->ResolveVertex(a_gid, ms::View::OLD).has_value());

    auto again = branch_context_->OverlayMarkDeleted(a_gid);
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(*again) << "a repeated delete is a no-op";

    diff_acc->Abort();
    branch_context_->EndTransaction();
  }

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW)) << "an aborted delete must leave no marker";
    ASSERT_TRUE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
    auto x_val = branch_context_->FindForkVertex(a_gid)->GetProperty(prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(1));
    std::vector<ms::Gid> scanned;
    for (auto v : branch_context_->Vertices(ms::View::NEW)) scanned.push_back(v.Gid());
    EXPECT_EQ(scanned, (std::vector<ms::Gid>{a_gid, b_gid})) << "the union scan must see the vertex again post-abort";

    auto deleted = branch_context_->OverlayMarkDeleted(a_gid);
    ASSERT_TRUE(deleted.has_value() && *deleted);
    ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    branch_context_->EndTransaction();
  }

  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW))
      << "a committed delete must stay visible to a later transaction";
  EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
  EXPECT_TRUE(branch_context_->ResolveVertex(b_gid, ms::View::NEW).has_value());
  std::vector<ms::Gid> scanned;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) scanned.push_back(v.Gid());
  EXPECT_EQ(scanned, std::vector<ms::Gid>{b_gid}) << "the union scan must skip the deleted fork vertex";
}

// Fork-edge delete after a property write: hidden at View::NEW but not OLD within the deleting command, sets main's
// branched bit, hidden at both views later. A self-loop is recorded once and excluded from OUT and IN resolution.
TEST_F(VersioningBranchEngineTest, DeleteForkEdgeHiddenPerViewThenCommitted) {
  const auto prop_w = storage_->NameToProperty("w");
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid s_gid, a_gid, u_gid, m_gid, edge_gid, loop_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto s = acc->CreateVertex();
    s_gid = s.Gid();
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    auto u = acc->CreateVertex();
    u_gid = u.Gid();
    auto edge = acc->CreateEdge(&s, &a, edge_type);
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(prop_w, ms::PropertyValue(1)).has_value());
    auto m = acc->CreateVertex();
    m_gid = m.Gid();
    auto p1 = acc->CreateVertex();
    auto p2 = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&m, &p1, edge_type).has_value());
    ASSERT_TRUE(acc->CreateEdge(&m, &p2, edge_type).has_value());
    auto loop = acc->CreateEdge(&u, &u, edge_type);
    ASSERT_TRUE(loop.has_value());
    loop_gid = loop->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Fork();

  // Fork edges bound independently of the branch context.
  auto independent_hist = mem_storage_->HistoricalAccess(*fork_ts_);
  ASSERT_TRUE(independent_hist.has_value());
  auto out_edges_of = [&](ms::Gid gid) {
    auto out = (*independent_hist)->FindVertex(gid, ms::View::OLD)->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    return std::vector<ms::EdgeAccessor>(out->edges.begin(), out->edges.end());
  };
  auto const s_out = out_edges_of(s_gid);
  ASSERT_EQ(s_out.size(), 1u);
  const ms::EdgeAccessor fork_edge = s_out[0];
  auto const u_out = out_edges_of(u_gid);
  ASSERT_EQ(u_out.size(), 1u);
  const ms::EdgeAccessor fork_loop = u_out[0];
  ASSERT_EQ(fork_loop.Gid(), loop_gid);
  auto m_edges = out_edges_of(m_gid);
  ASSERT_EQ(m_edges.size(), 2u);
  std::ranges::sort(m_edges, [](const auto &l, const auto &r) { return l.Gid() > r.Gid(); });
  EXPECT_FALSE(fork_loop.MaybeBranchedHint()) << "an untouched fork edge is not branched";

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    ASSERT_TRUE(branch_context_->PrepareForkEdgeWrite(fork_edge).has_value());
    ASSERT_TRUE(branch_context_->EdgeOverlaySetProperty(fork_edge, prop_w, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    branch_context_->EndTransaction();
  }
  EXPECT_TRUE(fork_edge.MaybeBranchedHint()) << "PrepareForkEdgeWrite must set main's own Edge::branched() bit";

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    ASSERT_NE(branch_context_->ReadEdgeOverlayVersion(edge_gid, ms::View::NEW), nullptr)
        << "the edge write committed by the previous statement must be in the overlay";
    auto deleted = branch_context_->DeleteForkEdge(fork_edge);
    ASSERT_TRUE(deleted.has_value());
    ASSERT_TRUE(*deleted);

    EXPECT_FALSE(branch_context_->FindDiffEdge(edge_gid, ms::View::NEW).has_value()) << "no diff-engine edge, ever";
    EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
    EXPECT_FALSE(branch_context_->IsForkEdgeHidden(fork_edge, ms::View::OLD))
        << "the deleting command's own delete is invisible at View::OLD";
    EXPECT_TRUE(Core().MayHaveForkEdgeDeleted(edge_gid));
    EXPECT_TRUE(Core().MayHaveEdgeChange(s_gid) && Core().MayHaveEdgeChange(a_gid));

    auto again = branch_context_->DeleteForkEdge(fork_edge);
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(*again) << "a repeated delete is a no-op";

    auto loop_deleted = branch_context_->DeleteForkEdge(fork_loop);
    ASSERT_TRUE(loop_deleted.has_value());
    EXPECT_TRUE(*loop_deleted);
    EXPECT_TRUE(fork_loop.MaybeBranchedHint()) << "DeleteForkEdge must set main's branched bit on the fork edge";
    auto loop_again = branch_context_->DeleteForkEdge(fork_loop);
    ASSERT_TRUE(loop_again.has_value());
    EXPECT_FALSE(*loop_again) << "a self-loop is recorded once";

    EXPECT_FALSE(m_edges[0].MaybeBranchedHint());
    for (const auto &e : m_edges) {
      auto m_deleted = branch_context_->DeleteForkEdge(e);
      ASSERT_TRUE(m_deleted.has_value());
      EXPECT_TRUE(*m_deleted);
    }

    ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    branch_context_->EndTransaction();
  }

  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  for (auto view : {ms::View::NEW, ms::View::OLD}) {
    EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_edge, view));
    for (const auto &e : m_edges) EXPECT_TRUE(branch_context_->IsForkEdgeHidden(e, view));
  }
  EXPECT_TRUE(branch_context_->ResolveEdges(s_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(m_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_loop, ms::View::NEW));
  EXPECT_TRUE(branch_context_->ResolveEdges(u_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(u_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
}

// Writes to a fork edge are overlay versions: they create no diff-engine edge and no anchor stub, roll back on abort,
// and are visible after commit; the fork edge itself is never changed.
class VersioningBranchForkEdgeTest : public VersioningBranchEngineTest {
 protected:
  void CheckOverlayWritesLeaveNoDiffObjects() {
    const auto prop_w = storage_->NameToProperty("w");
    const auto prop_x = storage_->NameToProperty("x");
    const auto edge_type = storage_->NameToEdgeType("E");
    ms::Gid s_gid, a_gid, edge_gid;
    {
      auto acc = storage_->Access(ms::WRITE);
      auto s = acc->CreateVertex();
      s_gid = s.Gid();
      auto a = acc->CreateVertex();
      a_gid = a.Gid();
      auto edge = acc->CreateEdge(&s, &a, edge_type);
      ASSERT_TRUE(edge.has_value());
      edge_gid = edge->Gid();
      ASSERT_TRUE(edge->SetProperty(prop_w, ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(edge->SetProperty(prop_x, ms::PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    Fork();

    auto fork_edge_of = [&]() {
      auto out = branch_context_->historical().FindVertex(s_gid, ms::View::OLD)->OutEdges(ms::View::OLD);
      EXPECT_TRUE(out.has_value());
      EXPECT_EQ(out->edges.size(), 1u);
      return out->edges[0];
    };
    auto no_stub_anywhere = [&] {
      size_t stubs = 0;
      Core().stub_index().ForEachStub([&](ms::Vertex *) { ++stubs; });
      return stubs == 0 && !branch_context_->FindDiffEdge(edge_gid, ms::View::NEW).has_value();
    };

    {
      auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
      Begin(diff_acc.get());
      auto const fork_edge = fork_edge_of();
      ASSERT_TRUE(branch_context_->PrepareForkEdgeWrite(fork_edge).has_value());
      EXPECT_TRUE(fork_edge.MaybeBranchedHint());
      auto old = branch_context_->EdgeOverlaySetProperty(fork_edge, prop_w, ms::PropertyValue(2));
      ASSERT_TRUE(old.has_value());
      EXPECT_EQ(*old, ms::PropertyValue(1));
      std::map<ms::PropertyId, ms::PropertyValue> remove_x{{prop_x, ms::PropertyValue()}};
      ASSERT_TRUE(branch_context_->EdgeOverlayUpdateProperties(fork_edge, remove_x).has_value());

      auto const *version = branch_context_->ReadEdgeOverlayVersion(edge_gid, ms::View::NEW);
      ASSERT_NE(version, nullptr);
      EXPECT_FALSE(version->deleted());
      EXPECT_EQ(version->properties().GetProperty(prop_w), ms::PropertyValue(2));
      ASSERT_EQ(version->removed_pids().size(), 1u);
      EXPECT_EQ(version->removed_pids()[0], prop_x);
      EXPECT_EQ(branch_context_->OverlayWrittenGids().size(), 0u) << "an edge key is not a vertex gid";
      EXPECT_TRUE(no_stub_anywhere());
      EXPECT_EQ(*fork_edge.GetProperty(prop_w, ms::View::OLD), ms::PropertyValue(1)) << "the fork edge is untouched";
      diff_acc->Abort();
      branch_context_->EndTransaction();
    }
    EXPECT_EQ(Core().overlay_store().size(), 0u) << "an aborted edge write is rolled back";

    {
      auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
      Begin(diff_acc.get());
      auto const fork_edge = fork_edge_of();
      ASSERT_TRUE(branch_context_->PrepareForkEdgeWrite(fork_edge).has_value());
      ASSERT_TRUE(branch_context_->EdgeOverlaySetProperty(fork_edge, prop_w, ms::PropertyValue(3)).has_value());
      ASSERT_TRUE(branch_context_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
      branch_context_->EndTransaction();
    }

    auto read_acc = branch_context_->diff_engine().Access(ms::READ);
    Begin(read_acc.get());
    auto const *version = branch_context_->ReadEdgeOverlayVersion(edge_gid, ms::View::NEW);
    ASSERT_NE(version, nullptr);
    EXPECT_EQ(version->properties().GetProperty(prop_w), ms::PropertyValue(3));
    EXPECT_FALSE(branch_context_->ResolveEdges(s_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
    EXPECT_FALSE(branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
    EXPECT_TRUE(no_stub_anywhere());
  }
};

class VersioningBranchForkEdgeLightEdgeTest : public VersioningBranchForkEdgeTest {
 protected:
  bool LightEdge() const override { return true; }
};

TEST_F(VersioningBranchForkEdgeTest, OverlayWritesCreateNoDiffEngineObjectsAndRollBack) {
  CheckOverlayWritesLeaveNoDiffObjects();
}

TEST_F(VersioningBranchForkEdgeLightEdgeTest, OverlayWritesCreateNoDiffEngineObjectsAndRollBack) {
  CheckOverlayWritesLeaveNoDiffObjects();
}

// Stubs are not transactional: concurrent CoWs that all roll back leave exactly one stub per touched gid, none of them
// in the diff engine's vertices_, and lookups of untouched gids fall back to the fork state.
TEST_F(VersioningBranchEngineTest, StubIndexRecordsTouchedGidsUnderConcurrentCowAndRollback) {
  constexpr size_t kVertices = 300;  // spans several 16-gid pages
  constexpr size_t kThreads = 4;

  std::vector<ms::Gid> gids;
  {
    auto acc = storage_->Access(ms::WRITE);
    for (size_t i = 0; i < kVertices; ++i) gids.push_back(acc->CreateVertex().Gid());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  Fork();
  auto core = branch_context_->core();

  // Even indexes are COWed by every thread (contended); a touched odd index by one thread; the rest stay untouched.
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
        auto cowed = ctx->CowVertex(gids[i]);
        EXPECT_TRUE(cowed.has_value());
      }
      ctx->EndTransaction();
      diff_acc->Abort();
    });
  }
  for (auto &th : threads) th.join();

  auto diff_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(diff_acc.get());

  size_t diff_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++diff_count;
  }
  EXPECT_EQ(diff_count, 0u) << "stubs must not be entries of the diff engine's vertices_";
  size_t expected_count = 0;
  for (size_t i = 0; i < kVertices; ++i) expected_count += is_touched[i] ? 1 : 0;
  size_t stub_count = 0;
  core->stub_index().ForEachStub([&](ms::Vertex *) { ++stub_count; });
  EXPECT_EQ(stub_count, expected_count) << "exactly one stub per touched gid";

  auto &index = core->stub_index();
  for (size_t i = 0; i < kVertices; ++i) {
    const auto gid = gids[i];
    auto diff_vertex = branch_context_->FindStub(gid, ms::View::NEW);
    auto hist_vertex = branch_context_->historical().FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(hist_vertex.has_value());
    if (!is_touched[i]) {
      EXPECT_FALSE(diff_vertex.has_value());
      EXPECT_EQ(index.FindStub(gid.AsUint()), nullptr);
      EXPECT_EQ(index.FindFork(gid.AsUint()), nullptr);
      EXPECT_EQ(branch_context_->FindForkVertex(gid)->vertex_, hist_vertex->vertex_) << "miss falls back";
      continue;
    }
    ASSERT_TRUE(diff_vertex.has_value()) << "a rolled-back COW must leave the stub";
    EXPECT_EQ(index.FindStub(gid.AsUint()), diff_vertex->vertex_);
    EXPECT_EQ(index.FindFork(gid.AsUint()), hist_vertex->vertex_);
    auto fork_via_index = branch_context_->FindForkVertex(gid);
    ASSERT_TRUE(fork_via_index.has_value());
    EXPECT_TRUE(*fork_via_index == *hist_vertex);
  }
}

// Branch transactions over the core fixture's tenant wiring: the diff engine's GC, the overlay's commit paths and its
// GC. Every test starts after a collect, with the unreleased-delta gauge at `baseline_`.
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

  void CommitStubWrite(int64_t value) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto v = ctx_->FindStub(v_gid_, ms::View::NEW);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_x_, ms::PropertyValue(value)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }

  void MaterializeVertexStub() {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(ctx_->CowVertex(v_gid_).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }

  // The one fork edge out of `from_gid`, read through `ctx`'s open transaction.
  static ms::EdgeAccessor ForkEdge(mv::BranchContext &ctx, ms::Gid from_gid) {
    auto out = ctx.historical().FindVertex(from_gid, ms::View::OLD)->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1u);
    return out->edges[0];
  }

  size_t ChainLength(uint64_t key) { return core_->overlay_store().ChainLength(key); }

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

// Another session's open transaction defeats fast-discard, so every committed branch update keeps its delta until GC.
TEST_F(VersioningBranchGcTest, DeltasRetainedBehindOpenTransactionAreFreedByGc) {
  MaterializeVertexStub();
  constexpr int64_t kCommits = 2000;
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 1; i <= kCommits; ++i) CommitStubWrite(i);
    Collect();
    EXPECT_GE(Deltas() - baseline_, kCommits) << "an older open transaction must pin the committed deltas";
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4);

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(*ctx_->FindStub(v_gid_, ms::View::NEW)->GetProperty(prop_x_, ms::View::NEW), ms::PropertyValue(kCommits));
  ctx_->EndTransaction();
}

TEST_F(VersioningBranchGcTest, RolledBackBigTransactionLeavesNoDeltasAfterGc) {
  constexpr int64_t kWrites = 2000;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    // The stub is delta-less, so the abort cannot undo it.
    auto v = ctx_->CowVertex(v_gid_);
    ASSERT_TRUE(v.has_value());
    for (int64_t i = 0; i < kWrites; ++i) {
      acc->CreateVertex();
      ASSERT_TRUE(v->SetProperty(prop_x_, ms::PropertyValue(i)).has_value());
    }
    EXPECT_GE(Deltas() - baseline_, 2 * kWrites);
    acc->Abort();
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4);

  auto acc = Diff().Access(ms::READ);
  size_t natives = 0;
  for (auto v : acc->Vertices(ms::View::NEW)) {
    if (!ctx_->IsForkVertexGid(v.Gid())) ++natives;
  }
  EXPECT_EQ(natives, 0u);
}

// A stub outlives an aborted transaction and GC without being an entry of vertices_: the abort undoes only its deltas,
// GC never retires it, and the branch still finds it and reads the fork state through it.
TEST_F(VersioningBranchGcTest, AbortedStubWriteThenGcKeepsStubOutsideDiffEngineAndForkReadable) {
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto stub = ctx_->CowVertex(v_gid_);
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(stub->SetProperty(prop_x_, ms::PropertyValue(7)).has_value());
    acc->Abort();
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4);

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_EQ(acc->ApproximateVertexCount(), 0U);

  auto stub = ctx_->FindStub(v_gid_, ms::View::NEW);
  ASSERT_TRUE(stub.has_value());
  auto stub_x = stub->GetProperty(prop_x_, ms::View::NEW);
  ASSERT_TRUE(stub_x.has_value());
  EXPECT_TRUE(stub_x->IsNull()) << "the aborted write must be undone on the stub";

  auto fork = ctx_->FindForkVertex(v_gid_);
  ASSERT_TRUE(fork.has_value());
  EXPECT_EQ(*fork->GetProperty(prop_x_, ms::View::OLD), ms::PropertyValue(0));
  EXPECT_TRUE(fork->HasLabel(label_, ms::View::OLD).value_or(false));
  ctx_->EndTransaction();
}

// A deleted fork edge stays hidden once GC collapses its edge versions, and refuses further writes.
TEST_F(VersioningBranchGcTest, CollectedDeletedForkEdgeStaysHidden) {
  for (int64_t value : {7, 8}) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto const fork_edge = ForkEdge(*ctx_, v_gid_);
    ASSERT_TRUE(ctx_->PrepareForkEdgeWrite(fork_edge).has_value());
    ASSERT_TRUE(ctx_->EdgeOverlaySetProperty(fork_edge, prop_x_, ms::PropertyValue(value)).has_value());
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto deleted = ctx_->DeleteForkEdge(ForkEdge(*ctx_, v_gid_));
    ASSERT_TRUE(deleted.has_value());
    EXPECT_TRUE(*deleted);
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  EXPECT_EQ(ChainLength(mv::EdgeKey(edge_gid_.AsUint())), 3U);
  core_->CollectOverlayGarbage();
  core_->CollectOverlayGarbage();
  EXPECT_EQ(ChainLength(mv::EdgeKey(edge_gid_.AsUint())), 1U);

  auto acc = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  auto fork_edge = ForkEdge(*ctx_, v_gid_);
  EXPECT_TRUE(ctx_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
  EXPECT_TRUE(ctx_->ResolveEdges(v_gid_, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_FALSE(ctx_->PrepareForkEdgeWrite(fork_edge).has_value());
  acc->Abort();
  ctx_->EndTransaction();
}

// An overlay-only commit takes a timestamp from the diff engine's clock that no diff commit owns: it must still be
// finished in the commit log, or the diff engine's GC would stall behind it.
TEST_F(VersioningBranchGcTest, OverlayOnlyCommitsAreVisibleAndDoNotStallDiffEngineGc) {
  constexpr int64_t kCommits = 50;
  for (int64_t i = 1; i <= kCommits; ++i) {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(ctx_->PrepareForkWrite(v_gid_).has_value());
    ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(i)).has_value());
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  MaterializeVertexStub();
  for (int64_t i = 1; i <= 20; ++i) CommitStubWrite(i);
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4) << "the diff engine's deltas must still be collectable";

  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  auto const *overlay = ctx_->ReadOverlayVersion(v_gid_, ms::View::NEW);
  ASSERT_NE(overlay, nullptr);
  EXPECT_EQ(overlay->properties().GetProperty(prop_x_), ms::PropertyValue(kCommits));
  ctx_->EndTransaction();
}

// A mixed transaction (overlay write + diff-engine create) commits or aborts both halves together.
TEST_F(VersioningBranchGcTest, MixedTransactionCommitsAndAbortsOverlayAndDiffTogether) {
  ms::Gid native, aborted_native;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(ctx_->PrepareForkWrite(v_gid_).has_value());
    ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(5)).has_value());
    aborted_native = ctx_->CreateNativeVertex().Gid();
    acc->Abort();
    ctx_->EndTransaction();
  }
  {
    auto acc = Diff().Access(ms::READ);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    EXPECT_FALSE(ctx_->HasVertexOverlay(v_gid_)) << "an aborted overlay write must be rolled back";
    EXPECT_EQ(core_->overlay_store().size(), 0U);
    EXPECT_FALSE(ctx_->FindStub(aborted_native, ms::View::NEW).has_value()) << "an aborted native create must be gone";
    EXPECT_FALSE(acc->FindVertex(aborted_native, ms::View::NEW).has_value());
    ctx_->EndTransaction();
  }
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(ctx_->PrepareForkWrite(v_gid_).has_value());
    ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(6)).has_value());
    native = ctx_->CreateNativeVertex().Gid();
    ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  EXPECT_TRUE(ctx_->FindStub(native, ms::View::NEW).has_value());
  auto const *overlay = ctx_->ReadOverlayVersion(v_gid_, ms::View::NEW);
  ASSERT_NE(overlay, nullptr);
  EXPECT_EQ(overlay->properties().GetProperty(prop_x_), ms::PropertyValue(6));
  ctx_->EndTransaction();
}

// Two branch transactions writing one fork vertex conflict, and the loser's diff transaction is poisoned like a storage
// serialization error; the overlay of a concurrent snapshot reader is unaffected.
TEST_F(VersioningBranchGcTest, ConcurrentOverlayWritersConflictAndReadersKeepTheirSnapshot) {
  mv::BranchContext other{core_};
  auto first = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(first.get()).has_value());
  auto second = Diff().Access(ms::WRITE);
  ASSERT_TRUE(other.BeginTransaction(second.get()).has_value());
  auto reader = Diff().Access(ms::READ);
  mv::BranchContext reading{core_};
  ASSERT_TRUE(reading.BeginTransaction(reader.get()).has_value());

  ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(1)).has_value());
  auto conflict = other.OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(2));
  ASSERT_FALSE(conflict.has_value());
  EXPECT_EQ(conflict.error(), ms::Error::SERIALIZATION_ERROR);
  EXPECT_TRUE(second->TransactionHasSerializationError());

  ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  EXPECT_FALSE(reading.HasVertexOverlay(v_gid_)) << "a snapshot older than the commit must not see it";
  other.EndTransaction();
  reading.EndTransaction();
  ctx_->EndTransaction();
}

// A DETACH DELETE of a fork endpoint checks its incident fork edges: one deleted by an in-flight transaction conflicts
// and poisons the checker; the deleter's own delete does not.
TEST_F(VersioningBranchGcTest, CanDeleteForkEdgeConflictsWithAnInFlightEdgeDelete) {
  mv::BranchContext other{core_};
  auto first = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(first.get()).has_value());
  auto second = Diff().Access(ms::WRITE);
  ASSERT_TRUE(other.BeginTransaction(second.get()).has_value());

  EXPECT_TRUE(other.CanDeleteForkEdge(edge_gid_)) << "no edge delete recorded yet";
  auto deleted = ctx_->DeleteForkEdge(ForkEdge(*ctx_, v_gid_));
  ASSERT_TRUE(deleted.has_value() && *deleted);
  EXPECT_TRUE(ctx_->CanDeleteForkEdge(edge_gid_));
  EXPECT_FALSE(first->TransactionHasSerializationError());
  EXPECT_FALSE(other.CanDeleteForkEdge(edge_gid_));
  EXPECT_TRUE(second->TransactionHasSerializationError());

  ASSERT_TRUE(ctx_->CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  other.EndTransaction();
  ctx_->EndTransaction();
}

// A periodic commit publishes each batch's overlay writes (overlay-only or mixed) and continues in a fresh overlay
// transaction; overlay GC keeps every version an open snapshot may still read.
TEST_F(VersioningBranchGcTest, PeriodicCommitPublishesEachBatchAndGcKeepsPinnedSnapshots) {
  auto reader = Diff().Access(ms::READ);
  mv::BranchContext reading{core_};
  ASSERT_TRUE(reading.BeginTransaction(reader.get()).has_value());

  ms::Gid native;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    for (int64_t batch = 1; batch <= 3; ++batch) {
      ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(batch)).has_value());
      if (batch == 2) native = ctx_->CreateNativeVertex().Gid();
      ASSERT_TRUE(ctx_->PeriodicCommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    ASSERT_TRUE(ctx_->OverlaySetProperty(v_gid_, prop_x_, ms::PropertyValue(4)).has_value());
    acc->Abort();
    ctx_->EndTransaction();
  }
  EXPECT_EQ(ChainLength(v_gid_.AsUint()), 3U) << "one version per committed batch; the aborted one is unlinked";
  core_->CollectOverlayGarbage();
  core_->CollectOverlayGarbage();
  EXPECT_EQ(ChainLength(v_gid_.AsUint()), 3U) << "the reader's snapshot predates every batch";
  EXPECT_FALSE(reading.HasVertexOverlay(v_gid_));
  reading.EndTransaction();

  core_->CollectOverlayGarbage();
  core_->CollectOverlayGarbage();
  EXPECT_EQ(ChainLength(v_gid_.AsUint()), 1U);
  auto acc = Diff().Access(ms::READ);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  auto const *overlay = ctx_->ReadOverlayVersion(v_gid_, ms::View::NEW);
  ASSERT_NE(overlay, nullptr);
  EXPECT_EQ(overlay->properties().GetProperty(prop_x_), ms::PropertyValue(3));
  EXPECT_TRUE(ctx_->FindStub(native, ms::View::NEW).has_value()) << "the mixed batch's native create committed";
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
  MaterializeVertexStub();
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 1; i <= 500; ++i) CommitStubWrite(i);
    EXPECT_GE(Deltas() - baseline_, 500);
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (Deltas() - baseline_ > 4 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_LE(Deltas() - baseline_, 4);
}

}  // namespace
