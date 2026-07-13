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

// Graph Versioning CHUNK 6: MERGE unit tests.
//
// MergeBranch is exercised directly against a real InMemoryStorage + hand-built change-logs
// (mirroring versioning_branch_reconstruction.cpp's own fixture style) -- it is not wired into the
// interpreter yet (chunk 7), so there is no CREATE BRANCH / real branch-write session here, only
// the change-log MergeBranch itself consumes.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/merge.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;
using WalDeltaData = ms::durability::WalDeltaData;

namespace {

class VersioningMergeTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  // A raw NameIdMapper* is only reachable through an Accessor (mirrors
  // versioning_branch_reconstruction.cpp's own helper) -- the returned pointer outlives this
  // transient accessor since it points into InMemoryStorage's own long-lived name_id_mapper_.
  ms::NameIdMapper *Mapper() {
    auto acc = storage_->Access(ms::READ);
    return acc->GetNameIdMapper();
  }

  // Registers a fork pin at the CURRENT tip and remembers it for TearDown's release.
  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
};

// Clean merge with no conflicts: a branch-created vertex, a property set on a fork-existing
// vertex, and a create-vertex-then-edge sequence -- all must land on main exactly as a direct
// write would have produced.
TEST_F(VersioningMergeTest, CleanMergeAppliesCreateModifyAndEdge) {
  const auto prop_p = storage_->NameToProperty("p");

  ms::Gid v1_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.SetProperty(prop_p, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  const auto v_new_gid = ms::Gid::FromUint(5'000'000);
  const auto e_new_gid = ms::Gid::FromUint(6'000'000);

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_new_gid}},
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(100), mapper)}},
      WalDeltaData{ms::durability::WalEdgeCreate{e_new_gid, "E1", v1_gid, v_new_gid}},
  };

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value()) << (result.has_value() ? std::string{} : result.error().message);
  EXPECT_TRUE(result->vertex_gid_remap.empty()) << "no collision expected -- gid must be reused as-is";
  EXPECT_TRUE(result->edge_gid_remap.empty());
  EXPECT_EQ(result->vertices_created, 1u);
  EXPECT_EQ(result->edges_created, 1u);
  EXPECT_EQ(result->objects_modified, 1u);

  auto acc = storage_->Access(ms::READ);
  auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  auto props = v1->Properties(ms::View::OLD);
  ASSERT_TRUE(props.has_value());
  EXPECT_EQ(props->at(prop_p), ms::PropertyValue(100));

  EXPECT_TRUE(acc->FindVertex(v_new_gid, ms::View::OLD).has_value());

  auto e_new = acc->FindEdge(e_new_gid, ms::View::OLD);
  ASSERT_TRUE(e_new.has_value());
  EXPECT_EQ(e_new->FromVertex().Gid(), v1_gid);
  EXPECT_EQ(e_new->ToVertex().Gid(), v_new_gid);
}

// D3: main modifies X after the fork point; the branch's own change-log (built against the
// fork-state, unaware of main's edit) also modifies X -- the WHOLE merge must be rejected, main
// must be byte-unchanged, and the branch's fork pin must survive (a subsequent HistoricalAccess
// still succeeds) so the user can fix up the branch and retry.
TEST_F(VersioningMergeTest, ModifyConflictRejectedAtomically) {
  const auto prop_p = storage_->NameToProperty("p");

  ms::Gid vx_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto vx = acc->CreateVertex();
    vx_gid = vx.Gid();
    ASSERT_TRUE(vx.SetProperty(prop_p, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  // Main modifies X AFTER the fork point.
  {
    auto acc = storage_->Access(ms::WRITE);
    auto vx = acc->FindVertex(vx_gid, ms::View::OLD);
    ASSERT_TRUE(vx.has_value());
    ASSERT_TRUE(vx->SetProperty(prop_p, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::vector<WalDeltaData> changelog{WalDeltaData{
      ms::durability::WalVertexSetProperty{vx_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(999), mapper)}}};

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::MergeErrorKind::kModifyConflict);
  ASSERT_EQ(result.error().conflicting_gids.size(), 1u);
  EXPECT_EQ(result.error().conflicting_gids[0], vx_gid);

  {
    auto acc = storage_->Access(ms::READ);
    auto vx = acc->FindVertex(vx_gid, ms::View::OLD);
    ASSERT_TRUE(vx.has_value());
    auto props = vx->Properties(ms::View::OLD);
    ASSERT_TRUE(props.has_value());
    EXPECT_EQ(props->at(prop_p), ms::PropertyValue(2)) << "main must still show ITS OWN post-fork edit, not the "
                                                          "branch's -- a rejected merge must leave main untouched";
  }

  auto historical = mem_storage_->HistoricalAccess(fork_ts);
  EXPECT_TRUE(historical.has_value()) << "R37: a rejected merge must not release the branch's fork pin";
}

// R11 (no crash) / R19 (no silent loss): the branch independently creates a vertex + a vertex + an
// edge whose gids happen to collide with objects main independently created after the fork point
// (both sides drawing new gids from the same post-fork counter range). The merge must neither
// crash (edge gid collision) nor silently drop the colliding vertex -- it must remap to a fresh,
// guaranteed-unique gid, and the create-vertex-then-edge sequence must resolve consistently
// against the REMAPPED endpoints.
TEST_F(VersioningMergeTest, CreateCollisionRemapsInsteadOfCrashingOrLosingData) {
  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  ms::Gid main_v1_gid, main_v2_gid, main_e_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    main_v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    main_v2_gid = v2.Gid();
    auto e = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("MAIN_EDGE"));
    ASSERT_TRUE(e.has_value());
    main_e_gid = e->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The branch's own change-log deliberately reuses main's exact gids (simulating both sides
  // drawing from the same post-fork counter range independently).
  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{main_v1_gid}},
      WalDeltaData{ms::durability::WalVertexCreate{main_v2_gid}},
      WalDeltaData{ms::durability::WalEdgeCreate{main_e_gid, "BRANCH_EDGE", main_v1_gid, main_v2_gid}},
  };

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value()) << (result.has_value() ? std::string{} : result.error().message);

  ASSERT_TRUE(result->vertex_gid_remap.contains(main_v1_gid));
  ASSERT_TRUE(result->vertex_gid_remap.contains(main_v2_gid));
  ASSERT_TRUE(result->edge_gid_remap.contains(main_e_gid));

  const auto remapped_v1 = result->vertex_gid_remap.at(main_v1_gid);
  const auto remapped_v2 = result->vertex_gid_remap.at(main_v2_gid);
  const auto remapped_e = result->edge_gid_remap.at(main_e_gid);

  EXPECT_NE(remapped_v1, main_v1_gid);
  EXPECT_NE(remapped_v2, main_v2_gid);
  EXPECT_NE(remapped_e, main_e_gid);

  auto acc = storage_->Access(ms::READ);
  // Main's ORIGINAL objects must be completely untouched by the remap.
  EXPECT_TRUE(acc->FindVertex(main_v1_gid, ms::View::OLD).has_value());
  EXPECT_TRUE(acc->FindVertex(main_v2_gid, ms::View::OLD).has_value());
  EXPECT_TRUE(acc->FindEdge(main_e_gid, ms::View::OLD).has_value());

  // The branch's remapped objects must ALSO now exist, with the branch-created edge resolved
  // against the REMAPPED vertex endpoints -- a create-vertex-then-edge sequence resolves.
  EXPECT_TRUE(acc->FindVertex(remapped_v1, ms::View::OLD).has_value());
  EXPECT_TRUE(acc->FindVertex(remapped_v2, ms::View::OLD).has_value());

  auto remapped_edge = acc->FindEdge(remapped_e, ms::View::OLD);
  ASSERT_TRUE(remapped_edge.has_value());
  EXPECT_EQ(remapped_edge->FromVertex().Gid(), remapped_v1);
  EXPECT_EQ(remapped_edge->ToVertex().Gid(), remapped_v2);
}

// Main changed OTHER objects (not touched by the branch at all) after the fork point -- the merge
// must still succeed; D3 conflicts are per-object, not "main changed anything at all".
TEST_F(VersioningMergeTest, MergeSucceedsWhenMainOnlyTouchedOtherObjects) {
  const auto prop_p = storage_->NameToProperty("p");

  ms::Gid branch_target_gid, other_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    branch_target_gid = v.Gid();
    auto other = acc->CreateVertex();
    other_gid = other.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto other = acc->FindVertex(other_gid, ms::View::OLD);
    ASSERT_TRUE(other.has_value());
    ASSERT_TRUE(other->SetProperty(prop_p, ms::PropertyValue(42)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::vector<WalDeltaData> changelog{WalDeltaData{ms::durability::WalVertexSetProperty{
      branch_target_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(7), mapper)}}};

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value()) << (result.has_value() ? std::string{} : result.error().message);

  auto acc = storage_->Access(ms::READ);
  auto target = acc->FindVertex(branch_target_gid, ms::View::OLD);
  ASSERT_TRUE(target.has_value());
  auto target_props = target->Properties(ms::View::OLD);
  ASSERT_TRUE(target_props.has_value());
  EXPECT_EQ(target_props->at(prop_p), ms::PropertyValue(7));

  auto other = acc->FindVertex(other_gid, ms::View::OLD);
  ASSERT_TRUE(other.has_value());
  auto other_props = other->Properties(ms::View::OLD);
  ASSERT_TRUE(other_props.has_value());
  EXPECT_EQ(other_props->at(prop_p), ms::PropertyValue(42)) << "main's own independent edit must be untouched";
}

// Atomicity: a change-log whose FIRST two ops would apply cleanly on their own, but whose THIRD op
// conflicts, must leave main EXACTLY as it was before the call -- no partial application of the
// first two ops.
TEST_F(VersioningMergeTest, ConflictPartwayThroughLeavesNoPartialApplication) {
  const auto prop_p = storage_->NameToProperty("p");

  ms::Gid v_ok_gid, v_conflict_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_ok = acc->CreateVertex();
    v_ok_gid = v_ok.Gid();
    auto v_conflict = acc->CreateVertex();
    v_conflict_gid = v_conflict.Gid();
    ASSERT_TRUE(v_conflict.SetProperty(prop_p, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_conflict = acc->FindVertex(v_conflict_gid, ms::View::OLD);
    ASSERT_TRUE(v_conflict.has_value());
    ASSERT_TRUE(v_conflict->SetProperty(prop_p, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto v_new_gid = ms::Gid::FromUint(7'000'000);

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_new_gid}},
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v_ok_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(55), mapper)}},
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v_conflict_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(999), mapper)}},
  };

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::MergeErrorKind::kModifyConflict);

  auto acc = storage_->Access(ms::READ);
  EXPECT_FALSE(acc->FindVertex(v_new_gid, ms::View::OLD).has_value())
      << "the vertex created earlier in the same aborted merge pass must not exist on main";

  auto v_ok = acc->FindVertex(v_ok_gid, ms::View::OLD);
  ASSERT_TRUE(v_ok.has_value());
  auto v_ok_props = v_ok->Properties(ms::View::OLD);
  ASSERT_TRUE(v_ok_props.has_value());
  EXPECT_TRUE(v_ok_props->empty())
      << "v_ok's property set earlier in the same aborted merge pass must not persist on main";
}

// MEDIUM-1 regression: main deletes a fork-existing vertex V after the fork point; the branch's
// own change-log (recorded before it knew about main's deletion) references V only via an edge
// operation (never V's own labels/properties, so V never gets its own D3 pre-check). This must be
// reported as a D3 modify-conflict naming V, NOT as a corrupt change-log naming the edge -- pass 1
// must have snapshotted V's fork-state via the edge op's endpoint alone.
TEST_F(VersioningMergeTest, MissingEdgeEndpointClassifiedAsModifyConflictNotCorrupt) {
  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  // Main deletes v2 AFTER the fork point.
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v2.has_value());
    auto ret = acc->DeleteVertex(&*v2);
    ASSERT_TRUE(ret.has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The branch's own change-log creates a new edge from v1 to v2 -- v2 is referenced ONLY via this
  // edge op, never its own labels/properties.
  const auto e_new_gid = ms::Gid::FromUint(8'000'000);
  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalEdgeCreate{e_new_gid, "E1", v1_gid, v2_gid}},
  };

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::MergeErrorKind::kModifyConflict)
      << "a deleted endpoint must be reported as a D3 modify-conflict naming the vertex, not a "
         "corrupt change-log naming the edge -- got: "
      << result.error().message;
  ASSERT_EQ(result.error().conflicting_gids.size(), 1u);
  EXPECT_EQ(result.error().conflicting_gids[0], v2_gid);

  // Main must be byte-unchanged.
  auto acc = storage_->Access(ms::READ);
  EXPECT_FALSE(acc->FindVertex(v2_gid, ms::View::OLD).has_value());
  EXPECT_FALSE(acc->FindEdge(e_new_gid, ms::View::OLD).has_value());
}

// MEDIUM-2 (targeted) regression: main attaches a NEW edge to a fork-existing vertex the branch
// wants to delete (valid when the branch recorded the delete, since it had no edges then). The
// branch's own labels/properties D3 pre-check sees no diff (V's labels/properties never changed),
// so only DeleteVertex's own VERTEX_HAS_EDGES failure reveals the conflict -- this must surface as
// kModifyConflict, not a generic kApplyFailed.
TEST_F(VersioningMergeTest, DeleteConflictsWhenMainAttachedAnEdgeSinceFork) {
  ms::Gid v_target_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_target = acc->CreateVertex();
    v_target_gid = v_target.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  // Main attaches a NEW edge to v_target AFTER the fork point.
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v_other = acc->CreateVertex();
    auto v_target = acc->FindVertex(v_target_gid, ms::View::OLD);
    ASSERT_TRUE(v_target.has_value());
    auto e = acc->CreateEdge(&*v_target, &v_other, acc->NameToEdgeType("MAIN_EDGE"));
    ASSERT_TRUE(e.has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The branch's own change-log deletes v_target -- valid when it was recorded (no edges then).
  std::vector<WalDeltaData> changelog{WalDeltaData{ms::durability::WalVertexDelete{v_target_gid}}};

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::MergeErrorKind::kModifyConflict)
      << "main attaching a new edge to a vertex the branch wants to delete must be a D3 conflict, "
         "not a generic apply failure -- got: "
      << result.error().message;

  // Main must be byte-unchanged: v_target still exists with its edge intact.
  auto acc = storage_->Access(ms::READ);
  EXPECT_TRUE(acc->FindVertex(v_target_gid, ms::View::OLD).has_value());
}

// S8 regression fixture: identical to VersioningMergeTest except `properties_on_edges = false`
// (light edges) -- a light edge has no delta chain of its own, which is exactly what made D3's old
// edge check (a bare `FindEdge(gid, View::OLD)` scan) unreliable. See merge.cpp's
// FindHistoricalEdgeByEndpoint doc-comment for the full mechanism.
class VersioningMergeLightEdgeTest : public VersioningMergeTest {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.salient.items.properties_on_edges = false;
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }
};

// S8 (HIGH, data-integrity): in light-edge mode, a branch that deletes a fork-existing edge main
// never touched must merge cleanly -- before the fix, `CheckEdgeUnchangedSinceFork`'s
// `FindEdge(gid, View::OLD)` probe came back empty for a perfectly-present light edge (its
// adjacency-vector entry has no delta chain to fall back on), producing a FALSE "main deleted edge"
// conflict and silently discarding the branch's deletion.
TEST_F(VersioningMergeLightEdgeTest, LightEdgeDeleteMergesCleanlyNoFalseConflict) {
  ms::Gid v1_gid, v2_gid, e_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto e = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("R"));
    ASSERT_TRUE(e.has_value());
    e_gid = e->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto *mapper = Mapper();

  // Main changes NOTHING after the fork point. The branch's own change-log deletes the edge.
  std::vector<WalDeltaData> changelog{WalDeltaData{ms::durability::WalEdgeDelete{e_gid, "R", v1_gid, v2_gid}}};

  auto result = mv::MergeBranch(*mem_storage_, fork_ts, changelog, mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value()) << (result.has_value() ? std::string{} : result.error().message);
  EXPECT_EQ(result->objects_deleted, 1u);

  auto acc = storage_->Access(ms::READ);
  EXPECT_FALSE(acc->FindEdge(e_gid, ms::View::OLD).has_value())
      << "the branch's edge deletion must actually land on main, not be silently lost to a false D3 conflict";
  auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
  ASSERT_TRUE(v1.has_value());
  auto out_edges = v1->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out_edges.has_value());
  EXPECT_TRUE(out_edges->edges.empty()) << "v1's adjacency list must no longer contain the deleted edge";
}

}  // namespace
