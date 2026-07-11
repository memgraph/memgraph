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

// Graph Versioning CHUNK 5a: the production branch OVERLAY CONTAINER (B1/R38).
//
// BranchOverlay::Materialize replays a branch's forward change-log (chunk 3a's WalDeltaData
// records, hand-built here rather than round-tripped through a real BranchLog file -- that
// round-trip is already covered by versioning_branch_log.cpp) against a fork-state base (chunk
// 4's HistoricalAccess) to build a delta-scale private store of the branch's created/modified/
// deleted objects. The one invariant this whole suite is really checking is R35: main's own
// Vertex/Edge objects must never be mutated by Materialize -- COW means "copy fork-state OUT",
// never "modify the original in place".

#include <algorithm>
#include <expected>
#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_overlay.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;
using WalDeltaData = ms::durability::WalDeltaData;

namespace {

class VersioningBranchOverlayTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    CloseFork();
    storage_.reset();
  }

  using HistoricalResult =
      std::expected<std::unique_ptr<ms::Storage::Accessor>, ms::InMemoryStorage::HistoricalAccessError>;

  // Registers a fork pin at the current tip and opens a HistoricalAccess accessor there. Must be
  // called after any pre-fork base-graph writes have been committed, and before Materialize().
  ms::Storage::Accessor &OpenFork() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    historical_ = mem_storage_->HistoricalAccess(*fork_ts_);
    EXPECT_TRUE(historical_->has_value());
    return ***historical_;
  }

  // Releases a fork session opened via OpenFork(). Idempotent (safe to call from TearDown even if
  // a test already closed its own fork, or never opened one). Tests that need MORE THAN ONE
  // sequential fork session (e.g. to exercise two independent Materialize() calls) must call this
  // between sessions -- otherwise OpenFork()'s second call overwrites fork_ts_/historical_ and the
  // first session's pin is leaked until process exit.
  void CloseFork() {
    if (historical_ && historical_->has_value()) (*historical_)->reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    historical_.reset();
    fork_ts_.reset();
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::optional<HistoricalResult> historical_;
};

// End-to-end materialize covering every op kind chunk 5a is scoped to handle: vertex create/
// modify(COW)/delete(tombstone), edge create/modify(COW), all cross-checked against R35 (main
// untouched) and the read-surface (LookupVertex/LookupEdge/LookupEdgeEndpoints/DeletedVertices).
TEST_F(VersioningBranchOverlayTest, MaterializeAppliesChangelogAndNeverTouchesMain) {
  const auto label_l1 = storage_->NameToLabel("L1");
  const auto label_l2 = storage_->NameToLabel("L2");
  const auto label_l3 = storage_->NameToLabel("L3");
  const auto prop_p = storage_->NameToProperty("p");
  const auto prop_q = storage_->NameToProperty("q");
  const auto prop_w0 = storage_->NameToProperty("w0");

  // Pre-fork base graph.
  ms::Gid v1_gid, v2_gid, v5_gid, e0_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.AddLabel(label_l1).has_value());
    ASSERT_TRUE(v1.SetProperty(prop_p, ms::PropertyValue(1)).has_value());

    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    ASSERT_TRUE(v2.AddLabel(label_l2).has_value());
    ASSERT_TRUE(v2.SetProperty(prop_q, ms::PropertyValue(2)).has_value());

    auto v5 = acc->CreateVertex();
    v5_gid = v5.Gid();

    auto edge_type_e0 = acc->NameToEdgeType("E0");
    auto e0 = acc->CreateEdge(&v1, &v2, edge_type_e0);
    ASSERT_TRUE(e0.has_value());
    e0_gid = e0->Gid();
    ASSERT_TRUE(e0->SetProperty(prop_w0, ms::PropertyValue(1)).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  fork_ts_ = mem_storage_->RegisterForkPin();
  historical_ = mem_storage_->HistoricalAccess(*fork_ts_);
  ASSERT_TRUE(historical_->has_value());
  auto &fork_base = ***historical_;  // Storage::Accessor&

  auto *mapper = fork_base.GetNameIdMapper();
  const auto v3_gid = ms::Gid::FromUint(1'000'000);  // branch-created, no pre-fork counterpart
  const auto e1_gid = ms::Gid::FromUint(2'000'000);  // branch-created edge

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v3_gid}},
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v3_gid, "p3", ms::ToExternalPropertyValue(ms::PropertyValue(300), mapper)}},
      // Modify a fork-existing vertex's property -- must COW, not touch main's v1.
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(100), mapper)}},
      // Second touch of the same (now overlay-resident) vertex -- must reuse the COW copy, not
      // COW again / clobber the property just set above.
      WalDeltaData{ms::durability::WalVertexAddLabel{v1_gid, "L3"}},
      // Modify a DIFFERENT fork-existing vertex -- fresh COW.
      WalDeltaData{ms::durability::WalVertexRemoveLabel{v2_gid, "L2"}},
      // Tombstone a fork-existing vertex the branch never otherwise touched.
      WalDeltaData{ms::durability::WalVertexDelete{v5_gid}},
      // Branch-created edge between two fork-existing vertices.
      WalDeltaData{ms::durability::WalEdgeCreate{e1_gid, "E1", v1_gid, v2_gid}},
      WalDeltaData{ms::durability::WalEdgeSetProperty{e1_gid,
                                                      "w1",
                                                      ms::ToExternalPropertyValue(ms::PropertyValue(1), mapper),
                                                      std::nullopt,
                                                      std::nullopt,
                                                      std::nullopt}},
      // Modify a fork-existing edge's property -- must COW, not touch main's e0.
      WalDeltaData{ms::durability::WalEdgeSetProperty{
          e0_gid, "w0", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper), v1_gid, v2_gid, std::string{"E0"}}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);

  // --- v3: branch-created, no COW involved ---
  ms::Vertex *v3 = nullptr;
  EXPECT_EQ(overlay.LookupVertex(v3_gid, &v3), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v3, nullptr);
  EXPECT_EQ(v3->properties.GetProperty(storage_->NameToProperty("p3")), ms::PropertyValue(300));
  EXPECT_TRUE(v3->labels.empty());

  // --- v1: COW'd, property changed + label added ---
  ms::Vertex *v1_overlay = nullptr;
  EXPECT_EQ(overlay.LookupVertex(v1_gid, &v1_overlay), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v1_overlay, nullptr);
  EXPECT_EQ(v1_overlay->properties.GetProperty(prop_p), ms::PropertyValue(100));
  EXPECT_TRUE(std::ranges::contains(v1_overlay->labels, label_l1)) << "COW must preserve fork-state labels";
  EXPECT_TRUE(std::ranges::contains(v1_overlay->labels, label_l3)) << "branch's own AddLabel must also apply";
  EXPECT_EQ(v1_overlay->labels.size(), 2u);

  // --- R35: main's v1 is untouched by the above ---
  {
    auto acc = storage_->Access(ms::READ);
    auto v1_main = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1_main.has_value());
    auto prop = v1_main->GetProperty(prop_p, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(1)) << "R35: overlay COW must never mutate main's live vertex";
    auto labels = v1_main->Labels(ms::View::OLD);
    ASSERT_TRUE(labels.has_value());
    EXPECT_EQ(labels->size(), 1u);
    EXPECT_TRUE(std::ranges::contains(*labels, label_l1));
    EXPECT_FALSE(std::ranges::contains(*labels, label_l3)) << "R35: branch's AddLabel must not leak into main";
  }

  // --- v2: COW'd, label removed ---
  ms::Vertex *v2_overlay = nullptr;
  EXPECT_EQ(overlay.LookupVertex(v2_gid, &v2_overlay), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v2_overlay, nullptr);
  EXPECT_TRUE(v2_overlay->labels.empty()) << "branch's RemoveLabel must apply to the overlay copy";

  // --- R35: main's v2 still has L2 ---
  {
    auto acc = storage_->Access(ms::READ);
    auto v2_main = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v2_main.has_value());
    EXPECT_TRUE(v2_main->HasLabel(label_l2, ms::View::OLD).value_or(false))
        << "R35: overlay COW must never mutate main's live vertex";
  }

  // --- v5: tombstoned, never COW'd (branch never modified it, only deleted it) ---
  EXPECT_EQ(overlay.LookupVertex(v5_gid), mv::BranchOverlay::Status::kTombstoned);
  EXPECT_TRUE(overlay.DeletedVertices().contains(v5_gid));

  // --- an untouched gid is absent, not tombstoned/present ---
  EXPECT_EQ(overlay.LookupVertex(ms::Gid::FromUint(9'999'999)), mv::BranchOverlay::Status::kAbsent);

  // --- e1: branch-created edge ---
  ms::Edge *e1_overlay = nullptr;
  EXPECT_EQ(overlay.LookupEdge(e1_gid, &e1_overlay), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(e1_overlay, nullptr);
  EXPECT_EQ(e1_overlay->properties.GetProperty(storage_->NameToProperty("w1")), ms::PropertyValue(1));
  auto *e1_endpoints = overlay.LookupEdgeEndpoints(e1_gid);
  ASSERT_NE(e1_endpoints, nullptr);
  EXPECT_EQ(e1_endpoints->edge_type, storage_->NameToEdgeType("E1"));
  EXPECT_EQ(e1_endpoints->from_vertex, v1_gid);
  EXPECT_EQ(e1_endpoints->to_vertex, v2_gid);

  // --- e0: COW'd fork-existing edge ---
  ms::Edge *e0_overlay = nullptr;
  EXPECT_EQ(overlay.LookupEdge(e0_gid, &e0_overlay), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(e0_overlay, nullptr);
  EXPECT_EQ(e0_overlay->properties.GetProperty(prop_w0), ms::PropertyValue(2));
  auto *e0_endpoints = overlay.LookupEdgeEndpoints(e0_gid);
  ASSERT_NE(e0_endpoints, nullptr);
  EXPECT_EQ(e0_endpoints->edge_type, storage_->NameToEdgeType("E0"));
  EXPECT_EQ(e0_endpoints->from_vertex, v1_gid);
  EXPECT_EQ(e0_endpoints->to_vertex, v2_gid);

  // --- R35: main's e0 is untouched ---
  {
    auto acc = storage_->Access(ms::READ);
    auto e0_main = acc->FindEdge(e0_gid, ms::View::OLD);
    ASSERT_TRUE(e0_main.has_value());
    auto prop = e0_main->GetProperty(prop_w0, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(1)) << "R35: overlay COW must never mutate main's live edge";
  }
}

// Fix #1 (High/latent, logic-verifier): a delete followed later in the SAME pass by a create of
// the SAME gid must clear the tombstone -- the recreated object must read back as kPresent with
// its fresh data, not permanently kTombstoned. Unreachable via today's monotonic gid allocation,
// but a later chunk (merge/rebase/undo) replaying a delete+recreate pair for one gid must not
// regress into a dead overlay entry.
TEST_F(VersioningBranchOverlayTest, RecreateAfterTombstoneIsPresentNotTombstoned) {
  auto &fork_base = OpenFork();  // Empty main: every gid below is branch-only.
  auto *mapper = fork_base.GetNameIdMapper();

  const auto v_gid = ms::Gid::FromUint(42);
  const auto va_gid = ms::Gid::FromUint(43);
  const auto vb_gid = ms::Gid::FromUint(44);
  const auto e_gid = ms::Gid::FromUint(45);

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_gid}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v_gid, "x", ms::ToExternalPropertyValue(ms::PropertyValue(1), mapper)}},
      WalDeltaData{ms::durability::WalVertexDelete{v_gid}},
      WalDeltaData{ms::durability::WalVertexCreate{v_gid}},  // recreate: must clear the tombstone
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v_gid, "x", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper)}},
      WalDeltaData{ms::durability::WalVertexCreate{va_gid}},
      WalDeltaData{ms::durability::WalVertexCreate{vb_gid}},
      WalDeltaData{ms::durability::WalEdgeCreate{e_gid, "ET", va_gid, vb_gid}},
      WalDeltaData{ms::durability::WalEdgeSetProperty{e_gid,
                                                      "y",
                                                      ms::ToExternalPropertyValue(ms::PropertyValue(1), mapper),
                                                      std::nullopt,
                                                      std::nullopt,
                                                      std::nullopt}},
      WalDeltaData{ms::durability::WalEdgeDelete{e_gid}},
      WalDeltaData{ms::durability::WalEdgeCreate{e_gid, "ET2", va_gid, vb_gid}},  // recreate
      WalDeltaData{ms::durability::WalEdgeSetProperty{e_gid,
                                                      "y",
                                                      ms::ToExternalPropertyValue(ms::PropertyValue(5), mapper),
                                                      std::nullopt,
                                                      std::nullopt,
                                                      std::nullopt}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);

  ms::Vertex *v = nullptr;
  EXPECT_EQ(overlay.LookupVertex(v_gid, &v), mv::BranchOverlay::Status::kPresent)
      << "recreate after delete must clear the tombstone";
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->properties.GetProperty(storage_->NameToProperty("x")), ms::PropertyValue(2))
      << "must reflect the RECREATED vertex's data, not the deleted one's";
  EXPECT_FALSE(overlay.DeletedVertices().contains(v_gid));

  ms::Edge *e = nullptr;
  EXPECT_EQ(overlay.LookupEdge(e_gid, &e), mv::BranchOverlay::Status::kPresent)
      << "recreate after delete must clear the tombstone";
  ASSERT_NE(e, nullptr);
  EXPECT_FALSE(overlay.DeletedEdges().contains(e_gid));
  auto *endpoints = overlay.LookupEdgeEndpoints(e_gid);
  ASSERT_NE(endpoints, nullptr);
  EXPECT_EQ(endpoints->edge_type, storage_->NameToEdgeType("ET2"))
      << "must reflect the RECREATED edge, not the deleted one";
}

// Fix #2 (Risky, logic-verifier): a mutation targeting a gid that is CURRENTLY tombstoned (deleted
// and not since recreated) has no live object left to mutate. Picked behavior: treat it as the
// same class of change-log anomaly as a duplicate create -- throw utils::BasicException rather
// than silently re-COW a now-unreachable fork-state object.
TEST_F(VersioningBranchOverlayTest, MutateAfterDeleteWithoutRecreateThrowsForVertex) {
  ms::Gid v1_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.SetProperty(storage_->NameToProperty("p"), ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto &fork_base = OpenFork();
  auto *mapper = fork_base.GetNameIdMapper();

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexDelete{v1_gid}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper)}},
  };

  mv::BranchOverlay overlay(mapper);
  EXPECT_THROW(overlay.Materialize(changelog, fork_base), memgraph::utils::BasicException);
}

TEST_F(VersioningBranchOverlayTest, MutateAfterDeleteWithoutRecreateThrowsForEdge) {
  ms::Gid v1_gid, v2_gid, e0_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto e0 = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("E0"));
    ASSERT_TRUE(e0.has_value());
    e0_gid = e0->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto &fork_base = OpenFork();
  auto *mapper = fork_base.GetNameIdMapper();

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalEdgeDelete{e0_gid}},
      WalDeltaData{ms::durability::WalEdgeSetProperty{
          e0_gid, "w0", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper), v1_gid, v2_gid, std::string{"E0"}}},
  };

  mv::BranchOverlay overlay(mapper);
  EXPECT_THROW(overlay.Materialize(changelog, fork_base), memgraph::utils::BasicException);
}

// (c) Repeated SetProperty on the same property must leave the LAST value in place.
TEST_F(VersioningBranchOverlayTest, RepeatedSetPropertySamePropertyFinalValueWins) {
  ms::Gid v1_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.SetProperty(storage_->NameToProperty("p"), ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto &fork_base = OpenFork();
  auto *mapper = fork_base.GetNameIdMapper();

  std::vector<WalDeltaData> changelog{
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper)}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(3), mapper)}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);

  ms::Vertex *v = nullptr;
  ASSERT_EQ(overlay.LookupVertex(v1_gid, &v), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->properties.GetProperty(storage_->NameToProperty("p")), ms::PropertyValue(3));
}

// (c) AddLabel then RemoveLabel of the SAME label on one vertex must leave the label absent.
TEST_F(VersioningBranchOverlayTest, AddThenRemoveSameLabelLeavesItAbsent) {
  auto &fork_base = OpenFork();  // Empty main: v_gid is branch-only.
  auto *mapper = fork_base.GetNameIdMapper();

  const auto v_gid = ms::Gid::FromUint(7);
  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_gid}},
      WalDeltaData{ms::durability::WalVertexAddLabel{v_gid, "Ltemp"}},
      WalDeltaData{ms::durability::WalVertexRemoveLabel{v_gid, "Ltemp"}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);

  ms::Vertex *v = nullptr;
  ASSERT_EQ(overlay.LookupVertex(v_gid, &v), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v, nullptr);
  EXPECT_TRUE(v->labels.empty()) << "AddLabel immediately undone by RemoveLabel must leave no trace";
}

// (d) Create -> SetProperty -> SetProperty on a create-origin (branch-only, not-in-fork_base) gid
// must never consult fork_base. Proven negatively: fork_base has NOTHING at this gid, so if
// EnsureOverlayVertex incorrectly tried to COW it, Materialize would throw; a clean, non-throwing
// materialize with the right final value proves the overlay-resident fast path was taken both
// times.
TEST_F(VersioningBranchOverlayTest, CreateThenSetPropertyTwiceNeverConsultsForkBase) {
  auto &fork_base = OpenFork();  // Empty main.
  auto *mapper = fork_base.GetNameIdMapper();

  const auto v_gid = ms::Gid::FromUint(11);
  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_gid}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(1), mapper)}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper)}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);  // must not throw -- gid isn't in fork_base at all

  ms::Vertex *v = nullptr;
  ASSERT_EQ(overlay.LookupVertex(v_gid, &v), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->properties.GetProperty(storage_->NameToProperty("p")), ms::PropertyValue(2));
}

// (e) Deleting a BRANCH-CREATED vertex/edge (never present in fork_base) must tombstone it without
// ever consulting fork_base -- proven the same negative way as above.
TEST_F(VersioningBranchOverlayTest, DeleteOfBranchCreatedObjectsNeverConsultsForkBase) {
  auto &fork_base = OpenFork();  // Empty main.
  auto *mapper = fork_base.GetNameIdMapper();

  const auto v_gid = ms::Gid::FromUint(21);
  const auto va_gid = ms::Gid::FromUint(22);
  const auto vb_gid = ms::Gid::FromUint(23);
  const auto e_gid = ms::Gid::FromUint(24);

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_gid}},
      WalDeltaData{ms::durability::WalVertexDelete{v_gid}},
      WalDeltaData{ms::durability::WalVertexCreate{va_gid}},
      WalDeltaData{ms::durability::WalVertexCreate{vb_gid}},
      WalDeltaData{ms::durability::WalEdgeCreate{e_gid, "ET", va_gid, vb_gid}},
      WalDeltaData{ms::durability::WalEdgeDelete{e_gid}},
  };

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);  // must not throw

  EXPECT_EQ(overlay.LookupVertex(v_gid), mv::BranchOverlay::Status::kTombstoned);
  EXPECT_EQ(overlay.LookupEdge(e_gid), mv::BranchOverlay::Status::kTombstoned);
}

// (f) Edge resolution tier-2 (from_gid only) and tier-3 (gid only) must actually reach fork_base
// and COW a fork-existing edge, not just tier-1 (all three hints present).
TEST_F(VersioningBranchOverlayTest, EdgeResolutionTier2AndTier3ReachForkBase) {
  ms::Gid v1_gid, v2_gid, e0_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto e0 = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("E0"));
    ASSERT_TRUE(e0.has_value());
    e0_gid = e0->Gid();
    ASSERT_TRUE(e0->SetProperty(storage_->NameToProperty("w0"), ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  (void)v2_gid;  // only v1_gid/e0_gid are exercised below (from_gid-only / gid-only resolution)

  // Tier-2: from_gid only.
  {
    auto &fork_base = OpenFork();
    auto *mapper = fork_base.GetNameIdMapper();
    std::vector<WalDeltaData> changelog{WalDeltaData{ms::durability::WalEdgeSetProperty{
        e0_gid, "w0", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper), v1_gid, std::nullopt, std::nullopt}}};
    mv::BranchOverlay overlay(mapper);
    overlay.Materialize(changelog, fork_base);
    ms::Edge *e = nullptr;
    ASSERT_EQ(overlay.LookupEdge(e0_gid, &e), mv::BranchOverlay::Status::kPresent);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->properties.GetProperty(storage_->NameToProperty("w0")), ms::PropertyValue(2))
        << "tier-2 (from_gid only) must reach fork_base.FindEdge(gid, from_gid, view)";
  }
  CloseFork();  // release the first session before opening a second one below.

  // Tier-3: gid only.
  {
    auto &fork_base = OpenFork();
    auto *mapper = fork_base.GetNameIdMapper();
    std::vector<WalDeltaData> changelog{
        WalDeltaData{ms::durability::WalEdgeSetProperty{e0_gid,
                                                        "w0",
                                                        ms::ToExternalPropertyValue(ms::PropertyValue(3), mapper),
                                                        std::nullopt,
                                                        std::nullopt,
                                                        std::nullopt}}};
    mv::BranchOverlay overlay(mapper);
    overlay.Materialize(changelog, fork_base);
    ms::Edge *e = nullptr;
    ASSERT_EQ(overlay.LookupEdge(e0_gid, &e), mv::BranchOverlay::Status::kPresent);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->properties.GetProperty(storage_->NameToProperty("w0")), ms::PropertyValue(3))
        << "tier-3 (gid only) must reach fork_base.FindEdge(gid, view)";
  }
}

// Fix #3 (Risky, logic-verifier): a sentinel to_gid (kInvalidGid) alongside otherwise-complete
// hints must NOT be trusted as tier-1 -- it must fall through to tier-2 (from_gid only) instead of
// spuriously throwing "corrupt change-log" (which is what happens without the guard: FindVertex on
// the sentinel to_gid fails, e_acc stays unset, and the function throws instead of retrying).
TEST_F(VersioningBranchOverlayTest, EdgeResolutionSentinelToGidFallsThroughToTier2) {
  ms::Gid v1_gid, v2_gid, e0_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto e0 = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("E0"));
    ASSERT_TRUE(e0.has_value());
    e0_gid = e0->Gid();
    ASSERT_TRUE(e0->SetProperty(storage_->NameToProperty("w0"), ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  (void)v2_gid;  // deliberately NOT used as to_gid below -- kInvalidGid stands in for it instead

  auto &fork_base = OpenFork();
  auto *mapper = fork_base.GetNameIdMapper();

  // to_gid is the sentinel kInvalidGid even though from_gid and edge_type are both present and
  // individually valid -- the guard must reject this as tier-1 and retry via tier-2 (from_gid).
  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalEdgeSetProperty{e0_gid,
                                                      "w0",
                                                      ms::ToExternalPropertyValue(ms::PropertyValue(9), mapper),
                                                      v1_gid,
                                                      ms::kInvalidGid,
                                                      std::string{"E0"}}}};

  mv::BranchOverlay overlay(mapper);
  overlay.Materialize(changelog, fork_base);  // must NOT throw

  ms::Edge *e = nullptr;
  ASSERT_EQ(overlay.LookupEdge(e0_gid, &e), mv::BranchOverlay::Status::kPresent);
  ASSERT_NE(e, nullptr);
  EXPECT_EQ(e->properties.GetProperty(storage_->NameToProperty("w0")), ms::PropertyValue(9));
}

}  // namespace
