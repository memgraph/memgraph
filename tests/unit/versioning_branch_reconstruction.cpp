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

// Graph Versioning CHUNK 5b: the branch RECONSTRUCTION read primitive (R20) + the write-CAPTURE
// seam.
//
// Two independent groups of tests live here:
//   - VersioningBranchReconstructionTest: BranchReconstruction::Open/UnionVertices/FindVertex/
//     FindEdge, against a real InMemoryStorage + a hand-built change-log (mirrors
//     versioning_branch_overlay.cpp's own fixture style, one level up the stack). The one
//     invariant every test here ultimately checks is R20 (the union is exactly "branch's view
//     overrides main's fork-state view") plus R35 (main is never touched, and keeps evolving
//     independent of an open reconstruction).
//   - VersioningBranchReconstructionCaptureTest: CaptureBranchCommit driven against a
//     synthetically-built storage::Transaction (mirrors versioning_branch_log.cpp's own
//     TestTransaction fixture, since the real branch-write transaction plumbing is chunk 7's job
//     -- see branch_reconstruction.hpp's class comment "SCOPE / DEFERRED TO CHUNK 7").

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/constraints/existence_constraints.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_log.hpp"
#include "versioning/branch_reconstruction.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;
using WalDeltaData = ms::durability::WalDeltaData;

namespace {

// ---------------------------------------------------------------------------------------------
// Group 1: BranchReconstruction (union read-path).
// ---------------------------------------------------------------------------------------------

class VersioningBranchReconstructionTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    reconstruction_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  // A raw NameIdMapper* is only reachable through an Accessor (Storage itself exposes no direct
  // getter) -- the returned pointer outlives this transient accessor since it points into
  // InMemoryStorage's own long-lived name_id_mapper_.
  ms::NameIdMapper *Mapper() {
    auto acc = storage_->Access(ms::READ);
    return acc->GetNameIdMapper();
  }

  // Registers a fork pin at the current tip and opens a BranchReconstruction over `changelog`.
  mv::BranchReconstruction &OpenReconstruction(const std::vector<WalDeltaData> &changelog) {
    fork_ts_ = mem_storage_->RegisterForkPin();
    auto result = mv::BranchReconstruction::Open(*mem_storage_, *fork_ts_, changelog, Mapper());
    EXPECT_TRUE(result.has_value());
    reconstruction_ = std::move(*result);
    return *reconstruction_;
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchReconstruction> reconstruction_;
};

// R20: the union is a true streaming gid-ordered merge -- created-only overlay rows, COW-modified
// rows (overlay wins), untouched rows (main-as-of-fork), and tombstoned rows (absent entirely) all
// reconcile correctly, and the whole result comes back in strictly ascending gid order (proving
// the merge itself, not just the final set, is correct).
TEST_F(VersioningBranchReconstructionTest, UnionVerticesReconcilesMainAndOverlayInGidOrder) {
  const auto label_l1 = storage_->NameToLabel("L1");
  const auto prop_p = storage_->NameToProperty("p");
  const auto prop_new = storage_->NameToProperty("new");

  ms::Gid v1_gid, v2_gid, v5_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.AddLabel(label_l1).has_value());
    ASSERT_TRUE(v1.SetProperty(prop_p, ms::PropertyValue(1)).has_value());

    auto v2 = acc->CreateVertex();  // untouched control vertex
    v2_gid = v2.Gid();

    auto v5 = acc->CreateVertex();  // to be deleted by the branch
    v5_gid = v5.Gid();

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto *mapper = Mapper();
  const auto v3_gid = ms::Gid::FromUint(1'000'000);  // branch-created, no pre-fork counterpart

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v3_gid}},
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v3_gid, "new", ms::ToExternalPropertyValue(ms::PropertyValue(999), mapper)}},
      // Modify a fork-existing vertex -- overlay must COW + win over main's fork-state copy.
      WalDeltaData{ms::durability::WalVertexSetProperty{
          v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(100), mapper)}},
      // Tombstone a fork-existing vertex the branch never otherwise touched.
      WalDeltaData{ms::durability::WalVertexDelete{v5_gid}},
  };

  auto &reconstruction = OpenReconstruction(changelog);

  std::vector<mv::BranchReconstruction::ReconstructedVertex> rows;
  for (const auto &row : reconstruction.UnionVertices()) {
    rows.push_back(row);
  }

  for (size_t i = 1; i < rows.size(); ++i) {
    EXPECT_LT(rows[i - 1].gid.AsUint(), rows[i].gid.AsUint())
        << "UnionVertices must yield strictly gid-ascending rows (streaming merge, not a set)";
  }

  ASSERT_EQ(rows.size(), 3u) << "v1 (modified) + v2 (untouched) + v3 (created); v5 (deleted) must be absent";

  auto find_gid = [&](ms::Gid gid) {
    return std::ranges::find(rows, gid, &mv::BranchReconstruction::ReconstructedVertex::gid);
  };

  // v1: fork-existing, modified by the branch -- overlay wins, branch's value + fork-state label.
  auto it_v1 = find_gid(v1_gid);
  ASSERT_NE(it_v1, rows.end());
  EXPECT_EQ(it_v1->source, mv::BranchReconstruction::Source::kOverlay);
  ASSERT_TRUE(it_v1->properties.contains(prop_p));
  EXPECT_EQ(it_v1->properties.at(prop_p), ms::PropertyValue(100));
  EXPECT_TRUE(std::ranges::contains(it_v1->labels, label_l1)) << "COW must preserve v1's fork-state label";

  // v2: fork-existing, untouched by the branch -- main-as-of-fork, unmodified.
  auto it_v2 = find_gid(v2_gid);
  ASSERT_NE(it_v2, rows.end());
  EXPECT_EQ(it_v2->source, mv::BranchReconstruction::Source::kMain);
  EXPECT_TRUE(it_v2->properties.empty());
  EXPECT_TRUE(it_v2->labels.empty());

  // v3: branch-created -- overlay only, no fork-state counterpart.
  auto it_v3 = find_gid(v3_gid);
  ASSERT_NE(it_v3, rows.end());
  EXPECT_EQ(it_v3->source, mv::BranchReconstruction::Source::kOverlay);
  ASSERT_TRUE(it_v3->properties.contains(prop_new));
  EXPECT_EQ(it_v3->properties.at(prop_new), ms::PropertyValue(999));

  // v5: tombstoned by the branch -- must not appear in the union at all.
  EXPECT_EQ(find_gid(v5_gid), rows.end());
}

// FindVertex/FindEdge: overlay-first point lookup, covering all four reconciliation states plus a
// gid neither side has ever heard of.
TEST_F(VersioningBranchReconstructionTest, FindVertexAndFindEdgeCoverEveryReconciliationState) {
  const auto prop_p = storage_->NameToProperty("p");
  const auto prop_w = storage_->NameToProperty("w");

  ms::Gid v1_gid, v2_gid, v3_gid, e0_gid, e1_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();  // untouched
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();  // to be modified
    ASSERT_TRUE(v2.SetProperty(prop_p, ms::PropertyValue(1)).has_value());
    auto v3 = acc->CreateVertex();
    v3_gid = v3.Gid();  // to be deleted

    auto e0 = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("E0"));  // untouched edge
    ASSERT_TRUE(e0.has_value());
    e0_gid = e0->Gid();
    ASSERT_TRUE(e0->SetProperty(prop_w, ms::PropertyValue(1)).has_value());

    auto e1 = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("E1"));  // to be deleted
    ASSERT_TRUE(e1.has_value());
    e1_gid = e1->Gid();

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto *mapper = Mapper();
  const auto v_new_gid = ms::Gid::FromUint(2'000'000);
  const auto e_new_gid = ms::Gid::FromUint(3'000'000);

  std::vector<WalDeltaData> changelog{
      WalDeltaData{ms::durability::WalVertexCreate{v_new_gid}},
      WalDeltaData{
          ms::durability::WalVertexSetProperty{v2_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(2), mapper)}},
      WalDeltaData{ms::durability::WalVertexDelete{v3_gid}},
      WalDeltaData{ms::durability::WalEdgeCreate{e_new_gid, "E2", v1_gid, v2_gid}},
      WalDeltaData{ms::durability::WalEdgeDelete{e1_gid}},
  };

  auto &reconstruction = OpenReconstruction(changelog);

  // --- vertices ---
  {
    auto found = reconstruction.FindVertex(v_new_gid);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->source, mv::BranchReconstruction::Source::kOverlay);
  }
  {
    auto found = reconstruction.FindVertex(v2_gid);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->source, mv::BranchReconstruction::Source::kOverlay);
    ASSERT_TRUE(found->properties.contains(prop_p));
    EXPECT_EQ(found->properties.at(prop_p), ms::PropertyValue(2));
  }
  {
    auto found = reconstruction.FindVertex(v1_gid);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->source, mv::BranchReconstruction::Source::kMain);
  }
  EXPECT_FALSE(reconstruction.FindVertex(v3_gid).has_value()) << "tombstoned -- must be absent";
  EXPECT_FALSE(reconstruction.FindVertex(ms::Gid::FromUint(9'999'999)).has_value())
      << "a gid neither side ever heard of must be absent, not crash";

  // --- edges ---
  {
    auto found = reconstruction.FindEdge(e_new_gid);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->source, mv::BranchReconstruction::Source::kOverlay);
    EXPECT_EQ(found->from_vertex, v1_gid);
    EXPECT_EQ(found->to_vertex, v2_gid);
  }
  {
    auto found = reconstruction.FindEdge(e0_gid);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->source, mv::BranchReconstruction::Source::kMain);
    ASSERT_TRUE(found->properties.contains(prop_w));
    EXPECT_EQ(found->properties.at(prop_w), ms::PropertyValue(1));
  }
  EXPECT_FALSE(reconstruction.FindEdge(e1_gid).has_value()) << "tombstoned -- must be absent";
}

// R35 + S2/D2: main must never be mutated by reconstruction, and must keep evolving independent of
// a still-open BranchReconstruction -- while the reconstruction itself must keep showing the
// branch's own (fork_ts-anchored) view, unaffected by whatever main does AFTER Open().
TEST_F(VersioningBranchReconstructionTest, MainStaysLiveAndIndependentOfAnOpenReconstruction) {
  const auto prop_p = storage_->NameToProperty("p");

  ms::Gid v1_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    ASSERT_TRUE(v1.SetProperty(prop_p, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto *mapper = Mapper();
  std::vector<WalDeltaData> changelog{WalDeltaData{
      ms::durability::WalVertexSetProperty{v1_gid, "p", ms::ToExternalPropertyValue(ms::PropertyValue(100), mapper)}}};

  auto &reconstruction = OpenReconstruction(changelog);

  // Post-fork, post-open: an ORDINARY main write bumps v1's property to a THIRD, distinct value.
  {
    auto acc = storage_->Access(ms::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v1->SetProperty(prop_p, ms::PropertyValue(777)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The reconstruction must still show the BRANCH's own value (100) -- neither the original
  // fork-state value (1) nor the concurrent main write that happened after Open() (777).
  {
    auto found = reconstruction.FindVertex(v1_gid);
    ASSERT_TRUE(found.has_value());
    ASSERT_TRUE(found->properties.contains(prop_p));
    EXPECT_EQ(found->properties.at(prop_p), ms::PropertyValue(100));
  }

  // An ordinary, brand-new main accessor must see CURRENT main (777) -- untouched by the branch
  // overlay/reconstruction and unaffected by it still being open (R35, S2/D2: main stays fully
  // writable while a branch/reconstruction exists).
  {
    auto acc = storage_->Access(ms::READ);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto prop = v1->GetProperty(prop_p, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(777))
        << "R35 + S2/D2: main must keep evolving independent of the open reconstruction";
  }
}

// ---------------------------------------------------------------------------------------------
// Group 2: CaptureBranchCommit (write-capture seam).
//
// Mirrors versioning_branch_log.cpp's own TestTransaction fixture (a minimal, representative
// stand-in for a real MVCC transaction's deltas) -- reduced to just enough op kinds to prove
// CaptureBranchCommit's owner-resolution walk forwards the right records in the right order.
// Exercising this against a REAL branch write transaction is chunk 7's job (no branch write
// path/interpreter wiring exists yet); see branch_reconstruction.hpp's class comment.
// ---------------------------------------------------------------------------------------------

class SyntheticBranchTransaction {
 public:
  SyntheticBranchTransaction(ms::InMemoryStorage *storage, uint64_t transaction_id, uint64_t start_timestamp)
      : storage_(storage),
        transaction_(
            transaction_id, start_timestamp, ms::IsolationLevel::SNAPSHOT_ISOLATION,
            ms::StorageMode::IN_MEMORY_TRANSACTIONAL, /*edge_import_mode_active=*/false,
            ms::PointIndexStorage{}.CreatePointIndexContext(),
            std::make_shared<ms::ActiveIndices>(std::make_unique<ms::InMemoryLabelIndex::ActiveIndices>(),
                                                std::make_unique<ms::InMemoryLabelPropertyIndex::ActiveIndices>(),
                                                std::make_unique<ms::InMemoryEdgeTypeIndex::ActiveIndices>(),
                                                std::make_unique<ms::InMemoryEdgeTypePropertyIndex::ActiveIndices>(),
                                                std::make_unique<ms::InMemoryEdgePropertyIndex::ActiveIndices>(),
                                                std::make_unique<ms::TextIndex::ActiveIndices>(),
                                                std::make_unique<ms::TextEdgeIndex::ActiveIndices>(),
                                                std::make_unique<ms::PointIndexStorage::ActiveIndices>(),
                                                std::make_unique<ms::VectorIndex::ActiveIndices>(),
                                                std::make_unique<ms::VectorEdgeIndex::ActiveIndices>()),
            std::make_shared<ms::ActiveConstraints>(
                std::make_shared<ms::ExistenceConstraints::ActiveConstraints>(),
                std::make_shared<ms::InMemoryUniqueConstraints::ActiveConstraints>(),
                std::make_shared<ms::TypeConstraints::ActiveConstraints>())) {}

  ms::NameIdMapper &Mapper() { return *storage_->name_id_mapper_; }

  const ms::Transaction &Raw() const { return transaction_; }

  ms::Vertex *CreateVertex() {
    auto gid = ms::Gid::FromUint(vertices_count_++);
    auto *delta = ms::CreateDeleteObjectDelta(&transaction_);
    auto &vertex = vertices_.emplace_back(gid, delta);
    if (delta != nullptr) delta->prev.Set(&vertex);
    return &vertex;
  }

  ms::Edge *CreateEdge(ms::Vertex *from, ms::Vertex *to, const std::string &edge_type) {
    auto gid = ms::Gid::FromUint(edges_count_++);
    auto edge_type_id = ms::EdgeTypeId::FromUint(Mapper().NameToId(edge_type));
    auto *delta = ms::CreateDeleteObjectDelta(&transaction_);
    auto &edge = edges_.emplace_back(gid, delta);
    if (delta != nullptr) delta->prev.Set(&edge);
    auto edge_ref = ms::EdgeRef(&edge);
    ms::CreateAndLinkDelta(&transaction_, from, ms::Delta::RemoveOutEdgeTag(), edge_type_id, to, edge_ref);
    from->out_edges.emplace_back(edge_type_id, to, edge_ref);
    ms::CreateAndLinkDelta(&transaction_, to, ms::Delta::RemoveInEdgeTag(), edge_type_id, from, edge_ref);
    to->in_edges.emplace_back(edge_type_id, from, edge_ref);
    // Populates the exact cache CaptureBranchCommit (production code) reads via
    // transaction.GetEdgeSetPropertyInfo -- mirrors what a real edge-property write's commit path
    // relies on (transaction.hpp's EdgeSetPropertyInfo), not a test-only side channel.
    transaction_.RecordEdgeSetPropertyInfo(gid, to->gid, edge_type_id);
    return &edge;
  }

  void AddLabel(ms::Vertex *vertex, const std::string &label) {
    auto label_id = ms::LabelId::FromUint(Mapper().NameToId(label));
    vertex->labels.push_back(label_id);
    ms::CreateAndLinkDelta(&transaction_, vertex, ms::Delta::RemoveLabelTag(), label_id);
  }

  void SetProperty(ms::Vertex *vertex, const std::string &property, const ms::PropertyValue &value) {
    auto property_id = ms::PropertyId::FromUint(Mapper().NameToId(property));
    auto old_value = vertex->properties.GetProperty(property_id);
    ms::CreateAndLinkDelta(&transaction_, vertex, ms::Delta::SetPropertyTag(), property_id, old_value);
    vertex->properties.SetProperty(property_id, value);
  }

  void SetEdgeProperty(ms::Edge *edge, ms::Vertex *from_vertex, const std::string &property,
                       const ms::PropertyValue &value) {
    auto property_id = ms::PropertyId::FromUint(Mapper().NameToId(property));
    auto old_value = edge->properties.GetProperty(property_id);
    ms::CreateAndLinkDelta(&transaction_, edge, ms::Delta::SetPropertyTag(), from_vertex, property_id, old_value);
    edge->properties.SetProperty(property_id, value);
  }

  void DeleteVertex(ms::Vertex *vertex) {
    ms::CreateAndLinkDelta(&transaction_, vertex, ms::Delta::RecreateObjectTag());
  }

 private:
  ms::InMemoryStorage *storage_;
  ms::Transaction transaction_;
  uint64_t vertices_count_{0};
  uint64_t edges_count_{0};
  std::list<ms::Vertex> vertices_;
  std::list<ms::Edge> edges_;
};

class VersioningBranchReconstructionCaptureTest : public ::testing::Test {
 public:
  void SetUp() override {
    Clear();
    std::filesystem::create_directories(MainDir());
    std::filesystem::create_directories(BranchDir());
  }

  void TearDown() override { Clear(); }

  std::filesystem::path RootDir() const {
    return std::filesystem::temp_directory_path() / "MG_test_unit_versioning_branch_reconstruction";
  }

  std::filesystem::path MainDir() const { return RootDir() / "main"; }

  std::filesystem::path BranchDir() const { return RootDir() / "branch"; }

 private:
  void Clear() {
    if (std::filesystem::exists(RootDir())) std::filesystem::remove_all(RootDir());
  }
};

// CaptureBranchCommit is the thin, production-grade seam: it resolves each delta's true owner
// (mirroring main's own commit-time WAL-append owner-resolution) and forwards to BranchLog's own
// Append* surface (chunk 3a), unchanged. Driven here against a synthetic transaction; real
// branch-write wiring is chunk 7 (flagged in branch_reconstruction.hpp).
TEST_F(VersioningBranchReconstructionCaptureTest, CapturesOwnerResolvedDeltasIntoBranchLog) {
  ms::Config config;
  config.durability.storage_directory = MainDir();
  auto main_storage = std::make_unique<ms::InMemoryStorage>(config);

  ms::SalientConfig::Items items{.properties_on_edges = true};

  SyntheticBranchTransaction txn(main_storage.get(), ms::kTransactionInitialId, ms::kTimestampInitialId);

  auto *v1 = txn.CreateVertex();
  auto *v2 = txn.CreateVertex();
  auto *v3 = txn.CreateVertex();
  txn.AddLabel(v1, "Person");
  txn.SetProperty(v1, "name", ms::PropertyValue("Alice"));
  auto *edge = txn.CreateEdge(v1, v2, "KNOWS");
  txn.SetEdgeProperty(edge, v1, "since", ms::PropertyValue(2020));
  txn.DeleteVertex(v3);

  const uint64_t commit_timestamp = ms::kTimestampInitialId + 1;

  mv::BranchLog branch_log(BranchDir(), items, &txn.Mapper());
  auto const txn_end_pos = mv::CaptureBranchCommit(branch_log, txn.Raw(), main_storage.get(), commit_timestamp);
  branch_log.Finalize();

  auto records = mv::BranchLog::ReadAll(branch_log.Path());

  using namespace ms::durability;  // NOLINT(google-build-using-namespace)

  const auto name_prop = ms::PropertyId::FromUint(txn.Mapper().NameToId("name"));
  const auto since_prop = ms::PropertyId::FromUint(txn.Mapper().NameToId("since"));
  const auto name_value = ToExternalPropertyValue(v1->properties.GetProperty(name_prop), &txn.Mapper());
  const auto since_value = ToExternalPropertyValue(edge->properties.GetProperty(since_prop), &txn.Mapper());

  std::vector<WalDeltaData> const expected{
      WalDeltaData{WalVertexCreate{v1->gid}},
      WalDeltaData{WalVertexCreate{v2->gid}},
      WalDeltaData{WalVertexCreate{v3->gid}},
      WalDeltaData{WalVertexAddLabel{v1->gid, "Person"}},
      WalDeltaData{WalVertexSetProperty{v1->gid, "name", name_value}},
      WalDeltaData{WalEdgeCreate{edge->gid, "KNOWS", v1->gid, v2->gid}},
      WalDeltaData{WalEdgeSetProperty{edge->gid, "since", since_value, v1->gid, v2->gid, std::string{"KNOWS"}}},
      WalDeltaData{WalVertexDelete{v3->gid}},
      WalDeltaData{WalTransactionEnd{txn_end_pos.stored_crc_}},
  };

  ASSERT_EQ(records.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(records[i], expected[i]) << "mismatch decoding captured branch log record at index " << i;
  }

  EXPECT_EQ(branch_log.Path().parent_path(), BranchDir()) << "R21: the branch log lives in its own directory";
}

}  // namespace
