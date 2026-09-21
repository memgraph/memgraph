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

// Graph Versioning: the write-CAPTURE seam (CaptureBranchCommit).
//
// VersioningBranchReconstructionCaptureTest drives CaptureBranchCommit against a
// synthetically-built storage::Transaction (mirrors versioning_branch_log.cpp's own
// TestTransaction fixture, since the real branch-write transaction plumbing lives in the
// interpreter's commit path, not here).

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
#include "storage/v2/inmemory/vertex_property_index.hpp"
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

// CaptureBranchCommit (write-capture seam).
//
// Mirrors versioning_branch_log.cpp's own TestTransaction fixture (a minimal, representative
// stand-in for a real MVCC transaction's deltas) -- reduced to just enough op kinds to prove
// CaptureBranchCommit's owner-resolution walk forwards the right records in the right order.
// Exercising this against a REAL branch write transaction requires the interpreter's branch-write
// wiring (query/interpreter.cpp's commit path), not exercised here.

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
                                                std::make_unique<ms::InMemoryVertexPropertyIndex::ActiveIndices>(),
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
// Append* surface (chunk 3a), unchanged. Driven here against a synthetic transaction; the real
// branch-write wiring lives in the interpreter's commit path (query/interpreter.cpp).
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

  mv::BranchLog branch_log(BranchDir(), items, &txn.Mapper(), /*seq_num=*/0);
  uint64_t captured = 0;
  auto const txn_end_pos =
      mv::CaptureBranchCommit(branch_log, txn.Raw(), main_storage.get(), commit_timestamp, &captured);
  branch_log.Finalize();

  auto records = mv::BranchLog::ReadAll(branch_log.Path());

  // Ground-truth invariant: out_record_count MUST equal the number of records ReadAll returns --
  // this is the unit the retention cap depends on (CollectBranchChangelog/changelog.size() seeds
  // BranchContext::changelog_length_ straight from ReadAll's record count on re-checkout, and
  // AddCapturedRecords(captured) increments the very same counter on the live-session path -- if
  // the two units drift, the cap systematically under- or over-enforces).
  EXPECT_EQ(captured, records.size())
      << "out_record_count MUST equal the number of records ReadAll returns -- this is the unit the "
         "retention cap depends on";

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
