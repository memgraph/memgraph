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

#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
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
#include "versioning/branch_log.hpp"

import memgraph.storage.property_value;

using namespace memgraph;  // NOLINT(google-build-using-namespace)

namespace {

// Mimics just enough of the real storage engine internals -- mirroring
// tests/unit/storage_v2_wal_file.cpp's DeltaGenerator -- to produce a representative,
// realistically-linked set of committed MVCC deltas for a single transaction. Unlike that model
// (which appends straight into a raw storage::durability::WalFile), CommitInto() below routes the
// exact same deltas through a versioning::BranchLog, proving the branch-owned stream round-trips
// through the same forward-record format main's WAL uses (spec A.1/R3), with no new encode/decode
// logic of its own.
class TestTransaction {
 public:
  TestTransaction(storage::InMemoryStorage *storage, uint64_t transaction_id, uint64_t start_timestamp)
      : storage_(storage),
        transaction_(transaction_id, start_timestamp, storage::IsolationLevel::SNAPSHOT_ISOLATION,
                     storage::StorageMode::IN_MEMORY_TRANSACTIONAL, /*edge_import_mode_active=*/false,
                     storage::PointIndexStorage{}.CreatePointIndexContext(),
                     std::make_shared<storage::ActiveIndices>(
                         std::make_unique<storage::InMemoryLabelIndex::ActiveIndices>(),
                         std::make_unique<storage::InMemoryLabelPropertyIndex::ActiveIndices>(),
                         std::make_unique<storage::InMemoryEdgeTypeIndex::ActiveIndices>(),
                         std::make_unique<storage::InMemoryEdgeTypePropertyIndex::ActiveIndices>(),
                         std::make_unique<storage::InMemoryEdgePropertyIndex::ActiveIndices>(),
                         std::make_unique<storage::TextIndex::ActiveIndices>(),
                         std::make_unique<storage::TextEdgeIndex::ActiveIndices>(),
                         std::make_unique<storage::PointIndexStorage::ActiveIndices>(),
                         std::make_unique<storage::VectorIndex::ActiveIndices>(),
                         std::make_unique<storage::VectorEdgeIndex::ActiveIndices>()),
                     std::make_shared<storage::ActiveConstraints>(
                         std::make_shared<storage::ExistenceConstraints::ActiveConstraints>(),
                         std::make_shared<storage::InMemoryUniqueConstraints::ActiveConstraints>(),
                         std::make_shared<storage::TypeConstraints::ActiveConstraints>())) {}

  storage::NameIdMapper &Mapper() { return *storage_->name_id_mapper_; }

  storage::Vertex *CreateVertex() {
    auto gid = storage::Gid::FromUint(vertices_count_++);
    auto *delta = storage::CreateDeleteObjectDelta(&transaction_);
    auto &vertex = vertices_.emplace_back(gid, delta);
    if (delta != nullptr) delta->prev.Set(&vertex);
    return &vertex;
  }

  storage::Edge *CreateEdge(storage::Vertex *from, storage::Vertex *to, const std::string &edge_type) {
    auto gid = storage::Gid::FromUint(edges_count_++);
    auto edge_type_id = storage::EdgeTypeId::FromUint(Mapper().NameToId(edge_type));
    auto *delta = storage::CreateDeleteObjectDelta(&transaction_);
    auto &edge = edges_.emplace_back(gid, delta);
    if (delta != nullptr) delta->prev.Set(&edge);
    auto edge_ref = storage::EdgeRef(&edge);
    storage::CreateAndLinkDelta(&transaction_, from, storage::Delta::RemoveOutEdgeTag(), edge_type_id, to, edge_ref);
    from->out_edges.emplace_back(edge_type_id, to, edge_ref);
    storage::CreateAndLinkDelta(&transaction_, to, storage::Delta::RemoveInEdgeTag(), edge_type_id, from, edge_ref);
    to->in_edges.emplace_back(edge_type_id, from, edge_ref);
    edge_metadata_[gid.AsUint()] = {edge_type_id, from, to};
    return &edge;
  }

  void AddLabel(storage::Vertex *vertex, const std::string &label) {
    auto label_id = storage::LabelId::FromUint(Mapper().NameToId(label));
    vertex->labels.push_back(label_id);
    storage::CreateAndLinkDelta(&transaction_, vertex, storage::Delta::RemoveLabelTag(), label_id);
  }

  void SetProperty(storage::Vertex *vertex, const std::string &property, const storage::PropertyValue &value) {
    auto property_id = storage::PropertyId::FromUint(Mapper().NameToId(property));
    auto old_value = vertex->properties.GetProperty(property_id);
    storage::CreateAndLinkDelta(&transaction_, vertex, storage::Delta::SetPropertyTag(), property_id, old_value);
    vertex->properties.SetProperty(property_id, value);
  }

  void SetEdgeProperty(storage::Edge *edge, storage::Vertex *from_vertex, const std::string &property,
                       const storage::PropertyValue &value) {
    auto property_id = storage::PropertyId::FromUint(Mapper().NameToId(property));
    auto old_value = edge->properties.GetProperty(property_id);
    storage::CreateAndLinkDelta(
        &transaction_, edge, storage::Delta::SetPropertyTag(), from_vertex, property_id, old_value);
    edge->properties.SetProperty(property_id, value);
  }

  void DeleteVertex(storage::Vertex *vertex) {
    storage::CreateAndLinkDelta(&transaction_, vertex, storage::Delta::RecreateObjectTag());
  }

  // Walks every MVCC delta this (synthetic) transaction produced -- following the exact same
  // owner-resolution and skip rules as tests/unit/storage_v2_wal_file.cpp's
  // DeltaGenerator::Transaction::Finalize -- and appends the forward record for each into
  // `branch_log`, then closes the transaction with a WalTransactionEnd marker.
  storage::durability::WalTxnEndPos CommitInto(versioning::BranchLog &branch_log, uint64_t commit_timestamp) {
    for (const auto &delta : transaction_.deltas) {
      if (delta.action == storage::Delta::Action::ADD_IN_EDGE ||
          delta.action == storage::Delta::Action::REMOVE_IN_EDGE) {
        continue;
      }
      auto owner = delta.prev.Get();
      while (owner.type == storage::PreviousPtr::Type::DELTA) {
        owner = owner.delta->prev.Get();
      }
      if (owner.type == storage::PreviousPtr::Type::VERTEX) {
        branch_log.AppendDelta(delta, owner.vertex, commit_timestamp, storage_);
      } else if (owner.type == storage::PreviousPtr::Type::EDGE) {
        // Only SET_PROPERTY deltas are encoded for edges; DELETE_OBJECT etc. are not WAL-encoded.
        if (delta.action != storage::Delta::Action::SET_PROPERTY) continue;
        auto it = edge_metadata_.find(owner.edge->gid.AsUint());
        if (it == edge_metadata_.end()) {
          ADD_FAILURE() << "edge metadata missing for gid " << owner.edge->gid.AsUint();
          continue;
        }
        branch_log.AppendDelta(
            delta, owner.edge, commit_timestamp, storage_, it->second.to_vertex->gid, it->second.edge_type_id);
      } else {
        ADD_FAILURE() << "Invalid delta owner!";
      }
    }
    return branch_log.AppendTransactionEnd(commit_timestamp);
  }

 private:
  struct EdgeMeta {
    storage::EdgeTypeId edge_type_id;
    storage::Vertex *from_vertex;
    storage::Vertex *to_vertex;
  };

  storage::InMemoryStorage *storage_;
  storage::Transaction transaction_;
  uint64_t vertices_count_{0};
  uint64_t edges_count_{0};
  std::list<storage::Vertex> vertices_;
  std::list<storage::Edge> edges_;
  std::unordered_map<uint64_t, EdgeMeta> edge_metadata_;
};

}  // namespace

class VersioningBranchLogTest : public ::testing::Test {
 public:
  void SetUp() override {
    Clear();
    std::filesystem::create_directories(MainDir());
    std::filesystem::create_directories(BranchDir());
  }

  void TearDown() override { Clear(); }

  // A branch log lives in its OWN directory (never main's WAL directory -- spec R21). Both
  // directories sit under a single unique temp root so cleanup is a single remove_all, and this
  // test never touches the worktree.
  std::filesystem::path RootDir() const {
    return std::filesystem::temp_directory_path() / "MG_test_unit_versioning_branch_log";
  }

  std::filesystem::path MainDir() const { return RootDir() / "main"; }

  std::filesystem::path BranchDir() const { return RootDir() / "branch"; }

 private:
  void Clear() {
    if (std::filesystem::exists(RootDir())) std::filesystem::remove_all(RootDir());
  }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(VersioningBranchLogTest, RoundTripRepresentativeOpsAndMainUntouched) {
  storage::Config config;
  config.durability.storage_directory = MainDir();
  // WAL stays disabled on `main_storage` (default SnapshotWalMode::DISABLED): this test proves
  // the branch log is a fully independent stream, not a consumer of main's WAL machinery, so main
  // is never even asked to turn its WAL on.
  auto main_storage = std::make_unique<storage::InMemoryStorage>(config);

  storage::SalientConfig::Items items{.properties_on_edges = true};

  TestTransaction txn(main_storage.get(), storage::kTransactionInitialId, storage::kTimestampInitialId);

  auto *v1 = txn.CreateVertex();
  auto *v2 = txn.CreateVertex();
  auto *v3 = txn.CreateVertex();
  txn.AddLabel(v1, "Person");
  txn.SetProperty(v1, "name", storage::PropertyValue("Alice"));
  auto *edge = txn.CreateEdge(v1, v2, "KNOWS");
  txn.SetEdgeProperty(edge, v1, "since", storage::PropertyValue(2020));
  txn.DeleteVertex(v3);

  const uint64_t commit_timestamp = storage::kTimestampInitialId + 1;

  // The branch log directory is entirely separate from `main_storage`'s (own uuid/seq -- R21).
  versioning::BranchLog branch_log(BranchDir(), items, &txn.Mapper());
  auto const txn_end_pos = txn.CommitInto(branch_log, commit_timestamp);
  branch_log.Finalize();

  auto records = versioning::BranchLog::ReadAll(branch_log.Path());

  using namespace storage::durability;  // NOLINT(google-build-using-namespace)

  auto const name_prop = storage::PropertyId::FromUint(txn.Mapper().NameToId("name"));
  auto const since_prop = storage::PropertyId::FromUint(txn.Mapper().NameToId("since"));
  auto const name_value = ToExternalPropertyValue(v1->properties.GetProperty(name_prop), &txn.Mapper());
  auto const since_value = ToExternalPropertyValue(edge->properties.GetProperty(since_prop), &txn.Mapper());

  std::vector<WalDeltaData> const expected{
      WalDeltaData{WalVertexCreate{v1->gid}},
      WalDeltaData{WalVertexCreate{v2->gid}},
      WalDeltaData{WalVertexCreate{v3->gid}},
      WalDeltaData{WalVertexAddLabel{v1->gid, "Person"}},
      WalDeltaData{WalVertexSetProperty{v1->gid, "name", name_value}},
      WalDeltaData{WalEdgeCreate{edge->gid, "KNOWS", v1->gid, v2->gid}},
      // from_gid/to_gid/edge_type are documented as "extra information" and are NOT part of
      // WalEdgeSetProperty::operator== (see wal.hpp) -- filled in here to match reality anyway.
      WalDeltaData{WalEdgeSetProperty{edge->gid, "since", since_value, v1->gid, v2->gid, std::string{"KNOWS"}}},
      WalDeltaData{WalVertexDelete{v3->gid}},
      WalDeltaData{WalTransactionEnd{txn_end_pos.stored_crc_}},
  };

  ASSERT_EQ(records.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(records[i], expected[i]) << "mismatch decoding branch log record at index " << i;
  }

  // The branch log is a standalone, own-directory stream (R21): its file must live under
  // BranchDir(), never under main's directory, and main's own directory must carry no WAL/seq
  // artifacts of its own -- the branch log write never touched it.
  EXPECT_EQ(branch_log.Path().parent_path(), BranchDir());
  ASSERT_TRUE(std::filesystem::exists(MainDir()));
  EXPECT_TRUE(std::filesystem::is_empty(MainDir())) << "main's directory must be untouched by the branch log write";

  std::vector<std::filesystem::path> branch_files;
  for (auto const &entry : std::filesystem::directory_iterator(BranchDir())) {
    branch_files.push_back(entry.path());
  }
  EXPECT_EQ(branch_files.size(), 1u) << "the branch log must be the only file in its own directory";
}
