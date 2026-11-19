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

// MarkVertexDeleted / MarkEdgeDeleted (overlay_mark_deleted.hpp) and the collection pass that keeps a deleted overlay
// alive (InMemoryStorage::SetRetainedGidFloors). Overlays are heap objects outside vertices_/edges_ with no delta, as a
// branch creates them.

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/overlay_mark_deleted.hpp"
#include "versioning/overlay_state.hpp"

namespace ms = memgraph::storage;
namespace overlay = memgraph::versioning::overlay;

namespace {

constexpr uint64_t kFloor = 1000;
constexpr auto kSnapshot = ms::IsolationLevel::SNAPSHOT_ISOLATION;

std::unique_ptr<ms::InMemoryStorage> MakeStorage(bool floors) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  auto storage = std::make_unique<ms::InMemoryStorage>(config);
  if (floors) {
    storage->vertex_id_ = kFloor;
    storage->edge_id_ = kFloor;
    storage->SetRetainedGidFloors(kFloor, kFloor);
  }
  return storage;
}

class MarkDeletedTest : public testing::Test {
 protected:
  void SetUp() override {
    storage = MakeStorage(true);
    type = storage->NameToEdgeType("T");
    vertex = std::make_unique<ms::Vertex>(ms::Gid::FromUint(7), nullptr);
    other = std::make_unique<ms::Vertex>(ms::Gid::FromUint(8), nullptr);
    edge = std::make_unique<ms::Edge>(ms::Gid::FromUint(9), nullptr);
  }

  std::unique_ptr<ms::Storage::Accessor> Writer() { return storage->Access(ms::WRITE, kSnapshot, std::nullopt); }

  std::unique_ptr<ms::Storage::Accessor> Reader() { return storage->Access(ms::READ, kSnapshot, std::nullopt); }

  static void Commit(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  bool VertexLive(ms::Storage::Accessor &acc) {
    return ms::VertexAccessor::Create(vertex.get(), storage.get(), acc.GetTransaction(), ms::View::NEW).has_value();
  }

  bool EdgeLive(ms::Storage::Accessor &acc) {
    return ms::EdgeAccessor::Create(ms::EdgeRef{edge.get()},
                                    type,
                                    vertex.get(),
                                    other.get(),
                                    storage.get(),
                                    acc.GetTransaction(),
                                    ms::View::NEW)
        .has_value();
  }

  void Collect() { static_cast<ms::Storage &>(*storage).FreeMemory(); }

  std::unique_ptr<ms::InMemoryStorage> storage;
  ms::EdgeTypeId type;
  std::unique_ptr<ms::Vertex> vertex, other;
  std::unique_ptr<ms::Edge> edge;
};

}  // namespace

TEST_F(MarkDeletedTest, ACommittedVertexMarkDeletedSurvivesCollectionAndOlderSnapshotsStillSeeTheVertex) {
  auto old_reader = Reader();
  auto writer = Writer();
  auto const marked_deleted = overlay::MarkVertexDeleted(writer->GetTransaction(), *storage, vertex.get());
  ASSERT_TRUE(marked_deleted.has_value() && *marked_deleted);
  EXPECT_FALSE(VertexLive(*writer));
  Commit(*writer);
  writer.reset();

  EXPECT_TRUE(VertexLive(*old_reader)) << "a snapshot older than the commit";
  old_reader.reset();
  Collect();

  EXPECT_TRUE(vertex->deleted());
  auto reader = Reader();
  EXPECT_FALSE(VertexLive(*reader));
}

TEST_F(MarkDeletedTest, ACommittedEdgeMarkDeletedSurvivesCollectionWithoutTouchingTheEdgeCount) {
  auto const count = storage->edge_count_.load();
  auto old_reader = Reader();
  auto writer = Writer();
  auto const marked_deleted = overlay::MarkEdgeDeleted(writer->GetTransaction(), edge.get());
  ASSERT_TRUE(marked_deleted.has_value() && *marked_deleted);
  EXPECT_FALSE(EdgeLive(*writer));
  Commit(*writer);
  writer.reset();

  EXPECT_TRUE(EdgeLive(*old_reader)) << "a snapshot older than the commit";
  old_reader.reset();
  Collect();

  EXPECT_TRUE(edge->deleted());
  auto reader = Reader();
  EXPECT_FALSE(EdgeLive(*reader));
  EXPECT_EQ(storage->edge_count_.load(), count);
}

TEST_F(MarkDeletedTest, AnAbortedMarkDeletedRestoresTheOverlayAndTheEdgeCount) {
  auto const count = storage->edge_count_.load();
  {
    auto writer = Writer();
    ASSERT_TRUE(overlay::MarkVertexDeleted(writer->GetTransaction(), *storage, vertex.get()).has_value());
    ASSERT_TRUE(overlay::MarkEdgeDeleted(writer->GetTransaction(), edge.get()).has_value());
    EXPECT_EQ(storage->edge_count_.load(), count);
    writer->Abort();
  }
  Collect();

  EXPECT_FALSE(vertex->deleted());
  EXPECT_FALSE(edge->deleted());
  auto reader = Reader();
  EXPECT_TRUE(VertexLive(*reader));
  EXPECT_TRUE(EdgeLive(*reader));
  EXPECT_EQ(storage->edge_count_.load(), count);
}

TEST_F(MarkDeletedTest, AMarkDeletedConflictsWithAConcurrentWriterOfTheSameOverlay) {
  auto const label = storage->NameToLabel("L");
  {
    auto first = Writer();
    auto second = Writer();
    ASSERT_TRUE(overlay::MarkVertexDeleted(first->GetTransaction(), *storage, vertex.get()).has_value());
    auto const lost = overlay::MarkVertexDeleted(second->GetTransaction(), *storage, vertex.get());
    ASSERT_FALSE(lost.has_value());
    EXPECT_EQ(lost.error(), ms::Error::SERIALIZATION_ERROR);
    EXPECT_FALSE(
        (ms::VertexAccessor{vertex.get(), storage.get(), second->GetTransaction()}.AddLabel(label).has_value()));
    second->Abort();
    first->Abort();
  }
  {
    auto first = Writer();
    auto second = Writer();
    ASSERT_TRUE((ms::VertexAccessor{vertex.get(), storage.get(), first->GetTransaction()}.AddLabel(label).has_value()));
    auto const lost = overlay::MarkVertexDeleted(second->GetTransaction(), *storage, vertex.get());
    ASSERT_FALSE(lost.has_value());
    EXPECT_EQ(lost.error(), ms::Error::SERIALIZATION_ERROR);
    second->Abort();
    first->Abort();
  }
  {
    auto first = Writer();
    auto second = Writer();
    ASSERT_TRUE(overlay::MarkEdgeDeleted(first->GetTransaction(), edge.get()).has_value());
    auto const lost = overlay::MarkEdgeDeleted(second->GetTransaction(), edge.get());
    ASSERT_FALSE(lost.has_value());
    EXPECT_EQ(lost.error(), ms::Error::SERIALIZATION_ERROR);
    second->Abort();
    first->Abort();
  }
}

TEST_F(MarkDeletedTest, AVertexMarkDeletedRefusesARealEdgeButNotAMarkerEntry) {
  auto const marker = overlay::Mark(type);
  vertex->out_edges.emplace_back(marker, other.get(), ms::EdgeRef{edge.get()});
  {
    auto writer = Writer();
    auto const marked_deleted = overlay::MarkVertexDeleted(writer->GetTransaction(), *storage, vertex.get());
    ASSERT_TRUE(marked_deleted.has_value() && *marked_deleted) << "markers are the hidden-edge set, not edges";
    writer->Abort();
  }
  vertex->in_edges.emplace_back(type, other.get(), ms::EdgeRef{edge.get()});
  auto writer = Writer();
  auto const refused = overlay::MarkVertexDeleted(writer->GetTransaction(), *storage, vertex.get());
  ASSERT_FALSE(refused.has_value());
  EXPECT_EQ(refused.error(), ms::Error::VERTEX_HAS_EDGES);
  EXPECT_FALSE(vertex->deleted());
}

// With floors, collection retires native objects (gid >= floor) as on main while keeping the overlays.
TEST(MarkDeletedCollection, ANativeObjectIsRetiredWithOrWithoutFloors) {
  for (bool const floors : {false, true}) {
    SCOPED_TRACE(floors);
    auto storage = MakeStorage(floors);
    auto const type = storage->NameToEdgeType("T");
    auto overlay_vertex = std::make_unique<ms::Vertex>(ms::Gid::FromUint(7), nullptr);
    {
      auto acc = storage->Access(ms::WRITE, kSnapshot, std::nullopt);
      auto from = acc->CreateVertex();
      auto to = acc->CreateVertex();
      ASSERT_TRUE(acc->CreateEdge(&from, &to, type).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    ASSERT_EQ(storage->VertexStoreSize(), 2U);
    ASSERT_EQ(storage->EdgeStoreSize(), 1U);
    {
      auto acc = storage->Access(ms::WRITE, kSnapshot, std::nullopt);
      if (floors)
        ASSERT_TRUE(overlay::MarkVertexDeleted(acc->GetTransaction(), *storage, overlay_vertex.get()).has_value());
      auto natives = std::vector<ms::VertexAccessor>{};
      for (auto const &vertex : acc->Vertices(ms::View::OLD)) natives.push_back(vertex);
      for (auto &vertex : natives) ASSERT_TRUE(acc->DetachDeleteVertex(&vertex).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    static_cast<ms::Storage &>(*storage).FreeMemory();
    EXPECT_EQ(storage->VertexStoreSize(), 0U);
    EXPECT_EQ(storage->EdgeStoreSize(), 0U);
    if (floors) EXPECT_TRUE(overlay_vertex->deleted());
  }
}
