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

// Stock DeleteVertex on an overlay, MarkEdgeDeleted (overlay_mark_deleted.hpp) and the collection pass that keeps a
// deleted overlay alive (Config::retained_gid_floors). Overlays are heap objects outside vertices_/edges_ with no
// delta, as a branch creates them.

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
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

using Acc = ms::Storage::Accessor;
using Err = std::optional<ms::Error>;
// An operation under a writer; nullopt on success.
using Op = std::function<Err(Acc &)>;

template <typename R>
Err ErrorOf(R const &result) {
  return result.has_value() ? Err{} : Err{result.error()};
}

std::unique_ptr<ms::InMemoryStorage> MakeStorage(bool floors) {
  ms::Config config;
  config.gc = {.type = ms::Config::Gc::Type::NONE};
  if (floors) config.retained_gid_floors = {.vertex = kFloor, .edge = kFloor};
  auto storage = std::make_unique<ms::InMemoryStorage>(config);
  if (floors) {
    storage->vertex_id_ = kFloor;
    storage->edge_id_ = kFloor;
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

  std::unique_ptr<Acc> Writer() { return storage->Access(ms::WRITE, kSnapshot, std::nullopt); }

  std::unique_ptr<Acc> Reader() { return storage->Access(ms::READ, kSnapshot, std::nullopt); }

  static void Commit(Acc &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  bool VertexLive(Acc &acc) {
    return ms::VertexAccessor::Create(vertex.get(), storage.get(), acc.GetTransaction(), ms::View::NEW).has_value();
  }

  bool EdgeLive(Acc &acc) {
    return ms::EdgeAccessor::Create(ms::EdgeRef{edge.get()},
                                    type,
                                    vertex.get(),
                                    other.get(),
                                    storage.get(),
                                    acc.GetTransaction(),
                                    ms::View::NEW)
        .has_value();
  }

  // Stock non-detach DeleteVertex, as a branch deletes a vertex overlay.
  ms::Result<bool> DeleteVertex(Acc &acc) {
    auto overlay_vertex = ms::VertexAccessor{vertex.get(), storage.get(), acc.GetTransaction()};
    auto deleted = acc.DeleteVertex(&overlay_vertex);
    if (!deleted) return std::unexpected{deleted.error()};
    return deleted->has_value();
  }

  ms::Result<bool> DeleteEdge(Acc &acc, ms::Vertex *from, ms::Vertex *to) {
    return overlay::DeleteBaseEdge(acc.GetTransaction(), *storage, edge.get(), from, to, type);
  }

  Op DeleteVertexOp() {
    return [this](Acc &acc) { return ErrorOf(DeleteVertex(acc)); };
  }

  Op AddLabelOp() {
    return [this](Acc &acc) {
      return ErrorOf(
          ms::VertexAccessor{vertex.get(), storage.get(), acc.GetTransaction()}.AddLabel(storage->NameToLabel("L")));
    };
  }

  Op MarkEdgeDeletedOp() {
    return [this](Acc &acc) { return ErrorOf(overlay::MarkEdgeDeleted(acc.GetTransaction(), edge.get())); };
  }

  Op DeleteEdgeOp() {
    return [this](Acc &acc) { return ErrorOf(DeleteEdge(acc, vertex.get(), other.get())); };
  }

  Op SetEdgePropertyOp() {
    return [this](Acc &acc) {
      return ErrorOf(ms::EdgeAccessor{
          ms::EdgeRef{edge.get()}, type, vertex.get(), other.get(), storage.get(), acc.GetTransaction()}
                         .SetProperty(storage->NameToProperty("p"), ms::PropertyValue{1}));
    };
  }

  // `first` succeeds, then `second` under a concurrent writer loses.
  void Conflict(Op const &first, Op const &second) {
    auto a = Writer();
    auto b = Writer();
    EXPECT_EQ(first(*a), Err{});
    EXPECT_EQ(second(*b), Err{ms::Error::SERIALIZATION_ERROR});
    b->Abort();
    a->Abort();
  }

  static size_t Markers(ms::Vertex const &v, bool out) {
    auto const &list = out ? v.out_edges : v.in_edges;
    return static_cast<size_t>(
        std::ranges::count_if(list, [](auto const &e) { return ms::IsReservedId(std::get<ms::EdgeTypeId>(e)); }));
  }

  void Collect() { static_cast<ms::Storage &>(*storage).FreeMemory(); }

  std::unique_ptr<ms::InMemoryStorage> storage;
  ms::EdgeTypeId type;
  std::unique_ptr<ms::Vertex> vertex, other;
  std::unique_ptr<ms::Edge> edge;
};

class MarkDeletedLifecycle : public MarkDeletedTest, public testing::WithParamInterface<bool> {};

}  // namespace

// A delete mark commits (surviving collection, hidden from new snapshots, visible to older ones, edge count untouched)
// or aborts (overlay and count restored).
TEST_P(MarkDeletedLifecycle, VertexAndEdgeMarkDeletedCommitOrAbort) {
  bool const commit = GetParam();
  auto const count = storage->edge_count_.load();
  auto old_reader = Reader();
  {
    auto writer = Writer();
    ASSERT_EQ(DeleteVertex(*writer), ms::Result<bool>{true});
    ASSERT_EQ(ErrorOf(overlay::MarkEdgeDeleted(writer->GetTransaction(), edge.get())), Err{});
    EXPECT_FALSE(VertexLive(*writer));
    EXPECT_FALSE(EdgeLive(*writer));
    EXPECT_EQ(storage->edge_count_.load(), count);
    if (commit) {
      Commit(*writer);
    } else {
      writer->Abort();
    }
  }
  EXPECT_TRUE(VertexLive(*old_reader)) << "a snapshot older than the commit";
  EXPECT_TRUE(EdgeLive(*old_reader));
  old_reader.reset();
  Collect();

  EXPECT_EQ(vertex->deleted(), commit);
  EXPECT_EQ(edge->deleted(), commit);
  auto reader = Reader();
  EXPECT_EQ(VertexLive(*reader), !commit);
  EXPECT_EQ(EdgeLive(*reader), !commit);
  EXPECT_EQ(storage->edge_count_.load(), count);
}

INSTANTIATE_TEST_SUITE_P(CommitOrAbort, MarkDeletedLifecycle, testing::Bool());

TEST_F(MarkDeletedTest, ConcurrentWritersOfTheSameOverlayConflict) {
  Conflict(DeleteVertexOp(), DeleteVertexOp());
  Conflict(DeleteVertexOp(), AddLabelOp());
  Conflict(AddLabelOp(), DeleteVertexOp());
  Conflict(MarkEdgeDeletedOp(), MarkEdgeDeletedOp());
  Conflict(DeleteEdgeOp(), DeleteEdgeOp());
  Conflict(DeleteEdgeOp(), SetEdgePropertyOp());
  Conflict(SetEdgePropertyOp(), DeleteEdgeOp());
}

TEST_F(MarkDeletedTest, ADeleteRefusesARealEdgeButNotAMarkerEntry) {
  vertex->out_edges.emplace_back(overlay::Mark(type), other.get(), ms::EdgeRef{edge.get()});
  {
    auto writer = Writer();
    ASSERT_EQ(DeleteVertex(*writer), ms::Result<bool>{true}) << "markers are the hidden-edge set, not edges";
    writer->Abort();
  }
  vertex->in_edges.emplace_back(type, other.get(), ms::EdgeRef{edge.get()});
  auto writer = Writer();
  EXPECT_EQ(ErrorOf(DeleteVertex(*writer)), Err{ms::Error::VERTEX_HAS_EDGES});
  EXPECT_FALSE(vertex->deleted());
}

TEST_F(MarkDeletedTest, DeleteBaseEdgeMarksTheEdgeDeletedAndEachEndpointOnce) {
  auto const expect_markers = [&] {
    EXPECT_EQ(Markers(*vertex, true), 1U);
    EXPECT_EQ(Markers(*vertex, false), 0U);
    EXPECT_EQ(Markers(*other, false), 1U);
    EXPECT_EQ(Markers(*other, true), 0U);
  };
  {
    auto writer = Writer();
    ASSERT_EQ(DeleteEdge(*writer, vertex.get(), other.get()), ms::Result<bool>{true});
    EXPECT_TRUE(edge->deleted());
    expect_markers();
    Commit(*writer);
  }
  Collect();
  EXPECT_TRUE(edge->deleted()) << "the floor keeps the edge overlay through collection";
  expect_markers();
  auto writer = Writer();
  EXPECT_EQ(DeleteEdge(*writer, vertex.get(), other.get()), ms::Result<bool>{false});
  expect_markers();  // an already deleted edge gets no second marker
}

TEST_F(MarkDeletedTest, DeleteBaseEdgeOnASelfLoopMarksOneEntryPerListAndAbortRemovesThem) {
  auto const count = storage->edge_count_.load();
  auto writer = Writer();
  ASSERT_EQ(DeleteEdge(*writer, vertex.get(), vertex.get()), ms::Result<bool>{true});
  EXPECT_EQ(Markers(*vertex, true), 1U);
  EXPECT_EQ(Markers(*vertex, false), 1U);
  writer->Abort();
  EXPECT_FALSE(edge->deleted());
  EXPECT_EQ(Markers(*vertex, true), 0U);
  EXPECT_EQ(Markers(*vertex, false), 0U);
  EXPECT_EQ(storage->edge_count_.load(), count);
}

// Real endpoints, so a stock edge creation can run against them; the base edge overlay is the heap one.
TEST_F(MarkDeletedTest, ANewEdgeOnAnEndpointConflictsWithDeleteBaseEdgeInEitherOrder) {
  ms::Gid from_gid, to_gid;
  {
    auto acc = Writer();
    from_gid = acc->CreateVertex().Gid();
    to_gid = acc->CreateVertex().Gid();
    Commit(*acc);
  }
  auto const endpoints = [&](Acc &acc) {
    auto from = acc.FindVertex(from_gid, ms::View::NEW);
    auto to = acc.FindVertex(to_gid, ms::View::NEW);
    EXPECT_TRUE(from.has_value() && to.has_value());
    return std::pair{*from, *to};
  };
  Op const delete_edge = [&](Acc &acc) {
    auto [from, to] = endpoints(acc);
    return ErrorOf(DeleteEdge(acc, from.vertex_, to.vertex_));
  };
  Op const create = [&](Acc &acc) {
    auto [from, to] = endpoints(acc);
    return ErrorOf(acc.CreateEdge(&from, &to, type));
  };
  Conflict(delete_edge, create);
  Conflict(create, delete_edge);
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
      if (floors) {
        auto overlay_accessor = ms::VertexAccessor{overlay_vertex.get(), storage.get(), acc->GetTransaction()};
        ASSERT_TRUE(acc->DeleteVertex(&overlay_accessor).has_value());
      }
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
