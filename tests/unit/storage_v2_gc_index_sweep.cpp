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

// Which indexes a collection cycle sweeps, asked of the cycle rather than of the arming.
//
// The rules themselves are stated against IndexArming in storage_v2_index_arming.cpp, where one
// costs six lines instead of forty. What is left here is what only CollectGarbage can answer:
// that the arming a cycle sweeps with is claimed and reset per cycle, that object deletion still
// forces the whole family, that the one arming reaches each of the sweep's callees, and that
// narrowing never drops an entry the sweep was obliged to collect.

#include <gtest/gtest.h>

#include <array>
#include <string_view>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "storage_v2_gc_metrics_fixture.hpp"
#include "tests/test_commit_args_helper.hpp"

#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

namespace ms = memgraph::storage;

namespace {

// Two of everything, so "swept one" and "swept all" are different numbers.
constexpr auto kIndexes = uint64_t{2};

}  // namespace

class StorageV2GcIndexSweepCountTest : public StorageV2GcMetricsTest {
 protected:
  // How many indexes one collection cycle visited. The sweep looks at every entry of an index
  // rather than only the stale ones, so this, and not the sweep latency, is what says how much of
  // a cycle was worth doing.
  //
  // The pass adopts a hold it is handed, so it runs on this thread rather than the collection
  // thread and the count belongs to a known set of writes.
  //
  // Counted over the seven families the sweep covers: label, label-property, global vertex
  // property, edge-type, edge-type-property and edge-property indexes, plus unique constraints. An
  // eighth family joining the sweep without arming support breaks every expectation below, which
  // is the intent: it should not be possible to add one silently.
  uint64_t SweptByOnePass() {
    auto const before = handles_.gc_index_sweeps.Value();
    auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
    mem_storage->FreeMemory(UniqueGuard(storage->main_lock_), false);
    return static_cast<uint64_t>(handles_.gc_index_sweeps.Value() - before);
  }

  // Commit helpers come in two halves because one test needs a commit that is expected to fail.
  // Only the asserting half may be called through ASSERT_NO_FATAL_FAILURE.
  static bool TryCommit(std::unique_ptr<ms::Storage::Accessor> const &acc) {
    return acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  }

  static void Commit(std::unique_ptr<ms::Storage::Accessor> const &acc) { ASSERT_TRUE(TryCommit(acc)); }

  void CreateLabelIndex(std::string_view label) {
    auto acc = storage->UniqueAccess();
    ASSERT_NO_ERROR(acc->CreateIndex(storage->NameToLabel(label)));
    ASSERT_TRUE(TryCommit(acc));
  }

  void CreateLabelPropertyIndex(std::string_view label, std::string_view property) {
    auto acc = storage->UniqueAccess();
    ASSERT_NO_ERROR(
        acc->CreateIndex(storage->NameToLabel(label), {ms::PropertyPath{storage->NameToProperty(property)}}));
    ASSERT_TRUE(TryCommit(acc));
  }

  void CreateUniqueConstraint(std::string_view label, std::string_view property) {
    auto acc = storage->UniqueAccess();
    ASSERT_NO_ERROR(acc->CreateUniqueConstraint(storage->NameToLabel(label), {storage->NameToProperty(property)}));
    ASSERT_TRUE(TryCommit(acc));
  }

  void CreateGlobalVertexIndex(std::string_view property) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_NO_ERROR(acc->CreateGlobalVertexIndex(storage->NameToProperty(property)));
    ASSERT_TRUE(TryCommit(acc));
  }

  void CreateEdgeTypePropertyIndex(std::string_view edge_type, std::string_view property) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_NO_ERROR(acc->CreateIndex(storage->NameToEdgeType(edge_type), storage->NameToProperty(property)));
    ASSERT_TRUE(TryCommit(acc));
  }

  void CreateEdgeTypeIndex(std::string_view edge_type) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_NO_ERROR(acc->CreateIndex(storage->NameToEdgeType(edge_type)));
    ASSERT_TRUE(TryCommit(acc));
  }

  // Index size is exact rather than estimated: it is the skiplist's own count, decremented inside
  // remove(). A sweep that collected an entry is visible immediately, and one that was skipped
  // leaves the size inflated.
  uint64_t IndexedCount(std::string_view label) {
    auto acc = storage->Access(ms::READ);
    auto const count = acc->ApproximateVertexCount(storage->NameToLabel(label));
    acc->Abort();
    return count;
  }

  uint64_t GlobalIndexedCount(std::string_view property) {
    auto acc = storage->Access(ms::READ);
    auto const count = acc->ApproximateVertexCount(storage->NameToProperty(property));
    acc->Abort();
    return count;
  }

  uint64_t IndexedCount(std::string_view label, std::string_view property) {
    auto acc = storage->Access(ms::READ);
    auto const count = acc->ApproximateVertexCount(storage->NameToLabel(label),
                                                   std::array{ms::PropertyPath{storage->NameToProperty(property)}});
    acc->Abort();
    return count;
  }
};

///// THE ARMING A CYCLE SWEEPS WITH

// A cycle claims the published arming by swapping its own reset one in. If it did not, the writes
// of one cycle would keep arming every cycle after it.
TEST_F(StorageV2GcIndexSweepCountTest, IdleDatabaseSweepsNothing) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("A"));
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("B"));

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }

  // The write above says the vertex indexes may hold something to collect, so this pass sweeps.
  EXPECT_GT(SweptByOnePass(), 0);

  // Nothing has been written since, so there is nothing to look for.
  EXPECT_EQ(SweptByOnePass(), 0);
}

///// ONE ARMING, REACHING EACH OF THE SWEEP'S CALLEES

TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenLabelsIndexIsSwept) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("A"));
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("B"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  // A write naming one label only. The index on the other label cannot have gained anything to
  // collect, and walking it would cost its whole size to find that out.
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(*vertex->AddLabel(acc->NameToLabel("B")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// A unique constraint keeps a skiplist keyed the way an index is and is swept the same way, by a
// callee of its own, so a write that cannot have staled it should not cost its whole size either.
TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysConstraintIsSwept) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("L", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("L", "b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{2}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);

  // The constraint left unswept still holds: its entries were never stale, only unvisited.
  {
    auto acc = storage->Access(ms::WRITE);
    auto other = acc->CreateVertex();
    ASSERT_TRUE(*other.AddLabel(acc->NameToLabel("L")));
    ASSERT_NO_ERROR(other.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    EXPECT_FALSE(TryCommit(acc));
  }
}

// An index keyed on a property alone is swept by a callee of its own, and is armed by that
// property being written rather than by anything the vertex is labelled with.
TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysGlobalVertexIndexIsSwept) {
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("a"));
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{2}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// Such an index holds an entry for a vertex whatever it is labelled with, and no entry of it is
// touched when a label comes or goes, so a workload writing only labels must not walk it.
TEST_F(StorageV2GcIndexSweepCountTest, ALabelWriteSweepsNoGlobalVertexIndex) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("A"));
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("a"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(*vertex->AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysEdgeIndexIsSwept) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "b"));

  ms::Gid edge_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{2}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// An index on an edge type alone cannot be staled by writing a property: the entry holds no
// property, and the type it does hold cannot change.
TEST_F(StorageV2GcIndexSweepCountTest, AnEdgePropertyWriteSweepsNoEdgeTypeIndex) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypeIndex("E"));
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypeIndex("F"));

  ms::Gid edge_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  // Creating the edge is a structural change, which names no property, so both are swept.
  EXPECT_EQ(SweptByOnePass(), kIndexes);

  {
    auto acc = storage->Access(ms::WRITE);
    auto edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(acc->DeleteEdge(&*edge));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), kIndexes);

  auto acc = storage->Access(ms::READ);
  EXPECT_EQ(acc->ApproximateEdgeCount(acc->NameToEdgeType("E")), 0);
  acc->Abort();
}

///// WHERE NARROWING MUST NOT REACH

// A deleted vertex leaves index entries pointing at memory about to be freed, and no delta says
// which indexes hold them. Missing one is a dangling pointer rather than a wasted sweep.
TEST_F(StorageV2GcIndexSweepCountTest, ADeletedVertexSweepsEveryVertexIndex) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("A"));
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("B"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), kIndexes);
}

// The same holds for an index keyed on a property alone: its entries point at the vertex too, and
// the delete names no property to arm it with.
TEST_F(StorageV2GcIndexSweepCountTest, ADeletedVertexSweepsEveryGlobalVertexIndex) {
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("a"));
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->FindVertex(gid, ms::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_NO_ERROR(acc->DeleteVertex(&*vertex));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), kIndexes);
  EXPECT_EQ(GlobalIndexedCount("a"), 0);
}

// The edge counterpart. An edge's delta carries its type rather than any property an index is
// keyed on, so nothing names the entries left pointing at it.
TEST_F(StorageV2GcIndexSweepCountTest, ARemovedEdgeSweepsEveryEdgeIndex) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "b"));

  ms::Gid edge_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto edge = acc->FindEdge(edge_gid, ms::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(acc->DeleteEdge(&*edge));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), kIndexes);
}

///// NARROWING DROPS NOTHING IT WAS OBLIGED TO COLLECT

// The saving must not come at the cost of leaving entries behind: an index whose label was
// written is swept, and collects everything a sweep of it would.
TEST_F(StorageV2GcIndexSweepCountTest, TheSweptIndexStillCollectsItsStaleEntries) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("A"));
  ASSERT_NO_FATAL_FAILURE(CreateLabelIndex("B"));

  constexpr int kVertices = 100;
  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int i = 0; i != kVertices; ++i) {
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("B")));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(IndexedCount("A"), kVertices);
  ASSERT_EQ(IndexedCount("B"), kVertices);

  {
    auto acc = storage->Access(ms::WRITE);
    for (auto const gid : gids) {
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(*vertex->RemoveLabel(acc->NameToLabel("A")));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }

  EXPECT_EQ(SweptByOnePass(), 1);
  EXPECT_EQ(IndexedCount("A"), 0);
  EXPECT_EQ(IndexedCount("B"), kVertices);
}

// A property write says the vertex indexes may hold something to collect but a delta cannot say
// which property it was without being read for it. Were the write to arm the family and name no
// property, every label-property index would be skipped and the entries it left would stay.
TEST_F(StorageV2GcIndexSweepCountTest, APropertyWriteStillCollectsTheEntriesItStaled) {
  ASSERT_NO_FATAL_FAILURE(CreateLabelPropertyIndex("L", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateLabelPropertyIndex("L", "b"));

  constexpr int kVertices = 100;
  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int i = 0; i != kVertices; ++i) {
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
      ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{i}));
      ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{i}));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(IndexedCount("L", "a"), kVertices);

  // Rewriting a property leaves the entry holding the old value behind for the sweep to find.
  {
    auto acc = storage->Access(ms::WRITE);
    for (auto const gid : gids) {
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{-1}));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_EQ(IndexedCount("L", "a"), 2 * kVertices);

  EXPECT_EQ(SweptByOnePass(), 1);
  EXPECT_EQ(IndexedCount("L", "a"), kVertices);
  EXPECT_EQ(IndexedCount("L", "b"), kVertices);
}

// The same write reaches an index keyed on that property alone, and the entry it leaves there has
// to be collected by the pass the same property armed.
TEST_F(StorageV2GcIndexSweepCountTest, APropertyWriteStillCollectsWhatItStaledGlobally) {
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("a"));
  ASSERT_NO_FATAL_FAILURE(CreateGlobalVertexIndex("b"));

  constexpr int kVertices = 100;
  std::vector<ms::Gid> gids;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int i = 0; i != kVertices; ++i) {
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{i}));
      ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{i}));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(GlobalIndexedCount("a"), kVertices);

  {
    auto acc = storage->Access(ms::WRITE);
    for (auto const gid : gids) {
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{-1}));
    }
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_EQ(GlobalIndexedCount("a"), 2 * kVertices);

  EXPECT_EQ(SweptByOnePass(), 1);
  EXPECT_EQ(GlobalIndexedCount("a"), kVertices);
  EXPECT_EQ(GlobalIndexedCount("b"), kVertices);
}

///// AN ENTRY NOTHING IS LEFT TO COLLECT

// An abort undoes the index entries its writes made. It finds an edge's type by scanning
// from_vertex->out_edges, which no longer holds an edge the same transaction deleted, so the entry
// for it is never collected. Nothing else collects it either: no delta names the property, so the
// sweep is not armed for that index, and the edge itself is freed by the skiplist GC, leaving the
// entry pointing at freed memory for the next armed sweep to lock.
TEST_F(StorageV2GcIndexSweepCountTest, AnAbortLeavesNoEntryForAnEdgeItAlsoDeleted) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "p"));

  // Raw skiplist size, so an entry left behind for a deleted edge still counts.
  auto const indexed = [&] {
    auto acc = storage->Access(ms::READ);
    auto const count = acc->ApproximateEdgeCount(storage->NameToEdgeType("E"), storage->NameToProperty("p"));
    acc->Abort();
    return count;
  };
  ASSERT_EQ(indexed(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("p"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(acc->DeleteEdge(&*edge));
    acc->Abort();
  }
  // Whether the abort undoes the entry itself or arms the sweep to do it is a design choice; what
  // must hold either way is that no entry for an edge that no longer exists outlives the collection
  // cycles that follow, because the edge is freed by then.
  for (int i = 0; i < 3; ++i) SweptByOnePass();
  EXPECT_EQ(indexed(), 0) << "an entry for an edge deleted by an aborted transaction was never reclaimed";
}

// The same edge, against each kind of index that holds a property of it, and with the endpoints
// committed beforehand so the aborting transaction's deltas are laid out differently. Undoing the
// entry needs the edge's type, which is recorded only on the link the same transaction removed, so
// the lookup has to survive both delta layouts and reach every index keyed on that property.
TEST_F(StorageV2GcIndexSweepCountTest, AnAbortLeavesNoEntryInAnyEdgeIndexForAnEdgeItAlsoDeleted) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypePropertyIndex("E", "p"));
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypeIndex("E"));
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_NO_ERROR(acc->CreateGlobalEdgeIndex(storage->NameToProperty("p")));
    ASSERT_TRUE(TryCommit(acc));
  }

  // Endpoints committed first, so the aborting transaction below owns only the edge's deltas.
  ms::Gid from_gid, to_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    from_gid = from.Gid();
    to_gid = to.Gid();
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }

  {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->FindVertex(from_gid, ms::View::OLD);
    auto to = acc->FindVertex(to_gid, ms::View::OLD);
    ASSERT_TRUE(from.has_value() && to.has_value());
    auto edge = acc->CreateEdge(&*from, &*to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_ERROR(edge->SetProperty(acc->NameToProperty("p"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(acc->DeleteEdge(&*edge));
    acc->Abort();
  }

  for (int i = 0; i != 3; ++i) SweptByOnePass();

  auto acc = storage->Access(ms::READ);
  EXPECT_EQ(acc->ApproximateEdgeCount(acc->NameToEdgeType("E"), acc->NameToProperty("p")), 0)
      << "edge-type-property index kept an entry for an edge that never existed";
  EXPECT_EQ(acc->ApproximateEdgeCount(acc->NameToProperty("p")), 0)
      << "edge-property index kept an entry for an edge that never existed";
  EXPECT_EQ(acc->ApproximateEdgeCount(acc->NameToEdgeType("E")), 0)
      << "edge-type index kept an entry for an edge that never existed";
  acc->Abort();
}
