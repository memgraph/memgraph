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
#include "storage/v2/inmemory/unique_constraints.hpp"
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
    auto const before = handles().gc_index_sweeps.Value();
    auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
    mem_storage->FreeMemory(UniqueGuard(storage->main_lock_), false);
    return static_cast<uint64_t>(handles().gc_index_sweeps.Value() - before);
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

  // A constraint holds one entry per vertex it covers. Anything beyond that is an obsolete entry,
  // which only a sweep of that constraint removes.
  uint64_t ConstraintEntryCount(std::string_view label, std::string_view property) {
    auto *constraints = static_cast<ms::InMemoryUniqueConstraints *>(storage->constraints_.unique_constraints_.get());
    auto const count = constraints->EntryCount(storage->NameToLabel(label), {storage->NameToProperty(property)});
    EXPECT_TRUE(count.has_value());
    return count.value_or(0);
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

// A commit may only add an entry to a constraint a sweep will come back for. Since a sweep visits
// a constraint only when a write named its label or one of its properties, an entry added to a
// constraint the write named neither of is never collected, and repeating the write accumulates
// them without bound.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintTheWriteDidNotNameGainsNoEntry) {
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
  ASSERT_EQ(ConstraintEntryCount("L", "b"), 1);

  // Ten writes naming "a" only, each followed by the collection it arms. The value of "b" is the
  // same throughout, so the constraint on it covers the one vertex it covered to begin with.
  for (auto value = 2; value != 12; ++value) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{value}));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("L", "b"), 1);
}

// A property no constraint is keyed on, written over and over on a vertex one constraint covers.
// Such a write names nothing the constraint holds, so the constraint is left both unwritten and
// unswept, and what it holds does not depend on how many times the write is repeated.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintGainsNoEntryFromAPropertyNoConstraintIsKeyedOn) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "id"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Item")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("id"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("v"), ms::PropertyValue{0}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(ConstraintEntryCount("Item", "id"), 1);

  for (auto value = 1; value != 11; ++value) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("v"), ms::PropertyValue{value}));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("Item", "id"), 1);
}

// The property written is a key of a constraint on a label this vertex does not carry. That is
// enough for the write to be reported, because what counts as reportable is gathered across every
// constraint rather than per constraint, so the constraint the vertex does fall under must still
// be left alone.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintGainsNoEntryFromAKeyOfAConstraintOnAnotherLabel) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Elsewhere", "b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Item")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(ConstraintEntryCount("Item", "a"), 1);

  for (auto value = 2; value != 12; ++value) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("b"), ms::PropertyValue{value}));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("Item", "a"), 1);
}

// A label arriving on a vertex arms the constraints keyed on that label, and says nothing about
// the constraints keyed on the labels it already carried.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintGainsNoEntryFromASecondLabelArriving) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Added", "b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Item")));
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Added")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(ConstraintEntryCount("Item", "a"), 1);

  // Taking the second label off and putting it back names only that label.
  for (auto round = 0; round != 10; ++round) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(*vertex->RemoveLabel(acc->NameToLabel("Added")));
      ASSERT_TRUE(*vertex->AddLabel(acc->NameToLabel("Added")));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("Item", "a"), 1);
}

// Writing both keys at once through one map, where one of them keeps the value it already had.
// Whether that one counts as written decides both whether an entry is added for it and whether its
// constraint is swept, and those two must not part company.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintGainsNoEntryFromAMapThatRewritesItsKeyUnchanged) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "b"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Item")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("b"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  auto const before = ConstraintEntryCount("Item", "a");

  for (auto value = 2; value != 12; ++value) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      auto update = std::map<ms::PropertyId, ms::PropertyValue>{
          {acc->NameToProperty("a"), ms::PropertyValue{1}},
          {acc->NameToProperty("b"), ms::PropertyValue{value}},
      };
      ASSERT_NO_ERROR(vertex->UpdateProperties(update));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("Item", "a"), before);
}

// One transaction committing repeatedly. What an earlier batch wrote is not what a later one
// wrote, so a batch that names no key of a constraint may not add to it, however the batch before
// it was reported.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintGainsNoEntryFromABatchThatDidNotNameIt) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("Item", "a"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("Item")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("v"), ms::PropertyValue{0}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(ms::WRITE);
    // The first batch writes the key, which is what puts the vertex on the list of those owing a
    // constraint check.
    {
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{2}));
      ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    auto const after_the_key_write = ConstraintEntryCount("Item", "a");

    // Every batch after it writes a property no constraint is keyed on.
    for (auto value = 1; value != 11; ++value) {
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("v"), ms::PropertyValue{value}));
      ASSERT_TRUE(acc->PeriodicCommit(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Asked while the transaction is still open, because a collection pass cannot run until it
    // closes, and one that runs afterwards still holds the first batch's arming and would collect
    // whatever the later batches added.
    EXPECT_EQ(ConstraintEntryCount("Item", "a"), after_the_key_write)
        << "a batch naming no key of this constraint added to it anyway";

    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
}

// The same rule across the labels of one vertex: a write reaching a constraint on one of them says
// nothing about a constraint on another, which is left unswept and so must be left unwritten.
TEST_F(StorageV2GcIndexSweepCountTest, AConstraintOnAnotherOfTheVertexsLabelsGainsNoEntry) {
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("L", "a"));
  ASSERT_NO_FATAL_FAILURE(CreateUniqueConstraint("M", "c"));

  ms::Gid gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("M")));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("a"), ms::PropertyValue{1}));
    ASSERT_NO_ERROR(vertex.SetProperty(acc->NameToProperty("c"), ms::PropertyValue{1}));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  ASSERT_GT(SweptByOnePass(), 0);
  ASSERT_EQ(ConstraintEntryCount("M", "c"), 1);

  for (auto value = 2; value != 12; ++value) {
    {
      auto acc = storage->Access(ms::WRITE);
      auto vertex = acc->FindVertex(gid, ms::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_NO_ERROR(vertex->SetProperty(acc->NameToProperty("a"), ms::PropertyValue{value}));
      ASSERT_NO_FATAL_FAILURE(Commit(acc));
    }
    SweptByOnePass();
  }

  EXPECT_EQ(ConstraintEntryCount("M", "c"), 1);
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

///// ONE RULE, TWO ROUTES TO IT

// What a write armed is worked out two separate ways. A transactional write is read back off its
// deltas when they are unlinked; an analytical write leaves no deltas, so it notes itself as it
// happens. They are two implementations of one rule, and a route that stops noting what the other
// notes leaves entries nothing comes back for.
//
// Indexes only. A unique constraint cannot be reached under the analytical route: a write there
// makes no delta, a commit with no deltas returns before constraints are validated, and switching
// a database to that mode is refused while a constraint exists. The constraint cases are
// transactional for that reason, not by omission.
class StorageV2GcArmingRouteTest : public StorageV2GcIndexSweepCountTest,
                                   public testing::WithParamInterface<ms::StorageMode> {
 protected:
  void SetUp() override {
    StorageV2GcIndexSweepCountTest::SetUp();
    // Before any write, so the count belongs to writes made under one route.
    static_cast<ms::InMemoryStorage *>(storage.get())->SetStorageMode(GetParam());
  }
};

TEST_P(StorageV2GcArmingRouteTest, ALabelWriteArmsThatLabelsIndexAlone) {
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
    ASSERT_TRUE(*vertex->AddLabel(acc->NameToLabel("B")));
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

TEST_P(StorageV2GcArmingRouteTest, AVertexPropertyWriteArmsThatPropertysIndexAlone) {
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

TEST_P(StorageV2GcArmingRouteTest, AnEdgePropertyWriteArmsThatPropertysIndexAlone) {
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

INSTANTIATE_TEST_SUITE_P(BothArmingRoutes, StorageV2GcArmingRouteTest,
                         testing::Values(ms::StorageMode::IN_MEMORY_TRANSACTIONAL,
                                         ms::StorageMode::IN_MEMORY_ANALYTICAL),
                         [](testing::TestParamInfo<ms::StorageMode> const &mode) {
                           return mode.param == ms::StorageMode::IN_MEMORY_TRANSACTIONAL ? "Transactional"
                                                                                         : "Analytical";
                         });

// The one write the two routes answer differently, asserted so that it stays a deliberate
// difference rather than becoming one. Creating an edge stales nothing: the entry it adds can only
// go stale once the edge is removed, and a removal arms every edge index by itself. Arming nothing
// is therefore the tighter of the two answers, and the transactional route does not reach it
// because it treats the four deltas that link and unlink an edge alike.
TEST_F(StorageV2GcIndexSweepCountTest, AnEdgeCreationArmsOnlyOnTheTransactionalRoute) {
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypeIndex("E"));
  ASSERT_NO_FATAL_FAILURE(CreateEdgeTypeIndex("F"));

  auto const create_an_edge = [this] {
    auto acc = storage->Access(ms::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    ASSERT_NO_FATAL_FAILURE(Commit(acc));
  };

  ASSERT_NO_FATAL_FAILURE(create_an_edge());
  EXPECT_EQ(SweptByOnePass(), kIndexes) << "the transactional route arms every edge index";

  static_cast<ms::InMemoryStorage *>(storage.get())->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  // Changing mode is itself a reason to sweep everything, so spend that before measuring.
  SweptByOnePass();

  ASSERT_NO_FATAL_FAILURE(create_an_edge());
  EXPECT_EQ(SweptByOnePass(), 0) << "the analytical route arms none, and nothing it could sweep is stale";
}
