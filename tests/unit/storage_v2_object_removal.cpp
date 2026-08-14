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

// Removals must happen in the collection pass, ordered after that pass's index cleanup. A removal on
// a client thread races every reader that started after it, because an accessor holds back nothing
// removed before it was taken.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/v2/inmemory/claimed_objects.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::View;

namespace {

struct Stores {
  uint64_t vertices;
  uint64_t edges;

  friend bool operator==(Stores const &, Stores const &) = default;
};

auto StoreSizes(InMemoryStorage const &storage) -> Stores {
  return {storage.VertexStoreSize(), storage.EdgeStoreSize()};
}

// GC is driven explicitly, so a pass is never in flight for reasons a test cannot see.
auto MakeStorage(bool light_edges = false) -> std::unique_ptr<InMemoryStorage> {
  Config config{};
  config.salient.items.properties_on_edges = true;
  config.salient.items.storage_light_edge = light_edges;
  config.salient.items.enable_edges_metadata = light_edges;
  config.gc.type = Config::Gc::Type::NONE;
  return std::make_unique<InMemoryStorage>(config);
}

// The first pass unlinks and marks, the second removes what that freed up.
void Collect(InMemoryStorage &storage) {
  storage.FreeMemory({}, false);
  storage.FreeMemory({}, false);
}

}  // namespace

// An aborted CREATE leaves its objects in storage until the next GC pass. They are invisible to
// queries from the moment the abort returns, but the memory is reclaimed by the pass that also
// cleans the indexes, so nothing can be freed under a reader that is mid-index-walk.
TEST(ObjectRemoval, AbortLeavesRemovalToGarbageCollection) {
  auto storage = MakeStorage();
  auto const edge_type = storage->NameToEdgeType("E");

  auto const before = StoreSizes(*storage);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, edge_type);
    ASSERT_TRUE(edge.has_value());
    acc->Abort();
  }

  EXPECT_EQ(StoreSizes(*storage), (Stores{before.vertices + 2, before.edges + 1}))
      << "an abort must not remove objects on its own thread, where no reader can have pinned them";

  {
    auto acc = storage->Access(memgraph::storage::READ);
    uint64_t visible = 0;
    for ([[maybe_unused]] auto vertex : acc->Vertices(View::NEW)) ++visible;
    EXPECT_EQ(visible, 0U) << "aborted objects stay invisible to queries";
    acc->Abort();
  }

  Collect(*storage);

  EXPECT_EQ(StoreSizes(*storage), before) << "the GC pass must reclaim what the abort handed it";
}

// Both kinds of dead object leave storage the same way.
TEST(ObjectRemoval, CommittedDeleteLeavesRemovalToGarbageCollection) {
  auto storage = MakeStorage();
  auto const edge_type = storage->NameToEdgeType("E");

  auto const before = StoreSizes(*storage);

  Gid from_gid{};
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, edge_type);
    ASSERT_TRUE(edge.has_value());
    from_gid = from.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->FindVertex(from_gid, View::NEW);
    ASSERT_TRUE(from.has_value());
    auto out = from->OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    for (auto &edge : out->edges) {
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_EQ(StoreSizes(*storage).edges, before.edges + 1) << "a committed delete does not remove on its own thread";

  Collect(*storage);

  EXPECT_EQ(StoreSizes(*storage).edges, before.edges);
}

// A collection pass takes its objects from two sources: the list transactions hand over, and, in
// analytical, a scan of the store. Both name an object once it is deleted and its deltas are gone,
// so the scan is filtered against what the handover already claimed. Retiring an object twice would
// remove it from storage twice.
TEST(ClaimedObjects, ClaimsNothingWhenNothingWasHandedOver) {
  auto const claimed = memgraph::storage::ClaimedObjects<Gid>{std::vector<Gid>{}};

  EXPECT_TRUE(claimed.empty());
  EXPECT_FALSE(claimed.contains(Gid::FromUint(1)));
}

TEST(ClaimedObjects, ReportsOnlyTheObjectsHandedOver) {
  auto const handed_over = std::vector<Gid>{Gid::FromUint(7), Gid::FromUint(2), Gid::FromUint(9)};
  auto const claimed = memgraph::storage::ClaimedObjects{handed_over};

  EXPECT_TRUE(claimed.contains(Gid::FromUint(2)));
  EXPECT_TRUE(claimed.contains(Gid::FromUint(7)));
  EXPECT_TRUE(claimed.contains(Gid::FromUint(9)));
  EXPECT_FALSE(claimed.contains(Gid::FromUint(1)));
  EXPECT_FALSE(claimed.contains(Gid::FromUint(8)));
  EXPECT_FALSE(claimed.contains(Gid::FromUint(10)));
}

// The source list may be consumed after the claim is taken, so membership must not depend on it.
TEST(ClaimedObjects, SurvivesTheHandoverListItWasBuiltFrom) {
  auto handed_over = std::vector<Gid>{Gid::FromUint(3)};
  auto const claimed = memgraph::storage::ClaimedObjects{handed_over};
  handed_over.clear();

  EXPECT_TRUE(claimed.contains(Gid::FromUint(3)));
}

// Objects deleted before a storage-mode change are handed over for the collector to retire, and the
// scan that analytical runs afterwards looks for the same condition. Each must be retired once.
TEST(ObjectRemoval, ModeChangeWithPendingDeletesRetiresEachObjectOnce) {
  auto storage = MakeStorage();
  auto const edge_type = storage->NameToEdgeType("E");

  auto const before = StoreSizes(*storage);

  std::vector<Gid> vertices;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (int i = 0; i != 3; ++i) {
      auto from = acc->CreateVertex();
      auto to = acc->CreateVertex();
      ASSERT_TRUE(acc->CreateEdge(&from, &to, edge_type).has_value());
      vertices.push_back(from.Gid());
      vertices.push_back(to.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Deleted, committed, and not yet collected: these are sitting in the handover list.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (auto const gid : vertices) {
      auto vertex = acc->FindVertex(gid, View::NEW);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(acc->DetachDeleteVertex(&*vertex).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ASSERT_GT(StoreSizes(*storage).vertices, before.vertices) << "the deletes must still be uncollected here";

  storage->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&from, &to, edge_type).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc->Vertices(View::NEW)) {
      ASSERT_TRUE(acc->DetachDeleteVertex(&vertex).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Collect(*storage);

  EXPECT_EQ(StoreSizes(*storage), before) << "every object deleted either side of the change must be retired";
}

// A light edge is named only by the adjacency of the vertices it joins, and deleting an edge erases
// it from both, so nothing left in the store names it afterwards. This is what the delete-time
// handover exists for, and what makes a scan of the store unable to find one.
TEST(ObjectRemoval, DeletedLightEdgeIsNoLongerNamedByItsEndpoints) {
  auto storage = MakeStorage(true);
  storage->SetStorageMode(memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL);
  auto const edge_type = storage->NameToEdgeType("E");

  Gid from_gid{};
  Gid to_gid{};
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&from, &to, edge_type).has_value());
    from_gid = from.Gid();
    to_gid = to.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->FindVertex(from_gid, View::NEW);
    ASSERT_TRUE(from.has_value());
    auto out = from->OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 1U);
    auto edge = out->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Analytical keeps no older version of the adjacency, so this is the adjacency itself.
  {
    auto acc = storage->Access(memgraph::storage::READ);
    auto from = acc->FindVertex(from_gid, View::OLD);
    auto to = acc->FindVertex(to_gid, View::OLD);
    ASSERT_TRUE(from.has_value());
    ASSERT_TRUE(to.has_value());
    EXPECT_EQ(from->OutEdges(View::OLD)->edges.size(), 0U);
    EXPECT_EQ(to->InEdges(View::OLD)->edges.size(), 0U);
    acc->Abort();
  }

  Collect(*storage);

  // Create and delete again: the collection that just ran must have left the metadata index able to
  // take the same work a second time.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->FindVertex(from_gid, View::NEW);
    auto to = acc->FindVertex(to_gid, View::NEW);
    ASSERT_TRUE(from.has_value());
    ASSERT_TRUE(to.has_value());
    ASSERT_TRUE(acc->CreateEdge(&*from, &*to, edge_type).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->FindVertex(from_gid, View::NEW);
    auto edge = from->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Collect(*storage);

  auto acc = storage->Access(memgraph::storage::READ);
  auto from = acc->FindVertex(from_gid, View::OLD);
  ASSERT_TRUE(from.has_value());
  EXPECT_EQ(from->OutEdges(View::OLD)->edges.size(), 0U);
  acc->Abort();
}
