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

#include <memory>

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
auto MakeStorage() -> std::unique_ptr<InMemoryStorage> {
  Config config{};
  config.salient.items.properties_on_edges = true;
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
