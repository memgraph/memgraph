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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <random>
#include <thread>
#include <vector>

#include "dbms/database.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using namespace memgraph::storage;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

/// Create an InMemoryStorage with light edges enabled.
auto MakeLightEdgeStorage() -> std::unique_ptr<Storage> {
  return std::make_unique<InMemoryStorage>(
      Config{.salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}}});
}

/// Create an InMemoryStorage with light edges + edges_metadata enabled.
auto MakeLightEdgeStorageWithMetadata() -> std::unique_ptr<Storage> {
  return std::make_unique<InMemoryStorage>(Config{
      .salient = {.items = {.properties_on_edges = true, .enable_edges_metadata = true, .storage_light_edge = true}}});
}

/// Helper: create two vertices and return their Gids.
auto CreateTwoVertices(Storage *store) -> std::pair<Gid, Gid> {
  auto acc = store->Access(WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto gid1 = v1.Gid();
  auto gid2 = v2.Gid();
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return {gid1, gid2};
}

}  // namespace

// ===========================================================================
// 1. Config Validation
// ===========================================================================

TEST(LightEdgesConfig, LightEdgeWithPropertiesOnEdges) {
  // Light edges require properties_on_edges=true.
  // Verify the combination works: edges get full property support.
  auto store = MakeLightEdgeStorage();
  auto acc = store->Access(WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto et = acc->NameToEdgeType("ET");
  auto edge_res = acc->CreateEdge(&v1, &v2, et);
  ASSERT_TRUE(edge_res.has_value());

  // Set a property on the edge to verify properties work
  auto prop = acc->NameToProperty("key");
  auto set_res = edge_res->SetProperty(prop, PropertyValue(42));
  ASSERT_TRUE(set_res.has_value());

  auto props = edge_res->Properties(View::NEW);
  ASSERT_TRUE(props.has_value());
  ASSERT_EQ(props->size(), 1);
  ASSERT_EQ(props->at(prop).ValueInt(), 42);

  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// ===========================================================================
// 3. CreateEdge
// ===========================================================================

TEST(LightEdgesCreateEdge, BasicCreateAndCommit) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge
  Gid edge_gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    ASSERT_TRUE(vf);
    ASSERT_TRUE(vt);

    auto et = acc->NameToEdgeType("KNOWS");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();

    // Before commit: NEW shows edge, OLD doesn't
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 1);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
    ASSERT_EQ(vt->InEdges(View::NEW)->edges.size(), 1);
    ASSERT_EQ(vt->InEdges(View::OLD)->edges.size(), 0);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // After commit: both views show the edge
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge_gid);
  }
}

TEST(LightEdgesCreateEdge, EdgeWithProperties) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("HAS");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());

    auto prop_weight = acc->NameToProperty("weight");
    auto prop_name = acc->NameToProperty("name");

    ASSERT_TRUE(res->SetProperty(prop_weight, PropertyValue(3.14)).has_value());
    ASSERT_TRUE(res->SetProperty(prop_name, PropertyValue("test")).has_value());

    auto props = res->Properties(View::NEW);
    ASSERT_TRUE(props.has_value());
    ASSERT_EQ(props->size(), 2);
    ASSERT_DOUBLE_EQ(props->at(prop_weight).ValueDouble(), 3.14);
    ASSERT_EQ(props->at(prop_name).ValueString(), "test");

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Verify properties after commit
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_EQ(out->edges.size(), 1);
    auto props = out->edges[0].Properties(View::OLD);
    ASSERT_TRUE(props.has_value());
    ASSERT_EQ(props->size(), 2);
  }
}

TEST(LightEdgesCreateEdge, SelfLoop) {
  auto store = MakeLightEdgeStorage();

  Gid gid;
  {
    auto acc = store->Access(WRITE);
    auto v = acc->CreateVertex();
    gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(WRITE);
    auto v = acc->FindVertex(gid, View::NEW);
    ASSERT_TRUE(v);
    auto et = acc->NameToEdgeType("SELF");
    auto res = acc->CreateEdge(&*v, &*v, et);
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(WRITE);
    auto v = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(v);
    ASSERT_EQ(v->OutEdges(View::OLD)->edges.size(), 1);
    ASSERT_EQ(v->InEdges(View::OLD)->edges.size(), 1);
    ASSERT_EQ(*v->OutDegree(View::OLD), 1);
    ASSERT_EQ(*v->InDegree(View::OLD), 1);
  }
}

TEST(LightEdgesCreateEdge, MultipleEdgesBetweenSameVertices) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et1 = acc->NameToEdgeType("KNOWS");
    auto et2 = acc->NameToEdgeType("LIKES");

    auto r1 = acc->CreateEdge(&*vf, &*vt, et1);
    ASSERT_TRUE(r1.has_value());
    auto r2 = acc->CreateEdge(&*vf, &*vt, et2);
    ASSERT_TRUE(r2.has_value());
    auto r3 = acc->CreateEdge(&*vf, &*vt, et1);
    ASSERT_TRUE(r3.has_value());

    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 3);
    ASSERT_EQ(vt->InEdges(View::NEW)->edges.size(), 3);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 3);
  }
}

// ===========================================================================
// 7. FindEdge
// ===========================================================================

// FindEdge is a private method tested indirectly through WAL recovery,
// snapshot recovery, and replication. We test the public edge-lookup behavior
// here (via OutEdges/InEdges on vertices).

TEST(LightEdgesFindEdge, EdgeAccessibleViaVertexWithMetadata) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  Gid edge_gid = Gid::FromUint(0);
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("FOUND");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Edge should be findable via vertex out_edges
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge_gid);
  }
}

// Two-arg FindEdge(edge_gid, from_vertex_gid) with a stale/absent from_vertex
// must SOFT-MISS (return nullopt), matching the heavy arm and the optional
// contract of the public wrapper — not throw across the query boundary.
TEST(LightEdgesFindEdge, FindEdgeWithStaleFromVertexSoftMisses) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  Gid edge_gid = Gid::FromUint(0);
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("FOUND");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(WRITE);
    // A from_vertex_gid that never existed: must return nullopt, not throw.
    auto never = Gid::FromUint(999999);
    std::optional<EdgeAccessor> found;
    ASSERT_NO_THROW({ found = acc->FindEdge(edge_gid, never, View::OLD); });
    ASSERT_FALSE(found.has_value());
    // And the real (gid, from) still resolves.
    auto real = acc->FindEdge(edge_gid, gid_from, View::OLD);
    ASSERT_TRUE(real.has_value());
    ASSERT_EQ(real->Gid(), edge_gid);
  }
}

TEST(LightEdgesFindEdge, EdgeAccessibleViaVertexWithoutMetadata) {
  auto store = MakeLightEdgeStorage();  // no edges_metadata
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  Gid edge_gid = Gid::FromUint(0);
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("FOUND");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge_gid);
  }
}

TEST(LightEdgesFindEdge, NoEdgesOnEmptyVertex) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  auto acc = store->Access(WRITE);
  auto vf = acc->FindVertex(gid_from, View::OLD);
  ASSERT_TRUE(vf);
  auto out = vf->OutEdges(View::OLD);
  ASSERT_EQ(out->edges.size(), 0);
}

// ===========================================================================
// 10. Storage Cleanup
// ===========================================================================

TEST(LightEdgesCleanup, DestructorFreesAllEdges) {
  // Simply verifies no crash / ASAN errors on destruction with edges alive
  {
    auto store = MakeLightEdgeStorage();
    auto [gid_from, gid_to] = CreateTwoVertices(store.get());

    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("ALIVE");
    for (int i = 0; i < 100; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    // Accessor drops, then store destroys -> should free all 100 edges from pool
  }
  // If we get here without ASAN/MSAN errors, cleanup worked
}

TEST(LightEdgesCleanup, DestroyAndRecreateStorage) {
  // Verify that destroying a storage with edges and creating a new one works
  // without memory issues (tests pool allocator recycling).
  for (int round = 0; round < 3; ++round) {
    auto store = MakeLightEdgeStorage();
    auto [gid_from, gid_to] = CreateTwoVertices(store.get());

    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("CYCLE");
    for (int i = 0; i < 50; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    // store destructor frees everything, next iteration creates fresh storage
  }
}

// ===========================================================================
// 12. DropGraph tests
// ===========================================================================

// DropGraph on a light-edge storage must free all edges (no
// ASAN/MSAN errors), leave the storage empty, and leave the pool reusable.
TEST(LightEdgesCleanup, DropGraphFreesEdgesAndPool) {
  auto store = MakeLightEdgeStorage();
  auto *mem_storage = static_cast<InMemoryStorage *>(store.get());

  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create enough edges to span multiple pool chunks.
  constexpr int kEdges = 500;
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto etype = acc->NameToEdgeType("T");
    for (int idx = 0; idx < kEdges; ++idx) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, etype).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // DropGraph requires IN_MEMORY_ANALYTICAL mode.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = store->UniqueAccess();
    acc->DropGraph();
  }

  // Verify storage is empty.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  {
    auto acc = store->Access(READ);
    int edge_count = 0;
    for (auto vertex : acc->Vertices(View::OLD)) {
      auto out = vertex.OutEdges(View::OLD);
      if (out.has_value()) edge_count += static_cast<int>(out->edges.size());
    }
    EXPECT_EQ(edge_count, 0);
  }

  // Pool must still be usable after DropGraph + Reclaim.
  auto [gf2, gt2] = CreateTwoVertices(store.get());
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gf2, View::NEW);
    auto vt = acc->FindVertex(gt2, View::NEW);
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, acc->NameToEdgeType("NEW")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// ===========================================================================
// 4. DeleteEdge
// ===========================================================================

TEST(LightEdgesDeleteEdge, BasicDeleteAndCommit) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge
  Gid edge_gid = Gid::FromUint(0);
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("REL");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete edge
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    ASSERT_TRUE(vf);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];

    auto del = acc->DeleteEdge(&edge);
    ASSERT_TRUE(del.has_value());

    // After delete: NEW shows 0 edges, OLD shows 1
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 1);

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // After commit: edge gone
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesDeleteEdge, DetachDeleteVertex) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("REL");
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Detach delete from_vertex
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    ASSERT_TRUE(vf);
    auto del = acc->DetachDeleteVertex(&*vf);
    ASSERT_TRUE(del.has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // to_vertex should have no in_edges
  {
    auto acc = store->Access(WRITE);
    auto vt = acc->FindVertex(gid_to, View::OLD);
    ASSERT_TRUE(vt);
    ASSERT_EQ(vt->InEdges(View::OLD)->edges.size(), 0);
  }
}

// ===========================================================================
// 5. Abort Handling
// ===========================================================================

TEST(LightEdgesAbort, AbortCreation) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge then abort (don't commit)
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("TEMP");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 1);
    // Accessor destructor will abort
  }

  // Edge should not exist
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
  }
}

// Regression: with enable_edges_metadata + light edges, CREATING an edge then
// ABORTING the transaction must not double-remove the metadata entry. Abort
// routes the aborted-created light Edge* into deleted_edges_ (the heal), so the
// single metadata OnEdgesDeleted must happen at CollectGarbage time, NOT also at
// abort time. Before the fix, OnEdgesDeleted fired at abort AND again in GC,
// tripping MG_ASSERT(acc.remove(gid)) in EdgeMetadataIndex on the second pass.
TEST(LightEdgesAbort, AbortCreationWithMetadataNoDoubleRemove) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge then abort (don't commit).
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("TEMP");
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    // Accessor destructor aborts; metadata entry for the aborted edge is removed
    // exactly once (at GC), not here.
  }

  // GC processes the abort-routed Edge* in deleted_edges_: OnEdgesDeleted removes
  // its metadata entry. Pre-fix this MG_ASSERT-aborts (entry already removed at
  // abort time). Call twice to drain every batch.
  store->FreeMemory();
  store->FreeMemory();

  // Edge must not exist and the storage must be consistent (no crash above).
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesAbort, AbortDeletion) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("PERM");
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete edge then abort
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    auto del = acc->DeleteEdge(&edge);
    ASSERT_TRUE(del.has_value());
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    // Don't commit - abort
  }

  // Edge should still exist
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 1);
  }
}

TEST(LightEdgesAbort, AbortCreateAndDeleteInSameTxn) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create + delete in same txn, then abort
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("EPHEMERAL");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 1);

    auto del = acc->DeleteEdge(&*res);
    ASSERT_TRUE(del.has_value());
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    // Abort
  }

  // Nothing should exist
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

// ===========================================================================
// 6. MVCC + GC (graveyard)
// ===========================================================================

TEST(LightEdgesMVCC, ConcurrentTxnSeesDeletedEdge) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("REL");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());

    // Set a property so we can verify the Edge* is valid
    auto prop = acc->NameToProperty("val");
    ASSERT_TRUE(res->SetProperty(prop, PropertyValue(42)).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Start reader txn BEFORE the delete
  auto reader = store->Access(WRITE);
  auto vf_reader = reader->FindVertex(gid_from, View::OLD);
  ASSERT_TRUE(vf_reader);
  ASSERT_EQ(vf_reader->OutEdges(View::OLD)->edges.size(), 1);

  // Delete edge in a separate txn
  {
    auto writer = store->Access(WRITE);
    auto vf = writer->FindVertex(gid_from, View::NEW);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(writer->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // GC runs but reader is still active -> graveyard shouldn't drain
  store->FreeMemory();

  // Reader should still see the edge with correct properties
  {
    auto out = vf_reader->OutEdges(View::OLD);
    ASSERT_EQ(out->edges.size(), 1);
    auto prop = reader->NameToProperty("val");
    auto val = out->edges[0].GetProperty(prop, View::OLD);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val->ValueInt(), 42);
  }

  // Close reader
  reader.reset();

  // Now GC should drain the graveyard
  store->FreeMemory();

  // Edge is gone for new transactions
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesMVCC, GCDrainsGraveyardAfterAllReadersGone) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create and delete multiple edges
  constexpr int kEdges = 10;
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("MULTI");
    for (int i = 0; i < kEdges; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete all edges
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    for (int i = 0; i < kEdges; ++i) {
      auto edge = vf->OutEdges(View::NEW).value().edges[0];
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Run GC
  store->FreeMemory();

  // Verify edges are gone
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

// ===========================================================================
// 6b. Graveyard-specific tests
// ===========================================================================

// Regression: with enable_edges_metadata + light edges, deleting an edge and
// running GC routes the Edge* through CollectGarbage (which removes the
// edges-metadata entry via OnEdgesDeleted) and then through
// DrainLightEdgeGraveyard in the SAME FreeMemory() cycle. Before the fix the
// drain re-called OnEdgeDeleted(gid) on the already-removed entry, tripping the
// `MG_ASSERT(acc.remove(gid))` in edge_metadata_index.hpp (double-remove abort).
// After the fix the drain only frees memory and the test runs clean; we also
// create+delete a second edge afterwards to prove the metadata index stays
// consistent (a stale double-removed entry would corrupt later inserts).
TEST(LightEdgesGraveyard, EdgesMetadataConsistentThroughDeleteAndDrain) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create + commit a batch of edges.
  constexpr int kEdges = 5;
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("META");
    for (int i = 0; i < kEdges; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete + commit all of them.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    for (int i = 0; i < kEdges; ++i) {
      auto edge = vf->OutEdges(View::NEW).value().edges[0];
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // GC collects the deleted light edges: CollectGarbage removes their metadata
  // entries and pushes the Edge* to the graveyard, then DrainLightEdgeGraveyard
  // frees them in the same cycle. Without the fix this MG_ASSERT-aborts on the
  // double OnEdgeDeleted. Call twice to be sure every batch drains.
  store->FreeMemory();
  store->FreeMemory();

  // Edge is gone.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }

  // Subsequent create/delete/GC must still work: a corrupted metadata index
  // (from a stale double-removed entry) would surface as an abort or a wrong
  // edge view here.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("META2");
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 1);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  store->FreeMemory();
  store->FreeMemory();
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesGraveyard, MultipleBatchesAccumulateAndDrain) {
  // Delete edges across many separate transactions, each creating a graveyard batch.
  // Verify all batches drain after all readers are gone.
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  constexpr int kEdges = 20;

  // Create edges
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("BATCH");
    auto prop = acc->NameToProperty("idx");
    for (int i = 0; i < kEdges; ++i) {
      auto res = acc->CreateEdge(&*vf, &*vt, et);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res->SetProperty(prop, PropertyValue(i)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete edges one-by-one in separate transactions -> one graveyard batch each
  for (int i = 0; i < kEdges; ++i) {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto out = vf->OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    ASSERT_FALSE(out->edges.empty());
    auto edge = out->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // All edges deleted, verify vertex shows 0
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }

  // GC drains all batches (call twice for good measure)
  store->FreeMemory();
  store->FreeMemory();

  // Verify no crash on final state
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

// FastDiscard path: create+commit then delete+commit with no overlapping
// transaction (FastDiscard fires on the delete commit). Verify that after
// FreeMemory() the metadata index has no stale entry and FindEdge returns
// nullopt cleanly (no crash / UAF).
TEST(LightEdgesGraveyard, FastDiscardRouteKeepsMetadataConsistent) {
  auto store = MakeLightEdgeStorageWithMetadata();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  Gid edge_gid = Gid::FromUint(std::numeric_limits<uint64_t>::max());

  // Step 1: create + commit the edge (no concurrent reader -> FastDiscard eligible).
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("FD");
    auto res = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Verify edge is visible.
  {
    auto acc = store->Access(READ);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 1);
    // FindEdge via accessor must succeed while edge is live.
    ASSERT_TRUE(acc->FindEdge(edge_gid, View::OLD).has_value());
  }

  // Step 2: delete + commit (no concurrent reader -> FastDiscard fires, Edge*
  // routed through deleted_edges_ into CollectGarbage, NOT directly to graveyard).
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 1);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_EQ(vf->OutEdges(View::NEW)->edges.size(), 0);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Step 3: run GC twice — CollectGarbage picks up deleted_edges_, removes
  // metadata entry, pushes to graveyard; DrainLightEdgeGraveyard frees.
  // Two FreeMemory() calls ensure every GC phase completes.
  store->FreeMemory();
  store->FreeMemory();

  // Step 4: verify no stale metadata entry and no crash (no UAF).
  {
    auto acc = store->Access(READ);
    // Vertex must see zero edges.
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
    // FindEdge must return nullopt cleanly — stale metadata entry would either
    // abort (double-remove assert) or return a dangling pointer.
    ASSERT_FALSE(acc->FindEdge(edge_gid, View::OLD).has_value());
  }
}

// ===========================================================================
// 10. Storage Cleanup
// ===========================================================================

TEST(LightEdgesCleanup, DestructorFreesGraveyardEdges) {
  // Verify no crash when edges are in the graveyard at destruction time
  {
    auto store = MakeLightEdgeStorage();
    auto [gid_from, gid_to] = CreateTwoVertices(store.get());

    // Create edges
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      auto vt = acc->FindVertex(gid_to, View::NEW);
      auto et = acc->NameToEdgeType("GRAVE");
      for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Delete all edges (they go to graveyard)
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      for (int i = 0; i < 50; ++i) {
        auto edge = vf->OutEdges(View::NEW).value().edges[0];
        ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // DON'T run GC - leave edges in graveyard
    // Store destruction should clean them up
  }
  // If we get here without errors, graveyard cleanup worked
}

// Regression: with the collection-time routing, a DELETE+commit (without any
// subsequent GC/FreeMemory) leaves the deleted light Edge* sitting in
// deleted_edges_ — they are NOT pushed to the graveyard until a GC cycle swaps
// deleted_edges_ and snaps the graveyard watermark. ClearLightEdges() (run from
// ~InMemoryStorage) must drain deleted_edges_ via LightEdgePool::Destroy or those
// pool-allocated Edge* leak. Under ASan this test fails on the leak if the drain
// is missing; under RelWithDebInfo it at least exercises the teardown path.
TEST(LightEdgesCleanup, DeletedButNotGCedEdgesFreedAtTeardown) {
  {
    auto store = MakeLightEdgeStorage();
    auto [gid_from, gid_to] = CreateTwoVertices(store.get());

    // Create + commit edges.
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      auto vt = acc->FindVertex(gid_to, View::NEW);
      auto et = acc->NameToEdgeType("LEAK");
      for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // DELETE + commit all edges. These go to deleted_edges_, NOT the graveyard.
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      while (true) {
        auto out = vf->OutEdges(View::NEW);
        if (!out.has_value() || out->edges.empty()) break;
        auto edge = out->edges[0];
        ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Deliberately DO NOT run GC/FreeMemory: the deleted Edge* stay in
    // deleted_edges_ and never reach the graveyard. Store destruction
    // (~InMemoryStorage -> ClearLightEdges) must free them.
  }
  // If we get here without ASan leak/UAF errors, the deleted_edges_ drain worked.
}

// Regression, Clear() path: same scenario as above but the deleted
// light edges are reclaimed through InMemoryStorage::Clear() (reachable in
// analytical mode via the storage clearing path) rather than the destructor.
TEST(LightEdgesCleanup, DeletedButNotGCedEdgesFreedAtClear) {
  auto store = MakeLightEdgeStorage();
  auto *mem_storage = static_cast<InMemoryStorage *>(store.get());

  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create + commit edges.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("LEAKC");
    for (int i = 0; i < 100; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // DELETE + commit all edges -> deleted_edges_ (not graveyard, no GC run).
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    while (true) {
      auto out = vf->OutEdges(View::NEW);
      if (!out.has_value() || out->edges.empty()) break;
      auto edge = out->edges[0];
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // DropGraph routes through ClearLightEdges (analytical). Use it as the
  // reachable Clear()-family path: it must drain deleted_edges_.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = store->UniqueAccess();
    acc->DropGraph();
    // No ASan leak/UAF = deleted_edges_ drained on the clear path.
  }
}

// ===========================================================================
// 12. DropGraph tests
// ===========================================================================

// DropGraph with edges in the graveyard (deleted but not yet GC'd).
TEST(LightEdgesCleanup, DropGraphWithGraveyardEdges) {
  auto store = MakeLightEdgeStorage();
  auto *mem_storage = static_cast<InMemoryStorage *>(store.get());

  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create and then delete edges so they sit in the graveyard.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto etype = acc->NameToEdgeType("T");
    for (int idx = 0; idx < 50; ++idx) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, etype).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    while (true) {
      auto out = vf->OutEdges(View::NEW);
      if (!out.has_value() || out->edges.empty()) break;
      auto edge = out->edges[0];
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  // Deliberately skip GC so edges remain in graveyard.

  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = store->UniqueAccess();
    acc->DropGraph();
    // No crash / ASAN error = graveyard was drained and pool reclaimed.
  }
}

TEST(LightEdgesGraveyard, PartialDrainMultipleBatches) {
  // Create edges in separate transactions so they get different graveyard timestamps.
  // Hold a reader between the two deletes so only the first batch drains.
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create 2 edges in separate txns
  Gid edge1_gid, edge2_gid;
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("GY");
    auto r1 = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(r1.has_value());
    edge1_gid = r1->Gid();
    auto r2 = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(r2.has_value());
    edge2_gid = r2->Gid();

    auto prop = acc->NameToProperty("batch");
    ASSERT_TRUE(r1->SetProperty(prop, PropertyValue(1)).has_value());
    ASSERT_TRUE(r2->SetProperty(prop, PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete edge1 in txn A (graveyard batch 1)
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto out = vf->OutEdges(View::NEW);
    // Find edge1 specifically
    for (auto &e : out->edges) {
      if (e.Gid() == edge1_gid) {
        ASSERT_TRUE(acc->DeleteEdge(&e).has_value());
        break;
      }
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // GC to advance timestamps and drain batch 1
  store->FreeMemory();

  // Now open a reader that pins current state (edge2 still alive)
  auto reader = store->Access(WRITE);

  // Delete edge2 in txn B (graveyard batch 2)
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto out = vf->OutEdges(View::NEW);
    ASSERT_TRUE(!out->edges.empty());
    auto edge = out->edges[0];
    ASSERT_EQ(edge.Gid(), edge2_gid);
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // GC: batch 2 should NOT drain because reader is still active
  store->FreeMemory();

  // Reader should still see edge2 with valid properties
  {
    auto vf = reader->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_TRUE(out.has_value());
    // Reader started after edge1 was deleted but before edge2 was deleted,
    // so it sees only edge2
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge2_gid);
    auto prop = reader->NameToProperty("batch");
    auto val = out->edges[0].GetProperty(prop, View::OLD);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val->ValueInt(), 2);
  }

  // Close reader, GC should now drain batch 2
  reader.reset();
  store->FreeMemory();

  // Both edges gone
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesGraveyard, ConcurrentDeleteAndGC) {
  // Multiple threads delete edges while GC runs concurrently.
  // Verify no crash and final state is consistent.
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  constexpr int kEdges = 100;

  // Create many edges
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("CONC");
    for (int i = 0; i < kEdges; ++i) {
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete edges from multiple threads while GC runs
  std::atomic<bool> gc_running{true};
  std::atomic<uint64_t> total_deleted{0};

  std::thread gc_thread([&]() {
    while (gc_running.load()) {
      store->FreeMemory();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    store->FreeMemory();
    store->FreeMemory();
  });

  constexpr int kDeleteThreads = 4;
  std::vector<std::thread> deleters;
  deleters.reserve(kDeleteThreads);
  for (int t = 0; t < kDeleteThreads; ++t) {
    deleters.emplace_back([&]() {
      while (true) {
        auto acc = store->Access(WRITE);
        auto vf = acc->FindVertex(gid_from, View::NEW);
        if (!vf) break;
        auto out = vf->OutEdges(View::NEW);
        if (!out.has_value() || out->edges.empty()) break;

        auto edge = out->edges[0];
        auto del = acc->DeleteEdge(&edge);
        if (!del.has_value()) continue;  // serialization conflict, retry

        if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
          total_deleted.fetch_add(1);
        }
      }
    });
  }

  for (auto &d : deleters) d.join();
  gc_running.store(false);
  gc_thread.join();

  // All edges should be deleted
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
  EXPECT_EQ(total_deleted.load(), kEdges);
}

TEST(LightEdgesGraveyard, ReaderPinsMultipleGraveyardBatches) {
  // A long-lived reader pins all graveyard batches. After the reader closes,
  // GC drains everything in one pass.
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  constexpr int kEdges = 5;

  // Create edges
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto et = acc->NameToEdgeType("PIN");
    auto prop = acc->NameToProperty("val");
    for (int i = 0; i < kEdges; ++i) {
      auto res = acc->CreateEdge(&*vf, &*vt, et);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res->SetProperty(prop, PropertyValue(i * 10)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Open a reader that sees all edges
  auto reader = store->Access(WRITE);
  {
    auto vf = reader->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), kEdges);
  }

  // Delete all edges in separate txns (creates separate graveyard batches)
  for (int i = 0; i < kEdges; ++i) {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto out = vf->OutEdges(View::NEW);
    ASSERT_FALSE(out->edges.empty());
    auto edge = out->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // GC multiple times - nothing should drain because reader is active
  for (int i = 0; i < 3; ++i) {
    store->FreeMemory();
  }

  // Reader still sees all edges with valid properties
  {
    auto vf = reader->FindVertex(gid_from, View::OLD);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_EQ(out->edges.size(), kEdges);
    auto prop = reader->NameToProperty("val");
    for (const auto &e : out->edges) {
      auto val = e.GetProperty(prop, View::OLD);
      ASSERT_TRUE(val.has_value());
      // Just verify the property is readable (value is an int multiple of 10)
      ASSERT_TRUE(val->IsInt());
    }
  }

  // Close reader, GC drains all batches
  reader.reset();
  store->FreeMemory();

  // All edges gone
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_EQ(vf->OutEdges(View::OLD)->edges.size(), 0);
  }
}

TEST(LightEdgesGraveyard, PostCommitIndexIterableBlocksDrain) {
  // Regression test for collection-time graveyard watermark.
  //
  // The bug: a deleted light edge's guard_epoch used to be snapped at COMMIT
  // time (in FastDiscard) / ABORT time, before any post-commit index iterable
  // could exist — so guard_epoch was 0. A reader that then started an index
  // iterable (epoch id 0) was NOT >= guard_epoch, so the graveyard drained on
  // the next GC and freed the Edge* out from under the live iterable (UAF).
  //
  // The fix: light edges no longer push to the graveyard at commit/abort time.
  // They defer through deleted_edges_ into CollectGarbage, which snaps
  // guard_epoch = CurrentEpoch() at GC-COLLECTION time — after
  // RemoveObsoleteEdgeEntries removed the stale index entry, and after any
  // post-commit reader has Acquired its epoch. The collection-time watermark is
  // therefore > the iterable's epoch id, so IsSafeToFree stays false until the
  // iterable is destroyed. (This mirrors heavy edges, whose skiplist-collection
  // watermark is likewise always >= any iterable opened since the commit.)
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Create edge type index.
  EdgeTypeId et;
  {
    auto acc = store->UniqueAccess();
    et = acc->NameToEdgeType("REL");
    ASSERT_TRUE(acc->CreateIndex(et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Create edge.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete the edge — defers the Edge* into deleted_edges_ (no graveyard push
  // yet; the graveyard watermark is snapped later at GC-collection time).
  {
    auto writer = store->Access(WRITE);
    auto vf = writer->FindVertex(gid_from, View::NEW);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(writer->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Start a post-commit reader.  It acquires epoch id e_R (the delete has not
  // been collected yet, so no graveyard watermark exists for it).
  auto reader = store->Access(WRITE);
  // Constructing the iterable acquires epoch e_R from light_edge_iterable_tracker_.
  // The iterable is kept alive for the rest of the test to hold the epoch guard.
  auto iterable = std::make_optional(reader->Edges(et, View::OLD));

  // GC cycle 1: CollectGarbage swaps the edge out of deleted_edges_,
  // RemoveObsoleteEdgeEntries removes the stale index entry, then the
  // collection-time light arm snaps guard_epoch = CurrentEpoch() > e_R, so
  // IsSafeToFree(guard_epoch) stays false while e_R is live and the Edge* is NOT
  // freed. Iterating the index iterable and dereferencing each Edge would be a
  // use-after-free (ASan abort) had the drain freed it prematurely.
  auto probe_iterable = [&] {
    for (auto edge : *iterable) {
      (void)edge.Gid();
      (void)edge.EdgeType();
    }
  };
  store->FreeMemory();
  probe_iterable();

  // A second GC cycle must also leave the Edge* alive.
  store->FreeMemory();
  probe_iterable();

  // Destroying the iterable releases e_R, so the graveyard may now drain.
  iterable.reset();  // Note: Result<Iterable> — reset the optional
  reader.reset();
  store->FreeMemory();

  // The deleted edge must no longer be visible to a fresh transaction.
  {
    auto checker = store->Access(WRITE);
    auto vf = checker->FindVertex(gid_from, View::NEW);
    ASSERT_TRUE(vf.has_value());
    auto out = vf->OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 0U) << "deleted light edge must no longer be visible after drain";
  }
}

// UAF realization test for the commit fast-path (FastDiscard).
//
// It ITERATES the post-commit index iterable AFTER a FreeMemory drain and
// dereferences each Edge (via .Gid(), which reads edge_.ptr->gid). With the old
// commit-time guard_epoch snap (guard_epoch == 0) the drain would free the Edge*
// here, so this iteration would be a use-after-free that ASan catches. With the
// collection-time watermark the Edge* is kept alive, so the iteration is valid.
TEST(LightEdgesGraveyard, PostCommitIterateAfterDrainNoUseAfterFreeFastDiscard) {
  auto store = MakeLightEdgeStorage();
  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  EdgeTypeId et;
  {
    auto acc = store->UniqueAccess();
    et = acc->NameToEdgeType("REL");
    ASSERT_TRUE(acc->CreateIndex(et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  Gid edge_gid{};
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto e = acc->CreateEdge(&*vf, &*vt, et);
    ASSERT_TRUE(e.has_value());
    edge_gid = e->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Delete + commit. The commit fast-path (FastDiscard) defers the Edge* to
  // deleted_edges_; no graveyard push happens yet, so no early guard_epoch == 0.
  {
    auto writer = store->Access(WRITE);
    auto vf = writer->FindVertex(gid_from, View::NEW);
    auto edge = vf->OutEdges(View::NEW).value().edges[0];
    ASSERT_TRUE(writer->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Post-commit reader: opens an edge-type-index iterable that still references
  // the deleted-but-not-yet-collected Edge*. Its epoch guard must block the free.
  auto reader = store->Access(WRITE);
  auto iterable = std::make_optional(reader->Edges(et, View::OLD));

  // Drain attempt. With the early-snap bug this frees the Edge* now (guard_epoch
  // was 0). With the fix, the collection-time watermark keeps it alive.
  store->FreeMemory();

  // ITERATE and dereference every Edge in the iterable. If the Edge* were freed
  // by the drain above, .Gid() would be a use-after-free (ASan abort). We accept
  // any gid value; the point is that the dereference must touch live memory.
  std::size_t seen = 0;
  for (auto edge : *iterable) {
    (void)edge.Gid();
    (void)edge.EdgeType();
    ++seen;
  }
  // The deleted edge may or may not pass the OLD-view filter; either way the
  // iteration must not crash. Touch is what matters for the UAF probe.
  (void)seen;

  // Release the reader; now the graveyard may drain.
  iterable.reset();
  reader.reset();
  store->FreeMemory();

  // The deleted edge must no longer be visible to a fresh transaction.
  {
    auto checker = store->Access(WRITE);
    auto vf = checker->FindVertex(gid_from, View::NEW);
    ASSERT_TRUE(vf.has_value());
    auto out = vf->OutEdges(View::NEW);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 0U) << "deleted light edge must no longer be visible after drain";
  }
}

// ===========================================================================
// 7. FindEdge
// ===========================================================================

// FindEdge is a private method tested indirectly through WAL recovery,
// snapshot recovery, and replication. We test the public edge-lookup behavior
// here (via OutEdges/InEdges on vertices).

TEST(LightEdgesStress, ConcurrentCrudWithIndices) {
  constexpr int kVertices = 100;
  constexpr int kWriterThreads = 4;
  constexpr int kReaderThreads = 4;
  constexpr int kWriterIterations = 200;

  auto store = MakeLightEdgeStorage();

  // --- Setup: create vertices ---
  std::vector<Gid> vertex_gids;
  vertex_gids.reserve(kVertices);
  {
    auto acc = store->Access(WRITE);
    for (int i = 0; i < kVertices; ++i) {
      auto v = acc->CreateVertex();
      vertex_gids.push_back(v.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // --- Setup: create edge indices (requires UniqueAccess, single-threaded) ---

  EdgeTypeId stress_et;
  PropertyId weight_prop;

  // Edge type index on STRESS_ET
  {
    auto acc = store->UniqueAccess();
    stress_et = acc->NameToEdgeType("STRESS_ET");
    ASSERT_TRUE(acc->CreateIndex(stress_et).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Edge type + property index on (STRESS_ET, weight)
  {
    auto acc = store->UniqueAccess();
    stress_et = acc->NameToEdgeType("STRESS_ET");
    weight_prop = acc->NameToProperty("weight");
    ASSERT_TRUE(acc->CreateIndex(stress_et, weight_prop).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Global edge property index on weight
  {
    auto acc = store->UniqueAccess();
    weight_prop = acc->NameToProperty("weight");
    ASSERT_TRUE(acc->CreateGlobalEdgeIndex(weight_prop).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Vector edge index on (VEC_ET, embedding) with dimension=4
  EdgeTypeId vec_et;
  PropertyId embedding_prop;
  {
    auto acc = store->UniqueAccess();
    vec_et = acc->NameToEdgeType("VEC_ET");
    embedding_prop = acc->NameToProperty("embedding");
    VectorEdgeIndexSpec spec{
        .index_name = "stress_vec_idx",
        .edge_type_filter =
            memgraph::storage::VectorEdgeTypeFilter{memgraph::storage::VectorMatchMode::SINGLE, {vec_et}},
        .property = embedding_prop,
        .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
        .dimension = 4,
        .resize_coefficient = 2,
        .capacity = static_cast<std::size_t>(kWriterThreads * kWriterIterations),
        .scalar_kind = unum::usearch::scalar_kind_t::f32_k};
    ASSERT_TRUE(acc->CreateVectorEdgeIndex(spec).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // --- Concurrent phase ---
  std::atomic<bool> running{true};
  std::atomic<int> writers_done{0};
  std::atomic<uint64_t> total_creates{0};
  std::atomic<uint64_t> total_deletes{0};
  std::atomic<uint64_t> total_updates{0};

  // Writer threads
  std::vector<std::thread> writers;
  writers.reserve(kWriterThreads);
  for (int t = 0; t < kWriterThreads; ++t) {
    writers.emplace_back([&, t]() {
      std::mt19937 rng(42 + t);
      std::uniform_int_distribution<int> vertex_dist(0, kVertices - 1);
      std::uniform_int_distribution<int> op_dist(0, 2);  // 0=create, 1=delete, 2=update
      std::uniform_int_distribution<int> weight_dist(1, 10000);
      std::uniform_real_distribution<float> float_dist(-1.0f, 1.0f);

      for (int i = 0; i < kWriterIterations; ++i) {
        int op = op_dist(rng);

        if (op == 0) {
          // CREATE edge with STRESS_ET + weight property
          auto acc = store->Access(WRITE);
          int from_idx = vertex_dist(rng);
          int to_idx = vertex_dist(rng);
          auto vf = acc->FindVertex(vertex_gids[from_idx], View::NEW);
          auto vt = acc->FindVertex(vertex_gids[to_idx], View::NEW);
          if (!vf || !vt) continue;

          auto et = acc->NameToEdgeType("STRESS_ET");
          auto res = acc->CreateEdge(&*vf, &*vt, et);
          if (!res.has_value()) continue;

          auto wp = acc->NameToProperty("weight");
          res->SetProperty(wp, PropertyValue(weight_dist(rng)));

          if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
            total_creates.fetch_add(1);
          }
        } else if (op == 1) {
          // DELETE an edge from a random vertex
          auto acc = store->Access(WRITE);
          int from_idx = vertex_dist(rng);
          auto vf = acc->FindVertex(vertex_gids[from_idx], View::NEW);
          if (!vf) continue;

          auto out = vf->OutEdges(View::NEW);
          if (!out.has_value() || out->edges.empty()) continue;

          auto edge = out->edges[0];
          auto del = acc->DeleteEdge(&edge);
          if (!del.has_value()) continue;

          if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
            total_deletes.fetch_add(1);
          }
        } else {
          // UPDATE property on an existing STRESS_ET edge
          auto acc = store->Access(WRITE);
          int from_idx = vertex_dist(rng);
          auto vf = acc->FindVertex(vertex_gids[from_idx], View::NEW);
          if (!vf) continue;

          auto et = acc->NameToEdgeType("STRESS_ET");
          auto out = vf->OutEdges(View::NEW, {et});
          if (!out.has_value() || out->edges.empty()) continue;

          auto edge = out->edges[0];
          auto wp = acc->NameToProperty("weight");
          auto set_res = edge.SetProperty(wp, PropertyValue(weight_dist(rng)));
          if (!set_res.has_value()) continue;

          if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
            total_updates.fetch_add(1);
          }
        }
      }

      // Also create some edges with VEC_ET + embedding property for vector index
      for (int i = 0; i < 10; ++i) {
        auto acc = store->Access(WRITE);
        int from_idx = vertex_dist(rng);
        int to_idx = vertex_dist(rng);
        auto vf = acc->FindVertex(vertex_gids[from_idx], View::NEW);
        auto vt = acc->FindVertex(vertex_gids[to_idx], View::NEW);
        if (!vf || !vt) continue;

        auto et = acc->NameToEdgeType("VEC_ET");
        auto res = acc->CreateEdge(&*vf, &*vt, et);
        if (!res.has_value()) continue;

        auto ep = acc->NameToProperty("embedding");
        std::vector<PropertyValue> vec_vals;
        vec_vals.reserve(4);
        for (int d = 0; d < 4; ++d) {
          vec_vals.emplace_back(static_cast<double>(float_dist(rng)));
        }
        res->SetProperty(ep, PropertyValue(std::move(vec_vals)));

        acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
      }

      if (writers_done.fetch_add(1) + 1 == kWriterThreads) {
        running.store(false);
      }
    });
  }

  // Reader threads
  std::vector<std::thread> readers;
  readers.reserve(kReaderThreads);
  for (int t = 0; t < kReaderThreads; ++t) {
    readers.emplace_back([&, t]() {
      std::mt19937 rng(1000 + t);
      std::uniform_int_distribution<int> vertex_dist(0, kVertices - 1);
      std::uniform_int_distribution<int> query_dist(0, 3);
      std::uniform_real_distribution<float> float_dist(-1.0f, 1.0f);

      while (running.load()) {
        int query = query_dist(rng);

        if (query == 0) {
          // Vertex scan: pick a random vertex, read its out-edges and properties
          auto acc = store->Access(WRITE);
          int idx = vertex_dist(rng);
          auto vf = acc->FindVertex(vertex_gids[idx], View::NEW);
          if (!vf) continue;
          auto out = vf->OutEdges(View::NEW);
          if (!out.has_value()) continue;
          for (const auto &edge : out->edges) {
            // Read properties to exercise Edge* dereferencing
            [[maybe_unused]] auto props = edge.Properties(View::NEW);
          }
        } else if (query == 1) {
          // Edge type index query
          auto acc = store->Access(WRITE);
          auto et = acc->NameToEdgeType("STRESS_ET");
          auto edges = acc->Edges(et, View::NEW);
          uint64_t count = 0;
          for ([[maybe_unused]] const auto &e : edges) {
            ++count;
          }
          // Just verify iteration works without crash
          (void)count;
        } else if (query == 2) {
          // Edge type + property index query
          auto acc = store->Access(WRITE);
          auto et = acc->NameToEdgeType("STRESS_ET");
          auto wp = acc->NameToProperty("weight");
          auto edges = acc->Edges(et, wp, View::NEW);
          uint64_t count = 0;
          for ([[maybe_unused]] const auto &e : edges) {
            ++count;
          }
          (void)count;
        } else {
          // Global edge property index query
          auto acc = store->Access(WRITE);
          auto wp = acc->NameToProperty("weight");
          auto edges = acc->Edges(wp, View::NEW);
          uint64_t count = 0;
          for ([[maybe_unused]] const auto &e : edges) {
            ++count;
          }
          (void)count;
        }
      }
    });
  }

  // GC thread
  std::thread gc_thread([&]() {
    while (running.load()) {
      store->FreeMemory();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // Final GC passes
    store->FreeMemory();
    store->FreeMemory();
  });

  // Join all threads
  for (auto &w : writers) w.join();
  for (auto &r : readers) r.join();
  gc_thread.join();

  // --- Verification ---
  // Count edges via vertex scan
  uint64_t vertex_scan_count = 0;
  {
    auto acc = store->Access(WRITE);
    for (const auto &gid : vertex_gids) {
      auto v = acc->FindVertex(gid, View::OLD);
      if (!v) continue;
      auto out = v->OutEdges(View::OLD);
      if (out.has_value()) {
        vertex_scan_count += out->edges.size();
      }
    }
  }

  // Count edges via edge type index
  uint64_t stress_et_index_count = 0;
  {
    auto acc = store->Access(WRITE);
    auto et = acc->NameToEdgeType("STRESS_ET");
    auto edges = acc->Edges(et, View::OLD);
    for ([[maybe_unused]] const auto &e : edges) {
      ++stress_et_index_count;
    }
  }

  // Count edges via edge type + property index
  uint64_t stress_et_prop_index_count = 0;
  {
    auto acc = store->Access(WRITE);
    auto et = acc->NameToEdgeType("STRESS_ET");
    auto wp = acc->NameToProperty("weight");
    auto edges = acc->Edges(et, wp, View::OLD);
    for ([[maybe_unused]] const auto &e : edges) {
      ++stress_et_prop_index_count;
    }
  }

  // Count edges via global edge property index
  uint64_t global_prop_index_count = 0;
  {
    auto acc = store->Access(WRITE);
    auto wp = acc->NameToProperty("weight");
    auto edges = acc->Edges(wp, View::OLD);
    for ([[maybe_unused]] const auto &e : edges) {
      ++global_prop_index_count;
    }
  }

  // Count VEC_ET edges via vertex scan
  uint64_t vec_et_count = 0;
  {
    auto acc = store->Access(WRITE);
    auto vet = acc->NameToEdgeType("VEC_ET");
    for (const auto &gid : vertex_gids) {
      auto v = acc->FindVertex(gid, View::OLD);
      if (!v) continue;
      auto out = v->OutEdges(View::OLD);
      if (!out.has_value()) continue;
      for (const auto &e : out->edges) {
        if (e.EdgeType() == vet) ++vec_et_count;
      }
    }
  }

  // Vector index search (just verify no crash, don't assert exact counts)
  {
    auto acc = store->Access(WRITE);
    std::vector<float> query_vec{0.1f, 0.2f, 0.3f, 0.4f};
    auto results = acc->VectorIndexSearchOnEdges("stress_vec_idx", 5, query_vec);
    // Results may be empty if all VEC_ET edges were deleted
    (void)results;
  }

  // Verify counts are consistent
  // stress_et_index_count should match STRESS_ET edges from vertex scan
  uint64_t stress_et_from_vertex_scan = vertex_scan_count - vec_et_count;
  EXPECT_EQ(stress_et_index_count, stress_et_from_vertex_scan);

  // edge type+property index count should equal edge type index count
  // (all STRESS_ET edges have the weight property set)
  EXPECT_EQ(stress_et_prop_index_count, stress_et_index_count);

  // global edge property index count should equal edge type+property index count
  // (only STRESS_ET edges have the weight property, VEC_ET edges have embedding)
  EXPECT_EQ(global_prop_index_count, stress_et_prop_index_count);

  // Sanity: we should have done some creates and deletes
  EXPECT_GT(total_creates.load(), 0u);
  EXPECT_GT(total_deletes.load(), 0u);
  EXPECT_GT(total_updates.load(), 0u);

  // Run final GC and verify no crash during cleanup
  store->FreeMemory();
  store->FreeMemory();
}

// ===========================================================================
// 12. DropGraph tests
// ===========================================================================

// DropGraph on a light-edge storage must free all edges (no
// ASAN/MSAN errors), leave the storage empty, and leave the pool reusable.

// A light edge created AND deleted in the same committed transaction, never GC'd.
// When this is the only transaction in flight, FastDiscardOfDeltas fires at
// commit time (no older/newer transactions), which routes the deleted Edge* into
// deleted_edges_; ClearLightEdges then drains it. This test verifies the basic
// DropGraph + ClearLightEdges path for the single-transaction case. The
// concurrent-transaction variant (FastDiscard blocked) is covered by
// DropGraphFreesDeltaChainOnlyEdgesWithConcurrentTxn below.
// NOTE: DropGraph does NOT call Clear() — it calls ClearLightEdges directly plus
// HarvestDeltaChainOnlyLightEdges.
TEST(LightEdgesCleanup, DropGraphFreesDeltaChainOnlyEdges) {
  auto store = MakeLightEdgeStorage();
  auto *mem_storage = static_cast<InMemoryStorage *>(store.get());

  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // In ONE transaction: create an edge and immediately delete it, then commit.
  // FastDiscard fires (only transaction), so the Edge* lands in deleted_edges_
  // after commit — ClearLightEdges drains it.
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::NEW);
    auto vt = acc->FindVertex(gid_to, View::NEW);
    auto etype = acc->NameToEdgeType("EPHEMERAL");
    auto edge_res = acc->CreateEdge(&*vf, &*vt, etype);
    ASSERT_TRUE(edge_res.has_value());
    // Delete the edge in the same transaction before committing.
    ASSERT_TRUE(acc->DeleteEdge(&*edge_res).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Deliberately do NOT call FreeMemory — GC must NOT have collected the delta
  // chain. In the single-txn case the edge is in deleted_edges_ (not the
  // graveyard), so ClearLightEdges will drain it.

  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = store->UniqueAccess();
    acc->DropGraph();
    // No ASan leak = ClearLightEdges drained deleted_edges_ (single-txn path).
  }

  // Storage must be fully empty and the pool reusable after DropGraph.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  {
    auto acc = store->Access(READ);
    int edge_count = 0;
    for (auto vertex : acc->Vertices(View::OLD)) {
      auto out = vertex.OutEdges(View::OLD);
      if (out.has_value()) edge_count += static_cast<int>(out->edges.size());
    }
    EXPECT_EQ(edge_count, 0);
  }

  // Pool must still be usable after DropGraph + Reclaim.
  auto [gf2, gt2] = CreateTwoVertices(store.get());
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gf2, View::NEW);
    auto vt = acc->FindVertex(gt2, View::NEW);
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, acc->NameToEdgeType("NEW")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// DropGraph must free delta-chain-only light edges even when FastDiscardOfDeltas
// was blocked by a concurrent older transaction.
//
// FastDiscardOfDeltas only fires when ALL of the following hold at txn B's
// commit time:
//   (a) OldestActive() == commit_ts_B  — B is the oldest still-active entry
//   (b) transaction_id_  == B.id + 1  — no newer transaction was ever opened
// Opening txn A BEFORE txn B ensures A's start_timestamp < B's commit_ts, so
// condition (a) fails: OldestActive() returns A's start_ts, not commit_ts_B.
// FastDiscard is blocked. B's deleted Edge* therefore stays in
// committed_transactions_ as a RECREATE_OBJECT delta (delta-chain-only).
// Without the HarvestDeltaChainOnlyLightEdges() call in DropGraph,
// committed_transactions_.clear() destroys the delta and loses the only handle
// to the pool-allocated Edge* — a memory leak. Under ASan this test reports a
// leak without the fix and is clean with it.
TEST(LightEdgesCleanup, DropGraphFreesDeltaChainOnlyEdgesWithConcurrentTxn) {
  auto store = MakeLightEdgeStorage();
  auto *mem_storage = static_cast<InMemoryStorage *>(store.get());

  auto [gid_from, gid_to] = CreateTwoVertices(store.get());

  // Open txn A first so its start_timestamp is registered in the commit log as
  // the oldest active entry. A does no writes — it only pins the commit log.
  auto acc_a = store->Access(WRITE);

  // Txn B: create and immediately delete an edge, then commit.
  // FastDiscard is blocked (see above), so the Edge* stays in
  // committed_transactions_ as a RECREATE_OBJECT delta-chain-only reference.
  {
    auto acc_b = store->Access(WRITE);
    auto vf = acc_b->FindVertex(gid_from, View::NEW);
    auto vt = acc_b->FindVertex(gid_to, View::NEW);
    auto etype = acc_b->NameToEdgeType("EPHEMERAL_CONCURRENT");
    auto edge_res = acc_b->CreateEdge(&*vf, &*vt, etype);
    ASSERT_TRUE(edge_res.has_value());
    ASSERT_TRUE(acc_b->DeleteEdge(&*edge_res).has_value());
    ASSERT_TRUE(acc_b->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Abort txn A WITHOUT calling FreeMemory so committed_transactions_ still
  // holds B's delta chain (the only owner of the deleted Edge*). Because GC never
  // ran, the edge is still a delta-chain-only reference — it never reached the
  // graveyard.
  acc_a->Abort();
  acc_a.reset();

  // DropGraph on the analytical accessor. Without
  // HarvestDeltaChainOnlyLightEdges (before committed_transactions_.clear()),
  // ASan reports a pool-allocated Edge* leak here.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = store->UniqueAccess();
    acc->DropGraph();
    // No ASan leak = HarvestDeltaChainOnlyLightEdges() ran before
    // committed_transactions_->clear() inside DropGraph (F2 fix).
  }

  // Storage must be fully empty after DropGraph.
  mem_storage->SetStorageMode(StorageMode::IN_MEMORY_TRANSACTIONAL);
  {
    auto acc = store->Access(READ);
    int edge_count = 0;
    for (auto vertex : acc->Vertices(View::OLD)) {
      auto out = vertex.OutEdges(View::OLD);
      if (out.has_value()) edge_count += static_cast<int>(out->edges.size());
    }
    EXPECT_EQ(edge_count, 0);
  }

  // Pool must still be usable after DropGraph + Reclaim.
  auto [gf3, gt3] = CreateTwoVertices(store.get());
  {
    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gf3, View::NEW);
    auto vt = acc->FindVertex(gt3, View::NEW);
    ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, acc->NameToEdgeType("NEW")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}
