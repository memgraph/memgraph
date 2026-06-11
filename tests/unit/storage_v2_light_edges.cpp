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

#include "dbms/database.hpp"
#include "storage/v2/config.hpp"
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
