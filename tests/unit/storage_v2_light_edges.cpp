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
