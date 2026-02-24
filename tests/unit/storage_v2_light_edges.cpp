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
#include <filesystem>
#include <thread>
#include <vector>

#include "dbms/database.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/file.hpp"
#include "utils/memory.hpp"

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
// 1. Pool Allocator Tests
// ===========================================================================

TEST(LightEdgesPoolAllocator, BasicAllocDealloc) {
  // Pool with block_size = 32, 64 blocks per chunk
  memgraph::utils::SingleSizeThreadSafePoolResource pool(32, 64);

  void *p1 = pool.allocate(32, 8);
  ASSERT_NE(p1, nullptr);

  void *p2 = pool.allocate(32, 8);
  ASSERT_NE(p2, nullptr);
  ASSERT_NE(p1, p2);

  // Deallocate and re-allocate: should reuse
  pool.deallocate(p1, 32, 8);
  void *p3 = pool.allocate(32, 8);
  ASSERT_NE(p3, nullptr);

  pool.deallocate(p2, 32, 8);
  pool.deallocate(p3, 32, 8);
}

TEST(LightEdgesPoolAllocator, MultiThreadedStress) {
  memgraph::utils::SingleSizeThreadSafePoolResource pool(32, 1024);
  constexpr int kThreads = 8;
  constexpr int kOpsPerThread = 10000;

  std::atomic<int> total_allocs{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&pool, &total_allocs] {
      std::vector<void *> ptrs;
      ptrs.reserve(kOpsPerThread);
      for (int i = 0; i < kOpsPerThread; ++i) {
        ptrs.push_back(pool.allocate(32, 8));
        ASSERT_NE(ptrs.back(), nullptr);
      }
      total_allocs.fetch_add(kOpsPerThread);
      // Free in reverse
      for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
        pool.deallocate(*it, 32, 8);
      }
    });
  }

  for (auto &t : threads) t.join();
  ASSERT_EQ(total_allocs.load(), kThreads * kOpsPerThread);
}

// ===========================================================================
// 2. Config Validation
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
// 8. Snapshot round-trip
// ===========================================================================

class LightEdgesSnapshotTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_directory_ = std::filesystem::temp_directory_path() / "mg_test_light_edges_snapshot";
    std::filesystem::remove_all(storage_directory_);
  }

  void TearDown() override { std::filesystem::remove_all(storage_directory_); }

  std::filesystem::path storage_directory_;
};

TEST_F(LightEdgesSnapshotTest, SnapshotRoundTrip) {
  Gid gid_from, gid_to, edge_gid;
  PropertyId prop_weight;

  // Create data and snapshot
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .snapshot_on_exit = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    gid_from = v1.Gid();
    gid_to = v2.Gid();

    auto et = acc->NameToEdgeType("SNAP_EDGE");
    auto res = acc->CreateEdge(&v1, &v2, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();

    prop_weight = acc->NameToProperty("weight");
    ASSERT_TRUE(res->SetProperty(prop_weight, PropertyValue(99.9)).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    // Database destructor triggers snapshot_on_exit
  }

  // Recover from snapshot
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .recover_on_startup = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto vt = acc->FindVertex(gid_to, View::OLD);
    ASSERT_TRUE(vt);

    auto out = vf->OutEdges(View::OLD);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge_gid);

    // Verify property survived
    prop_weight = acc->NameToProperty("weight");
    auto props = out->edges[0].Properties(View::OLD);
    ASSERT_TRUE(props.has_value());
    ASSERT_EQ(props->size(), 1);
    ASSERT_DOUBLE_EQ(props->at(prop_weight).ValueDouble(), 99.9);

    // Verify in_edges on to_vertex
    auto in = vt->InEdges(View::OLD);
    ASSERT_TRUE(in.has_value());
    ASSERT_EQ(in->edges.size(), 1);
    ASSERT_EQ(in->edges[0].Gid(), edge_gid);
  }
}

TEST_F(LightEdgesSnapshotTest, SnapshotRoundTripMultipleEdges) {
  constexpr int kEdges = 50;
  std::vector<Gid> edge_gids;

  // Create data
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .snapshot_on_exit = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto et = acc->NameToEdgeType("BULK");
    auto prop = acc->NameToProperty("idx");

    for (int i = 0; i < kEdges; ++i) {
      auto res = acc->CreateEdge(&v1, &v2, et);
      ASSERT_TRUE(res.has_value());
      ASSERT_TRUE(res->SetProperty(prop, PropertyValue(i)).has_value());
      edge_gids.push_back(res->Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Recover
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .recover_on_startup = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    // Find any vertex
    uint64_t total_out = 0;
    auto vertices = acc->Vertices(View::OLD);
    for (auto v : vertices) {
      auto out = v.OutEdges(View::OLD);
      if (out.has_value()) total_out += out->edges.size();
    }
    ASSERT_EQ(total_out, kEdges);
  }
}

// ===========================================================================
// 9. WAL round-trip
// ===========================================================================

class LightEdgesWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_directory_ = std::filesystem::temp_directory_path() / "mg_test_light_edges_wal";
    std::filesystem::remove_all(storage_directory_);
  }

  void TearDown() override { std::filesystem::remove_all(storage_directory_); }

  std::filesystem::path storage_directory_;
};

TEST_F(LightEdgesWalTest, WalCreateAndRecover) {
  Gid gid_from, gid_to, edge_gid;

  // Create data with WAL
  {
    Config config{
        .durability = {.storage_directory = storage_directory_,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(1)},
                       .wal_file_flush_every_n_tx = 1},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    gid_from = v1.Gid();
    gid_to = v2.Gid();
    auto et = acc->NameToEdgeType("WAL_EDGE");
    auto res = acc->CreateEdge(&v1, &v2, et);
    ASSERT_TRUE(res.has_value());
    edge_gid = res->Gid();

    auto prop = acc->NameToProperty("data");
    ASSERT_TRUE(res->SetProperty(prop, PropertyValue("wal_test")).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    // DB destructor flushes WAL
  }

  // Recover from WAL
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .recover_on_startup = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 1);
    ASSERT_EQ(out->edges[0].Gid(), edge_gid);

    auto prop = acc->NameToProperty("data");
    auto val = out->edges[0].GetProperty(prop, View::OLD);
    ASSERT_TRUE(val.has_value());
    ASSERT_EQ(val->ValueString(), "wal_test");
  }
}

TEST_F(LightEdgesWalTest, WalCreateDeleteAndRecover) {
  Gid gid_from, gid_to;

  // Create edge, then delete it - both recorded in WAL
  {
    Config config{
        .durability = {.storage_directory = storage_directory_,
                       .snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                       .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(1)},
                       .wal_file_flush_every_n_tx = 1},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    // Create vertices
    {
      auto acc = store->Access(WRITE);
      auto v1 = acc->CreateVertex();
      auto v2 = acc->CreateVertex();
      gid_from = v1.Gid();
      gid_to = v2.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Create edge
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      auto vt = acc->FindVertex(gid_to, View::NEW);
      auto et = acc->NameToEdgeType("TEMP");
      ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Delete edge
    {
      auto acc = store->Access(WRITE);
      auto vf = acc->FindVertex(gid_from, View::NEW);
      auto edge = vf->OutEdges(View::NEW).value().edges[0];
      ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
  }

  // Recover - edge should be gone
  {
    Config config{
        .durability = {.storage_directory = storage_directory_, .recover_on_startup = true},
        .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}},
    };
    memgraph::dbms::Database db{config};
    auto *store = db.storage();

    auto acc = store->Access(WRITE);
    auto vf = acc->FindVertex(gid_from, View::OLD);
    ASSERT_TRUE(vf);
    auto out = vf->OutEdges(View::OLD);
    ASSERT_TRUE(out.has_value());
    ASSERT_EQ(out->edges.size(), 0);
  }
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
