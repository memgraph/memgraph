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

#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using testing::UnorderedElementsAre;

namespace ms = memgraph::storage;

// TODO: The point of these is not to test GC fully, these are just simple
// sanity checks. These will be superseded by a more sophisticated stress test
// which will verify that GC is working properly in a multithreaded environment.

// A simple test trying to get GC to run while a transaction is still alive and
// then verify that GC didn't delete anything it shouldn't have.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2Gc, Sanity) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}}));

  std::vector<memgraph::storage::Gid> vertices;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    // Create some vertices, but delete some of them immediately.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->CreateVertex();
      vertices.push_back(vertex.Gid());
    }

    acc->AdvanceCommand();

    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      if (i % 5 == 0) {
        EXPECT_FALSE(!acc->DeleteVertex(&vertex.value()).has_value());
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex_old = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      auto vertex_new = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_TRUE(vertex_old.has_value());
      EXPECT_EQ(vertex_new.has_value(), i % 5 != 0);
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Verify existing vertices and add labels to some of them.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);

      if (vertex.has_value()) {
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i)).has_value());
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i + 1)).has_value());
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i + 2)).has_value());
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Verify labels.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);

      if (vertex.has_value()) {
        auto labels_old = vertex->Labels(memgraph::storage::View::OLD);
        EXPECT_TRUE(labels_old.has_value());
        EXPECT_TRUE(labels_old->empty());

        auto labels_new = vertex->Labels(memgraph::storage::View::NEW);
        EXPECT_TRUE(labels_new.has_value());
        EXPECT_THAT(labels_new.value(),
                    UnorderedElementsAre(memgraph::storage::LabelId::FromUint(3 * i),
                                         memgraph::storage::LabelId::FromUint(3 * i + 1),
                                         memgraph::storage::LabelId::FromUint(3 * i + 2)));
      }
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Add and remove some edges.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto from_vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      auto to_vertex = acc->FindVertex(vertices[(i + 1) % 1000], memgraph::storage::View::OLD);
      EXPECT_EQ(from_vertex.has_value(), i % 5 != 0);
      EXPECT_EQ(to_vertex.has_value(), (i + 1) % 5 != 0);

      if (from_vertex.has_value() && to_vertex.has_value()) {
        EXPECT_FALSE(
            !acc->CreateEdge(&from_vertex.value(), &to_vertex.value(), memgraph::storage::EdgeTypeId::FromUint(i))
                 .has_value());
      }
    }

    // Detach delete some vertices.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);
      if (vertex.has_value()) {
        if (i % 3 == 0) {
          EXPECT_FALSE(!acc->DetachDeleteVertex(&vertex.value()).has_value());
        }
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Vertify edges.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0 && i % 3 != 0);
      if (vertex.has_value()) {
        auto out_edges = vertex->OutEdges(memgraph::storage::View::NEW)->edges;
        if (i % 5 != 4 && i % 3 != 2) {
          EXPECT_EQ(out_edges.size(), 1);
          EXPECT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
          EXPECT_EQ(out_edges.at(0).EdgeType().AsUint(), i);
        } else {
          EXPECT_TRUE(out_edges.empty());
        }

        auto in_edges = vertex->InEdges(memgraph::storage::View::NEW)->edges;
        if (i % 5 != 1 && i % 3 != 1) {
          EXPECT_EQ(in_edges.size(), 1);
          EXPECT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
          EXPECT_EQ(in_edges.at(0).EdgeType().AsUint(), (i + 999) % 1000);
        } else {
          EXPECT_TRUE(in_edges.empty());
        }
      }
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// A simple sanity check for index GC:
// 1. Start transaction 0, create some vertices, add a label to them and
//    commit.
// 2. Start transaction 1.
// 3. Start transaction 2, remove the labels and commit;
// 4. Wait for GC. GC shouldn't remove the vertices from index because
//    transaction 1 can still see them with that label.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2Gc, Indices) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}}));
  {
    auto unique_acc = storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(storage->NameToLabel("label")).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc0->CreateVertex();
      ASSERT_TRUE(*vertex.AddLabel(acc0->NameToLabel("label")));
    }
    ASSERT_TRUE(acc0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc1 = storage->Access(memgraph::storage::WRITE);

    auto acc2 = storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc2->Vertices(memgraph::storage::View::OLD)) {
      ASSERT_TRUE(*vertex.RemoveLabel(acc2->NameToLabel("label")));
    }
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::set<memgraph::storage::Gid> gids;
    for (auto vertex : acc1->Vertices(acc1->NameToLabel("label"), memgraph::storage::View::OLD)) {
      gids.insert(vertex.Gid());
    }
    EXPECT_EQ(gids.size(), 1000);
  }
}

TEST(StorageV2Gc, InterleavedDeltasWithCommittedContributorsAreGarbagedCollected) {
  // Need a periodic garbage collector so that certain fast path optimisations
  // aren't taken when cleaning deltas, but with an inter-collection pause large
  // enough that we can step through when debugging, etc, without anything being
  // unexpectedly reclaimed from underneath us.
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();  // older accessor is just used to stop GC.
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.HasValue());

    // The 6 unreleased deltas are:
    // - 2 x CREATE_OBJECT, to create the edges
    // - 2 x ADD_IN_EDGE
    // - 2 x ADD_OUT_EDGE
    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // Commit in order `acc1` and `acc2`. This means that even though `acc2` does
    // have interleaved deltas, everything downstream from them is committed
    // and so garbage collection can skip the waiting list.
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();
    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, InterleavedDeltasWithAbortedContributorsAreGarbagedCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.HasValue());

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();
    acc1->Abort();
    acc1.reset();
    acc0.reset();

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  // First GC: moves `waiting_gc_deltas_` to `aborted_transactions_` or
  // `committed_transactions_`
  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Second GC: committed deltas are unlinked, and all deltas (be they committed
  // or aborted) move to `garbage_undo_buffers_` for reclamation.
  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, InterleavedDeltasWithMultipleAbortsAreGarbageCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.HasValue());

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // Both transactions abort - all deltas should be cleaned up
    acc2->Abort();
    acc2.reset();
    acc1->Abort();

    acc1.reset();
    acc0.reset();

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  // First GC: moves from waiting_gc_deltas_ to aborted_transactions_
  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Second GC: moves to garbage_undo_buffers_
  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Third GC: frees the deltas
  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, DownstreamDeltaChainsAreGarbageCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();
    auto acc3 = storage->Access();

    // Create three edges to form downstream delta chain: TX1 -> TX2 -> TX3
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).HasValue());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).HasValue());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // Commit TX1, abort TX2 and TX3
    // TX3's deltas are downstream from TX2, which are downstream from TX1
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();
    acc3->Abort();
    acc3.reset();
    acc2->Abort();
    acc2.reset();
    acc0.reset();

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  // Multiple GC cycles to process the downstream chain
  for (int i = 0; i < 4; ++i) {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, MixedCommitAbortCommitInterleavedDeltasAreGarbageCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();
    auto acc3 = storage->Access();

    // Create interleaved deltas with mixed commit/abort pattern
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).HasValue());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).HasValue());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // TX1 commits, TX2 aborts, TX3 commits
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();
    acc2->Abort();
    acc2.reset();
    ASSERT_FALSE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc3.reset();
    acc0.reset();

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  // Multiple GC cycles to handle mixed commit/abort
  for (int i = 0; i < 4; ++i) {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, InterleavedDeltasWithTwoContributorsAreGarbagedCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();
    auto acc3 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.HasValue());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    auto edge3_result = acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3"));
    ASSERT_TRUE(edge3_result.HasValue());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_FALSE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc3.reset();
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();
    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, InterleavedDeltasWithUncommittedContributorsAreGarbagedCollected) {
  // Use periodic GC with short interval to automatically test intermediate states
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();  // older accessor is just used to stop GC.
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).HasValue());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).HasValue());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).HasValue());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // When acc2 commits, its transaction has interleaved deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc2 committed but acc1 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc1
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, InterleavedDeltasWithUncommittedContributorsAreGarbagedCollected_SwapCommitOrder) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc0 = storage->Access();  // older accessor is just used to stop GC.
    auto acc1 = storage->Access();
    auto acc2 = storage->Access();

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).HasValue());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).HasValue());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).HasValue());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).HasValue());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // When acc1 commits first, its transaction has interleaved deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc1 committed but acc2 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc2
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, ConcurrentEdgeOperationsAbortDeleteRepeat) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto tx1 = storage->Access();
  auto tx2 = storage->Access();

  {
    auto v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx1->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx1->CreateEdge(&v1, &v2, tx1->NameToEdgeType("Edge1")).HasValue());
  }

  {
    auto v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx2->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx2->CreateEdge(&v1, &v2, tx2->NameToEdgeType("Edge2")).HasValue());
  }

  tx1->Abort();
  tx2->Abort();

  // Wait for GC to clean up
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  {
    auto reader = storage->Access();
    auto v1 = reader->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2 = reader->FindVertex(v2_gid, memgraph::storage::View::OLD);

    if (v1.has_value()) {
      auto edges = v1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.HasValue());
    }
    if (v2.has_value()) {
      auto edges = v2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.HasValue());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2Gc, RapidGcOutOfOrderCommitTimestamps) {
  memgraph::storage::Gid vertex_gid;

  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}}));

  {
    auto acc = storage->Access();
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // blocker transaction prevents rapid GC
  auto blocker = storage->Access();

  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_a;
  {
    accessor_a = storage->Access();
    auto v = accessor_a->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_FALSE(v->AddLabel(accessor_a->NameToLabel("LabelA")).HasError());
    ASSERT_FALSE(accessor_a->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto accessor_b = storage->Access();
    auto v = accessor_b->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_FALSE(v->AddLabel(accessor_b->NameToLabel("LabelB")).HasError());

    ASSERT_FALSE(accessor_b->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  accessor_a.reset();

  // Destroy blocker to allow rapid GC
  blocker.reset();

  {
    auto gc_trigger = storage->Access();
    gc_trigger->CreateVertex();
    ASSERT_FALSE(gc_trigger->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto reader = storage->Access();
    auto v = reader->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());

    auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.HasValue());
    EXPECT_EQ(labels->size(), 2);
  }
}

// Tests for abort with interleaved deltas - verifying internal state after abort
// Case 1: tx10 aborting with no interleaved deltas
// Chain: (v)-[tx10]-[tx10]
// After abort: vertex->delta should be nullptr, vertex->out_edges should be empty
TEST(StorageV2Gc, AbortNoInterleavedDeltas_Case1) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc = storage->Access();
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    auto edge1 = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1.HasValue());
    auto edge2 = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2.HasValue());

    // Before abort: vertex should have deltas and out_edges
    ASSERT_NE(v1->vertex_->delta, nullptr);
    ASSERT_EQ(v1->vertex_->out_edges.size(), 2);

    acc->Abort();

    // After abort: vertex->delta should be nullptr, out_edges should be empty
    EXPECT_EQ(v1->vertex_->delta, nullptr);
    EXPECT_EQ(v1->vertex_->out_edges.size(), 0);
  }
}

// Case 2: tx10 aborting with interleaved deltas, has downstream deltas from other tx
// Chain: (v)-[tx10 intr]-[tx10 intr]-[tx9]
// After tx10 abort: vertex->delta should point to tx9's delta, out_edges should only have tx9's edge
TEST(StorageV2Gc, AbortWithDownstreamInterleaved_Case2) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx9 starts first, creates edge first (will be downstream in delta chain)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // tx10 starts after, creates edges (will be at head, interleaved above tx9)
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // Chain is now: (v1)-[tx10]-[tx10]-[tx9]
    // tx10 is at head, tx9 is downstream
    ms::Delta *tx9_delta = v1_t9->vertex_->delta->next.load()->next.load();
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 3 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 3);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should point to tx9's delta
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should only have tx9's edge
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 1);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 3: tx10 aborting with upstream interleaved deltas from other tx
// Chain: (v)-[tx9 intr]-[tx10]-[tx10]
// After tx10 abort: vertex->delta should still point to tx9's delta, out_edges should only have tx9's edge
TEST(StorageV2Gc, AbortWithUpstreamInterleaved_Case3) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx10 starts first, creates edges first (will be downstream in delta chain)
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx9 starts after, creates edge (will be at head, interleaved above tx10)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // Chain is now: (v1)-[tx9]-[tx10]-[tx10]
    // tx9 is at head, tx10 is downstream
    ms::Delta *tx9_delta = v1_t9->vertex_->delta;
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 3 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 3);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should still point to tx9's delta (tx9 owns head)
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should only have tx9's edge (tx10's edges removed)
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 1);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 4: tx10 aborting with both upstream and downstream interleaved deltas
// Chain: (v)-[tx9 intr]-[tx10 intr]-[tx10 intr]-[tx8]
// After tx10 abort: vertex->delta should still point to tx9, out_edges should have tx9's and tx8's edges
TEST(StorageV2Gc, AbortWithUpstreamAndDownstreamInterleaved_Case4) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx8 starts first, creates edge (will be most downstream)
    auto acc8 = storage->Access();
    auto v1_t8 = acc8->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t8 = acc8->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t8.has_value() && v2_t8.has_value());
    auto edge8 = acc8->CreateEdge(&*v1_t8, &*v2_t8, acc8->NameToEdgeType("Edge8"));
    ASSERT_TRUE(edge8.HasValue());

    // tx10 starts, creates edges (will be in middle)
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx9 starts last, creates edge (will be at head)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // Chain is now: (v1)-[tx9]-[tx10]-[tx10]-[tx8]
    ms::Delta *tx9_delta = v1_t9->vertex_->delta;
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 4 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 4);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should still point to tx9's delta
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should have tx9's and tx8's edges (tx10's removed)
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 2);

    // tx8 and tx9 commit
    ASSERT_FALSE(acc8->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 5: Same as case 4 but tx8 is already committed
// Chain: (v)-[tx9 intr]-[tx10 intr]-[tx10 intr]-[tx8 committed]
// After tx10 abort: out_edges should have tx9's and tx8's edges
TEST(StorageV2Gc, AbortWithUpstreamAndCommittedDownstream_Case5) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // tx8 creates and commits edge first
  {
    auto acc8 = storage->Access();
    auto v1_t8 = acc8->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t8 = acc8->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t8.has_value() && v2_t8.has_value());
    auto edge8 = acc8->CreateEdge(&*v1_t8, &*v2_t8, acc8->NameToEdgeType("Edge8"));
    ASSERT_TRUE(edge8.HasValue());
    ASSERT_FALSE(acc8->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx10 starts, creates edges (will be in middle)
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx9 starts, creates edge (will be at head)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // Chain is now: (v1)-[tx9]-[tx10]-[tx10]-[tx8 committed]
    ms::Delta *tx9_delta = v1_t9->vertex_->delta;
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 4 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 4);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should still point to tx9's delta
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should have tx9's and tx8's edges (tx10's removed)
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 2);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 6: Abort at end of chain
// Chain: (v)-[tx9]-[tx10]-[tx10]
// After tx10 abort: vertex->delta should still point to tx9, out_edges should only have tx9's edge
TEST(StorageV2Gc, AbortAtEndOfChain_Case6) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx10 starts first, creates edges (will be at head initially)
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx9 starts after, creates edge (will prepend, becoming upstream)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // Chain is now: (v1)-[tx9]-[tx10]-[tx10]
    ms::Delta *tx9_delta = v1_t9->vertex_->delta;
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 3 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 3);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should still point to tx9's delta
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should only have tx9's edge (tx10's edges removed)
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 1);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 7: Single delta abort with upstream interleaved
// Chain: (v)-[tx9]-[tx10]
// After tx10 abort: vertex->delta should still point to tx9, out_edges should only have tx9's edge
TEST(StorageV2Gc, AbortSingleDeltaWithUpstream_Case7) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx10 starts first, creates single edge
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10 = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10"));
    ASSERT_TRUE(edge10.HasValue());

    // tx9 starts after, creates edge (will prepend)
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // Chain is now: (v1)-[tx9]-[tx10]
    ms::Delta *tx9_delta = v1_t9->vertex_->delta;
    ASSERT_NE(tx9_delta, nullptr);

    // Before abort: 2 edges
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 2);

    // tx10 aborts
    acc10->Abort();

    // After abort: vertex->delta should still point to tx9's delta
    EXPECT_EQ(v1_t9->vertex_->delta, tx9_delta);
    // out_edges should only have tx9's edge
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 1);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 8: Multiple transactions prepending during abort
// Chain starts: (v)-[tx10]-[tx10]
// tx11 prepends: (v)-[tx11]-[tx10]-[tx10]
// tx12 prepends: (v)-[tx12]-[tx11]-[tx10]-[tx10]
// After tx10 abort: (v)-[tx12]-[tx11]
TEST(StorageV2Gc, AbortWithMultiplePrepends_Case8) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx10 starts first, creates edges
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx11 starts, prepends edge
    auto acc11 = storage->Access();
    auto v1_t11 = acc11->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t11 = acc11->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t11.has_value() && v2_t11.has_value());
    auto edge11 = acc11->CreateEdge(&*v1_t11, &*v2_t11, acc11->NameToEdgeType("Edge11"));
    ASSERT_TRUE(edge11.HasValue());

    // tx12 starts, prepends edge
    auto acc12 = storage->Access();
    auto v1_t12 = acc12->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t12 = acc12->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t12.has_value() && v2_t12.has_value());
    auto edge12 = acc12->CreateEdge(&*v1_t12, &*v2_t12, acc12->NameToEdgeType("Edge12"));
    ASSERT_TRUE(edge12.HasValue());

    // Chain is now: (v1)-[tx12]-[tx11]-[tx10]-[tx10]
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 4);

    // tx10 aborts
    acc10->Abort();

    // After abort: out_edges should have tx12's and tx11's edges only
    EXPECT_EQ(v1_t12->vertex_->out_edges.size(), 2);

    // tx11 and tx12 commit
    ASSERT_FALSE(acc11->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    ASSERT_FALSE(acc12->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 9: Two transactions aborting concurrently from same chain
// Chain: (v)-[tx11]-[tx10]-[tx10]-[tx9]
// tx10 and tx11 both abort
// After both aborts: (v)-[tx9], out_edges should only have tx9's edge
TEST(StorageV2Gc, ConcurrentAborts_Case9) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    // tx9 starts first
    auto acc9 = storage->Access();
    auto v1_t9 = acc9->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t9 = acc9->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t9.has_value() && v2_t9.has_value());
    auto edge9 = acc9->CreateEdge(&*v1_t9, &*v2_t9, acc9->NameToEdgeType("Edge9"));
    ASSERT_TRUE(edge9.HasValue());

    // tx10 starts, creates edges
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // tx11 starts, prepends edge
    auto acc11 = storage->Access();
    auto v1_t11 = acc11->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t11 = acc11->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t11.has_value() && v2_t11.has_value());
    auto edge11 = acc11->CreateEdge(&*v1_t11, &*v2_t11, acc11->NameToEdgeType("Edge11"));
    ASSERT_TRUE(edge11.HasValue());

    // Chain is now: (v1)-[tx11]-[tx10]-[tx10]-[tx9]
    ASSERT_EQ(v1_t11->vertex_->out_edges.size(), 4);

    // Both tx10 and tx11 abort
    acc10->Abort();
    acc11->Abort();

    // After both aborts: out_edges should only have tx9's edge
    EXPECT_EQ(v1_t9->vertex_->out_edges.size(), 1);

    // tx9 commits
    ASSERT_FALSE(acc9->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

// Case 10: Abort all deltas in chain (nothing left)
// Chain: (v)-[tx10]-[tx10]
// After tx10 abort: vertex->delta should be nullptr, out_edges empty
TEST(StorageV2Gc, AbortAllDeltas_Case10) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto acc10 = storage->Access();
    auto v1_t10 = acc10->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t10 = acc10->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t10.has_value() && v2_t10.has_value());
    auto edge10a = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10a"));
    ASSERT_TRUE(edge10a.HasValue());
    auto edge10b = acc10->CreateEdge(&*v1_t10, &*v2_t10, acc10->NameToEdgeType("Edge10b"));
    ASSERT_TRUE(edge10b.HasValue());

    // Before abort: 2 edges, deltas present
    ASSERT_EQ(v1_t10->vertex_->out_edges.size(), 2);
    ASSERT_NE(v1_t10->vertex_->delta, nullptr);

    // tx10 aborts
    acc10->Abort();

    // After abort: no deltas, no edges
    EXPECT_EQ(v1_t10->vertex_->delta, nullptr);
    EXPECT_EQ(v1_t10->vertex_->out_edges.size(), 0);
  }
}
