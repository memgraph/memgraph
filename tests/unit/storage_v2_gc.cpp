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

TEST(StorageV2Gc, NonSequentialDeltasWithCommittedContributorsAreGarbagedCollected) {
  // Need a periodic garbage collector so that certain fast path optimisations
  // aren't taken when cleaning deltas, but with an inter-collection pause large
  // enough that we can step through when debugging, etc, without anything being
  // unexpectedly reclaimed from underneath us.
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    // The 6 unreleased deltas are:
    // - 2 x CREATE_OBJECT, to create the edges
    // - 2 x ADD_IN_EDGE
    // - 2 x ADD_OUT_EDGE
    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // Commit in order `acc1` and `acc2`. This means that even though `acc2` does
    // have non-sequential deltas, everything downstream from them is committed
    // and so garbage collection can skip the waiting list.
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, NonSequentialDeltasWithAbortedContributorsAreGarbagedCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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

TEST(StorageV2Gc, NonSequentialDeltasWithMultipleAbortsAreGarbageCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

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
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    // Create three edges to form downstream delta chain: TX1 -> TX2 -> TX3
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).has_value());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // Commit TX1, abort TX2 and TX3
    // TX3's deltas are downstream from TX2, which are downstream from TX1
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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

TEST(StorageV2Gc, MixedCommitAbortCommitNonSequentialDeltasAreGarbageCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    // Create non-sequential deltas with mixed commit/abort pattern
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).has_value());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // TX1 commits, TX2 aborts, TX3 commits
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    acc2->Abort();
    acc2.reset();
    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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

TEST(StorageV2Gc, NonSequentialDeltasWithTwoContributorsAreGarbagedCollected) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    auto edge3_result = acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3"));
    ASSERT_TRUE(edge3_result.has_value());

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc3.reset();
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    ASSERT_EQ(9, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, NonSequentialDeltasWithUncommittedContributorsAreGarbagedCollected) {
  // Use periodic GC with short interval to automatically test intermediate states
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).has_value());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // When acc2 commits, its transaction has non-sequential deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc2 committed but acc1 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc1
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}

TEST(StorageV2Gc, NonSequentialDeltasWithUncommittedContributorsAreGarbagedCollected_SwapCommitOrder) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).has_value());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    // When acc1 commits first, its transaction has non-sequential deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc1 committed but acc2 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc2
    ASSERT_EQ(12, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));

    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
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
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto tx2 = storage->Access(memgraph::storage::WRITE);

  {
    auto v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx1->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx1->CreateEdge(&v1, &v2, tx1->NameToEdgeType("Edge1")).has_value());
  }

  {
    auto v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx2->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx2->CreateEdge(&v1, &v2, tx2->NameToEdgeType("Edge2")).has_value());
  }

  tx1->Abort();
  tx2->Abort();

  // Wait for GC to clean up
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  {
    auto reader = storage->Access(memgraph::storage::WRITE);
    auto v1 = reader->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2 = reader->FindVertex(v2_gid, memgraph::storage::View::OLD);

    if (v1.has_value()) {
      auto edges = v1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.has_value());
    }
    if (v2.has_value()) {
      auto edges = v2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.has_value());
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
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // blocker transaction prevents rapid GC
  auto blocker = storage->Access(memgraph::storage::WRITE);

  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_a;
  {
    accessor_a = storage->Access(memgraph::storage::WRITE);
    auto v = accessor_a->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(accessor_a->NameToLabel("LabelA")).has_value());
    ASSERT_TRUE(accessor_a->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto accessor_b = storage->Access(memgraph::storage::WRITE);
    auto v = accessor_b->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(accessor_b->NameToLabel("LabelB")).has_value());

    ASSERT_TRUE(accessor_b->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  accessor_a.reset();

  // Destroy blocker to allow rapid GC
  blocker.reset();

  {
    auto gc_trigger = storage->Access(memgraph::storage::WRITE);
    gc_trigger->CreateVertex();
    ASSERT_TRUE(gc_trigger->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto reader = storage->Access(memgraph::storage::WRITE);
    auto v = reader->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());

    auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.has_value());
    EXPECT_EQ(labels->size(), 2);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v1)-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithWithNoNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    ASSERT_TRUE(tx1->CreateEdge(&*v1, &*v2, tx1->NameToEdgeType("EDGE1")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1, &*v2, tx1->NameToEdgeType("EDGE2")).has_value());

    ASSERT_NE(v1->vertex_->delta(), nullptr);
    ASSERT_EQ(v1->vertex_->out_edges.size(), 2);

    tx1->Abort();
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 0);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 0);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

//==============================================================================
// Following tests all peek at internal state of vertices before, during, or
// after Abort(). As such, they "bypass" isolation and visibility to directly
// assert what we expect in terms of deltas and vertex state.

// Tests state after aborting tx2 when we have the following chain:
// (v1)-[tx2 intr]-[tx2 intr]-[tx1]
TEST(StorageV2Gc, AbortsWhenDeltasAreNonSequential) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto const *delta_v1_t1_head = v1_t1->vertex_->delta();

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE3")).has_value());

    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 3);

    tx2->Abort();

    ASSERT_EQ(v1_t1->vertex_->delta(), delta_v1_t1_head);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithUpstreamNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto *delta_v1_t2 = v1_t2->vertex_->delta();
    ASSERT_NE(delta_v1_t2, nullptr);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 3);

    tx1->Abort();

    EXPECT_EQ(v1_t2->vertex_->delta(), delta_v1_t2);
    EXPECT_EQ(v1_t2->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx2 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx2 intr]-[tx1]
TEST(StorageV2Gc, AbortsWithUpstreamAndDownstreamNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2A")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2B")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    auto *delta_v1_t3 = v1_t3->vertex_->delta();
    ASSERT_NE(delta_v1_t3, nullptr);
    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx2->Abort();

    EXPECT_EQ(v1_t3->vertex_->delta(), delta_v1_t3);
    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx2 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx2 intr]-[tx1 committed]
TEST(StorageV2Gc, AbortsWithCommittedDownstreamDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());
    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2A")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2B")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    auto *delta_v1_t3 = v1_t3->vertex_->delta();
    ASSERT_NE(delta_v1_t3, nullptr);
    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx2->Abort();

    EXPECT_EQ(v1_t3->vertex_->delta(), delta_v1_t3);
    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsAtEndOfNonSequentialChain) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto *delta_v1_t2 = v1_t2->vertex_->delta();
    ASSERT_NE(delta_v1_t2, nullptr);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 3);

    tx1->Abort();

    EXPECT_EQ(v1_t2->vertex_->delta(), delta_v1_t2);
    EXPECT_EQ(v1_t2->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithMultipleTransactionsPrepending) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 4);

    tx1->Abort();

    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 and tx2 when we have the following chain:
// (v)-[tx2 intr]-[tx1 intr]-[tx1 intr]-[tx0]
TEST(StorageV2Gc, AbortsTwoTransactions) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1_t0 = tx0->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t0 = tx0->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t0.has_value() && v2_t0.has_value());
    ASSERT_TRUE(tx0->CreateEdge(&*v1_t0, &*v2_t0, tx0->NameToEdgeType("EDGE0")).has_value());

    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx1->Abort();
    tx2->Abort();

    EXPECT_EQ(v1_t0->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagRemainsAfterAbort) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagRemainsAfterPartialAbort) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    {
      auto const guard = std::shared_lock{v1_t3->vertex_->lock};
      ASSERT_TRUE(v1_t3->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();

    {
      auto const guard = std::shared_lock{v1_t3->vertex_->lock};
      ASSERT_TRUE(v1_t3->vertex_->has_uncommitted_non_sequential_deltas());
    }

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagClearedWhenAllDeltasRemoved) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();
    tx1->Abort();
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}
