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

using memgraph::replication_coordination_glue::ReplicationRole;
using testing::UnorderedElementsAre;

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

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
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

    // When acc2 commits, its transaction has interleaved deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc2.reset();
    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc1.reset();

    ASSERT_EQ(6, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
  }

  {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }

  ASSERT_EQ(0, memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects));
}
