// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <latch>

#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

// @TODO tests for non commutative ops, such as SetProp

TEST(StorageV2Mvcc, CanUpdateAnObjectWithNoDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage->Access();
  auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
  auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1.has_value() && v2.has_value());

  auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("TestEdge"));
  EXPECT_TRUE(edge_result.HasValue());
}

TEST(StorageV2Mvcc, CanUpdateAnObjectWithCommittedDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();

    auto edge_result = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge_result.HasValue());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto acc = storage->Access();
  auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
  auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
  ASSERT_TRUE(v1.has_value() && v2.has_value());

  auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("Edge2"));
  EXPECT_TRUE(edge_result.HasValue());
}

TEST(StorageV2Mvcc, CanWaitForUncommittedCommutativeTransaction) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch t2_started{2};
  std::jthread t2([&]() {
    auto acc2 = storage->Access();
    auto v1_acc2 = acc2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_acc2 = acc2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_acc2.has_value() && v2_acc2.has_value());

    t2_started.arrive_and_wait();

    auto edge2_result = acc2->CreateEdge(&*v1_acc2, &*v2_acc2, acc2->NameToEdgeType("Edge2"));
    EXPECT_TRUE(edge2_result.HasValue());
  });

  {
    auto acc1 = storage->Access();
    auto v1_acc1 = acc1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_acc1 = acc1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_acc1.has_value() && v2_acc1.has_value());

    auto edge1_result = acc1->CreateEdge(&*v1_acc1, &*v2_acc1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    t2_started.arrive_and_wait();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  t2.join();
}

TEST(StorageV2Mvcc, TwoWayCircularDependencyDetected) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid a_gid, b_gid, c_gid, d_gid;
  {
    auto acc = storage->Access();
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    auto c = acc->CreateVertex();
    auto d = acc->CreateVertex();
    a_gid = a.Gid();
    b_gid = b.Gid();
    c_gid = c.Gid();
    d_gid = d.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch all_initial_edges_created{2};
  std::latch all_final_edges_created{2};  // @TODO rename
  std::atomic<int> conflicts_detected{0};

  {
    std::jthread t1([&]() {
      auto acc1 = storage->Access();
      auto a = acc1->FindVertex(a_gid, ms::View::OLD);
      auto b = acc1->FindVertex(b_gid, ms::View::OLD);
      auto c = acc1->FindVertex(c_gid, ms::View::OLD);
      auto d = acc1->FindVertex(d_gid, ms::View::OLD);
      ASSERT_TRUE(a.has_value() && b.has_value() && c.has_value() && d.has_value());

      auto edge1_result = acc1->CreateEdge(&*a, &*b, acc1->NameToEdgeType("AB"));
      ASSERT_TRUE(edge1_result.HasValue());

      all_initial_edges_created.arrive_and_wait();

      auto edge2_result = acc1->CreateEdge(&*c, &*d, acc1->NameToEdgeType("CD_from_T1"));
      if (edge2_result.HasError() && edge2_result.GetError() == ms::Error::SERIALIZATION_ERROR) {
        conflicts_detected++;
      } else {
        ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      acc1.reset();
      all_final_edges_created.arrive_and_wait();
    });

    std::jthread t2([&]() {
      auto acc2 = storage->Access();
      auto a = acc2->FindVertex(a_gid, ms::View::OLD);
      auto b = acc2->FindVertex(b_gid, ms::View::OLD);
      auto c = acc2->FindVertex(c_gid, ms::View::OLD);
      auto d = acc2->FindVertex(d_gid, ms::View::OLD);
      ASSERT_TRUE(a.has_value() && b.has_value() && c.has_value() && d.has_value());

      auto edge1_result = acc2->CreateEdge(&*c, &*d, acc2->NameToEdgeType("CD"));
      ASSERT_TRUE(edge1_result.HasValue());

      all_initial_edges_created.arrive_and_wait();

      auto edge2_result = acc2->CreateEdge(&*a, &*b, acc2->NameToEdgeType("AB_from_T2"));
      if (edge2_result.HasError() && edge2_result.GetError() == ms::Error::SERIALIZATION_ERROR) {
        conflicts_detected++;
      } else {
        ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      acc2.reset();
      all_final_edges_created.arrive_and_wait();
    });
  }

  EXPECT_GE(conflicts_detected.load(), 1);
}

TEST(StorageV2Mvcc, NonCommutativeOperationOnInterleavedDeltaFails) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch t2_started{2};
  std::jthread t2([&]() {
    auto acc2 = storage->Access();
    auto v1_acc2 = acc2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_acc2 = acc2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_acc2.has_value() && v2_acc2.has_value());

    t2_started.arrive_and_wait();

    auto edge2_result = acc2->CreateEdge(&*v1_acc2, &*v2_acc2, acc2->NameToEdgeType("Edge2"));
    EXPECT_TRUE(edge2_result.HasValue());

    auto prop_result = v1_acc2->SetProperty(acc2->NameToProperty("prop"), ms::PropertyValue(42));
    ASSERT_TRUE(prop_result.HasError() && prop_result.GetError() == ms::Error::SERIALIZATION_ERROR);
  });

  {
    auto acc1 = storage->Access();
    auto v1_acc1 = acc1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_acc1 = acc1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_acc1.has_value() && v2_acc1.has_value());

    auto edge1_result = acc1->CreateEdge(&*v1_acc1, &*v2_acc1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.HasValue());

    t2_started.arrive_and_wait();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TEST(StorageV2Mvcc, SnapshotIsolationHasCorrectInterleavedDeltaVisibility) {
  auto config = ms::Config{};
  config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED;
  config.salient.items.properties_on_edges = false;
  auto storage = std::make_unique<ms::InMemoryStorage>(config);

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(ms::IsolationLevel::SNAPSHOT_ISOLATION);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch sync_point{2};
  std::jthread t1([&]() {
    auto acc = storage->Access(ms::IsolationLevel::SNAPSHOT_ISOLATION);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("T1_Edge"));
    ASSERT_TRUE(edge_result.HasValue());

    sync_point.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto degree_old_result = v1->OutDegree(ms::View::OLD);
    ASSERT_TRUE(degree_old_result.HasValue());
    EXPECT_EQ(degree_old_result.GetValue(), 0);

    auto degree_new_result = v1->OutDegree(ms::View::NEW);
    ASSERT_TRUE(degree_new_result.HasValue());
    EXPECT_EQ(degree_new_result.GetValue(), 1);

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  });

  std::jthread t2([&]() {
    auto acc = storage->Access(ms::IsolationLevel::SNAPSHOT_ISOLATION);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    sync_point.arrive_and_wait();

    auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("T2_Interleaved"));
    EXPECT_TRUE(edge_result.HasValue());

    auto degree_old_result = v1->OutDegree(ms::View::OLD);
    ASSERT_TRUE(degree_old_result.HasValue());
    EXPECT_EQ(degree_old_result.GetValue(), 0);

    auto degree_new_result = v1->OutDegree(ms::View::NEW);
    ASSERT_TRUE(degree_new_result.HasValue());
    EXPECT_EQ(degree_new_result.GetValue(), 2);

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  });
}

TEST(StorageV2Mvcc, ReadCommitedHasCorrectInterleavedDeltaVisibility) {
  auto config = ms::Config{};
  config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED;
  config.salient.items.properties_on_edges = false;
  auto storage = std::make_unique<ms::InMemoryStorage>(config);

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(ms::IsolationLevel::READ_COMMITTED);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch sync_point{2};

  std::jthread writer([&]() {
    auto acc1 = storage->Access(ms::IsolationLevel::READ_COMMITTED);
    auto v1 = acc1->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    auto edge1_result = acc1->CreateEdge(&*v1, &*v2, acc1->NameToEdgeType("T1_Edge"));
    ASSERT_TRUE(edge1_result.HasValue());

    sync_point.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  });

  {
    auto acc2 = storage->Access(ms::IsolationLevel::READ_COMMITTED);
    auto v1 = acc2->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    sync_point.arrive_and_wait();

    auto edge2_result = acc2->CreateEdge(&*v1, &*v2, acc2->NameToEdgeType("T2_Interleaved"));
    EXPECT_TRUE(edge2_result.HasValue());

    auto degree_result = v1->OutDegree(ms::View::OLD);
    ASSERT_TRUE(degree_result.HasValue());
    EXPECT_EQ(degree_result.GetValue(), 2);

    ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TEST(StorageV2Mvcc, ReadUncommitedHasCorrectInterleavedDeltaVisibility) {
  auto config = ms::Config{};
  config.durability.snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::DISABLED;
  config.salient.items.properties_on_edges = false;
  auto storage = std::make_unique<ms::InMemoryStorage>(config);

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(ms::IsolationLevel::READ_UNCOMMITTED);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch sync_point{2};

  std::jthread t1([&]() {
    auto acc = storage->Access(ms::IsolationLevel::READ_UNCOMMITTED);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("T1_Edge"));
    ASSERT_TRUE(edge_result.HasValue());

    sync_point.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  });

  {
    auto acc = storage->Access(ms::IsolationLevel::READ_UNCOMMITTED);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    sync_point.arrive_and_wait();

    auto edge_result = acc->CreateEdge(&*v1, &*v2, acc->NameToEdgeType("T2_Interleaved"));
    EXPECT_TRUE(edge_result.HasValue());

    auto degree_result = v1->OutDegree(ms::View::OLD);
    ASSERT_TRUE(degree_result.HasValue());
    EXPECT_EQ(degree_result.GetValue(), 2);

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TEST(StorageV2Mvcc, ThreeWayCircularDependencyDetected) {
  auto storage = std::make_unique<ms::InMemoryStorage>(ms::Config{});

  ms::Gid a_gid, b_gid, c_gid, d_gid, e_gid, f_gid;
  {
    auto acc = storage->Access();
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    auto c = acc->CreateVertex();
    auto d = acc->CreateVertex();
    auto e = acc->CreateVertex();
    auto f = acc->CreateVertex();
    a_gid = a.Gid();
    b_gid = b.Gid();
    c_gid = c.Gid();
    d_gid = d.Gid();
    e_gid = e.Gid();
    f_gid = f.Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::latch all_initial_edges_created{3};
  std::latch all_final_edges_created{3};
  std::atomic<int> conflicts_detected{0};

  {
    std::jthread t1([&]() {
      auto acc1 = storage->Access();
      auto a = acc1->FindVertex(a_gid, ms::View::OLD);
      auto b = acc1->FindVertex(b_gid, ms::View::OLD);
      auto c = acc1->FindVertex(c_gid, ms::View::OLD);
      auto d = acc1->FindVertex(d_gid, ms::View::OLD);
      ASSERT_TRUE(a.has_value() && b.has_value() && c.has_value() && d.has_value());

      auto edge1_result = acc1->CreateEdge(&*a, &*b, acc1->NameToEdgeType("AB"));
      ASSERT_TRUE(edge1_result.HasValue());

      all_initial_edges_created.arrive_and_wait();

      auto edge2_result = acc1->CreateEdge(&*c, &*d, acc1->NameToEdgeType("CD_from_T1"));
      if (edge2_result.HasError() && edge2_result.GetError() == ms::Error::SERIALIZATION_ERROR) {
        conflicts_detected++;
      } else {
        ASSERT_FALSE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      acc1.reset();
      all_final_edges_created.arrive_and_wait();
    });

    std::jthread t2([&]() {
      auto acc2 = storage->Access();
      auto c = acc2->FindVertex(c_gid, ms::View::OLD);
      auto d = acc2->FindVertex(d_gid, ms::View::OLD);
      auto e = acc2->FindVertex(e_gid, ms::View::OLD);
      auto f = acc2->FindVertex(f_gid, ms::View::OLD);
      ASSERT_TRUE(c.has_value() && d.has_value() && e.has_value() && f.has_value());

      auto edge1_result = acc2->CreateEdge(&*c, &*d, acc2->NameToEdgeType("CD"));
      ASSERT_TRUE(edge1_result.HasValue());

      all_initial_edges_created.arrive_and_wait();

      auto edge2_result = acc2->CreateEdge(&*e, &*f, acc2->NameToEdgeType("EF_from_T2"));
      if (edge2_result.HasError() && edge2_result.GetError() == ms::Error::SERIALIZATION_ERROR) {
        conflicts_detected++;
      } else {
        ASSERT_FALSE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      acc2.reset();
      all_final_edges_created.arrive_and_wait();
    });

    std::jthread t3([&]() {
      auto acc3 = storage->Access();
      auto e = acc3->FindVertex(e_gid, ms::View::OLD);
      auto f = acc3->FindVertex(f_gid, ms::View::OLD);
      auto a = acc3->FindVertex(a_gid, ms::View::OLD);
      auto b = acc3->FindVertex(b_gid, ms::View::OLD);
      ASSERT_TRUE(e.has_value() && f.has_value() && a.has_value() && b.has_value());

      auto edge1_result = acc3->CreateEdge(&*e, &*f, acc3->NameToEdgeType("EF"));
      ASSERT_TRUE(edge1_result.HasValue());

      all_initial_edges_created.arrive_and_wait();

      auto edge2_result = acc3->CreateEdge(&*a, &*b, acc3->NameToEdgeType("AB_from_T3"));
      if (edge2_result.HasError() && edge2_result.GetError() == ms::Error::SERIALIZATION_ERROR) {
        conflicts_detected++;
      } else {
        ASSERT_FALSE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      acc3.reset();
      all_final_edges_created.arrive_and_wait();
    });
  }

  EXPECT_GE(conflicts_detected.load(), 1);
}
