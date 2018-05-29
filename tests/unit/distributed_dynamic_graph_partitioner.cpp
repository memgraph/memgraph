#include "distributed_common.hpp"

#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "distributed/updates_rpc_clients.hpp"
#include "storage/dynamic_graph_partitioner/dgp.hpp"

using namespace distributed;
using namespace database;

DECLARE_int32(dgp_max_batch_size);

TEST_F(DistributedGraphDbTest, CountLabels) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(worker(1));
  auto vc = InsertVertex(worker(2));
  for (int i = 0; i < 2; ++i) InsertEdge(va, va, "edge");
  for (int i = 0; i < 3; ++i) InsertEdge(va, vb, "edge");
  for (int i = 0; i < 4; ++i) InsertEdge(va, vc, "edge");
  for (int i = 0; i < 5; ++i) InsertEdge(vb, va, "edge");
  for (int i = 0; i < 6; ++i) InsertEdge(vc, va, "edge");

  DynamicGraphPartitioner dgp(&master());
  GraphDbAccessor dba(master());
  VertexAccessor v(va, dba);
  auto count_labels = dgp.CountLabels(v);

  // Self loops counted twice
  EXPECT_EQ(count_labels[master().WorkerId()], 2 * 2);

  EXPECT_EQ(count_labels[worker(1).WorkerId()], 3 + 5);
  EXPECT_EQ(count_labels[worker(2).WorkerId()], 4 + 6);
}

TEST_F(DistributedGraphDbTest, FindMigrationsMoveVertex) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(worker(1));

  // Balance the number of nodes on workers a bit
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  for (int i = 0; i < 100; ++i) InsertEdge(va, vb, "edge");
  DynamicGraphPartitioner dgp(&master());
  GraphDbAccessor dba(master());
  auto migrations = dgp.FindMigrations(dba);
  // Expect `va` to try to move to another worker, the one connected to it
  ASSERT_EQ(migrations.size(), 1);
  EXPECT_EQ(migrations[0].second, worker(1).WorkerId());
}

TEST_F(DistributedGraphDbTest, FindMigrationsNoChange) {
  InsertVertex(master());
  InsertVertex(worker(1));
  InsertVertex(worker(2));

  // Everything is balanced, there should be no movement

  DynamicGraphPartitioner dgp(&master());
  GraphDbAccessor dba(master());
  auto migrations = dgp.FindMigrations(dba);
  EXPECT_EQ(migrations.size(), 0);
}

TEST_F(DistributedGraphDbTest, FindMigrationsMultipleAndLimit) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(master());
  auto vc = InsertVertex(worker(1));

  // Balance the number of nodes on workers a bit
  InsertVertex(worker(1));
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  for (int i = 0; i < 100; ++i) InsertEdge(va, vc, "edge");
  for (int i = 0; i < 100; ++i) InsertEdge(vb, vc, "edge");
  DynamicGraphPartitioner dgp(&master());
  GraphDbAccessor dba(master());
  {
    auto migrations = dgp.FindMigrations(dba);
    // Expect vertices to try to move to another worker
    ASSERT_EQ(migrations.size(), 2);
  }

  // See if flag affects number of returned results
  {
    FLAGS_dgp_max_batch_size = 1;
    auto migrations = dgp.FindMigrations(dba);
    // Expect vertices to try to move to another worker
    ASSERT_EQ(migrations.size(), 1);
  }
}

TEST_F(DistributedGraphDbTest, Run) {
  // Emulate a bipartite graph with lots of connections on the left, and right
  // side, and some connections between the halfs
  std::vector<storage::VertexAddress> left;
  for (int i = 0; i < 10; ++i) {
    left.push_back(InsertVertex(master()));
  }
  std::vector<storage::VertexAddress> right;
  for (int i = 0; i < 10; ++i) {
    right.push_back(InsertVertex(master()));
  }

  // Force the nodes of both sides to stay on one worker by inserting a lot of
  // edges in between them
  for (int i = 0; i < 1000; ++i) {
    InsertEdge(left[rand() % 10], left[rand() % 10], "edge");
    InsertEdge(right[rand() % 10], right[rand() % 10], "edge");
  }

  // Insert edges between left and right side
  for (int i = 0; i < 50; ++i)
    InsertEdge(left[rand() % 10], right[rand() % 10], "edge");

  // Balance it out so that the vertices count on workers don't influence the
  // partitioning too much
  for (int i = 0; i < 10; ++i) InsertVertex(worker(2));

  DynamicGraphPartitioner dgp(&master());
  // Transfer one by one to actually converge
  FLAGS_dgp_max_batch_size = 1;
  // Try a bit more transfers to see if we reached a steady state
  for (int i = 0; i < 15; ++i) {
    dgp.Run();
  }

  EXPECT_EQ(VertexCount(master()), 10);
  EXPECT_EQ(VertexCount(worker(1)), 10);

  auto CountRemotes = [](GraphDbAccessor &dba) {
    int64_t cnt = 0;
    for (auto vertex : dba.Vertices(false)) {
      for (auto edge : vertex.in())
        if (edge.from_addr().is_remote()) ++cnt;
      for (auto edge : vertex.out())
        if (edge.to_addr().is_remote()) ++cnt;
    }
    return cnt;
  };

  GraphDbAccessor dba_m(master());
  GraphDbAccessor dba_w1(worker(1));
  EXPECT_EQ(CountRemotes(dba_m), 50);
  EXPECT_EQ(CountRemotes(dba_w1), 50);
}
