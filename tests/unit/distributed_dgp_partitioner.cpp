#include "distributed_common.hpp"

#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "distributed/dgp/partitioner.hpp"
#include "distributed/updates_rpc_clients.hpp"

using namespace distributed;
using namespace database;

DECLARE_int32(dgp_max_batch_size);

class DistributedDynamicGraphPartitionerTest : public DistributedGraphDbTest {
 public:
  DistributedDynamicGraphPartitionerTest()
      : DistributedGraphDbTest("dynamic_graph_partitioner") {}

  void LogClusterState() {
    LOG(INFO) << "master_v: " << VertexCount(master())
              << " master_e: " << EdgeCount(master());
    LOG(INFO) << "worker1_v: " << VertexCount(worker(1))
              << " worker1_e: " << EdgeCount(worker(1));
    LOG(INFO) << "worker2_v: " << VertexCount(worker(2))
              << " worker2_e: " << EdgeCount(worker(2));
  }
};

TEST_F(DistributedDynamicGraphPartitionerTest, CountLabels) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(worker(1));
  auto vc = InsertVertex(worker(2));
  for (int i = 0; i < 2; ++i) InsertEdge(va, va, "edge");
  for (int i = 0; i < 3; ++i) InsertEdge(va, vb, "edge");
  for (int i = 0; i < 4; ++i) InsertEdge(va, vc, "edge");
  for (int i = 0; i < 5; ++i) InsertEdge(vb, va, "edge");
  for (int i = 0; i < 6; ++i) InsertEdge(vc, va, "edge");

  distributed::dgp::Partitioner dgp(&master());
  auto dba = master().Access();
  VertexAccessor v(va, *dba);
  auto count_labels = dgp.CountLabels(v);

  // Self loops counted twice
  EXPECT_EQ(count_labels[master().WorkerId()], 2 * 2);

  EXPECT_EQ(count_labels[worker(1).WorkerId()], 3 + 5);
  EXPECT_EQ(count_labels[worker(2).WorkerId()], 4 + 6);
}

TEST_F(DistributedDynamicGraphPartitionerTest, FindMigrationsMoveVertex) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(worker(1));

  // Balance the number of nodes on workers a bit
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  for (int i = 0; i < 100; ++i) InsertEdge(va, vb, "edge");
  distributed::dgp::Partitioner dgp(&master());
  auto dba = master().Access();
  auto data = dgp.FindMigrations(*dba);
  // Expect `va` to try to move to another worker, the one connected to it
  ASSERT_EQ(data.migrations.size(), 1);
  EXPECT_EQ(data.migrations[0].second, worker(1).WorkerId());
}

TEST_F(DistributedDynamicGraphPartitionerTest, FindMigrationsNoChange) {
  InsertVertex(master());
  InsertVertex(worker(1));
  InsertVertex(worker(2));

  // Everything is balanced, there should be no movement

  distributed::dgp::Partitioner dgp(&master());
  auto dba = master().Access();
  auto data = dgp.FindMigrations(*dba);
  EXPECT_EQ(data.migrations.size(), 0);
}

TEST_F(DistributedDynamicGraphPartitionerTest, FindMigrationsMultipleAndLimit) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(master());
  auto vc = InsertVertex(worker(1));

  // Balance the number of nodes on workers a bit
  InsertVertex(worker(1));
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  for (int i = 0; i < 100; ++i) InsertEdge(va, vc, "edge");
  for (int i = 0; i < 100; ++i) InsertEdge(vb, vc, "edge");
  distributed::dgp::Partitioner dgp(&master());
  auto dba = master().Access();
  {
    auto data = dgp.FindMigrations(*dba);
    // Expect vertices to try to move to another worker
    ASSERT_EQ(data.migrations.size(), 2);
  }

  // See if flag affects number of returned results
  {
    FLAGS_dgp_max_batch_size = 1;
    auto data = dgp.FindMigrations(*dba);
    // Expect vertices to try to move to another worker
    ASSERT_EQ(data.migrations.size(), 1);
  }
}

TEST_F(DistributedDynamicGraphPartitionerTest, Run) {
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

  distributed::dgp::Partitioner dgp(&master());
  // Transfer one by one to actually converge
  FLAGS_dgp_max_batch_size = 1;
  // Try a bit more transfers to see if we reached a steady state
  for (int i = 0; i < 15; ++i) {
    dgp.Partition();
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

  auto dba_m = master().Access();
  auto dba_w1 = worker(1).Access();
  EXPECT_EQ(CountRemotes(*dba_m), 50);
  EXPECT_EQ(CountRemotes(*dba_w1), 50);
}

TEST_F(DistributedDynamicGraphPartitionerTest, Convergence) {
  auto seed = std::time(nullptr);
  LOG(INFO) << "Seed: " << seed;
  std::srand(seed);

  // Generate random graph across cluster.
  std::vector<storage::VertexAddress> master_vertices;
  for (int i = 0; i < 1000; ++i) {
    master_vertices.push_back(InsertVertex(master()));
  }
  std::vector<storage::VertexAddress> worker1_vertices;
  for (int i = 0; i < 1000; ++i) {
    worker1_vertices.push_back(InsertVertex(worker(1)));
  }
  std::vector<storage::VertexAddress> worker2_vertices;
  for (int i = 0; i < 1000; ++i) {
    worker2_vertices.push_back(InsertVertex(worker(2)));
  }

  // Generate random edges between machines.
  for (int i = 0; i < 1000; ++i) {
    InsertEdge(master_vertices[rand() % 1000], worker1_vertices[rand() % 1000],
               "edge");
    InsertEdge(master_vertices[rand() % 1000], worker2_vertices[rand() % 1000],
               "edge");
    InsertEdge(worker1_vertices[rand() % 1000], master_vertices[rand() % 1000],
               "edge");
    InsertEdge(worker1_vertices[rand() % 1000], worker2_vertices[rand() % 1000],
               "edge");
    InsertEdge(worker2_vertices[rand() % 1000], master_vertices[rand() % 1000],
               "edge");
    InsertEdge(worker2_vertices[rand() % 1000], worker1_vertices[rand() % 1000],
               "edge");
  }

  // Run the partitioning algorithm, after some time it should stop doing
  // migrations.
  distributed::dgp::Partitioner dgp_master(&master());
  std::vector<double> master_scores;
  distributed::dgp::Partitioner dgp_worker1(&worker(1));
  std::vector<double> worker1_scores;
  distributed::dgp::Partitioner dgp_worker2(&worker(2));
  std::vector<double> worker2_scores;
  FLAGS_dgp_max_batch_size = 10;
  for (int i = 0; i < 100; ++i) {
    LOG(INFO) << "Iteration: " << i;

    auto data_master = dgp_master.Partition();
    LOG(INFO) << "Master score: " << data_master.first;
    master_scores.push_back(data_master.first);
    LogClusterState();

    auto data_worker1 = dgp_worker1.Partition();
    LOG(INFO) << "Worker1 score: " << data_worker1.first;
    worker1_scores.push_back(data_worker1.first);
    LogClusterState();

    auto data_worker2 = dgp_worker2.Partition();
    LOG(INFO) << "Worker2 score: " << data_worker2.first;
    worker2_scores.push_back(data_worker2.first);
    LogClusterState();
  }

  // Check that the last N scores from each instance are the same.
  int scores_to_validate = 10;
  auto score_equality = [](double x, double y) {
    return std::abs(x - y) < 10e-1;
  };
  ASSERT_TRUE(std::all_of(master_scores.end() - scores_to_validate,
                          master_scores.end(),
                          [&score_equality, &master_scores](double elem) {
                            return score_equality(elem, master_scores.back());
                          }));
  ASSERT_TRUE(std::all_of(worker1_scores.end() - scores_to_validate,
                          worker1_scores.end(),
                          [&score_equality, &worker1_scores](double elem) {
                            return score_equality(elem, worker1_scores.back());
                          }));
  ASSERT_TRUE(std::all_of(worker2_scores.end() - scores_to_validate,
                          worker2_scores.end(),
                          [&score_equality, &worker2_scores](double elem) {
                            return score_equality(elem, worker2_scores.back());
                          }));
}
