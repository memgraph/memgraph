#include "gtest/gtest.h"

#include "database/graph_db_accessor.hpp"
#include "distributed/bfs_rpc_clients.hpp"

#include "distributed_common.hpp"

using namespace database;

std::vector<int> V = {0, 1, 1, 0, 1, 2};
std::vector<std::pair<int, int>> E = {{0, 1}, {1, 2}, {1, 5},
                                      {2, 4}, {2, 5}, {3, 4}};

class BfsTest : public DistributedGraphDbTest {
 protected:
  void SetUp() override {
    DistributedGraphDbTest::SetUp();

    for (int v : V) {
      auto vertex = v == 0 ? InsertVertex(master()) : InsertVertex(worker(v));
      vertices.emplace_back(vertex);
    }

    for (auto e : E) {
      edges[e] = InsertEdge(vertices[e.first], vertices[e.second], "Edge");
    }
  }

 public:
  BfsTest() : DistributedGraphDbTest("bfs") {}
  std::vector<storage::VertexAddress> vertices;
  std::map<std::pair<int, int>, storage::EdgeAddress> edges;
};

TEST_F(BfsTest, Expansion) {
  GraphDbAccessor dba{master()};

  auto &clients = master().bfs_subcursor_clients();
  auto subcursor_ids = clients.CreateBfsSubcursors(
      dba.transaction_id(), query::EdgeAtom::Direction::BOTH,
      {dba.EdgeType("Edge")}, query::GraphView::OLD);
  clients.RegisterSubcursors(subcursor_ids);

  clients.SetSource(subcursor_ids, vertices[0]);

  auto pull = [&clients, &subcursor_ids, &dba](int worker_id) {
    return clients.Pull(worker_id, subcursor_ids[worker_id], &dba);
  };

  EXPECT_EQ(pull(0), std::experimental::nullopt);
  EXPECT_EQ(pull(1)->GlobalAddress(), vertices[1]);
  EXPECT_EQ(pull(2), std::experimental::nullopt);

  clients.PrepareForExpand(subcursor_ids, false);
  clients.ExpandLevel(subcursor_ids);

  EXPECT_EQ(pull(0), std::experimental::nullopt);
  EXPECT_EQ(pull(1)->GlobalAddress(), vertices[2]);
  EXPECT_EQ(pull(1), std::experimental::nullopt);
  EXPECT_EQ(pull(2)->GlobalAddress(), vertices[5]);
  EXPECT_EQ(pull(2), std::experimental::nullopt);

  clients.PrepareForExpand(subcursor_ids, false);
  clients.ExpandLevel(subcursor_ids);

  EXPECT_EQ(pull(0), std::experimental::nullopt);
  EXPECT_EQ(pull(1)->GlobalAddress(), vertices[4]);
  EXPECT_EQ(pull(1), std::experimental::nullopt);
  EXPECT_EQ(pull(2), std::experimental::nullopt);

  clients.PrepareForExpand(subcursor_ids, false);
  clients.ExpandLevel(subcursor_ids);

  EXPECT_EQ(pull(0)->GlobalAddress(), vertices[3]);
  EXPECT_EQ(pull(0), std::experimental::nullopt);
  EXPECT_EQ(pull(1), std::experimental::nullopt);
  EXPECT_EQ(pull(2), std::experimental::nullopt);

  auto compare = [this](const std::vector<EdgeAccessor> &lhs,
                        const std::vector<std::pair<int, int>> &rhs) {
    EXPECT_EQ(lhs.size(), rhs.size());
    if (lhs.size() != rhs.size()) return;
    for (auto idx = 0u; idx < lhs.size(); ++idx) {
      EXPECT_EQ(lhs[idx].GlobalAddress(), edges[rhs[idx]]);
    }
  };

  distributed::PathSegment ps;

  ps = clients.ReconstructPath(subcursor_ids, vertices[3], &dba);
  ASSERT_EQ(ps.next_vertex, vertices[4]);
  ASSERT_EQ(ps.next_edge, std::experimental::nullopt);
  compare(ps.edges, {{3, 4}});

  ps = clients.ReconstructPath(subcursor_ids, vertices[4], &dba);
  EXPECT_EQ(ps.next_vertex, std::experimental::nullopt);
  EXPECT_EQ(ps.next_edge, (edges[{0, 1}]));
  compare(ps.edges, {{2, 4}, {1, 2}});

  ps = clients.ReconstructPath(subcursor_ids, edges[{0, 1}], &dba);
  EXPECT_EQ(ps.next_vertex, std::experimental::nullopt);
  EXPECT_EQ(ps.next_edge, std::experimental::nullopt);
  compare(ps.edges, {{0, 1}});

  clients.PrepareForExpand(subcursor_ids, true);
  clients.SetSource(subcursor_ids, vertices[3]);

  EXPECT_EQ(pull(0), std::experimental::nullopt);
  EXPECT_EQ(pull(1)->GlobalAddress(), vertices[4]);
  EXPECT_EQ(pull(1), std::experimental::nullopt);
  EXPECT_EQ(pull(2), std::experimental::nullopt);

  clients.RemoveBfsSubcursors(subcursor_ids);
}
