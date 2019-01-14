#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "distributed/coordination.hpp"
#include "storage/distributed/edge.hpp"
#include "storage/distributed/vertex.hpp"
#include "transactions/distributed/engine_master.hpp"
#include "utils/algorithm.hpp"

#include "test_coordination.hpp"

#include "storage/distributed/edges.hpp"

TEST(Edges, Filtering) {
  Edges edges;

  TestMasterCoordination coordination;
  tx::EngineMaster tx_engine(&coordination);
  auto tx = tx_engine.Begin();

  int64_t vertex_gid = 0;

  mvcc::VersionList<Vertex> v0(*tx, vertex_gid, vertex_gid);
  storage::VertexAddress va0{&v0};
  vertex_gid++;

  mvcc::VersionList<Vertex> v1(*tx, vertex_gid, vertex_gid);
  storage::VertexAddress va1{&v1};
  vertex_gid++;

  mvcc::VersionList<Vertex> v2(*tx, vertex_gid, vertex_gid);
  storage::VertexAddress va2{&v2};
  vertex_gid++;

  mvcc::VersionList<Vertex> v3(*tx, vertex_gid, vertex_gid);
  storage::VertexAddress va3{&v3};
  vertex_gid++;

  storage::EdgeType t1{1};
  storage::EdgeType t2{2};

  int64_t edge_gid = 0;

  mvcc::VersionList<Edge> e1(*tx, edge_gid, edge_gid, va0, va1, t1);
  storage::EdgeAddress ea1(&e1);
  edges.emplace(va1, ea1, t1);
  edge_gid++;

  mvcc::VersionList<Edge> e2(*tx, edge_gid, edge_gid, va0, va2, t1);
  storage::EdgeAddress ea2(&e2);
  edges.emplace(va2, ea2, t2);
  edge_gid++;

  mvcc::VersionList<Edge> e3(*tx, edge_gid, edge_gid, va0, va3, t1);
  storage::EdgeAddress ea3(&e3);
  edges.emplace(va3, ea3, t1);
  edge_gid++;

  mvcc::VersionList<Edge> e4(*tx, edge_gid, edge_gid, va0, va1, t1);
  storage::EdgeAddress ea4(&e4);
  edges.emplace(va1, ea4, t2);
  edge_gid++;

  mvcc::VersionList<Edge> e5(*tx, edge_gid, edge_gid, va0, va2, t1);
  storage::EdgeAddress ea5(&e5);
  edges.emplace(va2, ea5, t1);
  edge_gid++;

  mvcc::VersionList<Edge> e6(*tx, edge_gid, edge_gid, va0, va3, t1);
  storage::EdgeAddress ea6(&e6);
  edges.emplace(va3, ea6, t2);
  edge_gid++;

  auto edge_addresses =
      [edges](std::experimental::optional<storage::VertexAddress> dest,
              std::vector<storage::EdgeType> *edge_types) {
        std::vector<storage::EdgeAddress> ret;
        for (auto it = edges.begin(dest, edge_types); it != edges.end(); ++it)
          ret.push_back(it->edge);
        return ret;
      };

  {  // no filtering
    EXPECT_THAT(edge_addresses(std::experimental::nullopt, nullptr),
                ::testing::UnorderedElementsAre(ea1, ea2, ea3, ea4, ea5, ea6));
  }
  {
    // filter by node
    EXPECT_THAT(edge_addresses(va1, nullptr),
                ::testing::UnorderedElementsAre(ea1, ea4));
    EXPECT_THAT(edge_addresses(va2, nullptr),
                ::testing::UnorderedElementsAre(ea2, ea5));
    EXPECT_THAT(edge_addresses(va3, nullptr),
                ::testing::UnorderedElementsAre(ea3, ea6));
  }

  {
    // filter by edge type
    std::vector<storage::EdgeType> f1{t1};
    std::vector<storage::EdgeType> f2{t2};
    std::vector<storage::EdgeType> f3{t1, t2};

    EXPECT_THAT(edge_addresses(std::experimental::nullopt, &f1),
                ::testing::UnorderedElementsAre(ea1, ea3, ea5));
    EXPECT_THAT(edge_addresses(std::experimental::nullopt, &f2),
                ::testing::UnorderedElementsAre(ea2, ea4, ea6));
    EXPECT_THAT(edge_addresses(std::experimental::nullopt, &f3),
                ::testing::UnorderedElementsAre(ea1, ea2, ea3, ea4, ea5, ea6));
  }

  {
    // filter by both node and edge type
    std::vector<storage::EdgeType> f1{t1};
    std::vector<storage::EdgeType> f2{t2};

    EXPECT_THAT(edge_addresses(va1, &f1), ::testing::UnorderedElementsAre(ea1));
    EXPECT_THAT(edge_addresses(va1, &f2), ::testing::UnorderedElementsAre(ea4));
    EXPECT_THAT(edge_addresses(va2, &f1), ::testing::UnorderedElementsAre(ea5));
    EXPECT_THAT(edge_addresses(va2, &f2), ::testing::UnorderedElementsAre(ea2));
    EXPECT_THAT(edge_addresses(va3, &f1), ::testing::UnorderedElementsAre(ea3));
    EXPECT_THAT(edge_addresses(va3, &f2), ::testing::UnorderedElementsAre(ea6));
  }

  tx_engine.Abort(*tx);
}
