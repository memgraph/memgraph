#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/single_node/edge.hpp"
#include "storage/single_node/vertex.hpp"
#include "transactions/single_node/engine.hpp"
#include "utils/algorithm.hpp"

#include "storage/single_node/edges.hpp"

TEST(Edges, Filtering) {
  Edges edges;

  tx::Engine tx_engine;
  auto tx = tx_engine.Begin();

  int64_t vertex_gid = 0;
  mvcc::VersionList<Vertex> v0(*tx, vertex_gid++);
  mvcc::VersionList<Vertex> v1(*tx, vertex_gid++);
  mvcc::VersionList<Vertex> v2(*tx, vertex_gid++);
  mvcc::VersionList<Vertex> v3(*tx, vertex_gid++);

  storage::EdgeType t1{1};
  storage::EdgeType t2{2};

  int64_t edge_gid = 0;
  mvcc::VersionList<Edge> e1(*tx, edge_gid++, &v0, &v1, t1);
  edges.emplace(&v1, &e1, t1);

  mvcc::VersionList<Edge> e2(*tx, edge_gid++, &v0, &v2, t2);
  edges.emplace(&v2, &e2, t2);

  mvcc::VersionList<Edge> e3(*tx, edge_gid++, &v0, &v3, t1);
  edges.emplace(&v3, &e3, t1);

  mvcc::VersionList<Edge> e4(*tx, edge_gid++, &v0, &v1, t2);
  edges.emplace(&v1, &e4, t2);

  mvcc::VersionList<Edge> e5(*tx, edge_gid++, &v0, &v2, t1);
  edges.emplace(&v2, &e5, t1);

  mvcc::VersionList<Edge> e6(*tx, edge_gid++, &v0, &v3, t2);
  edges.emplace(&v3, &e6, t2);

  auto edge_addresses = [edges](mvcc::VersionList<Vertex> *dest,
                                std::vector<storage::EdgeType> *edge_types) {
    std::vector<mvcc::VersionList<Edge> *> ret;
    for (auto it = edges.begin(dest, edge_types); it != edges.end(); ++it)
      ret.push_back(it->edge);
    return ret;
  };

  {  // no filtering
    EXPECT_THAT(edge_addresses(nullptr, nullptr),
                ::testing::UnorderedElementsAre(&e1, &e2, &e3, &e4, &e5, &e6));
  }

  {
    // filter by node
    EXPECT_THAT(edge_addresses(&v1, nullptr),
                ::testing::UnorderedElementsAre(&e1, &e4));
    EXPECT_THAT(edge_addresses(&v2, nullptr),
                ::testing::UnorderedElementsAre(&e2, &e5));
    EXPECT_THAT(edge_addresses(&v3, nullptr),
                ::testing::UnorderedElementsAre(&e3, &e6));
  }

  {
    // filter by edge type
    std::vector<storage::EdgeType> f1{t1};
    std::vector<storage::EdgeType> f2{t2};
    std::vector<storage::EdgeType> f3{t1, t2};

    EXPECT_THAT(edge_addresses(nullptr, &f1),
                ::testing::UnorderedElementsAre(&e1, &e3, &e5));
    EXPECT_THAT(edge_addresses(nullptr, &f2),
                ::testing::UnorderedElementsAre(&e2, &e4, &e6));
    EXPECT_THAT(edge_addresses(nullptr, &f3),
                ::testing::UnorderedElementsAre(&e1, &e2, &e3, &e4, &e5, &e6));
  }

  {
    // filter by both node and edge type
    std::vector<storage::EdgeType> f1{t1};
    std::vector<storage::EdgeType> f2{t2};

    EXPECT_THAT(edge_addresses(&v1, &f1), ::testing::UnorderedElementsAre(&e1));
    EXPECT_THAT(edge_addresses(&v1, &f2), ::testing::UnorderedElementsAre(&e4));
    EXPECT_THAT(edge_addresses(&v2, &f1), ::testing::UnorderedElementsAre(&e5));
    EXPECT_THAT(edge_addresses(&v2, &f2), ::testing::UnorderedElementsAre(&e2));
    EXPECT_THAT(edge_addresses(&v3, &f1), ::testing::UnorderedElementsAre(&e3));
    EXPECT_THAT(edge_addresses(&v3, &f2), ::testing::UnorderedElementsAre(&e6));
  }

  tx_engine.Abort(*tx);
}
