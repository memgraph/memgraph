#include <experimental/optional>
#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

using namespace database;

template <typename TIterable>
auto Count(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}

TEST(GraphDbAccessorTest, InsertVertex) {
  SingleNode db;
  GraphDbAccessor accessor(db);
  gid::Generator generator(0);

  EXPECT_EQ(Count(accessor.Vertices(false)), 0);

  EXPECT_EQ(accessor.InsertVertex().gid(), generator.Next());
  EXPECT_EQ(Count(accessor.Vertices(false)), 0);
  EXPECT_EQ(Count(accessor.Vertices(true)), 1);
  accessor.AdvanceCommand();
  EXPECT_EQ(Count(accessor.Vertices(false)), 1);

  EXPECT_EQ(accessor.InsertVertex().gid(), generator.Next());
  EXPECT_EQ(Count(accessor.Vertices(false)), 1);
  EXPECT_EQ(Count(accessor.Vertices(true)), 2);
  accessor.AdvanceCommand();
  EXPECT_EQ(Count(accessor.Vertices(false)), 2);
}

TEST(GraphDbAccessorTest, UniqueVertexId) {
  SingleNode db;
  SkipList<int64_t> ids;

  std::vector<std::thread> threads;
  for (int i = 0; i < 50; i++) {
    threads.emplace_back([&db, &ids]() {
      GraphDbAccessor dba(db);
      auto access = ids.access();
      for (int i = 0; i < 200; i++) access.insert(dba.InsertVertex().gid());
    });
  }

  for (auto &thread : threads) thread.join();
  EXPECT_EQ(ids.access().size(), 50 * 200);
}

TEST(GraphDbAccessorTest, RemoveVertexSameTransaction) {
  SingleNode db;
  GraphDbAccessor accessor(db);

  EXPECT_EQ(Count(accessor.Vertices(false)), 0);

  auto va1 = accessor.InsertVertex();
  accessor.AdvanceCommand();
  EXPECT_EQ(Count(accessor.Vertices(false)), 1);

  EXPECT_TRUE(accessor.RemoveVertex(va1));
  EXPECT_EQ(Count(accessor.Vertices(false)), 1);
  EXPECT_EQ(Count(accessor.Vertices(true)), 0);
  accessor.AdvanceCommand();
  EXPECT_EQ(Count(accessor.Vertices(false)), 0);
  EXPECT_EQ(Count(accessor.Vertices(true)), 0);
}

TEST(GraphDbAccessorTest, RemoveVertexDifferentTransaction) {
  SingleNode db;
  // first transaction creates a vertex
  {
    GraphDbAccessor accessor(db);
    accessor.InsertVertex();
    accessor.Commit();
  }
  // second transaction checks that it sees it, and deletes it
  {
    GraphDbAccessor accessor(db);
    EXPECT_EQ(Count(accessor.Vertices(false)), 1);
    EXPECT_EQ(Count(accessor.Vertices(true)), 1);
    for (auto vertex_accessor : accessor.Vertices(false))
      accessor.RemoveVertex(vertex_accessor);
    accessor.Commit();
  }
  // third transaction checks that it does not see the vertex
  {
    GraphDbAccessor accessor(db);
    EXPECT_EQ(Count(accessor.Vertices(false)), 0);
    EXPECT_EQ(Count(accessor.Vertices(true)), 0);
  }
}

TEST(GraphDbAccessorTest, InsertEdge) {
  SingleNode db;
  GraphDbAccessor dba(db);

  auto va1 = dba.InsertVertex();
  auto va2 = dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 0);
  EXPECT_EQ(va2.in_degree(), 0);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2)
  dba.InsertEdge(va1, va2, dba.EdgeType("likes"));
  EXPECT_EQ(Count(dba.Edges(false)), 0);
  EXPECT_EQ(Count(dba.Edges(true)), 1);
  dba.AdvanceCommand();
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  EXPECT_EQ(Count(dba.Edges(true)), 1);
  EXPECT_EQ(va1.out().begin()->to(), va2);
  EXPECT_EQ(va2.in().begin()->from(), va1);
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 1);
  EXPECT_EQ(va2.in_degree(), 1);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va3 = dba.InsertVertex();
  dba.InsertEdge(va3, va2, dba.EdgeType("hates"));
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  EXPECT_EQ(Count(dba.Edges(true)), 2);
  dba.AdvanceCommand();
  EXPECT_EQ(Count(dba.Edges(false)), 2);
  EXPECT_EQ(va3.out().begin()->to(), va2);
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 1);
  EXPECT_EQ(va2.in_degree(), 2);
  EXPECT_EQ(va2.out_degree(), 0);
  EXPECT_EQ(va3.in_degree(), 0);
  EXPECT_EQ(va3.out_degree(), 1);
}

TEST(GraphDbAccessorTest, UniqueEdgeId) {
  SingleNode db;
  SkipList<int64_t> ids;

  std::vector<std::thread> threads;
  for (int i = 0; i < 50; i++) {
    threads.emplace_back([&db, &ids]() {
      GraphDbAccessor dba(db);
      auto v1 = dba.InsertVertex();
      auto v2 = dba.InsertVertex();
      auto edge_type = dba.EdgeType("edge_type");
      auto access = ids.access();
      for (int i = 0; i < 200; i++)
        access.insert(dba.InsertEdge(v1, v2, edge_type).gid());
    });
  }

  for (auto &thread : threads) thread.join();
  EXPECT_EQ(ids.access().size(), 50 * 200);
}

TEST(GraphDbAccessorTest, RemoveEdge) {
  SingleNode db;
  GraphDbAccessor dba(db);

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba.InsertVertex();
  auto va2 = dba.InsertVertex();
  auto va3 = dba.InsertVertex();
  dba.InsertEdge(va1, va2, dba.EdgeType("likes"));
  dba.InsertEdge(va3, va2, dba.EdgeType("hates"));
  dba.AdvanceCommand();
  EXPECT_EQ(Count(dba.Edges(false)), 2);
  EXPECT_EQ(Count(dba.Edges(true)), 2);

  // remove all [:hates] edges
  for (auto edge : dba.Edges(false))
    if (edge.EdgeType() == dba.EdgeType("hates")) dba.RemoveEdge(edge);
  EXPECT_EQ(Count(dba.Edges(false)), 2);
  EXPECT_EQ(Count(dba.Edges(true)), 1);

  // current state: (v1) - [:likes] -> (v2), (v3)
  dba.AdvanceCommand();
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  EXPECT_EQ(Count(dba.Edges(true)), 1);
  EXPECT_EQ(Count(dba.Vertices(false)), 3);
  EXPECT_EQ(Count(dba.Vertices(true)), 3);
  for (auto edge : dba.Edges(false)) {
    EXPECT_EQ(edge.EdgeType(), dba.EdgeType("likes"));
    auto v1 = edge.from();
    auto v2 = edge.to();

    // ensure correct connectivity for all the vertices
    for (auto vertex : dba.Vertices(false)) {
      if (vertex == v1) {
        EXPECT_EQ(vertex.in_degree(), 0);
        EXPECT_EQ(vertex.out_degree(), 1);
      } else if (vertex == v2) {
        EXPECT_EQ(vertex.in_degree(), 1);
        EXPECT_EQ(vertex.out_degree(), 0);
      } else {
        EXPECT_EQ(vertex.in_degree(), 0);
        EXPECT_EQ(vertex.out_degree(), 0);
      }
    }
  }
}

TEST(GraphDbAccessorTest, DetachRemoveVertex) {
  SingleNode db;
  GraphDbAccessor dba(db);

  // setup (v0)- []->(v1)<-[]-(v2)<-[]-(v3)
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 4; ++i) vertices.emplace_back(dba.InsertVertex());

  auto edge_type = dba.EdgeType("type");
  dba.InsertEdge(vertices[0], vertices[1], edge_type);
  dba.InsertEdge(vertices[2], vertices[1], edge_type);
  dba.InsertEdge(vertices[3], vertices[2], edge_type);

  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  // ensure that plain remove does NOT work
  EXPECT_EQ(Count(dba.Vertices(false)), 4);
  EXPECT_EQ(Count(dba.Edges(false)), 3);
  EXPECT_FALSE(dba.RemoveVertex(vertices[0]));
  EXPECT_FALSE(dba.RemoveVertex(vertices[1]));
  EXPECT_FALSE(dba.RemoveVertex(vertices[2]));
  EXPECT_EQ(Count(dba.Vertices(false)), 4);
  EXPECT_EQ(Count(dba.Edges(false)), 3);

  dba.DetachRemoveVertex(vertices[2]);
  EXPECT_EQ(Count(dba.Vertices(false)), 4);
  EXPECT_EQ(Count(dba.Vertices(true)), 3);
  EXPECT_EQ(Count(dba.Edges(false)), 3);
  EXPECT_EQ(Count(dba.Edges(true)), 1);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  EXPECT_EQ(Count(dba.Vertices(false)), 3);
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  EXPECT_TRUE(dba.RemoveVertex(vertices[3]));
  EXPECT_EQ(Count(dba.Vertices(true)), 2);
  EXPECT_EQ(Count(dba.Vertices(false)), 3);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  EXPECT_EQ(Count(dba.Vertices(false)), 2);
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  for (auto va : dba.Vertices(false)) EXPECT_FALSE(dba.RemoveVertex(va));
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  EXPECT_EQ(Count(dba.Vertices(false)), 2);
  EXPECT_EQ(Count(dba.Edges(false)), 1);
  for (auto va : dba.Vertices(false)) {
    EXPECT_FALSE(dba.RemoveVertex(va));
    dba.DetachRemoveVertex(va);
    break;
  }
  EXPECT_EQ(Count(dba.Vertices(true)), 1);
  EXPECT_EQ(Count(dba.Vertices(false)), 2);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  EXPECT_EQ(Count(dba.Vertices(false)), 1);
  EXPECT_EQ(Count(dba.Edges(false)), 0);

  // remove the last vertex, it has no connections
  // so that should work
  for (auto va : dba.Vertices(false)) EXPECT_TRUE(dba.RemoveVertex(va));
  dba.AdvanceCommand();

  EXPECT_EQ(Count(dba.Vertices(false)), 0);
  EXPECT_EQ(Count(dba.Edges(false)), 0);
}

TEST(GraphDbAccessorTest, DetachRemoveVertexMultiple) {
  // This test checks that we can detach remove the
  // same vertex / edge multiple times

  SingleNode db;
  GraphDbAccessor dba(db);

  // setup: make a fully connected N graph
  // with cycles too!
  int N = 7;
  std::vector<VertexAccessor> vertices;
  auto edge_type = dba.EdgeType("edge");
  for (int i = 0; i < N; ++i) vertices.emplace_back(dba.InsertVertex());
  for (int j = 0; j < N; ++j)
    for (int k = 0; k < N; ++k)
      dba.InsertEdge(vertices[j], vertices[k], edge_type);

  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();

  EXPECT_EQ(Count(dba.Vertices(false)), N);
  EXPECT_EQ(Count(dba.Edges(false)), N * N);

  // detach delete one edge
  dba.DetachRemoveVertex(vertices[0]);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();
  EXPECT_EQ(Count(dba.Vertices(false)), N - 1);
  EXPECT_EQ(Count(dba.Edges(false)), (N - 1) * (N - 1));

  // detach delete two neighboring edges
  dba.DetachRemoveVertex(vertices[1]);
  dba.DetachRemoveVertex(vertices[2]);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();
  EXPECT_EQ(Count(dba.Vertices(false)), N - 3);
  EXPECT_EQ(Count(dba.Edges(false)), (N - 3) * (N - 3));

  // detach delete everything, buwahahahaha
  for (int l = 3; l < N; ++l) dba.DetachRemoveVertex(vertices[l]);
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();
  EXPECT_EQ(Count(dba.Vertices(false)), 0);
  EXPECT_EQ(Count(dba.Edges(false)), 0);
}

TEST(GraphDbAccessorTest, Labels) {
  SingleNode db;
  GraphDbAccessor dba(db);

  Label label_friend = dba.Label("friend");
  EXPECT_EQ(label_friend, dba.Label("friend"));
  EXPECT_NE(label_friend, dba.Label("friend2"));
  EXPECT_EQ(dba.LabelName(label_friend), "friend");

  // test that getting labels through a different accessor works
  EXPECT_EQ(label_friend, GraphDbAccessor(db).Label("friend"));
  EXPECT_NE(label_friend, GraphDbAccessor(db).Label("friend2"));
}

TEST(GraphDbAccessorTest, EdgeTypes) {
  SingleNode db;
  GraphDbAccessor dba(db);

  EdgeType edge_type = dba.EdgeType("likes");
  EXPECT_EQ(edge_type, dba.EdgeType("likes"));
  EXPECT_NE(edge_type, dba.EdgeType("hates"));
  EXPECT_EQ(dba.EdgeTypeName(edge_type), "likes");

  // test that getting labels through a different accessor works
  EXPECT_EQ(edge_type, GraphDbAccessor(db).EdgeType("likes"));
  EXPECT_NE(edge_type, GraphDbAccessor(db).EdgeType("hates"));
}

TEST(GraphDbAccessorTest, Properties) {
  SingleNode db;
  GraphDbAccessor dba(db);

  Property prop = dba.Property("name");
  EXPECT_EQ(prop, dba.Property("name"));
  EXPECT_NE(prop, dba.Property("surname"));
  EXPECT_EQ(dba.PropertyName(prop), "name");

  // test that getting labels through a different accessor works
  EXPECT_EQ(prop, GraphDbAccessor(db).Property("name"));
  EXPECT_NE(prop, GraphDbAccessor(db).Property("surname"));
}

TEST(GraphDbAccessorTest, Transfer) {
  SingleNode db;

  GraphDbAccessor dba1(db);
  auto prop = dba1.Property("property");
  VertexAccessor v1 = dba1.InsertVertex();
  v1.PropsSet(prop, 1);
  VertexAccessor v2 = dba1.InsertVertex();
  v2.PropsSet(prop, 2);
  EdgeAccessor e12 = dba1.InsertEdge(v1, v2, dba1.EdgeType("et"));
  e12.PropsSet(prop, 12);

  // make dba2 that has dba1 in it's snapshot, so data isn't visible
  GraphDbAccessor dba2(db);
  EXPECT_EQ(dba2.Transfer(v1), std::experimental::nullopt);
  EXPECT_EQ(dba2.Transfer(e12), std::experimental::nullopt);

  // make dba3 that does not have dba1 in it's snapshot
  dba1.Commit();
  GraphDbAccessor dba3(db);
  // we can transfer accessors even though the GraphDbAccessor they
  // belong to is not alive anymore
  EXPECT_EQ(dba3.Transfer(v1)->PropsAt(prop).Value<int64_t>(), 1);
  EXPECT_EQ(dba3.Transfer(e12)->PropsAt(prop).Value<int64_t>(), 12);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  //  ::testing::GTEST_FLAG(filter) = "*.DetachRemoveVertex";
  return RUN_ALL_TESTS();
}
