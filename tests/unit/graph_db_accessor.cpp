#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

size_t CountVertices(GraphDbAccessor &db_accessor) {
  size_t r_val = 0;
  for ([[gnu::unused]] auto va : db_accessor.vertices()) r_val++;

  return r_val;
}

size_t CountEdges(GraphDbAccessor &db_accessor) {
  size_t r_val = 0;
  for ([[gnu::unused]] auto va : db_accessor.edges()) r_val++;

  return r_val;
}

TEST(GraphDbAccessorTest, DbmsCreateDefault) {
  Dbms dbms;
  auto accessor = dbms.active();
  EXPECT_EQ(accessor->name(), "default");
}

TEST(GraphDbAccessorTest, InsertVertex) {
  Dbms dbms;
  auto accessor = dbms.active();

  EXPECT_EQ(CountVertices(*accessor), 0);

  accessor->insert_vertex();
  accessor->advance_command();
  EXPECT_EQ(CountVertices(*accessor), 1);

  accessor->insert_vertex();
  accessor->advance_command();
  EXPECT_EQ(CountVertices(*accessor), 2);
}

TEST(GraphDbAccessorTest, RemoveVertexSameTransaction) {
  Dbms dbms;
  auto accessor = dbms.active();

  EXPECT_EQ(CountVertices(*accessor), 0);

  auto va1 = accessor->insert_vertex();
  accessor->advance_command();
  EXPECT_EQ(CountVertices(*accessor), 1);

  EXPECT_TRUE(accessor->remove_vertex(va1));
  EXPECT_EQ(CountVertices(*accessor), 1);
  accessor->advance_command();
  EXPECT_EQ(CountVertices(*accessor), 0);
}

TEST(GraphDbAccessorTest, RemoveVertexDifferentTransaction) {
  Dbms dbms;

  // first transaction creates a vertex
  auto accessor1 = dbms.active();
  accessor1->insert_vertex();
  accessor1->commit();

  // second transaction checks that it sees it, and deletes it
  auto accessor2 = dbms.active();
  EXPECT_EQ(CountVertices(*accessor2), 1);
  for (auto vertex_accessor : accessor2->vertices())
    accessor2->remove_vertex(vertex_accessor);
  accessor2->commit();

  // third transaction checks that it does not see the vertex
  auto accessor3 = dbms.active();
  EXPECT_EQ(CountVertices(*accessor3), 0);
}

TEST(GraphDbAccessorTest, InsertEdge) {
  Dbms dbms;
  auto dba = dbms.active();

  auto va1 = dba->insert_vertex();
  auto va2 = dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 0);
  EXPECT_EQ(va2.in_degree(), 0);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2)
  dba->insert_edge(va1, va2, dba->edge_type("likes"));
  dba->advance_command();
  EXPECT_EQ(CountEdges(*dba), 1);
  EXPECT_EQ(va1.out().begin()->to(), va2);
  EXPECT_EQ(va2.in().begin()->from(), va1);
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 1);
  EXPECT_EQ(va2.in_degree(), 1);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va3 = dba->insert_vertex();
  dba->insert_edge(va3, va2, dba->edge_type("hates"));
  dba->advance_command();
  EXPECT_EQ(CountEdges(*dba), 2);
  EXPECT_EQ(va3.out().begin()->to(), va2);
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 1);
  EXPECT_EQ(va2.in_degree(), 2);
  EXPECT_EQ(va2.out_degree(), 0);
  EXPECT_EQ(va3.in_degree(), 0);
  EXPECT_EQ(va3.out_degree(), 1);
}

TEST(GraphDbAccessorTest, RemoveEdge) {
  Dbms dbms;
  auto dba = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba->insert_vertex();
  auto va2 = dba->insert_vertex();
  auto va3 = dba->insert_vertex();
  dba->insert_edge(va1, va2, dba->edge_type("likes"));
  dba->insert_edge(va3, va2, dba->edge_type("hates"));
  dba->advance_command();
  EXPECT_EQ(CountEdges(*dba), 2);

  // remove all [:hates] edges
  EXPECT_EQ(CountEdges(*dba), 2);
  for (auto edge : dba->edges())
    if (edge.edge_type() == dba->edge_type("hates")) dba->remove_edge(edge);

  // current state: (v1) - [:likes] -> (v2), (v3)
  dba->advance_command();
  EXPECT_EQ(CountEdges(*dba), 1);
  EXPECT_EQ(CountVertices(*dba), 3);
  for (auto edge : dba->edges()) {
    EXPECT_EQ(edge.edge_type(), dba->edge_type("likes"));
    auto v1 = edge.from();
    auto v2 = edge.to();

    // ensure correct connectivity for all the vertices
    for (auto vertex : dba->vertices()) {
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
  Dbms dbms;
  auto dba = dbms.active();

  // setup (v1)- []->(v2)<-[]-(v3)<-[]-(v4)
  auto va1 = dba->insert_vertex();
  auto va2 = dba->insert_vertex();
  auto va3 = dba->insert_vertex();
  auto va4 = dba->insert_vertex();
  auto edge_type = dba->edge_type("type");
  dba->insert_edge(va1, va2, edge_type);
  dba->insert_edge(va1, va3, edge_type);
  dba->insert_edge(va4, va3, edge_type);
  dba->advance_command();

  // ensure that plain remove does NOT work
  EXPECT_EQ(CountVertices(*dba), 4);
  EXPECT_EQ(CountEdges(*dba), 3);
  EXPECT_FALSE(dba->remove_vertex(va1));
  EXPECT_FALSE(dba->remove_vertex(va2));
  EXPECT_FALSE(dba->remove_vertex(va3));
  EXPECT_EQ(CountVertices(*dba), 4);
  EXPECT_EQ(CountEdges(*dba), 3);

  dba->detach_remove_vertex(va3);
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), 3);
  EXPECT_EQ(CountEdges(*dba), 1);
  EXPECT_TRUE(dba->remove_vertex(va4));
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), 2);
  EXPECT_EQ(CountEdges(*dba), 1);
  for (auto va : dba->vertices()) EXPECT_FALSE(dba->remove_vertex(va));
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), 2);
  EXPECT_EQ(CountEdges(*dba), 1);
  for (auto va : dba->vertices()) {
    EXPECT_FALSE(dba->remove_vertex(va));
    dba->detach_remove_vertex(va);
    break;
  }
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), 1);
  EXPECT_EQ(CountEdges(*dba), 0);

  // remove the last vertex, it has no connections
  // so that should work
  for (auto va : dba->vertices()) EXPECT_TRUE(dba->remove_vertex(va));
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), 0);
  EXPECT_EQ(CountEdges(*dba), 0);
}

TEST(GraphDbAccessorTest, DetachRemoveVertexMultiple) {
  // This test checks that we can detach remove the
  // same vertex / edge multiple times

  Dbms dbms;
  auto dba = dbms.active();

  // setup: make a fully connected N graph
  // with cycles too!
  int N = 7;
  std::vector<VertexAccessor> vertices;
  auto edge_type = dba->edge_type("edge");
  for (int i = 0; i < N; ++i)
    vertices.emplace_back(dba->insert_vertex());
  for (int j = 0; j < N; ++j)
    for (int k = 0; k < N; ++k)
      dba->insert_edge(vertices[j], vertices[k], edge_type);
  dba->advance_command();

  EXPECT_EQ(CountVertices(*dba), N);
  EXPECT_EQ(CountEdges(*dba), N * N);

  // detach delete one edge
  dba->detach_remove_vertex(vertices[0]);
  dba->advance_command();
  EXPECT_EQ(CountVertices(*dba), N - 1);
  EXPECT_EQ(CountEdges(*dba), (N - 1) * (N - 1));

  // detach delete two neighboring edges
  dba->detach_remove_vertex(vertices[1]);
  dba->detach_remove_vertex(vertices[2]);
  dba->advance_command();
  EXPECT_EQ(CountVertices(*dba), N - 3);
  EXPECT_EQ(CountEdges(*dba), (N - 3) * (N - 3));

  // detach delete everything, buwahahahaha
  for (int l = 3; l < N ; ++l)
    dba->detach_remove_vertex(vertices[l]);
  dba->advance_command();
  EXPECT_EQ(CountVertices(*dba), 0);
  EXPECT_EQ(CountEdges(*dba), 0);
}

TEST(GraphDbAccessorTest, Labels) {
  Dbms dbms;
  auto dba1 = dbms.active();

  GraphDbTypes::Label label_friend = dba1->label("friend");
  EXPECT_EQ(label_friend, dba1->label("friend"));
  EXPECT_NE(label_friend, dba1->label("friend2"));
  EXPECT_EQ(dba1->label_name(label_friend), "friend");

  // test that getting labels through a different accessor works
  EXPECT_EQ(label_friend, dbms.active()->label("friend"));
  EXPECT_NE(label_friend, dbms.active()->label("friend2"));
}

TEST(GraphDbAccessorTest, EdgeTypes) {
  Dbms dbms;
  auto dba1 = dbms.active();

  GraphDbTypes::EdgeType edge_type = dba1->edge_type("likes");
  EXPECT_EQ(edge_type, dba1->edge_type("likes"));
  EXPECT_NE(edge_type, dba1->edge_type("hates"));
  EXPECT_EQ(dba1->edge_type_name(edge_type), "likes");

  // test that getting labels through a different accessor works
  EXPECT_EQ(edge_type, dbms.active()->edge_type("likes"));
  EXPECT_NE(edge_type, dbms.active()->edge_type("hates"));
}

TEST(GraphDbAccessorTest, Properties) {
  Dbms dbms;
  auto dba1 = dbms.active();

  GraphDbTypes::EdgeType prop = dba1->property("name");
  EXPECT_EQ(prop, dba1->property("name"));
  EXPECT_NE(prop, dba1->property("surname"));
  EXPECT_EQ(dba1->property_name(prop), "name");

  // test that getting labels through a different accessor works
  EXPECT_EQ(prop, dbms.active()->property("name"));
  EXPECT_NE(prop, dbms.active()->property("surname"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
//  ::testing::GTEST_FLAG(filter) = "*.DetachRemoveVertex";
  return RUN_ALL_TESTS();
}
