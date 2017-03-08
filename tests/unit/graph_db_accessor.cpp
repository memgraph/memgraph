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
  GraphDbAccessor accessor = dbms.active();
  EXPECT_EQ(accessor.name(), "default");
}

TEST(GraphDbAccessorTest, InsertVertex) {
  Dbms dbms;
  GraphDbAccessor accessor = dbms.active();

  EXPECT_EQ(CountVertices(accessor), 0);

  accessor.insert_vertex();
  EXPECT_EQ(CountVertices(accessor), 1);

  accessor.insert_vertex();
  EXPECT_EQ(CountVertices(accessor), 2);
}

TEST(GraphDbAccessorTest, RemoveVertexSameTransaction) {
  Dbms dbms;
  GraphDbAccessor accessor = dbms.active();

  EXPECT_EQ(CountVertices(accessor), 0);

  auto va1 = accessor.insert_vertex();
  EXPECT_EQ(CountVertices(accessor), 1);

  EXPECT_TRUE(accessor.remove_vertex(va1));
  EXPECT_EQ(CountVertices(accessor), 1);
  accessor.advance_command();
  EXPECT_EQ(CountVertices(accessor), 0);
}

TEST(GraphDbAccessorTest, RemoveVertexDifferentTransaction) {
  Dbms dbms;

  // first transaction creates a vertex
  GraphDbAccessor accessor1 = dbms.active();
  accessor1.insert_vertex();
  accessor1.commit();

  // second transaction checks that it sees it, and deletes it
  GraphDbAccessor accessor2 = dbms.active();
  EXPECT_EQ(CountVertices(accessor2), 1);
  for (auto vertex_accessor : accessor2.vertices())
    accessor2.remove_vertex(vertex_accessor);
  accessor2.commit();

  // third transaction checks that it does not see the vertex
  GraphDbAccessor accessor3 = dbms.active();
  EXPECT_EQ(CountVertices(accessor3), 0);
}

TEST(GraphDbAccessorTest, InsertEdge) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto va1 = dba.insert_vertex();
  auto va2 = dba.insert_vertex();
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 0);
  EXPECT_EQ(va2.in_degree(), 0);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2)
  dba.insert_edge(va1, va2, dba.edge_type("likes"));
  EXPECT_EQ(CountEdges(dba), 1);
  EXPECT_EQ(va1.out().begin()->to(), va2);
  EXPECT_EQ(va2.in().begin()->from(), va1);
  EXPECT_EQ(va1.in_degree(), 0);
  EXPECT_EQ(va1.out_degree(), 1);
  EXPECT_EQ(va2.in_degree(), 1);
  EXPECT_EQ(va2.out_degree(), 0);

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va3 = dba.insert_vertex();
  dba.insert_edge(va3, va2, dba.edge_type("hates"));
  EXPECT_EQ(CountEdges(dba), 2);
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
  GraphDbAccessor dba1 = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba1.insert_vertex();
  auto va2 = dba1.insert_vertex();
  auto va3 = dba1.insert_vertex();
  dba1.insert_edge(va1, va2, dba1.edge_type("likes"));
  dba1.insert_edge(va3, va2, dba1.edge_type("hates"));
  EXPECT_EQ(CountEdges(dba1), 2);

  // remove all [:hates] edges
  dba1.commit();
  GraphDbAccessor dba2 = dbms.active();
  EXPECT_EQ(CountEdges(dba2), 2);
  for (auto edge : dba2.edges())
    if (edge.edge_type() == dba2.edge_type("hates")) dba2.remove_edge(edge);

  // current state: (v1) - [:likes] -> (v2), (v3)
  dba2.commit();
  GraphDbAccessor dba3 = dbms.active();
  EXPECT_EQ(CountEdges(dba3), 1);
  EXPECT_EQ(CountVertices(dba3), 3);
  for (auto edge : dba3.edges()) {
    EXPECT_EQ(edge.edge_type(), dba3.edge_type("likes"));
    auto v1 = edge.from();
    auto v2 = edge.to();

    // ensure correct connectivity for all the vertices
    for (auto vertex : dba3.vertices()) {
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
  GraphDbAccessor dba1 = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba1.insert_vertex();
  auto va2 = dba1.insert_vertex();
  auto va3 = dba1.insert_vertex();
  dba1.insert_edge(va1, va2, dba1.edge_type("likes"));
  dba1.insert_edge(va1, va3, dba1.edge_type("likes"));

  // ensure that plain remove does NOT work
  EXPECT_EQ(CountVertices(dba1), 3);
  EXPECT_EQ(CountEdges(dba1), 2);
  EXPECT_FALSE(dba1.remove_vertex(va1));
  EXPECT_FALSE(dba1.remove_vertex(va2));
  EXPECT_FALSE(dba1.remove_vertex(va3));
  EXPECT_EQ(CountVertices(dba1), 3);
  EXPECT_EQ(CountEdges(dba1), 2);

  // make a new transaction because at the moment deletions
  // in the same transaction are not visible
  // DETACH REMOVE V3
  // new situation: (v1) - [:likes] -> (v2)
  dba1.detach_remove_vertex(va3);
  dba1.commit();
  GraphDbAccessor dba2 = dbms.active();

  EXPECT_EQ(CountVertices(dba2), 2);
  EXPECT_EQ(CountEdges(dba2), 1);
  for (auto va : dba2.vertices()) EXPECT_FALSE(dba2.remove_vertex(va));

  dba2.commit();
  GraphDbAccessor dba3 = dbms.active();
  EXPECT_EQ(CountVertices(dba3), 2);
  EXPECT_EQ(CountEdges(dba3), 1);

  for (auto va : dba3.vertices()) {
    EXPECT_FALSE(dba3.remove_vertex(va));
    dba3.detach_remove_vertex(va);
    break;
  }

  dba3.commit();
  GraphDbAccessor dba4 = dbms.active();
  EXPECT_EQ(CountVertices(dba4), 1);
  EXPECT_EQ(CountEdges(dba4), 0);

  // remove the last vertex, it has no connections
  // so that should work
  for (auto va : dba4.vertices()) EXPECT_TRUE(dba4.remove_vertex(va));

  dba4.commit();
  GraphDbAccessor dba5 = dbms.active();
  EXPECT_EQ(CountVertices(dba5), 0);
  EXPECT_EQ(CountEdges(dba5), 0);
}

TEST(GraphDbAccessorTest, Labels) {
  Dbms dbms;
  GraphDbAccessor dba1 = dbms.active();

  GraphDb::Label label_friend = dba1.label("friend");
  EXPECT_EQ(label_friend, dba1.label("friend"));
  EXPECT_NE(label_friend, dba1.label("friend2"));
  EXPECT_EQ(dba1.label_name(label_friend), "friend");

  // test that getting labels through a different accessor works
  EXPECT_EQ(label_friend, dbms.active().label("friend"));
  EXPECT_NE(label_friend, dbms.active().label("friend2"));
}

TEST(GraphDbAccessorTest, EdgeTypes) {
  Dbms dbms;
  GraphDbAccessor dba1 = dbms.active();

  GraphDb::EdgeType edge_type = dba1.edge_type("likes");
  EXPECT_EQ(edge_type, dba1.edge_type("likes"));
  EXPECT_NE(edge_type, dba1.edge_type("hates"));
  EXPECT_EQ(dba1.edge_type_name(edge_type), "likes");

  // test that getting labels through a different accessor works
  EXPECT_EQ(edge_type, dbms.active().edge_type("likes"));
  EXPECT_NE(edge_type, dbms.active().edge_type("hates"));
}

TEST(GraphDbAccessorTest, Properties) {
  Dbms dbms;
  GraphDbAccessor dba1 = dbms.active();

  GraphDb::EdgeType prop = dba1.property("name");
  EXPECT_EQ(prop, dba1.property("name"));
  EXPECT_NE(prop, dba1.property("surname"));
  EXPECT_EQ(dba1.property_name(prop), "name");

  // test that getting labels through a different accessor works
  EXPECT_EQ(prop, dbms.active().property("name"));
  EXPECT_NE(prop, dbms.active().property("surname"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
