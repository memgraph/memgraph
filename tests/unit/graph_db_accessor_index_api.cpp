#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "data_structures/ptr_int.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"

using testing::UnorderedElementsAreArray;

template <typename TIterable>
auto Count(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}

TEST(GraphDbAccessor, VertexCount) {
  Dbms dbms;
  auto dba = dbms.active();
  auto lab1 = dba->label("lab1");
  auto lab2 = dba->label("lab2");

  EXPECT_EQ(dba->vertices_count(lab1), 0);
  EXPECT_EQ(dba->vertices_count(lab2), 0);
  EXPECT_EQ(dba->vertices_count(), 0);
  for (int i = 0; i < 11; ++i) dba->insert_vertex().add_label(lab1);
  for (int i = 0; i < 17; ++i) dba->insert_vertex().add_label(lab2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->vertices_count(lab1), 11);
  EXPECT_EQ(dba->vertices_count(lab2), 17);
  EXPECT_EQ(dba->vertices_count(), 28);
}

TEST(GraphDbAccessor, EdgeCount) {
  Dbms dbms;
  auto dba = dbms.active();
  auto t1 = dba->edge_type("t1");
  auto t2 = dba->edge_type("t2");

  EXPECT_EQ(dba->edges_count(t1), 0);
  EXPECT_EQ(dba->edges_count(t2), 0);
  EXPECT_EQ(dba->edges_count(), 0);
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  for (int i = 0; i < 11; ++i) dba->insert_edge(v1, v2, t1);
  for (int i = 0; i < 17; ++i) dba->insert_edge(v1, v2, t2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->edges_count(t1), 11);
  EXPECT_EQ(dba->edges_count(t2), 17);
  EXPECT_EQ(dba->edges_count(), 28);
}

TEST(GraphDbAccessor, VisibilityAfterInsertion) {

  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto lab1 = dba->label("lab1");
  auto lab2 = dba->label("lab2");
  v1.add_label(lab1);
  auto type1 = dba->edge_type("type1");
  auto type2 = dba->edge_type("type2");
  dba->insert_edge(v1, v2, type1);

  EXPECT_EQ(Count(dba->vertices(lab1)), 0);
  EXPECT_EQ(Count(dba->vertices(lab1, true)), 1);
  EXPECT_EQ(Count(dba->vertices(lab2)), 0);
  EXPECT_EQ(Count(dba->vertices(lab2, true)), 0);
  EXPECT_EQ(Count(dba->edges(type1)), 0);
  EXPECT_EQ(Count(dba->edges(type1, true)), 1);
  EXPECT_EQ(Count(dba->edges(type2)), 0);
  EXPECT_EQ(Count(dba->edges(type2, true)), 0);

  dba->advance_command();

  EXPECT_EQ(Count(dba->vertices(lab1)), 1);
  EXPECT_EQ(Count(dba->vertices(lab1, true)), 1);
  EXPECT_EQ(Count(dba->vertices(lab2)), 0);
  EXPECT_EQ(Count(dba->vertices(lab2, true)), 0);
  EXPECT_EQ(Count(dba->edges(type1)), 1);
  EXPECT_EQ(Count(dba->edges(type1, true)), 1);
  EXPECT_EQ(Count(dba->edges(type2)), 0);
  EXPECT_EQ(Count(dba->edges(type2, true)), 0);
}

TEST(GraphDbAccessor, VisibilityAfterDeletion) {

  Dbms dbms;
  auto dba = dbms.active();
  auto lab = dba->label("lab");
  for (int i = 0; i < 5; ++i)
    dba->insert_vertex().add_label(lab);
  dba->advance_command();
  auto type = dba->edge_type("type");
  for (int j = 0; j < 3; ++j) {
    auto vertices_it = dba->vertices().begin();
    dba->insert_edge(*vertices_it++, *vertices_it, type);
  }
  dba->advance_command();

  EXPECT_EQ(Count(dba->vertices(lab)), 5);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 5);
  EXPECT_EQ(Count(dba->edges(type)), 3);
  EXPECT_EQ(Count(dba->edges(type, true)), 3);

  // delete two edges
  auto edges_it = dba->edges().begin();
  for (int k = 0; k < 2; ++k)
    dba->remove_edge(*edges_it++);
  EXPECT_EQ(Count(dba->edges(type)), 3);
  EXPECT_EQ(Count(dba->edges(type, true)), 1);
  dba->advance_command();
  EXPECT_EQ(Count(dba->edges(type)), 1);
  EXPECT_EQ(Count(dba->edges(type, true)), 1);

  // detach-delete 2 vertices
  auto vertices_it = dba->vertices().begin();
  for (int k = 0; k < 2; ++k)
    dba->detach_remove_vertex(*vertices_it++);
  EXPECT_EQ(Count(dba->vertices(lab)), 5);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 3);
  dba->advance_command();
  EXPECT_EQ(Count(dba->vertices(lab)), 3);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 3);
}
