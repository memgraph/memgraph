#include <vector>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/property_value.hpp"
#include "storage/vertex_accessor.hpp"

TEST(RecordAccessor, Properties) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto vertex = dba.insert_vertex();
  auto& properties = vertex.Properties();

  auto property = dba.property("PropName");
  auto property_other = dba.property("Other");
  EXPECT_EQ(vertex.PropsAt(property).type(), PropertyValue::Type::Null);

  vertex.PropsSet(property, 42);
  EXPECT_EQ(vertex.PropsAt(property).Value<int64_t>(), 42);
  EXPECT_EQ(properties.at(property).Value<int64_t>(), 42);
  EXPECT_EQ(vertex.PropsAt(property_other).type(), PropertyValue::Type::Null);
  EXPECT_EQ(properties.at(property_other).type(), PropertyValue::Type::Null);

  vertex.PropsErase(property);
  EXPECT_EQ(vertex.PropsAt(property).type(), PropertyValue::Type::Null);
  EXPECT_EQ(properties.at(property).type(), PropertyValue::Type::Null);
}

TEST(RecordAccessor, DbAccessor) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto vertex = dba.insert_vertex();
  const auto& const_vertex_dba = vertex.db_accessor();
  EXPECT_EQ(&dba, &const_vertex_dba);
  auto& vertex_dba = vertex.db_accessor();
  EXPECT_EQ(&dba, &vertex_dba);
}

TEST(RecordAccessor, RecordEquality) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto v1 = dba.insert_vertex();
  auto v2 = dba.insert_vertex();
  EXPECT_EQ(v1, v1);
  EXPECT_NE(v1, v2);

  auto e1 = dba.insert_edge(v1, v2, dba.edge_type("type"));
  auto e2 = dba.insert_edge(v1, v2, dba.edge_type("type"));
  EXPECT_EQ(e1, e1);
  EXPECT_NE(e1, e2);
}

TEST(RecordAccessor, RecordLessThan) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();

  auto v1 = dba.insert_vertex();
  auto v2 = dba.insert_vertex();
  EXPECT_NE(v1, v2);
  EXPECT_TRUE(v1 < v2 || v2 < v1);
  EXPECT_FALSE(v1 < v1);
  EXPECT_FALSE(v2 < v2);
  auto e1 = dba.insert_edge(v1, v2, dba.edge_type("type"));
  auto e2 = dba.insert_edge(v1, v2, dba.edge_type("type"));
  EXPECT_NE(e1, e2);
  EXPECT_TRUE(e1 < e2 || e2 < e1);
  EXPECT_FALSE(e1 < e1);
  EXPECT_FALSE(e2 < e2);
}

TEST(RecordAccessor, VertexLabels) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();
  auto v1 = dba.insert_vertex();
  auto& labels = v1.labels();

  EXPECT_EQ(v1.labels().size(), 0);

  GraphDb::Label l1 = dba.label("label1");
  GraphDb::Label l2 = dba.label("label2");

  // adding labels
  EXPECT_FALSE(v1.has_label(l1));
  EXPECT_TRUE(v1.add_label(l1));
  EXPECT_TRUE(v1.has_label(l1));

  EXPECT_EQ(v1.labels().size(), 1);
  EXPECT_EQ(labels.size(), 1);
  EXPECT_FALSE(v1.add_label(l1));
  EXPECT_EQ(v1.labels().size(), 1);
  EXPECT_EQ(labels.size(), 1);

  EXPECT_FALSE(v1.has_label(l2));
  EXPECT_TRUE(v1.add_label(l2));
  EXPECT_TRUE(v1.has_label(l2));
  EXPECT_EQ(v1.labels().size(), 2);
  EXPECT_EQ(labels.size(), 2);

  // removing labels
  GraphDb::Label l3 = dba.label("label3");
  EXPECT_EQ(v1.remove_label(l3), 0);
  EXPECT_EQ(labels.size(), 2);

  EXPECT_EQ(v1.remove_label(l1), 1);
  EXPECT_FALSE(v1.has_label(l1));
  EXPECT_EQ(v1.labels().size(), 1);

  EXPECT_EQ(v1.remove_label(l1), 0);
  EXPECT_TRUE(v1.has_label(l2));
}

TEST(RecordAccessor, EdgeType) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();
  auto v1 = dba.insert_vertex();
  auto v2 = dba.insert_vertex();

  GraphDb::EdgeType likes = dba.edge_type("likes");
  GraphDb::EdgeType hates = dba.edge_type("hates");

  auto edge = dba.insert_edge(v1, v2, likes);
  EXPECT_EQ(edge.edge_type(), likes);
  EXPECT_NE(edge.edge_type(), hates);

  edge.set_edge_type(hates);
  EXPECT_EQ(edge.edge_type(), hates);
  EXPECT_NE(edge.edge_type(), likes);
}

TEST(RecordAccessor, VertexEdgeConnections) {
  Dbms dbms;
  GraphDbAccessor dba = dbms.active();
  auto v1 = dba.insert_vertex();
  auto v2 = dba.insert_vertex();
  auto edge = dba.insert_edge(v1, v2, dba.edge_type("likes"));

  EXPECT_EQ(edge.from(), v1);
  EXPECT_NE(edge.from(), v2);
  EXPECT_EQ(edge.to(), v2);
  EXPECT_NE(edge.to(), v1);

  EXPECT_EQ(v1.in_degree(), 0);
  EXPECT_EQ(v1.out_degree(), 1);
  EXPECT_EQ(v2.in_degree(), 1);
  EXPECT_EQ(v2.out_degree(), 0);

  for (auto e : v1.out()) EXPECT_EQ(edge, e);

  for (auto e : v2.in()) EXPECT_EQ(edge, e);
}
