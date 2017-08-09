#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "database/dbms.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/property_value.hpp"
#include "storage/vertex_accessor.hpp"

TEST(RecordAccessor, Properties) {
  Dbms dbms;
  auto dba = dbms.active();

  auto vertex = dba->InsertVertex();
  auto &properties = vertex.Properties();

  auto property = dba->Property("PropName");
  auto property_other = dba->Property("Other");
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
  auto dba = dbms.active();

  auto vertex = dba->InsertVertex();
  const auto &const_vertex_dba = vertex.db_accessor();
  EXPECT_EQ(dba.get(), &const_vertex_dba);
  auto &vertex_dba = vertex.db_accessor();
  EXPECT_EQ(dba.get(), &vertex_dba);
}

TEST(RecordAccessor, RecordEquality) {
  Dbms dbms;
  auto dba = dbms.active();

  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  EXPECT_EQ(v1, v1);
  EXPECT_NE(v1, v2);

  auto e1 = dba->InsertEdge(v1, v2, dba->EdgeType("type"));
  auto e2 = dba->InsertEdge(v1, v2, dba->EdgeType("type"));
  EXPECT_EQ(e1, e1);
  EXPECT_NE(e1, e2);
}

TEST(RecordAccessor, RecordLessThan) {
  Dbms dbms;
  auto dba = dbms.active();

  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  EXPECT_NE(v1, v2);
  EXPECT_TRUE(v1 < v2 || v2 < v1);
  EXPECT_FALSE(v1 < v1);
  EXPECT_FALSE(v2 < v2);
  auto e1 = dba->InsertEdge(v1, v2, dba->EdgeType("type"));
  auto e2 = dba->InsertEdge(v1, v2, dba->EdgeType("type"));
  EXPECT_NE(e1, e2);
  EXPECT_TRUE(e1 < e2 || e2 < e1);
  EXPECT_FALSE(e1 < e1);
  EXPECT_FALSE(e2 < e2);
}

TEST(RecordAccessor, SwitchOldAndSwitchNewMemberFunctionTest) {
  Dbms dbms;

  // test both Switches work on new record
  {
    auto dba = dbms.active();
    auto v1 = dba->InsertVertex();
    v1.SwitchOld();
    v1.SwitchNew();
    dba->Commit();
  }

  // test both Switches work on existing record
  {
    auto dba = dbms.active();
    auto v1 = *dba->Vertices(false).begin();
    v1.SwitchOld();
    v1.SwitchNew();
  }

  // ensure switch exposes the right data
  {
    auto dba = dbms.active();
    auto label = dba->Label("label");
    auto v1 = *dba->Vertices(false).begin();

    EXPECT_FALSE(v1.has_label(label));  // old record
    v1.add_label(label);                // modifying data does not switch to new
    EXPECT_FALSE(v1.has_label(label));  // old record
    v1.SwitchNew();
    EXPECT_TRUE(v1.has_label(label));
    v1.SwitchOld();
    EXPECT_FALSE(v1.has_label(label));
  }
}

TEST(RecordAccessor, Reconstruct) {
  Dbms dbms;
  auto label = dbms.active()->Label("label");

  {
    // we must operate on an old vertex
    // because otherwise we only have new
    // so create a vertex and commit it
    auto dba = dbms.active();
    dba->InsertVertex();
    dba->Commit();
  }

  // ensure we don't have label set
  auto dba = dbms.active();
  auto v1 = *dba->Vertices(false).begin();
  v1.SwitchNew();
  EXPECT_FALSE(v1.has_label(label));

  {
    // update the record through a different accessor
    auto v1_other_accessor = *dba->Vertices(false).begin();
    v1_other_accessor.add_label(label);
    EXPECT_FALSE(v1.has_label(label));
    v1_other_accessor.SwitchNew();
    EXPECT_TRUE(v1_other_accessor.has_label(label));
  }

  EXPECT_FALSE(v1.has_label(label));
  v1.Reconstruct();
  v1.SwitchNew();
  EXPECT_TRUE(v1.has_label(label));
}

TEST(RecordAccessor, VertexLabels) {
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->InsertVertex();
  auto &labels = v1.labels();

  EXPECT_EQ(v1.labels().size(), 0);

  GraphDbTypes::Label l1 = dba->Label("label1");
  GraphDbTypes::Label l2 = dba->Label("label2");

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
  GraphDbTypes::Label l3 = dba->Label("label3");
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
  auto dba = dbms.active();
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();

  GraphDbTypes::EdgeType likes = dba->EdgeType("likes");
  GraphDbTypes::EdgeType hates = dba->EdgeType("hates");

  auto edge = dba->InsertEdge(v1, v2, likes);
  EXPECT_EQ(edge.EdgeType(), likes);
  EXPECT_NE(edge.EdgeType(), hates);
}

TEST(RecordAccessor, EdgeIsCycle) {
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto likes = dba->EdgeType("edge_type");

  EXPECT_TRUE(dba->InsertEdge(v1, v1, likes).is_cycle());
  EXPECT_TRUE(dba->InsertEdge(v2, v2, likes).is_cycle());
  EXPECT_FALSE(dba->InsertEdge(v1, v2, likes).is_cycle());
  EXPECT_FALSE(dba->InsertEdge(v2, v1, likes).is_cycle());
}

TEST(RecordAccessor, VertexEdgeConnections) {
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge = dba->InsertEdge(v1, v2, dba->EdgeType("likes"));
  dba->AdvanceCommand();

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
