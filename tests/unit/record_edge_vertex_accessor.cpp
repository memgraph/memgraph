#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "mvcc/version_list.hpp"
#include "storage/address.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/property_value.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"

TEST(RecordAccessor, Properties) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);

  auto vertex = dba.InsertVertex();
  auto &properties = vertex.Properties();

  auto property = dba.Property("PropName");
  auto property_other = dba.Property("Other");
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
  database::SingleNode db;
  database::GraphDbAccessor dba(db);

  auto vertex = dba.InsertVertex();
  const auto &const_vertex_dba = vertex.db_accessor();
  EXPECT_EQ(&dba, &const_vertex_dba);
  auto &vertex_dba = vertex.db_accessor();
  EXPECT_EQ(&dba, &vertex_dba);
}

TEST(RecordAccessor, RecordEquality) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);

  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  EXPECT_EQ(v1, v1);
  EXPECT_NE(v1, v2);

  auto e1 = dba.InsertEdge(v1, v2, dba.EdgeType("type"));
  auto e2 = dba.InsertEdge(v1, v2, dba.EdgeType("type"));
  EXPECT_EQ(e1, e1);
  EXPECT_NE(e1, e2);
}

TEST(RecordAccessor, GlobalToLocalAddressConversion) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);

  auto v1 = dba.InsertVertex();
  storage::Address<mvcc::VersionList<Vertex>> global_address{v1.gid(),
                                                             db.WorkerId()};
  EXPECT_FALSE(global_address.is_local());
  auto v1_from_global = VertexAccessor(global_address, dba);
  EXPECT_TRUE(v1_from_global.address().is_local());
  EXPECT_EQ(v1_from_global.address(), v1.address());
}

TEST(RecordAccessor, SwitchOldAndSwitchNewMemberFunctionTest) {
  database::SingleNode db;

  // test both Switches work on new record
  {
    database::GraphDbAccessor dba(db);
    auto v1 = dba.InsertVertex();
    v1.SwitchOld();
    v1.SwitchNew();
    dba.Commit();
  }

  // test both Switches work on existing record
  {
    database::GraphDbAccessor dba(db);
    auto v1 = *dba.Vertices(false).begin();
    v1.SwitchOld();
    v1.SwitchNew();
  }

  // ensure switch exposes the right data
  {
    database::GraphDbAccessor dba(db);
    auto label = dba.Label("label");
    auto v1 = *dba.Vertices(false).begin();

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
  database::SingleNode db;
  auto label = database::GraphDbAccessor(db).Label("label");

  {
    // we must operate on an old vertex
    // because otherwise we only have new
    // so create a vertex and commit it
    database::GraphDbAccessor dba(db);
    dba.InsertVertex();
    dba.Commit();
  }

  // ensure we don't have label set
  database::GraphDbAccessor dba(db);
  auto v1 = *dba.Vertices(false).begin();
  v1.SwitchNew();
  EXPECT_FALSE(v1.has_label(label));

  {
    // update the record through a different accessor
    auto v1_other_accessor = *dba.Vertices(false).begin();
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
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto &labels = v1.labels();

  EXPECT_EQ(v1.labels().size(), 0);

  storage::Label l1 = dba.Label("label1");
  storage::Label l2 = dba.Label("label2");

  // adding labels
  EXPECT_FALSE(v1.has_label(l1));
  v1.add_label(l1);
  EXPECT_TRUE(v1.has_label(l1));

  EXPECT_EQ(v1.labels().size(), 1);
  EXPECT_EQ(labels.size(), 1);
  v1.add_label(l1);
  EXPECT_EQ(v1.labels().size(), 1);
  EXPECT_EQ(labels.size(), 1);

  EXPECT_FALSE(v1.has_label(l2));
  v1.add_label(l2);
  EXPECT_TRUE(v1.has_label(l2));
  EXPECT_EQ(v1.labels().size(), 2);
  EXPECT_EQ(labels.size(), 2);

  // removing labels
  storage::Label l3 = dba.Label("label3");
  v1.remove_label(l3);
  EXPECT_EQ(labels.size(), 2);

  v1.remove_label(l1);
  EXPECT_FALSE(v1.has_label(l1));
  EXPECT_EQ(v1.labels().size(), 1);

  v1.remove_label(l1);
  EXPECT_TRUE(v1.has_label(l2));
}

TEST(RecordAccessor, EdgeType) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();

  storage::EdgeType likes = dba.EdgeType("likes");
  storage::EdgeType hates = dba.EdgeType("hates");

  auto edge = dba.InsertEdge(v1, v2, likes);
  EXPECT_EQ(edge.EdgeType(), likes);
  EXPECT_NE(edge.EdgeType(), hates);
}

TEST(RecordAccessor, EdgeIsCycle) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto likes = dba.EdgeType("edge_type");

  EXPECT_TRUE(dba.InsertEdge(v1, v1, likes).is_cycle());
  EXPECT_TRUE(dba.InsertEdge(v2, v2, likes).is_cycle());
  EXPECT_FALSE(dba.InsertEdge(v1, v2, likes).is_cycle());
  EXPECT_FALSE(dba.InsertEdge(v2, v1, likes).is_cycle());
}

TEST(RecordAccessor, VertexEdgeConnections) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge = dba.InsertEdge(v1, v2, dba.EdgeType("likes"));
  dba.AdvanceCommand();

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

#define TEST_EDGE_ITERABLE(iterable, ...)                                     \
  {                                                                           \
    std::vector<EdgeAccessor> edge_accessors;                                 \
    auto expected_vec = std::vector<EdgeAccessor>(__VA_ARGS__);               \
    for (const auto &ea : iterable) edge_accessors.emplace_back(ea);          \
    ASSERT_EQ(edge_accessors.size(), expected_vec.size());                    \
    EXPECT_TRUE(std::is_permutation(                                          \
        edge_accessors.begin(), edge_accessors.end(), expected_vec.begin())); \
  }

TEST(RecordAccessor, VertexEdgeConnectionsWithExistingVertex) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("edge type");
  auto e12 = dba.InsertEdge(v1, v2, edge_type);
  auto e22 = dba.InsertEdge(v2, v2, edge_type);
  auto e23a = dba.InsertEdge(v2, v3, edge_type);
  auto e23b = dba.InsertEdge(v2, v3, edge_type);
  auto e32 = dba.InsertEdge(v3, v2, edge_type);
  dba.AdvanceCommand();

  TEST_EDGE_ITERABLE(v1.out(v1));
  TEST_EDGE_ITERABLE(v1.out(v2), {e12});
  TEST_EDGE_ITERABLE(v1.out(v3));
  TEST_EDGE_ITERABLE(v2.out(v1));
  TEST_EDGE_ITERABLE(v2.out(v2), {e22});
  TEST_EDGE_ITERABLE(v2.out(v3), {e23a, e23b});
  TEST_EDGE_ITERABLE(v3.out(v1));
  TEST_EDGE_ITERABLE(v3.out(v2), {e32});
  TEST_EDGE_ITERABLE(v3.out(v3));

  TEST_EDGE_ITERABLE(v1.in(v1));
  TEST_EDGE_ITERABLE(v1.in(v2));
  TEST_EDGE_ITERABLE(v1.in(v3));
  TEST_EDGE_ITERABLE(v2.in(v1), {e12});
  TEST_EDGE_ITERABLE(v2.in(v2), {e22});
  TEST_EDGE_ITERABLE(v2.in(v3), {e32});
  TEST_EDGE_ITERABLE(v3.in(v1));
  TEST_EDGE_ITERABLE(v3.in(v2), {e23a, e23b});
  TEST_EDGE_ITERABLE(v3.in(v3));
}

TEST(RecordAccessor, VertexEdgeConnectionsWithEdgeType) {
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto a = dba.EdgeType("a");
  auto b = dba.EdgeType("b");
  auto c = dba.EdgeType("c");
  auto ea = dba.InsertEdge(v1, v2, a);
  auto eb_1 = dba.InsertEdge(v2, v1, b);
  auto eb_2 = dba.InsertEdge(v2, v1, b);
  auto ec = dba.InsertEdge(v1, v2, c);
  dba.AdvanceCommand();

  TEST_EDGE_ITERABLE(v1.in(), {eb_1, eb_2});
  TEST_EDGE_ITERABLE(v2.in(), {ea, ec});

  std::vector<storage::EdgeType> edges_a{a};
  std::vector<storage::EdgeType> edges_b{b};
  std::vector<storage::EdgeType> edges_ac{a, c};
  TEST_EDGE_ITERABLE(v1.in(&edges_a));
  TEST_EDGE_ITERABLE(v1.in(&edges_b), {eb_1, eb_2});
  TEST_EDGE_ITERABLE(v1.out(&edges_a), {ea});
  TEST_EDGE_ITERABLE(v1.out(&edges_b));
  TEST_EDGE_ITERABLE(v1.out(&edges_ac), {ea, ec});
  TEST_EDGE_ITERABLE(v2.in(&edges_a), {ea});
  TEST_EDGE_ITERABLE(v2.in(&edges_b));
  TEST_EDGE_ITERABLE(v2.out(&edges_a));
  TEST_EDGE_ITERABLE(v2.out(&edges_b), {eb_1, eb_2});
}

#undef TEST_EDGE_ITERABLE
