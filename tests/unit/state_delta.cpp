#include <gtest/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "durability/single_node/state_delta.hpp"

TEST(StateDelta, CreateVertex) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  {
    auto dba = db.Access();
    auto delta =
        database::StateDelta::CreateVertex(dba.transaction_id(), gid0);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto vertex = dba.FindVertexOptional(gid0, false);
    EXPECT_TRUE(vertex);
    EXPECT_EQ(vertex->CypherId(), 0);
  }
}

TEST(StateDelta, RemoveVertex) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  {
    auto dba = db.Access();
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta =
        database::StateDelta::RemoveVertex(dba.transaction_id(), gid0, true);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto vertex = dba.FindVertexOptional(gid0, false);
    EXPECT_FALSE(vertex);
  }
}

TEST(StateDelta, CreateEdge) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    auto dba = db.Access();
    dba.InsertVertex(gid0);
    dba.InsertVertex(gid1);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta =
        database::StateDelta::CreateEdge(dba.transaction_id(), gid2, gid0,
                                         gid1, dba.EdgeType("edge"), "edge");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto edge = dba.FindEdgeOptional(gid2, false);
    EXPECT_TRUE(edge);
  }
}

TEST(StateDelta, RemoveEdge) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    auto dba = db.Access();
    auto v0 = dba.InsertVertex(gid0);
    auto v1 = dba.InsertVertex(gid1);
    dba.InsertEdge(v0, v1, dba.EdgeType("edge"), gid2);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta = database::StateDelta::RemoveEdge(dba.transaction_id(), gid2);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto edge = dba.FindEdgeOptional(gid2, false);
    EXPECT_FALSE(edge);
  }
}

TEST(StateDelta, AddLabel) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  {
    auto dba = db.Access();
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta = database::StateDelta::AddLabel(dba.transaction_id(), gid0,
                                                dba.Label("label"), "label");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto vertex = dba.FindVertexOptional(gid0, false);
    EXPECT_TRUE(vertex);
    auto labels = vertex->labels();
    EXPECT_EQ(labels.size(), 1);
    EXPECT_EQ(labels[0], dba.Label("label"));
  }
}

TEST(StateDelta, RemoveLabel) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  {
    auto dba = db.Access();
    auto vertex = dba.InsertVertex(gid0);
    vertex.add_label(dba.Label("label"));
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta = database::StateDelta::RemoveLabel(
        dba.transaction_id(), gid0, dba.Label("label"), "label");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto vertex = dba.FindVertexOptional(gid0, false);
    EXPECT_TRUE(vertex);
    auto labels = vertex->labels();
    EXPECT_EQ(labels.size(), 0);
  }
}

TEST(StateDelta, SetPropertyVertex) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  {
    auto dba = db.Access();
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta = database::StateDelta::PropsSetVertex(
        dba.transaction_id(), gid0, dba.Property("property"), "property",
        PropertyValue(2212));
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto vertex = dba.FindVertexOptional(gid0, false);
    EXPECT_TRUE(vertex);
    auto prop = vertex->PropsAt(dba.Property("property"));
    EXPECT_EQ(prop.Value<int64_t>(), 2212);
  }
}

TEST(StateDelta, SetPropertyEdge) {
  database::GraphDb db;
  gid::Generator generator;
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    auto dba = db.Access();
    auto v0 = dba.InsertVertex(gid0);
    auto v1 = dba.InsertVertex(gid1);
    dba.InsertEdge(v0, v1, dba.EdgeType("edge"), gid2);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto delta = database::StateDelta::PropsSetEdge(
        dba.transaction_id(), gid2, dba.Property("property"), "property",
        PropertyValue(2212));
    delta.Apply(dba);
    dba.Commit();
  }
  {
    auto dba = db.Access();
    auto edge = dba.FindEdgeOptional(gid2, false);
    EXPECT_TRUE(edge);
    auto prop = edge->PropsAt(dba.Property("property"));
    EXPECT_EQ(prop.Value<int64_t>(), 2212);
  }
}
