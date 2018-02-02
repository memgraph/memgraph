#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"

TEST(StateDelta, CreateVertex) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::CreateVertex(dba.transaction_id(), gid0);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.FindVertex(gid0, false);
    EXPECT_TRUE(vertex);
  }
}

TEST(StateDelta, RemoveVertex) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::RemoveVertex(dba.transaction_id(), gid0);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.FindVertex(gid0, false);
    EXPECT_FALSE(vertex);
  }
}

TEST(StateDelta, CreateEdge) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    dba.InsertVertex(gid0);
    dba.InsertVertex(gid1);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::CreateEdge(
        dba.transaction_id(), gid2, gid0, gid1, dba.EdgeType("edge"), "edge");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto edge = dba.FindEdge(gid2, false);
    EXPECT_TRUE(edge);
  }
}

TEST(StateDelta, RemoveEdge) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    auto v0 = dba.InsertVertex(gid0);
    auto v1 = dba.InsertVertex(gid1);
    dba.InsertEdge(v0, v1, dba.EdgeType("edge"), gid2);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::RemoveEdge(dba.transaction_id(), gid2);
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto edge = dba.FindEdge(gid2, false);
    EXPECT_FALSE(edge);
  }
}

TEST(StateDelta, AddLabel) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::AddLabel(dba.transaction_id(), gid0,
                                                dba.Label("label"), "label");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.FindVertex(gid0, false);
    EXPECT_TRUE(vertex);
    auto labels = vertex->labels();
    EXPECT_EQ(labels.size(), 1);
    EXPECT_EQ(labels[0], dba.Label("label"));
  }
}

TEST(StateDelta, RemoveLabel) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.InsertVertex(gid0);
    vertex.add_label(dba.Label("label"));
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::RemoveLabel(dba.transaction_id(), gid0,
                                                   dba.Label("label"), "label");
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.FindVertex(gid0, false);
    EXPECT_TRUE(vertex);
    auto labels = vertex->labels();
    EXPECT_EQ(labels.size(), 0);
  }
}

TEST(StateDelta, SetPropertyVertex) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    dba.InsertVertex(gid0);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::PropsSetVertex(
        dba.transaction_id(), gid0, dba.Property("property"), "property",
        PropertyValue(2212));
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.FindVertex(gid0, false);
    EXPECT_TRUE(vertex);
    auto prop = vertex->PropsAt(dba.Property("property"));
    EXPECT_EQ(prop.Value<int64_t>(), 2212);
  }
}

TEST(StateDelta, SetPropertyEdge) {
  database::SingleNode db;
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    database::GraphDbAccessor dba(db);
    auto v0 = dba.InsertVertex(gid0);
    auto v1 = dba.InsertVertex(gid1);
    dba.InsertEdge(v0, v1, dba.EdgeType("edge"), gid2);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto delta = database::StateDelta::PropsSetEdge(
        dba.transaction_id(), gid2, dba.Property("property"), "property",
        PropertyValue(2212));
    delta.Apply(dba);
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba(db);
    auto edge = dba.FindEdge(gid2, false);
    EXPECT_TRUE(edge);
    auto prop = edge->PropsAt(dba.Property("property"));
    EXPECT_EQ(prop.Value<int64_t>(), 2212);
  }
}
