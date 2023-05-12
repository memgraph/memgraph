// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <exception>
#include <unordered_set>

#include "query/common.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

class RocksDBStorageTest : public ::testing::TestWithParam<bool> {
 public:
  ~RocksDBStorageTest() { db.Clear(); }

 protected:
  memgraph::storage::rocks::RocksDBStorage db;
  memgraph::storage::Storage storage;
};

TEST_F(RocksDBStorageTest, SerializeVertexGID) {
  // empty vertices, only gid is serialized
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  std::unordered_set<uint64_t> gids;
  for (uint64_t i = 0; i < 5; ++i) {
    gids.insert(i);
    auto impl = dba.InsertVertex();
    impl.SetGid(memgraph::storage::Gid::FromUint(i));
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto loaded_vertices = db.Vertices(dba);
  ASSERT_EQ(loaded_vertices.size(), 5);
  for (const auto &vertex_acc : loaded_vertices) {
    ASSERT_TRUE(gids.contains(vertex_acc.Gid().AsUint()));
  }
}

TEST_F(RocksDBStorageTest, SerializeVertexGIDLabels) {
  // serialize vertex's gid with its single label
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // save vertices on disk
  std::unordered_set<uint64_t> gids;
  std::vector<memgraph::storage::LabelId> label_ids{dba.NameToLabel("Player"), dba.NameToLabel("Person"),
                                                    dba.NameToLabel("Ball")};
  for (int i = 0; i < 5; ++i) {
    gids.insert(i);
    auto impl = dba.InsertVertex();
    impl.SetGid(memgraph::storage::Gid::FromUint(i));
    impl.AddLabel(label_ids[i % 3]);
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto loaded_vertices = db.Vertices(dba);
  ASSERT_EQ(loaded_vertices.size(), 5);
  for (const auto &vertex_acc : loaded_vertices) {
    ASSERT_TRUE(gids.contains(vertex_acc.Gid().AsUint()));
    auto labels = vertex_acc.Labels(memgraph::storage::View::OLD);
    ASSERT_EQ(labels->size(), 1);
    ASSERT_TRUE(std::all_of(labels->begin(), labels->end(), [&label_ids](const auto &label_id) {
      return std::find(label_ids.begin(), label_ids.end(), label_id) != label_ids.end();
    }));
  }
}

TEST_F(RocksDBStorageTest, SerializeVertexGIDMutlipleLabels) {
  // serialize vertex's gid with multiple labels it contains
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // save vertices on disk
  std::unordered_set<uint64_t> gids;
  std::vector<memgraph::storage::LabelId> label_ids{dba.NameToLabel("Player"), dba.NameToLabel("Person"),
                                                    dba.NameToLabel("Ball")};
  for (int i = 0; i < 5; ++i) {
    gids.insert(i);
    auto impl = dba.InsertVertex();
    impl.SetGid(memgraph::storage::Gid::FromUint(i));
    impl.AddLabel(label_ids[i % 3]);
    impl.AddLabel(label_ids[(i + 1) % 3]);
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto loaded_vertices = db.Vertices(dba);
  ASSERT_EQ(loaded_vertices.size(), 5);
  for (const auto &vertex_acc : loaded_vertices) {
    ASSERT_TRUE(gids.contains(vertex_acc.Gid().AsUint()));
    auto labels = vertex_acc.Labels(memgraph::storage::View::OLD);
    ASSERT_EQ(labels->size(), 2);
    ASSERT_TRUE(std::all_of(labels->begin(), labels->end(), [&label_ids](const auto &label_id) {
      return std::find(label_ids.begin(), label_ids.end(), label_id) != label_ids.end();
    }));
  }
}

TEST_F(RocksDBStorageTest, GetVerticesByLabel) {
  // search vertices by label
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // prepare labels
  std::vector<memgraph::storage::LabelId> label_ids{dba.NameToLabel("Player"), dba.NameToLabel("Player"),
                                                    dba.NameToLabel("Ball")};
  // insert vertices
  for (int i = 0; i < 5; ++i) {
    auto impl = dba.InsertVertex();
    impl.AddLabel(label_ids[i % 3]);
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto player_vertices = db.Vertices(dba, dba.NameToLabel("Player"));
  auto ball_vertices = db.Vertices(dba, dba.NameToLabel("Ball"));
  ASSERT_EQ(player_vertices.size(), 4);
  ASSERT_EQ(ball_vertices.size(), 1);
}

TEST_F(RocksDBStorageTest, GetVerticesByProperty) {
  // search vertices by property value
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // prepare ssd properties
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> ssd_properties_1;
  ssd_properties_1.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(225.84));
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> ssd_properties_2;
  ssd_properties_2.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(226.84));
  // prepare hdd properties
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> hdd_properties_1;
  hdd_properties_1.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(125.84));
  std::vector properties{ssd_properties_1, ssd_properties_2, hdd_properties_1, hdd_properties_1};
  // insert vertices
  for (int i = 0; i < 4; ++i) {
    auto impl = dba.InsertVertex();
    memgraph::query::MultiPropsInitChecked(&impl, properties[i]);
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto ssd_vertices_1 = db.Vertices(dba, dba.NameToProperty("price"), memgraph::storage::PropertyValue(225.84));
  auto hdd_vertices = db.Vertices(dba, dba.NameToProperty("price"), memgraph::storage::PropertyValue(125.84));
  auto hdd_vertices_non_existing =
      db.Vertices(dba, dba.NameToProperty("price"), memgraph::storage::PropertyValue(125.81));
  ASSERT_EQ(ssd_vertices_1.size(), 1);
  ASSERT_EQ(hdd_vertices.size(), 2);
  ASSERT_EQ(hdd_vertices_non_existing.size(), 0);
}

TEST_F(RocksDBStorageTest, DeleteVertex) {
  // auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTE);
  auto storage_dba = storage.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties;
  // samo 1 property stane
  properties.emplace(dba.NameToProperty("sum"), memgraph::storage::PropertyValue("2TB"));
  properties.emplace(dba.NameToProperty("same_type"), memgraph::storage::PropertyValue(true));
  // properties.emplace(dba.NameToProperty("cluster_price"), memgraph::storage::PropertyValue(2000.42));
  // create vertex
  auto impl = dba.InsertVertex();
  impl.AddLabel(dba.NameToLabel("Player"));
  memgraph::query::MultiPropsInitChecked(&impl, properties);
  db.StoreVertex(impl);
  // find vertex should work now
  ASSERT_TRUE(db.FindVertex(std::to_string(impl.Gid().AsUint()), dba).has_value());
  db.FindVertex(std::to_string(impl.Gid().AsUint()), dba);
  // RocksDB doesn't physically delete entry so deletion will pass two times
  ASSERT_TRUE(db.DeleteVertex(impl).has_value());
  ASSERT_TRUE(db.DeleteVertex(impl).has_value());
  // second time you shouldn't be able to find the vertex
  ASSERT_FALSE(db.FindVertex(std::to_string(impl.Gid().AsUint()), dba).has_value());
}

TEST_F(RocksDBStorageTest, SerializeVertexGIDProperties) {
  // serializes vertex's gid, multiple labels and properties
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // prepare labels
  std::vector<memgraph::storage::LabelId> label_ids{dba.NameToLabel("Player"), dba.NameToLabel("Person"),
                                                    dba.NameToLabel("Ball")};
  // prepare properties
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties;
  properties.emplace(dba.NameToProperty("name"), memgraph::storage::PropertyValue("disk"));
  properties.emplace(dba.NameToProperty("memory"), memgraph::storage::PropertyValue("1TB"));
  properties.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(1000.21));
  properties.emplace(dba.NameToProperty("price2"), memgraph::storage::PropertyValue(1000.212));
  // gids
  std::unordered_set<uint64_t> gids;
  for (int i = 0; i < 5; ++i) {
    gids.insert(i);
    auto impl = dba.InsertVertex();
    impl.SetGid(memgraph::storage::Gid::FromUint(i));
    impl.AddLabel(label_ids[i % 3]);
    impl.AddLabel(label_ids[(i + 1) % 3]);
    memgraph::query::MultiPropsInitChecked(&impl, properties);
    db.StoreVertex(impl);
  }
  // load vertices from disk
  auto loaded_vertices = db.Vertices(dba);
  ASSERT_EQ(loaded_vertices.size(), 5);
  for (const auto &vertex_acc : loaded_vertices) {
    ASSERT_TRUE(gids.contains(vertex_acc.Gid().AsUint()));
    // labels
    auto labels = vertex_acc.Labels(memgraph::storage::View::OLD);
    ASSERT_EQ(labels->size(), 2);
    ASSERT_TRUE(std::all_of(labels->begin(), labels->end(), [&label_ids](const auto &label_id) {
      return std::find(label_ids.begin(), label_ids.end(), label_id) != label_ids.end();
    }));
    // check properties
    auto props = vertex_acc.Properties(memgraph::storage::View::OLD);
    ASSERT_FALSE(props.HasError());
    auto prop_name = vertex_acc.GetProperty(memgraph::storage::View::OLD, dba.NameToProperty("name"));
    auto prop_memory = vertex_acc.GetProperty(memgraph::storage::View::OLD, dba.NameToProperty("memory"));
    auto prop_price = vertex_acc.GetProperty(memgraph::storage::View::OLD, dba.NameToProperty("price"));
    auto prop_unexisting = vertex_acc.GetProperty(memgraph::storage::View::OLD, dba.NameToProperty("random"));
    ASSERT_TRUE(prop_name->IsString());
    ASSERT_EQ(prop_name->ValueString(), "disk");
    ASSERT_TRUE(prop_memory->IsString());
    ASSERT_EQ(prop_memory->ValueString(), "1TB");
    ASSERT_TRUE(prop_price->IsDouble());
    ASSERT_DOUBLE_EQ(prop_price->ValueDouble(), 1000.21);
    ASSERT_TRUE(prop_unexisting->IsNull());
  }
}

TEST_F(RocksDBStorageTest, SerializeEdge) {
  // create two vertices and edge between them
  // search by one of the vertices, return edge
  // check deserialization for both vertices and edge
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  std::vector<memgraph::storage::LabelId> label_ids{dba.NameToLabel("Player"), dba.NameToLabel("Referee")};
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties_1;
  properties_1.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(221.84));
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties_2;
  properties_2.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(222.84));
  std::vector properties{properties_1, properties_2};
  for (int i = 0; i < 2; ++i) {
    auto impl = dba.InsertVertex();
    impl.AddLabel(label_ids[i]);
    memgraph::query::MultiPropsInitChecked(&impl, properties[i]);
    db.StoreVertex(impl);
  }
  // prepare edge properties
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> edge_properties;
  edge_properties.emplace(dba.NameToProperty("sum"), memgraph::storage::PropertyValue("2TB"));
  edge_properties.emplace(dba.NameToProperty("same_type"), memgraph::storage::PropertyValue(true));
  edge_properties.emplace(dba.NameToProperty("cluster_price"), memgraph::storage::PropertyValue(2000.42));
  // Before inserting edge, find two vertices
  // find source vertex by the property
  auto src_vertices = db.Vertices(dba, dba.NameToProperty("price"), memgraph::storage::PropertyValue(221.84));
  ASSERT_EQ(src_vertices.size(), 1);
  auto src_vertex = src_vertices[0];
  // find destination vertex by the property
  auto dest_vertices = db.Vertices(dba, dba.NameToProperty("price"), memgraph::storage::PropertyValue(222.84));
  ASSERT_EQ(dest_vertices.size(), 1);
  auto dest_vertex = dest_vertices[0];
  // insert the edge
  uint64_t edge_gid = 2;
  auto edge_type_id = "CONNECTION";
  auto impl_edge = dba.InsertEdge(&src_vertex, &dest_vertex, dba.NameToEdgeType(edge_type_id));
  ASSERT_FALSE(impl_edge.HasError());
  (*impl_edge).SetGid(memgraph::storage::Gid::FromUint(edge_gid));
  memgraph::query::MultiPropsInitChecked(&*impl_edge, edge_properties);
  db.StoreEdge(*impl_edge);
  // Test out edges of the source vertex
  auto src_out_edges = db.OutEdges(src_vertex, dba);
  ASSERT_EQ(src_out_edges.size(), 1);
  auto src_out_edge = src_out_edges[0];
  // test from edge accessor
  auto from_out_edge_acc = src_out_edge.From();
  ASSERT_EQ(from_out_edge_acc.Gid(), src_vertex.Gid());
  ASSERT_EQ(from_out_edge_acc.Labels(memgraph::storage::View::OLD)->size(), 1);
  ASSERT_EQ(from_out_edge_acc.Labels(memgraph::storage::View::OLD)->at(0), label_ids[0]);
  ASSERT_EQ(*from_out_edge_acc.Properties(memgraph::storage::View::OLD), properties_1);
  // test to edge accessor
  auto to_out_edge_acc = src_out_edge.To();
  ASSERT_EQ(to_out_edge_acc.Gid(), dest_vertex.Gid());
  ASSERT_EQ(to_out_edge_acc.Labels(memgraph::storage::View::OLD)->size(), 1);
  ASSERT_EQ(to_out_edge_acc.Labels(memgraph::storage::View::OLD)->at(0), label_ids[1]);
  ASSERT_EQ(*to_out_edge_acc.Properties(memgraph::storage::View::OLD), properties_2);
  // test edge accessor
  ASSERT_EQ(src_out_edge.Gid().AsUint(), edge_gid);
  ASSERT_EQ(src_out_edge.EdgeType(), dba.NameToEdgeType(edge_type_id));
  ASSERT_EQ(*src_out_edge.Properties(memgraph::storage::View::OLD), edge_properties);
  // Test in edge of the destination vertex
  auto dest_in_edges = db.InEdges(dest_vertex, dba);
  ASSERT_EQ(dest_in_edges.size(), 1);
  auto dest_in_edge = dest_in_edges[0];
  // test from edge accessor
  auto from_in_edge_acc = dest_in_edge.From();
  ASSERT_EQ(from_in_edge_acc.Gid(), from_out_edge_acc.Gid());
  ASSERT_EQ(from_in_edge_acc.Labels(memgraph::storage::View::OLD)->size(), 1);
  ASSERT_EQ(from_in_edge_acc.Labels(memgraph::storage::View::OLD)->at(0),
            from_out_edge_acc.Labels(memgraph::storage::View::OLD)->at(0));
  ASSERT_EQ(*from_in_edge_acc.Properties(memgraph::storage::View::OLD),
            *from_out_edge_acc.Properties(memgraph::storage::View::OLD));
  // test in edge accessors
  auto to_in_edge_acc = dest_in_edge.To();
  ASSERT_EQ(to_in_edge_acc.Gid(), to_out_edge_acc.Gid());
  ASSERT_EQ(to_in_edge_acc.Labels(memgraph::storage::View::OLD)->size(), 1);
  ASSERT_EQ(to_in_edge_acc.Labels(memgraph::storage::View::OLD)->at(0),
            to_out_edge_acc.Labels(memgraph::storage::View::OLD)->at(0));
  ASSERT_EQ(*to_in_edge_acc.Properties(memgraph::storage::View::OLD),
            *to_out_edge_acc.Properties(memgraph::storage::View::OLD));
  // test edge accessors
  ASSERT_EQ(dest_in_edge.Gid(), src_out_edge.Gid());
  ASSERT_EQ(dest_in_edge.EdgeType(), src_out_edge.EdgeType());
  ASSERT_EQ(*dest_in_edge.Properties(memgraph::storage::View::OLD),
            *src_out_edge.Properties(memgraph::storage::View::OLD));
}
