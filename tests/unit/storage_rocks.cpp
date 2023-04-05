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
#include "storage/rocks/storage.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

class RocksDBStorageTest : public ::testing::TestWithParam<bool> {
 public:
  memgraph::storage::rocks::RocksDBStorage db;
  ~RocksDBStorageTest() { db.Clear(); }
};

TEST_F(RocksDBStorageTest, SerializeVertexGID) {
  memgraph::storage::Storage storage;
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

// TODO: clean labels string
TEST_F(RocksDBStorageTest, SerializeVertexGIDLabels) {
  memgraph::storage::Storage storage;
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // save vertices on disk
  std::vector<std::string> labels{"Player", "Person", "Ball"};
  std::unordered_set<uint64_t> gids;
  std::vector<memgraph::storage::LabelId> label_ids;
  for (int i = 0; i < 3; i++) {
    label_ids.push_back(dba.NameToLabel(labels[i]));
  }
  for (int i = 0; i < 5; ++i) {
    gids.insert(i);
    auto impl = dba.InsertVertex();
    impl.SetGid(memgraph::storage::Gid::FromUint(i));
    impl.AddLabel(dba.NameToLabel(labels[i % 3]));
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

// TODO: clean labels string
TEST_F(RocksDBStorageTest, SerializeVertexGIDMutlipleLabels) {
  memgraph::storage::Storage storage;
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // save vertices on disk
  std::vector<std::string> labels{"Player", "Person", "Ball"};
  std::unordered_set<uint64_t> gids;
  std::vector<memgraph::storage::LabelId> label_ids;
  for (int i = 0; i < 3; i++) {
    label_ids.push_back(dba.NameToLabel(labels[i]));
  }
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

TEST_F(RocksDBStorageTest, SerializeVertexGIDProperties) {
  memgraph::storage::Storage storage;
  auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  memgraph::query::DbAccessor dba(&storage_dba);
  // prepare labels
  std::vector<std::string> labels{"Player", "Person", "Ball"};
  std::vector<memgraph::storage::LabelId> label_ids;
  for (int i = 0; i < 3; i++) {
    label_ids.push_back(dba.NameToLabel(labels[i]));
  }
  // prepare properties
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> properties;
  properties.emplace(dba.NameToProperty("name"), memgraph::storage::PropertyValue("disk"));
  properties.emplace(dba.NameToProperty("memory"), memgraph::storage::PropertyValue("1TB"));
  properties.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(1000.21));
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
    {
      auto labels = vertex_acc.Labels(memgraph::storage::View::OLD);
      ASSERT_EQ(labels->size(), 2);
      ASSERT_TRUE(std::all_of(labels->begin(), labels->end(), [&label_ids](const auto &label_id) {
        return std::find(label_ids.begin(), label_ids.end(), label_id) != label_ids.end();
      }));
    }
    {
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
      // TODO: needs to be solved
      ASSERT_TRUE(prop_price->IsDouble());
      ASSERT_DOUBLE_EQ(prop_price->ValueDouble(), 1000.21);
      ASSERT_TRUE(prop_unexisting->IsNull());
    }
  }
}

// TEST_F(RocksDBStorageTest, SerializeEdgeNewTest) {
//   memgraph::storage::Storage storage;
//   auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
//   memgraph::query::DbAccessor dba(&storage_dba);
//   std::vector<std::string> labels{"Player", "Person", "Ball"};
//   std::unordered_set<uint64_t> gids;
//   std::vector<memgraph::storage::LabelId> label_ids;
//   for (int i = 0; i < 3; i++) {
//     label_ids.push_back(dba.NameToLabel(labels[i]));
//   }
//   for (int i = 0; i < 5; ++i) {
//     gids.insert(i);
//     auto impl = dba.InsertVertex();
//     impl.SetGid(memgraph::storage::Gid::FromUint(i));
//     impl.AddLabel(label_ids[i % 3]);
//     impl.AddLabel(label_ids[(i + 1) % 3]);
//     db.StoreVertex(impl);
//   }
//   spdlog::info("Finished...");
//   ASSERT_TRUE(true);
// memgraph::storage::Storage storage;
// auto storage_dba = storage.Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
// memgraph::query::DbAccessor dba(&storage_dba);
// prepare labels
// std::vector<std::string> labels{"Player", "Person"};
// prepare vertex properties
// std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> vertex_properties;
// vertex_properties.emplace(dba.NameToProperty("name"), memgraph::storage::PropertyValue("disk"));
// vertex_properties.emplace(dba.NameToProperty("memory"), memgraph::storage::PropertyValue("1TB"));
// vertex_properties.emplace(dba.NameToProperty("price"), memgraph::storage::PropertyValue(1000.21));
// create first vertex
// uint64_t src_id = 0;
// auto impl_src = dba.InsertVertex();
// impl_src.SetGid(memgraph::storage::Gid::FromUint(src_id));
// impl_src.AddLabel(dba.NameToLabel(labels[src_id]));
// memgraph::query::MultiPropsInitChecked(&impl_src, vertex_properties);
// db.StoreVertex(impl_src);
// create second vertex
// uint64_t dest_id = 1;
// auto impl_dest = dba.InsertVertex();
// impl_dest.SetGid(memgraph::storage::Gid::FromUint(dest_id));
// impl_dest.AddLabel(dba.NameToLabel(labels[dest_id]));
// memgraph::query::MultiPropsInitChecked(&impl_dest, vertex_properties);
// db.StoreVertex(impl_dest);
// // prepare edge properties
// std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> edge_properties;
// edge_properties.emplace(dba.NameToProperty("sum"), memgraph::storage::PropertyValue("2TB"));
// edge_properties.emplace(dba.NameToProperty("same_type"), memgraph::storage::PropertyValue(true));
// edge_properties.emplace(dba.NameToProperty("cluster_price"), memgraph::storage::PropertyValue(2000.42));
// create an edge between them
// uint64_t edge_gid = 2;
// auto edge_type_id = "CONNECTION";
// auto impl_edge = dba.InsertEdge(&impl_src, &impl_dest, dba.NameToEdgeType(edge_type_id));
// ASSERT_FALSE(impl_edge.HasError());
// (*impl_edge).SetGid(memgraph::storage::Gid::FromUint(edge_gid));
// memgraph::query::MultiPropsInitChecked(&*impl_edge, edge_properties);
// // load vertices from disk
// auto loaded_vertices = db.Vertices(dba);
// ASSERT_EQ(loaded_vertices.size(), 2);
// ASSERT_FALSE(true);
// }
