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

#include <gtest/gtest.h>
#include <cstdint>

#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_id.hpp"

namespace memgraph::storage::v3::tests {

class ShardSplitTest : public testing::Test {
 protected:
  void SetUp() override { storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "edge_property"}}); }

  const PropertyId primary_property{PropertyId::FromUint(2)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const EdgeTypeId edge_type_id{EdgeTypeId::FromUint(3)};
  Shard storage{primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector};

  coordinator::Hlc last_hlc{0, io::Time{}};

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(1);
    return last_hlc;
  }
};

TEST_F(ShardSplitTest, TestBasicSplitWithVertices) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(5)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());
  acc.Commit(GetNextHlc());
  storage.CollectGarbage(GetNextHlc().coordinator_wall_clock);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)});
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 0);
  EXPECT_EQ(splitted_data.transactions.size(), 0);
}

TEST_F(ShardSplitTest, TestBasicSplitVerticesAndEdges) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(5)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());

  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(2)}}, edge_type_id, Gid::FromUint(0))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(5)}}, edge_type_id, Gid::FromUint(1))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(4)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(6)}}, edge_type_id, Gid::FromUint(2))
                   .HasError());

  acc.Commit(GetNextHlc());
  storage.CollectGarbage(GetNextHlc().coordinator_wall_clock);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)});
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 2);
  EXPECT_EQ(splitted_data.transactions.size(), 0);
}

}  // namespace memgraph::storage::v3::tests
