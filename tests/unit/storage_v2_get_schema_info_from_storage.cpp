// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/inmemory/storage.hpp"
#include <gtest/gtest.h>
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

class GetSchemaInfoTest : public testing::Test {
 public:
  const std::string testSuite = "storage_v2__show_schema_info";

  GetSchemaInfoTest() { storage = std::make_unique<memgraph::storage::InMemoryStorage>(); }

  void TearDown() override { storage.reset(nullptr); }

  std::unique_ptr<Storage> storage;
};

TEST_F(GetSchemaInfoTest, CountOnOneCreateVertex) {
  auto acc = this->storage->Access();
  // CREATE (n)
  acc->CreateVertex();

  auto schemaInfo = acc->GetSchemaInfo();
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.size(), 1);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->first.empty());  // no labels
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.begin()->second.number_of_label_occurrences, 1);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->second.properties.empty());  // no properties
}

TEST_F(GetSchemaInfoTest, CountOnTwoCreateVertex) {
  auto acc = this->storage->Access();
  // CREATE (n)
  acc->CreateVertex();
  acc->CreateVertex();

  auto schemaInfo = acc->GetSchemaInfo();
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.size(), 1);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->first.empty());  // no labels
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.begin()->second.number_of_label_occurrences, 2);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->second.properties.empty());  // no properties
}

TEST_F(GetSchemaInfoTest, CountOnReplicationAccessorCreateVertexEx) {
  auto inmem_acc = std::unique_ptr<InMemoryStorage::InMemoryAccessor>(
      static_cast<InMemoryStorage::InMemoryAccessor *>(this->storage->Access().release()));
  auto acc = InMemoryStorage::ReplicationAccessor(std::move(*inmem_acc));
  // CREATE (n) in replication handler
  acc.CreateVertexEx(Gid::FromUint(0));
  acc.CreateVertexEx(Gid::FromUint(1));

  auto check_acc = this->storage->Access();
  auto schemaInfo = check_acc->GetSchemaInfo();
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.size(), 1);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->first.empty());  // no labels
  EXPECT_EQ(schemaInfo.nodes.node_types_properties.begin()->second.number_of_label_occurrences, 2);
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.begin()->second.properties.empty());  // no properties
}
