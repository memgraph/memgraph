// Copyright 2022 Memgraph Ltd.
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

#include <fmt/format.h>

#include "storage/v2/schema_validator.hpp"
#include "storage/v2/schemas.hpp"
#include "storage/v2/storage.hpp"

using namespace memgraph::storage;

using testing::UnorderedElementsAre;

class SchemaTest : public testing::Test {
 protected:
  SchemaTest()
      : prop1{storage.NameToProperty("prop1")},
        prop2{storage.NameToProperty("prop2")},
        label1{storage.NameToLabel("label1")},
        label2{storage.NameToLabel("label2")},
        schema_prop1{prop1, memgraph::common::SchemaType::STRING},
        schema_prop2{prop2, memgraph::common::SchemaType::INT} {}

  memgraph::storage::Storage storage;
  memgraph::storage::PropertyId prop1;
  memgraph::storage::PropertyId prop2;
  memgraph::storage::LabelId label1;
  memgraph::storage::LabelId label2;
  memgraph::storage::SchemaProperty schema_prop1;
  memgraph::storage::SchemaProperty schema_prop2;
};

TEST_F(SchemaTest, TestSchemaCreate) {
  Schemas schemas;
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop1}));
  EXPECT_EQ(schemas.ListSchemas().size(), 1);

  {
    EXPECT_TRUE(schemas.CreateSchema(label2, {schema_prop1, schema_prop2}));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    testing::Pair(label1, std::vector<memgraph::storage::SchemaProperty>{schema_prop1}),
                    testing::Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }
  {
    // Assert after unsuccessful creation, number oif schemas remains the same
    EXPECT_FALSE(schemas.CreateSchema(label2, {schema_prop2}));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    testing::Pair(label1, std::vector<memgraph::storage::SchemaProperty>{schema_prop1}),
                    testing::Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }
}

TEST_F(SchemaTest, TestSchemaDrop) {
  Schemas schemas;
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop1}));
  EXPECT_EQ(schemas.ListSchemas().size(), 1);

  EXPECT_TRUE(schemas.DropSchema(label1));
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop1}));
  EXPECT_TRUE(schemas.CreateSchema(label2, {schema_prop1, schema_prop2}));
  EXPECT_EQ(schemas.ListSchemas().size(), 2);

  {
    EXPECT_TRUE(schemas.DropSchema(label1));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 1);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    testing::Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }

  {
    // Cannot drop nonexisting schema
    EXPECT_FALSE(schemas.DropSchema(label1));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 1);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    testing::Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }

  EXPECT_TRUE(schemas.DropSchema(label2));
  EXPECT_EQ(schemas.ListSchemas().size(), 0);
}
