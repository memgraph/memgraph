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

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fmt/format.h>
#include <optional>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_validator.hpp"
#include "storage/v2/schemas.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"

using namespace memgraph::storage;

using testing::Pair;
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
    EXPECT_THAT(
        current_schemas,
        UnorderedElementsAre(Pair(label1, std::vector<memgraph::storage::SchemaProperty>{schema_prop1}),
                             Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }
  {
    // Assert after unsuccessful creation, number oif schemas remains the same
    EXPECT_FALSE(schemas.CreateSchema(label2, {schema_prop2}));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(
        current_schemas,
        UnorderedElementsAre(Pair(label1, std::vector<memgraph::storage::SchemaProperty>{schema_prop1}),
                             Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }
}

TEST_F(SchemaTest, TestSchemaList) {
  Schemas schemas;

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop1}));
  EXPECT_TRUE(
      schemas.CreateSchema(label2, {{storage.NameToProperty("prop1"), memgraph::common::SchemaType::STRING},
                                    {storage.NameToProperty("prop2"), memgraph::common::SchemaType::INT},
                                    {storage.NameToProperty("prop3"), memgraph::common::SchemaType::BOOL},
                                    {storage.NameToProperty("prop4"), memgraph::common::SchemaType::DATE},
                                    {storage.NameToProperty("prop5"), memgraph::common::SchemaType::LOCALDATETIME},
                                    {storage.NameToProperty("prop6"), memgraph::common::SchemaType::DURATION},
                                    {storage.NameToProperty("prop7"), memgraph::common::SchemaType::LOCALTIME}}));
  {
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    Pair(label1, std::vector<memgraph::storage::SchemaProperty>{schema_prop1}),
                    Pair(label2, std::vector<memgraph::storage::SchemaProperty>{
                                     {storage.NameToProperty("prop1"), memgraph::common::SchemaType::STRING},
                                     {storage.NameToProperty("prop2"), memgraph::common::SchemaType::INT},
                                     {storage.NameToProperty("prop3"), memgraph::common::SchemaType::BOOL},
                                     {storage.NameToProperty("prop4"), memgraph::common::SchemaType::DATE},
                                     {storage.NameToProperty("prop5"), memgraph::common::SchemaType::LOCALDATETIME},
                                     {storage.NameToProperty("prop6"), memgraph::common::SchemaType::DURATION},
                                     {storage.NameToProperty("prop7"), memgraph::common::SchemaType::LOCALTIME}})));
  }
  {
    const auto schema1 = schemas.GetSchema(label1);
    EXPECT_NE(schema1, std::nullopt);
    EXPECT_EQ(*schema1, (Schemas::Schema{label1, std::vector<SchemaProperty>{schema_prop1}}));
  }
  {
    const auto schema2 = schemas.GetSchema(label2);
    EXPECT_NE(schema2, std::nullopt);
    EXPECT_EQ(schema2->first, label2);
    EXPECT_EQ(schema2->second.size(), 7);
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
    EXPECT_THAT(
        current_schemas,
        UnorderedElementsAre(Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }

  {
    // Cannot drop nonexisting schema
    EXPECT_FALSE(schemas.DropSchema(label1));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 1);
    EXPECT_THAT(
        current_schemas,
        UnorderedElementsAre(Pair(label2, std::vector<memgraph::storage::SchemaProperty>{schema_prop1, schema_prop2})));
  }

  EXPECT_TRUE(schemas.DropSchema(label2));
  EXPECT_EQ(schemas.ListSchemas().size(), 0);
}

class SchemaValidatorTest : public testing::Test {
 protected:
  SchemaValidatorTest()
      : schema_validator{schemas},
        prop1{storage.NameToProperty("prop1")},
        prop2{storage.NameToProperty("prop2")},
        prop3{storage.NameToProperty("prop3")},
        label1{storage.NameToLabel("label1")},
        label2{storage.NameToLabel("label2")},
        schema_prop1{prop1, memgraph::common::SchemaType::STRING},
        schema_prop2{prop2, memgraph::common::SchemaType::INT},
        schema_prop3{prop3, memgraph::common::SchemaType::DURATION} {
    EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop1}));
    EXPECT_TRUE(schemas.CreateSchema(label2, {schema_prop1, schema_prop2, schema_prop3}));
  }

  memgraph::storage::Storage storage;
  Schemas schemas;
  SchemaValidator schema_validator;
  memgraph::storage::PropertyId prop1;
  memgraph::storage::PropertyId prop2;
  memgraph::storage::PropertyId prop3;
  memgraph::storage::LabelId label1;
  memgraph::storage::LabelId label2;
  memgraph::storage::SchemaProperty schema_prop1;
  memgraph::storage::SchemaProperty schema_prop2;
  memgraph::storage::SchemaProperty schema_prop3;
};

TEST_F(SchemaValidatorTest, TestSchemaValidateVertexCreate) {
  // Validate against secondary label
  {
    const auto schema_violation =
        schema_validator.ValidateVertexCreate(storage.NameToLabel("test"), {}, {{prop1, PropertyValue(1)}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SchemaViolation(SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL,
                                                 storage.NameToLabel("test")));
  }
  // Validate missing property
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label1, {}, {{prop2, PropertyValue(1)}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PROPERTY, label1, schema_prop1));
  }
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label2, {}, {});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PROPERTY, label2, schema_prop1));
  }
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label2, {}, {});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_HAS_NO_PROPERTY, label2, schema_prop1));
  }
  // Validate wrong secondary label
  {
    const auto schema_violation =
        schema_validator.ValidateVertexCreate(label1, {label1}, {{prop1, PropertyValue("test")}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, label1));
  }
  {
    const auto schema_violation =
        schema_validator.ValidateVertexCreate(label1, {label2}, {{prop1, PropertyValue("test")}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, label2));
  }
  // Validate wrong property type
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label1, {}, {{prop1, PropertyValue(1)}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, label1,
                                                 schema_prop1, PropertyValue(1)));
  }
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(
        label2, {}, {{prop1, PropertyValue("test")}, {prop2, PropertyValue(12)}, {prop3, PropertyValue(1)}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, label2,
                                                 schema_prop3, PropertyValue(1)));
  }
  {
    const auto wrong_prop = PropertyValue(TemporalData(TemporalType::Date, 1234));
    const auto schema_violation = schema_validator.ValidateVertexCreate(
        label2, {}, {{prop1, PropertyValue("test")}, {prop2, PropertyValue(12)}, {prop3, wrong_prop}});
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_PROPERTY_WRONG_TYPE, label2,
                                                 schema_prop3, wrong_prop));
  }
  // Passing validations
  EXPECT_EQ(schema_validator.ValidateVertexCreate(label1, {}, {{prop1, PropertyValue("test")}}), std::nullopt);
  EXPECT_EQ(
      schema_validator.ValidateVertexCreate(label1, {storage.NameToLabel("label3"), storage.NameToLabel("label4")},
                                            {{prop1, PropertyValue("test")}}),
      std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexCreate(label2, {},
                                                  {{prop1, PropertyValue("test")},
                                                   {prop2, PropertyValue(122)},
                                                   {prop3, PropertyValue(TemporalData(TemporalType::Duration, 1234))}}),
            std::nullopt);
  EXPECT_EQ(
      schema_validator.ValidateVertexCreate(label2, {storage.NameToLabel("label5"), storage.NameToLabel("label6")},
                                            {{prop1, PropertyValue("test123")},
                                             {prop2, PropertyValue(122221)},
                                             {prop3, PropertyValue(TemporalData(TemporalType::Duration, 12344321))}}),
      std::nullopt);
}

TEST_F(SchemaValidatorTest, TestSchemaValidateVertexUpdate) {
  // Validate against secondary label
  {
    const auto schema_violation = schema_validator.ValidateVertexUpdate(storage.NameToLabel("test"), prop1);
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SchemaViolation(SchemaViolation::ValidationStatus::NO_SCHEMA_DEFINED_FOR_LABEL,
                                                 storage.NameToLabel("test")));
  }
  // Validate updating of primary key
  {
    const auto schema_violation = schema_validator.ValidateVertexUpdate(label1, prop1);
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY, label1, schema_prop1));
  }
  {
    const auto schema_violation = schema_validator.ValidateVertexUpdate(label2, prop3);
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY, label2, schema_prop3));
  }
  EXPECT_EQ(schema_validator.ValidateVertexUpdate(label1, prop2), std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexUpdate(label1, prop3), std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexUpdate(label2, storage.NameToProperty("test")), std::nullopt);
}

TEST_F(SchemaValidatorTest, TestSchemaValidateVertexUpdateLabel) {
  // Validate adding primary label
  {
    const auto schema_violation = schema_validator.ValidateLabelUpdate(label1);
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_ALREADY_HAS_PRIMARY_LABEL, label1));
  }
  {
    const auto schema_violation = schema_validator.ValidateLabelUpdate(label2);
    EXPECT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation,
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_ALREADY_HAS_PRIMARY_LABEL, label2));
  }
  EXPECT_EQ(schema_validator.ValidateLabelUpdate(storage.NameToLabel("test")), std::nullopt);
}
