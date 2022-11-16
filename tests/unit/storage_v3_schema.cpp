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
#include <string>
#include <vector>

#include "common/types.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/temporal.hpp"

using testing::Pair;
using testing::UnorderedElementsAre;
using SchemaType = memgraph::common::SchemaType;

namespace memgraph::storage::v3::tests {

class SchemaTest : public testing::Test {
 private:
  NameIdMapper id_mapper_{{{1, "label1"}, {2, "label2"}, {3, "prop1"}, {4, "prop2"}}};

 protected:
  LabelId NameToLabel(const std::string &name) { return LabelId::FromUint(id_mapper_.NameToId(name)); }

  PropertyId NameToProperty(const std::string &name) { return PropertyId::FromUint(id_mapper_.NameToId(name)); }

  PropertyId prop1{NameToProperty("prop1")};
  PropertyId prop2{NameToProperty("prop2")};
  LabelId label1{NameToLabel("label1")};
  LabelId label2{NameToLabel("label2")};
  SchemaProperty schema_prop_string{prop1, SchemaType::STRING};
  SchemaProperty schema_prop_int{prop2, SchemaType::INT};
};

TEST_F(SchemaTest, TestSchemaCreate) {
  Schemas schemas;
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop_string}));
  EXPECT_EQ(schemas.ListSchemas().size(), 1);

  {
    EXPECT_TRUE(schemas.CreateSchema(label2, {schema_prop_string, schema_prop_int}));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(Pair(label1, std::vector<SchemaProperty>{schema_prop_string}),
                                     Pair(label2, std::vector<SchemaProperty>{schema_prop_string, schema_prop_int})));
  }
  {
    // Assert after unsuccessful creation, number oif schemas remains the same
    EXPECT_FALSE(schemas.CreateSchema(label2, {schema_prop_int}));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(Pair(label1, std::vector<SchemaProperty>{schema_prop_string}),
                                     Pair(label2, std::vector<SchemaProperty>{schema_prop_string, schema_prop_int})));
  }
}

TEST_F(SchemaTest, TestSchemaList) {
  Schemas schemas;

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop_string}));
  EXPECT_TRUE(schemas.CreateSchema(label2, {{NameToProperty("prop1"), SchemaType::STRING},
                                            {NameToProperty("prop2"), SchemaType::INT},
                                            {NameToProperty("prop3"), SchemaType::BOOL},
                                            {NameToProperty("prop4"), SchemaType::DATE},
                                            {NameToProperty("prop5"), SchemaType::LOCALDATETIME},
                                            {NameToProperty("prop6"), SchemaType::DURATION},
                                            {NameToProperty("prop7"), SchemaType::LOCALTIME}}));
  {
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 2);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(
                    Pair(label1, std::vector<SchemaProperty>{schema_prop_string}),
                    Pair(label2, std::vector<SchemaProperty>{{NameToProperty("prop1"), SchemaType::STRING},
                                                             {NameToProperty("prop2"), SchemaType::INT},
                                                             {NameToProperty("prop3"), SchemaType::BOOL},
                                                             {NameToProperty("prop4"), SchemaType::DATE},
                                                             {NameToProperty("prop5"), SchemaType::LOCALDATETIME},
                                                             {NameToProperty("prop6"), SchemaType::DURATION},
                                                             {NameToProperty("prop7"), SchemaType::LOCALTIME}})));
  }
  {
    const auto *const schema1 = schemas.GetSchema(label1);
    ASSERT_NE(schema1, nullptr);
    EXPECT_EQ(*schema1, (Schemas::Schema{label1, std::vector<SchemaProperty>{schema_prop_string}}));
  }
  {
    const auto *const schema2 = schemas.GetSchema(label2);
    ASSERT_NE(schema2, nullptr);
    EXPECT_EQ(schema2->first, label2);
    EXPECT_EQ(schema2->second.size(), 7);
  }
}

TEST_F(SchemaTest, TestSchemaDrop) {
  Schemas schemas;
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop_string}));
  EXPECT_EQ(schemas.ListSchemas().size(), 1);

  EXPECT_TRUE(schemas.DropSchema(label1));
  EXPECT_EQ(schemas.ListSchemas().size(), 0);

  EXPECT_TRUE(schemas.CreateSchema(label1, {schema_prop_string}));
  EXPECT_TRUE(schemas.CreateSchema(label2, {schema_prop_string, schema_prop_int}));
  EXPECT_EQ(schemas.ListSchemas().size(), 2);

  {
    EXPECT_TRUE(schemas.DropSchema(label1));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 1);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(Pair(label2, std::vector<SchemaProperty>{schema_prop_string, schema_prop_int})));
  }

  {
    // Cannot drop nonexisting schema
    EXPECT_FALSE(schemas.DropSchema(label1));
    const auto current_schemas = schemas.ListSchemas();
    EXPECT_EQ(current_schemas.size(), 1);
    EXPECT_THAT(current_schemas,
                UnorderedElementsAre(Pair(label2, std::vector<SchemaProperty>{schema_prop_string, schema_prop_int})));
  }

  EXPECT_TRUE(schemas.DropSchema(label2));
  EXPECT_EQ(schemas.ListSchemas().size(), 0);
}

class SchemaValidatorTest : public testing::Test {
 private:
  NameIdMapper id_mapper_{{{1, "label1"},
                           {2, "label2"},
                           {3, "prop1"},
                           {4, "prop2"},
                           {5, "prop3"},
                           {6, "label4"},
                           {7, "label5"},
                           {8, "label6"},
                           {9, "test"}}};

 protected:
  void SetUp() override {
    ASSERT_TRUE(schemas.CreateSchema(label1, {schema_prop_string}));
    ASSERT_TRUE(schemas.CreateSchema(label2, {schema_prop_string, schema_prop_int, schema_prop_duration}));
  }

  LabelId NameToLabel(const std::string &name) { return LabelId::FromUint(id_mapper_.NameToId(name)); }

  PropertyId NameToProperty(const std::string &name) { return PropertyId::FromUint(id_mapper_.NameToId(name)); }

  Schemas schemas;
  SchemaValidator schema_validator{schemas, id_mapper_};
  PropertyId prop_string{NameToProperty("prop1")};
  PropertyId prop_int{NameToProperty("prop2")};
  PropertyId prop_duration{NameToProperty("prop3")};
  LabelId label1{NameToLabel("label1")};
  LabelId label2{NameToLabel("label2")};
  SchemaProperty schema_prop_string{prop_string, SchemaType::STRING};
  SchemaProperty schema_prop_int{prop_int, SchemaType::INT};
  SchemaProperty schema_prop_duration{prop_duration, SchemaType::DURATION};
};

TEST_F(SchemaValidatorTest, TestSchemaValidateVertexCreate) {
  // Validate against secondary label
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(NameToLabel("test"), {}, {PropertyValue(1)});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_NO_SCHEMA_DEFINED_FOR_LABEL));
  }

  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label2, {}, {});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_PRIMARY_PROPERTIES_UNDEFINED));
  }
  // Validate wrong secondary label
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label1, {label1}, {PropertyValue("test")});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY));
  }
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label1, {label2}, {PropertyValue("test")});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_SECONDARY_LABEL_IS_PRIMARY));
  }
  // Validate wrong property type
  {
    const auto schema_violation = schema_validator.ValidateVertexCreate(label1, {}, {PropertyValue(1)});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_PROPERTY_WRONG_TYPE));
  }
  {
    const auto schema_violation =
        schema_validator.ValidateVertexCreate(label2, {}, {PropertyValue("test"), PropertyValue(12), PropertyValue(1)});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_PROPERTY_WRONG_TYPE));
  }
  {
    const auto wrong_prop = PropertyValue(TemporalData(TemporalType::Date, 1234));
    const auto schema_violation =
        schema_validator.ValidateVertexCreate(label2, {}, {PropertyValue("test"), PropertyValue(12), wrong_prop});
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_PROPERTY_WRONG_TYPE));
  }
  // Passing validations
  EXPECT_EQ(schema_validator.ValidateVertexCreate(label1, {}, {PropertyValue("test")}), std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexCreate(label1, {NameToLabel("label3"), NameToLabel("label4")},
                                                  {PropertyValue("test")}),
            std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexCreate(
                label2, {},
                {PropertyValue("test"), PropertyValue(122), PropertyValue(TemporalData(TemporalType::Duration, 1234))}),
            std::nullopt);
  EXPECT_EQ(schema_validator.ValidateVertexCreate(label2, {NameToLabel("label5"), NameToLabel("label6")},
                                                  {PropertyValue("test123"), PropertyValue(122221),
                                                   PropertyValue(TemporalData(TemporalType::Duration, 12344321))}),
            std::nullopt);
}

TEST_F(SchemaValidatorTest, TestSchemaValidatePropertyUpdate) {
  // Validate updating of primary key
  {
    const auto schema_violation = schema_validator.ValidatePropertyUpdate(label1, prop_string);
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_KEY));
  }
  {
    const auto schema_violation = schema_validator.ValidatePropertyUpdate(label2, prop_duration);
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_KEY));
  }
  EXPECT_EQ(schema_validator.ValidatePropertyUpdate(label1, prop_int), std::nullopt);
  EXPECT_EQ(schema_validator.ValidatePropertyUpdate(label1, prop_duration), std::nullopt);
  EXPECT_EQ(schema_validator.ValidatePropertyUpdate(label2, NameToProperty("test")), std::nullopt);
}

TEST_F(SchemaValidatorTest, TestSchemaValidatePropertyUpdateLabel) {
  // Validate adding primary label
  {
    const auto schema_violation = schema_validator.ValidateLabelUpdate(label1);
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL));
  }
  {
    const auto schema_violation = schema_validator.ValidateLabelUpdate(label2);
    ASSERT_NE(schema_violation, std::nullopt);
    EXPECT_EQ(*schema_violation, SHARD_ERROR(ErrorCode::SCHEMA_VERTEX_UPDATE_PRIMARY_LABEL));
  }
  EXPECT_EQ(schema_validator.ValidateLabelUpdate(NameToLabel("test")), std::nullopt);
}
}  // namespace memgraph::storage::v3::tests
