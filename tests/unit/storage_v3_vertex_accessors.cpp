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

#include <limits>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v3/delta.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage_v3_test_utils.hpp"

using testing::UnorderedElementsAre;

namespace memgraph::storage::v3::tests {

class StorageV3Accessor : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(
        storage.CreateSchema(primary_label, {storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}}));
  }

  VertexAccessor CreateVertexAndValidate(Storage::Accessor &acc, LabelId primary_label,
                                         const std::vector<LabelId> &labels,
                                         const std::vector<std::pair<PropertyId, PropertyValue>> &properties) {
    auto vtx = acc.CreateVertexAndValidate(primary_label, labels, properties);
    EXPECT_TRUE(vtx.HasValue());
    return *vtx;
  }

  Storage storage;
  const LabelId primary_label{storage.NameToLabel("label")};
  const PropertyId primary_property{storage.NameToProperty("property")};
};

TEST_F(StorageV3Accessor, TestPrimaryLabel) {
  {
    auto acc = storage.Access();
    const auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(0)}});
    ASSERT_TRUE(vertex.PrimaryLabel(View::NEW).HasValue());
    const auto vertex_primary_label = vertex.PrimaryLabel(View::NEW).GetValue();
    ASSERT_FALSE(vertex.PrimaryLabel(View::OLD).HasValue());
    EXPECT_EQ(vertex_primary_label, primary_label);
  }
  {
    auto acc = storage.Access();
    const auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(1)}});
    ASSERT_TRUE(vertex.PrimaryLabel(View::OLD).HasError());
    const auto error_primary_label = vertex.PrimaryLabel(View::OLD).GetError();
    ASSERT_FALSE(vertex.PrimaryLabel(View::NEW).HasError());
    EXPECT_EQ(error_primary_label, Error::NONEXISTENT_OBJECT);
  }
  {
    auto acc = storage.Access();
    CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(2)}});
    ASSERT_FALSE(acc.Commit().HasError());
  }
  {
    auto acc = storage.Access();
    const auto vertex = acc.FindVertex(primary_label, {PropertyValue{2}}, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(acc.FindVertex(primary_label, {PropertyValue{2}}, View::NEW).has_value());
    ASSERT_TRUE(vertex->PrimaryLabel(View::NEW).HasValue());
    ASSERT_TRUE(vertex->PrimaryLabel(View::OLD).HasValue());
    const auto vertex_primary_label = vertex->PrimaryLabel(View::NEW).GetValue();
    EXPECT_EQ(vertex_primary_label, primary_label);
  }
}

TEST_F(StorageV3Accessor, TestAddLabels) {
  {
    auto acc = storage.Access();
    const auto label1 = storage.NameToLabel("label1");
    const auto label2 = storage.NameToLabel("label2");
    const auto label3 = storage.NameToLabel("label3");
    const auto vertex =
        CreateVertexAndValidate(acc, primary_label, {label1, label2, label3}, {{primary_property, PropertyValue(0)}});
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    ASSERT_FALSE(vertex.Labels(View::OLD).HasValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1, label2, label3));
  }
  {
    auto acc = storage.Access();
    const auto label1 = storage.NameToLabel("label1");
    const auto label2 = storage.NameToLabel("label2");
    const auto label3 = storage.NameToLabel("label3");
    auto vertex = CreateVertexAndValidate(acc, primary_label, {label1}, {{primary_property, PropertyValue(1)}});
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    ASSERT_FALSE(vertex.Labels(View::OLD).HasValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1));
    EXPECT_TRUE(vertex.AddLabelAndValidate(label2).HasValue());
    EXPECT_TRUE(vertex.AddLabelAndValidate(label3).HasValue());
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    ASSERT_FALSE(vertex.Labels(View::OLD).HasValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1, label2, label3));
  }
  {
    auto acc = storage.Access();
    const auto label1 = storage.NameToLabel("label");
    auto vertex = acc.CreateVertexAndValidate(primary_label, {label1}, {{primary_property, PropertyValue(2)}});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, label1));
  }
  {
    auto acc = storage.Access();
    const auto label1 = storage.NameToLabel("label");
    auto vertex = acc.CreateVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(3)}});
    ASSERT_TRUE(vertex.HasValue());
    const auto schema_violation = vertex->AddLabelAndValidate(label1);
    ASSERT_TRUE(schema_violation.HasError());
    const auto sasa = std::get<SchemaViolation>(schema_violation.GetError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(schema_violation.GetError()));
    EXPECT_EQ(sasa, SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, label1));
  }
}

}  // namespace memgraph::storage::v3::tests
