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
#include <optional>
#include <variant>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/types.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage_v3_test_utils.hpp"

using testing::UnorderedElementsAre;

namespace memgraph::storage::v3::tests {

class StorageV3Accessor : public ::testing::Test {
 protected:
  void SetUp() override { storage.StoreMapping({{1, "label"}, {2, "property"}}); }

  VertexAccessor CreateVertexAndValidate(Shard::Accessor &acc, LabelId primary_label,
                                         const std::vector<LabelId> &labels,
                                         const std::vector<std::pair<PropertyId, PropertyValue>> &properties) {
    auto vtx = acc.CreateVertexAndValidate(primary_label, labels, properties);
    EXPECT_TRUE(vtx.HasValue());
    return *vtx;
  }

  LabelId NameToLabelId(std::string_view label_name) { return storage.NameToLabel(label_name); }

  PropertyId NameToPropertyId(std::string_view property_name) { return storage.NameToProperty(property_name); }

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(10);
    return last_hlc;
  }

  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const PropertyId primary_property{PropertyId::FromUint(2)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  Shard storage{primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector};

  coordinator::Hlc last_hlc{0, io::Time{}};
};

TEST_F(StorageV3Accessor, TestPrimaryLabel) {
  {
    auto acc = storage.Access(GetNextHlc());
    const auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(0)}});
    ASSERT_TRUE(vertex.PrimaryLabel(View::NEW).HasValue());
    const auto vertex_primary_label = vertex.PrimaryLabel(View::NEW).GetValue();
    ASSERT_FALSE(vertex.PrimaryLabel(View::OLD).HasValue());
    EXPECT_EQ(vertex_primary_label, primary_label);
  }
  {
    auto acc = storage.Access(GetNextHlc());
    const auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(1)}});
    ASSERT_TRUE(vertex.PrimaryLabel(View::OLD).HasError());
    const auto error_primary_label = vertex.PrimaryLabel(View::OLD).GetError();
    ASSERT_FALSE(vertex.PrimaryLabel(View::NEW).HasError());
    EXPECT_EQ(error_primary_label, Error::NONEXISTENT_OBJECT);
  }
  {
    auto acc = storage.Access(GetNextHlc());
    CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(2)}});
    acc.Commit(GetNextHlc());
  }
  {
    auto acc = storage.Access(GetNextHlc());
    const auto vertex = acc.FindVertex({PropertyValue{2}}, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(acc.FindVertex({PropertyValue{2}}, View::NEW).has_value());
    ASSERT_TRUE(vertex->PrimaryLabel(View::NEW).HasValue());
    ASSERT_TRUE(vertex->PrimaryLabel(View::OLD).HasValue());
    const auto vertex_primary_label = vertex->PrimaryLabel(View::NEW).GetValue();
    EXPECT_EQ(vertex_primary_label, primary_label);
  }
}

TEST_F(StorageV3Accessor, TestAddLabels) {
  storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "label1"}, {4, "label2"}, {5, "label3"}});
  {
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label1");
    const auto label2 = NameToLabelId("label2");
    const auto label3 = NameToLabelId("label3");
    const auto vertex =
        CreateVertexAndValidate(acc, primary_label, {label1, label2, label3}, {{primary_property, PropertyValue(0)}});
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    ASSERT_FALSE(vertex.Labels(View::OLD).HasValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1, label2, label3));
  }
  {
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label1");
    const auto label2 = NameToLabelId("label2");
    const auto label3 = NameToLabelId("label3");
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
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label");
    auto vertex = acc.CreateVertexAndValidate(primary_label, {label1}, {{primary_property, PropertyValue(2)}});
    ASSERT_TRUE(vertex.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(vertex.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(vertex.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_SECONDARY_LABEL_IS_PRIMARY, label1));
  }
  {
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label");
    auto vertex = acc.CreateVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(3)}});
    ASSERT_TRUE(vertex.HasValue());
    const auto schema_violation = vertex->AddLabelAndValidate(label1);
    ASSERT_TRUE(schema_violation.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(schema_violation.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(schema_violation.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_LABEL, label1));
  }
}

TEST_F(StorageV3Accessor, TestRemoveLabels) {
  storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "label1"}, {4, "label2"}, {5, "label3"}});

  {
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label1");
    const auto label2 = NameToLabelId("label2");
    const auto label3 = NameToLabelId("label3");
    auto vertex =
        CreateVertexAndValidate(acc, primary_label, {label1, label2, label3}, {{primary_property, PropertyValue(0)}});
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1, label2, label3));
    const auto res1 = vertex.RemoveLabelAndValidate(label2);
    ASSERT_TRUE(res1.HasValue());
    EXPECT_TRUE(res1.GetValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label1, label3));
    const auto res2 = vertex.RemoveLabelAndValidate(label1);
    ASSERT_TRUE(res2.HasValue());
    EXPECT_TRUE(res2.GetValue());
    EXPECT_THAT(vertex.Labels(View::NEW).GetValue(), UnorderedElementsAre(label3));
    const auto res3 = vertex.RemoveLabelAndValidate(label3);
    ASSERT_TRUE(res3.HasValue());
    ASSERT_TRUE(res3.HasValue());
    EXPECT_TRUE(res3.GetValue());
    EXPECT_TRUE(vertex.Labels(View::NEW).GetValue().empty());
  }
  {
    auto acc = storage.Access(GetNextHlc());
    const auto label1 = NameToLabelId("label1");
    auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(1)}});
    ASSERT_TRUE(vertex.Labels(View::NEW).HasValue());
    EXPECT_TRUE(vertex.Labels(View::NEW).GetValue().empty());
    const auto res1 = vertex.RemoveLabelAndValidate(label1);
    ASSERT_TRUE(res1.HasValue());
    EXPECT_FALSE(res1.GetValue());
  }
  {
    auto acc = storage.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(2)}});
    const auto res1 = vertex.RemoveLabelAndValidate(primary_label);
    ASSERT_TRUE(res1.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(res1.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(res1.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_LABEL, primary_label));
  }
}

TEST_F(StorageV3Accessor, TestSetKeysAndProperties) {
  storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "prop1"}});
  storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "prop1"}});
  {
    auto acc = storage.Access(GetNextHlc());
    const PropertyId prop1{NameToPropertyId("prop1")};
    auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(0)}});
    const auto res = vertex.SetPropertyAndValidate(prop1, PropertyValue(1));
    ASSERT_TRUE(res.HasValue());
  }
  {
    auto acc = storage.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(1)}});
    const auto res = vertex.SetPropertyAndValidate(primary_property, PropertyValue(1));
    ASSERT_TRUE(res.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(res.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(res.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY, primary_label,
                              SchemaProperty{primary_property, common::SchemaType::INT}));
  }
  {
    auto acc = storage.Access(GetNextHlc());
    auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(2)}});
    const auto res = vertex.SetPropertyAndValidate(primary_property, PropertyValue());
    ASSERT_TRUE(res.HasError());
    ASSERT_TRUE(std::holds_alternative<SchemaViolation>(res.GetError()));
    EXPECT_EQ(std::get<SchemaViolation>(res.GetError()),
              SchemaViolation(SchemaViolation::ValidationStatus::VERTEX_UPDATE_PRIMARY_KEY, primary_label,
                              SchemaProperty{primary_property, common::SchemaType::INT}));
  }
}

}  // namespace memgraph::storage::v3::tests
