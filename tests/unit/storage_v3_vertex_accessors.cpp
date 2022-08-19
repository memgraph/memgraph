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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v3/delta.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
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
    const auto maybe_primary_label = vertex.PrimaryLabel(View::NEW);
    ASSERT_TRUE(maybe_primary_label.HasValue());
    EXPECT_EQ(maybe_primary_label.GetValue(), primary_label);
  }
  {
    auto acc = storage.Access();
    const auto vertex = CreateVertexAndValidate(acc, primary_label, {}, {{primary_property, PropertyValue(1)}});
    const auto maybe_primary_label = vertex.PrimaryLabel(View::OLD);
    ASSERT_TRUE(maybe_primary_label.HasError());
    EXPECT_EQ(maybe_primary_label.GetError(), Error::NONEXISTENT_OBJECT);
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
    const auto maybe_primary_label = vertex->PrimaryLabel(View::NEW);
    ASSERT_TRUE(maybe_primary_label.HasValue());
    EXPECT_EQ(maybe_primary_label.GetValue(), primary_label);
  }
}
}  // namespace memgraph::storage::v3::tests
