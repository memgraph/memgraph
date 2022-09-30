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
#include <gmock/gmock-more-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>

#include "storage/v3/id_types.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/temporal.hpp"
#include "storage/v3/view.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)

using testing::IsEmpty;
using testing::Pair;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

namespace memgraph::storage::v3::tests {

class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    storage.StoreMapping({{1, "label"}, {2, "property"}, {3, "label1"}, {4, "label2"}, {5, "id"}, {6, "val"}});
  }

  const PropertyId primary_property{PropertyId::FromUint(2)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  Shard storage{primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector};

  const PropertyId prop_id{PropertyId::FromUint(5)};
  const PropertyId prop_val{PropertyId::FromUint(6)};
  const LabelId label1{LabelId::FromUint(3)};
  const LabelId label2{LabelId::FromUint(4)};
  static constexpr std::chrono::seconds wall_clock_increment{10};
  static constexpr std::chrono::seconds reclamation_interval{wall_clock_increment / 2};
  static constexpr io::Duration one_time_unit{1};
  int primary_key_id{0};
  int vertex_id{0};
  coordinator::Hlc last_hlc{0, io::Time{}};

  LabelId NameToLabelId(std::string_view label_name) { return storage.NameToLabel(label_name); }

  PropertyId NameToPropertyId(std::string_view property_name) { return storage.NameToProperty(property_name); }

  VertexAccessor CreateVertex(Shard::Accessor &accessor) {
    auto vertex = *accessor.CreateVertexAndValidate(
        primary_label, {},
        {{primary_property, PropertyValue(primary_key_id++)}, {prop_id, PropertyValue(vertex_id++)}});
    return vertex;
  }

  template <class TIterable>
  std::vector<int64_t> GetIds(TIterable iterable, View view = View::OLD) {
    std::vector<int64_t> ret;
    for (auto vertex : iterable) {
      ret.push_back(vertex.GetProperty(prop_id, view)->ValueInt());
    }
    return ret;
  }

  template <class TIterable>
  std::vector<int64_t> GetPrimaryKeyIds(TIterable iterable, View view = View::OLD) {
    std::vector<int64_t> ret;
    for (auto vertex : iterable) {
      EXPECT_TRUE(vertex.PrimaryKey(view).HasValue());
      const auto pk = vertex.PrimaryKey(view).GetValue();
      EXPECT_EQ(pk.size(), 1);
      ret.push_back(pk[0].ValueInt());
    }
    return ret;
  }

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += wall_clock_increment;
    return last_hlc;
  }

  void CleanupHlc(const coordinator::Hlc hlc) {
    storage.CollectGarbage(hlc.coordinator_wall_clock + reclamation_interval + one_time_unit);
  }
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexCreate) {
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    }
    acc.Commit(GetNextHlc());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));

  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.Abort();
  }

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.Commit(GetNextHlc());
  }

  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.Commit(GetNextHlc());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexDrop) {
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    }
    acc.Commit(GetNextHlc());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));

  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  EXPECT_TRUE(storage.DropIndex(label1));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  EXPECT_FALSE(storage.DropIndex(label1));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    }
    acc.Commit(GetNextHlc());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(acc.LabelIndexExists(label1));
  }
  EXPECT_THAT(storage.ListAllIndices().label, UnorderedElementsAre(label1));

  {
    auto acc = storage.Access(GetNextHlc());

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 vertices numbered from 0 to 9.
  // 2. Add Label1 to odd numbered, and Label2 to even numbered vertices.
  // 3. Remove Label1 from odd numbered vertices, and add it to even numbered
  //    vertices.
  // 4. Delete even numbered vertices.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc = storage.Access(GetNextHlc());
  EXPECT_THAT(storage.ListAllIndices().label, UnorderedElementsAre(label1, label2));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = CreateVertex(acc);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  acc.AdvanceCommand();
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.RemoveLabelAndValidate(label1));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexDuplicateVersions) {
  // By removing labels and adding them again we create duplicate entries for
  // the same vertex in the index (they only differ by the timestamp). This test
  // checks that duplicates are properly filtered out.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 0; i < 5; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

    acc.Commit(GetNextHlc());
  }

  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.RemoveLabelAndValidate(label1));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    }
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexTransactionalIsolation) {
  // Check that transactions only see entries they are supposed to see.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc_before = storage.Access(GetNextHlc());
  auto acc = storage.Access(GetNextHlc());
  auto acc_after = storage.Access(GetNextHlc());

  for (int i = 0; i < 5; ++i) {
    auto vertex = CreateVertex(acc);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
  EXPECT_THAT(GetIds(acc_before.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, View::NEW), View::NEW), IsEmpty());

  acc.Commit(GetNextHlc());

  auto acc_after_commit = storage.Access(GetNextHlc());

  EXPECT_THAT(GetIds(acc_before.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after_commit.Vertices(label1, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexCountEstimate) {
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc = storage.Access(GetNextHlc());
  for (int i = 0; i < 20; ++i) {
    auto vertex = CreateVertex(acc);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 3 ? label1 : label2));
  }

  EXPECT_EQ(acc.ApproximateVertexCount(label1), 13);
  EXPECT_EQ(acc.ApproximateVertexCount(label2), 7);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexCreateAndDrop) {
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  EXPECT_TRUE(storage.CreateIndex(label1, prop_id));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(acc.LabelPropertyIndexExists(label1, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property, UnorderedElementsAre(std::make_pair(label1, prop_id)));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_FALSE(storage.CreateIndex(label1, prop_id));
  EXPECT_THAT(storage.ListAllIndices().label_property, UnorderedElementsAre(std::make_pair(label1, prop_id)));

  EXPECT_TRUE(storage.CreateIndex(label2, prop_id));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(label1, prop_id), std::make_pair(label2, prop_id)));

  EXPECT_TRUE(storage.DropIndex(label1, prop_id));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label1, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property, UnorderedElementsAre(std::make_pair(label2, prop_id)));
  EXPECT_FALSE(storage.DropIndex(label1, prop_id));

  EXPECT_TRUE(storage.DropIndex(label2, prop_id));
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
}

// The following three tests are almost an exact copy-paste of the corresponding
// label index tests. We request all vertices with given label and property from
// the index, without range filtering. Range filtering is tested in a separate
// test.

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexBasic) {
  storage.CreateIndex(label1, prop_val);
  storage.CreateIndex(label2, prop_val);

  auto acc = storage.Access(GetNextHlc());
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = CreateVertex(acc);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(i % 2 ? label1 : label2));
    ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue()));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW), IsEmpty());

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexDuplicateVersions) {
  storage.CreateIndex(label1, prop_val);
  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 0; i < 5; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(i)));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));

    acc.Commit(GetNextHlc());
  }

  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue()));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(42)));
    }
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexTransactionalIsolation) {
  storage.CreateIndex(label1, prop_val);

  auto acc_before = storage.Access(GetNextHlc());
  auto acc = storage.Access(GetNextHlc());
  auto acc_after = storage.Access(GetNextHlc());

  for (int i = 0; i < 5; ++i) {
    auto vertex = CreateVertex(acc);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0, 1, 2, 3, 4));
  EXPECT_THAT(GetIds(acc_before.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());

  acc.Commit(GetNextHlc());

  auto acc_after_commit = storage.Access(GetNextHlc());

  EXPECT_THAT(GetIds(acc_before.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc_after_commit.Vertices(label1, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexFiltering) {
  // We insert vertices with values:
  // 0 0.0 1 1.0 2 2.0 3 3.0 4 4.0
  // Then we check all combinations of inclusive and exclusive bounds.
  // We also have a mix of doubles and integers to verify that they are sorted
  // properly.

  storage.CreateIndex(label1, prop_val);

  {
    auto acc = storage.Access(GetNextHlc());

    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, i % 2 ? PropertyValue(i / 2) : PropertyValue(i / 2.0)));
    }
    acc.Commit(GetNextHlc());
  }
  {
    auto acc = storage.Access(GetNextHlc());
    for (int i = 0; i < 5; ++i) {
      EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, PropertyValue(i), View::OLD)),
                  UnorderedElementsAre(2 * i, 2 * i + 1));
    }

    // [1, +inf>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                    std::nullopt, View::OLD)),
                UnorderedElementsAre(2, 3, 4, 5, 6, 7, 8, 9));
    // <1, +inf>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                    std::nullopt, View::OLD)),
                UnorderedElementsAre(4, 5, 6, 7, 8, 9));

    // <-inf, 3]
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, std::nullopt,
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));
    // <-inf, 3>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, std::nullopt,
                                    memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5));

    // [1, 3]
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(2, 3, 4, 5, 6, 7));
    // <1, 3]
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                    memgraph::utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(4, 5, 6, 7));
    // [1, 3>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                    memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(2, 3, 4, 5));
    // <1, 3>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, memgraph::utils::MakeBoundExclusive(PropertyValue(1)),
                                    memgraph::utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
                UnorderedElementsAre(4, 5));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexCountEstimate) {
  storage.CreateIndex(label1, prop_val);

  auto acc = storage.Access(GetNextHlc());
  for (int i = 1; i <= 10; ++i) {
    for (int j = 0; j < i; ++j) {
      auto vertex = CreateVertex(acc);
      ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
      ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(i)));
    }
  }

  EXPECT_EQ(acc.ApproximateVertexCount(label1, prop_val), 55);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(acc.ApproximateVertexCount(label1, prop_val, PropertyValue(i)), i);
  }

  EXPECT_EQ(acc.ApproximateVertexCount(label1, prop_val, memgraph::utils::MakeBoundInclusive(PropertyValue(2)),
                                       memgraph::utils::MakeBoundInclusive(PropertyValue(6))),
            2 + 3 + 4 + 5 + 6);
}

TEST_F(IndexTest, LabelPropertyIndexMixedIteration) {
  storage.CreateIndex(label1, prop_val);

  const std::array temporals{TemporalData{TemporalType::Date, 23}, TemporalData{TemporalType::Date, 28},
                             TemporalData{TemporalType::LocalDateTime, 20}};

  std::vector<PropertyValue> values = {
      PropertyValue(false),
      PropertyValue(true),
      PropertyValue(-std::numeric_limits<double>::infinity()),
      PropertyValue(std::numeric_limits<int64_t>::min()),
      PropertyValue(-1),
      PropertyValue(-0.5),
      PropertyValue(0),
      PropertyValue(0.5),
      PropertyValue(1),
      PropertyValue(1.5),
      PropertyValue(2),
      PropertyValue(std::numeric_limits<int64_t>::max()),
      PropertyValue(std::numeric_limits<double>::infinity()),
      PropertyValue(""),
      PropertyValue("a"),
      PropertyValue("b"),
      PropertyValue("c"),
      PropertyValue(std::vector<PropertyValue>()),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(2)}),
      PropertyValue(std::map<std::string, PropertyValue>()),
      PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
      PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}}),
      PropertyValue(temporals[0]),
      PropertyValue(temporals[1]),
      PropertyValue(temporals[2]),
  };

  // Create vertices, each with one of the values above.
  {
    auto acc = storage.Access(GetNextHlc());
    for (const auto &value : values) {
      auto v = acc.CreateVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(primary_key_id++)}});
      ASSERT_TRUE(v->AddLabelAndValidate(label1).HasValue());
      ASSERT_TRUE(v->SetPropertyAndValidate(prop_val, value).HasValue());
    }
    acc.Commit(GetNextHlc());
  }

  // Verify that all nodes are in the index.
  {
    auto acc = storage.Access(GetNextHlc());
    auto iterable = acc.Vertices(label1, prop_val, View::OLD);
    auto it = iterable.begin();
    for (const auto &value : values) {
      ASSERT_NE(it, iterable.end());
      auto vertex = *it;
      auto maybe_value = vertex.GetProperty(prop_val, View::OLD);
      ASSERT_TRUE(maybe_value.HasValue());
      ASSERT_EQ(value, *maybe_value);
      ++it;
    }
    ASSERT_EQ(it, iterable.end());
  }

  auto verify = [&](const std::optional<memgraph::utils::Bound<PropertyValue>> &from,
                    const std::optional<memgraph::utils::Bound<PropertyValue>> &to,
                    const std::vector<PropertyValue> &expected) {
    auto acc = storage.Access(GetNextHlc());
    auto iterable = acc.Vertices(label1, prop_val, from, to, View::OLD);
    size_t i = 0;
    for (auto it = iterable.begin(); it != iterable.end(); ++it, ++i) {
      auto vertex = *it;
      auto maybe_value = vertex.GetProperty(prop_val, View::OLD);
      ASSERT_TRUE(maybe_value.HasValue());
      ASSERT_EQ(*maybe_value, expected[i]);
    }
    ASSERT_EQ(i, expected.size());
  };

  // Range iteration with two specified bounds that have the same type should
  // yield the naturally expected items.
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundInclusive(PropertyValue(true)), {PropertyValue(true)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {PropertyValue(false)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)),
         memgraph::utils::MakeBoundInclusive(PropertyValue(true)), {PropertyValue(false), PropertyValue(true)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(0)), memgraph::utils::MakeBoundExclusive(PropertyValue(1.8)),
         {PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(0)), memgraph::utils::MakeBoundInclusive(PropertyValue(1.8)),
         {PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(0)), memgraph::utils::MakeBoundExclusive(PropertyValue(1.8)),
         {PropertyValue(0), PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(0)), memgraph::utils::MakeBoundInclusive(PropertyValue(1.8)),
         {PropertyValue(0), PropertyValue(0.5), PropertyValue(1), PropertyValue(1.5)});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundExclusive(PropertyValue("memgraph")), {PropertyValue("c")});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundInclusive(PropertyValue("memgraph")), {PropertyValue("c")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundExclusive(PropertyValue("memgraph")), {PropertyValue("b"), PropertyValue("c")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")),
         memgraph::utils::MakeBoundInclusive(PropertyValue("memgraph")), {PropertyValue("b"), PropertyValue("c")});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})),
         memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue("b")})),
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5.0)}})),
         memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue("b")}})),
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});

  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(TemporalData{TemporalType::Date, 200})),
         // LocalDateTime has a "higher" type number so it is not part of the range
         {PropertyValue(temporals[1])});
  verify(memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[1]), PropertyValue(temporals[2])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundExclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1])});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[0])),
         memgraph::utils::MakeBoundInclusive(PropertyValue(temporals[2])),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});

  // Range iteration with one unspecified bound should only yield items that
  // have the same type as the specified bound.
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(false)), std::nullopt,
         {PropertyValue(false), PropertyValue(true)});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(true)), {PropertyValue(false)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(1)), std::nullopt,
         {PropertyValue(1), PropertyValue(1.5), PropertyValue(2), PropertyValue(std::numeric_limits<int64_t>::max()),
          PropertyValue(std::numeric_limits<double>::infinity())});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(0)),
         {PropertyValue(-std::numeric_limits<double>::infinity()), PropertyValue(std::numeric_limits<int64_t>::min()),
          PropertyValue(-1), PropertyValue(-0.5)});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue("b")), std::nullopt,
         {PropertyValue("b"), PropertyValue("c")});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue("b")),
         {PropertyValue(""), PropertyValue("a")});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(false)})),
         std::nullopt,
         {PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)}),
          PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(std::vector<PropertyValue>{PropertyValue(1)})),
         {PropertyValue(std::vector<PropertyValue>()), PropertyValue(std::vector<PropertyValue>{PropertyValue(0.8)})});
  verify(memgraph::utils::MakeBoundInclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(false)}})),
         std::nullopt,
         {PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}}),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(10)}})});
  verify(std::nullopt,
         memgraph::utils::MakeBoundExclusive(
             PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(7.5)}})),
         {PropertyValue(std::map<std::string, PropertyValue>()),
          PropertyValue(std::map<std::string, PropertyValue>{{"id", PropertyValue(5)}})});
  verify(memgraph::utils::MakeBoundInclusive(PropertyValue(TemporalData(TemporalType::Date, 10))), std::nullopt,
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});
  verify(std::nullopt, memgraph::utils::MakeBoundExclusive(PropertyValue(TemporalData(TemporalType::Duration, 0))),
         {PropertyValue(temporals[0]), PropertyValue(temporals[1]), PropertyValue(temporals[2])});

  // Range iteration with two specified bounds that don't have the same type
  // should yield no items.
  for (size_t i = 0; i < values.size(); ++i) {
    for (size_t j = i; j < values.size(); ++j) {
      if (PropertyValue::AreComparableTypes(values[i].type(), values[j].type())) {
        verify(memgraph::utils::MakeBoundInclusive(values[i]), memgraph::utils::MakeBoundInclusive(values[j]),
               {values.begin() + i, values.begin() + j + 1});
      } else {
        verify(memgraph::utils::MakeBoundInclusive(values[i]), memgraph::utils::MakeBoundInclusive(values[j]), {});
      }
    }
  }

  // Iteration without any bounds should return all items of the index.
  verify(std::nullopt, std::nullopt, values);
}

TEST_F(IndexTest, LabelPropertyIndexCreateWithExistingPrimaryKey) {
  // Create index on primary label and on primary key
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  EXPECT_FALSE(storage.CreateIndex(primary_label, primary_property));
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  // Create index on primary label and on secondary property
  EXPECT_TRUE(storage.CreateIndex(primary_label, prop_id));
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 1);
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(acc.LabelPropertyIndexExists(primary_label, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property, UnorderedElementsAre(Pair(primary_label, prop_id)));

  // Create index on primary label
  EXPECT_FALSE(storage.CreateIndex(primary_label));
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 1);

  // Create index on secondary label
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_EQ(storage.ListAllIndices().label.size(), 1);
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 1);
}

TEST_F(IndexTest, LabelIndexCreateVertexAndValidate) {
  {
    EXPECT_EQ(storage.ListAllIndices().label.size(), 0);
    EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  }
  {
    auto acc = storage.Access(GetNextHlc());

    // Create vertices with CreateVertexAndValidate
    for (int i = 0; i < 5; ++i) {
      auto vertex =
          acc.CreateVertexAndValidate(primary_label, {label1}, {{primary_property, PropertyValue(primary_key_id++)}});
      ASSERT_TRUE(vertex.HasValue());
    }
    acc.Commit(GetNextHlc());
  }
  {
    EXPECT_TRUE(storage.CreateIndex(label1));
    {
      auto acc = storage.Access(GetNextHlc());
      EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
    }
  }
  {
    EXPECT_TRUE(storage.DropIndex(label1));
    {
      auto acc = storage.Access(GetNextHlc());
      EXPECT_FALSE(acc.LabelIndexExists(label1));
    }
    EXPECT_EQ(storage.ListAllIndices().label.size(), 0);
  }
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(storage.CreateIndex(label1));
    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));

    for (int i = 0; i < 5; ++i) {
      auto vertex =
          acc.CreateVertexAndValidate(primary_label, {label1}, {{primary_property, PropertyValue(primary_key_id++)}});
      ASSERT_TRUE(vertex.HasValue());
    }

    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, View::OLD), View::OLD), UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

TEST_F(IndexTest, LabelPropertyIndexCreateVertexAndValidate) {
  {
    EXPECT_EQ(storage.ListAllIndices().label.size(), 0);
    EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  }
  {
    auto acc = storage.Access(GetNextHlc());

    // Create vertices with CreateVertexAndValidate
    for (int i = 0; i < 5; ++i) {
      auto vertex = acc.CreateVertexAndValidate(
          primary_label, {label1},
          {{primary_property, PropertyValue(primary_key_id++)}, {prop_id, PropertyValue(vertex_id++)}});
      ASSERT_TRUE(vertex.HasValue());
    }
    acc.Commit(GetNextHlc());
  }
  {
    EXPECT_TRUE(storage.CreateIndex(label1, prop_id));
    {
      auto acc = storage.Access(GetNextHlc());
      EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, prop_id, View::OLD), View::OLD),
                  UnorderedElementsAre(0, 1, 2, 3, 4));
    }
  }
  {
    EXPECT_TRUE(storage.DropIndex(label1, prop_id));
    {
      auto acc = storage.Access(GetNextHlc());
      EXPECT_FALSE(acc.LabelPropertyIndexExists(label1, prop_id));
    }
    EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  }
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_TRUE(storage.CreateIndex(label1, prop_id));
    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, prop_id, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    for (int i = 0; i < 5; ++i) {
      auto vertex = acc.CreateVertexAndValidate(
          primary_label, {label1},
          {{primary_property, PropertyValue(primary_key_id++)}, {prop_id, PropertyValue(vertex_id++)}});
      ASSERT_TRUE(vertex.HasValue());
    }

    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, prop_id, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    EXPECT_THAT(GetPrimaryKeyIds(acc.Vertices(label1, prop_id, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

TEST_F(IndexTest, CollectGarbageDeleteVertex) {
  // First part
  // T1 (start 1, commit 3) Creates the vertex, adds label and property
  // T2 (start 2, no commit) reads nothing
  // T3 (start 4, commit 6) deletes label and property
  // T4 (start 5, no commit) reads the vertex
  // T5 (start 7, no commit) reads nothing

  auto t1_start = GetNextHlc();
  auto t2_start = GetNextHlc();
  auto t1_commit = GetNextHlc();
  auto t3_start = GetNextHlc();
  auto t4_start = GetNextHlc();
  auto t3_commit = GetNextHlc();
  auto t5_start = GetNextHlc();

  ASSERT_TRUE(storage.CreateIndex(label1, prop_val));

  auto acc1 = storage.Access(t1_start);
  {
    auto vertex = CreateVertex(acc1);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(42)));
  }
  acc1.Commit(t1_commit);
  EXPECT_THAT(GetIds(acc1.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc1.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));

  auto acc2 = storage.Access(t2_start);
  EXPECT_THAT(GetIds(acc2.Vertices(View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());

  auto acc3 = storage.Access(t3_start);
  {
    auto vertices = acc3.Vertices(label1, prop_val, View::OLD);
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
    auto vertex = *vertices.begin();
    ASSERT_NO_ERROR(acc3.DeleteVertex(&vertex));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  }
  acc3.Commit(t3_commit);
  auto check_t3 = [this, &acc3]() {
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  };
  check_t3();

  auto acc4 = storage.Access(t4_start);
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));

  auto acc5 = storage.Access(t5_start);
  auto check_t5 = [this, &acc5]() {
    EXPECT_THAT(GetIds(acc5.Vertices(View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(GetIds(acc5.Vertices(View::NEW), View::NEW), IsEmpty());
    EXPECT_THAT(GetIds(acc5.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(GetIds(acc5.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  };
  check_t5();

  // Second part
  // Start to clean up things.
  CleanupHlc(t1_start);
  // As the deltas of T1 is cleaned up, T2 will see the vertex as an existing one
  EXPECT_THAT(GetIds(acc2.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
  check_t3();
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
  check_t5();

  CleanupHlc(t3_start);
  // As T3 got cleaned up, it will delete the vertex from the actual storage and the index
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  check_t5();
}

TEST_F(IndexTest, CollectGarbageRemoveLabel) {
  // First part
  // T1 (start 1, commit 3) Creates the vertex, adds label and property
  // T2 (start 2, no commit) reads nothing
  // T3 (start 4, commit 6) removes label
  // T4 (start 5, no commit) reads the vertex
  // T5 (start 7, no commit) reads nothing

  auto t1_start = GetNextHlc();
  auto t2_start = GetNextHlc();
  auto t1_commit = GetNextHlc();
  auto t3_start = GetNextHlc();
  auto t4_start = GetNextHlc();
  auto t3_commit = GetNextHlc();
  auto t5_start = GetNextHlc();

  ASSERT_TRUE(storage.CreateIndex(label1, prop_val));

  auto acc1 = storage.Access(t1_start);
  {
    auto vertex = CreateVertex(acc1);
    ASSERT_NO_ERROR(vertex.AddLabelAndValidate(label1));
    ASSERT_NO_ERROR(vertex.SetPropertyAndValidate(prop_val, PropertyValue(42)));
  }
  acc1.Commit(t1_commit);
  EXPECT_THAT(GetIds(acc1.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc1.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));

  auto acc2 = storage.Access(t2_start);
  EXPECT_THAT(GetIds(acc2.Vertices(View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());

  auto acc3 = storage.Access(t3_start);
  {
    auto vertices = acc3.Vertices(label1, prop_val, View::OLD);
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
    auto vertex = *vertices.begin();
    ASSERT_NO_ERROR(vertex.RemoveLabelAndValidate(label1));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  }
  // We clean up T1 here to make sure the garbace collection doesn't remove the vertex from the indices until the vertex
  // has a version that is reachable from an active transaction, even though the latest version of the vertex doesn't
  // belong to the index
  CleanupHlc(t1_start);
  acc3.Commit(t3_commit);
  auto check_t3 = [this, &acc3]() {
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc3.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  };
  check_t3();

  auto acc4 = storage.Access(t4_start);
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));

  auto acc5 = storage.Access(t5_start);
  auto check_t5 = [this, &acc5]() {
    EXPECT_THAT(GetIds(acc5.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc5.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
    EXPECT_THAT(GetIds(acc5.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
    EXPECT_THAT(GetIds(acc5.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  };
  check_t5();

  // As the deltas of T1 is cleaned up, T2 will see the vertex as an existing one
  EXPECT_THAT(GetIds(acc2.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc2.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
  check_t3();
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), UnorderedElementsAre(0));
  check_t5();

  // Second part
  // Clean up T3 and check the changes
  CleanupHlc(t3_start);
  // As T3 got cleaned up, it will delete the vertex from the index but not from the actual storage
  EXPECT_THAT(GetIds(acc4.Vertices(View::OLD), View::OLD), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(View::NEW), View::NEW), UnorderedElementsAre(0));
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc4.Vertices(label1, prop_val, View::NEW), View::NEW), IsEmpty());
  check_t5();
}
}  // namespace memgraph::storage::v3::tests
