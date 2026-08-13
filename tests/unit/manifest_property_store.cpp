// Copyright 2026 Memgraph Ltd.
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

#include <map>
#include <random>
#include <string>
#include <vector>

#include "storage/v2/manifest_property_store.hpp"
#include "storage/v2/property_store.hpp"

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;

namespace {

PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }

}  // namespace

class ManifestPropertyStoreTest : public ::testing::Test {
 protected:
  ManifestRegistry registry_;
  ManifestPropertyStore store_;

  void Set(uint32_t id, PropertyValue value) { store_.SetProperty(registry_, Prop(id), value); }

  PropertyValue Get(uint32_t id) const { return store_.GetProperty(registry_, Prop(id)); }
};

TEST_F(ManifestPropertyStoreTest, EmptyStoreHasNothing) {
  EXPECT_TRUE(Get(1).IsNull());
  EXPECT_FALSE(store_.HasProperty(registry_, Prop(1)));
  EXPECT_TRUE(store_.Properties(registry_).empty());
}

TEST_F(ManifestPropertyStoreTest, RoundTripsBool) {
  Set(1, PropertyValue(true));
  Set(2, PropertyValue(false));

  EXPECT_EQ(Get(1), PropertyValue(true));
  EXPECT_EQ(Get(2), PropertyValue(false));
}

TEST_F(ManifestPropertyStoreTest, RoundTripsIntegersOfEveryWidthClass) {
  auto const values = std::vector<int64_t>{0,
                                           1,
                                           -1,
                                           127,
                                           -128,
                                           32767,
                                           -32768,
                                           2147483647,
                                           -2147483648LL,
                                           std::numeric_limits<int64_t>::max(),
                                           std::numeric_limits<int64_t>::min()};
  for (auto const value : values) {
    Set(1, PropertyValue(value));
    EXPECT_EQ(Get(1), PropertyValue(value)) << "value " << value;
  }
}

TEST_F(ManifestPropertyStoreTest, RoundTripsDouble) {
  Set(1, PropertyValue(3.25));
  EXPECT_EQ(Get(1), PropertyValue(3.25));

  Set(1, PropertyValue(-0.0001220703125));
  EXPECT_EQ(Get(1), PropertyValue(-0.0001220703125));
}

TEST_F(ManifestPropertyStoreTest, RoundTripsStrings) {
  Set(1, PropertyValue(std::string{}));
  EXPECT_EQ(Get(1), PropertyValue(std::string{}));

  Set(1, PropertyValue(std::string{"cat_07"}));
  EXPECT_EQ(Get(1), PropertyValue(std::string{"cat_07"}));

  Set(1, PropertyValue(std::string(5000, 'x')));
  EXPECT_EQ(Get(1), PropertyValue(std::string(5000, 'x')));
}

TEST_F(ManifestPropertyStoreTest, HoldsManyPropertiesOfMixedKinds) {
  Set(1, PropertyValue(int64_t{42}));
  Set(2, PropertyValue(std::string{"region_3"}));
  Set(3, PropertyValue(2.5));
  Set(4, PropertyValue(true));
  Set(5, PropertyValue(std::string{"cat_00"}));

  EXPECT_EQ(Get(1), PropertyValue(int64_t{42}));
  EXPECT_EQ(Get(2), PropertyValue(std::string{"region_3"}));
  EXPECT_EQ(Get(3), PropertyValue(2.5));
  EXPECT_EQ(Get(4), PropertyValue(true));
  EXPECT_EQ(Get(5), PropertyValue(std::string{"cat_00"}));

  auto const expected = std::map<PropertyId, PropertyValue>{
      {Prop(1), PropertyValue(int64_t{42})},
      {Prop(2), PropertyValue(std::string{"region_3"})},
      {Prop(3), PropertyValue(2.5)},
      {Prop(4), PropertyValue(true)},
      {Prop(5), PropertyValue(std::string{"cat_00"})},
  };
  EXPECT_EQ(store_.Properties(registry_), expected);
}

TEST_F(ManifestPropertyStoreTest, AbsentPropertiesReadAsNull) {
  Set(2, PropertyValue(int64_t{1}));

  EXPECT_TRUE(Get(1).IsNull());
  EXPECT_TRUE(Get(3).IsNull());
  EXPECT_FALSE(store_.HasProperty(registry_, Prop(3)));
  EXPECT_TRUE(store_.HasProperty(registry_, Prop(2)));
}

TEST_F(ManifestPropertyStoreTest, SetReportsWhetherItInserted) {
  EXPECT_TRUE(store_.SetProperty(registry_, Prop(1), PropertyValue(int64_t{1})));
  EXPECT_FALSE(store_.SetProperty(registry_, Prop(1), PropertyValue(int64_t{2})));
  EXPECT_TRUE(store_.SetProperty(registry_, Prop(2), PropertyValue(int64_t{1})));
}

TEST_F(ManifestPropertyStoreTest, SettingNullRemovesTheProperty) {
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(std::string{"gone"}));

  Set(2, PropertyValue());

  EXPECT_FALSE(store_.HasProperty(registry_, Prop(2)));
  EXPECT_TRUE(Get(2).IsNull());
  EXPECT_EQ(Get(1), PropertyValue(int64_t{1}));
  EXPECT_EQ(store_.Properties(registry_).size(), 1);
}

// The shape stays put when a value is overwritten in place, which is what keeps the common
// update off the interning path.
TEST_F(ManifestPropertyStoreTest, OverwritingWithinAWidthClassKeepsTheShape) {
  Set(1, PropertyValue(int64_t{1000}));
  Set(2, PropertyValue(2.0));
  auto const shape = store_.manifest();

  Set(1, PropertyValue(int64_t{2000}));
  Set(2, PropertyValue(4.0));

  EXPECT_EQ(store_.manifest(), shape);
  EXPECT_EQ(Get(1), PropertyValue(int64_t{2000}));
  EXPECT_EQ(Get(2), PropertyValue(4.0));
}

TEST_F(ManifestPropertyStoreTest, GrowingPastAWidthClassMovesToANewShape) {
  Set(1, PropertyValue(int64_t{1}));
  auto const narrow = store_.manifest();

  Set(1, PropertyValue(int64_t{1} << 40));

  EXPECT_NE(store_.manifest(), narrow);
  EXPECT_EQ(Get(1), PropertyValue(int64_t{1} << 40));
}

TEST_F(ManifestPropertyStoreTest, ChangingTypeMovesToANewShape) {
  Set(1, PropertyValue(int64_t{1}));
  auto const as_int = store_.manifest();

  Set(1, PropertyValue(std::string{"now a string"}));

  EXPECT_NE(store_.manifest(), as_int);
  EXPECT_EQ(Get(1), PropertyValue(std::string{"now a string"}));
}

TEST_F(ManifestPropertyStoreTest, RecordsOfTheSameShapeShareOneManifest) {
  ManifestPropertyStore other;
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(std::string{"a"}));
  other.SetProperty(registry_, Prop(2), PropertyValue(std::string{"b"}));
  other.SetProperty(registry_, Prop(1), PropertyValue(int64_t{2}));

  EXPECT_EQ(store_.manifest(), other.manifest());
}

// Building a record one property at a time interns a shape per prefix, and records written
// in different orders pass through different prefixes. Only the shape they end on is shared,
// so a bulk-init path matters for keeping the registry small on load.
TEST_F(ManifestPropertyStoreTest, BuildingARecordInternsAShapePerPrefix) {
  ManifestPropertyStore other;
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(std::string{"a"}));
  other.SetProperty(registry_, Prop(2), PropertyValue(std::string{"b"}));
  other.SetProperty(registry_, Prop(1), PropertyValue(int64_t{2}));

  // {int}, {string}, and the {int, string} both end on.
  EXPECT_EQ(registry_.size(), 3);
}

TEST_F(ManifestPropertyStoreTest, InitPropertiesSetsThemAllAtOnce) {
  auto const properties = std::map<PropertyId, PropertyValue>{
      {Prop(1), PropertyValue(int64_t{42})},
      {Prop(2), PropertyValue(std::string{"region_3"})},
      {Prop(3), PropertyValue(true)},
  };

  EXPECT_TRUE(store_.InitProperties(registry_, properties));
  EXPECT_EQ(store_.Properties(registry_), properties);
}

// Bulk init is what keeps a load off the per-prefix interning path: one shape, not one per
// property added.
TEST_F(ManifestPropertyStoreTest, InitPropertiesInternsExactlyOneShape) {
  store_.InitProperties(registry_,
                        {
                            {Prop(1), PropertyValue(int64_t{42})},
                            {Prop(2), PropertyValue(std::string{"region_3"})},
                            {Prop(3), PropertyValue(true)},
                        });

  EXPECT_EQ(registry_.size(), 1);
}

TEST_F(ManifestPropertyStoreTest, InitPropertiesRefusesANonEmptyRecord) {
  Set(1, PropertyValue(int64_t{1}));

  EXPECT_FALSE(store_.InitProperties(registry_, {{Prop(2), PropertyValue(int64_t{2})}}));
  EXPECT_FALSE(store_.HasProperty(registry_, Prop(2)));
  EXPECT_EQ(Get(1), PropertyValue(int64_t{1}));
}

TEST_F(ManifestPropertyStoreTest, InitPropertiesIgnoresNulls) {
  EXPECT_TRUE(store_.InitProperties(registry_,
                                    {
                                        {Prop(1), PropertyValue(int64_t{1})},
                                        {Prop(2), PropertyValue()},
                                    }));

  EXPECT_FALSE(store_.HasProperty(registry_, Prop(2)));
  EXPECT_EQ(store_.Properties(registry_).size(), 1);
}

TEST_F(ManifestPropertyStoreTest, ClearRemovesEverything) {
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(std::string{"x"}));

  EXPECT_TRUE(store_.ClearProperties());
  EXPECT_TRUE(store_.Properties(registry_).empty());
  EXPECT_TRUE(Get(1).IsNull());
  EXPECT_FALSE(store_.ClearProperties());
}

TEST_F(ManifestPropertyStoreTest, VariableValuesStayIntactWhenNeighboursChange) {
  Set(1, PropertyValue(std::string{"first"}));
  Set(2, PropertyValue(std::string{"second"}));
  Set(3, PropertyValue(std::string{"third"}));

  Set(2, PropertyValue(std::string{"a much longer replacement for the middle value"}));

  EXPECT_EQ(Get(1), PropertyValue(std::string{"first"}));
  EXPECT_EQ(Get(2), PropertyValue(std::string{"a much longer replacement for the middle value"}));
  EXPECT_EQ(Get(3), PropertyValue(std::string{"third"}));
}

// The store has to agree with the one it would replace, over sequences neither was written for.
TEST(ManifestPropertyStoreDifferential, AgreesWithPropertyStoreOverRandomOperations) {
  constexpr auto kSeeds = 200;
  constexpr auto kOperations = 60;
  constexpr auto kProperties = 6;

  for (auto seed = 0; seed < kSeeds; ++seed) {
    ManifestRegistry registry;
    ManifestPropertyStore manifest_store;
    PropertyStore reference;

    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint32_t> property_dist(1, kProperties);
    std::uniform_int_distribution<int> kind_dist(0, 5);
    std::uniform_int_distribution<int64_t> int_dist(std::numeric_limits<int64_t>::min(),
                                                    std::numeric_limits<int64_t>::max());
    std::uniform_int_distribution<size_t> length_dist(0, 40);

    for (auto op = 0; op < kOperations; ++op) {
      auto const property = Prop(property_dist(gen));
      auto value = PropertyValue{};
      switch (kind_dist(gen)) {
        case 0:
          break;  // null, i.e. a removal
        case 1:
          value = PropertyValue(int_dist(gen));
          break;
        case 2:
          value = PropertyValue(static_cast<int64_t>(int_dist(gen) % 1000));
          break;
        case 3:
          value = PropertyValue(static_cast<double>(int_dist(gen)) / 7.0);
          break;
        case 4:
          value = PropertyValue(int_dist(gen) % 2 == 0);
          break;
        default:
          value = PropertyValue(std::string(length_dist(gen), 'a' + static_cast<char>(op % 26)));
          break;
      }

      auto const manifest_inserted = manifest_store.SetProperty(registry, property, value);
      auto const reference_inserted = reference.SetProperty(property, value);
      ASSERT_EQ(manifest_inserted, reference_inserted) << "seed " << seed << " op " << op;
      ASSERT_EQ(manifest_store.Properties(registry), reference.Properties()) << "seed " << seed << " op " << op;
    }
  }
}

TEST_F(ManifestPropertyStoreTest, RoundTripsTemporalData) {
  using memgraph::storage::TemporalData;
  using memgraph::storage::TemporalType;

  Set(1, PropertyValue(TemporalData{TemporalType::Date, 1'600'000'000}));
  EXPECT_EQ(Get(1), PropertyValue(TemporalData{TemporalType::Date, 1'600'000'000}));

  Set(2, PropertyValue(TemporalData{TemporalType::Duration, -42}));
  EXPECT_EQ(Get(2), PropertyValue(TemporalData{TemporalType::Duration, -42}));
}

TEST_F(ManifestPropertyStoreTest, TemporalValuesOfDifferentTypesTakeDifferentShapes) {
  using memgraph::storage::TemporalData;
  using memgraph::storage::TemporalType;

  Set(1, PropertyValue(TemporalData{TemporalType::Date, 1}));
  auto const as_date = store_.manifest();

  Set(1, PropertyValue(TemporalData{TemporalType::LocalTime, 1}));

  EXPECT_NE(store_.manifest(), as_date);
  EXPECT_EQ(Get(1), PropertyValue(TemporalData{TemporalType::LocalTime, 1}));
}

TEST_F(ManifestPropertyStoreTest, RoundTripsEnum) {
  using memgraph::storage::Enum;
  using memgraph::storage::EnumTypeId;
  using memgraph::storage::EnumValueId;

  Set(1, PropertyValue(Enum{EnumTypeId{3}, EnumValueId{7}}));
  EXPECT_EQ(Get(1), PropertyValue(Enum{EnumTypeId{3}, EnumValueId{7}}));

  Set(1, PropertyValue(Enum{EnumTypeId{3}, EnumValueId{9}}));
  EXPECT_EQ(Get(1), PropertyValue(Enum{EnumTypeId{3}, EnumValueId{9}}));
}

TEST_F(ManifestPropertyStoreTest, RoundTripsPoints) {
  using memgraph::storage::CoordinateReferenceSystem;
  using memgraph::storage::Point2d;
  using memgraph::storage::Point3d;

  Set(1, PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, 1.5, -2.25}));
  EXPECT_EQ(Get(1), PropertyValue(Point2d{CoordinateReferenceSystem::WGS84_2d, 1.5, -2.25}));

  Set(2, PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}));
  EXPECT_EQ(Get(2), PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}));
}

TEST_F(ManifestPropertyStoreTest, PointsOfDifferentDimensionsTakeDifferentShapes) {
  using memgraph::storage::CoordinateReferenceSystem;
  using memgraph::storage::Point2d;
  using memgraph::storage::Point3d;

  Set(1, PropertyValue(Point2d{CoordinateReferenceSystem::Cartesian_2d, 1.0, 2.0}));
  auto const flat = store_.manifest();

  Set(1, PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}));

  EXPECT_NE(store_.manifest(), flat);
  EXPECT_EQ(Get(1), PropertyValue(Point3d{CoordinateReferenceSystem::Cartesian_3d, 1.0, 2.0, 3.0}));
}

TEST_F(ManifestPropertyStoreTest, RecordsSurviveOutgrowingTheInlineStorage) {
  Set(1, PropertyValue(int64_t{7}));
  EXPECT_EQ(Get(1), PropertyValue(int64_t{7}));

  Set(2, PropertyValue(std::string(500, 'x')));

  EXPECT_EQ(Get(1), PropertyValue(int64_t{7}));
  EXPECT_EQ(Get(2), PropertyValue(std::string(500, 'x')));
  EXPECT_GT(store_.buffer_size(), 500);
}
