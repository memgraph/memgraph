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
#include "tests/unit/collecting_materialiser.hpp"

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;
using memgraph::storage::test::CollectingMaterialiser;

namespace {

PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }

/// What a batched read produces for `properties`, through `memo`.
auto ReadAll(ManifestPropertyStore const &store, ManifestRegistry const &registry,
             std::vector<PropertyId> const &properties, memgraph::storage::PropertyPlanMemo &memo)
    -> std::vector<PropertyValue> {
  CollectingMaterialiser out{properties.size()};
  store.ExtractPropertiesInto(registry, properties, memo, out);
  return std::move(out.values);
}

/// The same, resolving from scratch.
auto ReadAll(ManifestPropertyStore const &store, ManifestRegistry const &registry,
             std::vector<PropertyId> const &properties) -> std::vector<PropertyValue> {
  CollectingMaterialiser out{properties.size()};
  store.ExtractPropertiesInto(registry, properties, out);
  return std::move(out.values);
}

/// The reference store answers with a map; compare like with like.
auto AsMap(memgraph::utils::small_vector<ManifestPropertyStore::PropertyPair> const &properties)
    -> std::map<PropertyId, PropertyValue> {
  return {properties.begin(), properties.end()};
}

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

// A value that carries a discriminator of its own keeps it in the shape, so these have to
// round-trip the discriminator as well as the payload.
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

// A two dimensional point and a three dimensional one are different widths, so they cannot
// share a shape even though they are both points.
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

// Small records live in the object; a record that outgrows it moves to the heap and has to
// keep reading back the same.
TEST_F(ManifestPropertyStoreTest, RecordsSurviveOutgrowingTheInlineStorage) {
  Set(1, PropertyValue(int64_t{7}));
  EXPECT_EQ(Get(1), PropertyValue(int64_t{7}));

  Set(2, PropertyValue(std::string(500, 'x')));

  EXPECT_EQ(Get(1), PropertyValue(int64_t{7}));
  EXPECT_EQ(Get(2), PropertyValue(std::string(500, 'x')));
  EXPECT_GT(store_.buffer_size(), 500);
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
  EXPECT_EQ(AsMap(store_.Properties(registry_)), expected);
}

// A scan resolves where a property lives once and reads every record of that shape with it,
// which has to agree with looking it up per record, including where a record is missing it.
TEST_F(ManifestPropertyStoreTest, ReadingAtAResolvedLocationMatchesLookingItUp) {
  std::vector<ManifestPropertyStore> records(4);
  for (auto i = 0; i < 4; ++i) {
    records[i].InitProperties(
        registry_,
        {{Prop(1), PropertyValue(static_cast<int64_t>(i))}, {Prop(2), PropertyValue(std::string{"shared"})}});
  }
  records[2].SetProperty(registry_, Prop(1), PropertyValue());  // one record stops carrying it

  auto const shape = records[0].manifest();
  auto const &manifest = registry_.Resolve(shape);
  auto const location = manifest.Find(Prop(1));
  ASSERT_TRUE(location);

  for (auto i = 0; i < 4; ++i) {
    ASSERT_EQ(records[i].manifest(), shape) << "record " << i;
    EXPECT_EQ(records[i].GetProperty(manifest, *location), records[i].GetProperty(registry_, Prop(1)))
        << "record " << i;
  }
  EXPECT_TRUE(records[2].GetProperty(manifest, *location).IsNull());
}

// A scan reads the same property from record after record of one shape. The memo is what lets it
// resolve where that property sits once; every record still answers for itself whether it carries
// a value there.
TEST_F(ManifestPropertyStoreTest, MemoisedReadsMatchResolvingEveryTime) {
  std::vector<ManifestPropertyStore> records(4);
  for (auto i = 0; i < 4; ++i) {
    records[i].InitProperties(
        registry_,
        {{Prop(1), PropertyValue(static_cast<int64_t>(i))}, {Prop(2), PropertyValue(std::string{"shared"})}});
  }
  records[2].SetProperty(registry_, Prop(1), PropertyValue());  // one record stops carrying it

  memgraph::storage::PropertyLocationMemo memo;
  for (auto i = 0; i < 4; ++i) {
    EXPECT_EQ(records[i].GetProperty(registry_, Prop(1), memo), records[i].GetProperty(registry_, Prop(1)))
        << "record " << i;
    EXPECT_EQ(records[i].GetProperty(registry_, Prop(2), memo), records[i].GetProperty(registry_, Prop(2)))
        << "record " << i;
  }
}

// The memo answers for a shape, so a record on a different shape must not be read through it.
TEST_F(ManifestPropertyStoreTest, MemoDoesNotServeARecordOfAnotherShape) {
  ManifestPropertyStore narrow;
  narrow.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{7})}});

  ManifestPropertyStore wide;
  wide.InitProperties(registry_,
                      {{Prop(0), PropertyValue(std::string{"first"})}, {Prop(1), PropertyValue(int64_t{9})}});
  ASSERT_NE(narrow.manifest(), wide.manifest()) << "the two records must be differently shaped";

  memgraph::storage::PropertyLocationMemo memo;
  EXPECT_EQ(narrow.GetProperty(registry_, Prop(1), memo).ValueInt(), 7);
  EXPECT_EQ(wide.GetProperty(registry_, Prop(1), memo).ValueInt(), 9);
  EXPECT_EQ(narrow.GetProperty(registry_, Prop(1), memo).ValueInt(), 7);
}

// Reshaping a record moves where its properties sit. A memo filled before the reshape must not be
// used to read after it.
TEST_F(ManifestPropertyStoreTest, MemoFollowsARecordThatChangesShape) {
  store_.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{42})}});

  memgraph::storage::PropertyLocationMemo memo;
  ASSERT_EQ(store_.GetProperty(registry_, Prop(1), memo).ValueInt(), 42);

  store_.SetProperty(registry_, Prop(0), PropertyValue(std::string{"pushes the layout"}));
  EXPECT_EQ(store_.GetProperty(registry_, Prop(1), memo).ValueInt(), 42);
  EXPECT_EQ(store_.GetProperty(registry_, Prop(0), memo).ValueString(), "pushes the layout");
}

// An empty record has no shape, and its reported id is the same value a real shape can be given.
TEST_F(ManifestPropertyStoreTest, MemoIsNotConfusedByAnEmptyRecord) {
  ManifestPropertyStore first;
  first.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{5})}});
  ASSERT_EQ(first.manifest().value, memgraph::storage::ManifestId{}.value)
      << "this test needs the first shape interned";

  ManifestPropertyStore empty;
  memgraph::storage::PropertyLocationMemo memo;
  EXPECT_EQ(first.GetProperty(registry_, Prop(1), memo).ValueInt(), 5);
  EXPECT_TRUE(empty.GetProperty(registry_, Prop(1), memo).IsNull());
  EXPECT_EQ(first.GetProperty(registry_, Prop(1), memo).ValueInt(), 5);
}

// A property the shape does not hold is worth remembering too: it is the common answer on a scan
// over records that do not carry it.
TEST_F(ManifestPropertyStoreTest, MemoRemembersAnAbsentProperty) {
  store_.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{1})}});

  memgraph::storage::PropertyLocationMemo memo;
  EXPECT_TRUE(store_.GetProperty(registry_, Prop(9), memo).IsNull());
  EXPECT_TRUE(store_.GetProperty(registry_, Prop(9), memo).IsNull());
  EXPECT_EQ(store_.GetProperty(registry_, Prop(1), memo).ValueInt(), 1);
}

// Two databases in one process have a registry each, and both hand out the same shape ids. A memo
// filled against one must not answer for the other.
TEST_F(ManifestPropertyStoreTest, MemoDoesNotServeAnotherRegistry) {
  ManifestRegistry other_registry;

  ManifestPropertyStore here;
  here.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{11})}});

  ManifestPropertyStore there;
  there.InitProperties(other_registry, {{Prop(0), PropertyValue(int64_t{0})}, {Prop(1), PropertyValue(int64_t{22})}});
  ASSERT_EQ(here.manifest(), there.manifest()) << "both registries must have issued the same id";

  memgraph::storage::PropertyLocationMemo memo;
  EXPECT_EQ(here.GetProperty(registry_, Prop(1), memo).ValueInt(), 11);
  EXPECT_EQ(there.GetProperty(other_registry, Prop(1), memo).ValueInt(), 22);
  EXPECT_EQ(here.GetProperty(registry_, Prop(1), memo).ValueInt(), 11);
}

// Removing a property leaves the field in the shape and clears the record's claim to it. Every
// way of reading it has to answer Null, not the bytes the value left behind.
TEST_F(ManifestPropertyStoreTest, ReadingARemovedPropertyIsNull) {
  store_.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{42})}, {Prop(2), PropertyValue(int64_t{7})}});
  auto const shape = store_.manifest();
  store_.SetProperty(registry_, Prop(1), PropertyValue());
  ASSERT_EQ(store_.manifest(), shape) << "this test needs the field to stay in the shape";

  EXPECT_TRUE(store_.GetProperty(registry_, Prop(1)).IsNull());

  memgraph::storage::PropertyLocationMemo memo;
  {
    CollectingMaterialiser out{1};
    store_.ExtractPropertyInto(registry_, Prop(1), memo, out);
    EXPECT_TRUE(out.values[0].IsNull()) << "resolved for the first time";
  }
  {
    CollectingMaterialiser out{1};
    store_.ExtractPropertyInto(registry_, Prop(1), memo, out);
    EXPECT_TRUE(out.values[0].IsNull()) << "answered from the memo";
  }

  std::vector<PropertyId> const properties{Prop(1), Prop(2)};
  EXPECT_TRUE(ReadAll(store_, registry_, properties)[0].IsNull());
}

// A batch of properties read from record after record of one shape must answer exactly as
// resolving that shape for every record does, including for a property the shape does not hold.
TEST_F(ManifestPropertyStoreTest, MemoisedBatchedReadsMatchResolvingEveryTime) {
  std::vector<ManifestPropertyStore> records(4);
  for (auto i = 0; i < 4; ++i) {
    records[i].InitProperties(
        registry_,
        {{Prop(1), PropertyValue(static_cast<int64_t>(i))}, {Prop(2), PropertyValue(std::string{"shared"})}});
  }
  records[2].SetProperty(registry_, Prop(1), PropertyValue());  // one record stops carrying it

  std::vector<PropertyId> const properties{Prop(1), Prop(2), Prop(9)};
  memgraph::storage::PropertyPlanMemo memo;
  for (auto i = 0; i < 4; ++i) {
    EXPECT_EQ(ReadAll(records[i], registry_, properties, memo), ReadAll(records[i], registry_, properties))
        << "record " << i;
  }
}

// The remembered positions belong to one shape, so a record of any other shape has to be resolved
// afresh rather than read at them.
TEST_F(ManifestPropertyStoreTest, MemoisedBatchDoesNotServeARecordOfAnotherShape) {
  ManifestPropertyStore narrow;
  narrow.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{7})}});

  ManifestPropertyStore wide;
  wide.InitProperties(registry_,
                      {{Prop(0), PropertyValue(std::string{"first"})}, {Prop(1), PropertyValue(int64_t{9})}});
  ASSERT_NE(narrow.manifest(), wide.manifest()) << "the two records must be differently shaped";

  std::vector<PropertyId> const properties{Prop(0), Prop(1)};
  memgraph::storage::PropertyPlanMemo memo;
  EXPECT_EQ(ReadAll(narrow, registry_, properties, memo), ReadAll(narrow, registry_, properties));
  EXPECT_EQ(ReadAll(wide, registry_, properties, memo), ReadAll(wide, registry_, properties));
  EXPECT_EQ(ReadAll(narrow, registry_, properties, memo), ReadAll(narrow, registry_, properties));
}

// An empty record carries nothing, and the shape id it reports is one a real shape can be given.
TEST_F(ManifestPropertyStoreTest, MemoisedBatchIsNotConfusedByAnEmptyRecord) {
  ManifestPropertyStore first;
  first.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{5})}});
  ASSERT_EQ(first.manifest().value, memgraph::storage::ManifestId{}.value)
      << "this test needs the first shape interned";

  std::vector<PropertyId> const properties{Prop(1)};
  ManifestPropertyStore empty;
  memgraph::storage::PropertyPlanMemo memo;
  EXPECT_EQ(ReadAll(first, registry_, properties, memo)[0].ValueInt(), 5);
  EXPECT_TRUE(ReadAll(empty, registry_, properties, memo)[0].IsNull());
  EXPECT_EQ(ReadAll(first, registry_, properties, memo)[0].ValueInt(), 5);
}

// Two databases in one process each hand out the same shape ids from their own registry.
TEST_F(ManifestPropertyStoreTest, MemoisedBatchDoesNotServeAnotherRegistry) {
  ManifestRegistry other_registry;

  ManifestPropertyStore here;
  here.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{11})}});

  ManifestPropertyStore there;
  there.InitProperties(other_registry, {{Prop(0), PropertyValue(int64_t{0})}, {Prop(1), PropertyValue(int64_t{22})}});
  ASSERT_EQ(here.manifest(), there.manifest()) << "both registries must have issued the same id";

  std::vector<PropertyId> const properties{Prop(1)};
  memgraph::storage::PropertyPlanMemo memo;
  EXPECT_EQ(ReadAll(here, registry_, properties, memo)[0].ValueInt(), 11);
  EXPECT_EQ(ReadAll(there, other_registry, properties, memo)[0].ValueInt(), 22);
  EXPECT_EQ(ReadAll(here, registry_, properties, memo)[0].ValueInt(), 11);
}

// Reshaping a record moves where its properties sit, so what was remembered before the reshape
// must not be read after it.
TEST_F(ManifestPropertyStoreTest, MemoisedBatchFollowsARecordThatChangesShape) {
  store_.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{42})}});

  std::vector<PropertyId> const properties{Prop(0), Prop(1)};
  memgraph::storage::PropertyPlanMemo memo;
  ASSERT_EQ(ReadAll(store_, registry_, properties, memo)[1].ValueInt(), 42);

  store_.SetProperty(registry_, Prop(0), PropertyValue(std::string{"pushes the layout"}));
  auto const after = ReadAll(store_, registry_, properties, memo);
  EXPECT_EQ(after[0].ValueString(), "pushes the layout");
  EXPECT_EQ(after[1].ValueInt(), 42);
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
  EXPECT_EQ(AsMap(store_.Properties(registry_)), properties);
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

// Removing a property leaves the shape and the record's allocation alone, and only clears
// the bit that says the value is there. Shape churn on removal is what this avoids.
TEST_F(ManifestPropertyStoreTest, RemovingAPropertyKeepsTheShape) {
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(int64_t{2}));
  auto const shape = store_.manifest();
  auto const bytes = store_.buffer_size();
  auto const shapes_before = registry_.size();

  Set(2, PropertyValue());

  EXPECT_EQ(store_.manifest(), shape);
  EXPECT_EQ(store_.buffer_size(), bytes);
  EXPECT_EQ(registry_.size(), shapes_before);
  EXPECT_TRUE(Get(2).IsNull());
  EXPECT_FALSE(store_.HasProperty(registry_, Prop(2)));
  EXPECT_EQ(Get(1), PropertyValue(int64_t{1}));
}

// Putting a value back where one was removed reuses the slot the shape still points at.
TEST_F(ManifestPropertyStoreTest, ReAddingARemovedPropertyOfTheSameTypeKeepsTheShape) {
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(int64_t{2}));
  auto const shape = store_.manifest();
  Set(2, PropertyValue());

  EXPECT_TRUE(store_.SetProperty(registry_, Prop(2), PropertyValue(int64_t{7})));

  EXPECT_EQ(store_.manifest(), shape);
  EXPECT_EQ(Get(2), PropertyValue(int64_t{7}));
}

TEST_F(ManifestPropertyStoreTest, RemovingAStringLeavesTheOtherValuesReadable) {
  Set(1, PropertyValue(std::string{"first"}));
  Set(2, PropertyValue(std::string{"second"}));
  Set(3, PropertyValue(int64_t{3}));

  Set(2, PropertyValue());

  EXPECT_EQ(Get(1), PropertyValue(std::string{"first"}));
  EXPECT_TRUE(Get(2).IsNull());
  EXPECT_EQ(Get(3), PropertyValue(int64_t{3}));
  EXPECT_EQ(store_.Properties(registry_).size(), 2);
}

// A field the record is laid out for but does not carry survives a change of shape, so that
// laying a record out in advance is not undone by the first value that has to reshape it.
// The cost is that a removed field keeps its slot until the record is cleared.
TEST_F(ManifestPropertyStoreTest, AShapeChangeKeepsFieldsTheRecordNoLongerCarries) {
  Set(1, PropertyValue(int64_t{1}));
  Set(2, PropertyValue(int64_t{2}));
  Set(2, PropertyValue());

  Set(3, PropertyValue(int64_t{3}));  // adding a property re-interns the shape

  auto const &manifest = registry_.Resolve(store_.manifest());
  EXPECT_EQ(manifest.size(), 3);
  EXPECT_TRUE(manifest.Find(Prop(2)).has_value());
  EXPECT_TRUE(Get(2).IsNull());
  EXPECT_EQ(store_.Properties(registry_).size(), 2);
}

// Records holding the same fields share a shape whether or not they all carry a value for
// every one of them, which is what keeps removals from multiplying shapes.
TEST_F(ManifestPropertyStoreTest, RecordsWithDifferentFieldsMissingShareAShape) {
  ManifestPropertyStore other;
  store_.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{1})}, {Prop(2), PropertyValue(int64_t{2})}});
  other.InitProperties(registry_, {{Prop(1), PropertyValue(int64_t{3})}, {Prop(2), PropertyValue(int64_t{4})}});
  auto const shapes_before = registry_.size();

  Set(1, PropertyValue());
  other.SetProperty(registry_, Prop(2), PropertyValue());

  EXPECT_EQ(store_.manifest(), other.manifest());
  EXPECT_EQ(registry_.size(), shapes_before);
}

// A caller that knows which properties it is about to set can have the record laid out for
// them once, and then every set lands in a slot that is already there.
TEST_F(ManifestPropertyStoreTest, ReservedFieldsAreFilledWithoutReshaping) {
  using memgraph::storage::ManifestEntry;
  using memgraph::storage::PropertyStoreType;
  using memgraph::storage::StoredType;

  auto const fields = std::vector<ManifestEntry>{
      {.property = Prop(1), .stored_type = StoredType::Fixed(PropertyStoreType::INT, 4)},
      {.property = Prop(2), .stored_type = StoredType::Fixed(PropertyStoreType::DOUBLE, 8)},
      {.property = Prop(3), .stored_type = StoredType::Fixed(PropertyStoreType::BOOL, 1)},
  };
  store_.ReserveFields(registry_, fields);
  auto const shape = store_.manifest();
  auto const shapes_after_reserving = registry_.size();

  // Reserved but not yet given a value: the record carries nothing.
  EXPECT_TRUE(store_.Properties(registry_).empty());
  EXPECT_FALSE(store_.HasProperty(registry_, Prop(1)));

  EXPECT_TRUE(store_.SetProperty(registry_, Prop(1), PropertyValue(int64_t{70000})));
  EXPECT_TRUE(store_.SetProperty(registry_, Prop(2), PropertyValue(2.5)));
  EXPECT_TRUE(store_.SetProperty(registry_, Prop(3), PropertyValue(true)));

  EXPECT_EQ(store_.manifest(), shape);
  EXPECT_EQ(registry_.size(), shapes_after_reserving);
  EXPECT_EQ(Get(1), PropertyValue(int64_t{70000}));
  EXPECT_EQ(Get(2), PropertyValue(2.5));
  EXPECT_EQ(Get(3), PropertyValue(true));
}

// The reservation is a guess about widths and types. A value that does not fit the guess has
// to still be stored correctly, by reshaping the record as it would have anyway.
TEST_F(ManifestPropertyStoreTest, AValueWiderThanReservedStillStores) {
  using memgraph::storage::ManifestEntry;
  using memgraph::storage::PropertyStoreType;
  using memgraph::storage::StoredType;

  store_.ReserveFields(registry_,
                       std::vector<ManifestEntry>{
                           {.property = Prop(1), .stored_type = StoredType::Fixed(PropertyStoreType::INT, 1)},
                           {.property = Prop(2), .stored_type = StoredType::Fixed(PropertyStoreType::INT, 1)},
                       });
  Set(1, PropertyValue(int64_t{5}));

  Set(2, PropertyValue(int64_t{1} << 40));  // far wider than the byte reserved for it

  EXPECT_EQ(Get(1), PropertyValue(int64_t{5}));
  EXPECT_EQ(Get(2), PropertyValue(int64_t{1} << 40));
}

TEST_F(ManifestPropertyStoreTest, ReservingLeavesTheRecordEmptyNotAbsent) {
  using memgraph::storage::ManifestEntry;
  using memgraph::storage::PropertyStoreType;
  using memgraph::storage::StoredType;

  store_.ReserveFields(registry_,
                       std::vector<ManifestEntry>{
                           {.property = Prop(1), .stored_type = StoredType::Variable(PropertyStoreType::STRING)},
                       });

  EXPECT_TRUE(Get(1).IsNull());
  EXPECT_TRUE(store_.SetProperty(registry_, Prop(1), PropertyValue(std::string{"late"})));
  EXPECT_EQ(Get(1), PropertyValue(std::string{"late"}));
}

// A list moved into the vector index leaves a value behind holding the ids it was given. The
// float vector itself lives in the index, and reading the property back gives the ids and an
// empty vector, which is what the store it replaced did.
TEST_F(ManifestPropertyStoreTest, VectorIndexIdsRoundTrip) {
  using memgraph::storage::PropertyValue;

  auto const ids = PropertyValue::vector_index_id_t{7, 11, 13};
  Set(1, PropertyValue(PropertyValue::VectorIndexIdData{.ids = ids, .vector = {}}));

  auto const read = Get(1);
  ASSERT_TRUE(read.IsVectorIndexId());
  EXPECT_EQ(read.ValueVectorIndexIds(), ids);
  EXPECT_TRUE(read.ValueVectorIndexList().empty());

  // The type is answered from the shape, so it has to name this as what it is.
  EXPECT_EQ(store_.GetExtendedPropertyType(registry_, Prop(1)).type,
            memgraph::storage::PropertyValueType::VectorIndexId);

  // And a value comparison works on the encoded bytes, so it has to agree with the round trip.
  EXPECT_TRUE(store_.IsPropertyEqual(
      registry_, Prop(1), PropertyValue(PropertyValue::VectorIndexIdData{.ids = ids, .vector = {}})));
  EXPECT_FALSE(store_.IsPropertyEqual(
      registry_,
      Prop(1),
      PropertyValue(PropertyValue::VectorIndexIdData{.ids = PropertyValue::vector_index_id_t{7, 11}, .vector = {}})));
}

TEST_F(ManifestPropertyStoreTest, VectorIndexIdsSurviveNeighbouringWrites) {
  using memgraph::storage::PropertyValue;

  auto const ids = PropertyValue::vector_index_id_t{1, 2, 3, 4};
  Set(1, PropertyValue(PropertyValue::VectorIndexIdData{.ids = ids, .vector = {}}));
  Set(2, PropertyValue(std::string{"neighbour"}));
  Set(2, PropertyValue(std::string{"a much longer neighbour that moves the variable region"}));

  auto const read = Get(1);
  ASSERT_TRUE(read.IsVectorIndexId());
  EXPECT_EQ(read.ValueVectorIndexIds(), ids);
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
      ASSERT_EQ(AsMap(manifest_store.Properties(registry)), reference.Properties()) << "seed " << seed << " op " << op;
    }
  }
}
