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

#include <cstdint>
#include <limits>
#include <string_view>
#include <vector>

#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/manifest_property_store.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/temporal.hpp"
#include "storage/v2/vertex.hpp"
#include "tests/unit/value_shapes.hpp"

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;

namespace {
PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }
}  // namespace

// A vertex must not grow when its properties become a record, and these three say it cannot.
// A member replaced by one no larger, at an offset its alignment already allows, leaves every
// later member where it was and so leaves the size where it was. Stated against the two stores
// rather than against a copy of a vertex's members, which would be a second place to maintain
// a layout and would agree with the first only by luck.
//
// Here rather than in either header, since neither includes the other and the switchover is
// what will bring them together.
static_assert(sizeof(ManifestPropertyStore) <= sizeof(PropertyStore),
              "A record must fit where the store it replaces sits");
static_assert(alignof(ManifestPropertyStore) <= alignof(memgraph::storage::Vertex),
              "A record must not align a vertex more strictly than it already is");
static_assert(offsetof(memgraph::storage::Vertex, properties) % alignof(ManifestPropertyStore) == 0,
              "A record must sit where the store it replaces sits");

/// Gives both stores the same properties, the same replacements and the same removals, and
/// requires them to answer alike throughout. The old store is the specification, so a
/// disagreement is this store being wrong.
void ExpectAgreesWithPropertyStore(std::vector<PropertyValue> const &values) {
  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  auto const probe_all = [&](std::string_view after) {
    for (uint32_t probe = 0; probe <= values.size() + 1; ++probe) {
      ASSERT_EQ(manifest.GetProperty(registry, Prop(probe)), current.GetProperty(Prop(probe)))
          << "reading " << probe << " after " << after;
      // Every candidate against every property, so each stored kind is asked about a value of
      // its own kind and of all the others, which is where a comparison gets equality wrong.
      for (std::size_t candidate = 0; candidate != values.size(); ++candidate) {
        ASSERT_EQ(manifest.IsPropertyEqual(registry, Prop(probe), values[candidate]),
                  current.IsPropertyEqual(Prop(probe), values[candidate]))
            << "comparing " << probe << " against candidate " << candidate << " after " << after;
      }
      ASSERT_EQ(manifest.IsPropertyEqual(registry, Prop(probe), PropertyValue{}),
                current.IsPropertyEqual(Prop(probe), PropertyValue{}))
          << "comparing " << probe << " against Null after " << after;
    }
    ASSERT_EQ(manifest.Properties(registry), current.Properties()) << "the whole record after " << after;
  };

  // One of each, so the record holds every shape at once and each one's bytes sit next to a
  // neighbour's.
  for (std::size_t at = 0; at != values.size(); ++at) {
    auto const property = Prop(static_cast<uint32_t>(at + 1));
    ASSERT_EQ(manifest.SetProperty(registry, property, values[at]), current.SetProperty(property, values[at]))
        << "setting " << at + 1;
    probe_all("setting them in turn");
  }

  // Then each property takes its neighbour's value, so every one changes type and the
  // variable ones change length.
  for (std::size_t at = 0; at != values.size(); ++at) {
    auto const property = Prop(static_cast<uint32_t>(at + 1));
    auto const &replacement = values[(at + 1) % values.size()];
    ASSERT_EQ(manifest.SetProperty(registry, property, replacement), current.SetProperty(property, replacement))
        << "replacing " << at + 1;
    probe_all("shifting each value onto its neighbour");
  }

  // Then away one at a time, so the record shrinks through every intermediate shape.
  for (std::size_t at = 0; at != values.size(); ++at) {
    auto const property = Prop(static_cast<uint32_t>(at + 1));
    ASSERT_EQ(manifest.SetProperty(registry, property, PropertyValue{}), current.SetProperty(property, PropertyValue{}))
        << "removing " << at + 1;
    probe_all("removing them one at a time");
  }

  ASSERT_TRUE(manifest.Properties(registry).empty());
  ASSERT_TRUE(current.Properties().empty());
}

// A constraint and an index ask whether a stored property is equivalent to a value, and do it
// per record, so the answer must come without building the stored value up again.
TEST(ManifestPropertyStore, AnIntegerIsPlacedUnderEquivalenceWithoutBeingRead) {
  ManifestRegistry registry;
  ManifestPropertyStore store;
  store.SetProperty(registry, Prop(1), PropertyValue{int64_t{300}});

  EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{int64_t{300}}));
  EXPECT_FALSE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{int64_t{301}}));
  // A stored integer and a double of the same number are equivalent.
  EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{300.0}));
  EXPECT_FALSE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{300.5}));
  EXPECT_FALSE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{"300"}));
  // A property the record does not hold reads as Null, so it is equivalent to Null and to
  // nothing else.
  EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(2), PropertyValue{}));
  EXPECT_FALSE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{}));
}

// A record describes itself by naming its shape, so reading a property back means resolving
// that shape and reading the value from where the shape says it sits.
TEST(ManifestPropertyStore, AnIntegerIsReadBackAsItWasSet) {
  ManifestRegistry registry;
  ManifestPropertyStore store;

  EXPECT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{42}));
  EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{42});
}

// An integer is stored at the narrowest of four widths that holds it, and the width belongs
// to the shape rather than to the record. Reading it back has to sign-extend from that width,
// so the value at each boundary is the value that was written and not a wider one's bit
// pattern read short.
TEST(ManifestPropertyStore, AnIntegerIsReadBackAtEveryWidth) {
  for (auto const value : {std::numeric_limits<int64_t>::min(),
                           std::numeric_limits<int64_t>::min() + 1,
                           int64_t{std::numeric_limits<int32_t>::min()} - 1,
                           int64_t{std::numeric_limits<int32_t>::min()},
                           int64_t{std::numeric_limits<int16_t>::min()} - 1,
                           int64_t{std::numeric_limits<int16_t>::min()},
                           int64_t{std::numeric_limits<int8_t>::min()} - 1,
                           int64_t{std::numeric_limits<int8_t>::min()},
                           int64_t{-1},
                           int64_t{0},
                           int64_t{1},
                           int64_t{std::numeric_limits<int8_t>::max()},
                           int64_t{std::numeric_limits<int8_t>::max()} + 1,
                           int64_t{std::numeric_limits<int16_t>::max()},
                           int64_t{std::numeric_limits<int16_t>::max()} + 1,
                           int64_t{std::numeric_limits<int32_t>::max()},
                           int64_t{std::numeric_limits<int32_t>::max()} + 1,
                           std::numeric_limits<int64_t>::max()}) {
    ManifestRegistry registry;
    ManifestPropertyStore store;

    ASSERT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{value}));
    EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{value}) << "value " << value;
  }
}

// A record holding several properties gives them one shape between them, so setting a second
// one changes the shape the record names and moves the first value to wherever the new shape
// puts it. Both have to read back.
TEST(ManifestPropertyStore, SeveralIntegersAreReadBackAfterTheShapeChanges) {
  ManifestRegistry registry;
  ManifestPropertyStore store;

  EXPECT_TRUE(store.SetProperty(registry, Prop(2), PropertyValue{int64_t{1}}));
  // Wider than the first and ordered before it, so neither its width nor its offset is what
  // it would have been had it been set alone.
  EXPECT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{int64_t{1} << 40}));

  EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{int64_t{1} << 40});
  EXPECT_EQ(store.GetProperty(registry, Prop(2)), PropertyValue{int64_t{1}});
  EXPECT_EQ(registry.size(), 2) << "one shape for the first property, one for the pair";
}

// Setting a property the record already carries replaces the value rather than adding it, and
// says so by answering false.
TEST(ManifestPropertyStore, ReplacingAValueKeepsOneProperty) {
  ManifestRegistry registry;
  ManifestPropertyStore store;

  EXPECT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{int64_t{7}}));
  EXPECT_FALSE(store.SetProperty(registry, Prop(1), PropertyValue{int64_t{8}}));

  EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{int64_t{8}});
}

// A property the record does not carry reads as Null, as asking the current store for one does.
TEST(ManifestPropertyStore, AnAbsentPropertyReadsAsNull) {
  ManifestRegistry registry;
  ManifestPropertyStore store;

  EXPECT_TRUE(store.GetProperty(registry, Prop(1)).IsNull()) << "an empty record carries nothing";

  ASSERT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{int64_t{1}}));
  EXPECT_TRUE(store.GetProperty(registry, Prop(2)).IsNull());
}

// The store that exists today is the specification. A record given the same properties in the
// same order has to answer every question the same way, including what setting a property
// reports and what the record holds afterwards. Where the two part, this one is wrong.
TEST(ManifestPropertyStore, AnswersAsThePropertyStoreDoesForIntegers) {
  struct Step {
    uint32_t property;
    PropertyValue value;
    char const *what;
  };

  auto const script = std::vector<Step>{
      {1, PropertyValue{int64_t{7}}, "a first property"},
      {3, PropertyValue{int64_t{1} << 40}, "a wider one after it"},
      {2, PropertyValue{int64_t{-1}}, "one ordered between them"},
      {1, PropertyValue{int64_t{1} << 20}, "widening one already there"},
      {1, PropertyValue{int64_t{1}}, "narrowing it again"},
      {2, PropertyValue{}, "removing one with a Null"},
      {2, PropertyValue{}, "removing one that is already gone"},
      {2, PropertyValue{int64_t{5}}, "putting it back"},
      {1, PropertyValue{}, "removing the first"},
      {3, PropertyValue{}, "removing the next"},
      {2, PropertyValue{}, "removing the last, leaving nothing"},
  };

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  for (auto const &step : script) {
    auto const property = Prop(step.property);
    EXPECT_EQ(manifest.SetProperty(registry, property, step.value), current.SetProperty(property, step.value))
        << "setting " << step.property << ", " << step.what;

    for (uint32_t probe = 1; probe <= 4; ++probe) {
      EXPECT_EQ(manifest.GetProperty(registry, Prop(probe)), current.GetProperty(Prop(probe)))
          << "reading " << probe << " after " << step.what;
    }
    EXPECT_EQ(manifest.Properties(registry), current.Properties()) << "the whole record after " << step.what;
  }
}

// A string's encoded width varies with the value, so the shape cannot give it a byte offset.
// It gives it an index into a table the record carries, and the record holds the bytes after
// the values whose width the shape does know.
TEST(ManifestPropertyStore, AStringIsReadBackAsItWasSet) {
  ManifestRegistry registry;
  ManifestPropertyStore store;

  EXPECT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{"hello"}));
  EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{"hello"});
}

// The same comparison against the old store, over a record whose strings and integers are
// interleaved. A string's bytes sit after the values whose width the shape knows, so changing
// one moves every string after it and none of the integers, which is the part a record holding
// only one kind never exercises.
TEST(ManifestPropertyStore, AnswersAsThePropertyStoreDoesForStringsAmongIntegers) {
  struct Step {
    uint32_t property;
    PropertyValue value;
    char const *what;
  };

  auto const script = std::vector<Step>{
      {2, PropertyValue{"beta"}, "a string"},
      {4, PropertyValue{"delta"}, "a second string after it"},
      {1, PropertyValue{int64_t{1}}, "an integer before both"},
      {3, PropertyValue{int64_t{1} << 40}, "a wide integer between the strings"},
      {2, PropertyValue{"beta-but-much-longer"}, "growing the first string"},
      {2, PropertyValue{"b"}, "shrinking it again"},
      {5, PropertyValue{""}, "an empty string last"},
      {0, PropertyValue{""}, "an empty string first"},
      {4, PropertyValue{}, "removing a string from the middle"},
      {3, PropertyValue{}, "removing the integer between them"},
      {2, PropertyValue{int64_t{9}}, "replacing a string with an integer"},
      {1, PropertyValue{"was-an-integer"}, "replacing an integer with a string"},
      {0, PropertyValue{}, "removing the empty one"},
      {5, PropertyValue{}, "removing the other empty one"},
      {1, PropertyValue{}, "removing what is left"},
      {2, PropertyValue{}, "leaving nothing"},
  };

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  for (auto const &step : script) {
    auto const property = Prop(step.property);
    EXPECT_EQ(manifest.SetProperty(registry, property, step.value), current.SetProperty(property, step.value))
        << "setting " << step.property << ", " << step.what;

    for (uint32_t probe = 0; probe <= 6; ++probe) {
      EXPECT_EQ(manifest.GetProperty(registry, Prop(probe)), current.GetProperty(Prop(probe)))
          << "reading " << probe << " after " << step.what;
    }
    EXPECT_EQ(manifest.Properties(registry), current.Properties()) << "the whole record after " << step.what;
  }
}

// Booleans and doubles have one width each, so a shape knows where they sit as it does for
// an integer. They go in beside the kinds already covered rather than on their own.
TEST(ManifestPropertyStore, AgreesOverBooleansAndDoubles) {
  ExpectAgreesWithPropertyStore({
      PropertyValue{true},
      PropertyValue{false},
      PropertyValue{2.5},
      PropertyValue{-0.0},
      PropertyValue{int64_t{3}},
      PropertyValue{"text"},
  });
}

// Where this is going: every shape a record can hold, in one record, adjudicated by the store
// that exists today. Null is left out because it removes a property rather than being one,
// and a vector index id because only its ids are written.
TEST(ManifestPropertyStore, AgreesOverEveryShape) {
  ExpectAgreesWithPropertyStore(memgraph::test::shapes::EveryShapeExcept(
      {memgraph::storage::PropertyValueType::Null, memgraph::storage::PropertyValueType::VectorIndexId}));
}

// An enum, a temporal value and a point are each a small fixed object, so a shape knows the
// width of all of them. The two kinds of point share one type tag and differ in width, which
// is enough to tell them apart because the width is part of the shape.
TEST(ManifestPropertyStore, AgreesOverEnumsTemporalsAndPoints) {
  using memgraph::storage::PropertyValueType;
  ExpectAgreesWithPropertyStore(memgraph::test::shapes::EveryShapeExcept({
      PropertyValueType::Null,
      PropertyValueType::VectorIndexId,
      PropertyValueType::List,
      PropertyValueType::Map,
      PropertyValueType::IntList,
      PropertyValueType::DoubleList,
      PropertyValueType::NumericList,
      PropertyValueType::ZonedTemporalData,
  }));
}

// A list or a map holds values of its own, so its payload has to say what each element is.
// The shape takes the type tag off a record's properties, not off the values nested inside
// one, so those carry their own.
TEST(ManifestPropertyStore, AgreesOverListsAndMaps) {
  using memgraph::storage::PropertyValueType;
  ExpectAgreesWithPropertyStore(memgraph::test::shapes::EveryShapeExcept({
      PropertyValueType::Null,
      PropertyValueType::VectorIndexId,
      PropertyValueType::ZonedTemporalData,
  }));
}

// A composite index asks about several properties at once, and one on a nested property asks
// about a key inside a value. Both are asked of every record the index covers, so both have
// to answer as the old store does.
TEST(ManifestPropertyStore, AnswersAsThePropertyStoreDoesForSeveralPropertiesAtOnce) {
  using memgraph::storage::PropertyPath;

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  auto const nested = PropertyValue{PropertyValue::map_t{
      {Prop(7), PropertyValue{int64_t{11}}},
      {Prop(8), PropertyValue{"inner"}},
  }};
  for (auto const &[property, value] : std::initializer_list<std::pair<PropertyId, PropertyValue>>{
           {Prop(1), PropertyValue{int64_t{300}}},
           {Prop(2), PropertyValue{"outer"}},
           {Prop(3), nested},
           {Prop(4), PropertyValue{2.5}},
       }) {
    manifest.SetProperty(registry, property, value);
    current.SetProperty(property, value);
  }

  // In path order, which is what the old store requires of a caller: its reader only goes
  // forwards, so a path behind where it has reached reads as absent. For the same reason a
  // path and a longer one starting with it are asked about separately.
  auto const paths = std::vector<PropertyPath>{
      PropertyPath{Prop(1)},
      PropertyPath{Prop(2)},
      PropertyPath{Prop(3)},
      // A property the record does not hold.
      PropertyPath{Prop(5)},
  };
  auto const nested_paths = std::vector<PropertyPath>{
      PropertyPath{Prop(3), Prop(7)},
      PropertyPath{Prop(3), Prop(8)},
      // A key the nested value does not hold.
      PropertyPath{Prop(3), Prop(9)},
  };
  auto const candidates = std::vector<PropertyValue>{
      PropertyValue{int64_t{300}},
      PropertyValue{300.0},
      PropertyValue{"outer"},
      PropertyValue{"inner"},
      PropertyValue{nested},
      PropertyValue{int64_t{11}},
      PropertyValue{2.5},
      PropertyValue{int64_t{}},
      PropertyValue{},
  };

  auto expected = std::vector<bool>{};
  auto actual = std::vector<bool>{};
  auto const expect_agreement = [&](std::vector<PropertyPath> const &asked,
                                    std::vector<PropertyValue> const &values,
                                    std::vector<std::size_t> const &position_lookup) {
    current.ArePropertiesEqual(asked, values, position_lookup, expected);
    manifest.ArePropertiesEqual(registry, asked, values, position_lookup, actual);
    EXPECT_EQ(actual, expected);
  };

  // Every path asked about every candidate, so a right answer for one path cannot stand in for
  // a wrong one for another.
  for (auto const &asked : {paths, nested_paths}) {
    auto const all_at_zero = std::vector<std::size_t>(asked.size(), 0);
    for (auto const &candidate : candidates) {
      expect_agreement(asked, std::vector<PropertyValue>{candidate}, all_at_zero);
    }
  }

  // Then each path against the value sitting at it, with the values in an order of their own
  // so the lookup is doing something.
  expect_agreement(paths,
                   {PropertyValue{nested}, PropertyValue{"outer"}, PropertyValue{}, PropertyValue{int64_t{300}}},
                   {3, 1, 0, 2});
  expect_agreement(nested_paths, {PropertyValue{"inner"}, PropertyValue{}, PropertyValue{int64_t{11}}}, {2, 0, 1});

  // A record carrying nothing answers for every path at once.
  ManifestRegistry empty_registry;
  ManifestPropertyStore empty_manifest;
  PropertyStore empty_current;
  for (auto const &asked : {paths, nested_paths}) {
    auto const values = std::vector<PropertyValue>{PropertyValue{int64_t{300}}, PropertyValue{}};
    auto const lookup = std::vector<std::size_t>(asked.size(), 1);
    empty_current.ArePropertiesEqual(asked, values, lookup, expected);
    empty_manifest.ArePropertiesEqual(empty_registry, asked, values, lookup, actual);
    EXPECT_EQ(actual, expected);
  }
}

// One path's answer must not depend on another's in the same ask, which a shape giving each
// property's place on its own is what secures. Stated as the answers themselves rather than
// against the old store, whose reader only goes forwards and so cannot answer these.
// A path's segments name keys, and a store answers for keys alone. A moment carries fields of
// its own that are spelled the way a key is, and whether a name means one is settled by the
// names behind the ids, which a record cannot see. So a path into a value that is not a map
// arrives at nothing here, and reading a field of one is answered above a store, by whatever
// holds the path and can resolve its names.
//
// Pinned because a store that answered fields itself would answer them twice over once it is
// read through an index, and one that stopped answering keys would answer them not at all.
TEST(ManifestPropertyStore, APathIntoAValueWithFieldsOfItsOwnArrivesAtNothing) {
  using memgraph::storage::PropertyPath;
  using memgraph::storage::TemporalData;
  using memgraph::storage::TemporalType;

  ManifestRegistry registry;
  ManifestPropertyStore store;
  // 2024-03-05, whose year is 2024 and whose `year` is no key of its.
  auto const moment = PropertyValue{TemporalData{TemporalType::Date, 1'709'596'800'000'000}};
  store.SetProperty(registry, Prop(1), moment);

  auto const asked = std::vector<PropertyPath>{PropertyPath{Prop(1), Prop(2)}};
  auto const values = std::vector<PropertyValue>{PropertyValue{int64_t{2024}}, PropertyValue{}};
  auto result = std::vector<bool>{};

  store.ArePropertiesEqual(registry, asked, values, std::vector<std::size_t>{0}, result);
  EXPECT_EQ(result, (std::vector<bool>{false}));
  store.ArePropertiesEqual(registry, asked, values, std::vector<std::size_t>{1}, result);
  EXPECT_EQ(result, (std::vector<bool>{true}));

  // And the moment itself is still read as the moment it is.
  EXPECT_EQ(store.GetProperty(registry, Prop(1)), moment);
}

TEST(ManifestPropertyStore, OnePathsAnswerDoesNotDependOnAnothers) {
  using memgraph::storage::PropertyPath;

  ManifestRegistry registry;
  ManifestPropertyStore store;
  store.SetProperty(registry, Prop(1), PropertyValue{int64_t{300}});
  store.SetProperty(registry,
                    Prop(3),
                    PropertyValue{PropertyValue::map_t{
                        {Prop(7), PropertyValue{int64_t{11}}},
                    }});

  auto const values = std::vector<PropertyValue>{PropertyValue{int64_t{11}}, PropertyValue{int64_t{300}}};
  auto result = std::vector<bool>{};

  // A path descending into a value that is not a map arrives at nothing, and the path after it
  // is still found.
  auto const after_a_non_map =
      std::vector<PropertyPath>{PropertyPath{Prop(1), Prop(7)}, PropertyPath{Prop(3), Prop(7)}};
  store.ArePropertiesEqual(registry, after_a_non_map, values, std::vector<std::size_t>{0, 0}, result);
  EXPECT_EQ(result, (std::vector<bool>{false, true}));

  // Arriving at nothing means nothing, and in particular not the value the walk stopped at:
  // asked about that value, the path still answers false, and about Null, true.
  store.ArePropertiesEqual(registry, after_a_non_map, values, std::vector<std::size_t>{1, 1}, result);
  EXPECT_EQ(result, (std::vector<bool>{false, false}));
  auto const null_and_eleven = std::vector<PropertyValue>{PropertyValue{}, PropertyValue{int64_t{11}}};
  store.ArePropertiesEqual(registry, after_a_non_map, null_and_eleven, std::vector<std::size_t>{0, 1}, result);
  EXPECT_EQ(result, (std::vector<bool>{true, true}));

  // And a path is still found when the property it starts at was asked about on its own first.
  auto const after_its_own_root = std::vector<PropertyPath>{PropertyPath{Prop(1)}, PropertyPath{Prop(3), Prop(7)}};
  store.ArePropertiesEqual(registry, after_its_own_root, values, std::vector<std::size_t>{1, 0}, result);
  EXPECT_EQ(result, (std::vector<bool>{true, true}));
}

// An index is keyed by the values at several paths at once, and builds that key per record, so
// it asks for all of them in one pass rather than a path at a time. A path arriving at nothing
// is keyed as Null, which is how a record is keyed on what it does not carry.
TEST(ManifestPropertyStore, ReadsSeveralPathsAtOnceAsThePropertyStoreDoes) {
  using memgraph::storage::PropertyPath;
  using memgraph::storage::TemporalData;
  using memgraph::storage::TemporalType;

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  auto const nested = PropertyValue{PropertyValue::map_t{
      {Prop(7), PropertyValue{int64_t{11}}},
      {Prop(8), PropertyValue{"inner"}},
  }};
  for (auto const &[property, value] : std::initializer_list<std::pair<PropertyId, PropertyValue>>{
           {Prop(1), PropertyValue{int64_t{300}}},
           {Prop(2), PropertyValue{"outer"}},
           {Prop(3), nested},
           // A value with fields of its own, which a path's last segment cannot name here.
           {Prop(4), PropertyValue{TemporalData{TemporalType::Date, 1'709'596'800'000'000}}},
       }) {
    manifest.SetProperty(registry, property, value);
    current.SetProperty(property, value);
  }

  // In path order, which the old store requires of a caller because its reader only goes
  // forwards. A path and a longer one starting with it are asked about separately for the same
  // reason.
  auto const whole = std::vector<PropertyPath>{
      PropertyPath{Prop(1)},
      PropertyPath{Prop(2)},
      PropertyPath{Prop(3)},
      PropertyPath{Prop(4)},
      // A property the record does not carry.
      PropertyPath{Prop(5)},
  };
  auto const nested_paths = std::vector<PropertyPath>{
      PropertyPath{Prop(3), Prop(7)},
      PropertyPath{Prop(3), Prop(8)},
      // A key the nested value does not hold.
      PropertyPath{Prop(3), Prop(9)},
      // A step into a value that is not a map, and one into a property that is not there.
      PropertyPath{Prop(4), Prop(9)},
      PropertyPath{Prop(5), Prop(9)},
  };

  for (auto const &asked : {whole, nested_paths}) {
    EXPECT_EQ(manifest.ExtractPropertyValuesMissingAsNull(registry, asked),
              current.ExtractPropertyValuesMissingAsNull(asked));
  }

  // The same answers written into a caller's buffer rather than a returned one, which is how a
  // fixed-arity key is built without allocating.
  for (auto const &asked : {whole, nested_paths}) {
    auto into = std::vector<PropertyValue>(asked.size());
    manifest.ExtractPropertyValuesMissingAsNull(registry, asked, into);
    EXPECT_EQ(into, current.ExtractPropertyValuesMissingAsNull(asked));
  }

  // A record carrying nothing is keyed as Null at every path.
  ManifestRegistry empty_registry;
  ManifestPropertyStore empty;
  for (auto const &asked : {whole, nested_paths}) {
    EXPECT_EQ(empty.ExtractPropertyValuesMissingAsNull(empty_registry, asked),
              std::vector<PropertyValue>(asked.size()));
  }
}

// An index keys a record by reading it, and a path's last segment may name a field the value
// carries of its own rather than a key it holds. Which of the two it is takes the names behind
// the ids, which no store can see, so the index resolves the name once and then reads the
// record's own property. This store is asked for that property the way the old one is, and a
// record it holds is keyed exactly as the same record in the old store.
TEST(ManifestPropertyStore, AnIndexKeysARecordAsItKeysAPropertyStore) {
  using memgraph::storage::IndexOrderedValuesVector;
  using memgraph::storage::NameIdMapper;
  using memgraph::storage::PropertiesPermutationHelper;
  using memgraph::storage::PropertyPath;
  using memgraph::storage::TemporalData;
  using memgraph::storage::TemporalType;

  NameIdMapper mapper;
  auto const a = PropertyId::FromUint(mapper.NameToId("a"));
  auto const b = PropertyId::FromUint(mapper.NameToId("b"));
  auto const c = PropertyId::FromUint(mapper.NameToId("c"));
  auto const year = PropertyId::FromUint(mapper.NameToId("year"));
  auto const k = PropertyId::FromUint(mapper.NameToId("k"));

  // A whole property, a field of a second property, and a key inside a third, which is a shape
  // an index can be created in. The paths are given out of order, so the permutation is doing
  // something.
  auto const paths = std::vector<PropertyPath>{PropertyPath{c, k}, PropertyPath{a}, PropertyPath{b, year}};
  PropertiesPermutationHelper helper{paths, mapper};

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;
  // 2024-03-05, whose year is 2024 and whose `year` is no key of its.
  auto const held = std::vector<std::pair<PropertyId, PropertyValue>>{
      {a, PropertyValue{int64_t{300}}},
      {b, PropertyValue{TemporalData{TemporalType::Date, 1'709'596'800'000'000}}},
      {c, PropertyValue{PropertyValue::map_t{{k, PropertyValue{int64_t{11}}}}}},
  };
  for (auto const &[property, value] : held) {
    manifest.SetProperty(registry, property, value);
    current.SetProperty(property, value);
  }
  auto const record = memgraph::storage::ManifestRecord{manifest, registry};

  auto const expected = helper.ApplyPermutation(helper.Extract(current));
  auto const keyed = helper.ApplyPermutation(helper.Extract(record));
  EXPECT_EQ(keyed.values_, expected.values_);
  // The field is read rather than left as the Null a store alone would answer.
  EXPECT_EQ(keyed.values_[2], PropertyValue{int64_t{2024}});

  // The same key built into a caller's buffer rather than a returned one.
  auto into = std::vector<PropertyValue>(paths.size());
  helper.ExtractInto(record, into);
  helper.ApplyPermutationInPlace(into);
  EXPECT_EQ(into, expected.values_);

  // And a record compares against a key the way it is read into one, for the key it is keyed
  // on and for one it is not.
  auto const elsewhere = IndexOrderedValuesVector{std::vector<PropertyValue>{
      PropertyValue{int64_t{11}}, PropertyValue{int64_t{301}}, PropertyValue{int64_t{2024}}}};
  for (auto const &against : {expected, elsewhere}) {
    auto from_current = std::vector<bool>{};
    auto from_manifest = std::vector<bool>{};
    helper.MatchesValues(current, against, from_current);
    helper.MatchesValues(record, against, from_manifest);
    EXPECT_EQ(from_manifest, from_current);
  }

  // A record carrying nothing is keyed as Null at every path, as the old store's is.
  ManifestRegistry empty_registry;
  ManifestPropertyStore empty;
  PropertyStore empty_current;
  EXPECT_EQ(helper.Extract(memgraph::storage::ManifestRecord{empty, empty_registry}), helper.Extract(empty_current));
}

// A record names its own shape, in its own bytes, so that holding a record costs a pointer and
// nothing else. Two records of the same shape name the same one, and a record carrying nothing
// names none.
TEST(ManifestPropertyStore, ARecordNamesItsOwnShape) {
  ManifestRegistry registry;
  ManifestPropertyStore store;
  EXPECT_FALSE(store.manifest().has_value());

  store.SetProperty(registry, Prop(1), PropertyValue{int64_t{300}});
  auto const one_integer = store.manifest();
  ASSERT_TRUE(one_integer.has_value());

  // A second property is a different shape, and the first shape is still the first.
  store.SetProperty(registry, Prop(2), PropertyValue{"outer"});
  auto const integer_and_string = store.manifest();
  ASSERT_TRUE(integer_and_string.has_value());
  EXPECT_NE(integer_and_string->value, one_integer->value);

  // A shape carries the width its integers are stored at, so another record names the same
  // shape when its integer is stored at the same width, and a different one when a narrower
  // integer is stored at a narrower width. Its string's length is no part of either, being a
  // width the shape does not know.
  ManifestPropertyStore alike;
  alike.SetProperty(registry, Prop(1), PropertyValue{int64_t{301}});
  alike.SetProperty(registry, Prop(2), PropertyValue{"a longer string than the other one"});
  EXPECT_EQ(alike.manifest()->value, integer_and_string->value);

  ManifestPropertyStore narrower;
  narrower.SetProperty(registry, Prop(1), PropertyValue{int64_t{7}});
  narrower.SetProperty(registry, Prop(2), PropertyValue{"other"});
  EXPECT_NE(narrower.manifest()->value, integer_and_string->value);

  // Reading the values back is unaffected by the shape sitting in front of them.
  EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{int64_t{300}});
  EXPECT_EQ(store.GetProperty(registry, Prop(2)), PropertyValue{"outer"});

  // Emptied, it names no shape, because it has no record to name one in.
  store.SetProperty(registry, Prop(1), PropertyValue{});
  store.SetProperty(registry, Prop(2), PropertyValue{});
  EXPECT_FALSE(store.manifest().has_value());
}

namespace {
/// Leaves the resolution as it was found, so one of these tests cannot decide what another
/// stores.
struct RestoreFpResolutionGuard {
  uint64_t saved = FLAGS_storage_floating_point_resolution_bits;

  ~RestoreFpResolutionGuard() { FLAGS_storage_floating_point_resolution_bits = saved; }
};

/// What the shape says the record's only property takes.
auto StoredWidthOf(ManifestRegistry const &registry, ManifestPropertyStore const &store) -> uint8_t {
  auto const entries = registry.Resolve(*store.manifest()).entries();
  return entries.front().stored_type.width;
}
}  // namespace

// A double may be stored at less than its full width, which is a setting. The width is settled
// when the record is written and kept in its shape, so the setting is read once per write and
// never by a read.
TEST(ManifestPropertyStore, ADoubleIsStoredAtTheWidthTheSettingAsksFor) {
  RestoreFpResolutionGuard guard;

  for (auto const [resolution, width] : {std::pair{uint64_t{16}, uint8_t{2}},
                                         std::pair{uint64_t{32}, uint8_t{4}},
                                         std::pair{uint64_t{64}, uint8_t{8}}}) {
    FLAGS_storage_floating_point_resolution_bits = resolution;

    ManifestRegistry registry;
    ManifestPropertyStore store;
    ASSERT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{1.5}));

    EXPECT_EQ(StoredWidthOf(registry, store), width) << "at a resolution of " << resolution;
    // A value the narrowest width holds exactly, so it reads back as itself at every one.
    EXPECT_EQ(store.GetProperty(registry, Prop(1)), PropertyValue{1.5}) << "at a resolution of " << resolution;
    EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(1), PropertyValue{1.5})) << "at a resolution of " << resolution;
  }
}

// What a record says cannot depend on anything but the record and the shape it names. The
// setting is the one piece of state that could reach in, so a record written under one is read
// the same way after it changes: the width it was written at is in its shape, and a shape never
// changes once interned.
TEST(ManifestPropertyStore, ARecordReadsTheSameAfterTheSettingChanges) {
  RestoreFpResolutionGuard guard;

  // A value with more precision than a half or a float can hold, so reading it at the wrong
  // width could not pass for reading it at the right one.
  auto const value = 1.2345678901234567;

  for (auto const written_at : {uint64_t{16}, uint64_t{32}, uint64_t{64}}) {
    FLAGS_storage_floating_point_resolution_bits = written_at;

    ManifestRegistry registry;
    ManifestPropertyStore store;
    ASSERT_TRUE(store.SetProperty(registry, Prop(1), PropertyValue{value}));
    auto const as_stored = store.GetProperty(registry, Prop(1));
    ASSERT_TRUE(as_stored.IsDouble());

    for (auto const read_at : {uint64_t{16}, uint64_t{32}, uint64_t{64}}) {
      FLAGS_storage_floating_point_resolution_bits = read_at;
      EXPECT_EQ(store.GetProperty(registry, Prop(1)), as_stored)
          << "written at " << written_at << ", read at " << read_at;
      EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(1), as_stored))
          << "written at " << written_at << ", compared at " << read_at;
      // And the record still says what it carries, which is one double at the width it was
      // written at.
      EXPECT_EQ(store.Properties(registry).at(Prop(1)), as_stored)
          << "written at " << written_at << ", read at " << read_at;
    }
  }
}

// At one setting the two stores answer alike, which is what makes the new one a replacement
// rather than a different store that happens to hold doubles.
TEST(ManifestPropertyStore, ADoubleIsStoredAsTheOldStoreStoresIt) {
  RestoreFpResolutionGuard guard;

  for (auto const resolution : {uint64_t{16}, uint64_t{32}, uint64_t{64}}) {
    FLAGS_storage_floating_point_resolution_bits = resolution;

    for (auto const value : {0.0,
                             -0.0,
                             1.5,
                             -1.5,
                             0.1,
                             1.2345678901234567,
                             65504.0,
                             1e-8,
                             std::numeric_limits<double>::infinity(),
                             -std::numeric_limits<double>::infinity()}) {
      ManifestRegistry registry;
      ManifestPropertyStore manifest;
      PropertyStore current;
      ASSERT_EQ(manifest.SetProperty(registry, Prop(1), PropertyValue{value}),
                current.SetProperty(Prop(1), PropertyValue{value}));

      EXPECT_EQ(manifest.GetProperty(registry, Prop(1)), current.GetProperty(Prop(1)))
          << value << " at a resolution of " << resolution;
      // And each store compares its own stored value the way it reads it.
      auto const stored = current.GetProperty(Prop(1));
      EXPECT_EQ(manifest.IsPropertyEqual(registry, Prop(1), stored), current.IsPropertyEqual(Prop(1), stored))
          << value << " at a resolution of " << resolution;
    }
  }
}

// The other piece of state a read could depend on is the registry a named zone is interned in,
// which is one for the process and not one per database. Naming more zones after a record is
// written must not change what that record says, which holding ids that are never reused is
// what secures.
TEST(ManifestPropertyStore, ARecordsZoneIsUnaffectedByWhatIsNamedAfterIt) {
  using memgraph::storage::ZonedTemporalData;
  using memgraph::storage::ZonedTemporalType;
  using memgraph::utils::Timezone;

  auto const moment = std::chrono::sys_time<std::chrono::microseconds>{std::chrono::microseconds{1'700'000'000'000}};
  auto const value =
      PropertyValue{ZonedTemporalData{ZonedTemporalType::ZonedDateTime, moment, Timezone{"Europe/London"}}};

  ManifestRegistry registry;
  ManifestPropertyStore store;
  ASSERT_TRUE(store.SetProperty(registry, Prop(1), value));
  ASSERT_EQ(store.GetProperty(registry, Prop(1)), value);

  // Several other zones named afterwards, in another record, so the registry holds more than
  // it did when this one was written.
  ManifestPropertyStore later;
  for (auto const *name : {"America/Los_Angeles", "Asia/Tokyo", "Australia/Sydney", "Africa/Cairo"}) {
    later.SetProperty(
        registry, Prop(1), PropertyValue{ZonedTemporalData{ZonedTemporalType::ZonedDateTime, moment, Timezone{name}}});
  }

  EXPECT_EQ(store.GetProperty(registry, Prop(1)), value);
  EXPECT_TRUE(store.IsPropertyEqual(registry, Prop(1), value));
}

// A vector index takes a list of numbers out of the record and leaves a handle behind: the ids
// the list was given. The numbers live in the index, so the record holds the ids and nothing
// else, and reading the property gives them back with no numbers beside them. Whatever reads a
// record for a user puts those back from the index.
TEST(ManifestPropertyStore, ARecordHoldsTheHandleAVectorIndexLeftIt) {
  auto const handle = [](std::initializer_list<PropertyValue::vector_index_id_t::value_type> ids) {
    return PropertyValue{PropertyValue::VectorIndexIdData{.ids = PropertyValue::vector_index_id_t(ids),
                                                          .vector = memgraph::utils::small_vector<float>{}}};
  };

  ManifestRegistry registry;
  ManifestPropertyStore manifest;
  PropertyStore current;

  for (auto const &value : {handle({7}), handle({7, 11, 13}), handle({13, 11, 7})}) {
    ASSERT_EQ(manifest.SetProperty(registry, Prop(1), value), current.SetProperty(Prop(1), value));

    auto const read = manifest.GetProperty(registry, Prop(1));
    ASSERT_TRUE(read.IsVectorIndexId());
    EXPECT_EQ(read, current.GetProperty(Prop(1)));
    EXPECT_EQ(read.ValueVectorIndexIds(), value.ValueVectorIndexIds());
    EXPECT_TRUE(read.ValueVectorIndexList().empty()) << "the numbers live in the index, not the record";

    // A handle is the set of ids rather than a list of them, so the same ids in another order
    // are the same handle. Both stores place it that way.
    EXPECT_EQ(manifest.IsPropertyEqual(registry, Prop(1), value), current.IsPropertyEqual(Prop(1), value));
    EXPECT_TRUE(manifest.IsPropertyEqual(registry, Prop(1), value));
    EXPECT_EQ(manifest.IsPropertyEqual(registry, Prop(1), handle({11})),
              current.IsPropertyEqual(Prop(1), handle({11})));
  }

  // Two ids in either order are one handle, and a handle of two is not a handle of three.
  // Replacing rather than inserting, the record already carrying the property.
  ASSERT_FALSE(manifest.SetProperty(registry, Prop(1), handle({7, 11})));
  EXPECT_TRUE(manifest.IsPropertyEqual(registry, Prop(1), handle({11, 7})));
  EXPECT_FALSE(manifest.IsPropertyEqual(registry, Prop(1), handle({7, 11, 13})));
  EXPECT_FALSE(manifest.IsPropertyEqual(registry, Prop(1), handle({7})));
  // And a handle is not a list of the numbers that happen to be those ids.
  EXPECT_FALSE(manifest.IsPropertyEqual(
      registry, Prop(1), PropertyValue{PropertyValue::list_t{PropertyValue{int64_t{7}}, PropertyValue{int64_t{11}}}}));

  // It sits in a record beside other properties without disturbing them.
  ASSERT_TRUE(manifest.SetProperty(registry, Prop(2), PropertyValue{"beside it"}));
  EXPECT_EQ(manifest.GetProperty(registry, Prop(1)).ValueVectorIndexIds(), handle({7, 11}).ValueVectorIndexIds());
  EXPECT_EQ(manifest.GetProperty(registry, Prop(2)), PropertyValue{"beside it"});
}

// A zoned temporal value is a moment, a kind, and a zone. A zone given as an offset is a
// number within a known range, and one taken from the database is named, so naming it by an
// interned id makes the whole value one of a known width. Nothing about it then needs the
// record's table of where variable values end.
TEST(ManifestPropertyStore, AZonedTemporalIsStoredAtAKnownWidth) {
  using memgraph::storage::ZonedTemporalData;
  using memgraph::storage::ZonedTemporalType;
  using memgraph::utils::Timezone;

  auto const moment = std::chrono::sys_time<std::chrono::microseconds>{std::chrono::microseconds{1'700'000'000'000}};

  for (auto const &zone : {Timezone{"Europe/London"}, Timezone{std::chrono::minutes{-330}}}) {
    ManifestRegistry registry;
    ManifestPropertyStore store;
    auto const value = PropertyValue{ZonedTemporalData{ZonedTemporalType::ZonedDateTime, moment, zone}};

    ASSERT_TRUE(store.SetProperty(registry, Prop(1), value));
    EXPECT_EQ(store.GetProperty(registry, Prop(1)), value);

    ASSERT_TRUE(store.manifest().has_value());
    EXPECT_EQ(registry.Resolve(*store.manifest()).variable_count(), 0)
        << "a zoned temporal value should not need the variable region";
  }
}
