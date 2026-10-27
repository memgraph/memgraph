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

#include "storage/v2/manifest_property_store.hpp"
#include "storage/v2/property_store.hpp"
#include "tests/unit/value_shapes.hpp"

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;

namespace {
PropertyId Prop(uint32_t id) { return PropertyId::FromUint(id); }
}  // namespace

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
//
// Disabled until every kind is encoded. Enabling it is what says this store is finished, and
// running it names the next kind to do.
TEST(ManifestPropertyStore, DISABLED_AgreesOverEveryShape) {
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
