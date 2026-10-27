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
