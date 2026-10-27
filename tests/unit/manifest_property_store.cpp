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

#include "storage/v2/manifest_property_store.hpp"

using memgraph::storage::ManifestPropertyStore;
using memgraph::storage::ManifestRegistry;
using memgraph::storage::PropertyId;
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
