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

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"

using namespace memgraph::storage;

namespace {
const PropertyId kP = PropertyId::FromUint(1);
const PropertyId kQ = PropertyId::FromUint(2);
}  // namespace

TEST(PropertyStoreExplicitNull, TriStateLookup) {
  PropertyStore store;
  EXPECT_FALSE(store.FindProperty(kP).has_value());

  EXPECT_TRUE(store.SetExplicitNull(kP));
  auto found = store.FindProperty(kP);
  ASSERT_TRUE(found.has_value());
  EXPECT_TRUE(found->IsNull());
  EXPECT_TRUE(store.GetProperty(kP).IsNull());
  EXPECT_FALSE(store.FindProperty(kQ).has_value());
}

TEST(PropertyStoreExplicitNull, PropertiesReportsExplicitNullAsNull) {
  PropertyStore store;
  store.SetProperty(kQ, PropertyValue{7});
  store.SetExplicitNull(kP);
  auto props = store.Properties();
  ASSERT_EQ(props.size(), 2);
  EXPECT_TRUE(props.at(kP).IsNull());
  EXPECT_EQ(props.at(kQ).ValueInt(), 7);
}

TEST(PropertyStoreExplicitNull, OverwriteNullValueNull) {
  PropertyStore store;
  store.SetExplicitNull(kP);
  EXPECT_FALSE(store.SetProperty(kP, PropertyValue{"v"}));  // replaced an existing entry
  EXPECT_EQ(store.FindProperty(kP)->ValueString(), "v");
  EXPECT_FALSE(store.SetExplicitNull(kP));
  ASSERT_TRUE(store.FindProperty(kP).has_value());
  EXPECT_TRUE(store.FindProperty(kP)->IsNull());
}

TEST(PropertyStoreExplicitNull, PlainSetNullStillErases) {
  PropertyStore store;
  store.SetExplicitNull(kP);
  store.SetProperty(kQ, PropertyValue{7});
  EXPECT_FALSE(store.SetProperty(kP, PropertyValue{}));
  EXPECT_FALSE(store.FindProperty(kP).has_value());
  EXPECT_TRUE(store.FindProperty(kQ).has_value());
}
