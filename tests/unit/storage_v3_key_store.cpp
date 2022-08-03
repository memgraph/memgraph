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

#include <algorithm>
#include <string>
#include <vector>

/**
 * gtest/gtest.h must be included before rapidcheck/gtest.h!
 */
#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::storage::v3::test {

RC_GTEST_PROP(KeyStore, KeyStore, (std::vector<std::string> values)) {
  RC_PRE(!values.empty());

  std::vector<PropertyValue> property_values;
  property_values.reserve(values.size());
  std::transform(values.begin(), values.end(), std::back_inserter(property_values),
                 [](std::string &value) { return PropertyValue{std::move(value)}; });

  KeyStore key_store{property_values};

  const auto keys = key_store.Keys();
  RC_ASSERT(keys.size() == property_values.size());
  for (int i = 0; i < keys.size(); ++i) {
    RC_ASSERT(keys[i] == property_values[i]);
    RC_ASSERT(key_store.GetKey(i) == property_values[i]);
  }
}
}  // namespace memgraph::storage::v3::test
