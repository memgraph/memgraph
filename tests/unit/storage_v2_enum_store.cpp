// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/enum_store.hpp"

using namespace memgraph::storage;
using namespace std::string_literals;

enum Doo {
  a = 9,
  b = 100,
};

enum class Doo2 {
  a = 9,
  b = 100,
};

TEST(EnumStore, BasicTests) {
  auto sut = EnumStore{};

  auto result = sut.register_enum("Location", {"Zagreb"s, "York"s});
  ASSERT_FALSE(result.HasError());

  auto enum_id = sut.to_enum_type("Location");
  ASSERT_TRUE(enum_id.has_value());
  ASSERT_EQ(enum_id, EnumTypeId{0});

  {
    auto enum_value_id = sut.to_enum_value(*enum_id, "Zagreb");
    ASSERT_TRUE(enum_value_id.has_value());
    ASSERT_EQ(enum_value_id, EnumValueId{0});
  }
  {
    auto enum_value_id = sut.to_enum_value(*enum_id, "York");
    ASSERT_TRUE(enum_value_id.has_value());
    ASSERT_EQ(enum_value_id, EnumValueId{1});
  }

  auto name = sut.to_type_string(EnumTypeId{0});
  ASSERT_EQ(name, "Location");

  auto value = sut.to_value_string(EnumTypeId{0}, EnumValueId{0});
  ASSERT_EQ(value, "Zagreb");
}
