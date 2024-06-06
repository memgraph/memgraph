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

TEST(EnumStore, BasicTests) {
  auto sut = EnumStore{};

  auto result = sut.register_enum("Location", {"Zagreb"s, "York"s});
  ASSERT_FALSE(result.HasError());

  auto enum_id = sut.to_enum_type("Location");
  ASSERT_TRUE(enum_id.HasValue());
  ASSERT_EQ(*enum_id, EnumTypeId{0});

  {
    auto enum_value_id = sut.to_enum_value(*enum_id, "Zagreb");
    ASSERT_TRUE(enum_value_id.HasValue());
    ASSERT_EQ(*enum_value_id, EnumValueId{0});
  }
  {
    auto enum_value_id = sut.to_enum_value(*enum_id, "York");
    ASSERT_TRUE(enum_value_id.HasValue());
    ASSERT_EQ(*enum_value_id, EnumValueId{1});
  }
  {
    auto name = sut.to_type_string(EnumTypeId{0});
    ASSERT_TRUE(name.HasValue());
    ASSERT_EQ(*name, "Location");
  }
  {
    auto value = sut.to_value_string(EnumTypeId{0}, EnumValueId{0});
    ASSERT_TRUE(value.HasValue());
    ASSERT_EQ(*value, "Zagreb");
  }
  {
    auto value = sut.add_value("Location", "London");
    ASSERT_TRUE(value.HasValue());
    ASSERT_EQ(value->type_id(), EnumTypeId{0});
    ASSERT_EQ(value->value_id(), EnumValueId{2});
  }
  ASSERT_TRUE(sut.add_value("Location", "London").HasError());
  ASSERT_TRUE(sut.update_value("Location", "London", "London").HasError());
  ASSERT_TRUE(sut.update_value("Location", "London", "York").HasError());
  {
    auto value = sut.update_value("Location", "London", "New York");
    ASSERT_TRUE(value.HasValue());
    ASSERT_EQ(value->type_id(), EnumTypeId{0});
    ASSERT_EQ(value->value_id(), EnumValueId{2});
    auto str = sut.to_value_string(value->type_id(), value->value_id());
    ASSERT_TRUE(str.HasValue());
    ASSERT_TRUE(*str == "New York");
  }
}
