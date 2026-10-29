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

#include "storage/v2/value_field.hpp"

using memgraph::storage::FieldNamed;
using memgraph::storage::PropertyValue;
using memgraph::storage::ReadField;
using memgraph::storage::TemporalData;
using memgraph::storage::TemporalType;

// A name on its own says which field is wanted. What that field is depends on the value it is
// read off, which is why naming one and reading it are two steps.
TEST(ValueField, ADateHasAYear) {
  auto const field = FieldNamed("year");
  ASSERT_TRUE(field.has_value());

  auto const date = PropertyValue{TemporalData{TemporalType::Date, 1'709'596'800'000'000}};
  EXPECT_EQ(ReadField(date, *field), PropertyValue{int64_t{2024}});
}
