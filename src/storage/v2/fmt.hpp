// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <map>
#include <string>

#include <fmt/format.h>

#include "storage/v2/property_value.hpp"

using SinglePropertyValue = memgraph::storage::PropertyValue;

template <>
class fmt::formatter<SinglePropertyValue> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(SinglePropertyValue const &, Context &ctx) const {
    return fmt::format_to(ctx.out(), "({})", "TODO: PropertyValue");
  }
};

template <>
class fmt::formatter<SinglePropertyValue::Type> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(SinglePropertyValue::Type const &, Context &ctx) const {
    return fmt::format_to(ctx.out(), "({})", "TODO: PropertyValue");
  }
};
