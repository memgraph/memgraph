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

#include <fmt/ostream.h>

#include "mg_procedure.h"

template <>
class fmt::formatter<mgp_log_level> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(mgp_log_level const &, Context &ctx) const {
    return fmt::format_to(ctx.out(), "({})", "TODO: format mgp_log_level");
  }
};

template <>
class fmt::formatter<mgp_error> {
 public:
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename Context>
  constexpr auto format(mgp_error const &, Context &ctx) const {
    return fmt::format_to(ctx.out(), "({})", "TODO: format mgp_error");
  }
};
