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

#pragma once
#include <string>

#include <range/v3/range/conversion.hpp>
#include <range/v3/view.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>

namespace memgraph::flags {

template <typename R>
inline auto CanonicalizeString(R &&rng) -> std::string {
  namespace rv = ranges::views;
  auto const is_space = [](auto c) { return c == ' '; };
  auto const to_lower = [](unsigned char c) { return std::tolower(c); };

  return rng | rv::drop_while(is_space) | rv::take_while(std::not_fn(is_space)) | rv::transform(to_lower) |
         ranges::to<std::string>;
};

}  // namespace memgraph::flags
