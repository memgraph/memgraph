// Copyright 2025 Memgraph Ltd.
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

#include <cstdint>
#include <functional>
#include <utility>

namespace memgraph::query::plan::v2 {

enum struct symbol : std::uint8_t {
  Once,
  Bind,
  Symbol,
  Literal,
  Identifier,
  Output,
  NamedOutput,
  ParamLookup,
};

}  // namespace memgraph::query::plan::v2

namespace std {
using std::hash;
template <>
struct hash<memgraph::query::plan::v2::symbol> {
  size_t operator()(memgraph::query::plan::v2::symbol const &value) const noexcept { return std::to_underlying(value); }
};
}  // namespace std
