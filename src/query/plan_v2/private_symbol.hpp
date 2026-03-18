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
  // Arithmetic operators (binary)
  Add,
  Sub,
  Mul,
  Div,
  Mod,
  Exp,
  // Comparison operators (binary)
  Eq,
  Neq,
  Lt,
  Lte,
  Gt,
  Gte,
  // Boolean operators
  And,
  Or,
  Xor,
  Not,
  // Unary operators
  UnaryMinus,
  UnaryPlus,
};

/// Classification predicates for symbol categories
template <symbol S>
constexpr bool is_binary_op_v =
    S == symbol::Add || S == symbol::Sub || S == symbol::Mul || S == symbol::Div || S == symbol::Mod ||
    S == symbol::Exp || S == symbol::Eq || S == symbol::Neq || S == symbol::Lt || S == symbol::Lte || S == symbol::Gt ||
    S == symbol::Gte || S == symbol::And || S == symbol::Or || S == symbol::Xor;

template <symbol S>
constexpr bool is_unary_op_v = S == symbol::Not || S == symbol::UnaryMinus || S == symbol::UnaryPlus;

}  // namespace memgraph::query::plan::v2

namespace std {
using std::hash;

template <>
struct hash<memgraph::query::plan::v2::symbol> {
  size_t operator()(memgraph::query::plan::v2::symbol const &value) const noexcept { return std::to_underlying(value); }
};
}  // namespace std
