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

#include <cstddef>

namespace memgraph::query::plan::v2::child {

// E-node child positions: the single place each symbol's child order is named.
// `symbol_make_traits<S>::make` builds children in this order, and every reader
// (cost, resolve, build) indexes by these names rather than raw integers. Pull
// the relevant one into a function with `using namespace child::<symbol>;`.

namespace bind {
inline constexpr std::size_t input = 0, sym = 1, expr = 2;
}

namespace unwind {
inline constexpr std::size_t input = 0, sym = 1, list = 2;
}

// Dead Unwind elides the sym binding but still evaluates the list (for its
// length), so its resolved children are the densely-packed [input, list] -
// distinct from the alive [input, sym, list] above.
namespace unwind_dead {
inline constexpr std::size_t input = 0, list = 1;
}

// WHERE filter: [input row pipe, predicate expression]. Introduces no binding.
namespace filter {
inline constexpr std::size_t input = 0, predicate = 1;
}

// DISTINCT: [input row pipe, value_ident...]. The value idents are the projected
// columns to dedup on; the build recovers their symbols. Introduces no binding.
namespace distinct {
inline constexpr std::size_t input = 0, first_value = 1;
}

// SKIP / LIMIT: [input row pipe, count expression]. count is evaluated once.
namespace skip {
inline constexpr std::size_t input = 0, count = 1;
}

namespace limit {
inline constexpr std::size_t input = 0, count = 1;
}

// ORDER BY: [input row pipe, sort_key..., value_ident...]. The first `orderings`
// children after the input are the sort keys; the remainder are the value idents
// to remember through the sort. The sort-key count is the interned orderings
// length carried in the disambiguator, so cost/resolve treat all expression
// children uniformly and only the build splits them. Introduces no binding.
namespace order_by {
inline constexpr std::size_t input = 0, first_expr = 1;
}

namespace identifier {
inline constexpr std::size_t sym = 0;
}

namespace named_out {
inline constexpr std::size_t sym = 0, expr = 1;
}

namespace output {
inline constexpr std::size_t pipe = 0, first_named = 1;
}

namespace subquery {
inline constexpr std::size_t outer = 0, inner = 1, first_exposed = 2;
}

namespace function {
inline constexpr std::size_t first_arg = 0;
}

namespace binary {
inline constexpr std::size_t lhs = 0, rhs = 1;
}

namespace unary {
inline constexpr std::size_t operand = 0;
}

}  // namespace memgraph::query::plan::v2::child
