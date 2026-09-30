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
