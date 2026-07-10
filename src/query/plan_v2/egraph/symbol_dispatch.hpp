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

#include <utility>

#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/egraph/symbol_lists.hpp"

namespace memgraph::query::plan::v2 {

// One per-axis dispatcher generated from the master EGRAPH_ALL_SYMBOLS list.
// Each axis (resolve, cost, ...) supplies a generic lambda that binds its
// trait + arguments at the call site, so trait method names stay descriptive
// (`thread`, `call`, ...) instead of forced to a single customisation point.
template <typename Op>
decltype(auto) DispatchBySymbol(symbol s, Op &&op) {
  // NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define X(NAME)      \
  case symbol::NAME: \
    return std::forward<Op>(op).template operator()<symbol::NAME>();
  switch (s) { EGRAPH_ALL_SYMBOLS(X) }
#undef X
  // NOLINTEND(cppcoreguidelines-macro-usage)
  std::unreachable();
}

}  // namespace memgraph::query::plan::v2
