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

#include <optional>
#include <span>

#include "query/plan_v2/egraph/symbol.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

/// Evaluate a foldable operator over constant operands, returning the result as
/// a constant. Folds the arithmetic, comparison, and boolean operators (unary
/// or binary) using Cypher's own value semantics.
///
/// Returns nullopt when the symbol is not a foldable operator, the operand
/// count does not match its arity, the result is not a scalar constant, or
/// evaluation would raise a runtime error (e.g. division by zero, a type
/// mismatch). Declining on error leaves the expression for runtime, so folding
/// never changes a query's observable error behaviour.
auto FoldConstant(symbol op, std::span<storage::ExternalPropertyValue const> operands)
    -> std::optional<storage::ExternalPropertyValue>;

}  // namespace memgraph::query::plan::v2
