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

/// Evaluate a foldable operator (arithmetic, comparison, boolean; unary or
/// binary) over constant operands using Cypher's value semantics. Returns
/// nullopt when `op` is not foldable, the operand count is wrong, the result is
/// not a scalar constant, or evaluation would raise a runtime error (e.g.
/// division by zero). Declining on error leaves it for runtime, so folding
/// never changes observable error behaviour.
///
/// Operands are passed by pointer so the caller need not copy the constants out
/// of the e-classes' analysis; each must stay live for the call. A large
/// String/List/Map constant is borrowed, not duplicated.
auto FoldConstant(symbol op, std::span<storage::ExternalPropertyValue const *const> operands)
    -> std::optional<storage::ExternalPropertyValue>;

}  // namespace memgraph::query::plan::v2
