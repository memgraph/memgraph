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

#include "query/plan/preprocess.hpp"

namespace memgraph::query::plan {

/**
 * @brief Try to generate an expression with as few filters as possible.
 *
 * Currently this only means we combine two compatible filters into a RangeOperator.
 * If we introduce parse-time literals, we can could combine more than two into a range, or even detect that the range
 * would be empty and remove branch/query from execution.
 *
 * @param filter_expr whole expression to compress
 * @param storage AST storage
 * @return Expression* compressed expression
 */
Expression *CompactFilters(Expression *filter_expr, AstStorage &storage);

/**
 * @brief Convert a RangeOperator to a Filter
 *
 * @param range
 * @param symbol_table
 * @return FilterInfo
 */
FilterInfo RangeOpToFilter(RangeOperator *range, const SymbolTable &symbol_table);

}  // namespace memgraph::query::plan
