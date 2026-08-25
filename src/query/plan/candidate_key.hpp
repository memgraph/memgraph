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
#include <vector>

#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class SymbolTable;
}

namespace memgraph::query::plan {

class LogicalOperator;

/// Symbols on which no two rows an operator produces can agree.
///
/// An empty key says the operator produces at most one row, since agreeing on nothing is agreeing.
/// Nothing is returned where no such set is known, which is the answer for any operator not
/// accounted for: a wrong key claims rows differ that do not, so only what has been established
/// gets one.
std::optional<std::vector<Symbol>> CandidateKeyOf(LogicalOperator const &op, SymbolTable const &symbol_table);

}  // namespace memgraph::query::plan
