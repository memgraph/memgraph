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
#include <set>
#include <utility>
#include <vector>

#include "query/frontend/semantic/symbol.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::query {
class SymbolTable;
}

namespace memgraph::query::plan {

class LogicalOperator;

/// Sets of properties no two vertices of a label may hold the same values for, as the storage
/// reports them.
using UniqueConstraints = std::vector<std::pair<storage::LabelId, std::set<storage::PropertyId>>>;

/// Symbols on which no two rows an operator produces can agree.
///
/// An empty key says the operator produces at most one row, since agreeing on nothing is agreeing.
/// Nothing is returned where no such set is known, which is the answer for any operator not
/// accounted for: a wrong key claims rows differ that do not, so only what has been established
/// gets one.
std::optional<std::vector<Symbol>> CandidateKeyOf(LogicalOperator const &op, SymbolTable const &symbol_table,
                                                  UniqueConstraints const &unique_constraints);

}  // namespace memgraph::query::plan
