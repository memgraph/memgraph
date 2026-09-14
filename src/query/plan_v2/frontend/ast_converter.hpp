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

#include <string>
#include <tuple>
#include <unordered_map>

#include "query/plan_v2/egraph/egraph.hpp"

namespace memgraph::query {
class CypherQuery;
class SymbolTable;
struct Parameters;
}  // namespace memgraph::query

namespace memgraph::query::plan::v2 {

/// Source text of each unaliased RETURN column, by token position. Stripping
/// replaces a column's expression with a placeholder, so this restores its
/// display name (e.g. `RETURN 1 + 2` -> "1 + 2") in the extracted plan.
using OutputColumnNames = std::unordered_map<int, std::string>;

/// Lower a parsed Cypher query to an e-graph. `parameters` carries the values of
/// stripped literals and user parameters by token position; plan_v2 does not
/// cache plans, so a ParameterLookup whose value is known is folded to a
/// constant, letting the analysis see the actual value (e.g. a list length).
/// `output_names` restores unaliased column display names lost to stripping.
auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table, Parameters const &parameters,
                     OutputColumnNames const &output_names = {}) -> std::tuple<egraph, eclass>;

}  // namespace memgraph::query::plan::v2
