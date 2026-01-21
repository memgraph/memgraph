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

#include <tuple>

#include "query/plan_v2/egraph.hpp"

namespace memgraph::query {
class CypherQuery;
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan::v2 {

auto ConvertToEgraph(CypherQuery const &query, SymbolTable const &symbol_table) -> std::tuple<egraph, eclass>;

}  // namespace memgraph::query::plan::v2
