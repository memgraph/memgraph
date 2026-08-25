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

#include <vector>

#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class NamedExpression;
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

/// The columns a Distinct has to keep, out of the projections it deduplicates.
///
/// A column whose value follows from other columns the key holds cannot
/// separate two rows the rest of the key does not, so deduplicating without it
/// gives the same rows for less copying and hashing. A property follows from the
/// vertex or edge it is read from, so `RETURN DISTINCT n.id, n` deduplicates on
/// `n` alone.
///
/// A column is only accounted for where its value is settled by what it reads,
/// which is read off the shape of the expression rather than assumed, so a
/// function whose result varies between calls is kept. The result keeps at least
/// one column, because deduplicating on nothing is not deduplicating.
std::vector<Symbol> ReducedDistinctKey(std::vector<NamedExpression *> const &projections,
                                       SymbolTable const &symbol_table);

}  // namespace memgraph::query::plan
