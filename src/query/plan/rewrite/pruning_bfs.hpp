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

#include <memory>

namespace memgraph::query {
class SymbolTable;
class Parameters;
}  // namespace memgraph::query

namespace memgraph::query::plan {

class LogicalOperator;

/// `reads_parameters` is set when an expansion's shape was settled by reading a
/// parameter, which leaves the plan correct only for the parameters it was
/// planned with. The plan cache is keyed on the stripped query, where those
/// values do not appear, so such a plan must not be stored there. It is only
/// ever set, so one flag can span the candidate plans of a single query.
std::unique_ptr<LogicalOperator> RewriteWithPruningBFS(std::unique_ptr<LogicalOperator> root_op,
                                                       SymbolTable const *symbol_table, Parameters const &parameters,
                                                       bool *reads_parameters);

}  // namespace memgraph::query::plan
