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
class AstStorage;
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

class LogicalOperator;

/// Drops a deduplicating operator whose input already produces each of its rows once.
///
/// Set `parallel_execution` where the plan will be run by several cursors at once, which leaves the
/// rows one of them produces no longer the rows the plan produces.
std::unique_ptr<LogicalOperator> RewriteWithDistinctRemoval(std::unique_ptr<LogicalOperator> root_op,
                                                            SymbolTable const *symbol_table, bool parallel_execution,
                                                            AstStorage *ast_storage);

}  // namespace memgraph::query::plan
