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
}

namespace memgraph::query::plan {

class LogicalOperator;

/// Narrows each Distinct to the columns that can separate rows, so that the
/// rest are neither copied nor hashed.
///
/// Left alone where the query writes: a property follows from the vertex it is
/// read from only while nothing assigns to it, and a query that sets one can
/// return the same vertex twice carrying two values.
std::unique_ptr<LogicalOperator> RewriteWithDistinctKey(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable const *symbol_table);

}  // namespace memgraph::query::plan
