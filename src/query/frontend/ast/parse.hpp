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

#include "query/frontend/ast/cypher_main_visitor.hpp"

namespace memgraph::query {
class AstStorage;
class Query;
}  // namespace memgraph::query

namespace memgraph::query::frontend {

using QueryInfo = CypherMainVisitor::QueryInfo;

/// Parses `query` into `storage`, which receives the nodes the query reaches and no others: building
/// a tree can leave one behind that nothing reads, and a cache holds what it is given for the life of
/// the entry. The root returned is owned by `storage`. Throws if the query does not parse, or parses
/// and asks for something the frontend rejects.
Query *ParseToAst(std::string const &query, ParsingContext context, Parameters *parameters, AstStorage &storage,
                  QueryInfo &info);

}  // namespace memgraph::query::frontend
