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

/// What parsing established about a query beyond its tree.
using QueryInfo = CypherMainVisitor::QueryInfo;

/// Parses `query` and puts the result in `storage`, which receives the nodes the query reaches and no
/// others. Building a tree can leave a node behind that nothing goes on to read, and anything reading
/// the storage rather than walking the tree would otherwise meet it; a storage this returns into has
/// none, and goes on holding none for as long as a cache keeps it.
///
/// Returns the root, which `storage` owns. Throws for a query that does not parse, and for one that
/// parses but asks for something the frontend rejects.
Query *ParseToAst(std::string const &query, ParsingContext context, Parameters *parameters, AstStorage &storage,
                  QueryInfo &info);

}  // namespace memgraph::query::frontend
