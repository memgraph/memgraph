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

#include "query/frontend/ast/parse.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/parser.hpp"

namespace memgraph::query::frontend {

Query *ParseToAst(std::string const &query, ParsingContext context, Parameters *parameters, AstStorage &storage,
                  QueryInfo &info) {
  opencypher::Parser parser{query};

  // The tree is built somewhere of its own, so whatever the build leaves behind is left behind here.
  // Copying follows the edges the query reaches, which is what makes the caller's storage hold those
  // nodes and no others.
  AstStorage built;
  CypherMainVisitor visitor{context, &built, parameters};
  visitor.visit(parser.tree());

  info = visitor.GetQueryInfo();

  storage.labels_ = built.labels_;
  storage.edge_types_ = built.edge_types_;
  storage.properties_ = built.properties_;
  storage.user_functions_ = built.user_functions_;
  storage.call_procedures_ = built.call_procedures_;

  return storage.Copy(visitor.query());
}

}  // namespace memgraph::query::frontend
