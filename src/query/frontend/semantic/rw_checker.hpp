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

#include "query/frontend/ast/ast_visitor.hpp"

namespace memgraph::query {

class Clause;

/// A clause that changes the graph: a write clause, MERGE, FOREACH or a write procedure.
bool IsWritingClause(const Clause &clause);

/// Visits the AST and generates symbols for variables.
///
/// During the process of symbol generation, simple semantic checks are
/// performed. Such as, redeclaring a variable or conflicting expectations of
/// variable types.
class RWChecker : public HierarchicalTreeVisitor {
 public:
  RWChecker() = default;

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using typename HierarchicalTreeVisitor::ReturnType;

  // CypherQuery
  bool PreVisit(CypherQuery &cypher_query) override;

  bool PreVisit(SingleQuery &single_query) override;

  ReturnType Visit(Identifier & /*unused*/) override { return true; }

  ReturnType Visit(PrimitiveLiteral & /*unused*/) override { return true; }

  ReturnType Visit(ParameterLookup & /*unused*/) override { return true; }

  ReturnType Visit(EnumValueAccess & /*unused*/) override { return true; }

  bool IsWrite() const { return is_write_; }

 private:
  bool is_write_{false};
};

}  // namespace memgraph::query
