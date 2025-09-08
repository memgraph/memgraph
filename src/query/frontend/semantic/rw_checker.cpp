// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/frontend/semantic/rw_checker.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"

namespace memgraph::query {

// CypherQuery

bool RWChecker::PreVisit(CypherQuery &cypher_query) {
  cypher_query.single_query_->Accept(*this);
  for (auto &cypher_union : cypher_query.cypher_unions_) {
    cypher_union->Accept(*this);
  }
  return true;
}

// Clauses

bool RWChecker::PreVisit(Create & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(CallProcedure &call_proc) {
  is_write_ |= call_proc.is_write_;
  return !is_write_;
}

bool RWChecker::PreVisit(SetProperty & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(SetProperties & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(RemoveProperty & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(SetLabels & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(RemoveLabels & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(Delete & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(Merge & /*unused*/) {
  is_write_ = true;
  return false;
}

bool RWChecker::PreVisit(Foreach & /*unused*/) {
  is_write_ = true;
  return false;
}

}  // namespace memgraph::query
