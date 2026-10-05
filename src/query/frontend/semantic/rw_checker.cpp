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

#include "query/frontend/semantic/rw_checker.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query {

bool IsWritingClause(const Clause &clause) {
  if (const auto *call_proc = utils::Downcast<const CallProcedure>(&clause)) {
    return call_proc->graph_access_ == GraphAccess::Write;
  }
  return utils::Downcast<const Create>(&clause) || utils::Downcast<const Delete>(&clause) ||
         utils::Downcast<const SetProperty>(&clause) || utils::Downcast<const SetProperties>(&clause) ||
         utils::Downcast<const SetLabels>(&clause) || utils::Downcast<const RemoveProperty>(&clause) ||
         utils::Downcast<const RemoveLabels>(&clause) || utils::Downcast<const Merge>(&clause) ||
         utils::Downcast<const Foreach>(&clause);
}

bool RWChecker::PreVisit(SingleQuery &single_query) {
  for (auto *clause : single_query.clauses_) {
    if (IsWritingClause(*clause)) {
      is_write_ = true;
      return false;
    }
    // A read clause can still hold a write, e.g. a CALL subquery body.
    clause->Accept(*this);
  }
  return false;
}

}  // namespace memgraph::query
