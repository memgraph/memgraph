// Copyright 2024 Memgraph Ltd.
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
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace impl {

template <class TDbAccessor>
class PeriodicDeleteRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  PeriodicDeleteRewriter(SymbolTable *symbolTable, AstStorage *astStorage, TDbAccessor *db)
      : symbol_table(symbolTable), ast_storage(astStorage), db(db) {}

  ~PeriodicDeleteRewriter() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &t) override { return true; }

  bool PreVisit(PeriodicCommit &op) override {
    commit_frequency_ = op.commit_frequency_;
    return true;
  }

  bool PreVisit(PeriodicSubquery &op) override {
    periodic_subquery_seen_ = true;
    return true;
  }

  bool PreVisit(Delete &op) override {
    // since periodic commit needs to flush deltas over time, buffer size needs to be put in delete
    // so it can know when to trigger delete of nodes and/or relationships
    if (commit_frequency_) {
      op.buffer_size_ = commit_frequency_;
    }
    if (periodic_subquery_seen_) {
      throw utils::NotYetImplemented(
          "Unfortunately, using DELETE with CALL IN TRANSACTIONS is currently not possible inside our query planner. "
          "Please use DELETE with the "
          "pre-query directive USING PERIODIC COMMIT.");
    }
    return true;
  }

 private:
  SymbolTable *symbol_table;
  AstStorage *ast_storage;
  TDbAccessor *db;
  Expression *commit_frequency_{nullptr};
  bool periodic_subquery_seen_{false};
  bool delete_seen_{false};
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewritePeriodicDelete(std::unique_ptr<LogicalOperator> root_op,
                                                       SymbolTable *symbol_table, AstStorage *ast_storage,
                                                       TDbAccessor *db) {
  auto rewriter = impl::PeriodicDeleteRewriter<TDbAccessor>{symbol_table, ast_storage, db};
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
