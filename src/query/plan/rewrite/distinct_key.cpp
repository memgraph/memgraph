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

#include "query/plan/rewrite/distinct_key.hpp"

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/distinct_key.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"

namespace memgraph::query::plan {

namespace {

class DistinctKeyRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  explicit DistinctKeyRewriter(SymbolTable const &symbol_table) : symbol_table_(symbol_table) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PreVisit(Distinct &op) override {
    // The columns are only known where the projection right below produced them.
    auto *produce = dynamic_cast<Produce *>(op.input_.get());
    if (!produce) return true;
    if (produce->OutputSymbols(symbol_table_) != op.value_symbols_) return true;

    op.value_symbols_ = ReducedDistinctKey(produce->named_expressions_, symbol_table_);
    return true;
  }

 private:
  SymbolTable const &symbol_table_;
};

}  // namespace

std::unique_ptr<LogicalOperator> RewriteWithDistinctKey(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable const *symbol_table) {
  // A property follows from the vertex it is read off only while nothing writes it. Where the query
  // writes, one vertex can answer the same lookup differently from one row to the next, and a column
  // dropped as redundant would have separated rows after all.
  auto writes = ReadWriteTypeChecker{};
  writes.InferRWType(*root_op);
  using RWType = ReadWriteTypeChecker::RWType;
  if (writes.type != RWType::R && writes.type != RWType::NONE) return root_op;

  auto rewriter = DistinctKeyRewriter(*symbol_table);
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
