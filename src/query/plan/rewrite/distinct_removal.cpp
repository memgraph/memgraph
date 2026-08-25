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

#include "query/plan/rewrite/distinct_removal.hpp"

#include <algorithm>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/candidate_key.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"

namespace memgraph::query::plan {

namespace {

/// Whether the rows reaching the deduplication already differ somewhere within its key, which
/// leaves it nothing to remove.
bool KeepsEveryRowAnyway(Distinct const &distinct, SymbolTable const &symbol_table) {
  auto const key = CandidateKeyOf(*distinct.input_, symbol_table);
  if (!key) return false;
  return std::ranges::all_of(*key, [&](Symbol const &symbol) {
    return std::ranges::find(distinct.value_symbols_, symbol) != distinct.value_symbols_.end();
  });
}

}  // namespace

std::unique_ptr<LogicalOperator> RewriteWithDistinctRemoval(std::unique_ptr<LogicalOperator> root_op,
                                                            SymbolTable const *symbol_table, bool parallel_execution,
                                                            AstStorage *ast_storage) {
  // Several cursors then divide the rows between them, and a set of symbols separating the rows one
  // of them produces need not separate the rows of the plan.
  if (parallel_execution) return root_op;

  // What a row holds can change under a write, and with it whether two rows are the same row.
  auto writes = ReadWriteTypeChecker{};
  writes.InferRWType(*root_op);
  using RWType = ReadWriteTypeChecker::RWType;
  if (writes.type != RWType::R && writes.type != RWType::NONE) return root_op;

  if (auto const *distinct = utils::Downcast<Distinct>(root_op.get());
      distinct != nullptr && KeepsEveryRowAnyway(*distinct, *symbol_table)) {
    // The tree below is shared while a plan is owned outright, so what is kept is copied out.
    return distinct->input_->Clone(ast_storage);
  }

  // Ordering, skipping and limiting the rows leaves a deduplication below them, reached by walking
  // down for as long as each operator has the one input to walk into.
  auto *parent = root_op.get();
  while (parent->HasSingleInput() && parent->input()) {
    auto const input = parent->input();
    auto const *distinct = utils::Downcast<Distinct>(input.get());
    if (distinct != nullptr && KeepsEveryRowAnyway(*distinct, *symbol_table)) {
      parent->set_input(distinct->input_);
      continue;
    }
    parent = input.get();
  }
  return root_op;
}

}  // namespace memgraph::query::plan
