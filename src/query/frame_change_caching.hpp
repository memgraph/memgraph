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

#pragma once

#include <set>

#include "query/dependant_symbol_visitor.hpp"
#include "query/frame_change.hpp"
#include "query/frontend/ast/ast.hpp"
#include "utils/frame_change_id.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query {

// Prepares caching for IN LIST and REGEX MATCH operations.
// Scans all IN LIST and REGEX MATCH operations in the AST storage and sets up caching for cacheable expressions.
// This function identifies InList operators that can benefit from caching (e.g., x IN range(1,100))
// and registers them with the FrameChangeCollector along with their dependencies for invalidation.
inline void PrepareCaching(const AstStorage &ast_storage, FrameChangeCollector *frame_change_collector) {
  if (!frame_change_collector) return;

  auto process_inlist = [&](InListOperator const &tree) {
    const auto cached_id = utils::GetFrameChangeId(tree);

    auto dependencies = std::set<Symbol::Position_t>{};
    auto visitor = DependantSymbolVisitor(dependencies);
    tree.expression2_->Accept(visitor);

    if (visitor.is_cacheable()) {
      // This InListOperator can be processed into a set and cached
      frame_change_collector->AddInListKey(cached_id);
      // If any dependency changes then the cache must be invalidated
      for (auto const symbol_pos : dependencies) {
        frame_change_collector->AddInvalidator(cached_id, symbol_pos);
      }
    }
  };

  auto process_regexMatch = [&](const RegexMatch &tree) {
    const auto cached_id = utils::GetFrameChangeId(tree);

    auto dependencies = std::set<Symbol::Position_t>{};
    auto visitor = DependantSymbolVisitor(dependencies);
    tree.regex_->Accept(visitor);

    if (visitor.is_cacheable()) {
      frame_change_collector->AddRegexKey(cached_id);
      for (auto const symbol_pos : dependencies) {
        frame_change_collector->AddInvalidator(cached_id, symbol_pos);
      }
    }
  };

  for (const auto &tree : ast_storage.storage_) {
    auto const type = tree->GetTypeInfo();
    if (type == InListOperator::kType) {
      process_inlist(static_cast<InListOperator &>(*tree));
    } else if (type == RegexMatch::kType) {
      process_regexMatch(static_cast<RegexMatch &>(*tree));
    }
  }
}

}  // namespace memgraph::query
