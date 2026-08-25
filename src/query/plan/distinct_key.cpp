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

#include "query/plan/distinct_key.hpp"

#include <algorithm>
#include <optional>
#include <unordered_set>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

namespace {

using SymbolPositions = std::unordered_set<int64_t>;

/// The symbols an expression's value follows from, or nullopt where the shape is
/// one this cannot account for. An expression reading nothing is constant, and
/// so follows from the empty set.
///
/// Shapes are recognised by naming them rather than by naming what to leave out,
/// so an expression this has never seen is kept rather than dropped.
std::optional<SymbolPositions> SettledBy(Expression *expression, SymbolTable const &symbol_table) {
  if (auto *identifier = utils::Downcast<Identifier>(expression)) {
    return SymbolPositions{symbol_table.at(*identifier).position()};
  }
  if (auto *lookup = utils::Downcast<PropertyLookup>(expression)) {
    return SettledBy(lookup->expression_, symbol_table);
  }
  if (utils::Downcast<PrimitiveLiteral>(expression) != nullptr) {
    return SymbolPositions{};
  }
  return std::nullopt;
}

}  // namespace

std::vector<Symbol> ReducedDistinctKey(std::vector<NamedExpression *> const &projections,
                                       SymbolTable const &symbol_table) {
  // A column reading a symbol outright is what puts that symbol in the key, and
  // so is what lets the columns reading it be dropped.
  SymbolPositions held;
  for (auto *projection : projections) {
    if (auto *identifier = utils::Downcast<Identifier>(projection->expression_)) {
      held.insert(symbol_table.at(*identifier).position());
    }
  }

  SymbolPositions already_held_by_a_kept_column;
  std::vector<Symbol> kept;
  kept.reserve(projections.size());

  for (auto *projection : projections) {
    auto const settled_by = SettledBy(projection->expression_, symbol_table);
    if (!settled_by) {
      kept.push_back(symbol_table.at(*projection));
      continue;
    }
    if (auto *identifier = utils::Downcast<Identifier>(projection->expression_)) {
      // The first column to hold a symbol keeps it in the key; a second reading
      // the same symbol adds nothing over the first.
      if (already_held_by_a_kept_column.insert(symbol_table.at(*identifier).position()).second) {
        kept.push_back(symbol_table.at(*projection));
      }
      continue;
    }
    if (!std::ranges::all_of(*settled_by, [&](auto position) { return held.contains(position); })) {
      kept.push_back(symbol_table.at(*projection));
    }
  }

  // Deduplicating on nothing is not deduplicating, so a key of constants alone
  // keeps one of them.
  if (kept.empty() && !projections.empty()) kept.push_back(symbol_table.at(*projections.front()));
  return kept;
}

}  // namespace memgraph::query::plan
