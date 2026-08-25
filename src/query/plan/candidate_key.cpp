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

#include "query/plan/candidate_key.hpp"

#include <algorithm>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace {

using Key = std::vector<Symbol>;

void Add(Key &key, Symbol const &symbol) {
  if (std::ranges::find(key, symbol) == key.end()) key.push_back(symbol);
}

/// Scans of vertices, each of which reaches a vertex at most once per row it is given.
///
/// Named one by one rather than by their common base, which edge scans share while binding symbols
/// beyond the one it carries.
bool ReachesEachVertexOnce(utils::TypeInfo const &type) {
  return type == ScanAll::kType || type == ScanAllByLabel::kType || type == ScanAllByLabelProperties::kType ||
         type == ScanAllById::kType || type == ScanAllByVertexProperty::kType ||
         type == ScanAllByPointDistance::kType || type == ScanAllByPointWithinbbox::kType;
}

/// Operators passing on a subset of the rows they are given, in some order, binding nothing.
bool PassesRowsOn(utils::TypeInfo const &type) {
  return type == Filter::kType || type == Skip::kType || type == Limit::kType || type == OrderBy::kType ||
         type == EdgeUniquenessFilter::kType || type == ConstructNamedPath::kType || type == Accumulate::kType;
}

/// The key a projection leaves behind, which holds only the columns returning a symbol of the key
/// outright. Nothing is returned where a symbol reaches the output through an expression, or not at
/// all, since the rows can then agree on every column while the symbol differs.
std::optional<Key> ThroughProjection(Key const &key, Produce const &produce, SymbolTable const &symbol_table) {
  Key projected;
  for (auto const &symbol : key) {
    auto const carrying = std::ranges::find_if(produce.named_expressions_, [&](NamedExpression *projection) {
      auto *identifier = utils::Downcast<Identifier>(projection->expression_);
      return identifier != nullptr && symbol_table.at(*identifier) == symbol;
    });
    if (carrying == produce.named_expressions_.end()) return std::nullopt;
    Add(projected, symbol_table.at(**carrying));
  }
  return projected;
}

}  // namespace

std::optional<Key> CandidateKeyOf(LogicalOperator const &op, SymbolTable const &symbol_table) {
  auto const &type = op.GetTypeInfo();

  // One row, so there is no second row to agree with it.
  if (type == Once::kType) return Key{};

  if (ReachesEachVertexOnce(type)) {
    auto const &scan = static_cast<ScanAll const &>(op);
    auto key = CandidateKeyOf(*scan.input(), symbol_table);
    if (!key) return std::nullopt;
    Add(*key, scan.output_symbol_);
    return key;
  }

  // A walk of one hop takes each edge leaving a row once, so rows differing nowhere else differ in
  // the edge walked. Two edges can join one pair of vertices, which is why the edge earns this and
  // the vertex reached does not.
  if (type == Expand::kType) {
    auto const &expand = static_cast<Expand const &>(op);
    auto key = CandidateKeyOf(*expand.input(), symbol_table);
    if (!key) return std::nullopt;
    Add(*key, expand.common_.edge_symbol);
    return key;
  }

  if (PassesRowsOn(type)) return CandidateKeyOf(*op.input(), symbol_table);

  if (type == Produce::kType) {
    auto const &produce = static_cast<Produce const &>(op);
    auto const key = CandidateKeyOf(*produce.input(), symbol_table);
    if (!key) return std::nullopt;
    return ThroughProjection(*key, produce, symbol_table);
  }

  // Deduplicating is what makes its key one.
  if (type == Distinct::kType) return static_cast<Distinct const &>(op).value_symbols_;

  // One row per group of values grouped on, and the symbols remembered alongside follow from the
  // group, so the grouped symbols alone separate the rows.
  if (type == Aggregate::kType) {
    auto const &aggregate = static_cast<Aggregate const &>(op);
    if (!CandidateKeyOf(*aggregate.input(), symbol_table)) return std::nullopt;
    Key key;
    for (auto *expression : aggregate.group_by_) {
      auto *identifier = utils::Downcast<Identifier>(expression);
      if (identifier == nullptr) return std::nullopt;
      Add(key, symbol_table.at(*identifier));
    }
    return key;
  }

  // Every row on one side meets every row on the other, so a pair of rows agreeing on both sides'
  // keys agrees on neither side alone.
  if (type == Cartesian::kType) {
    auto const &cartesian = static_cast<Cartesian const &>(op);
    auto left = CandidateKeyOf(*cartesian.left_op_, symbol_table);
    auto const right = CandidateKeyOf(*cartesian.right_op_, symbol_table);
    if (!left || !right) return std::nullopt;
    for (auto const &symbol : *right) Add(*left, symbol);
    return left;
  }

  return std::nullopt;
}

}  // namespace memgraph::query::plan
