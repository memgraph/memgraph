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

#include "query/frontend/ast/query/label_term.hpp"

#include <algorithm>
#include <iterator>
#include <optional>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::query {

LabelTerm LabelTerm::Clone(AstStorage *storage) const {
  auto clone_all = [&](const std::vector<LabelTerm> &operands) {
    std::vector<LabelTerm> clones;
    clones.reserve(operands.size());
    for (const auto &operand : operands) clones.push_back(operand.Clone(storage));
    return clones;
  };
  return std::visit(utils::Overloaded{
                        [&](const Label &leaf) { return LabelTerm{Label{storage->GetLabelIx(leaf.label.name)}}; },
                        [&](const Dynamic &leaf) { return LabelTerm{Dynamic{leaf.expression->Clone(storage)}}; },
                        [](const Wildcard &) { return LabelTerm{Wildcard{}}; },
                        [&](const And &conjunction) { return LabelTerm{And{clone_all(conjunction.operands)}}; },
                        [&](const Or &disjunction) { return LabelTerm{Or{clone_all(disjunction.operands)}}; },
                        [&](const Not &negation) { return LabelTerm{Not{negation.operand->Clone(storage)}}; },
                    },
                    node);
}

std::optional<std::vector<QueryLabelType>> LabelTerm::Conjunction() const {
  auto leaf = [](const LabelTerm &term) -> std::optional<QueryLabelType> {
    if (const auto *label = term.As<Label>()) return label->label;
    if (const auto *dynamic = term.As<Dynamic>()) return dynamic->expression;
    return std::nullopt;
  };
  if (auto single = leaf(*this)) return std::vector{*single};
  const auto *conjunction = As<And>();
  if (!conjunction) return std::nullopt;
  std::vector<QueryLabelType> labels;
  labels.reserve(conjunction->operands.size());
  for (const auto &operand : conjunction->operands) {
    auto label = leaf(operand);
    if (!label) return std::nullopt;
    labels.push_back(*label);
  }
  return labels;
}

namespace {

bool IsLabel(const LabelTerm &term) { return term.As<LabelTerm::Label>() != nullptr; }

/// A label, or a disjunction of labels: the conjuncts index selection can use.
bool IsLabelChoice(const LabelTerm &term) {
  if (IsLabel(term)) return true;
  const auto *disjunction = term.As<LabelTerm::Or>();
  return disjunction && !disjunction->operands.empty() && std::ranges::all_of(disjunction->operands, IsLabel);
}

/// `term` with `!!` dropped and nested `&` flattened along its conjunction. Nothing under a single `!` or a `|`
/// changes, as filter collection never looked there. Sets `changed` if anything did.
LabelTerm Normalise(LabelTerm term, bool &changed) {
  while (auto *negation = term.As<LabelTerm::Not>()) {
    auto *double_negation = negation->operand->As<LabelTerm::Not>();
    if (!double_negation) break;
    LabelTerm inner = std::move(*double_negation->operand);
    term = std::move(inner);
    changed = true;
  }
  auto *conjunction = term.As<LabelTerm::And>();
  if (!conjunction) return term;

  std::vector<LabelTerm> conjuncts;
  for (auto &operand : conjunction->operands) {
    auto normal = Normalise(std::move(operand), changed);
    auto *nested = normal.As<LabelTerm::And>();
    if (!nested) {
      conjuncts.push_back(std::move(normal));
      continue;
    }
    changed = true;
    std::ranges::move(nested->operands, std::back_inserter(conjuncts));
  }
  return LabelTerm{LabelTerm::And{std::move(conjuncts)}};
}

}  // namespace

LabelsTest *LabelsTest::Make(AstStorage &storage, Expression *subject, LabelTerm term) {
  if (auto labels = term.Conjunction()) return storage.Create<LabelsTest>(subject, *labels);
  if (IsLabelChoice(term)) {
    const auto &operands = term.As<LabelTerm::Or>()->operands;
    auto labels = std::vector<LabelIx>{};
    labels.reserve(operands.size());
    for (const auto &operand : operands) {
      // A repeated label adds nothing to the choice, but index selection would scan it once per copy.
      const auto &label = operand.As<LabelTerm::Label>()->label;
      if (!std::ranges::contains(labels, label)) labels.push_back(label);
    }
    // A choice of one label is that label, which the node must then carry outright.
    const bool or_group = labels.size() > 1U;
    return storage.Create<LabelsTest>(subject, std::move(labels), or_group);
  }
  return storage.Create<LabelsTest>(subject, std::move(term));
}

std::vector<LabelsTest *> LabelsTest::Split(AstStorage &storage, const LabelsTest &test) {
  // Only an identifier may be copied per piece: any other subject would be evaluated once per piece.
  const auto *whole = test.Term();
  if (!whole || !utils::Downcast<Identifier>(test.expression_)) return {};
  bool changed = false;
  auto term = Normalise(*whole, changed);
  auto *conjunction = term.As<LabelTerm::And>();
  if (!conjunction || std::ranges::none_of(conjunction->operands, IsLabelChoice)) {
    // Normalising alone can still leave a label, as of `!!A`, or fewer operators to test.
    if (!changed) return {};
    return {Make(storage, test.expression_->Clone(&storage), std::move(term))};
  }

  std::vector<LabelsTest *> pieces;
  std::vector<LabelTerm> rest;
  for (auto &conjunct : conjunction->operands) {
    if (IsLabelChoice(conjunct)) {
      pieces.push_back(Make(storage, test.expression_->Clone(&storage), std::move(conjunct)));
    } else {
      rest.push_back(std::move(conjunct));
    }
  }
  // The rest stay one test, so the subject is read once for all of them.
  if (!rest.empty()) {
    auto rest_term = rest.size() == 1U ? std::move(rest.front()) : LabelTerm{LabelTerm::And{std::move(rest)}};
    pieces.push_back(Make(storage, test.expression_->Clone(&storage), std::move(rest_term)));
  }
  DMG_ASSERT(std::ranges::all_of(pieces, [&](const LabelsTest *piece) { return Split(storage, *piece).empty(); }),
             "A piece of a split labels test splits again");
  return pieces;
}

}  // namespace memgraph::query
