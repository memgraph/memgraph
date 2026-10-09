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

#include "query/frontend/ast/projected_item_matcher.hpp"

#include <algorithm>
#include <optional>
#include <ranges>
#include <vector>

#include "query/frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/binary_operator.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/ast/query/named_expression.hpp"
#include "query/frontend/ast/query/where.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::frontend {
namespace {

class AggregationFinder : public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  bool PreVisit(Aggregation & /*aggregation*/) override {
    found_ = true;
    return false;
  }

  bool PreVisit(SubqueryExpression & /*subquery*/) override { return false; }

  bool PreVisit(PatternComprehension & /*comprehension*/) override { return false; }

  bool Visit(Identifier & /*identifier*/) override { return true; }

  bool Visit(PrimitiveLiteral & /*literal*/) override { return true; }

  bool Visit(ParameterLookup & /*lookup*/) override { return true; }

  bool Visit(EnumValueAccess & /*access*/) override { return true; }

  bool found_{false};
};

bool ProjectsAggregation(ReturnBody &body) {
  AggregationFinder finder;
  for (auto *item : body.named_expressions) {
    item->Accept(finder);
  }
  return finder.found_;
}

using ParameterNames = std::unordered_map<int32_t, std::string>;

using MapElements = std::unordered_map<PropertyIx, Expression *>;

// The values of a map ordered by their keys, so that two maps written with the same keys in a different order still
// pair each value with the one it should be compared against.
std::vector<Expression **> ValuesByKey(MapElements &elements) {
  auto entries = std::vector<MapElements::value_type *>{};
  entries.reserve(elements.size());
  for (auto &entry : elements) entries.push_back(&entry);
  std::ranges::sort(entries, {}, [](auto const *entry) -> std::string const & { return entry->first.name; });
  return entries | std::views::transform([](auto *entry) { return &entry->second; }) | std::ranges::to<std::vector>();
}

// Whether two label expressions test the same thing. Operands are compared in the order written, so a conjunction
// and its reordering are two different tests here and one of them loses a match it could have had.
bool SameLabelTerm(LabelTerm const &lhs, LabelTerm const &rhs) {
  if (lhs.node.index() != rhs.node.index()) return false;
  if (auto const *label = lhs.As<LabelTerm::Label>()) return label->label == rhs.As<LabelTerm::Label>()->label;
  if (lhs.As<LabelTerm::Wildcard>()) return true;
  if (auto const *conjunction = lhs.As<LabelTerm::And>()) {
    return std::ranges::equal(conjunction->operands, rhs.As<LabelTerm::And>()->operands, SameLabelTerm);
  }
  if (auto const *disjunction = lhs.As<LabelTerm::Or>()) {
    return std::ranges::equal(disjunction->operands, rhs.As<LabelTerm::Or>()->operands, SameLabelTerm);
  }
  if (auto const *negation = lhs.As<LabelTerm::Not>()) {
    return SameLabelTerm(*negation->operand, *rhs.As<LabelTerm::Not>()->operand);
  }
  // What remains is a label named by an expression, which this does not compare.
  return false;
}

bool SameKeys(MapElements const &lhs, MapElements const &rhs) {
  return lhs.size() == rhs.size() &&
         std::ranges::all_of(lhs, [&](auto const &entry) { return rhs.contains(entry.first); });
}

// The child slots of the expression kinds that can match a projected item. Any other kind never matches.
std::optional<std::vector<Expression **>> MatchableChildren(Expression *expr) {
  auto addresses = [](std::vector<Expression *> &exprs) {
    return exprs | std::views::transform([](auto &child) { return &child; }) | std::ranges::to<std::vector>();
  };
  // An aggregation is matchable, and is the case the whole rewrite exists for: ORDER BY count(n) repeats a projected
  // count(n). It is named ahead of BinaryOperator, which it derives from and would otherwise be matched as.
  if (auto *agg = utils::Downcast<Aggregation>(expr)) return std::vector{&agg->expression1_, &agg->expression2_};
  if (auto *op = utils::Downcast<BinaryOperator>(expr)) return std::vector{&op->expression1_, &op->expression2_};
  if (auto *op = utils::Downcast<UnaryOperator>(expr)) return std::vector{&op->expression_};
  if (auto *lookup = utils::Downcast<PropertyLookup>(expr)) return std::vector{&lookup->expression_};
  if (auto *test = utils::Downcast<LabelsTest>(expr)) return std::vector{&test->expression_};
  if (auto *op = utils::Downcast<IfOperator>(expr)) {
    return std::vector{&op->condition_, &op->then_expression_, &op->else_expression_};
  }
  if (auto *op = utils::Downcast<ListSlicingOperator>(expr)) {
    return std::vector{&op->list_, &op->lower_bound_, &op->upper_bound_};
  }
  if (auto *match = utils::Downcast<RegexMatch>(expr)) return std::vector{&match->string_expr_, &match->regex_};
  if (auto *lookup = utils::Downcast<AllPropertiesLookup>(expr)) return std::vector{&lookup->expression_};
  if (auto *map = utils::Downcast<MapLiteral>(expr)) return ValuesByKey(map->elements_);
  if (auto *projection = utils::Downcast<MapProjectionLiteral>(expr)) {
    auto children = ValuesByKey(projection->elements_);
    children.push_back(&projection->map_variable_);
    return children;
  }
  if (auto *function = utils::Downcast<Function>(expr)) return addresses(function->arguments_);
  if (auto *coalesce = utils::Downcast<Coalesce>(expr)) return addresses(coalesce->expressions_);
  if (auto *list = utils::Downcast<ListLiteral>(expr)) return addresses(list->elements_);
  if (utils::IsSubtype(*expr, Identifier::kType) || utils::IsSubtype(*expr, PrimitiveLiteral::kType) ||
      utils::IsSubtype(*expr, ParameterLookup::kType) || utils::IsSubtype(*expr, EnumValueAccess::kType)) {
    return std::vector<Expression **>{};
  }
  return std::nullopt;
}

// Compares two expressions by the state they hold other than their children. A literal the stripper replaced is a
// ParameterLookup and matches by parameter name, which is distinct per literal, so two literals of different value
// never match. A literal the stripper left in place matches by value.
bool SameOwnFields(Expression &lhs, Expression &rhs, ParameterNames const &parameter_names) {
  // Two kinds that differ hold no common state to compare. The check also stands behind the stateless list at the
  // end, which reads the left side only.
  if (lhs.GetTypeInfo() != rhs.GetTypeInfo()) return false;

  if (auto *l = utils::Downcast<Aggregation>(&lhs), *r = utils::Downcast<Aggregation>(&rhs); l && r) {
    return l->op_ == r->op_ && l->distinct_ == r->distinct_;
  }
  if (auto *l = utils::Downcast<Identifier>(&lhs), *r = utils::Downcast<Identifier>(&rhs); l && r) {
    return l->name_ == r->name_;
  }
  if (auto *l = utils::Downcast<PrimitiveLiteral>(&lhs), *r = utils::Downcast<PrimitiveLiteral>(&rhs); l && r) {
    return l->value_ == r->value_;
  }
  if (auto *l = utils::Downcast<PropertyLookup>(&lhs), *r = utils::Downcast<PropertyLookup>(&rhs); l && r) {
    return l->property_path_ == r->property_path_;
  }
  if (auto *l = utils::Downcast<LabelsTest>(&lhs), *r = utils::Downcast<LabelsTest>(&rhs); l && r) {
    if (auto const *lhs_cnf = l->Cnf(), *rhs_cnf = r->Cnf(); lhs_cnf && rhs_cnf) return *lhs_cnf == *rhs_cnf;
    auto const *lhs_term = l->Term();
    auto const *rhs_term = r->Term();
    return lhs_term && rhs_term && SameLabelTerm(*lhs_term, *rhs_term);
  }
  if (auto *l = utils::Downcast<MapLiteral>(&lhs), *r = utils::Downcast<MapLiteral>(&rhs); l && r) {
    return SameKeys(l->elements_, r->elements_);
  }
  if (auto *l = utils::Downcast<MapProjectionLiteral>(&lhs), *r = utils::Downcast<MapProjectionLiteral>(&rhs); l && r) {
    return SameKeys(l->elements_, r->elements_);
  }
  if (auto *l = utils::Downcast<EnumValueAccess>(&lhs), *r = utils::Downcast<EnumValueAccess>(&rhs); l && r) {
    return l->enum_name_ == r->enum_name_ && l->enum_value_ == r->enum_value_;
  }
  if (auto *l = utils::Downcast<Function>(&lhs), *r = utils::Downcast<Function>(&rhs); l && r) {
    return l->function_name_ == r->function_name_ && IsFunctionPure(l->function_name_);
  }
  if (auto *l = utils::Downcast<ParameterLookup>(&lhs), *r = utils::Downcast<ParameterLookup>(&rhs); l && r) {
    auto const lhs_name = parameter_names.find(l->token_position_);
    auto const rhs_name = parameter_names.find(r->token_position_);
    return lhs_name != parameter_names.end() && rhs_name != parameter_names.end() &&
           lhs_name->second == rhs_name->second;
  }
  // The kinds whose whole state is their children, so two of the same kind match once their children do. Naming the
  // concrete kinds rather than BinaryOperator and UnaryOperator is what keeps that true: a new operator derived from
  // either is absent from here and loses a match, where a test against the base would match it while a field of its
  // own differed.
  static auto const kStateless =
      std::array{&OrOperator::kType,        &XorOperator::kType,         &AndOperator::kType,
                 &AdditionOperator::kType,  &SubtractionOperator::kType, &MultiplicationOperator::kType,
                 &DivisionOperator::kType,  &ModOperator::kType,         &ExponentiationOperator::kType,
                 &NotEqualOperator::kType,  &EqualOperator::kType,       &LessOperator::kType,
                 &GreaterOperator::kType,   &LessEqualOperator::kType,   &GreaterEqualOperator::kType,
                 &InListOperator::kType,    &SubscriptOperator::kType,   &NotOperator::kType,
                 &UnaryPlusOperator::kType, &UnaryMinusOperator::kType,  &IsNullOperator::kType,
                 &IfOperator::kType,        &ListSlicingOperator::kType, &Coalesce::kType,
                 &ListLiteral::kType,       &RegexMatch::kType,          &AllPropertiesLookup::kType};
  return std::ranges::any_of(kStateless, [&](auto const *kind) { return *kind == lhs.GetTypeInfo(); });
}

// An identifier named like a projected item refers to that item, not to the variable the item was computed from.
bool IsShadowed(Expression *expr, std::vector<NamedExpression *> const &items) {
  auto const *identifier = utils::Downcast<Identifier>(expr);
  return identifier && std::ranges::contains(items, identifier->name_, &NamedExpression::name_);
}

bool AreEquivalent(Expression *lhs, Expression *rhs, std::vector<NamedExpression *> const &items,
                   ParameterNames const &parameter_names) {
  if (!lhs || !rhs) return lhs == rhs;
  // SameOwnFields refuses two kinds that differ, so the children compared below belong to the one kind both have.
  if (!SameOwnFields(*lhs, *rhs, parameter_names) || IsShadowed(lhs, items)) return false;
  auto const lhs_children = MatchableChildren(lhs);
  auto const rhs_children = MatchableChildren(rhs);
  return lhs_children && rhs_children && std::ranges::equal(*lhs_children, *rhs_children, [&](auto *l, auto *r) {
           return AreEquivalent(*l, *r, items, parameter_names);
         });
}

// The storage owns every node the parser built, including the ones a replacement stopped the tree from reaching.
// Those keep the identifiers they were built with, which symbol generation only ever reaches through the tree, so
// anything reading the storage rather than walking it would meet an identifier carrying no symbol.
void DropDetached(AstStorage &storage, std::vector<Expression *> const &roots) {
  if (roots.empty()) return;
  auto doomed = std::vector<Tree const *>{};
  auto collect = [&doomed](auto const &self, Expression *expr) -> void {
    if (!expr) return;
    doomed.push_back(expr);
    // A subtree is only ever replaced once it has matched, so every kind in it is one of the kinds named here. A
    // kind that under-reports its children strands the rest, where the identifier with no symbol is still caught.
    for (auto *child : MatchableChildren(expr).value_or(std::vector<Expression **>{})) self(self, *child);
  };
  for (auto *root : roots) collect(collect, root);
  std::ranges::sort(doomed);
  std::erase_if(storage.storage_, [&doomed](auto const &node) {
    return std::ranges::binary_search(doomed, static_cast<Tree const *>(node.get()));
  });
}

void ReferToProjectedItems(Expression *&expr, std::vector<NamedExpression *> const &items,
                           ParameterNames const &parameter_names, AstStorage &storage,
                           std::vector<Expression *> &detached) {
  if (!expr) return;
  auto const item = std::ranges::find_if(
      items, [&](auto *item) { return AreEquivalent(item->expression_, expr, items, parameter_names); });
  if (item != items.end()) {
    detached.push_back(expr);
    expr = storage.Create<Identifier>((*item)->name_);
    return;
  }
  // An aggregation that repeats no projected item is rejected once the symbols are generated, so rewriting within its
  // arguments would only detach a subtree that nothing goes on to read.
  if (utils::IsSubtype(*expr, Aggregation::kType)) return;
  for (auto *child : MatchableChildren(expr).value_or(std::vector<Expression **>{})) {
    ReferToProjectedItems(*child, items, parameter_names, storage, detached);
  }
}

}  // namespace

void ReferToProjectedItems(ReturnBody &body, Where *where, ParameterNames const &parameter_names, AstStorage &storage) {
  if ((body.order_by.empty() && !where) || !ProjectsAggregation(body)) return;
  auto detached = std::vector<Expression *>{};
  for (auto &sort_item : body.order_by) {
    ReferToProjectedItems(sort_item.expression, body.named_expressions, parameter_names, storage, detached);
  }
  if (where) ReferToProjectedItems(where->expression_, body.named_expressions, parameter_names, storage, detached);
  DropDetached(storage, detached);
}

}  // namespace memgraph::query::frontend
