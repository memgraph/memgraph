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
#include <array>
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

// One expression kind's whole contribution to matching: the child slots to pair up, and how whatever state it holds
// besides those children compares. A kind with no row matches nothing, which is what makes a kind nobody has
// considered safe rather than silently wrong. Rows name concrete kinds rather than base classes, so an operator
// added later loses a match it could have had instead of being matched while a field of its own differs.
struct KindRules {
  utils::TypeInfo const *kind;
  std::vector<Expression **> (*children)(Expression &);
  bool (*same_own_fields)(Expression &, Expression &, ParameterNames const &);
};

std::vector<Expression **> NoChildren(Expression & /*expr*/) { return {}; }

std::vector<Expression **> BinaryChildren(Expression &expr) {
  auto &op = static_cast<BinaryOperator &>(expr);
  return {&op.expression1_, &op.expression2_};
}

// Every unary operator, a property lookup, a label test and an all-properties lookup each read one expression, and
// each names it the same.
template <typename TKind>
std::vector<Expression **> OneChild(Expression &expr) {
  return {&static_cast<TKind &>(expr).expression_};
}

template <typename TKind, auto TMember>
std::vector<Expression **> ChildList(Expression &expr) {
  auto &children = static_cast<TKind &>(expr).*TMember;
  return children | std::views::transform([](auto &child) { return &child; }) | std::ranges::to<std::vector>();
}

std::vector<Expression **> IfChildren(Expression &expr) {
  auto &op = static_cast<IfOperator &>(expr);
  return {&op.condition_, &op.then_expression_, &op.else_expression_};
}

std::vector<Expression **> SliceChildren(Expression &expr) {
  auto &op = static_cast<ListSlicingOperator &>(expr);
  return {&op.list_, &op.lower_bound_, &op.upper_bound_};
}

std::vector<Expression **> RegexChildren(Expression &expr) {
  auto &match = static_cast<RegexMatch &>(expr);
  return {&match.string_expr_, &match.regex_};
}

std::vector<Expression **> MapChildren(Expression &expr) {
  return ValuesByKey(static_cast<MapLiteral &>(expr).elements_);
}

std::vector<Expression **> MapProjectionChildren(Expression &expr) {
  auto &projection = static_cast<MapProjectionLiteral &>(expr);
  auto children = ValuesByKey(projection.elements_);
  children.push_back(&projection.map_variable_);
  return children;
}

// A kind whose whole state is its children matches whenever those children do.
bool NoOwnFields(Expression & /*lhs*/, Expression & /*rhs*/, ParameterNames const & /*parameter_names*/) {
  return true;
}

bool SameAggregation(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  auto &l = static_cast<Aggregation &>(lhs);
  auto &r = static_cast<Aggregation &>(rhs);
  return l.op_ == r.op_ && l.distinct_ == r.distinct_;
}

bool SameIdentifier(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  return static_cast<Identifier &>(lhs).name_ == static_cast<Identifier &>(rhs).name_;
}

// A literal the stripper left in place matches by value.
bool SamePrimitiveLiteral(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  return static_cast<PrimitiveLiteral &>(lhs).value_ == static_cast<PrimitiveLiteral &>(rhs).value_;
}

bool SamePropertyLookup(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  return static_cast<PropertyLookup &>(lhs).property_path_ == static_cast<PropertyLookup &>(rhs).property_path_;
}

bool SameLabelsTest(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  auto &l = static_cast<LabelsTest &>(lhs);
  auto &r = static_cast<LabelsTest &>(rhs);
  if (auto const *lhs_cnf = l.Cnf(), *rhs_cnf = r.Cnf(); lhs_cnf && rhs_cnf) return *lhs_cnf == *rhs_cnf;
  auto const *lhs_term = l.Term();
  auto const *rhs_term = r.Term();
  return lhs_term && rhs_term && SameLabelTerm(*lhs_term, *rhs_term);
}

template <typename TKind>
bool SameMapKeys(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  return SameKeys(static_cast<TKind &>(lhs).elements_, static_cast<TKind &>(rhs).elements_);
}

bool SameEnumValueAccess(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  auto &l = static_cast<EnumValueAccess &>(lhs);
  auto &r = static_cast<EnumValueAccess &>(rhs);
  return l.enum_name_ == r.enum_name_ && l.enum_value_ == r.enum_value_;
}

bool SameFunction(Expression &lhs, Expression &rhs, ParameterNames const & /*parameter_names*/) {
  auto &l = static_cast<Function &>(lhs);
  auto &r = static_cast<Function &>(rhs);
  return l.function_name_ == r.function_name_ && IsFunctionPure(l.function_name_);
}

// A literal the stripper replaced is a parameter, and matches by parameter name, which is distinct per literal, so
// two literals of different value never match.
bool SameParameterLookup(Expression &lhs, Expression &rhs, ParameterNames const &parameter_names) {
  auto const lhs_name = parameter_names.find(static_cast<ParameterLookup &>(lhs).token_position_);
  auto const rhs_name = parameter_names.find(static_cast<ParameterLookup &>(rhs).token_position_);
  return lhs_name != parameter_names.end() && rhs_name != parameter_names.end() && lhs_name->second == rhs_name->second;
}

static auto const kRules = std::array{
    // An aggregation is the case the whole rewrite exists for: ORDER BY count(n) repeats a projected count(n). It
    // derives from BinaryOperator and takes the same two children, but the function it names is its own state.
    KindRules{&Aggregation::kType, BinaryChildren, SameAggregation},

    KindRules{&OrOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&XorOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&AndOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&AdditionOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&SubtractionOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&MultiplicationOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&DivisionOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&ModOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&ExponentiationOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&NotEqualOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&EqualOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&LessOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&GreaterOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&LessEqualOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&GreaterEqualOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&InListOperator::kType, BinaryChildren, NoOwnFields},
    KindRules{&SubscriptOperator::kType, BinaryChildren, NoOwnFields},

    KindRules{&NotOperator::kType, OneChild<UnaryOperator>, NoOwnFields},
    KindRules{&UnaryPlusOperator::kType, OneChild<UnaryOperator>, NoOwnFields},
    KindRules{&UnaryMinusOperator::kType, OneChild<UnaryOperator>, NoOwnFields},
    KindRules{&IsNullOperator::kType, OneChild<UnaryOperator>, NoOwnFields},

    KindRules{&PropertyLookup::kType, OneChild<PropertyLookup>, SamePropertyLookup},
    KindRules{&LabelsTest::kType, OneChild<LabelsTest>, SameLabelsTest},
    KindRules{&AllPropertiesLookup::kType, OneChild<AllPropertiesLookup>, NoOwnFields},
    KindRules{&IfOperator::kType, IfChildren, NoOwnFields},
    KindRules{&ListSlicingOperator::kType, SliceChildren, NoOwnFields},
    KindRules{&RegexMatch::kType, RegexChildren, NoOwnFields},
    KindRules{&MapLiteral::kType, MapChildren, SameMapKeys<MapLiteral>},
    KindRules{&MapProjectionLiteral::kType, MapProjectionChildren, SameMapKeys<MapProjectionLiteral>},
    KindRules{&Function::kType, ChildList<Function, &Function::arguments_>, SameFunction},
    KindRules{&Coalesce::kType, ChildList<Coalesce, &Coalesce::expressions_>, NoOwnFields},
    KindRules{&ListLiteral::kType, ChildList<ListLiteral, &ListLiteral::elements_>, NoOwnFields},

    KindRules{&Identifier::kType, NoChildren, SameIdentifier},
    KindRules{&PrimitiveLiteral::kType, NoChildren, SamePrimitiveLiteral},
    KindRules{&ParameterLookup::kType, NoChildren, SameParameterLookup},
    KindRules{&EnumValueAccess::kType, NoChildren, SameEnumValueAccess},
};

KindRules const *RulesFor(Expression const &expr) {
  auto const &kind = expr.GetTypeInfo();
  auto const row = std::ranges::find_if(kRules, [&kind](auto const &rules) { return *rules.kind == kind; });
  return row == kRules.end() ? nullptr : &*row;
}

// The child slots of an expression the matcher knows, and none for one it does not, which is also what a kind with
// no children of its own gives back.
std::vector<Expression **> MatchableChildren(Expression &expr) {
  auto const *rules = RulesFor(expr);
  return rules ? rules->children(expr) : std::vector<Expression **>{};
}

// An identifier named like a projected item refers to that item, not to the variable the item was computed from.
bool IsShadowed(Expression *expr, std::vector<NamedExpression *> const &items) {
  auto const *identifier = utils::Downcast<Identifier>(expr);
  return identifier && std::ranges::contains(items, identifier->name_, &NamedExpression::name_);
}

bool AreEquivalent(Expression *lhs, Expression *rhs, std::vector<NamedExpression *> const &items,
                   ParameterNames const &parameter_names) {
  if (!lhs || !rhs) return lhs == rhs;
  // Two kinds that differ hold no common state, so neither side is read through the other. Past here both are the
  // one kind the row describes, which is what lets every rule reach its fields by a cast.
  if (lhs->GetTypeInfo() != rhs->GetTypeInfo()) return false;
  auto const *rules = RulesFor(*lhs);
  if (!rules || !rules->same_own_fields(*lhs, *rhs, parameter_names) || IsShadowed(lhs, items)) return false;
  return std::ranges::equal(rules->children(*lhs), rules->children(*rhs), [&](auto *l, auto *r) {
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
    for (auto *child : MatchableChildren(*expr)) self(self, *child);
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
  for (auto *child : MatchableChildren(*expr)) {
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
