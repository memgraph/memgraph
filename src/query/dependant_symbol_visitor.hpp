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
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/exists.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/ast/query/named_expression.hpp"
#include "query/frontend/ast/query/pattern_comprehension.hpp"

namespace memgraph::query {

class Symbol;

/**
 * @brief Visitor that collects dependent symbols from expressions and determines if they are cacheable.
 *
 * This visitor traverses expression trees to:
 * 1. Collect all symbol dependencies (identifiers that the expression depends on)
 * 2. Determine if the expression is cacheable (no non-deterministic functions, pattern matching, etc.)
 */
class DependantSymbolVisitor : public ExpressionVisitor<void> {
 public:
  using ExpressionVisitor<void>::Visit;

  explicit DependantSymbolVisitor(std::set<Symbol::Position_t> &dependencies) : dependencies_(dependencies) {}

  void Visit(Identifier &identifier) override {
    MG_ASSERT(identifier.symbol_pos_ != -1);
    dependencies_.insert(identifier.symbol_pos_);
  }

  void Visit(ListLiteral &list_literal) override {
    std::ranges::for_each(list_literal.elements_, [this](auto *arg) { arg->Accept(*this); });
  }

  void Visit(MapLiteral &map_literal) override {
    std::ranges::for_each(map_literal.elements_, [this](const auto &pair) { pair.second->Accept(*this); });
  }

  template <typename T>
  void VisitBinaryOperator(T &op) {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  template <typename T>
  void VisitUnaryOperator(T &op) {
    op.expression_->Accept(*this);
  }

  // binary operators
  void Visit(OrOperator &op) override { VisitBinaryOperator(op); }
  void Visit(XorOperator &op) override { VisitBinaryOperator(op); }
  void Visit(AndOperator &op) override { VisitBinaryOperator(op); }
  void Visit(AdditionOperator &op) override { VisitBinaryOperator(op); }
  void Visit(SubtractionOperator &op) override { VisitBinaryOperator(op); }
  void Visit(MultiplicationOperator &op) override { VisitBinaryOperator(op); }
  void Visit(DivisionOperator &op) override { VisitBinaryOperator(op); }
  void Visit(ModOperator &op) override { VisitBinaryOperator(op); }
  void Visit(ExponentiationOperator &op) override { VisitBinaryOperator(op); }
  void Visit(NotEqualOperator &op) override { VisitBinaryOperator(op); }
  void Visit(EqualOperator &op) override { VisitBinaryOperator(op); }
  void Visit(LessOperator &op) override { VisitBinaryOperator(op); }
  void Visit(GreaterOperator &op) override { VisitBinaryOperator(op); }
  void Visit(LessEqualOperator &op) override { VisitBinaryOperator(op); }
  void Visit(GreaterEqualOperator &op) override { VisitBinaryOperator(op); }
  void Visit(RangeOperator &op) override { VisitBinaryOperator(op); }
  void Visit(InListOperator &op) override { VisitBinaryOperator(op); }
  void Visit(SubscriptOperator &op) override { VisitBinaryOperator(op); }

  // unary operators
  void Visit(NotOperator &op) override { VisitUnaryOperator(op); }
  void Visit(UnaryPlusOperator &op) override { VisitUnaryOperator(op); }
  void Visit(UnaryMinusOperator &op) override { VisitUnaryOperator(op); }
  void Visit(IsNullOperator &op) override { VisitUnaryOperator(op); }

  void Visit(PropertyLookup &property_lookup) override { property_lookup.expression_->Accept(*this); }

  void Visit(AllPropertiesLookup &all_properties_lookup) override { all_properties_lookup.expression_->Accept(*this); }

  void Visit(ListSlicingOperator &slice) override {
    slice.list_->Accept(*this);
    if (slice.lower_bound_) {
      slice.lower_bound_->Accept(*this);
    }
    if (slice.upper_bound_) {
      slice.upper_bound_->Accept(*this);
    }
  }

  void Visit(Function &func) override {
    if (!IsFunctionPure(func.function_name_)) {
      is_cacheable_ = false;
      return;
    }
    std::ranges::for_each(func.arguments_, [this](auto *arg) { arg->Accept(*this); });
  }

  void Visit(Aggregation &agg) override {
    agg.expression1_->Accept(*this);
    if (agg.expression2_) {
      agg.expression2_->Accept(*this);
    }
  }

  void Visit(IfOperator &if_op) override {
    if_op.condition_->Accept(*this);
    if_op.then_expression_->Accept(*this);
    if_op.else_expression_->Accept(*this);
  }

  void Visit(Coalesce &coalesce) override {
    for (auto *arg : coalesce.expressions_) {
      arg->Accept(*this);
    }
  }

  void Visit(Reduce &reduce) override {
    reduce.list_->Accept(*this);
    reduce.initializer_->Accept(*this);
    reduce.expression_->Accept(*this);
    // Remove temporary symbols that are only valid within the reduce expression
    dependencies_.erase(reduce.accumulator_->symbol_pos_);
    dependencies_.erase(reduce.identifier_->symbol_pos_);
  }

  void Visit(Extract &extract) override {
    extract.list_->Accept(*this);
    extract.expression_->Accept(*this);
    // Remove temporary symbol that is only valid within the extract expression
    dependencies_.erase(extract.identifier_->symbol_pos_);
  }

  void Visit(All &all) override {
    all.list_expression_->Accept(*this);
    if (all.where_) {
      all.where_->expression_->Accept(*this);
    }
    dependencies_.erase(all.identifier_->symbol_pos_);
  }

  void Visit(Any &any) override {
    any.list_expression_->Accept(*this);
    if (any.where_) {
      any.where_->expression_->Accept(*this);
    }
    dependencies_.erase(any.identifier_->symbol_pos_);
  }

  void Visit(None &none) override {
    none.list_expression_->Accept(*this);
    if (none.where_) {
      none.where_->expression_->Accept(*this);
    }
    dependencies_.erase(none.identifier_->symbol_pos_);
  }

  void Visit(Single &single) override {
    single.list_expression_->Accept(*this);
    if (single.where_) {
      single.where_->expression_->Accept(*this);
    }
    dependencies_.erase(single.identifier_->symbol_pos_);
  }

  void Visit(LabelsTest &labels_test) override { labels_test.expression_->Accept(*this); }

  void Visit(RegexMatch &regex_match) override {
    regex_match.string_expr_->Accept(*this);
    regex_match.regex_->Accept(*this);
  }

  void Visit(ListComprehension &list_comp) override {
    list_comp.list_->Accept(*this);
    if (list_comp.where_) {
      list_comp.where_->expression_->Accept(*this);
    }
    if (list_comp.expression_) {
      list_comp.expression_->Accept(*this);
    }
    // Remove temporary symbol that is only valid within the list comprehension
    dependencies_.erase(list_comp.identifier_->symbol_pos_);
  }

  void Visit(PatternComprehension & /*pattern_comp*/) override {
    is_cacheable_ = false;  // Not cacheable due to pattern matching
  }

  void Visit(Exists & /*exists*/) override {
    is_cacheable_ = false;  // Not cacheable due to pattern/subquery
  }

  // Named expressions and other types
  void Visit(NamedExpression &named_expr) override { named_expr.expression_->Accept(*this); }
  void Visit(MapProjectionLiteral & /*map_proj*/) override {
    // Safe to cache - no action needed
  }

  // Primitive literals, parameters - safe to cache
  void Visit(PrimitiveLiteral & /*literal*/) override { /* Safe to cache */
  }
  void Visit(ParameterLookup & /*param*/) override { /* Safe to cache */
  }
  void Visit(EnumValueAccess & /*enum_access*/) override { /* Safe to cache */
  }

  bool is_cacheable() const { return is_cacheable_; }

 private:
  std::set<Symbol::Position_t> &dependencies_;
  bool is_cacheable_{true};
};

}  // namespace memgraph::query
