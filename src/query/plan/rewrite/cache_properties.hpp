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

#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "flags/general.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace impl {

/// Walks the expressions one operator evaluates per row, first counting the property lookups that are candidates for
/// caching, then swapping the chosen ones for identifiers bound to the cache's frame slots.
///
/// Both phases share one traversal so that a lookup which the traversal does not reach is neither counted nor
/// replaced; a subexpression this visitor refuses to enter simply keeps reading properties the old way.
class PropertyLookupCacher final : public ExpressionVisitor<void> {
 public:
  enum class Phase : uint8_t { GATHER, REPLACE };

  PropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  void Run(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// A slot the operator holds directly, which unlike a named expression's body can itself be the lookup to replace.
  void Run(Phase phase, Expression *&expression) {
    phase_ = phase;
    AcceptExpression(expression);
  }

  /// The symbol all gathered lookups agree on, or nullopt when there is nothing worth caching.
  auto Candidate() const -> std::optional<Symbol> {
    if (!single_symbol_ || lookup_count_ < 2) return std::nullopt;
    return single_symbol_;
  }

  auto PropertyNames() const -> const std::set<std::string> & { return property_names_; }

  void SetCachedSymbols(std::map<std::string, Symbol, std::less<>> cached) { cached_ = std::move(cached); }

 private:
  void Visit(PropertyLookup &op) override {
    // Only a lookup straight off a symbol qualifies: a nested path such as `n.a.b` reads a map, not the record.
    if (op.expression_->GetTypeInfo() != Identifier::kType) return;
    auto *identifier = static_cast<Identifier *>(op.expression_);
    if (identifier->symbol_pos_ < 0) return;

    switch (phase_) {
      case Phase::GATHER: {
        if (disabled_) return;
        auto const &symbol = symbol_table_->at(*identifier);
        // The cursor fills its slots with Null for anything that is not a vertex, so an edge or an untyped symbol
        // would silently lose its properties. A second symbol is out of scope for this rewrite.
        if (symbol.type() != Symbol::Type::VERTEX || (single_symbol_ && !(*single_symbol_ == symbol))) {
          single_symbol_.reset();
          lookup_count_ = 0;
          disabled_ = true;
          return;
        }
        single_symbol_ = symbol;
        property_names_.insert(op.property_.name);
        ++lookup_count_;
        return;
      }
      case Phase::REPLACE: {
        auto it = cached_.find(op.property_.name);
        if (it == cached_.end()) return;
        auto *replacement = ast_storage_->Create<Identifier>(it->second.name(), false);
        replacement->MapTo(it->second);
        prev_expressions_.back() = replacement;
        return;
      }
    }
  }

  void AcceptExpression(Expression *&expr) {
    prev_expressions_.emplace_back(expr);
    expr->Accept(*this);
    if (expr != prev_expressions_.back()) {
      expr = prev_expressions_.back();
    }
    prev_expressions_.pop_back();
  }

  void Visit(NotOperator &op) override { AcceptExpression(op.expression_); }

  void Visit(IsNullOperator &op) override { AcceptExpression(op.expression_); }

  void Visit(UnaryPlusOperator &op) override { AcceptExpression(op.expression_); }

  void Visit(UnaryMinusOperator &op) override { AcceptExpression(op.expression_); }

#define BINARY_VISIT(TOp)              \
  void Visit(TOp &op) override {       \
    AcceptExpression(op.expression1_); \
    AcceptExpression(op.expression2_); \
  }

  BINARY_VISIT(OrOperator)
  BINARY_VISIT(XorOperator)
  BINARY_VISIT(AndOperator)
  BINARY_VISIT(NotEqualOperator)
  BINARY_VISIT(EqualOperator)
  BINARY_VISIT(InListOperator)
  BINARY_VISIT(AdditionOperator)
  BINARY_VISIT(SubtractionOperator)
  BINARY_VISIT(MultiplicationOperator)
  BINARY_VISIT(DivisionOperator)
  BINARY_VISIT(ModOperator)
  BINARY_VISIT(ExponentiationOperator)
  BINARY_VISIT(LessOperator)
  BINARY_VISIT(GreaterOperator)
  BINARY_VISIT(LessEqualOperator)
  BINARY_VISIT(GreaterEqualOperator)
  BINARY_VISIT(RangeOperator)
  BINARY_VISIT(SubscriptOperator)

#undef BINARY_VISIT

  void Visit(Function &op) override {
    for (auto *&argument : op.arguments_) {
      AcceptExpression(argument);
    }
  }

  void Visit(Reduce &op) override { AcceptExpression(op.expression_); }

  void Visit(Extract &op) override { AcceptExpression(op.expression_); }

  void Visit(NamedExpression &op) override { AcceptExpression(op.expression_); }

  // An Aggregation's inner expression is evaluated by the Aggregate operator below this Produce, so a cache inserted
  // above it would leave the slot unwritten; the Aggregate is rewritten in its own right instead. Pattern
  // comprehensions are likewise evaluated by a subplan.
  void Visit(Aggregation &op) override {}

  void Visit(PatternComprehension &op) override {}

  void Visit(ListSlicingOperator &op) override {}

  void Visit(IfOperator &op) override {}

  void Visit(ListLiteral &op) override {}

  void Visit(MapLiteral &op) override {}

  void Visit(MapProjectionLiteral &op) override {}

  void Visit(LabelsTest &op) override {}

  void Visit(EdgeTypesTest &op) override {}

  void Visit(Coalesce &op) override {}

  void Visit(Exists &op) override {}

  void Visit(All &op) override {}

  void Visit(Single &op) override {}

  void Visit(Any &op) override {}

  void Visit(None &op) override {}

  void Visit(ListComprehension &op) override {}

  void Visit(Identifier &op) override {}

  void Visit(PrimitiveLiteral &op) override {}

  void Visit(AllPropertiesLookup &op) override {}

  void Visit(ParameterLookup &op) override {}

  void Visit(RegexMatch &op) override {}

  void Visit(EnumValueAccess &op) override {}

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  Phase phase_{Phase::GATHER};
  std::vector<Expression *> prev_expressions_;
  std::optional<Symbol> single_symbol_;
  std::set<std::string> property_names_;
  std::map<std::string, Symbol, std::less<>> cached_;
  int lookup_count_{0};
  bool disabled_{false};
};

template <class TDbAccessor>
class CachePropertiesRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  CachePropertiesRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db)
      : symbol_table_(symbol_table), ast_storage_(ast_storage), db_(db) {}

  ~CachePropertiesRewriter() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PostVisit(Produce &op) override {
    auto input = op.input();
    if (!input) return true;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(PropertyLookupCacher::Phase::GATHER, named_expression);
    }

    auto candidate = Candidate(cacher, *input);
    if (!candidate) return true;

    auto slots = MakeSlots(cacher.PropertyNames());

    // The variable-start planner rewrites several candidate plans that share one AST, so the replacement has to
    // happen on this plan's own copy of the named expressions.
    for (auto *&named_expression : op.named_expressions_) {
      named_expression = named_expression->Clone(ast_storage_);
    }

    cacher.SetCachedSymbols(std::move(slots.by_name));
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(PropertyLookupCacher::Phase::REPLACE, named_expression);
    }

    op.set_input(std::make_shared<CacheProperties>(input, *candidate, std::move(slots.properties)));
    return true;
  }

  bool PostVisit(Aggregate &op) override {
    auto input = op.input();
    if (!input) return true;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    ForEachRowExpression(op,
                         [&](Expression *&expression) { cacher.Run(PropertyLookupCacher::Phase::GATHER, expression); });

    auto candidate = Candidate(cacher, *input);
    if (!candidate) return true;

    auto slots = MakeSlots(cacher.PropertyNames());

    // These slots alias the AST that the Produce above still evaluates, so the replacement has to happen on this
    // operator's own copy of each expression.
    ForEachRowExpression(op, [&](Expression *&expression) { expression = expression->Clone(ast_storage_); });

    cacher.SetCachedSymbols(std::move(slots.by_name));
    ForEachRowExpression(
        op, [&](Expression *&expression) { cacher.Run(PropertyLookupCacher::Phase::REPLACE, expression); });

    op.set_input(std::make_shared<CacheProperties>(input, *candidate, std::move(slots.properties)));
    return true;
  }

 private:
  /// The expressions an Aggregate evaluates once per input row. `arg2` is left out because only some operations
  /// evaluate it, and `COUNT(*)` has no argument at all.
  static void ForEachRowExpression(Aggregate &op, auto &&function) {
    for (auto *&expression : op.group_by_) {
      function(expression);
    }
    for (auto &element : op.aggregations_) {
      if (element.arg1 != nullptr) function(element.arg1);
    }
  }

  struct CacheSlots {
    std::map<std::string, Symbol, std::less<>> by_name;
    std::vector<CachedProperty> properties;
  };

  auto Candidate(PropertyLookupCacher const &cacher, LogicalOperator const &input) const -> std::optional<Symbol> {
    auto candidate = cacher.Candidate();
    if (!candidate) return std::nullopt;
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    if (std::ranges::find(modified, *candidate) == modified.end()) return std::nullopt;
    return candidate;
  }

  auto MakeSlots(std::set<std::string> const &property_names) -> CacheSlots {
    auto slots = CacheSlots{};
    slots.properties.reserve(property_names.size());
    for (auto const &property_name : property_names) {
      auto const &output_symbol = symbol_table_->CreateAnonymousSymbol();
      slots.by_name.emplace(property_name, output_symbol);
      slots.properties.push_back(CachedProperty{db_->NameToProperty(property_name), output_symbol});
    }
    return slots;
  }

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  TDbAccessor *db_;
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteCacheProperties(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable *symbol_table, AstStorage *ast_storage,
                                                        TDbAccessor *db) {
  if (!FLAGS_query_cache_properties) return root_op;
  auto rewriter = impl::CachePropertiesRewriter<TDbAccessor>{symbol_table, ast_storage, db};
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
