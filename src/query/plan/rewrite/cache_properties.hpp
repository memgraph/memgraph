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

/// `Symbol` has no ordering of its own. Position is unique within a symbol table and stable across a plan,
/// which is what keeps the emitted operators in the same order from one planning run to the next.
struct SymbolPositionLess {
  bool operator()(Symbol const &lhs, Symbol const &rhs) const { return lhs.position() < rhs.position(); }
};

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

  /// Every symbol read often enough to be worth a cache, each with the properties to read for it.
  /// Ordered by symbol position so the plan does not depend on which lookup the traversal met first.
  ///
  /// Two lookups is the bar, counted as mentions rather than distinct properties: reading one property three
  /// times and reading three properties once each are both a single pass instead of several.
  auto Candidates() const -> std::map<Symbol, std::set<std::string>, SymbolPositionLess> {
    auto candidates = std::map<Symbol, std::set<std::string>, SymbolPositionLess>{};
    for (auto const &[symbol, gathered] : per_symbol_) {
      if (gathered.lookup_count < 2) continue;
      candidates.emplace(symbol, gathered.property_names);
    }
    return candidates;
  }

  void SetCachedSymbols(std::map<Symbol, std::map<std::string, Symbol, std::less<>>, SymbolPositionLess> cached) {
    cached_ = std::move(cached);
  }

 private:
  void Visit(PropertyLookup &op) override {
    // Only a lookup straight off a symbol qualifies: a nested path such as `n.a.b` reads a map, not the record.
    if (op.expression_->GetTypeInfo() != Identifier::kType) return;
    auto *identifier = static_cast<Identifier *>(op.expression_);
    if (identifier->symbol_pos_ < 0) return;

    auto const &symbol = symbol_table_->at(*identifier);

    switch (phase_) {
      case Phase::GATHER: {
        // The cursor fills its slots with Null for anything that is not a vertex, so an edge or an untyped
        // symbol would silently lose its properties. Such a lookup simply keeps reading the old way; it does
        // not disqualify the other symbols in the same expression.
        if (symbol.type() != Symbol::Type::VERTEX) return;
        auto &gathered = per_symbol_[symbol];
        gathered.property_names.insert(op.property_.name);
        ++gathered.lookup_count;
        return;
      }
      case Phase::REPLACE: {
        auto symbol_it = cached_.find(symbol);
        if (symbol_it == cached_.end()) return;
        auto it = symbol_it->second.find(op.property_.name);
        if (it == symbol_it->second.end()) return;
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

  struct Gathered {
    std::set<std::string> property_names;
    int lookup_count{0};
  };

  std::map<Symbol, Gathered, SymbolPositionLess> per_symbol_;
  std::map<Symbol, std::map<std::string, Symbol, std::less<>>, SymbolPositionLess> cached_;
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

    auto plan = PlanCaches(cacher, *input);
    if (plan.empty()) return true;

    // The variable-start planner rewrites several candidate plans that share one AST, so the replacement has to
    // happen on this plan's own copy of the named expressions.
    for (auto *&named_expression : op.named_expressions_) {
      named_expression = named_expression->Clone(ast_storage_);
    }

    cacher.SetCachedSymbols(TakeSlotsByName(plan));
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(PropertyLookupCacher::Phase::REPLACE, named_expression);
    }

    op.set_input(ChainCaches(input, std::move(plan)));
    return true;
  }

  bool PostVisit(Aggregate &op) override {
    auto input = op.input();
    if (!input) return true;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    ForEachRowExpression(op,
                         [&](Expression *&expression) { cacher.Run(PropertyLookupCacher::Phase::GATHER, expression); });

    auto plan = PlanCaches(cacher, *input);
    if (plan.empty()) return true;

    // These slots alias the AST that the Produce above still evaluates, so the replacement has to happen on this
    // operator's own copy of each expression.
    ForEachRowExpression(op, [&](Expression *&expression) { expression = expression->Clone(ast_storage_); });

    cacher.SetCachedSymbols(TakeSlotsByName(plan));
    ForEachRowExpression(
        op, [&](Expression *&expression) { cacher.Run(PropertyLookupCacher::Phase::REPLACE, expression); });

    op.set_input(ChainCaches(input, std::move(plan)));
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

  /// One cache to insert: the symbol to read from, the properties to read, and the slot each lands in.
  struct PlannedCache {
    Symbol input_symbol;
    std::map<std::string, Symbol, std::less<>> by_name;
    std::vector<CachedProperty> properties;
  };

  struct CacheSlots {
    std::map<std::string, Symbol, std::less<>> by_name;
    std::vector<CachedProperty> properties;
  };

  /// One planned cache per symbol the input actually binds. A symbol the input does not bind cannot be read
  /// here at all, and is dropped rather than disqualifying the rest.
  auto PlanCaches(PropertyLookupCacher const &cacher, LogicalOperator const &input) -> std::vector<PlannedCache> {
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    auto planned = std::vector<PlannedCache>{};
    for (auto const &[symbol, property_names] : cacher.Candidates()) {
      if (std::ranges::find(modified, symbol) == modified.end()) continue;
      auto slots = MakeSlots(property_names);
      planned.push_back(PlannedCache{symbol, std::move(slots.by_name), std::move(slots.properties)});
    }
    return planned;
  }

  static auto TakeSlotsByName(std::vector<PlannedCache> &plan)
      -> std::map<Symbol, std::map<std::string, Symbol, std::less<>>, SymbolPositionLess> {
    auto by_symbol = std::map<Symbol, std::map<std::string, Symbol, std::less<>>, SymbolPositionLess>{};
    for (auto &cache : plan) by_symbol.emplace(cache.input_symbol, cache.by_name);
    return by_symbol;
  }

  /// Caches stack: each reads from the row the one below produced, and every slot is written before the
  /// operator above evaluates anything.
  static auto ChainCaches(std::shared_ptr<LogicalOperator> input, std::vector<PlannedCache> plan)
      -> std::shared_ptr<LogicalOperator> {
    for (auto &cache : plan) {
      input = std::make_shared<CacheProperties>(input, cache.input_symbol, std::move(cache.properties));
    }
    return input;
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
