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
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "flags/general.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace impl {

/// Walks the named expressions of a single Produce, first counting the property lookups that are candidates for
/// caching, then swapping the chosen ones for identifiers bound to the cache's frame slots.
///
/// Both phases share one traversal so that a lookup which the traversal does not reach is neither counted nor
/// replaced; a subexpression this visitor refuses to enter simply keeps reading properties the old way.
class ProducePropertyLookupCacher final : public ExpressionVisitor<void> {
 public:
  enum class Phase : uint8_t { GATHER, REPLACE };

  ProducePropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  void Run(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// One symbol and the properties gathered for it, each named once and in the order the frontend
  /// interned them. `PropertyIx` is equal and ordered on that index alone, so the same property
  /// written twice is one entry however it was spelled, and the name travels with it for the
  /// replacement that needs it.
  struct Gathered {
    Symbol symbol;
    std::vector<PropertyIx> properties;
  };

  /// Which frame slot each cached property was read into, found by that same interned index. Sized
  /// once from the gathered properties, so the whole mapping is one allocation.
  using CachedSymbolsOfProperty = std::vector<std::pair<int64_t, Symbol>>;

  /// The symbols worth a pass of their own. A symbol carrying one lookup is left out: one pass for
  /// one property is what the lookup already does.
  auto Candidates() const -> std::vector<Gathered> {
    auto worthwhile = std::vector<Gathered>{};
    for (auto const &gathered : gathered_) {
      if (gathered.properties.size() < 2) continue;
      worthwhile.push_back(gathered);
    }
    return worthwhile;
  }

  void SetCachedSymbols(std::vector<std::pair<Symbol, CachedSymbolsOfProperty>> cached) { cached_ = std::move(cached); }

 private:
  void Visit(PropertyLookup &op) override {
    // Only a lookup straight off a symbol qualifies: a nested path such as `n.a.b` reads a map, not the record.
    if (op.expression_->GetTypeInfo() != Identifier::kType) return;
    auto *identifier = static_cast<Identifier *>(op.expression_);
    if (identifier->symbol_pos_ < 0) return;

    switch (phase_) {
      case Phase::GATHER: {
        auto const &symbol = symbol_table_->at(*identifier);
        // A vertex and an edge each keep their properties in a record the cursor can read in one pass. The cursor
        // fills its slots with Null for anything else, so an untyped symbol would silently lose its properties.
        auto const reads_a_record = symbol.type() == Symbol::Type::VERTEX || symbol.type() == Symbol::Type::EDGE;
        if (!reads_a_record) return;
        auto found = std::ranges::find_if(gathered_, [&symbol](auto const &g) { return g.symbol == symbol; });
        if (found == gathered_.end()) {
          gathered_.push_back(Gathered{.symbol = symbol, .properties = {}});
          found = std::prev(gathered_.end());
        }
        auto &properties = found->properties;
        auto const at = std::ranges::lower_bound(properties, op.property_.ix, {}, &PropertyIx::ix);
        if (at == properties.end() || at->ix != op.property_.ix) properties.insert(at, op.property_);
        return;
      }
      case Phase::REPLACE: {
        auto const &symbol = symbol_table_->at(*identifier);
        auto const group =
            std::ranges::find_if(cached_, [&symbol](auto const &entry) { return entry.first == symbol; });
        if (group == cached_.end()) return;
        auto const it = std::ranges::find(group->second, op.property_.ix, &std::pair<int64_t, Symbol>::first);
        if (it == group->second.end()) return;
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
  // above it would leave the slot unwritten. Pattern comprehensions and subqueries are likewise evaluated by a
  // subplan.
  void Visit(Aggregation &op) override {}

  void Visit(PatternComprehension &op) override {}

  void Visit(SubqueryExpression &op) override {}

  void Visit(ListSlicingOperator &op) override {}

  void Visit(IfOperator &op) override {}

  void Visit(ListLiteral &op) override {}

  void Visit(MapLiteral &op) override {}

  void Visit(MapProjectionLiteral &op) override {}

  void Visit(LabelsTest &op) override {}

  void Visit(EdgeTypesTest &op) override {}

  void Visit(Coalesce &op) override {}

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
  std::vector<Gathered> gathered_;
  std::vector<std::pair<Symbol, CachedSymbolsOfProperty>> cached_;
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

    auto cacher = ProducePropertyLookupCacher{symbol_table_, ast_storage_};
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(ProducePropertyLookupCacher::Phase::GATHER, named_expression);
    }

    auto const modified = input->ModifiedSymbols(*symbol_table_);
    auto cached_by_symbol = std::vector<std::pair<Symbol, ProducePropertyLookupCacher::CachedSymbolsOfProperty>>{};
    auto groups = std::vector<CachedPropertyGroup>{};

    for (auto const &candidate : cacher.Candidates()) {
      // A symbol the input does not bind is read by something else, so there is no record here to read it from.
      if (std::ranges::find(modified, candidate.symbol) == modified.end()) continue;

      auto cached_by_property = ProducePropertyLookupCacher::CachedSymbolsOfProperty{};
      auto cached_properties = std::vector<CachedProperty>{};
      cached_properties.reserve(candidate.properties.size());
      cached_by_property.reserve(candidate.properties.size());
      for (auto const &property : candidate.properties) {
        auto const &output_symbol = symbol_table_->CreateAnonymousSymbol();
        cached_by_property.emplace_back(property.ix, output_symbol);
        cached_properties.push_back(CachedProperty{db_->NameToProperty(property.name), output_symbol});
      }
      cached_by_symbol.emplace_back(candidate.symbol, std::move(cached_by_property));
      groups.emplace_back(candidate.symbol, std::move(cached_properties));
    }

    if (groups.empty()) return true;

    // The variable-start planner rewrites several candidate plans that share one AST, so the replacement has to
    // happen on this plan's own copy of the named expressions.
    for (auto *&named_expression : op.named_expressions_) {
      named_expression = named_expression->Clone(ast_storage_);
    }

    cacher.SetCachedSymbols(std::move(cached_by_symbol));
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(ProducePropertyLookupCacher::Phase::REPLACE, named_expression);
    }

    op.set_input(std::make_shared<CacheProperties>(input, std::move(groups)));
    return true;
  }

 private:
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
