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
  enum class Phase : uint8_t { GATHER, REPLACE, RETUNE };

  ProducePropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  void Run(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// The frontend's index for a property. A `storage::PropertyId` is the other integer naming a
  /// property here and means something else entirely, so this is constructible only from the index
  /// itself: the two cannot be written in place of one another.
  class PropertyIndex {
   public:
    PropertyIndex() = default;

    explicit PropertyIndex(PropertyIx const &property) : value_{property.ix} {}

    friend bool operator==(PropertyIndex const &, PropertyIndex const &) = default;

   private:
    int64_t value_{-1};
  };

  /// One symbol and the properties gathered for it, each named once and in the order they were met.
  /// A pass reads a set of properties, so what matters is that a property written twice is one entry
  /// and that the order is the same on every planning of the same query, not what that order is.
  struct Gathered {
    Symbol symbol;
    std::vector<PropertyIx> properties;
  };

  /// Which frame slot each cached property was read into. Sized once from the gathered properties,
  /// so the whole mapping is one allocation.
  using CachedSymbolsOfProperty = std::vector<std::pair<PropertyIndex, Symbol>>;

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
        if (!symbol.BindsOneRecord()) return;
        auto found = std::ranges::find_if(gathered_, [&symbol](auto const &g) { return g.symbol == symbol; });
        if (found == gathered_.end()) {
          gathered_.push_back(Gathered{.symbol = symbol, .properties = {}});
          found = std::prev(gathered_.end());
        }
        auto &properties = found->properties;
        if (!std::ranges::contains(properties, op.property_.ix, &PropertyIx::ix)) {
          properties.push_back(op.property_);
        }
        return;
      }
      case Phase::RETUNE: {
        auto const &symbol = symbol_table_->at(*identifier);
        auto const group =
            std::ranges::find_if(cached_, [&symbol](auto const &entry) { return entry.first == symbol; });
        if (group == cached_.end()) return;
        // A lookup still standing on a symbol whose properties were read in one pass is one the
        // pass could not take, because it sits where it may never be evaluated. Loading every
        // property for it would read the record a second time over, so it reads its own.
        op.evaluation_mode_ = PropertyLookup::EvaluationMode::GET_OWN_PROPERTY;
        return;
      }
      case Phase::REPLACE: {
        auto const &symbol = symbol_table_->at(*identifier);
        auto const group =
            std::ranges::find_if(cached_, [&symbol](auto const &entry) { return entry.first == symbol; });
        if (group == cached_.end()) return;
        auto const it =
            std::ranges::find(group->second, PropertyIndex{op.property_}, &std::pair<PropertyIndex, Symbol>::first);
        if (it == group->second.end()) return;
        auto *replacement = ast_storage_->Create<Identifier>(it->second.name(), false);
        replacement->MapTo(it->second);
        prev_expressions_.back() = replacement;
        return;
      }
    }
  }

  /// Descends where the caching phases will not, so that only retuning sees these.
  void DescendOnlyToRetune(Expression *&first, Expression *&second) {
    if (phase_ != Phase::RETUNE) return;
    AcceptExpression(first);
    AcceptExpression(second);
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

  BINARY_VISIT(XorOperator)
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

  void Visit(NamedExpression &op) override { AcceptExpression(op.expression_); }

/// An expression no phase enters. Whatever lookups it holds keep reading properties the way they
/// already did, so declining one is always safe; what it costs is the pass. Each group below is one
/// reason for declining, and which group a node is in is the whole of why it is here.
#define NOT_ENTERED(TOp) \
  void Visit(TOp &op) override {}

  // Evaluated by a subplan below this Produce rather than here, so a slot read above it would
  // never be written.
  NOT_ENTERED(Aggregation)
  NOT_ENTERED(PatternComprehension)
  NOT_ENTERED(SubqueryExpression)

  // May never be evaluated at all, so reading ahead for one would read a record the query never
  // asked for, and could refuse a query that answers. The two short circuits below are declined
  // outright; AND and OR are declined for the same reason but still entered to retune, which is
  // further down.
  NOT_ENTERED(IfOperator)
  NOT_ENTERED(Coalesce)

  // A body run once per element of a list, which an empty or Null list runs no times. Reading
  // ahead has the same fault as the group above.
  NOT_ENTERED(Reduce)
  NOT_ENTERED(Extract)
  NOT_ENTERED(All)
  NOT_ENTERED(Single)
  NOT_ENTERED(Any)
  NOT_ENTERED(None)
  NOT_ENTERED(ListComprehension)

  // Holds no lookup on a symbol, so there is nothing inside to reach.
  NOT_ENTERED(Identifier)
  NOT_ENTERED(PrimitiveLiteral)
  NOT_ENTERED(ParameterLookup)
  NOT_ENTERED(EnumValueAccess)
  NOT_ENTERED(AllPropertiesLookup)

  // Always evaluated, so entering these would be safe. They are left for a later pass rather than
  // for a reason, which is worth saying so that the safety of the groups above is not read onto
  // them.
  NOT_ENTERED(MapProjectionLiteral)
  NOT_ENTERED(ListSlicingOperator)
  NOT_ENTERED(LabelsTest)
  NOT_ENTERED(EdgeTypesTest)
  NOT_ENTERED(RegexMatch)

#undef NOT_ENTERED

  // Entered to retune and for nothing else. Nothing inside is ever read ahead: a short circuit may
  // never evaluate what it holds, and a literal is built from its entries rather than read from a
  // frame. What retuning changes is only how a lookup left standing here reads, never whether or
  // when it is evaluated.
  //
  // These are the ones that pay twice without it, since the pass has already read the record and
  // loading every property again for them reads it a second time over.
  void Visit(AndOperator &op) override { DescendOnlyToRetune(op.expression1_, op.expression2_); }

  void Visit(OrOperator &op) override { DescendOnlyToRetune(op.expression1_, op.expression2_); }

  void Visit(ListLiteral &op) override {
    if (phase_ != Phase::RETUNE) return;
    for (auto *&element : op.elements_) AcceptExpression(element);
  }

  void Visit(MapLiteral &op) override {
    if (phase_ != Phase::RETUNE) return;
    for (auto &[_, element] : op.elements_) AcceptExpression(element);
  }

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
        cached_by_property.emplace_back(ProducePropertyLookupCacher::PropertyIndex{property}, output_symbol);
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
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(ProducePropertyLookupCacher::Phase::RETUNE, named_expression);
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
