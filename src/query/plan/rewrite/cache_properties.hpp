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
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "flags/general.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

/// The frame slots a one-pass read writes into, held for as long as one query is being planned.
///
/// A planner may weigh many plans for one query against the single symbol table that sizes a
/// frame, and only the plan it settles on ever runs. Each operator reading a pass therefore draws
/// from a pool of its own: whichever plan is chosen gets slots nothing else is using, and the frame
/// is as wide as that plan needs rather than as wide as every plan considered.
class CachedPropertySlots {
 public:
  /// Draws one operator's slots in order, minting one only when the pool runs out.
  class Pool {
   public:
    explicit Pool(std::vector<Symbol> *slots) : slots_{slots} {}

    auto Next(SymbolTable &symbol_table) -> Symbol {
      if (drawn_ == slots_->size()) slots_->push_back(symbol_table.CreateAnonymousSymbol());
      return (*slots_)[drawn_++];
    }

   private:
    std::vector<Symbol> *slots_;
    size_t drawn_{0};
  };

  /// The expressions identify the operator reading the pass: a plan holds its own copy of the
  /// pointers, so two plans for one query name the same operator and a second operator names itself.
  /// Taken before any of them is copied, which is what makes the two plans agree.
  auto PoolFor(std::vector<Tree const *> const &expressions) -> Pool { return Pool{&slots_[expressions]}; }

 private:
  std::map<std::vector<Tree const *>, std::vector<Symbol>> slots_;
};

namespace impl {

/// Walks the expressions one operator evaluates, counting the lookups worth reading in a pass and
/// then swapping the chosen ones for identifiers on that pass's frame slots. A projection evaluates
/// named expressions and an aggregation evaluates what it aggregates and groups by; what they have
/// in common is that each of those is evaluated once for every row the operator is given.
///
/// The phases share one traversal, so a lookup it does not reach is neither counted nor replaced,
/// and keeps reading properties as it did.
class PropertyLookupCacher final : public ExpressionVisitor<void> {
 public:
  enum class Phase : uint8_t { GATHER, PROBE, REPLACE };

  PropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  void Run(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// As above for an expression its operator owns outright rather than through a named expression.
  /// The replacing phase may swap the whole of it for an identifier, so it is taken by reference.
  void Run(Phase phase, Expression *&expression) {
    phase_ = phase;
    AcceptExpression(expression);
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

  /// A property gathered for a symbol, with how many lookups named it. A pass reads a property once
  /// however many lookups asked for it, so the count is what says how many reads the pass saves.
  struct GatheredProperty {
    PropertyIx property;
    size_t lookups{0};
  };

  /// One symbol and the properties gathered for it, each named once and in the order they were met.
  /// A pass reads a set of properties, so what matters is that a property written twice is one entry
  /// and that the order is the same on every planning of the same query, not what that order is.
  struct Gathered {
    Symbol symbol;
    std::vector<GatheredProperty> properties;

    /// Whether some property here was named by more than one lookup.
    auto AnyPropertyReadTwice() const -> bool {
      return std::ranges::any_of(properties, [](auto const &p) { return p.lookups > 1; });
    }
  };

  /// Which frame slot each cached property was read into. Sized once from the gathered properties,
  /// so the whole mapping is one allocation.
  using CachedSymbolsOfProperty = std::vector<std::pair<PropertyIndex, Symbol>>;

  /// What a symbol's lookups must show before a pass of its own pays for itself. What separates the
  /// two is where the value the pass read is going: into a frame slot the operator was going to
  /// write anyway, or into something else.
  enum class Bar : uint8_t {
    /// Anything the pass would save, which is either a second property to read alongside the first
    /// or a second lookup of one property. What a projection asks for, because the slot holds the
    /// value its row returns and so costs the projection nothing.
    kAnythingToSave,
    /// A read the pass removes, which only a property named by more than one lookup gives it. What
    /// an aggregation asks for: it compares a value and discards it, so each slot is cost it would
    /// not otherwise pay, and batching reads it was going to make once each buys nothing.
    kAReadToRemove,
  };

  /// The symbols worth a pass of their own, by the standard the reading operator sets. A symbol
  /// whose lookups fall short is left out and keeps reading as it did.
  auto Candidates(Bar bar) const -> std::vector<Gathered> {
    auto worthwhile = std::vector<Gathered>{};
    for (auto const &gathered : gathered_) {
      auto const pays = bar == Bar::kAnythingToSave
                            ? (gathered.properties.size() >= 2 || gathered.AnyPropertyReadTwice())
                            : gathered.AnyPropertyReadTwice();
      if (!pays) continue;
      worthwhile.push_back(gathered);
    }
    return worthwhile;
  }

  void SetCachedSymbols(std::vector<std::pair<Symbol, CachedSymbolsOfProperty>> cached) { cached_ = std::move(cached); }

  /// Whether the later phases would change anything in this expression, asked before either of
  /// them writes to it. Only one they would change needs a copy of its own.
  auto WouldRewrite(NamedExpression *named_expression) -> bool {
    probe_found_ = false;
    Run(Phase::PROBE, named_expression);
    return probe_found_;
  }

  auto WouldRewrite(Expression *&expression) -> bool {
    probe_found_ = false;
    Run(Phase::PROBE, expression);
    return probe_found_;
  }

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
        auto const named = std::ranges::find_if(
            properties, [&op](GatheredProperty const &p) { return p.property.ix == op.property_.ix; });
        if (named == properties.end()) {
          properties.push_back(GatheredProperty{.property = op.property_, .lookups = 1});
        } else {
          ++named->lookups;
        }
        return;
      }
      case Phase::PROBE: {
        // Replacing acts on a lookup of a symbol read in one pass, so one found here is one it
        // would write to.
        auto const &symbol = symbol_table_->at(*identifier);
        if (std::ranges::any_of(cached_, [&symbol](auto const &entry) { return entry.first == symbol; })) {
          probe_found_ = true;
        }
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

/// An expression no phase enters. A lookup inside one keeps reading as it did, so declining is
/// always safe and costs only the pass. Each group below is one reason for it.
#define NOT_ENTERED(TOp) \
  void Visit(TOp &op) override {}

  // Evaluated by a subplan below the operator holding this expression rather than in it, so a slot
  // read here would never be written. What an aggregation evaluates is reached when this runs for
  // the aggregation itself, where its arguments stand on their own rather than inside one of these.
  NOT_ENTERED(Aggregation)
  NOT_ENTERED(PatternComprehension)
  NOT_ENTERED(SubqueryExpression)

  // May never be evaluated, so reading ahead would read a record the query never asked for and
  // could refuse a query that answers. AND and OR are declined for the same reason but are still
  // entered to retune, further down.
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

  // Always evaluated, so entering these would be safe. Left for a later pass rather than for a
  // reason, which is worth saying: the groups above are declined because entering them is not.
  NOT_ENTERED(MapProjectionLiteral)
  NOT_ENTERED(ListSlicingOperator)
  NOT_ENTERED(LabelsTest)
  NOT_ENTERED(EdgeTypesTest)
  NOT_ENTERED(RegexMatch)

  // A short circuit may never evaluate what it holds, so reading ahead for it has the same fault as
  // the group above. A literal is built from its entries rather than read from a frame, so a lookup
  // inside one is reached through the entry rather than here.
  NOT_ENTERED(AndOperator)
  NOT_ENTERED(OrOperator)
  NOT_ENTERED(ListLiteral)
  NOT_ENTERED(MapLiteral)

#undef NOT_ENTERED

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  Phase phase_{Phase::GATHER};
  std::vector<Expression *> prev_expressions_;
  std::vector<Gathered> gathered_;
  std::vector<std::pair<Symbol, CachedSymbolsOfProperty>> cached_;
  bool probe_found_{false};
};

template <class TDbAccessor>
class CachePropertiesRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  CachePropertiesRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db,
                          CachedPropertySlots *slots)
      : symbol_table_(symbol_table), ast_storage_(ast_storage), db_(db), slots_(slots) {}

  ~CachePropertiesRewriter() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PostVisit(Produce &op) override {
    auto input = op.input();
    if (!input) return true;

    auto pool = slots_->PoolFor({op.named_expressions_.begin(), op.named_expressions_.end()});
    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(PropertyLookupCacher::Phase::GATHER, named_expression);
    }

    auto groups = DrawGroups(cacher, *input, pool, PropertyLookupCacher::Bar::kAnythingToSave);
    if (groups.empty()) return true;

    // The planner may weigh several candidate plans for one query, and they share the expressions
    // they were planned from, so a phase below writes to this plan's own copy. One the phases
    // leave alone is shared as it stands: copying it would duplicate all it holds, down to a
    // whole subquery, once for every candidate.
    for (auto *&named_expression : op.named_expressions_) {
      if (cacher.WouldRewrite(named_expression)) named_expression = named_expression->Clone(ast_storage_);
    }

    for (auto *&named_expression : op.named_expressions_) {
      cacher.Run(PropertyLookupCacher::Phase::REPLACE, named_expression);
    }

    op.set_input(std::make_shared<CacheProperties>(input, std::move(groups)));
    return true;
  }

  /// An aggregation evaluates its own expressions once per input row, so a property two of them
  /// read is read twice for every row. The pass belongs below the aggregation, which is where the
  /// record still is: above it only the grouping keys and the aggregated values are defined.
  bool PostVisit(Aggregate &op) override {
    auto input = op.input();
    if (!input) return true;

    auto owned = ExpressionsOf(op);
    if (owned.empty()) return true;

    auto pool = slots_->PoolFor(PoolKeyOf(owned));
    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    for (auto *expression : owned) {
      cacher.Run(PropertyLookupCacher::Phase::GATHER, *expression);
    }

    auto groups = DrawGroups(cacher, *input, pool, PropertyLookupCacher::Bar::kAReadToRemove);
    if (groups.empty()) return true;

    for (auto *expression : owned) {
      if (cacher.WouldRewrite(*expression)) *expression = (*expression)->Clone(ast_storage_);
    }
    for (auto *expression : owned) {
      cacher.Run(PropertyLookupCacher::Phase::REPLACE, *expression);
    }

    op.set_input(std::make_shared<CacheProperties>(input, std::move(groups)));
    return true;
  }

 private:
  /// Every expression an aggregation evaluates per input row: what it aggregates, and what it
  /// groups by. `count(*)` carries no expression to read, and the secondary argument is there only
  /// for the aggregations that take one.
  static auto ExpressionsOf(Aggregate &op) -> std::vector<Expression **> {
    auto owned = std::vector<Expression **>{};
    for (auto &element : op.aggregations_) {
      if (element.arg1) owned.push_back(&element.arg1);
      if (element.arg2) owned.push_back(&element.arg2);
    }
    for (auto &group_by : op.group_by_) {
      owned.push_back(&group_by);
    }
    return owned;
  }

  static auto PoolKeyOf(std::vector<Expression **> const &owned) -> std::vector<Tree const *> {
    auto key = std::vector<Tree const *>{};
    key.reserve(owned.size());
    for (auto const *expression : owned) {
      key.push_back(*expression);
    }
    return key;
  }

  /// Draws the frame slots for every symbol whose lookups clear `bar`, telling the cacher which
  /// slot each property was given, and answering with the groups the pass below reads into them.
  auto DrawGroups(PropertyLookupCacher &cacher, LogicalOperator const &input, CachedPropertySlots::Pool &pool,
                  PropertyLookupCacher::Bar bar) -> std::vector<CachedPropertyGroup> {
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    auto cached_by_symbol = std::vector<std::pair<Symbol, PropertyLookupCacher::CachedSymbolsOfProperty>>{};
    auto groups = std::vector<CachedPropertyGroup>{};

    for (auto const &candidate : cacher.Candidates(bar)) {
      // A symbol the input does not bind is read by something else, so there is no record here to read it from.
      if (std::ranges::find(modified, candidate.symbol) == modified.end()) continue;

      auto cached_by_property = PropertyLookupCacher::CachedSymbolsOfProperty{};
      auto cached_properties = std::vector<CachedProperty>{};
      cached_properties.reserve(candidate.properties.size());
      cached_by_property.reserve(candidate.properties.size());
      for (auto const &gathered : candidate.properties) {
        auto const output_symbol = pool.Next(*symbol_table_);
        cached_by_property.emplace_back(PropertyLookupCacher::PropertyIndex{gathered.property}, output_symbol);
        cached_properties.push_back(CachedProperty{db_->NameToProperty(gathered.property.name), output_symbol});
      }
      cached_by_symbol.emplace_back(candidate.symbol, std::move(cached_by_property));
      groups.emplace_back(candidate.symbol, std::move(cached_properties));
    }

    if (!groups.empty()) cacher.SetCachedSymbols(std::move(cached_by_symbol));
    return groups;
  }

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  TDbAccessor *db_;
  CachedPropertySlots *slots_;
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteCacheProperties(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable *symbol_table, AstStorage *ast_storage,
                                                        TDbAccessor *db, CachedPropertySlots *slots) {
  if (!FLAGS_query_cache_properties) return root_op;
  auto rewriter = impl::CachePropertiesRewriter<TDbAccessor>{symbol_table, ast_storage, db, slots};
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
