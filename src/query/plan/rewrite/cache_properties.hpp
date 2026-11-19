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

/// The frame slots a one-pass read writes into, for the length of one query's planning.
///
/// Candidate plans for one query share the symbol table that sizes a frame, and only the chosen
/// plan runs. Giving each operator a pool of its own keeps the frame as wide as that plan needs
/// rather than as wide as every plan weighed.
class CachedPropertySlots {
 public:
  class Pool {
   public:
    explicit Pool(std::vector<Symbol> *slots) : slots_{slots} {}

    [[nodiscard]] auto NextSlot(SymbolTable &symbol_table) -> Symbol {
      if (assigned_ == slots_->size()) slots_->push_back(symbol_table.CreateAnonymousSymbol());
      return (*slots_)[assigned_++];
    }

   private:
    std::vector<Symbol> *slots_;
    size_t assigned_{0};
  };

  /// Keyed on the expressions because a plan holds its own copy of the pointers, so two plans for
  /// one query name the same operator. Taken before any is copied, which is what makes them agree.
  [[nodiscard]] auto PoolFor(std::vector<Tree const *> const &expressions) -> Pool {
    return Pool{&slots_[expressions]};
  }

 private:
  std::map<std::vector<Tree const *>, std::vector<Symbol>> slots_;
};

namespace impl {

/// Counts the lookups worth reading in a pass and swaps the chosen ones for identifiers on its
/// frame slots.
///
/// The phases share one traversal, so a lookup it does not reach is neither counted nor replaced.
class PropertyLookupCacher final : public ExpressionVisitor<void> {
 public:
  PropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  /// Counts every lookup this expression evaluates for a row.
  void Gather(NamedExpression *named_expression) { Walk(Phase::GATHER, named_expression); }

  void Gather(Expression *&expression) { Walk(Phase::GATHER, expression); }

  /// Swaps each lookup a pass reads for an identifier on the slot holding it.
  void Replace(NamedExpression *named_expression) { Walk(Phase::REPLACE, named_expression); }

  void Replace(Expression *&expression) { Walk(Phase::REPLACE, expression); }

  /// The frame slots a pass reads into, by the symbol and the property each holds.
  class CachedSlots {
   public:
    /// Records that the pass reads `property` of `symbol` into `slot`.
    void Add(Symbol const &symbol, PropertyIx const &property, Symbol const &slot) {
      auto found = std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol);
      if (found == by_symbol_.end()) {
        by_symbol_.push_back(SymbolSlots{.symbol = symbol});
        found = std::prev(by_symbol_.end());
      }
      found->slots.emplace_back(PropertyIndex{property}, slot);
    }

    /// Whether the pass reads anything of this symbol, which is all that looking for a lookup to
    /// replace asks: it decides only whether an expression needs a copy of its own.
    [[nodiscard]] auto Holds(Symbol const &symbol) const -> bool {
      return std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol) != by_symbol_.end();
    }

    [[nodiscard]] auto SlotFor(Symbol const &symbol, PropertyIx const &property) const -> std::optional<Symbol> {
      auto const found = std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol);
      if (found == by_symbol_.end()) return std::nullopt;
      auto const slot = std::ranges::find(found->slots, PropertyIndex{property}, &PropertySlot::first);
      if (slot == found->slots.end()) return std::nullopt;
      return slot->second;
    }

   private:
    /// A property as the frontend indexes it, and no more. A slot is never found by the name a
    /// `PropertyIx` also carries, and this mapping is built afresh for every candidate plan.
    class PropertyIndex {
     public:
      explicit PropertyIndex(PropertyIx const &property) : value_{property.ix} {}

      friend bool operator==(PropertyIndex const &, PropertyIndex const &) = default;

     private:
      int64_t value_;
    };

    using PropertySlot = std::pair<PropertyIndex, Symbol>;

    struct SymbolSlots {
      Symbol symbol;
      std::vector<PropertySlot> slots;
    };

    std::vector<SymbolSlots> by_symbol_;
  };

  /// What an operator does with a value its lookups read, which decides what a pass of its own has
  /// to save before it pays for itself.
  enum class ValueUse : uint8_t {
    /// A projection returns the value, so it was going to put it in a frame slot either way. Any
    /// saving then pays: a second property to read alongside the first, or a second lookup of one
    /// property.
    kKept,
    /// An aggregation compares the value and keeps none of it, so a slot is a write it would not
    /// otherwise make. Only a read the pass removes pays for that write, and only a property more
    /// than one lookup names gives it one.
    kDiscarded,
  };

  /// A property gathered for a symbol, with how many lookups named it. A pass reads it once
  /// however many asked, so the count says how many reads the pass saves.
  struct GatheredProperty {
    PropertyIx property;
    size_t lookups{0};

    void Note() { ++lookups; }
  };

  /// One symbol and its gathered properties, each named once. The order must be the same on every
  /// planning of a query rather than any particular order.
  struct Gathered {
    Symbol symbol;
    std::vector<GatheredProperty> properties;

    void Note(PropertyIx const &property) {
      auto found = std::ranges::find(properties, property, &GatheredProperty::property);
      if (found == properties.end()) {
        properties.push_back(GatheredProperty{.property = property});
        found = std::prev(properties.end());
      }
      found->Note();
    }

    /// The properties worth reading in one pass, and nothing when none of them are.
    [[nodiscard]] auto PropertiesWorthOnePass(ValueUse value_use) const -> std::optional<Gathered> {
      auto const removes_a_read = std::ranges::any_of(properties, [](auto const &p) { return p.lookups > 1; });
      auto const pays = value_use == ValueUse::kDiscarded ? removes_a_read : removes_a_read || properties.size() >= 2;
      if (!pays) return std::nullopt;
      return *this;
    }
  };

  /// The symbols worth a pass of their own, by what the operator does with the values. A symbol
  /// whose lookups save it too little is left out and keeps reading as it did.
  [[nodiscard]] auto SymbolsWorthOnePass(ValueUse value_use) const -> std::vector<Gathered> {
    auto worthwhile = std::vector<Gathered>{};
    for (auto const &gathered : gathered_) {
      if (auto properties = gathered.PropertiesWorthOnePass(value_use)) worthwhile.push_back(*std::move(properties));
    }
    return worthwhile;
  }

  void SetCachedSlots(CachedSlots cached) { cached_ = std::move(cached); }

  /// Whether this expression holds a lookup replacing would swap, asked before it writes. Only an
  /// expression it would change needs a copy of its own.
  [[nodiscard]] auto HasALookupToReplace(NamedExpression *named_expression) -> bool {
    found_one_to_replace_ = false;
    Walk(Phase::FIND, named_expression);
    return found_one_to_replace_;
  }

  /// As above. By value because looking does not write.
  [[nodiscard]] auto HasALookupToReplace(Expression *expression) -> bool {
    found_one_to_replace_ = false;
    Walk(Phase::FIND, expression);
    return found_one_to_replace_;
  }

 private:
  enum class Phase : uint8_t { GATHER, FIND, REPLACE };

  void Walk(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// By reference because replacing may swap the whole of the expression for an identifier.
  void Walk(Phase phase, Expression *&expression) {
    phase_ = phase;
    AcceptExpression(expression);
  }

  void Visit(PropertyLookup &op) override {
    // Only a lookup straight off a symbol qualifies: a nested path such as `n.a.b` reads a map, not the record.
    if (op.expression_->GetTypeInfo() != Identifier::kType) return;
    auto *identifier = static_cast<Identifier *>(op.expression_);
    if (identifier->symbol_pos_ < 0) return;

    switch (phase_) {
      case Phase::GATHER: {
        auto const &symbol = symbol_table_->at(*identifier);
        // The cursor fills a slot with Null for anything that is not a record, so an untyped symbol
        // would silently lose its properties.
        if (!symbol.BindsOneRecord()) return;
        GatheredFor(symbol).Note(op.property_);
        return;
      }
      case Phase::FIND: {
        if (cached_.Holds(symbol_table_->at(*identifier))) found_one_to_replace_ = true;
        return;
      }
      case Phase::REPLACE: {
        auto const slot = cached_.SlotFor(symbol_table_->at(*identifier), op.property_);
        if (!slot) return;
        auto *replacement = ast_storage_->Create<Identifier>(slot->name(), false);
        replacement->MapTo(*slot);
        prev_expressions_.back() = replacement;
        return;
      }
    }
  }

  /// The symbol's gathered properties, which its first lookup starts.
  [[nodiscard]] auto GatheredFor(Symbol const &symbol) -> Gathered & {
    auto found = std::ranges::find(gathered_, symbol, &Gathered::symbol);
    if (found != gathered_.end()) return *found;
    return gathered_.emplace_back(Gathered{.symbol = symbol, .properties = {}});
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
/// always safe. Each group below is one reason for it.
#define NOT_ENTERED(TOp) \
  void Visit(TOp &op) override {}

  // Evaluated by a subplan below the operator holding this expression rather than in it, so a slot
  // read here would never be written.
  NOT_ENTERED(Aggregation)
  NOT_ENTERED(PatternComprehension)
  NOT_ENTERED(SubqueryExpression)

  // May never be evaluated, so reading ahead would read a record the query never asked for and
  // could refuse a query that answers.
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

  // Safe to enter, unlike the groups above, and not yet entered.
  NOT_ENTERED(MapProjectionLiteral)
  NOT_ENTERED(ListSlicingOperator)
  NOT_ENTERED(LabelsTest)
  NOT_ENTERED(EdgeTypesTest)
  NOT_ENTERED(RegexMatch)

  // A short circuit may never evaluate its second operand, so reading ahead for a lookup there has
  // the same fault as the conditionals above.
  NOT_ENTERED(AndOperator)
  NOT_ENTERED(OrOperator)

  // Always evaluated, so entering these would be safe. An entry or an element consumes the value
  // rather than handing it back, though, so a slot taken for one adds a write and removes no read.
  NOT_ENTERED(ListLiteral)
  NOT_ENTERED(MapLiteral)

#undef NOT_ENTERED

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  Phase phase_{Phase::GATHER};
  std::vector<Expression *> prev_expressions_;
  std::vector<Gathered> gathered_;
  CachedSlots cached_;
  bool found_one_to_replace_{false};
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
    CacheBelow(
        op,
        [&op] { return std::vector<Tree const *>{op.named_expressions_.begin(), op.named_expressions_.end()}; },
        PropertyLookupCacher::ValueUse::kKept,
        [&op](auto &&each) {
          for (auto *&named_expression : op.named_expressions_) each(named_expression);
        });
    return true;
  }

  /// Below the aggregation, which is where the record still is: above it only the grouping keys
  /// and the aggregated values are defined.
  bool PostVisit(Aggregate &op) override {
    auto const owned = ExpressionsOf(op);
    if (owned.empty()) return true;

    CacheBelow(
        op,
        [&owned] { return PoolKeyOf(owned); },
        PropertyLookupCacher::ValueUse::kDiscarded,
        [&owned](auto &&each) {
          for (auto *expression : owned) each(*expression);
        });
    return true;
  }

 private:
  /// Puts a one-pass read below `op` for each symbol whose lookups save it enough, which is what
  /// `value_use` decides.
  ///
  /// `for_each_expression` reaches each expression the operator evaluates for every row, which a
  /// projection and an aggregation name differently. `pool_key` is asked only once a pass is going
  /// to be put there, since every operator reaching here would otherwise build one to discard.
  template <class TPoolKey, class TForEachExpression>
  void CacheBelow(LogicalOperator &op, TPoolKey pool_key, PropertyLookupCacher::ValueUse value_use,
                  TForEachExpression for_each_expression) {
    auto input = op.input();
    if (!input) return;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    for_each_expression([&cacher](auto &expression) { cacher.Gather(expression); });

    // Before a slot is taken or the input is walked, because most operators read nothing worth a
    // pass and should pay only for the gathering that says so.
    auto const worthwhile = cacher.SymbolsWorthOnePass(value_use);
    if (worthwhile.empty()) return;

    auto pool = slots_->PoolFor(pool_key());
    auto groups = MakeGroups(cacher, worthwhile, *input, pool);
    if (groups.empty()) return;

    // Candidate plans share the expressions they were planned from, so a phase below writes to this
    // plan's own copy. One left alone stays shared, since copying it would duplicate all it holds,
    // down to a whole subquery, for every candidate.
    for_each_expression([this, &cacher](auto &expression) {
      if (cacher.HasALookupToReplace(expression)) expression = expression->Clone(ast_storage_);
    });
    for_each_expression([&cacher](auto &expression) { cacher.Replace(expression); });

    op.set_input(std::make_shared<CacheProperties>(input, std::move(groups)));
  }

  /// What an aggregation evaluates for every row: what it aggregates, and what it groups by.
  ///
  /// A secondary argument, which a map collection and the graph projections take, is left out
  /// however many lookups it holds. It is reached only once the row's first value is accepted, so a
  /// Null value, or a repeat a distinct aggregation ignores, never evaluates it, and reading ahead
  /// would reach a record such a row never asks about.
  [[nodiscard]] static auto ExpressionsOf(Aggregate &op) -> std::vector<Expression **> {
    auto owned = std::vector<Expression **>{};
    for (auto &element : op.aggregations_) {
      if (element.arg1) owned.push_back(&element.arg1);
    }
    for (auto &group_by : op.group_by_) {
      owned.push_back(&group_by);
    }
    return owned;
  }

  [[nodiscard]] static auto PoolKeyOf(std::vector<Expression **> const &owned) -> std::vector<Tree const *> {
    auto key = std::vector<Tree const *>{};
    key.reserve(owned.size());
    for (auto const *expression : owned) {
      key.push_back(*expression);
    }
    return key;
  }

  /// Takes a frame slot for each property of each of `worthwhile` the input binds a record for,
  /// telling the cacher which slot each property was given, and answering with the groups the pass
  /// reads into them.
  auto MakeGroups(PropertyLookupCacher &cacher, std::vector<PropertyLookupCacher::Gathered> const &worthwhile,
                  LogicalOperator const &input, CachedPropertySlots::Pool &pool) -> std::vector<CachedPropertyGroup> {
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    auto slots = PropertyLookupCacher::CachedSlots{};
    auto groups = std::vector<CachedPropertyGroup>{};

    for (auto const &pass : worthwhile) {
      // A symbol the input does not bind is read by something else, so there is no record here to read it from.
      if (std::ranges::find(modified, pass.symbol) == modified.end()) continue;

      auto cached_properties = std::vector<CachedProperty>{};
      cached_properties.reserve(pass.properties.size());
      for (auto const &gathered : pass.properties) {
        auto const output_symbol = pool.NextSlot(*symbol_table_);
        slots.Add(pass.symbol, gathered.property, output_symbol);
        cached_properties.push_back(CachedProperty{db_->NameToProperty(gathered.property.name), output_symbol});
      }
      groups.emplace_back(pass.symbol, std::move(cached_properties));
    }

    if (!groups.empty()) cacher.SetCachedSlots(std::move(slots));
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
