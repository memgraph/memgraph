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

    [[nodiscard]] auto Next(SymbolTable &symbol_table) -> Symbol {
      if (drawn_ == slots_->size()) slots_->push_back(symbol_table.CreateAnonymousSymbol());
      return (*slots_)[drawn_++];
    }

   private:
    std::vector<Symbol> *slots_;
    size_t drawn_{0};
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
  enum class Phase : uint8_t { GATHER, PROBE, REPLACE };

  PropertyLookupCacher(SymbolTable *symbol_table, AstStorage *ast_storage)
      : symbol_table_(symbol_table), ast_storage_(ast_storage) {}

  using ExpressionVisitor<void>::Visit;

  void Run(Phase phase, NamedExpression *named_expression) {
    phase_ = phase;
    named_expression->Accept(*this);
  }

  /// As above, for an expression the operator owns outright. By reference because replacing may
  /// swap the whole of it for an identifier.
  void Run(Phase phase, Expression *&expression) {
    phase_ = phase;
    AcceptExpression(expression);
  }

  /// The frontend's index for a property. `storage::PropertyId` is the other integer naming one
  /// and means something else, so this is constructible only from the index itself.
  class PropertyIndex {
   public:
    PropertyIndex() = default;

    explicit PropertyIndex(PropertyIx const &property) : value_{property.ix} {}

    friend bool operator==(PropertyIndex const &, PropertyIndex const &) = default;

   private:
    int64_t value_{-1};
  };

  /// A property gathered for a symbol, with how many lookups named it. A pass reads it once
  /// however many asked, so the count says how many reads the pass saves.
  struct GatheredProperty {
    PropertyIx property;
    size_t lookups{0};
  };

  /// One symbol and its gathered properties, each named once. The order must be the same on every
  /// planning of a query rather than any particular order.
  struct Gathered {
    Symbol symbol;
    std::vector<GatheredProperty> properties;

    /// Whether a pass here would remove a read rather than move one.
    [[nodiscard]] auto AnyPropertyReadTwice() const -> bool {
      return std::ranges::any_of(properties, [](auto const &p) { return p.lookups > 1; });
    }
  };

  /// Which frame slot each cached property was read into, sized once so the mapping is one
  /// allocation.
  using CachedSymbolsOfProperty = std::vector<std::pair<PropertyIndex, Symbol>>;

  /// What a symbol's lookups must show before a pass of its own pays for itself. What separates the
  /// two is where the value the pass read is going: into a frame slot the operator was going to
  /// write anyway, or into something else.
  enum class Threshold : uint8_t {
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
  [[nodiscard]] auto Candidates(Threshold threshold) const -> std::vector<Gathered> {
    auto worthwhile = std::vector<Gathered>{};
    for (auto const &gathered : gathered_) {
      auto const pays = threshold == Threshold::kAnythingToSave
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
  [[nodiscard]] auto WouldRewrite(NamedExpression *named_expression) -> bool {
    probe_found_ = false;
    Run(Phase::PROBE, named_expression);
    return probe_found_;
  }

  /// As above. By value because probing only reads.
  [[nodiscard]] auto WouldRewrite(Expression *expression) -> bool {
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
        // The cursor fills a slot with Null for anything that is not a record, so an untyped symbol
        // would silently lose its properties.
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
        // A symbol match is enough, since this only decides whether the expression needs its own copy.
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
  // rather than handing it back, though, so a slot drawn for one adds a write and removes no read.
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
    CacheBelow(
        op,
        [&op] { return std::vector<Tree const *>{op.named_expressions_.begin(), op.named_expressions_.end()}; },
        PropertyLookupCacher::Threshold::kAnythingToSave,
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
        PropertyLookupCacher::Threshold::kAReadToRemove,
        [&owned](auto &&each) {
          for (auto *expression : owned) each(*expression);
        });
    return true;
  }

 private:
  /// Hangs a one-pass read below `op` for the symbols whose lookups clear `threshold`.
  ///
  /// `for_each_expression` reaches each expression the operator evaluates for every row, which a
  /// projection and an aggregation name differently. `pool_key` is asked only once a pass is going
  /// to be hung, since every operator reaching here would otherwise build one to discard.
  template <class TPoolKey, class TForEachExpression>
  void CacheBelow(LogicalOperator &op, TPoolKey pool_key, PropertyLookupCacher::Threshold threshold,
                  TForEachExpression for_each_expression) {
    auto input = op.input();
    if (!input) return;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    for_each_expression([&cacher](auto &expression) { cacher.Run(PropertyLookupCacher::Phase::GATHER, expression); });

    // Before a slot is drawn or the input is walked, because most operators read nothing worth a
    // pass and should pay only for the gathering that says so.
    auto const candidates = cacher.Candidates(threshold);
    if (candidates.empty()) return;

    auto pool = slots_->PoolFor(pool_key());
    auto groups = DrawGroups(cacher, candidates, *input, pool);
    if (groups.empty()) return;

    // Candidate plans share the expressions they were planned from, so a phase below writes to this
    // plan's own copy. One left alone stays shared, since copying it would duplicate all it holds,
    // down to a whole subquery, for every candidate.
    for_each_expression([this, &cacher](auto &expression) {
      if (cacher.WouldRewrite(expression)) expression = expression->Clone(ast_storage_);
    });
    for_each_expression([&cacher](auto &expression) { cacher.Run(PropertyLookupCacher::Phase::REPLACE, expression); });

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

  /// Draws the frame slots for each of `candidates` the input binds a record for, telling the cacher
  /// which slot each property was given, and answering with the groups the pass reads into them.
  auto DrawGroups(PropertyLookupCacher &cacher, std::vector<PropertyLookupCacher::Gathered> const &candidates,
                  LogicalOperator const &input, CachedPropertySlots::Pool &pool) -> std::vector<CachedPropertyGroup> {
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    auto cached_by_symbol = std::vector<std::pair<Symbol, PropertyLookupCacher::CachedSymbolsOfProperty>>{};
    auto groups = std::vector<CachedPropertyGroup>{};

    for (auto const &candidate : candidates) {
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
