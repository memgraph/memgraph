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

/// Where the frontend interned a property's name, which is what the AST carries in place of the
/// name itself. Both the names an `AstStorage` holds and the ids an `EvaluationContext` resolves
/// them to are indexed by it, so nothing holding one needs the name as well. Not a
/// `storage::PropertyId`, which is what names a property to the storage engine.
class AstPropertyIndex {
 public:
  explicit AstPropertyIndex(PropertyIx const &property) : value_{property.ix} {}

  [[nodiscard]] auto Position() const -> size_t { return static_cast<size_t>(value_); }

  friend bool operator==(AstPropertyIndex const &, AstPropertyIndex const &) = default;
  friend auto operator<=>(AstPropertyIndex const &, AstPropertyIndex const &) = default;

 private:
  int64_t value_;
};

/// What one query's planning settles once, which every candidate plan it weighs then takes as it
/// stands: the frame slots a pass reads into, and the rewrites made with them.
///
/// Candidates share the symbol table that sizes a frame and the expressions they were planned
/// from, and all but one is thrown away. Giving each operator slots of its own keeps the frame as
/// wide as the chosen plan needs rather than as wide as every plan weighed, and keeping what was
/// rewritten with them leaves one copy of it rather than one per candidate.
class PassPlanning {
 public:
  class SlotPool {
   public:
    explicit SlotPool(std::vector<Symbol> *slots) : slots_{slots} {}

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
  [[nodiscard]] auto SlotsFor(std::vector<Tree const *> const &expressions) -> SlotPool {
    return SlotPool{&slots_[expressions]};
  }

  /// One slot a pass reads into: the symbol whose record a property is read from, and the slot the
  /// value is written to. Both symbols are named by position alone so that comparing two plans does
  /// not mean holding on to what either was planned from.
  struct FilledSlot {
    Symbol::Position_t input_symbol;
    AstPropertyIndex property;
    Symbol::Position_t output_symbol;

    friend bool operator==(FilledSlot const &, FilledSlot const &) = default;
    friend auto operator<=>(FilledSlot const &, FilledSlot const &) = default;
  };

  /// Which slot holds which property of which symbol, which is what two plans must agree on
  /// before one can read the other's rewrite of an expression.
  using SlotSignature = std::vector<FilledSlot>;

  /// The rewrite of `original` a plan has already made under `signature`, if one has.
  ///
  /// Candidate plans share the expressions they were planned from, and all but one are thrown away,
  /// so a rewrite per plan leaves a copy per plan in a tree that outlives them all. Two plans
  /// reading the same properties into the same slots rewrite an expression identically, which is
  /// what lets them hold one rewrite between them.
  template <class TExpression>
  [[nodiscard]] auto RewrittenFor(TExpression *original, SlotSignature const &signature) const -> TExpression * {
    auto const found = rewritten_.find(Rewrite{original, signature});
    if (found == rewritten_.end()) return nullptr;
    // A key names one expression rewritten one way, so what it holds came from a slot of this type.
    return static_cast<TExpression *>(found->second);
  }

  void Remember(Tree const *original, SlotSignature signature, Tree *rewritten) {
    rewritten_.emplace(Rewrite{original, std::move(signature)}, rewritten);
  }

 private:
  using Rewrite = std::pair<Tree const *, SlotSignature>;

  std::map<std::vector<Tree const *>, std::vector<Symbol>> slots_;
  std::map<Rewrite, Tree *> rewritten_;
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

  using AstPropertyIndex = plan::AstPropertyIndex;

  /// The frame slots a pass reads into, by the symbol and the property each holds.
  class AssignedSlots {
   public:
    void Add(Symbol const &symbol, AstPropertyIndex property, Symbol const &slot) {
      auto found = std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol);
      if (found == by_symbol_.end()) {
        by_symbol_.push_back(SymbolSlots{.symbol = symbol});
        found = std::prev(by_symbol_.end());
      }
      found->slots.emplace_back(property, slot);
    }

    [[nodiscard]] auto Signature() const -> PassPlanning::SlotSignature {
      auto signature = PassPlanning::SlotSignature{};
      for (auto const &group : by_symbol_) {
        for (auto const &[property, slot] : group.slots) {
          signature.push_back(PassPlanning::FilledSlot{
              .input_symbol = group.symbol.position(), .property = property, .output_symbol = slot.position()});
        }
      }
      return signature;
    }

    /// Whether the pass reads anything of this symbol, which is all that looking for a lookup to
    /// replace asks: it decides only whether an expression needs a copy of its own.
    [[nodiscard]] auto Holds(Symbol const &symbol) const -> bool {
      return std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol) != by_symbol_.end();
    }

    [[nodiscard]] auto SlotHolding(Symbol const &symbol, AstPropertyIndex property) const -> std::optional<Symbol> {
      auto const found = std::ranges::find(by_symbol_, symbol, &SymbolSlots::symbol);
      if (found == by_symbol_.end()) return std::nullopt;
      auto const slot = std::ranges::find(found->slots, property, &PropertySlot::first);
      if (slot == found->slots.end()) return std::nullopt;
      return slot->second;
    }

   private:
    using PropertySlot = std::pair<AstPropertyIndex, Symbol>;

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
    /// An aggregation and a filter consume the value and keep none of it, so a slot is a write
    /// neither would otherwise make. Only a read the pass removes pays for that write, and only a
    /// property more than one lookup names gives it one.
    kDiscarded,
  };

  /// A property gathered for a symbol, with how many lookups named it and how many of those every
  /// row reaches.
  struct GatheredProperty {
    AstPropertyIndex property;
    size_t lookups{0};
    size_t unconditional_lookups{0};

    /// Records one lookup of this property, and whether every row reaching the operator evaluates it.
    void Note(bool unconditional) {
      ++lookups;
      unconditional_lookups += unconditional ? 1 : 0;
    }
  };

  /// One symbol and the properties a pass of its own would read. What the lookup counts were for
  /// is settled by the time one of these exists, so it carries the properties alone.
  struct Pass {
    Symbol symbol;
    std::vector<AstPropertyIndex> properties;
  };

  /// One symbol and its gathered properties, each named once. The order must be the same on every
  /// planning of a query rather than any particular order.
  struct Gathered {
    Symbol symbol;
    std::vector<GatheredProperty> properties;

    void Note(AstPropertyIndex property, bool unconditional) {
      auto found = std::ranges::find(properties, property, &GatheredProperty::property);
      if (found == properties.end()) {
        properties.push_back(GatheredProperty{.property = property});
        found = std::prev(properties.end());
      }
      found->Note(unconditional);
    }

    /// The properties worth reading in one pass, and nothing when none of them are.
    ///
    /// A pass reads only a property some lookup reads on every row, so what the operator needs
    /// saved is asked of those alone. Reading one the rows may not ask for would read for rows
    /// stopping first, which is work the query did not ask for and the one way a pass could reach a
    /// record it never touched.
    [[nodiscard]] auto PropertiesWorthOnePass(ValueUse value_use) const -> std::optional<Pass> {
      auto const read_on_every_row = [](GatheredProperty const &property) {
        return property.unconditional_lookups > 0;
      };
      auto const removes_a_read = std::ranges::any_of(
          properties, [&](auto const &property) { return read_on_every_row(property) && property.lookups > 1; });
      auto const pays = value_use == ValueUse::kDiscarded
                            ? removes_a_read
                            : removes_a_read || std::ranges::count_if(properties, read_on_every_row) >= 2;
      // Judged before anything is built, since most symbols fall short and a plan is one of hundreds.
      if (!pays) return std::nullopt;

      auto pass = Pass{.symbol = symbol, .properties = {}};
      for (auto const &gathered : properties) {
        if (read_on_every_row(gathered)) pass.properties.push_back(gathered.property);
      }
      return pass;
    }
  };

  /// The symbols worth a pass of their own, by what the operator does with the values. A symbol
  /// whose lookups save it too little is left out and keeps reading as it did.
  [[nodiscard]] auto SymbolsWorthOnePass(ValueUse value_use) const -> std::vector<Pass> {
    auto passes = std::vector<Pass>{};
    for (auto const &gathered : gathered_) {
      if (auto pass = gathered.PropertiesWorthOnePass(value_use)) passes.push_back(*std::move(pass));
    }
    return passes;
  }

  /// The slots a pass below filled, which lookups here may read instead of the record.
  void ReuseSlots(AssignedSlots filled) { assigned_ = std::move(filled); }

  /// Whether a lookup here reads a slot a pass below already filled, rather than the record.
  [[nodiscard]] auto ReadsAFilledSlot() const -> bool { return reads_a_filled_slot_; }

  /// Records the slot a new pass reads a property into, alongside any this operator reuses.
  void NoteSlot(Symbol const &symbol, AstPropertyIndex property, Symbol const &slot) {
    assigned_.Add(symbol, property, slot);
  }

  [[nodiscard]] auto Slots() const -> AssignedSlots const & { return assigned_; }

  /// What two plans must agree on before one can read the other's rewrite of an expression.
  [[nodiscard]] auto SlotSignature() const -> PassPlanning::SlotSignature { return assigned_.Signature(); }

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
        // A slot a pass below filled holds this row's value already, so reading it costs nothing
        // and earns no pass of its own.
        auto const property = AstPropertyIndex{op.property_};
        if (assigned_.SlotHolding(symbol, property)) {
          reads_a_filled_slot_ = true;
          return;
        }
        GatheredFor(symbol).Note(property, conditional_depth_ == 0);
        return;
      }
      case Phase::FIND: {
        if (assigned_.Holds(symbol_table_->at(*identifier))) found_one_to_replace_ = true;
        return;
      }
      case Phase::REPLACE: {
        auto const slot = assigned_.SlotHolding(symbol_table_->at(*identifier), AstPropertyIndex{op.property_});
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

  /// Which operand a row may not reach follows the operator rather than the values, so it is
  /// settled here rather than per row.
  void ShortCircuit(Expression *&always, Expression *&sometimes) {
    AcceptExpression(always);
    ++conditional_depth_;
    AcceptExpression(sometimes);
    --conditional_depth_;
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

  // Entered, with only the second operand marked as one a row may not reach, so a property read
  // on every row elsewhere is still read once and found in a slot here.
  void Visit(AndOperator &op) override { ShortCircuit(op.expression1_, op.expression2_); }

  void Visit(OrOperator &op) override { ShortCircuit(op.expression1_, op.expression2_); }

  // Always evaluated, so entering these would be safe. An entry or an element consumes the value
  // rather than handing it back, though, so a slot taken for one adds a write and removes no read.
  NOT_ENTERED(ListLiteral)
  NOT_ENTERED(MapLiteral)

#undef NOT_ENTERED

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  Phase phase_{Phase::GATHER};
  /// Short circuits between here and the operator. Zero means every row evaluates what is here.
  size_t conditional_depth_{0};
  std::vector<Expression *> prev_expressions_;
  std::vector<Gathered> gathered_;
  AssignedSlots assigned_;
  bool found_one_to_replace_{false};
  bool reads_a_filled_slot_{false};
};

template <class TDbAccessor>
class CachePropertiesRewriter final : public HierarchicalLogicalOperatorVisitor {
 public:
  CachePropertiesRewriter(SymbolTable *symbol_table, AstStorage *ast_storage, TDbAccessor *db,
                          PassPlanning *planning)
      : symbol_table_(symbol_table), ast_storage_(ast_storage), db_(db), planning_(planning) {}

  ~CachePropertiesRewriter() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once &) override { return true; }

  bool PostVisit(Produce &op) override {
    CacheBelow(op, PropertyLookupCacher::ValueUse::kKept, [&op](auto &&each) {
      for (auto *&named_expression : op.named_expressions_) each(named_expression);
    });
    return true;
  }

  bool PostVisit(Filter &op) override {
    if (!op.expression_) return true;

    CacheBelow(op, PropertyLookupCacher::ValueUse::kDiscarded, [&op](auto &&each) { each(op.expression_); });
    return true;
  }

  /// Below the aggregation, which is where the record still is: above it only the grouping keys
  /// and the aggregated values are defined.
  bool PostVisit(Aggregate &op) override {
    CacheBelow(op, PropertyLookupCacher::ValueUse::kDiscarded, [&op](auto &&each) {
      // A secondary argument, which a map collection and the graph projections take, is left out
      // however many lookups it holds. It is reached only once the row's first value is accepted,
      // so a Null value, or a repeat a distinct aggregation ignores, never evaluates it, and
      // reading ahead would reach a record such a row never asks about.
      for (auto &aggregation : op.aggregations_) {
        if (aggregation.arg1) each(aggregation.arg1);
      }
      for (auto *&group_by : op.group_by_) each(group_by);
    });
    return true;
  }

 private:
  /// Puts a one-pass read below `op` for each symbol whose lookups save it enough, which is what
  /// `value_use` decides.
  ///
  /// `for_each_expression` hands each expression the operator evaluates for every row to a visitor,
  /// which a projection and an aggregation hold differently. Each is passed by reference, since
  /// both replacing a lookup and copying an expression write back to the operator.
  template <class TForEachExpression>
  void CacheBelow(LogicalOperator &op, PropertyLookupCacher::ValueUse value_use,
                  TForEachExpression for_each_expression) {
    auto input = op.input();
    if (!input) return;

    auto cacher = PropertyLookupCacher{symbol_table_, ast_storage_};
    cacher.ReuseSlots(FilledSlotsBelow(*input));
    for_each_expression([&cacher](auto &expression) { cacher.Gather(expression); });

    // Before a slot is taken or the input is walked, because most operators read nothing worth a
    // pass and should pay only for the gathering that says so.
    auto const passes = cacher.SymbolsWorthOnePass(value_use);
    if (passes.empty() && !cacher.ReadsAFilledSlot()) return;

    auto groups = std::vector<CachedPropertyGroup>{};
    if (!passes.empty()) {
      // Taken from the expressions themselves, and before any is copied, which is what makes two
      // plans for one query agree on the operator these slots belong to.
      auto pool_key = std::vector<Tree const *>{};
      for_each_expression([&pool_key](auto &expression) { pool_key.push_back(expression); });

      auto pool = planning_->SlotsFor(pool_key);
      groups = MakeGroups(cacher, passes, *input, pool);
    }

    // Candidate plans share the expressions they were planned from, so replacing writes to a copy.
    // One expression nothing would change stays shared, since copying it would duplicate all it
    // holds, down to a whole subquery, for every candidate.
    auto const signature = cacher.SlotSignature();
    for_each_expression([this, &cacher, &signature](auto &expression) {
      if (!cacher.HasALookupToReplace(expression)) return;
      if (auto *rewritten = planning_->RewrittenFor(expression, signature)) {
        expression = rewritten;
        return;
      }
      auto *original = expression;
      expression = original->Clone(ast_storage_);
      cacher.Replace(expression);
      planning_->Remember(original, signature, expression);
    });

    if (groups.empty()) return;

    auto pass = std::make_shared<CacheProperties>(input, std::move(groups));
    // Both what this pass reads and what it reused, since a row reaching an operator above holds
    // all of it.
    slots_filled_by_.emplace(pass.get(), cacher.Slots());
    op.set_input(std::move(pass));
  }

  /// The slots a pass below `input` filled that every row reaching this operator still holds.
  ///
  /// Only an operator that neither writes a record nor rebuilds a row may stand between the two. A
  /// write would leave the slot holding what the property was before it, and an operator that
  /// restores rows restores only the symbols it was told to.
  [[nodiscard]] auto FilledSlotsBelow(LogicalOperator const &input) const -> PropertyLookupCacher::AssignedSlots {
    for (auto const *below = &input; below != nullptr;) {
      auto const filled = slots_filled_by_.find(below);
      if (filled != slots_filled_by_.end()) return filled->second;
      if (below->GetTypeInfo() != Filter::kType) break;
      below = below->HasSingleInput() ? below->input().get() : nullptr;
    }
    return {};
  }

  /// Takes a frame slot for each property of each of `passes` the input binds a record for, telling
  /// the cacher which slot each property was given, and answering with the groups the pass reads
  /// into them.
  auto MakeGroups(PropertyLookupCacher &cacher, std::vector<PropertyLookupCacher::Pass> const &passes,
                  LogicalOperator const &input, PassPlanning::SlotPool &pool) -> std::vector<CachedPropertyGroup> {
    auto const modified = input.ModifiedSymbols(*symbol_table_);
    auto groups = std::vector<CachedPropertyGroup>{};

    for (auto const &pass : passes) {
      // A symbol the input does not bind is read by something else, so there is no record here to read it from.
      if (std::ranges::find(modified, pass.symbol) == modified.end()) continue;

      auto cached_properties = std::vector<CachedProperty>{};
      cached_properties.reserve(pass.properties.size());
      for (auto const &property : pass.properties) {
        auto const output_symbol = pool.NextSlot(*symbol_table_);
        cacher.NoteSlot(pass.symbol, property, output_symbol);
        auto const &name = ast_storage_->properties_[property.Position()];
        cached_properties.push_back(CachedProperty{db_->NameToProperty(name), output_symbol});
      }
      groups.emplace_back(pass.symbol, std::move(cached_properties));
    }

    return groups;
  }

  SymbolTable *symbol_table_;
  AstStorage *ast_storage_;
  TDbAccessor *db_;
  PassPlanning *planning_;
  /// What each pass this rewrite hung leaves in the frame, so an operator above can read a slot
  /// rather than the record again.
  std::map<LogicalOperator const *, PropertyLookupCacher::AssignedSlots> slots_filled_by_;
};

}  // namespace impl

template <class TDbAccessor>
std::unique_ptr<LogicalOperator> RewriteCacheProperties(std::unique_ptr<LogicalOperator> root_op,
                                                        SymbolTable *symbol_table, AstStorage *ast_storage,
                                                        TDbAccessor *db, PassPlanning *planning) {
  if (!FLAGS_query_cache_properties) return root_op;
  auto rewriter = impl::CachePropertiesRewriter<TDbAccessor>{symbol_table, ast_storage, db, planning};
  root_op->Accept(rewriter);
  return root_op;
}

}  // namespace memgraph::query::plan
