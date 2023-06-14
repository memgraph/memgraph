// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

/// Collects symbols from identifiers found in visited AST nodes.
class UsedSymbolsCollector : public HierarchicalTreeVisitor {
 public:
  explicit UsedSymbolsCollector(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  bool PostVisit(All &all) override {
    // Remove the symbol which is bound by all, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*all.identifier_));
    return true;
  }

  bool PostVisit(Single &single) override {
    // Remove the symbol which is bound by single, because we are only
    // interested in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*single.identifier_));
    return true;
  }

  bool PostVisit(Any &any) override {
    // Remove the symbol which is bound by any, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*any.identifier_));
    return true;
  }

  bool PostVisit(None &none) override {
    // Remove the symbol which is bound by none, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*none.identifier_));
    return true;
  }

  bool PostVisit(Reduce &reduce) override {
    // Remove the symbols bound by reduce, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*reduce.accumulator_));
    symbols_.erase(symbol_table_.at(*reduce.identifier_));
    return true;
  }

  bool Visit(Identifier &ident) override {
    if (!in_exists || ident.user_declared_) {
      symbols_.insert(symbol_table_.at(ident));
    }

    return true;
  }

  bool PreVisit(Exists &exists) override {
    in_exists = true;

    // We do not visit pattern identifier since we're in exists filter pattern
    for (auto &atom : exists.pattern_->atoms_) {
      atom->Accept(*this);
    }

    return false;
  }

  bool PostVisit(Exists & /*exists*/) override {
    in_exists = false;
    return true;
  }

  bool Visit(PrimitiveLiteral &) override { return true; }
  bool Visit(ParameterLookup &) override { return true; }

  std::unordered_set<Symbol> symbols_;
  const SymbolTable &symbol_table_;

 private:
  bool in_exists{false};
};

/// Normalized representation of a pattern that needs to be matched.
struct Expansion {
  /// The first node in the expansion, it can be a single node.
  NodeAtom *node1 = nullptr;
  /// Optional edge which connects the 2 nodes.
  EdgeAtom *edge = nullptr;
  /// Direction of the edge, it may be flipped compared to original
  /// @c EdgeAtom during plan generation.
  EdgeAtom::Direction direction = EdgeAtom::Direction::BOTH;
  /// True if the direction and nodes were flipped.
  bool is_flipped = false;
  /// Set of symbols found inside the range expressions of a variable path edge.
  std::unordered_set<Symbol> symbols_in_range{};
  /// Optional node at the other end of an edge. If the expansion
  /// contains an edge, then this node is required.
  NodeAtom *node2 = nullptr;
};

struct FilterMatching;

enum class PatternFilterType { EXISTS };

/// Collects matchings from filters that include patterns
class PatternFilterVisitor : public ExpressionVisitor<void> {
 public:
  explicit PatternFilterVisitor(SymbolTable &symbol_table, AstStorage &storage)
      : symbol_table_(symbol_table), storage_(storage) {}

  using ExpressionVisitor<void>::Visit;

  // Unary operators
  void Visit(NotOperator &op) override { op.expression_->Accept(*this); }
  void Visit(IsNullOperator &op) override { op.expression_->Accept(*this); };
  void Visit(UnaryPlusOperator &op) override{};
  void Visit(UnaryMinusOperator &op) override{};

  // Binary operators
  void Visit(OrOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }
  void Visit(XorOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }
  void Visit(AndOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }
  void Visit(NotEqualOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  };
  void Visit(EqualOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  };
  void Visit(InListOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  };
  void Visit(AdditionOperator &op) override{};
  void Visit(SubtractionOperator &op) override{};
  void Visit(MultiplicationOperator &op) override{};
  void Visit(DivisionOperator &op) override{};
  void Visit(ModOperator &op) override{};
  void Visit(LessOperator &op) override{};
  void Visit(GreaterOperator &op) override{};
  void Visit(LessEqualOperator &op) override{};
  void Visit(GreaterEqualOperator &op) override{};
  void Visit(SubscriptOperator &op) override{};

  // Other
  void Visit(ListSlicingOperator &op) override{};
  void Visit(IfOperator &op) override{};
  void Visit(ListLiteral &op) override{};
  void Visit(MapLiteral &op) override{};
  void Visit(MapProjectionLiteral &op) override{};
  void Visit(LabelsTest &op) override{};
  void Visit(Aggregation &op) override{};
  void Visit(Function &op) override{};
  void Visit(Reduce &op) override{};
  void Visit(Coalesce &op) override{};
  void Visit(Extract &op) override{};
  void Visit(Exists &op) override;
  void Visit(All &op) override{};
  void Visit(Single &op) override{};
  void Visit(Any &op) override{};
  void Visit(None &op) override{};
  void Visit(Identifier &op) override{};
  void Visit(PrimitiveLiteral &op) override{};
  void Visit(PropertyLookup &op) override{};
  void Visit(AllPropertiesLookup &op) override{};
  void Visit(ParameterLookup &op) override{};
  void Visit(NamedExpression &op) override{};
  void Visit(RegexMatch &op) override{};

  std::vector<FilterMatching> getMatchings() { return matchings_; }

  SymbolTable &symbol_table_;
  AstStorage &storage_;

 private:
  /// Collection of matchings in the filter expression being analyzed.
  std::vector<FilterMatching> matchings_;
};

/// Stores the symbols and expression used to filter a property.
class PropertyFilter {
 public:
  using Bound = ScanAllByLabelPropertyRange::Bound;

  /// Depending on type, this PropertyFilter may be a value equality, regex
  /// matched value or a range with lower and (or) upper bounds, IN list filter.
  enum class Type { EQUAL, REGEX_MATCH, RANGE, IN, IS_NOT_NULL };

  /// Construct with Expression being the equality or regex match check.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIx, Expression *, Type);
  /// Construct the range based filter.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIx, const std::optional<Bound> &,
                 const std::optional<Bound> &);
  /// Construct a filter without an expression that produces a value.
  /// Used for the "PROP IS NOT NULL" filter, and can be used for any
  /// property filter that doesn't need to use an expression to produce
  /// values that should be filtered further.
  PropertyFilter(const Symbol &, PropertyIx, Type);

  /// Symbol whose property is looked up.
  Symbol symbol_;
  PropertyIx property_;
  Type type_;
  /// True if the same symbol is used in expressions for value or bounds.
  bool is_symbol_in_value_ = false;
  /// Expression which when evaluated produces the value a property must
  /// equal or regex match depending on type_.
  Expression *value_ = nullptr;
  /// Expressions which produce lower and upper bounds for a property.
  std::optional<Bound> lower_bound_{};
  std::optional<Bound> upper_bound_{};
};

/// Filtering by ID, for example `MATCH (n) WHERE id(n) = 42 ...`
class IdFilter {
 public:
  /// Construct with Expression being the required value for ID.
  IdFilter(const SymbolTable &, const Symbol &, Expression *);

  /// Symbol whose id is looked up.
  Symbol symbol_;
  /// Expression which when evaluated produces the value an ID must satisfy.
  Expression *value_;
  /// True if the same symbol is used in expressions for value.
  bool is_symbol_in_value_{false};
};

/// Stores additional information for a filter expression.
struct FilterInfo {
  /// A FilterInfo can be a generic filter expression or a specific filtering
  /// applied for labels or a property. Non generic types contain extra
  /// information which can be used to produce indexed scans of graph
  /// elements.
  enum class Type { Generic, Label, Property, Id, Pattern };

  Type type;
  /// The original filter expression which must be satisfied.
  Expression *expression;
  /// Set of used symbols by the filter @c expression.
  std::unordered_set<Symbol> used_symbols;
  /// Labels for Type::Label filtering.
  std::vector<LabelIx> labels;
  /// Property information for Type::Property filtering.
  std::optional<PropertyFilter> property_filter;
  /// Information for Type::Id filtering.
  std::optional<IdFilter> id_filter;
  /// Matchings for filters that include patterns
  std::vector<FilterMatching> matchings;
};

/// Stores information on filters used inside the @c Matching of a @c QueryPart.
///
/// Info is stored as a list of FilterInfo objects corresponding to all filter
/// expressions that should be generated.
class Filters final {
 public:
  using iterator = std::vector<FilterInfo>::iterator;
  using const_iterator = std::vector<FilterInfo>::const_iterator;

  auto begin() { return all_filters_.begin(); }
  auto begin() const { return all_filters_.begin(); }
  auto end() { return all_filters_.end(); }
  auto end() const { return all_filters_.end(); }

  auto empty() const { return all_filters_.empty(); }

  auto erase(iterator pos) { return all_filters_.erase(pos); }
  auto erase(const_iterator pos) { return all_filters_.erase(pos); }
  auto erase(iterator first, iterator last) { return all_filters_.erase(first, last); }
  auto erase(const_iterator first, const_iterator last) { return all_filters_.erase(first, last); }

  auto FilteredLabels(const Symbol &symbol) const {
    std::unordered_set<LabelIx> labels;
    for (const auto &filter : all_filters_) {
      if (filter.type == FilterInfo::Type::Label && utils::Contains(filter.used_symbols, symbol)) {
        MG_ASSERT(filter.used_symbols.size() == 1U, "Expected a single used symbol for label filter");
        labels.insert(filter.labels.begin(), filter.labels.end());
      }
    }
    return labels;
  }

  /// Remove a filter; may invalidate iterators.
  /// Removal is done by comparing only the expression, so that multiple
  /// FilterInfo objects using the same original expression are removed.
  void EraseFilter(const FilterInfo &);

  /// Remove a label filter for symbol; may invalidate iterators.
  /// If removed_filters is not nullptr, fills the vector with original
  /// `Expression *` which are now completely removed.
  void EraseLabelFilter(const Symbol &, LabelIx, std::vector<Expression *> *removed_filters = nullptr);

  /// Returns a vector of FilterInfo for properties.
  auto PropertyFilters(const Symbol &symbol) const {
    std::vector<FilterInfo> filters;
    for (const auto &filter : all_filters_) {
      if (filter.type == FilterInfo::Type::Property && filter.property_filter->symbol_ == symbol) {
        filters.push_back(filter);
      }
    }
    return filters;
  }

  /// Return a vector of FilterInfo for ID equality filtering.
  auto IdFilters(const Symbol &symbol) const {
    std::vector<FilterInfo> filters;
    for (const auto &filter : all_filters_) {
      if (filter.type == FilterInfo::Type::Id && filter.id_filter->symbol_ == symbol) {
        filters.push_back(filter);
      }
    }
    return filters;
  }

  /// Collects filtering information from a pattern.
  ///
  /// Goes through all the atoms in a pattern and generates filter expressions
  /// for found labels, properties and edge types. The generated expressions are
  /// stored.
  void CollectPatternFilters(Pattern &, SymbolTable &, AstStorage &);

  /// Collects filtering information from a where expression.
  ///
  /// Takes the where expression and stores it, then analyzes the expression for
  /// additional information. The additional information is used to populate
  /// label filters and property filters, so that indexed scanning can use it.
  void CollectWhereFilter(Where &, const SymbolTable &);

  /// Collects filtering information from an expression.
  ///
  /// Takes the where expression and stores it, then analyzes the expression for
  /// additional information. The additional information is used to populate
  /// label filters and property filters, so that indexed scanning can use it.
  void CollectFilterExpression(Expression *, const SymbolTable &);

 private:
  void AnalyzeAndStoreFilter(Expression *, const SymbolTable &);

  std::vector<FilterInfo> all_filters_;
};

/// Normalized representation of a single or multiple Match clauses.
///
/// For example, `MATCH (a :Label) -[e1]- (b) -[e2]- (c) MATCH (n) -[e3]- (m)
/// WHERE c.prop < 42` will produce the following.
/// Expansions will store `(a) -[e1]-(b)`, `(b) -[e2]- (c)` and
/// `(n) -[e3]- (m)`.
/// Edge symbols for Cyphermorphism will only contain the set `{e1, e2}` for the
/// first `MATCH` and the set `{e3}` for the second.
/// Filters will contain 2 pairs. One for testing `:Label` on symbol `a` and the
/// other obtained from `WHERE` on symbol `c`.
struct Matching {
  /// All expansions that need to be performed across @c Match clauses.
  std::vector<Expansion> expansions;
  /// Symbols for edges established in match, used to ensure Cyphermorphism.
  ///
  /// There are multiple sets, because each Match clause determines a single
  /// set.
  std::vector<std::unordered_set<Symbol>> edge_symbols;
  /// Information on used filter expressions while matching.
  Filters filters;
  /// Maps node symbols to expansions which bind them.
  std::unordered_map<Symbol, std::set<size_t>> node_symbol_to_expansions{};
  /// Maps named path symbols to a vector of Symbols that define its pattern.
  std::unordered_map<Symbol, std::vector<Symbol>> named_paths{};
  /// All node and edge symbols across all expansions (from all matches).
  std::unordered_set<Symbol> expansion_symbols{};
};

// TODO clumsy to need to declare it before, usually only the struct definition would be in header
struct QueryParts;

struct FilterMatching : Matching {
  /// Type of pattern filter
  PatternFilterType type;
  /// Symbol for the filter expression
  std::optional<Symbol> symbol;
};

/// @brief Represents a read (+ write) part of a query. Parts are split on
/// `WITH` clauses.
///
/// Each part ends with either:
///
///  * `RETURN` clause;
///  * `WITH` clause;
///  * `UNWIND` clause;
///  * `CALL` clause or
///  * any of the write clauses.
///
/// For a query `MATCH (n) MERGE (n) -[e]- (m) SET n.x = 42 MERGE (l)` the
/// generated SingleQueryPart will have `matching` generated for the `MATCH`.
/// `remaining_clauses` will contain `Merge`, `SetProperty` and `Merge` clauses
/// in that exact order. The pattern inside the first `MERGE` will be used to
/// generate the first `merge_matching` element, and the second `MERGE` pattern
/// will produce the second `merge_matching` element. This way, if someone
/// traverses `remaining_clauses`, the order of appearance of `Merge` clauses is
/// in the same order as their respective `merge_matching` elements.
/// An exception to the above rule is Foreach. Its update clauses will not be contained in
/// the `remaining_clauses`, but rather inside the foreach itself. The order guarantee is not
/// violated because the update clauses of the foreach are immediately processed in
/// the `RuleBasedPlanner` as if as they were pushed into the `remaining_clauses`.
struct SingleQueryPart {
  /// @brief All `MATCH` clauses merged into one @c Matching.
  Matching matching;
  /// @brief Each `OPTIONAL MATCH` converted to @c Matching.
  std::vector<Matching> optional_matching{};
  /// @brief @c Matching for each `MERGE` clause.
  ///
  /// Storing the normalized pattern of a @c Merge does not preclude storing the
  /// @c Merge clause itself inside `remaining_clauses`. The reason is that we
  /// need to have access to other parts of the clause, such as `SET` clauses
  /// which need to be run.
  ///
  /// Since @c Merge is contained in `remaining_clauses`, this vector contains
  /// matching in the same order as @c Merge appears.
  //
  /// Foreach @c does not violate this guarantee. However, update clauses are not stored
  /// in the `remaining_clauses` but rather in the `Foreach` itself and are guaranteed
  /// to be processed in the same order by the semantics of the `RuleBasedPlanner`.
  std::vector<Matching> merge_matching{};
  /// @brief All the remaining clauses (without @c Match).
  std::vector<Clause *> remaining_clauses{};
  /// The subqueries vector are all the subqueries in this query part ordered in a list by
  /// the order of calling.
  std::vector<std::shared_ptr<QueryParts>> subqueries{};
};

/// Holds query parts of a single query together with the optional information
/// about the combinator used between this single query and the previous one.
struct QueryPart {
  std::vector<SingleQueryPart> single_query_parts = {};
  /// Optional AST query combinator node
  Tree *query_combinator = nullptr;
};

/// Holds query parts of all single queries together with the information
/// whether or not the resulting set should contain distinct elements.
struct QueryParts {
  std::vector<QueryPart> query_parts = {};
  /// Distinct flag, determined by the query combinator
  bool distinct = false;
};

/// @brief Convert the AST to multiple @c QueryParts.
///
/// This function will normalize patterns inside @c Match and @c Merge clauses
/// and do some other preprocessing in order to generate multiple @c QueryPart
/// structures. @c AstStorage and @c SymbolTable may be used to create new
/// AST nodes.
QueryParts CollectQueryParts(SymbolTable &, AstStorage &, CypherQuery *);

}  // namespace memgraph::query::plan
