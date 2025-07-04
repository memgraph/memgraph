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

/// @file
#pragma once

#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// TODO: remove once ast is split over multiple files
#include "query/frontend/ast/ast.hpp"

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/point_distance_condition.hpp"
#include "utils/transparent_compare.hpp"

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

  bool PostVisit(ListComprehension &list_comprehension) override {
    // Remove the symbol which is bound by list comprehension, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*list_comprehension.identifier_));
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

    if (exists.HasPattern()) {
      // We do not visit pattern identifier since we're in exists filter pattern
      for (auto &atom : exists.pattern_->atoms_) {
        atom->Accept(*this);
      }
    } else if (exists.HasSubquery()) {
      // For subqueries, we need to collect symbols from the subquery
      auto *single_query = exists.subquery_->single_query_;
      if (single_query) {
        for (auto *clause : single_query->clauses_) {
          if (auto *match = utils::Downcast<Match>(clause)) {
            for (auto *pattern : match->patterns_) {
              for (auto &atom : pattern->atoms_) {
                atom->Accept(*this);
              }
            }
          }
        }
      }
    }

    return false;
  }

  bool PostVisit(Exists & /*exists*/) override {
    in_exists = false;
    return true;
  }

  bool Visit(PrimitiveLiteral &) override { return true; }
  bool Visit(ParameterLookup &) override { return true; }
  bool Visit(EnumValueAccess &) override { return true; }

  std::unordered_set<Symbol> symbols_;
  const SymbolTable &symbol_table_;

 private:
  bool in_exists{false};
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define PREPROCESS_DEFINE_ID_TYPE(name)                                                                       \
  class name final {                                                                                          \
   private:                                                                                                   \
    explicit name(uint64_t id) : id_(id) {}                                                                   \
                                                                                                              \
   public:                                                                                                    \
    /* Default constructor to allow serialization or preallocation. */                                        \
    name() = default;                                                                                         \
                                                                                                              \
    static name FromUint(uint64_t id) { return name(id); }                                                    \
    static name FromInt(int64_t id) { return name(utils::MemcpyCast<uint64_t>(id)); }                         \
    uint64_t AsUint() const { return id_; }                                                                   \
    int64_t AsInt() const { return utils::MemcpyCast<int64_t>(id_); }                                         \
                                                                                                              \
   private:                                                                                                   \
    uint64_t id_;                                                                                             \
  };                                                                                                          \
  static_assert(std::is_trivially_copyable_v<name>, "query::plan::" #name " must be trivially copyable!");    \
  inline bool operator==(const name &first, const name &second) { return first.AsUint() == second.AsUint(); } \
  inline bool operator!=(const name &first, const name &second) { return first.AsUint() != second.AsUint(); } \
  inline bool operator<(const name &first, const name &second) { return first.AsUint() < second.AsUint(); }   \
  inline bool operator>(const name &first, const name &second) { return first.AsUint() > second.AsUint(); }   \
  inline bool operator<=(const name &first, const name &second) { return first.AsUint() <= second.AsUint(); } \
  inline bool operator>=(const name &first, const name &second) { return first.AsUint() >= second.AsUint(); }

PREPROCESS_DEFINE_ID_TYPE(ExpansionGroupId);

#undef STORAGE_DEFINE_ID_TYPE

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
  // ExpansionGroupId represents a distinct part of the matching which is not tied to any other symbols.
  ExpansionGroupId expansion_group_id = ExpansionGroupId();
  bool expand_from_edge{false};
};

/// @brief Determine if the given expression is splitted on AND or OR operators.
enum class SplitExpressionMode { AND, OR };

struct PatternComprehensionMatching;
struct FilterMatching;

enum class PatternFilterType { EXISTS_PATTERN, EXISTS_SUBQUERY };

/// Collects matchings that include patterns
class PatternVisitor : public ExpressionVisitor<void> {
 public:
  explicit PatternVisitor(SymbolTable &symbol_table, AstStorage &storage);
  PatternVisitor(const PatternVisitor &);
  PatternVisitor &operator=(const PatternVisitor &) = delete;
  PatternVisitor(PatternVisitor &&) noexcept;
  PatternVisitor &operator=(PatternVisitor &&) noexcept = delete;
  ~PatternVisitor() override;

  using ExpressionVisitor<void>::Visit;

  // Unary operators
  void Visit(NotOperator &op) override { op.expression_->Accept(*this); }
  void Visit(IsNullOperator &op) override { op.expression_->Accept(*this); };
  void Visit(UnaryPlusOperator &op) override { op.expression_->Accept(*this); }
  void Visit(UnaryMinusOperator &op) override { op.expression_->Accept(*this); }

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
  }

  void Visit(EqualOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(InListOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(AdditionOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(SubtractionOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(MultiplicationOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(DivisionOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(ModOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(ExponentiationOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(LessOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(GreaterOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(LessEqualOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(GreaterEqualOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  void Visit(RangeOperator &op) override {
    op.expr1_->Accept(*this);
    op.expr2_->Accept(*this);
  }

  void Visit(SubscriptOperator &op) override {
    op.expression1_->Accept(*this);
    op.expression2_->Accept(*this);
  }

  // Other
  void Visit(ListSlicingOperator &op) override {
    op.list_->Accept(*this);
    if (op.lower_bound_) op.lower_bound_->Accept(*this);
    if (op.upper_bound_) op.upper_bound_->Accept(*this);
  };
  void Visit(IfOperator &op) override {
    op.condition_->Accept(*this);
    op.then_expression_->Accept(*this);
    op.else_expression_->Accept(*this);
  };
  void Visit(ListLiteral &op) override {
    for (auto element_expr : op.elements_) element_expr->Accept(*this);
  }
  void Visit(MapLiteral &op) override {
    for (auto pair : op.elements_) {
      pair.second->Accept(*this);
    }
  }
  void Visit(MapProjectionLiteral &op) override {
    for (auto pair : op.elements_) {
      pair.second->Accept(*this);
    }
  }
  void Visit(LabelsTest &op) override { op.expression_->Accept(*this); };
  void Visit(Aggregation &op) override {
    if (op.expression1_) op.expression1_->Accept(*this);
    if (op.expression2_) op.expression2_->Accept(*this);
  }

  void Visit(Function &op) override {
    for (auto *argument : op.arguments_) {
      argument->Accept(*this);
    }
  }

  void Visit(Reduce &op) override {
    if (op.initializer_) op.initializer_->Accept(*this);
    if (op.expression_) op.expression_->Accept(*this);
  }
  void Visit(Coalesce &op) override {
    for (auto element_expr : op.expressions_) element_expr->Accept(*this);
  }
  void Visit(Extract &op) override {
    op.list_->Accept(*this);
    op.expression_->Accept(*this);
  }
  void Visit(Exists &op) override;
  void Visit(All &op) override {
    op.list_expression_->Accept(*this);
    op.where_->expression_->Accept(*this);
  }
  void Visit(Single &op) override {
    op.list_expression_->Accept(*this);
    op.where_->expression_->Accept(*this);
  }
  void Visit(Any &op) override {
    op.list_expression_->Accept(*this);
    op.where_->expression_->Accept(*this);
  }
  void Visit(None &op) override {
    op.list_expression_->Accept(*this);
    op.where_->expression_->Accept(*this);
  }
  void Visit(ListComprehension &op) override {
    op.list_->Accept(*this);
    if (op.where_) {
      op.where_->expression_->Accept(*this);
    }
    if (op.expression_) {
      op.expression_->Accept(*this);
    }
  }
  void Visit(Identifier &op) override{};
  void Visit(PrimitiveLiteral &op) override{};
  void Visit(PropertyLookup &op) override{};
  void Visit(AllPropertiesLookup &op) override{};
  void Visit(ParameterLookup &op) override{};
  void Visit(RegexMatch &op) override {
    op.string_expr_->Accept(*this);
    op.regex_->Accept(*this);
  }
  void Visit(NamedExpression &op) override;
  void Visit(PatternComprehension &op) override;
  void Visit(EnumValueAccess &op) override{};

  std::vector<FilterMatching> getFilterMatchings();
  std::vector<PatternComprehensionMatching> getPatternComprehensionMatchings();

  SymbolTable &symbol_table_;
  AstStorage &storage_;

 private:
  /// Collection of matchings in the filter expression being analyzed.
  std::vector<FilterMatching> filter_matchings_;

  /// Collection of matchings in the pattern comprehension being analyzed.
  std::vector<PatternComprehensionMatching> pattern_comprehension_matchings_;
};

/// Stores the symbols and expression used to filter a property.
class PropertyFilter {
 public:
  using Bound = utils::Bound<Expression *>;

  /// Depending on type, this PropertyFilter may be a value equality, regex
  /// matched value or a range with lower and (or) upper bounds, IN list filter.
  enum class Type : uint8_t { EQUAL = 0, REGEX_MATCH = 1, RANGE = 2, IN = 3, IS_NOT_NULL = 4 };

  /// Construct with Expression being the equality or regex match check.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIx, Expression *, Type);
  /// Construct the range based filter.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIx, const std::optional<Bound> &,
                 const std::optional<Bound> &);
  /// Construct with Expression being the equality or regex match check used for multiple properties.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIxPath, Expression *, Type);
  /// Construct the range based filter used for multiple properties.
  PropertyFilter(const SymbolTable &, const Symbol &, PropertyIxPath, const std::optional<Bound> &,
                 const std::optional<Bound> &);
  /// Construct a filter without an expression that produces a value.
  /// Used for the "PROP IS NOT NULL" filter, and can be used for any
  /// property filter that doesn't need to use an expression to produce
  /// values that should be filtered further.
  PropertyFilter(Symbol, PropertyIx, Type);
  PropertyFilter(Symbol, PropertyIxPath, Type);

  /// Symbol whose property is looked up.
  Symbol symbol_;
  PropertyIxPath property_ids_;
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

/// Stores the symbols and expression used to filter a point.distance/withinbbox.
struct PointFilter {
  enum class Function : uint8_t { DISTANCE, WITHINBBOX };

  PointFilter(Symbol symbol, PropertyIx property, Expression *cmp_value, PointDistanceCondition boundary_condition,
              Expression *boundary_value)
      : symbol_(std::move(symbol)),
        property_(std::move(property)),
        function_(Function::DISTANCE),
        distance_{
            .cmp_value_ = cmp_value, .boundary_value_ = boundary_value, .boundary_condition_ = boundary_condition} {}

  PointFilter(Symbol symbol, PropertyIx property, Expression *bottom_left, Expression *top_right,
              WithinBBoxCondition condition)
      : symbol_(std::move(symbol)),
        property_(std::move(property)),
        function_(Function::WITHINBBOX),
        withinbbox_{.bottom_left_ = bottom_left, .top_right_ = top_right, .condition_ = condition} {}

  PointFilter(Symbol symbol, PropertyIx property, Expression *bottom_left, Expression *top_right,
              Expression *boundary_value)
      : symbol_(std::move(symbol)),
        property_(std::move(property)),
        function_(Function::WITHINBBOX),
        withinbbox_{
            .bottom_left_ = bottom_left, .top_right_ = top_right, .boundary_value_ = boundary_value, .condition_ = {}} {
  }

  /// Symbol whose property is looked up.
  Symbol symbol_;
  PropertyIx property_;
  Function function_;
  union {
    struct {
      Expression *cmp_value_ = nullptr;
      Expression *boundary_value_ = nullptr;
      PointDistanceCondition boundary_condition_;
    } distance_;
    struct {
      Expression *bottom_left_ = nullptr;
      Expression *top_right_ = nullptr;
      Expression *boundary_value_ = nullptr;
      std::optional<WithinBBoxCondition> condition_;
    } withinbbox_;
  };
};

/// Filtering by ID, for example `MATCH (n) WHERE id(n) = 42 ...`
class IdFilter {
 public:
  /// Construct with Expression being the required value for ID.
  IdFilter(const SymbolTable &, const Symbol &, Expression *);

  /// Symbol whose id is looked up.
  Symbol symbol_;
  /// Expression which when evaluted produces the value an ID must satisfy.
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
  enum class Type { Generic, Label, Property, Id, Pattern, Point };

  // FilterInfo is tricky because FilterMatching is not yet defined:
  //   * if no declared constructor -> FilterInfo is std::__is_complete_or_unbounded
  //   * if any user-declared constructor -> non-aggregate type -> no designated initializers are possible
  //   * IMPORTANT: Matchings will always be initialized to an empty container.
  explicit FilterInfo(Type type = Type::Generic, Expression *expression = nullptr,
                      std::unordered_set<Symbol> used_symbols = {}, std::optional<PropertyFilter> property_filter = {},
                      std::optional<IdFilter> id_filter = {});
  // All other constructors are also defined in the cpp file because this struct is incomplete here.
  FilterInfo(const FilterInfo &);
  FilterInfo &operator=(const FilterInfo &);
  FilterInfo(FilterInfo &&) noexcept;
  FilterInfo &operator=(FilterInfo &&) noexcept;
  ~FilterInfo();

  Type type{Type::Generic};
  /// The original filter expression which must be satisfied.
  Expression *expression{nullptr};
  /// Set of used symbols by the filter @c expression.
  std::unordered_set<Symbol> used_symbols{};
  /// Labels for Type::Label filtering.
  std::vector<LabelIx> labels{};
  /// Labels for Type::Label OR filtering.
  std::vector<std::vector<LabelIx>> or_labels{};
  /// Property information for Type::Property filtering.
  std::optional<PropertyFilter> property_filter{};
  /// Information for Type::Id filtering.
  std::optional<IdFilter> id_filter{};
  /// Matchings for filters that include patterns
  /// NOTE: The vector is not defined here because FilterMatching is forward declared above.
  std::vector<FilterMatching> matchings;
  /// Information for Type::Point filtering.
  std::optional<PointFilter> point_filter{};
};

/// Stores information on filters used inside the @c Matching of a @c QueryPart.
///
/// Info is stored as a list of FilterInfo objects corresponding to all filter
/// expressions that should be generated.
class Filters final {
 public:
  using iterator = std::vector<FilterInfo>::iterator;
  using const_iterator = std::vector<FilterInfo>::const_iterator;

  auto begin() -> iterator { return all_filters_.begin(); }
  auto begin() const -> const_iterator { return all_filters_.begin(); }
  auto end() -> iterator { return all_filters_.end(); }
  auto end() const -> const_iterator { return all_filters_.end(); }

  auto empty() const -> bool { return all_filters_.empty(); }

  auto erase(iterator pos) -> iterator;
  auto erase(const_iterator pos) -> iterator;
  auto erase(iterator first, iterator last) -> iterator;
  auto erase(const_iterator first, const_iterator last) -> iterator;

  void SetFilters(std::vector<FilterInfo> &&all_filters) { all_filters_ = std::move(all_filters); }

  auto FilteredLabels(const Symbol &symbol) const -> std::unordered_set<LabelIx>;
  auto FilteredOrLabels(const Symbol &symbol) const -> std::vector<std::vector<LabelIx>>;
  auto FilteredProperties(const Symbol &symbol) const -> std::set<PropertyIxPath>;

  /// Remove a filter; may invalidate iterators.
  /// Removal is done by comparing only the expression, so that multiple
  /// FilterInfo objects using the same original expression are removed.
  void EraseFilter(const FilterInfo &);

  /// Remove a label filter for symbol; may invalidate iterators.
  /// If removed_filters is not nullptr, fills the vector with original
  /// `Expression *` which are now completely removed.
  void EraseLabelFilter(const Symbol &symbol, const LabelIx &label,
                        std::vector<Expression *> *removed_filters = nullptr);

  /// Remove a label filter for OR expression for symbol; may invalidate iterators.
  /// If removed_filters is not nullptr, fills the vector with original
  /// `Expression *` which are now completely removed.
  void EraseOrLabelFilter(const Symbol &symbol, const std::vector<LabelIx> &labels,
                          std::vector<Expression *> *removed_filters = nullptr);

  /// Returns a vector of FilterInfo for properties.
  auto PropertyFilters(const Symbol &symbol) const -> std::vector<FilterInfo>;

  auto PointFilters(const Symbol &symbol) const -> std::vector<FilterInfo>;

  /// Return a vector of FilterInfo for ID equality filtering.
  auto IdFilters(const Symbol &symbol) const -> std::vector<FilterInfo>;

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
  std::vector<FilterInfo> all_filters_;
  void AnalyzeAndStoreFilter(Expression *, const SymbolTable &);
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
  /// Maps atom symbols to expansions which bind them.
  std::unordered_map<Symbol, std::set<size_t>> atom_symbol_to_expansions{};
  /// Tracker of the total number of expansion groups for correct assigning of expansion group IDs
  size_t number_of_expansion_groups{0};
  /// Maps every node symbol to its expansion group ID
  std::unordered_map<Symbol, ExpansionGroupId> node_symbol_to_expansion_group_id{};
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
  /// For EXISTS_SUBQUERY, holds the full subquery QueryParts
  std::shared_ptr<QueryParts> subquery;
};

inline auto Filters::erase(Filters::iterator pos) -> iterator { return all_filters_.erase(pos); }
inline auto Filters::erase(Filters::const_iterator pos) -> iterator { return all_filters_.erase(pos); }
inline auto Filters::erase(Filters::iterator first, Filters::iterator last) -> iterator {
  return all_filters_.erase(first, last);
}
inline auto Filters::erase(Filters::const_iterator first, Filters::const_iterator last) -> iterator {
  return all_filters_.erase(first, last);
}

// Returns label filters. Labels can refer to node and its labels or to an edge with respective edge type
inline auto Filters::FilteredLabels(const Symbol &symbol) const -> std::unordered_set<LabelIx> {
  std::unordered_set<LabelIx> labels;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Label && utils::Contains(filter.used_symbols, symbol)) {
      MG_ASSERT(filter.used_symbols.size() == 1U, "Expected a single used symbol for label filter");
      labels.insert(filter.labels.begin(), filter.labels.end());
    }
  }
  return labels;
}

inline auto Filters::FilteredOrLabels(const Symbol &symbol) const -> std::vector<std::vector<LabelIx>> {
  std::vector<std::vector<LabelIx>> or_labels;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Label && utils::Contains(filter.used_symbols, symbol)) {
      or_labels.insert(or_labels.end(), filter.or_labels.begin(), filter.or_labels.end());
    }
  }
  return or_labels;
}

inline auto Filters::FilteredProperties(const Symbol &symbol) const -> std::set<PropertyIxPath> {
  std::set<PropertyIxPath> properties;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Property && filter.property_filter->symbol_ == symbol) {
      properties.insert(filter.property_filter->property_ids_);
    }
  }
  return properties;
}

inline auto Filters::PropertyFilters(const Symbol &symbol) const -> std::vector<FilterInfo> {
  std::vector<FilterInfo> filters;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Property && filter.property_filter->symbol_ == symbol) {
      filters.push_back(filter);
    }
  }
  return filters;
}

inline auto Filters::PointFilters(const Symbol &symbol) const -> std::vector<FilterInfo> {
  std::vector<FilterInfo> filters;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Point && filter.point_filter->symbol_ == symbol) {
      filters.push_back(filter);
    }
  }
  return filters;
}

inline auto Filters::IdFilters(const Symbol &symbol) const -> std::vector<FilterInfo> {
  std::vector<FilterInfo> filters;
  for (const auto &filter : all_filters_) {
    if (filter.type == FilterInfo::Type::Id && filter.id_filter->symbol_ == symbol) {
      filters.push_back(filter);
    }
  }
  return filters;
}

struct PatternComprehensionMatching : Matching {
  /// Pattern comprehension result named expression
  NamedExpression *result_expr = nullptr;
  Symbol result_symbol;
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
  /// Foreach @c does not violate this gurantee. However, update clauses are not stored
  /// in the `remaining_clauses` but rather in the `Foreach` itself and are guranteed
  /// to be processed in the same order by the semantics of the `RuleBasedPlanner`.
  std::vector<Matching> merge_matching{};

  /// @brief @c NamedExpression name to @c PatternComprehensionMatching for each pattern comprehension.
  ///
  /// Storing the normalized pattern of a @c PatternComprehension does not preclude storing the
  /// @c PatternComprehension clause itself inside `remaining_clauses`. The reason is that we
  /// need to have access to other parts of the clause, such as pattern, filter clauses.
  std::unordered_map<std::string, PatternComprehensionMatching> pattern_comprehension_matchings{};

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
  /// Commit frequency for periodic commit
  Expression *commit_frequency = nullptr;
  bool is_subquery = false;
};

/// @brief Convert the AST to multiple @c QueryParts.
///
/// This function will normalize patterns inside @c Match and @c Merge clauses
/// and do some other preprocessing in order to generate multiple @c QueryPart
/// structures. @c AstStorage and @c SymbolTable may be used to create new
/// AST nodes.
QueryParts CollectQueryParts(SymbolTable &, AstStorage &, CypherQuery *, bool is_subquery);

/**
 * @brief Split expression on AND operators; useful for splitting single filters
 *
 * @param expression
 * @return std::vector<Expression *>
 */
std::vector<Expression *> SplitExpression(Expression *expression, SplitExpressionMode mode = SplitExpressionMode::AND);

/**
 * @brief Substitute an expression with a new one.
 * @note Whole expression gets split at every AND and its branches are compared and subsituted
 *
 * @param expr whole expression
 * @param old expression to replace
 * @param in expression to embed
 * @return Expression *
 */
Expression *SubstituteExpression(Expression *expr, Expression *old, Expression *in);

}  // namespace memgraph::query::plan
