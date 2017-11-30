/// @file
#pragma once

#include <experimental/optional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace query::plan {

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

/// Stores the symbols and expression used to filter a property.
class PropertyFilter {
 public:
  using Bound = ScanAllByLabelPropertyRange::Bound;

  PropertyFilter(const SymbolTable &, const Symbol &,
                 const GraphDbTypes::Property &, Expression *);
  PropertyFilter(const SymbolTable &, const Symbol &,
                 const GraphDbTypes::Property &,
                 const std::experimental::optional<Bound> &,
                 const std::experimental::optional<Bound> &);

  /// Symbol whose property is looked up.
  Symbol symbol_;
  GraphDbTypes::Property property_;
  /// True if the same symbol is used in expressions for value or bounds.
  bool is_symbol_in_value_ = false;
  /// Expression which when evaluated produces the value a property must
  /// equal.
  Expression *value_ = nullptr;
  /// Expressions which produce lower and upper bounds for a property.
  std::experimental::optional<Bound> lower_bound_{};
  std::experimental::optional<Bound> upper_bound_{};
};

/// Stores additional information for a filter expression.
struct FilterInfo {
  /// A FilterInfo can be a generic filter expression or a specific filtering
  /// applied for labels or a property. Non generic types contain extra
  /// information which can be used to produce indexed scans of graph
  /// elements.
  enum class Type { Generic, Label, Property };

  Type type;
  /// The filter expression which must be satisfied.
  Expression *expression;
  /// Set of used symbols by the filter @c expression.
  std::unordered_set<Symbol> used_symbols;
  /// Labels for Type::Label filtering.
  std::vector<GraphDbTypes::Label> labels;
  /// Property information for Type::Property filtering.
  std::experimental::optional<PropertyFilter> property_filter;
};

/// Stores information on filters used inside the @c Matching of a @c QueryPart.
///
/// Info is stored as a list of FilterInfo objects corresponding to all filter
/// expressions that should be generated.
class Filters {
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
  auto erase(iterator first, iterator last) {
    return all_filters_.erase(first, last);
  }
  auto erase(const_iterator first, const_iterator last) {
    return all_filters_.erase(first, last);
  }

  auto FilteredLabels(const Symbol &symbol) const {
    std::unordered_set<GraphDbTypes::Label> labels;
    for (const auto &filter : all_filters_) {
      if (filter.type == FilterInfo::Type::Label &&
          utils::Contains(filter.used_symbols, symbol)) {
        DCHECK(filter.used_symbols.size() == 1U)
            << "Expected a single used symbol for label filter";
        labels.insert(filter.labels.begin(), filter.labels.end());
      }
    }
    return labels;
  }

  // Remove a filter; may invalidate iterators.
  // Removal is done by comparing only the expression, so that multiple
  // FilterInfo objects using the same original expression are removed.
  void EraseFilter(const FilterInfo &);

  // Remove a label filter for symbol; may invalidate iterators.
  void EraseLabelFilter(const Symbol &, const GraphDbTypes::Label &);

  // Returns a vector of FilterInfo for properties.
  auto PropertyFilters(const Symbol &symbol) const {
    std::vector<FilterInfo> filters;
    for (const auto &filter : all_filters_) {
      if (filter.type == FilterInfo::Type::Property &&
          filter.property_filter->symbol_ == symbol) {
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
  void CollectPatternFilters(Pattern &, SymbolTable &, AstTreeStorage &);
  /// Collects filtering information from a where expression.
  ///
  /// Takes the where expression and stores it, then analyzes the expression for
  /// additional information. The additional information is used to populate
  /// label filters and property filters, so that indexed scanning can use it.
  void CollectWhereFilter(Where &, const SymbolTable &);

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
  std::unordered_map<Symbol, std::set<int>> node_symbol_to_expansions{};
  /// Maps named path symbols to a vector of Symbols that define its pattern.
  std::unordered_map<Symbol, std::vector<Symbol>> named_paths{};
  /// All node and edge symbols across all expansions (from all matches).
  std::unordered_set<Symbol> expansion_symbols{};
};

/// @brief Represents a read (+ write) part of a query. Parts are split on
/// `WITH` clauses.
///
/// Each part ends with either:
///
///  * `RETURN` clause;
///  * `WITH` clause or
///  * any of the write clauses.
///
/// For a query `MATCH (n) MERGE (n) -[e]- (m) SET n.x = 42 MERGE (l)` the
/// generated QueryPart will have `matching` generated for the `MATCH`.
/// `remaining_clauses` will contain `Merge`, `SetProperty` and `Merge` clauses
/// in that exact order. The pattern inside the first `MERGE` will be used to
/// generate the first `merge_matching` element, and the second `MERGE` pattern
/// will produce the second `merge_matching` element. This way, if someone
/// traverses `remaining_clauses`, the order of appearance of `Merge` clauses is
/// in the same order as their respective `merge_matching` elements.
struct QueryPart {
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
  std::vector<Matching> merge_matching{};
  /// @brief All the remaining clauses (without @c Match).
  std::vector<Clause *> remaining_clauses{};
};

/// @brief Convert the AST to multiple @c QueryParts.
///
/// This function will normalize patterns inside @c Match and @c Merge clauses
/// and do some other preprocessing in order to generate multiple @c QueryPart
/// structures. @c AstTreeStorage and @c SymbolTable may be used to create new
/// AST nodes.
std::vector<QueryPart> CollectQueryParts(SymbolTable &, AstTreeStorage &);

}  // namespace query::plan
