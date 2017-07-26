/// @file
#pragma once

#include <memory>

#include "query/plan/operator.hpp"

namespace query {

class AstTreeStorage;
class SymbolTable;

namespace plan {

/// Normalized representation of a pattern that needs to be matched.
struct Expansion {
  /// The first node in the expansion, it can be a single node.
  NodeAtom *node1 = nullptr;
  /// Optional edge which connects the 2 nodes.
  EdgeAtom *edge = nullptr;
  /// Direction of the edge, it may be flipped compared to original
  /// @c EdgeAtom during plan generation.
  EdgeAtom::Direction direction = EdgeAtom::Direction::BOTH;
  /// Set of symbols found inside the range expressions of a variable path edge.
  std::unordered_set<Symbol> symbols_in_range;
  /// Optional node at the other end of an edge. If the expansion
  /// contains an edge, then this node is required.
  NodeAtom *node2 = nullptr;
};

/// Stores information on filters used inside the @c Matching of a @c QueryPart.
class Filters {
 public:
  /// Stores the symbols and expression used to filter a property.
  struct PropertyFilter {
    using Bound = ScanAllByLabelPropertyRange::Bound;

    /// Set of used symbols in the @c expression.
    std::unordered_set<Symbol> used_symbols;
    /// Expression which when evaluated produces the value a property must
    /// equal.
    Expression *expression = nullptr;
    std::experimental::optional<Bound> lower_bound;
    std::experimental::optional<Bound> upper_bound;
  };

  /// All filter expressions that should be generated.
  auto &all_filters() { return all_filters_; }
  const auto &all_filters() const { return all_filters_; }
  /// Mapping from a symbol to labels that are filtered on it. These should be
  /// used only for generating indexed scans.
  const auto &label_filters() const { return label_filters_; }
  /// Mapping from a symbol to properties that are filtered on it. These should
  /// be used only for generating indexed scans.
  const auto &property_filters() const { return property_filters_; }

  /// Collects filtering information from a pattern.
  ///
  /// Goes through all the atoms in a pattern and generates filter expressions
  /// for found labels, properties and edge types. The generated expressions are
  /// stored in @c all_filters. Also, @c label_filters and @c property_filters
  /// are populated.
  void CollectPatternFilters(Pattern &, SymbolTable &, AstTreeStorage &);
  /// Collects filtering information from a where expression.
  ///
  /// Takes the where expression and stores it in @c all_filters, then analyzes
  /// the expression for additional information. The additional information is
  /// used to populate @c label_filters and @c property_filters, so that indexed
  /// scanning can use it.
  void CollectWhereFilter(Where &, const SymbolTable &);

 private:
  void AnalyzeFilter(Expression *, const SymbolTable &);

  std::vector<std::pair<Expression *, std::unordered_set<Symbol>>> all_filters_;
  std::unordered_map<Symbol, std::set<GraphDbTypes::Label>> label_filters_;
  std::unordered_map<
      Symbol, std::map<GraphDbTypes::Property, std::vector<PropertyFilter>>>
      property_filters_;
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
  std::vector<Matching> optional_matching;
  /// @brief @c Matching for each `MERGE` clause.
  ///
  /// Storing the normalized pattern of a @c Merge does not preclude storing the
  /// @c Merge clause itself inside `remaining_clauses`. The reason is that we
  /// need to have access to other parts of the clause, such as `SET` clauses
  /// which need to be run.
  ///
  /// Since @c Merge is contained in `remaining_clauses`, this vector contains
  /// matching in the same order as @c Merge appears.
  std::vector<Matching> merge_matching;
  /// @brief All the remaining clauses (without @c Match).
  std::vector<Clause *> remaining_clauses;
};

/// @brief Context which contains variables commonly used during planning.
struct PlanningContext {
  /// @brief SymbolTable is used to determine inputs and outputs of planned
  /// operators.
  ///
  /// Newly created AST nodes may be added to reference existing symbols.
  SymbolTable &symbol_table;
  /// @brief The storage is used to traverse the AST as well as create new nodes
  /// for use in operators.
  AstTreeStorage &ast_storage;
  /// @brief GraphDbAccessor, which may be used to get some information from the
  /// database to generate better plans. The accessor is required only to live
  /// long enough for the plan generation to finish.
  const GraphDbAccessor &db;
  /// @brief Symbol set is used to differentiate cycles in pattern matching.
  ///
  /// During planning, symbols will be added as each operator produces values
  /// for them. This way, the operator can be correctly initialized whether to
  /// read a symbol or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and
  /// write) the first `n`, but the latter `n` would only read the already
  /// written information.
  std::unordered_set<Symbol> bound_symbols;
};

/// @brief Planner which uses hardcoded rules to produce operators.
///
/// @sa MakeLogicalPlan
class RuleBasedPlanner {
 public:
  RuleBasedPlanner(PlanningContext &context) : context_(context) {}

  /// @brief The result of plan generation is the root of the generated operator
  /// tree.
  using PlanResult = std::unique_ptr<LogicalOperator>;
  /// @brief Generates the operator tree based on explicitly set rules.
  PlanResult Plan(std::vector<QueryPart> &);

 private:
  PlanningContext &context_;
};

/// @brief Planner which generates multiple plans by changing the order of graph
/// traversal.
///
/// This planner picks different starting nodes from which to start graph
/// traversal. Generating a single plan is backed by @c RuleBasedPlanner.
///
/// @sa MakeLogicalPlan
class VariableStartPlanner {
 public:
  VariableStartPlanner(PlanningContext &context) : context_(context) {}

  /// @brief The result of plan generation is a vector of roots to multiple
  /// generated operator trees.
  using PlanResult = std::vector<std::unique_ptr<LogicalOperator>>;
  /// @brief Generate multiple plans by varying the order of graph traversal.
  PlanResult Plan(std::vector<QueryPart> &);

 private:
  PlanningContext &context_;
};

/// @brief Convert the AST to multiple @c QueryParts.
///
/// This function will normalize patterns inside @c Match and @c Merge clauses
/// and do some other preprocessing in order to generate multiple @c QueryPart
/// structures. @c AstTreeStorage and @c SymbolTable may be used to create new
/// AST nodes.
std::vector<QueryPart> CollectQueryParts(SymbolTable &, AstTreeStorage &);

/// @brief Generates the LogicalOperator tree and returns the resulting plan.
///
/// @tparam TPlanner Type of the planner used for generation.
/// @param storage AstTreeStorage used to construct the operator tree by
///     traversing the @c Query node. The storage may also be used to create new
///     AST nodes for use in operators.
/// @param symbol_table SymbolTable used to determine inputs and outputs of
///     certain operators. Newly created AST nodes may be added to this symbol
///     table.
/// @param db Optional @c GraphDbAccessor, which is used to query database
///     information in order to improve generated plans.
/// @return @c PlanResult which depends on the @c TPlanner used.
///
/// @sa RuleBasedPlanner
/// @sa VariableStartPlanner
template <class TPlanner>
typename TPlanner::PlanResult MakeLogicalPlan(AstTreeStorage &storage,
                                              SymbolTable &symbol_table,
                                              const GraphDbAccessor &db) {
  auto query_parts = CollectQueryParts(symbol_table, storage);
  PlanningContext context{symbol_table, storage, db};
  return TPlanner(context).Plan(query_parts);
}

}  // namespace plan

}  // namespace query
