#pragma once

#include <memory>

#include "query/plan/operator.hpp"

namespace query {

class AstTreeStorage;
class SymbolTable;

namespace plan {

// Normalized representation of a pattern that needs to be matched.
struct Expansion {
  // The first node in the expansion, it can be a single node.
  NodeAtom *node1 = nullptr;
  // Optional edge which connects the 2 nodes.
  EdgeAtom *edge = nullptr;
  // Optional node at the other end of an edge. If the expansion contains an
  // edge, then this node is required.
  NodeAtom *node2 = nullptr;
};

// Normalized representation of a single or multiple Match clauses.
//
// For example, `MATCH (a :Label) -[e1]- (b) -[e2]- (c) MATCH (n) -[e3]- (m)
// WHERE c.prop < 42` will produce the following.
// Expansions will store `(a) -[e1]-(b)`, `(b) -[e2]- (c)` and `(n) -[e3]- (m)`.
// Edge symbols for Cyphermorphism will only contain the set `{e1, e2}` for the
// first `MATCH` and the set `{e3}` for the second.
// Filters will contain 2 pairs. One for testing `:Label` on symbol `a` and the
// other obtained from `WHERE` on symbol `c`.
struct Matching {
  // All expansions that need to be performed across Match clauses.
  std::vector<Expansion> expansions;
  // Symbols for edges established in match, used to ensure Cyphermorphism.
  // There are multiple sets, because each Match clause determines a single set.
  std::vector<std::unordered_set<Symbol>> edge_symbols;
  // Pairs of filter expression and symbols used in them. The list should be
  // filled using CollectPatternFilters function.
  std::vector<std::pair<Expression *, std::unordered_set<Symbol>>> filters;
};

// Represents a read (+ write) part of a query. Each part ends with either:
//  * RETURN clause;
//  * WITH clause or
//  * any of the write clauses.
//
// For a query `MATCH (n) MERGE (n) -[e]- (m) SET n.x = 42 MERGE (l)` the
// generated QueryPart will have `matching` generated for the `MATCH`.
// `remaining_clauses` will contain `Merge`, `SetProperty` and `Merge` clauses
// in that exact order. The pattern inside the first `MERGE` will be used to
// generate the first `merge_matching` element, and the second `MERGE` pattern
// will produce the second `merge_matching` element. This way, if someone
// traverses `remaining_clauses`, the order of appearance of `Merge` clauses is
// in the same order as their respective `merge_matching` elements.
struct QueryPart {
  // All MATCH clauses merged into one Matching.
  Matching matching;
  // Each OPTIONAL MATCH converted to Matching.
  std::vector<Matching> optional_matching;
  // Matching for each MERGE clause. Since Merge is contained in
  // remaining_clauses, this vector contains matching in the same order as Merge
  // appears.
  std::vector<Matching> merge_matching;
  // All the remaining clauses (without Match).
  std::vector<Clause *> remaining_clauses;
};

// Context which contains variables commonly used during planning.
struct PlanningContext {
  SymbolTable &symbol_table;
  AstTreeStorage &ast_storage;
  // bound_symbols set is used to differentiate cycles in pattern matching, so
  // that the operator can be correctly initialized whether to read the symbol
  // or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and write) the first
  // `n`, but the latter `n` would only read the already written information.
  std::unordered_set<Symbol> bound_symbols;
};

class RuleBasedPlanner {
 public:
  RuleBasedPlanner(PlanningContext &context) : context_(context) {}

  using PlanResult = std::unique_ptr<LogicalOperator>;
  PlanResult Plan(std::vector<QueryPart> &);

 private:
  PlanningContext &context_;
};

std::vector<QueryPart> CollectQueryParts(const SymbolTable &, AstTreeStorage &);

/// @brief Generates the LogicalOperator tree and returns the root operation.
///
/// The tree is constructed by traversing the @c Query node from given
/// @c AstTreeStorage. The storage may also be used to create new AST nodes for
/// use in operators. @c SymbolTable is used to determine inputs and outputs of
/// certain operators.
template <class TPlanner>
typename TPlanner::PlanResult MakeLogicalPlan(AstTreeStorage &storage,
                                              SymbolTable &symbol_table) {
  auto query_parts = CollectQueryParts(symbol_table, storage);
  PlanningContext context{symbol_table, storage};
  return TPlanner(context).Plan(query_parts);
}

}  // namespace plan

}  // namespace query
