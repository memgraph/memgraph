#include "query/frontend/logical/planner.hpp"

#include <unordered_set>

#include "query/frontend/ast/ast.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

namespace query {

namespace {

static LogicalOperator *GenCreate(
    Create& create, std::shared_ptr<LogicalOperator> input_op)
{
  if (input_op) {
    // TODO: Support clauses before CREATE, e.g. `MATCH (n) CREATE (m)`
    throw NotYetImplemented();
  }
  if (create.patterns_.size() != 1) {
    // TODO: Support creating multiple patterns, e.g. `CREATE (n), (m)`
    throw NotYetImplemented();
  }
  auto &pattern = create.patterns_[0];
  if (pattern->atoms_.size() != 1) {
    // TODO: Support creating edges.
    throw NotYetImplemented();
  }
  auto *node_atom = dynamic_cast<NodeAtom*>(pattern->atoms_[0]);
  debug_assert(node_atom, "First pattern atom is not a node");
  return new CreateOp(node_atom);
}

// Returns false if the symbol was already bound, otherwise binds it and
// returns true.
bool BindSymbol(std::unordered_set<int> &bound_symbols, const Symbol &symbol)
{
  auto insertion = bound_symbols.insert(symbol.position_);
  return insertion.second;
}

LogicalOperator *GenMatch(
    Match& match,
    std::shared_ptr<LogicalOperator> input_op,
    const SymbolTable &symbol_table,
    std::unordered_set<int> &bound_symbols)
{
  if (input_op) {
    // TODO: Support clauses before match.
    throw NotYetImplemented();
  }
  if (match.patterns_.size() != 1) {
    // TODO: Support matching multiple patterns.
    throw NotYetImplemented();
  }
  auto &pattern = match.patterns_[0];
  debug_assert(!pattern->atoms_.empty(), "Missing atoms in pattern");
  auto atoms_it = pattern->atoms_.begin();
  auto last_node = dynamic_cast<NodeAtom*>(*atoms_it++);
  debug_assert(last_node, "First pattern atom is not a node");
  // First atom always binds a symbol, and we don't care if it already existed,
  // because we create a ScanAll which writes that symbol. This may need to
  // change when we support clauses before match.
  BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_));
  LogicalOperator *last_op = new ScanAll(last_node);
  if (!last_node->labels_.empty() || !last_node->properties_.empty()) {
    last_op = new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                             symbol_table.at(*last_node->identifier_),
                             last_node);
  }
  EdgeAtom *last_edge = nullptr;
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  for ( ; atoms_it != pattern->atoms_.end(); ++atoms_it) {
    if (last_edge) {
      // Store the symbol from the first node as the input to Expand.
      auto input_symbol = symbol_table.at(*last_node->identifier_);
      last_node = dynamic_cast<NodeAtom*>(*atoms_it);
      debug_assert(last_node, "Expected a node atom in pattern.");
      // If the expand symbols were already bound, then we need to indicate
      // this as a cycle. The Expand will then check whether the pattern holds
      // instead of writing the expansion to symbols.
      auto node_cycle = false;
      auto edge_cycle = false;
      if (!BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_))) {
        node_cycle = true;
      }
      if (!BindSymbol(bound_symbols, symbol_table.at(*last_edge->identifier_))) {
        edge_cycle = true;
      }
      last_op = new Expand(last_node, last_edge,
                           std::shared_ptr<LogicalOperator>(last_op),
                           input_symbol, node_cycle, edge_cycle);
      if (!last_edge->edge_types_.empty()) {
        last_op = new EdgeFilter(std::shared_ptr<LogicalOperator>(last_op),
                                 symbol_table.at(*last_edge->identifier_),
                                 last_edge);
      }
      if (!last_node->labels_.empty() || !last_node->properties_.empty()) {
        last_op = new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                                 symbol_table.at(*last_node->identifier_),
                                 last_node);
      }
      // Don't forget to clear the edge, because we expect the next
      // (EdgeAtom, NodeAtom) sequence.
      last_edge = nullptr;
    } else {
      last_edge = dynamic_cast<EdgeAtom*>(*atoms_it);
      debug_assert(last_edge, "Expected an edge atom in pattern.");
    }
  }
  debug_assert(!last_edge, "Edge atom should not end the pattern.");
  return last_op;
}

Produce *GenReturn(Return& ret, std::shared_ptr<LogicalOperator> input_op)
{
  if (!input_op) {
    // TODO: Support standalone RETURN clause (e.g. RETURN 2)
    throw NotYetImplemented();
  }
  return new Produce(input_op, ret.named_expressions_);
}
}

std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    Query& query, const SymbolTable &symbol_table)
{
  // TODO: Extract functions and state into a class with methods. Possibly a
  // visitor or similar to avoid all those dynamic casts.
  LogicalOperator *input_op = nullptr;
  // bound_symbols set is used to differentiate cycles in pattern matching, so
  // that the operator can be correctly initialized whether to read the symbol
  // or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and write) the first
  // `n`, but the latter `n` would only read the already written information.
  std::unordered_set<int> bound_symbols;
  for (auto &clause : query.clauses_) {
    auto *clause_ptr = clause;
    if (auto *match = dynamic_cast<Match*>(clause_ptr)) {
      input_op = GenMatch(*match, std::shared_ptr<LogicalOperator>(input_op),
                          symbol_table, bound_symbols);
    } else if (auto *ret = dynamic_cast<Return*>(clause_ptr)) {
      input_op = GenReturn(*ret, std::shared_ptr<LogicalOperator>(input_op));
    } else if (auto *create = dynamic_cast<Create*>(clause_ptr)) {
      input_op = GenCreate(*create, std::shared_ptr<LogicalOperator>(input_op));
    } else {
      throw NotYetImplemented();
    }
  }
  return std::unique_ptr<LogicalOperator>(input_op);
}

}
