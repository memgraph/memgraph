#include "query/frontend/logical/planner.hpp"

#include <unordered_set>

#include "query/frontend/ast/ast.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

namespace query {

namespace {

// Returns false if the symbol was already bound, otherwise binds it and
// returns true.
bool BindSymbol(std::unordered_set<int> &bound_symbols, const Symbol &symbol) {
  auto insertion = bound_symbols.insert(symbol.position_);
  return insertion.second;
}

LogicalOperator *GenCreateForPattern(Pattern &pattern,
                                     LogicalOperator *input_op,
                                     const SymbolTable &symbol_table,
                                     std::unordered_set<int> bound_symbols) {
  auto atoms_it = pattern.atoms_.begin();
  auto last_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  debug_assert(last_node, "First pattern atom is not a node");
  auto last_op = input_op;
  if (BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_))) {
    // TODO: Pass last_op when CreateOp gets support for it. This will
    // support e.g. `MATCH (n) CREATE (m)` and `CREATE (n), (m)`.
    if (last_op) {
      throw NotYetImplemented();
    }
    last_op = new CreateOp(last_node);
  }
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    debug_assert(edge, "Expected an edge atom in pattern.");
    debug_assert(atoms_it != pattern.atoms_.end(),
                 "Edge atom should not end the pattern.");
    // Store the symbol from the first node as the input to CreateExpand.
    auto input_symbol = symbol_table.at(*last_node->identifier_);
    last_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    debug_assert(last_node, "Expected a node atom in pattern.");
    // If the expand node was already bound, then we need to indicate this,
    // so that CreateExpand only creates an edge.
    bool node_existing = false;
    if (!BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_))) {
      node_existing = true;
    }
    if (!BindSymbol(bound_symbols, symbol_table.at(*edge->identifier_))) {
      permanent_fail("Symbols used for created edges cannot be redeclared.");
    }
    last_op = new CreateExpand(last_node, edge,
                               std::shared_ptr<LogicalOperator>(last_op),
                               input_symbol, node_existing);
  }
  return last_op;
}

LogicalOperator *GenCreate(Create &create, LogicalOperator *input_op,
                           const SymbolTable &symbol_table,
                           std::unordered_set<int> bound_symbols) {
  auto last_op = input_op;
  for (auto pattern : create.patterns_) {
    last_op =
        GenCreateForPattern(*pattern, last_op, symbol_table, bound_symbols);
  }
  return last_op;
}

LogicalOperator *GenMatch(Match &match, LogicalOperator *input_op,
                          const SymbolTable &symbol_table,
                          std::unordered_set<int> &bound_symbols) {
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
  auto last_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  debug_assert(last_node, "First pattern atom is not a node");
  // First atom always binds a symbol, and we don't care if it already existed,
  // because we create a ScanAll which writes that symbol. This may need to
  // change when we support clauses before match.
  BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_));
  LogicalOperator *last_op = new ScanAll(last_node);
  if (!last_node->labels_.empty() || !last_node->properties_.empty()) {
    last_op =
        new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                       symbol_table.at(*last_node->identifier_), last_node);
  }
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern->atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    debug_assert(edge, "Expected an edge atom in pattern.");
    debug_assert(atoms_it != pattern->atoms_.end(),
                 "Edge atom should not end the pattern.");
    // Store the symbol from the first node as the input to Expand.
    auto input_symbol = symbol_table.at(*last_node->identifier_);
    last_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    debug_assert(last_node, "Expected a node atom in pattern.");
    // If the expand symbols were already bound, then we need to indicate
    // this as a cycle. The Expand will then check whether the pattern holds
    // instead of writing the expansion to symbols.
    auto node_cycle = false;
    auto edge_cycle = false;
    if (!BindSymbol(bound_symbols, symbol_table.at(*last_node->identifier_))) {
      node_cycle = true;
    }
    if (!BindSymbol(bound_symbols, symbol_table.at(*edge->identifier_))) {
      edge_cycle = true;
    }
    last_op =
        new Expand(last_node, edge, std::shared_ptr<LogicalOperator>(last_op),
                   input_symbol, node_cycle, edge_cycle);
    if (!edge->edge_types_.empty() || !edge->properties_.empty()) {
      last_op = new EdgeFilter(std::shared_ptr<LogicalOperator>(last_op),
                               symbol_table.at(*edge->identifier_), edge);
    }
    if (!last_node->labels_.empty() || !last_node->properties_.empty()) {
      last_op =
          new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                         symbol_table.at(*last_node->identifier_), last_node);
    }
  }
  return last_op;
}

Produce *GenReturn(Return &ret, LogicalOperator *input_op) {
  if (!input_op) {
    // TODO: Support standalone RETURN clause (e.g. RETURN 2)
    throw NotYetImplemented();
  }
  return new Produce(std::shared_ptr<LogicalOperator>(input_op),
                     ret.named_expressions_);
}

}  // namespace

std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    Query &query, const SymbolTable &symbol_table) {
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
    if (auto *match = dynamic_cast<Match *>(clause_ptr)) {
      input_op = GenMatch(*match, input_op, symbol_table, bound_symbols);
    } else if (auto *ret = dynamic_cast<Return *>(clause_ptr)) {
      input_op = GenReturn(*ret, input_op);
    } else if (auto *create = dynamic_cast<Create *>(clause_ptr)) {
      input_op = GenCreate(*create, input_op, symbol_table, bound_symbols);
    } else {
      throw NotYetImplemented();
    }
  }
  return std::unique_ptr<LogicalOperator>(input_op);
}

}  // namespace query
