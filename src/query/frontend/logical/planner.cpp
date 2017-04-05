#include "query/frontend/logical/planner.hpp"

#include <functional>
#include <unordered_set>

#include "query/frontend/ast/ast.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

namespace query {
namespace plan {

namespace {

// Returns false if the symbol was already bound, otherwise binds it and
// returns true.
bool BindSymbol(std::unordered_set<int> &bound_symbols, const Symbol &symbol) {
  auto insertion = bound_symbols.insert(symbol.position_);
  return insertion.second;
}

/// Utility function for iterating pattern atoms and accumulating a result.
///
/// Each pattern is of the form `NodeAtom (, EdgeAtom, NodeAtom)*`. Therefore,
/// the `base` function is called on the first `NodeAtom`, while the `collect`
/// is called for the whole triplet. Result of the function is passed to the
/// next call. Final result is returned.
///
/// Example usage of counting edge atoms in the pattern.
///
///    auto base = [](NodeAtom *first_node) { return 0; };
///    auto collect = [](int accum, NodeAtom *prev_node, EdgeAtom *edge,
///                      NodeAtom *node) {
///      return accum + 1;
///    };
///    int edge_count = ReducePattern<int>(pattern, base, collect);
///
// TODO: It might be a good idea to move this somewhere else, for easier usage
// in other files.
template <typename T>
auto ReducePattern(
    Pattern &pattern, std::function<T(NodeAtom *)> base,
    std::function<T(T, NodeAtom *, EdgeAtom *, NodeAtom *)> collect) {
  debug_assert(!pattern.atoms_.empty(), "Missing atoms in pattern");
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  debug_assert(current_node, "First pattern atom is not a node");
  auto last_res = base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    debug_assert(edge, "Expected an edge atom in pattern.");
    debug_assert(atoms_it != pattern.atoms_.end(),
                 "Edge atom should not end the pattern.");
    auto prev_node = current_node;
    current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    debug_assert(current_node, "Expected a node atom in pattern.");
    last_res = collect(last_res, prev_node, edge, current_node);
  }
  return last_res;
}

auto GenCreateForPattern(Pattern &pattern, LogicalOperator *input_op,
                         const query::SymbolTable &symbol_table,
                         std::unordered_set<int> bound_symbols) {
  auto base = [&](NodeAtom *node) -> LogicalOperator * {
    if (BindSymbol(bound_symbols, symbol_table.at(*node->identifier_)))
      return new CreateNode(node, std::shared_ptr<LogicalOperator>(input_op));
    else
      return input_op;
  };

  auto collect = [&](LogicalOperator *last_op, NodeAtom *prev_node,
                     EdgeAtom *edge, NodeAtom *node) {
    // Store the symbol from the first node as the input to CreateExpand.
    const auto &input_symbol = symbol_table.at(*prev_node->identifier_);
    // If the expand node was already bound, then we need to indicate this,
    // so that CreateExpand only creates an edge.
    bool node_existing = false;
    if (!BindSymbol(bound_symbols, symbol_table.at(*node->identifier_))) {
      node_existing = true;
    }
    if (!BindSymbol(bound_symbols, symbol_table.at(*edge->identifier_))) {
      permanent_fail("Symbols used for created edges cannot be redeclared.");
    }
    return new CreateExpand(node, edge,
                            std::shared_ptr<LogicalOperator>(last_op),
                            input_symbol, node_existing);
  };

  return ReducePattern<LogicalOperator *>(pattern, base, collect);
}

auto GenCreate(Create &create, LogicalOperator *input_op,
               const query::SymbolTable &symbol_table,
               std::unordered_set<int> bound_symbols) {
  auto last_op = input_op;
  for (auto pattern : create.patterns_) {
    last_op =
        GenCreateForPattern(*pattern, last_op, symbol_table, bound_symbols);
  }
  return last_op;
}

auto GenMatchForPattern(Pattern &pattern, LogicalOperator *input_op,
                        const query::SymbolTable &symbol_table,
                        std::unordered_set<int> &bound_symbols,
                        std::vector<Symbol> &edge_symbols) {
  auto base = [&](NodeAtom *node) {
    LogicalOperator *last_op = input_op;
    // If the first atom binds a symbol, we generate a ScanAll which writes it.
    // Otherwise, someone else generates it (e.g. a previous ScanAll).
    if (BindSymbol(bound_symbols, symbol_table.at(*node->identifier_))) {
      last_op = new ScanAll(node, std::shared_ptr<LogicalOperator>(last_op));
    }
    // Even though we may skip generating ScanAll, we still want to add a filter
    // in case this atom adds more labels/properties for filtering.
    if (!node->labels_.empty() || !node->properties_.empty()) {
      last_op = new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                               symbol_table.at(*node->identifier_), node);
    }
    return last_op;
  };
  auto collect = [&](LogicalOperator *last_op, NodeAtom *prev_node,
                     EdgeAtom *edge, NodeAtom *node) {
    // Store the symbol from the first node as the input to Expand.
    const auto &input_symbol = symbol_table.at(*prev_node->identifier_);
    // If the expand symbols were already bound, then we need to indicate
    // this as a cycle. The Expand will then check whether the pattern holds
    // instead of writing the expansion to symbols.
    auto node_cycle = false;
    auto edge_cycle = false;
    if (!BindSymbol(bound_symbols, symbol_table.at(*node->identifier_))) {
      node_cycle = true;
    }
    const auto &edge_symbol = symbol_table.at(*edge->identifier_);
    if (!BindSymbol(bound_symbols, edge_symbol)) {
      edge_cycle = true;
    }
    last_op = new Expand(node, edge, std::shared_ptr<LogicalOperator>(last_op),
                         input_symbol, node_cycle, edge_cycle);
    if (!edge_cycle) {
      // Ensure Cyphermorphism (different edge symbols always map to different
      // edges).
      if (!edge_symbols.empty()) {
        last_op = new ExpandUniquenessFilter<EdgeAccessor>(
            std::shared_ptr<LogicalOperator>(last_op), edge_symbol,
            edge_symbols);
      }
      edge_symbols.emplace_back(edge_symbol);
    }
    if (!edge->edge_types_.empty() || !edge->properties_.empty()) {
      last_op = new EdgeFilter(std::shared_ptr<LogicalOperator>(last_op),
                               symbol_table.at(*edge->identifier_), edge);
    }
    if (!node->labels_.empty() || !node->properties_.empty()) {
      last_op = new NodeFilter(std::shared_ptr<LogicalOperator>(last_op),
                               symbol_table.at(*node->identifier_), node);
    }
    return last_op;
  };
  return ReducePattern<LogicalOperator *>(pattern, base, collect);
}

auto GenMatch(Match &match, LogicalOperator *input_op,
              const query::SymbolTable &symbol_table,
              std::unordered_set<int> &bound_symbols) {
  auto last_op = input_op;
  std::vector<Symbol> edge_symbols;
  for (auto pattern : match.patterns_) {
    last_op = GenMatchForPattern(*pattern, last_op, symbol_table, bound_symbols,
                                 edge_symbols);
  }
  if (match.where_) {
    last_op = new Filter(std::shared_ptr<LogicalOperator>(last_op),
                         match.where_->expression_);
  }
  return last_op;
}

// Ast tree visitor which collects all the symbols referenced by identifiers.
class SymbolCollector : public TreeVisitorBase {
 public:
  SymbolCollector(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  void Visit(Identifier &ident) override {
    symbols_.insert(symbol_table_.at(ident));
  }

  const auto &symbols() const { return symbols_; }

 private:
  // Calculates the Symbol hash based on its position.
  struct SymbolHash {
    size_t operator()(const Symbol &symbol) const {
      return std::hash<int>{}(symbol.position_);
    }
  };

  const SymbolTable &symbol_table_;
  std::unordered_set<Symbol, SymbolHash> symbols_;
};

auto GenWith(With &with, LogicalOperator *input_op,
             const query::SymbolTable &symbol_table) {
  if (with.distinct_) {
    // TODO: Plan disctint with, when operator available.
    throw NotYetImplemented();
  }
  // WITH clause is Accumulate/Aggregate (advance_command) + Produce.
  SymbolCollector symbol_collector(symbol_table);
  // Collect used symbols so that accumulate doesn't copy the whole frame.
  for (auto &named_expr : with.named_expressions_) {
    named_expr->expression_->Accept(symbol_collector);
  }
  auto symbols = symbol_collector.symbols();
  // TODO: Check whether we need aggregate instead of accumulate.
  LogicalOperator *last_op =
      new Accumulate(std::shared_ptr<LogicalOperator>(input_op),
                     std::vector<Symbol>(symbols.begin(), symbols.end()), true);
  last_op = new Produce(std::shared_ptr<LogicalOperator>(last_op),
                        with.named_expressions_);
  if (with.where_) {
    last_op = new Filter(std::shared_ptr<LogicalOperator>(last_op),
                         with.where_->expression_);
  }
  return last_op;
}

}  // namespace

std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    query::Query &query, const query::SymbolTable &symbol_table) {
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
      input_op = new Produce(std::shared_ptr<LogicalOperator>(input_op),
                             ret->named_expressions_);
    } else if (auto *create = dynamic_cast<Create *>(clause_ptr)) {
      input_op = GenCreate(*create, input_op, symbol_table, bound_symbols);
    } else if (auto *del = dynamic_cast<query::Delete *>(clause_ptr)) {
      input_op = new plan::Delete(std::shared_ptr<LogicalOperator>(input_op),
                                  del->expressions_, del->detach_);
    } else if (auto *set = dynamic_cast<query::SetProperty *>(clause_ptr)) {
      input_op =
          new plan::SetProperty(std::shared_ptr<LogicalOperator>(input_op),
                                set->property_lookup_, set->expression_);
    } else if (auto *set = dynamic_cast<query::SetProperties *>(clause_ptr)) {
      auto op = set->update_ ? plan::SetProperties::Op::UPDATE
                             : plan::SetProperties::Op::REPLACE;
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      input_op =
          new plan::SetProperties(std::shared_ptr<LogicalOperator>(input_op),
                                  input_symbol, set->expression_, op);
    } else if (auto *set = dynamic_cast<query::SetLabels *>(clause_ptr)) {
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      input_op = new plan::SetLabels(std::shared_ptr<LogicalOperator>(input_op),
                                     input_symbol, set->labels_);
    } else if (auto *rem = dynamic_cast<query::RemoveProperty *>(clause_ptr)) {
      input_op = new plan::RemoveProperty(
          std::shared_ptr<LogicalOperator>(input_op), rem->property_lookup_);
    } else if (auto *rem = dynamic_cast<query::RemoveLabels *>(clause_ptr)) {
      const auto &input_symbol = symbol_table.at(*rem->identifier_);
      input_op =
          new plan::RemoveLabels(std::shared_ptr<LogicalOperator>(input_op),
                                 input_symbol, rem->labels_);
    } else if (auto *with = dynamic_cast<query::With *>(clause_ptr)) {
      input_op = GenWith(*with, input_op, symbol_table);
    } else {
      throw NotYetImplemented();
    }
  }
  return std::unique_ptr<LogicalOperator>(input_op);
}

}  // namespace plan
}  // namespace query
