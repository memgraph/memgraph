#include "query/plan/planner.hpp"

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
                         const SymbolTable &symbol_table,
                         std::unordered_set<int> &bound_symbols) {
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
               const SymbolTable &symbol_table,
               std::unordered_set<int> &bound_symbols) {
  auto last_op = input_op;
  for (auto pattern : create.patterns_) {
    last_op =
        GenCreateForPattern(*pattern, last_op, symbol_table, bound_symbols);
  }
  return last_op;
}

auto GenMatchForPattern(Pattern &pattern, LogicalOperator *input_op,
                        const SymbolTable &symbol_table,
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
              const SymbolTable &symbol_table,
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

// Ast tree visitor which collects the context for a return body. The return
// body are the named expressions found in WITH and RETURN clauses. The
// collected context consists of used symbols, aggregations and group by named
// expressions.
class ReturnBodyContext : public TreeVisitorBase {
 public:
  ReturnBodyContext(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  using TreeVisitorBase::PreVisit;
  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  void Visit(Literal &) override { has_aggregation_.emplace_back(false); }

  void Visit(Identifier &ident) override {
    symbols_.insert(symbol_table_.at(ident));
    has_aggregation_.emplace_back(false);
  }

#define VISIT_BINARY_OPERATOR(BinaryOperator)                              \
  void PostVisit(BinaryOperator &op) override {                            \
    /* has_aggregation_ stack is reversed, last result is from the 2nd     \
     * expression. */                                                      \
    bool aggr2 = has_aggregation_.back();                                  \
    has_aggregation_.pop_back();                                           \
    bool aggr1 = has_aggregation_.back();                                  \
    has_aggregation_.pop_back();                                           \
    bool has_aggr = aggr1 || aggr2;                                        \
    if (has_aggr) {                                                        \
      /* Group by the expression which does not contain aggregation. */    \
      /* Possible optimization is to ignore constant value expressions */  \
      group_by_.emplace_back(aggr1 ? op.expression2_ : op.expression1_);   \
    }                                                                      \
    /* Propagate that this whole expression may contain an aggregation. */ \
    has_aggregation_.emplace_back(has_aggr);                               \
  }

  VISIT_BINARY_OPERATOR(OrOperator)
  VISIT_BINARY_OPERATOR(XorOperator)
  VISIT_BINARY_OPERATOR(AndOperator)
  VISIT_BINARY_OPERATOR(AdditionOperator)
  VISIT_BINARY_OPERATOR(SubtractionOperator)
  VISIT_BINARY_OPERATOR(MultiplicationOperator)
  VISIT_BINARY_OPERATOR(DivisionOperator)
  VISIT_BINARY_OPERATOR(ModOperator)
  VISIT_BINARY_OPERATOR(NotEqualOperator)
  VISIT_BINARY_OPERATOR(EqualOperator)
  VISIT_BINARY_OPERATOR(LessOperator)
  VISIT_BINARY_OPERATOR(GreaterOperator)
  VISIT_BINARY_OPERATOR(LessEqualOperator)
  VISIT_BINARY_OPERATOR(GreaterEqualOperator)

#undef VISIT_BINARY_OPERATOR

  void PostVisit(Aggregation &aggr) override {
    // Aggregation contains a virtual symbol, where the result will be stored.
    const auto &symbol = symbol_table_.at(aggr);
    aggregations_.emplace_back(aggr.expression_, aggr.op_, symbol);
    has_aggregation_.back() = true;
    // Possible optimization is to skip remembering symbols inside aggregation.
    // If and when implementing this, don't forget that Accumulate needs *all*
    // the symbols, including those inside aggregation.
  }

  void PostVisit(NamedExpression &named_expr) override {
    if (!has_aggregation_.back()) {
      group_by_.emplace_back(named_expr.expression_);
    }
    has_aggregation_.pop_back();
  }

  // Set of symbols used inside the visited expressions outside of aggregation
  // expression.
  const auto &symbols() const { return symbols_; }
  // List of aggregation elements found in expressions.
  const auto &aggregations() const { return aggregations_; }
  // When there is at least one aggregation element, all the non-aggregate (sub)
  // expressions are used for grouping. For example, in `WITH sum(n.a) + 2 * n.b
  // AS sum, n.c AS nc`, we will group by `2 * n.b` and `n.c`.
  const auto &group_by() const { return group_by_; }

 private:
  // Calculates the Symbol hash based on its position.
  struct SymbolHash {
    size_t operator()(const Symbol &symbol) const {
      return std::hash<int>{}(symbol.position_);
    }
  };

  const SymbolTable &symbol_table_;
  std::unordered_set<Symbol, SymbolHash> symbols_;
  std::vector<Aggregate::Element> aggregations_;
  std::vector<Expression *> group_by_;
  // Flag indicating whether an expression contains an aggregation.
  std::list<bool> has_aggregation_;
};

auto GenReturnBody(LogicalOperator *input_op, bool advance_command,
                   const std::vector<NamedExpression *> &named_expressions,
                   const SymbolTable &symbol_table, bool accumulate = false) {
  ReturnBodyContext context(symbol_table);
  // Generate context for all named expressions.
  for (auto &named_expr : named_expressions) {
    named_expr->Accept(context);
  }
  auto symbols =
      std::vector<Symbol>(context.symbols().begin(), context.symbols().end());
  auto last_op = input_op;
  if (accumulate) {
    // We only advance the command in Accumulate. This is done for WITH clause,
    // when the first part updated the database. RETURN clause may only need an
    // accumulation after updates, without advancing the command.
    last_op = new Accumulate(std::shared_ptr<LogicalOperator>(last_op), symbols,
                             advance_command);
  }
  if (!context.aggregations().empty()) {
    last_op =
        new Aggregate(std::shared_ptr<LogicalOperator>(last_op),
                      context.aggregations(), context.group_by(), symbols);
  }
  return new Produce(std::shared_ptr<LogicalOperator>(last_op),
                     named_expressions);
}

auto GenWith(With &with, LogicalOperator *input_op,
             const SymbolTable &symbol_table, bool is_write,
             std::unordered_set<int> &bound_symbols) {
  // WITH clause is Accumulate/Aggregate (advance_command) + Produce and
  // optional Filter.
  if (with.distinct_) {
    // TODO: Plan distinct with, when operator available.
    throw NotYetImplemented();
  }
  // In case of update and aggregation, we want to accumulate first, so that
  // when aggregating, we get the latest results. Similar to RETURN clause.
  bool accumulate = is_write;
  // No need to advance the command if we only performed reads.
  bool advance_command = is_write;
  LogicalOperator *last_op =
      GenReturnBody(input_op, advance_command, with.named_expressions_,
                    symbol_table, accumulate);
  // Reset bound symbols, so that only those in WITH are exposed.
  bound_symbols.clear();
  for (auto &named_expr : with.named_expressions_) {
    BindSymbol(bound_symbols, symbol_table.at(*named_expr));
  }
  if (with.where_) {
    last_op = new Filter(std::shared_ptr<LogicalOperator>(last_op),
                         with.where_->expression_);
  }
  return last_op;
}

auto GenReturn(Return &ret, LogicalOperator *input_op,
               const SymbolTable &symbol_table, bool is_write) {
  // Similar to WITH clause, but we want to accumulate and advance command when
  // the query writes to the database. This way we handle the case when we want
  // to return expressions with the latest updated results. For example,
  // `MATCH (n) -- () SET n.prop = n.prop + 1 RETURN n.prop`. If we match same
  // `n` multiple 'k' times, we want to return 'k' results where the property
  // value is the same, final result of 'k' increments.
  bool accumulate = is_write;
  bool advance_command = false;
  return GenReturnBody(input_op, advance_command, ret.named_expressions_,
                       symbol_table, accumulate);
}

// Generate an operator for a clause which writes to the database. If the clause
// isn't handled, returns nullptr.
LogicalOperator *HandleWriteClause(Clause *clause, LogicalOperator *input_op,
                                   const SymbolTable &symbol_table,
                                   std::unordered_set<int> &bound_symbols) {
  if (auto *create = dynamic_cast<Create *>(clause)) {
    return GenCreate(*create, input_op, symbol_table, bound_symbols);
  } else if (auto *del = dynamic_cast<query::Delete *>(clause)) {
    return new plan::Delete(std::shared_ptr<LogicalOperator>(input_op),
                            del->expressions_, del->detach_);
  } else if (auto *set = dynamic_cast<query::SetProperty *>(clause)) {
    return new plan::SetProperty(std::shared_ptr<LogicalOperator>(input_op),
                                 set->property_lookup_, set->expression_);
  } else if (auto *set = dynamic_cast<query::SetProperties *>(clause)) {
    auto op = set->update_ ? plan::SetProperties::Op::UPDATE
                           : plan::SetProperties::Op::REPLACE;
    const auto &input_symbol = symbol_table.at(*set->identifier_);
    return new plan::SetProperties(std::shared_ptr<LogicalOperator>(input_op),
                                   input_symbol, set->expression_, op);
  } else if (auto *set = dynamic_cast<query::SetLabels *>(clause)) {
    const auto &input_symbol = symbol_table.at(*set->identifier_);
    return new plan::SetLabels(std::shared_ptr<LogicalOperator>(input_op),
                               input_symbol, set->labels_);
  } else if (auto *rem = dynamic_cast<query::RemoveProperty *>(clause)) {
    return new plan::RemoveProperty(std::shared_ptr<LogicalOperator>(input_op),
                                    rem->property_lookup_);
  } else if (auto *rem = dynamic_cast<query::RemoveLabels *>(clause)) {
    const auto &input_symbol = symbol_table.at(*rem->identifier_);
    return new plan::RemoveLabels(std::shared_ptr<LogicalOperator>(input_op),
                                  input_symbol, rem->labels_);
  }
  return nullptr;
}

}  // namespace

std::unique_ptr<LogicalOperator> MakeLogicalPlan(
    query::Query &query, const query::SymbolTable &symbol_table) {
  // bound_symbols set is used to differentiate cycles in pattern matching, so
  // that the operator can be correctly initialized whether to read the symbol
  // or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and write) the first
  // `n`, but the latter `n` would only read the already written information.
  std::unordered_set<int> bound_symbols;
  // Set to true if a query command performs a writes to the database.
  bool is_write = false;
  LogicalOperator *input_op = nullptr;
  for (auto &clause : query.clauses_) {
    // Clauses which read from the database.
    if (auto *match = dynamic_cast<Match *>(clause)) {
      input_op = GenMatch(*match, input_op, symbol_table, bound_symbols);
    } else if (auto *ret = dynamic_cast<Return *>(clause)) {
      input_op = GenReturn(*ret, input_op, symbol_table, is_write);
    } else if (auto *with = dynamic_cast<query::With *>(clause)) {
      input_op =
          GenWith(*with, input_op, symbol_table, is_write, bound_symbols);
      // WITH clause advances the command, so reset the flag.
      is_write = false;
    } else if (auto *op = HandleWriteClause(clause, input_op, symbol_table,
                                            bound_symbols)) {
      is_write = true;
      input_op = op;
    } else {
      throw NotYetImplemented();
    }
  }
  return std::unique_ptr<LogicalOperator>(input_op);
}

}  // namespace plan
}  // namespace query
