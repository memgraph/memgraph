#include "query/plan/rule_based_planner.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <unordered_set>

#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"

DEFINE_VALIDATED_int64(query_vertex_count_to_expand_existing, 10,
                       "Maximum count of indexed vertices which provoke "
                       "indexed lookup and then expand to existing, instead of "
                       "a regular expand. Default is 10, to turn off use -1.",
                       FLAG_IN_RANGE(-1,
                                     std::numeric_limits<std::int64_t>::max()));

namespace query::plan {

namespace {

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

void ForEachPattern(
    Pattern &pattern, std::function<void(NodeAtom *)> base,
    std::function<void(NodeAtom *, EdgeAtom *, NodeAtom *)> collect) {
  debug_assert(!pattern.atoms_.empty(), "Missing atoms in pattern");
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  debug_assert(current_node, "First pattern atom is not a node");
  base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    debug_assert(edge, "Expected an edge atom in pattern.");
    debug_assert(atoms_it != pattern.atoms_.end(),
                 "Edge atom should not end the pattern.");
    auto prev_node = current_node;
    current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    debug_assert(current_node, "Expected a node atom in pattern.");
    collect(prev_node, edge, current_node);
  }
}

auto GenCreate(Create &create, LogicalOperator *input_op,
               const SymbolTable &symbol_table,
               std::unordered_set<Symbol> &bound_symbols) {
  auto last_op = input_op;
  for (auto pattern : create.patterns_) {
    last_op = impl::GenCreateForPattern(*pattern, last_op, symbol_table,
                                        bound_symbols);
  }
  return last_op;
}

// Collects symbols from identifiers found in visited AST nodes.
class UsedSymbolsCollector : public HierarchicalTreeVisitor {
 public:
  explicit UsedSymbolsCollector(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::Visit;

  bool PostVisit(All &all) override {
    // Remove the symbol which is bound by all, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*all.identifier_));
    return true;
  }

  bool Visit(Identifier &ident) override {
    symbols_.insert(symbol_table_.at(ident));
    return true;
  }

  bool Visit(PrimitiveLiteral &) override { return true; }
  bool Visit(ParameterLookup &) override { return true; }
  bool Visit(query::CreateIndex &) override { return true; }

  std::unordered_set<Symbol> symbols_;
  const SymbolTable &symbol_table_;
};

bool HasBoundFilterSymbols(const std::unordered_set<Symbol> &bound_symbols,
                           const Filters::FilterInfo &filter) {
  for (const auto &symbol : filter.used_symbols) {
    if (bound_symbols.find(symbol) == bound_symbols.end()) {
      return false;
    }
  }
  return true;
}

// Ast tree visitor which collects the context for a return body.
// The return body of WITH and RETURN clauses consists of:
//
//   * named expressions (used to produce results);
//   * flag whether the results need to be DISTINCT;
//   * optional SKIP expression;
//   * optional LIMIT expression and
//   * optional ORDER BY expressions.
//
// In addition to the above, we collect information on used symbols,
// aggregations and expressions used for group by.
class ReturnBodyContext : public HierarchicalTreeVisitor {
 public:
  ReturnBodyContext(const ReturnBody &body, SymbolTable &symbol_table,
                    const std::unordered_set<Symbol> &bound_symbols,
                    AstTreeStorage &storage, Where *where = nullptr)
      : body_(body),
        symbol_table_(symbol_table),
        bound_symbols_(bound_symbols),
        storage_(storage),
        where_(where) {
    // Collect symbols from named expressions.
    output_symbols_.reserve(body_.named_expressions.size());
    if (body.all_identifiers) {
      // Expand '*' to expressions and symbols first, so that their results come
      // before regular named expressions.
      ExpandUserSymbols();
    }
    for (auto &named_expr : body_.named_expressions) {
      output_symbols_.emplace_back(symbol_table_.at(*named_expr));
      named_expr->Accept(*this);
      named_expressions_.emplace_back(named_expr);
    }
    // Collect aggregations.
    if (aggregations_.empty()) {
      // Visit order_by and where if we do not have aggregations. This way we
      // prevent collecting group_by expressions from order_by and where, which
      // would be very wrong. When we have aggregation, order_by and where can
      // only use new symbols (ensured in semantic analysis), so we don't care
      // about collecting used_symbols. Also, semantic analysis should
      // have prevented any aggregations from appearing here.
      for (const auto &order_pair : body.order_by) {
        order_pair.second->Accept(*this);
      }
      if (where) {
        where->Accept(*this);
      }
      debug_assert(aggregations_.empty(),
                   "Unexpected aggregations in ORDER BY or WHERE");
    }
  }

  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using HierarchicalTreeVisitor::PostVisit;

  bool Visit(PrimitiveLiteral &) override {
    has_aggregation_.emplace_back(false);
    return true;
  }

  bool PostVisit(ListLiteral &list_literal) override {
    debug_assert(
        list_literal.elements_.size() <= has_aggregation_.size(),
        "Expected has_aggregation_ flags as much as there are list elements.");
    bool has_aggr = false;
    auto it = has_aggregation_.end();
    std::advance(it, -list_literal.elements_.size());
    while (it != has_aggregation_.end()) {
      has_aggr = has_aggr || *it;
      it = has_aggregation_.erase(it);
    }
    has_aggregation_.emplace_back(has_aggr);
    return true;
  }

  bool PostVisit(MapLiteral &map_literal) override {
    debug_assert(
        map_literal.elements_.size() <= has_aggregation_.size(),
        "Expected has_aggregation_ flags as much as there are map elements.");
    bool has_aggr = false;
    auto it = has_aggregation_.end();
    std::advance(it, -map_literal.elements_.size());
    while (it != has_aggregation_.end()) {
      has_aggr = has_aggr || *it;
      it = has_aggregation_.erase(it);
    }
    has_aggregation_.emplace_back(has_aggr);
    return true;
  }

  bool PostVisit(All &all) override {
    // Remove the symbol which is bound by all, because we are only interested
    // in free (unbound) symbols.
    used_symbols_.erase(symbol_table_.at(*all.identifier_));
    debug_assert(has_aggregation_.size() >= 3U,
                 "Expected 3 has_aggregation_ flags for ALL arguments");
    bool has_aggr = false;
    for (int i = 0; i < 3; ++i) {
      has_aggr = has_aggr || has_aggregation_.back();
      has_aggregation_.pop_back();
    }
    has_aggregation_.emplace_back(has_aggr);
    return true;
  }

  bool Visit(Identifier &ident) override {
    const auto &symbol = symbol_table_.at(ident);
    if (std::find(output_symbols_.begin(), output_symbols_.end(), symbol) ==
        output_symbols_.end()) {
      // Don't pick up new symbols, even though they may be used in ORDER BY or
      // WHERE.
      used_symbols_.insert(symbol);
    }
    has_aggregation_.emplace_back(false);
    return true;
  }

  bool PreVisit(ListSlicingOperator &list_slicing) override {
    list_slicing.list_->Accept(*this);
    bool list_has_aggr = has_aggregation_.back();
    has_aggregation_.pop_back();
    bool has_aggr = list_has_aggr;
    if (list_slicing.lower_bound_) {
      list_slicing.lower_bound_->Accept(*this);
      has_aggr = has_aggr || has_aggregation_.back();
      has_aggregation_.pop_back();
    }
    if (list_slicing.upper_bound_) {
      list_slicing.upper_bound_->Accept(*this);
      has_aggr = has_aggr || has_aggregation_.back();
      has_aggregation_.pop_back();
    }
    if (has_aggr && !list_has_aggr) {
      // We need to group by the list expression, because it didn't have an
      // aggregation inside.
      group_by_.emplace_back(list_slicing.list_);
    }
    has_aggregation_.emplace_back(has_aggr);
    return false;
  }

  bool PreVisit(IfOperator &if_operator) override {
    if_operator.condition_->Accept(*this);
    bool has_aggr = has_aggregation_.back();
    has_aggregation_.pop_back();
    if_operator.then_expression_->Accept(*this);
    has_aggr = has_aggr || has_aggregation_.back();
    has_aggregation_.pop_back();
    if_operator.else_expression_->Accept(*this);
    has_aggr = has_aggr || has_aggregation_.back();
    has_aggregation_.pop_back();
    has_aggregation_.emplace_back(has_aggr);
    // TODO: Once we allow aggregations here, insert appropriate stuff in
    // group_by.
    debug_assert(!has_aggr, "Currently aggregations in CASE are not allowed");
    return false;
  }

  bool PostVisit(Function &function) override {
    debug_assert(function.arguments_.size() <= has_aggregation_.size(),
                 "Expected has_aggregation_ flags as much as there are "
                 "function arguments.");
    bool has_aggr = false;
    auto it = has_aggregation_.end();
    std::advance(it, -function.arguments_.size());
    while (it != has_aggregation_.end()) {
      has_aggr = has_aggr || *it;
      it = has_aggregation_.erase(it);
    }
    has_aggregation_.emplace_back(has_aggr);
    return true;
  }

#define VISIT_BINARY_OPERATOR(BinaryOperator)                              \
  bool PostVisit(BinaryOperator &op) override {                            \
    debug_assert(has_aggregation_.size() >= 2U,                            \
                 "Expected at least 2 has_aggregation_ flags.");           \
    /* has_aggregation_ stack is reversed, last result is from the 2nd */  \
    /* expression. */                                                      \
    bool aggr2 = has_aggregation_.back();                                  \
    has_aggregation_.pop_back();                                           \
    bool aggr1 = has_aggregation_.back();                                  \
    has_aggregation_.pop_back();                                           \
    bool has_aggr = aggr1 || aggr2;                                        \
    if (has_aggr && !(aggr1 && aggr2)) {                                   \
      /* Group by the expression which does not contain aggregation. */    \
      /* Possible optimization is to ignore constant value expressions */  \
      group_by_.emplace_back(aggr1 ? op.expression2_ : op.expression1_);   \
    }                                                                      \
    /* Propagate that this whole expression may contain an aggregation. */ \
    has_aggregation_.emplace_back(has_aggr);                               \
    return true;                                                           \
  }

  VISIT_BINARY_OPERATOR(OrOperator)
  VISIT_BINARY_OPERATOR(XorOperator)
  VISIT_BINARY_OPERATOR(AndOperator)
  VISIT_BINARY_OPERATOR(FilterAndOperator)
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
  VISIT_BINARY_OPERATOR(InListOperator)
  VISIT_BINARY_OPERATOR(ListMapIndexingOperator)

#undef VISIT_BINARY_OPERATOR

  bool PostVisit(Aggregation &aggr) override {
    // Aggregation contains a virtual symbol, where the result will be stored.
    const auto &symbol = symbol_table_.at(aggr);
    aggregations_.emplace_back(Aggregate::Element{
        aggr.expression1_, aggr.expression2_, aggr.op_, symbol});
    // Aggregation expression1_ is optional in COUNT(*), and COLLECT_MAP uses
    // two expressions, so we can have 0, 1 or 2 elements on the
    // has_aggregation_stack for this Aggregation expression.
    if (aggr.op_ == Aggregation::Op::COLLECT_MAP) has_aggregation_.pop_back();
    if (aggr.expression1_)
      has_aggregation_.back() = true;
    else
      has_aggregation_.emplace_back(true);
    // Possible optimization is to skip remembering symbols inside aggregation.
    // If and when implementing this, don't forget that Accumulate needs *all*
    // the symbols, including those inside aggregation.
    return true;
  }

  bool PostVisit(NamedExpression &named_expr) override {
    debug_assert(has_aggregation_.size() == 1U,
                 "Expected to reduce has_aggregation_ to single boolean.");
    if (!has_aggregation_.back()) {
      group_by_.emplace_back(named_expr.expression_);
    }
    has_aggregation_.pop_back();
    return true;
  }

  bool Visit(ParameterLookup &) override { return true; }
  bool Visit(query::CreateIndex &) override { return true; }

  // Creates NamedExpression with an Identifier for each user declared symbol.
  // This should be used when body.all_identifiers is true, to generate
  // expressions for Produce operator.
  void ExpandUserSymbols() {
    debug_assert(
        named_expressions_.empty(),
        "ExpandUserSymbols should be first to fill named_expressions_");
    debug_assert(output_symbols_.empty(),
                 "ExpandUserSymbols should be first to fill output_symbols_");
    for (const auto &symbol : bound_symbols_) {
      if (!symbol.user_declared()) {
        continue;
      }
      auto *ident = storage_.Create<Identifier>(symbol.name());
      symbol_table_[*ident] = symbol;
      auto *named_expr = storage_.Create<NamedExpression>(symbol.name(), ident);
      symbol_table_[*named_expr] = symbol;
      // Fill output expressions and symbols with expanded identifiers.
      named_expressions_.emplace_back(named_expr);
      output_symbols_.emplace_back(symbol);
      used_symbols_.insert(symbol);
      // Don't forget to group by expanded identifiers.
      group_by_.emplace_back(ident);
    }
    // Cypher RETURN/WITH * expects to expand '*' sorted by name.
    std::sort(output_symbols_.begin(), output_symbols_.end(),
              [](const auto &a, const auto &b) { return a.name() < b.name(); });
    std::sort(named_expressions_.begin(), named_expressions_.end(),
              [](const auto &a, const auto &b) { return a->name_ < b->name_; });
  }

  // If true, results need to be distinct.
  bool distinct() const { return body_.distinct; }
  // Named expressions which are used to produce results.
  const auto &named_expressions() const { return named_expressions_; }
  // Pairs of (Ordering, Expression *) for sorting results.
  const auto &order_by() const { return body_.order_by; }
  // Optional expression which determines how many results to skip.
  auto *skip() const { return body_.skip; }
  // Optional expression which determines how many results to produce.
  auto *limit() const { return body_.limit; }
  // Optional Where clause for filtering.
  const auto *where() const { return where_; }
  // Set of symbols used inside the visited expressions outside of aggregation
  // expression. These only includes old symbols, even though new ones may have
  // been used in ORDER BY or WHERE.
  const auto &used_symbols() const { return used_symbols_; }
  // List of aggregation elements found in expressions.
  const auto &aggregations() const { return aggregations_; }
  // When there is at least one aggregation element, all the non-aggregate (sub)
  // expressions are used for grouping. For example, in `WITH sum(n.a) + 2 * n.b
  // AS sum, n.c AS nc`, we will group by `2 * n.b` and `n.c`.
  const auto &group_by() const { return group_by_; }
  // All symbols generated by named expressions. They are collected in order of
  // named_expressions.
  const auto &output_symbols() const { return output_symbols_; }

 private:
  const ReturnBody &body_;
  SymbolTable &symbol_table_;
  const std::unordered_set<Symbol> &bound_symbols_;
  AstTreeStorage &storage_;
  const Where *const where_ = nullptr;
  std::unordered_set<Symbol> used_symbols_;
  std::vector<Symbol> output_symbols_;
  std::vector<Aggregate::Element> aggregations_;
  std::vector<Expression *> group_by_;
  // Flag indicating whether an expression contains an aggregation.
  std::list<bool> has_aggregation_;
  std::vector<NamedExpression *> named_expressions_;
};

auto GenReturnBody(LogicalOperator *input_op, bool advance_command,
                   const ReturnBodyContext &body, bool accumulate = false) {
  std::vector<Symbol> used_symbols(body.used_symbols().begin(),
                                   body.used_symbols().end());
  auto last_op = input_op;
  if (accumulate) {
    // We only advance the command in Accumulate. This is done for WITH clause,
    // when the first part updated the database. RETURN clause may only need an
    // accumulation after updates, without advancing the command.
    last_op = new Accumulate(std::shared_ptr<LogicalOperator>(last_op),
                             used_symbols, advance_command);
  }
  if (!body.aggregations().empty()) {
    // When we have aggregation, SKIP/LIMIT should always come after it.
    last_op = new Aggregate(std::shared_ptr<LogicalOperator>(last_op),
                            body.aggregations(), body.group_by(), used_symbols);
  }
  last_op = new Produce(std::shared_ptr<LogicalOperator>(last_op),
                        body.named_expressions());
  // Distinct in ReturnBody only makes Produce values unique, so plan after it.
  if (body.distinct()) {
    last_op = new Distinct(std::shared_ptr<LogicalOperator>(last_op),
                           body.output_symbols());
  }
  // Like Where, OrderBy can read from symbols established by named expressions
  // in Produce, so it must come after it.
  if (!body.order_by().empty()) {
    last_op = new OrderBy(std::shared_ptr<LogicalOperator>(last_op),
                          body.order_by(), body.output_symbols());
  }
  // Finally, Skip and Limit must come after OrderBy.
  if (body.skip()) {
    last_op = new Skip(std::shared_ptr<LogicalOperator>(last_op), body.skip());
  }
  // Limit is always after Skip.
  if (body.limit()) {
    last_op =
        new Limit(std::shared_ptr<LogicalOperator>(last_op), body.limit());
  }
  // Where may see new symbols so it comes after we generate Produce and in
  // general, comes after any OrderBy, Skip or Limit.
  if (body.where()) {
    last_op = new Filter(std::shared_ptr<LogicalOperator>(last_op),
                         body.where()->expression_);
  }
  return last_op;
}

// Converts multiple Patterns to Expansions. Each Pattern can contain an
// arbitrarily long chain of nodes and edges. The conversion to an Expansion is
// done by splitting a pattern into triplets (node1, edge, node2). The triplets
// conserve the semantics of the pattern. For example, in a pattern:
// (m) -[e]- (n) -[f]- (o) the same can be achieved with:
// (m) -[e]- (n), (n) -[f]- (o).
// This representation makes it easier to permute from which node or edge we
// want to start expanding.
std::vector<Expansion> NormalizePatterns(
    const SymbolTable &symbol_table, const std::vector<Pattern *> &patterns) {
  std::vector<Expansion> expansions;
  auto ignore_node = [&](auto *) {};
  auto collect_expansion = [&](auto *prev_node, auto *edge,
                               auto *current_node) {
    UsedSymbolsCollector collector(symbol_table);
    if (edge->lower_bound_) {
      edge->lower_bound_->Accept(collector);
    }
    if (edge->upper_bound_) {
      edge->upper_bound_->Accept(collector);
    }
    if (auto *bf_atom = dynamic_cast<BreadthFirstAtom *>(edge)) {
      // Get used symbols inside bfs filter expression and max depth.
      bf_atom->filter_expression_->Accept(collector);
      bf_atom->max_depth_->Accept(collector);
      // Remove symbols which are bound by the bfs itself.
      collector.symbols_.erase(
          symbol_table.at(*bf_atom->traversed_edge_identifier_));
      collector.symbols_.erase(
          symbol_table.at(*bf_atom->next_node_identifier_));
    }
    expansions.emplace_back(Expansion{prev_node, edge, edge->direction_, false,
                                      collector.symbols_, current_node});
  };
  for (const auto &pattern : patterns) {
    if (pattern->atoms_.size() == 1U) {
      auto *node = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
      debug_assert(node, "First pattern atom is not a node");
      expansions.emplace_back(Expansion{node});
    } else {
      ForEachPattern(*pattern, ignore_node, collect_expansion);
    }
  }
  return expansions;
}

// Fills the given Matching, by converting the Match patterns to normalized
// representation as Expansions. Filters used in the Match are also collected,
// as well as edge symbols which determine Cyphermorphism. Collecting filters
// will lift them out of a pattern and generate new expressions (just like they
// were in a Where clause).
void AddMatching(const std::vector<Pattern *> &patterns, Where *where,
                 SymbolTable &symbol_table, AstTreeStorage &storage,
                 Matching &matching) {
  auto expansions = NormalizePatterns(symbol_table, patterns);
  std::unordered_set<Symbol> edge_symbols;
  for (const auto &expansion : expansions) {
    // Matching may already have some expansions, so offset our index.
    const int expansion_ix = matching.expansions.size();
    // Map node1 symbol to expansion
    const auto &node1_sym = symbol_table.at(*expansion.node1->identifier_);
    matching.node_symbol_to_expansions[node1_sym].insert(expansion_ix);
    // Add node1 to all symbols.
    matching.expansion_symbols.insert(node1_sym);
    if (expansion.edge) {
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      // Fill edge symbols for Cyphermorphism.
      edge_symbols.insert(edge_sym);
      // Map node2 symbol to expansion
      const auto &node2_sym = symbol_table.at(*expansion.node2->identifier_);
      matching.node_symbol_to_expansions[node2_sym].insert(expansion_ix);
      // Add edge and node2 to all symbols
      matching.expansion_symbols.insert(edge_sym);
      matching.expansion_symbols.insert(node2_sym);
    }
    matching.expansions.push_back(expansion);
  }
  if (!edge_symbols.empty()) {
    matching.edge_symbols.emplace_back(edge_symbols);
  }
  for (auto *pattern : patterns) {
    matching.filters.CollectPatternFilters(*pattern, symbol_table, storage);
  }
  if (where) {
    matching.filters.CollectWhereFilter(*where, symbol_table);
  }
}
void AddMatching(const Match &match, SymbolTable &symbol_table,
                 AstTreeStorage &storage, Matching &matching) {
  return AddMatching(match.patterns_, match.where_, symbol_table, storage,
                     matching);
}

// Iterates over `all_filters` joining them in one expression via
// `FilterAndOperator`. Filters which use unbound symbols are skipped, as well
// as those that fail the `predicate` function. The function takes a single
// argument, `FilterInfo`. All the joined filters are removed from
// `all_filters`.
template <class TPredicate>
Expression *ExtractFilters(const std::unordered_set<Symbol> &bound_symbols,
                           std::vector<Filters::FilterInfo> &all_filters,
                           AstTreeStorage &storage,
                           const TPredicate &predicate) {
  Expression *filter_expr = nullptr;
  for (auto filters_it = all_filters.begin();
       filters_it != all_filters.end();) {
    if (HasBoundFilterSymbols(bound_symbols, *filters_it) &&
        predicate(*filters_it)) {
      filter_expr = impl::BoolJoin<FilterAndOperator>(storage, filter_expr,
                                                      filters_it->expression);
      filters_it = all_filters.erase(filters_it);
    } else {
      filters_it++;
    }
  }
  return filter_expr;
}

}  // namespace

namespace impl {

// Returns false if the symbol was already bound, otherwise binds it and
// returns true.
bool BindSymbol(std::unordered_set<Symbol> &bound_symbols,
                const Symbol &symbol) {
  auto insertion = bound_symbols.insert(symbol);
  return insertion.second;
}

Expression *ExtractMultiExpandFilter(
    const std::unordered_set<Symbol> &bound_symbols,
    const Symbol &expands_to_node,
    std::vector<Filters::FilterInfo> &all_filters, AstTreeStorage &storage) {
  return ExtractFilters(bound_symbols, all_filters, storage,
                        [&](const auto &filter) {
                          return filter.is_for_multi_expand &&
                                 filter.used_symbols.find(expands_to_node) ==
                                     filter.used_symbols.end();
                        });
}

LogicalOperator *GenFilters(LogicalOperator *last_op,
                            const std::unordered_set<Symbol> &bound_symbols,
                            std::vector<Filters::FilterInfo> &all_filters,
                            AstTreeStorage &storage) {
  auto *filter_expr = ExtractFilters(bound_symbols, all_filters, storage,
                                     [](const auto &) { return true; });
  if (filter_expr) {
    last_op =
        new Filter(std::shared_ptr<LogicalOperator>(last_op), filter_expr);
  }
  return last_op;
}

LogicalOperator *GenReturn(Return &ret, LogicalOperator *input_op,
                           SymbolTable &symbol_table, bool is_write,
                           const std::unordered_set<Symbol> &bound_symbols,
                           AstTreeStorage &storage) {
  // Similar to WITH clause, but we want to accumulate and advance command when
  // the query writes to the database. This way we handle the case when we want
  // to return expressions with the latest updated results. For example,
  // `MATCH (n) -- () SET n.prop = n.prop + 1 RETURN n.prop`. If we match same
  // `n` multiple 'k' times, we want to return 'k' results where the property
  // value is the same, final result of 'k' increments.
  bool accumulate = is_write;
  bool advance_command = false;
  ReturnBodyContext body(ret.body_, symbol_table, bound_symbols, storage);
  return GenReturnBody(input_op, advance_command, body, accumulate);
}

LogicalOperator *GenCreateForPattern(
    Pattern &pattern, LogicalOperator *input_op,
    const SymbolTable &symbol_table,
    std::unordered_set<Symbol> &bound_symbols) {
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

// Generate an operator for a clause which writes to the database. If the clause
// isn't handled, returns nullptr.
LogicalOperator *HandleWriteClause(Clause *clause, LogicalOperator *input_op,
                                   const SymbolTable &symbol_table,
                                   std::unordered_set<Symbol> &bound_symbols) {
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

LogicalOperator *GenWith(With &with, LogicalOperator *input_op,
                         SymbolTable &symbol_table, bool is_write,
                         std::unordered_set<Symbol> &bound_symbols,
                         AstTreeStorage &storage) {
  // WITH clause is Accumulate/Aggregate (advance_command) + Produce and
  // optional Filter. In case of update and aggregation, we want to accumulate
  // first, so that when aggregating, we get the latest results. Similar to
  // RETURN clause.
  bool accumulate = is_write;
  // No need to advance the command if we only performed reads.
  bool advance_command = is_write;
  ReturnBodyContext body(with.body_, symbol_table, bound_symbols, storage,
                         with.where_);
  LogicalOperator *last_op =
      GenReturnBody(input_op, advance_command, body, accumulate);
  // Reset bound symbols, so that only those in WITH are exposed.
  bound_symbols.clear();
  for (const auto &symbol : body.output_symbols()) {
    BindSymbol(bound_symbols, symbol);
  }
  return last_op;
}

}  // namespace impl

// Analyzes the filter expression by collecting information on filtering labels
// and properties to be used with indexing. Note that `all_filters_` are never
// updated here, but only `label_filters_` and `property_filters_` are.
void Filters::AnalyzeFilter(Expression *expr, const SymbolTable &symbol_table) {
  using Bound = ScanAllByLabelPropertyRange::Bound;
  auto get_property_lookup = [](auto *maybe_lookup, auto *&prop_lookup,
                                auto *&ident) {
    return (prop_lookup = dynamic_cast<PropertyLookup *>(maybe_lookup)) &&
           (ident = dynamic_cast<Identifier *>(prop_lookup->expression_));
  };
  auto add_prop_equal = [&](auto *maybe_lookup, auto *val_expr) {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      UsedSymbolsCollector collector(symbol_table);
      val_expr->Accept(collector);
      property_filters_[symbol_table.at(*ident)][prop_lookup->property_]
          .emplace_back(PropertyFilter{collector.symbols_, val_expr});
    }
  };
  auto add_prop_greater = [&](auto *expr1, auto *expr2, auto bound_type) {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(expr1, prop_lookup, ident)) {
      // n.prop > value
      UsedSymbolsCollector collector(symbol_table);
      expr2->Accept(collector);
      auto prop_filter = PropertyFilter{collector.symbols_};
      prop_filter.lower_bound = Bound{expr2, bound_type};
      property_filters_[symbol_table.at(*ident)][prop_lookup->property_]
          .emplace_back(std::move(prop_filter));
    }
    if (get_property_lookup(expr2, prop_lookup, ident)) {
      // value > n.prop
      UsedSymbolsCollector collector(symbol_table);
      expr1->Accept(collector);
      auto prop_filter = PropertyFilter{collector.symbols_};
      prop_filter.upper_bound = Bound{expr1, bound_type};
      property_filters_[symbol_table.at(*ident)][prop_lookup->property_]
          .emplace_back(std::move(prop_filter));
    }
  };
  // We are only interested to see the insides of And, because Or prevents
  // indexing since any labels and properties found there may be optional.
  if (auto *and_op = dynamic_cast<AndOperator *>(expr)) {
    AnalyzeFilter(and_op->expression1_, symbol_table);
    AnalyzeFilter(and_op->expression2_, symbol_table);
  } else if (auto *labels_test = dynamic_cast<LabelsTest *>(expr)) {
    // Since LabelsTest may contain any expression, we can only use the
    // simplest test on an identifier.
    if (auto *ident = dynamic_cast<Identifier *>(labels_test->expression_)) {
      const auto &symbol = symbol_table.at(*ident);
      label_filters_[symbol].insert(labels_test->labels_.begin(),
                                    labels_test->labels_.end());
    }
  } else if (auto *eq = dynamic_cast<EqualOperator *>(expr)) {
    // Try to get property equality test from the top expressions.
    // Unfortunately, we cannot go deeper inside Equal, because chained equals
    // need not correspond to And. For example, `(n.prop = value) = false)`:
    //         EQ
    //       /    \
    //      EQ   false  -- top expressions
    //    /    \
    // n.prop  value
    // Here the `prop` may be different than `value` resulting in `false`. This
    // would compare with the top level `false`, producing `true`. Therefore, it
    // is incorrect to pick up `n.prop = value` for scanning by property index.
    add_prop_equal(eq->expression1_, eq->expression2_);
    // And reversed.
    add_prop_equal(eq->expression2_, eq->expression1_);
  } else if (auto *gt = dynamic_cast<GreaterOperator *>(expr)) {
    add_prop_greater(gt->expression1_, gt->expression2_,
                     Bound::Type::EXCLUSIVE);
  } else if (auto *ge = dynamic_cast<GreaterEqualOperator *>(expr)) {
    add_prop_greater(ge->expression1_, ge->expression2_,
                     Bound::Type::INCLUSIVE);
  } else if (auto *lt = dynamic_cast<LessOperator *>(expr)) {
    // Like greater, but in reverse.
    add_prop_greater(lt->expression2_, lt->expression1_,
                     Bound::Type::EXCLUSIVE);
  } else if (auto *le = dynamic_cast<LessEqualOperator *>(expr)) {
    // Like greater equal, but in reverse.
    add_prop_greater(le->expression2_, le->expression1_,
                     Bound::Type::INCLUSIVE);
  }
  // TODO: Collect comparisons like `expr1 < n.prop < expr2` for potential
  // indexing by range. Note, that the generated Ast uses AND for chained
  // relation operators. Therefore, `expr1 < n.prop < expr2` will be represented
  // as `expr1 < n.prop AND n.prop < expr2`.
}

void Filters::CollectPatternFilters(Pattern &pattern, SymbolTable &symbol_table,
                                    AstTreeStorage &storage) {
  UsedSymbolsCollector collector(symbol_table);
  auto add_properties_filter = [&](auto *atom, bool is_variable_path = false) {
    debug_assert(
        dynamic_cast<BreadthFirstAtom *>(atom) && atom->properties_.empty() ||
            !dynamic_cast<BreadthFirstAtom *>(atom),
        "Property filters are not supported in BFS");
    const auto &symbol = symbol_table.at(*atom->identifier_);
    for (auto &prop_pair : atom->properties_) {
      collector.symbols_.clear();
      prop_pair.second->Accept(collector);
      auto *identifier = atom->identifier_;
      if (is_variable_path) {
        // Create a new identifier and a symbol which will be filled in All.
        identifier = identifier->Clone(storage);
        symbol_table[*identifier] =
            symbol_table.CreateSymbol(identifier->name_, false);
      } else {
        // Store a PropertyFilter on the value of the property.
        property_filters_[symbol][prop_pair.first.second].emplace_back(
            PropertyFilter{collector.symbols_, prop_pair.second});
      }
      // Create an equality expression and store it in all_filters_.
      auto *property_lookup =
          storage.Create<PropertyLookup>(identifier, prop_pair.first);
      auto *prop_equal =
          storage.Create<EqualOperator>(property_lookup, prop_pair.second);
      collector.symbols_.insert(symbol);  // PropertyLookup uses the symbol.
      if (is_variable_path) {
        all_filters_.emplace_back(
            FilterInfo{storage.Create<All>(identifier, atom->identifier_,
                                           storage.Create<Where>(prop_equal)),
                       collector.symbols_, true});
      } else {
        all_filters_.emplace_back(FilterInfo{prop_equal, collector.symbols_});
      }
    }
  };
  auto add_node_filter = [&](NodeAtom *node) {
    const auto &node_symbol = symbol_table.at(*node->identifier_);
    if (!node->labels_.empty()) {
      // Store the filtered labels.
      label_filters_[node_symbol].insert(node->labels_.begin(),
                                         node->labels_.end());
      // Create a LabelsTest and store it in all_filters_.
      all_filters_.emplace_back(FilterInfo{
          storage.Create<LabelsTest>(node->identifier_, node->labels_),
          std::unordered_set<Symbol>{node_symbol}});
    }
    add_properties_filter(node);
  };
  auto add_expand_filter = [&](NodeAtom *, EdgeAtom *edge, NodeAtom *node) {
    const auto &edge_symbol = symbol_table.at(*edge->identifier_);
    if (!edge->edge_types_.empty()) {
      edge_type_filters_[edge_symbol].insert(edge->edge_types_.begin(),
                                             edge->edge_types_.end());
      if (edge->has_range_) {
        // We need a new identifier and symbol for All.
        auto *ident_in_all = edge->identifier_->Clone(storage);
        symbol_table[*ident_in_all] =
            symbol_table.CreateSymbol(ident_in_all->name_, false);
        auto *edge_type_test =
            storage.Create<EdgeTypeTest>(ident_in_all, edge->edge_types_);
        all_filters_.emplace_back(FilterInfo{
            storage.Create<All>(ident_in_all, edge->identifier_,
                                storage.Create<Where>(edge_type_test)),
            std::unordered_set<Symbol>{edge_symbol}, true});
      } else if (auto *bf_atom = dynamic_cast<BreadthFirstAtom *>(edge)) {
        // BFS filters will be inlined inside the filter expression, so create
        // EdgeTypeTest which relies on traversed edge identifier. Set of
        // used symbols treats this as the original edge symbol.
        all_filters_.emplace_back(FilterInfo{
            storage.Create<EdgeTypeTest>(bf_atom->traversed_edge_identifier_,
                                         bf_atom->edge_types_),
            std::unordered_set<Symbol>{edge_symbol}, true});
      } else {
        all_filters_.emplace_back(FilterInfo{
            storage.Create<EdgeTypeTest>(edge->identifier_, edge->edge_types_),
            std::unordered_set<Symbol>{edge_symbol}});
      }
    }
    add_properties_filter(edge, edge->has_range_);
    add_node_filter(node);
  };
  ForEachPattern(pattern, add_node_filter, add_expand_filter);
}

// Adds the where filter expression to `all_filters_` and collects additional
// information for potential property and label indexing.
void Filters::CollectWhereFilter(Where &where,
                                 const SymbolTable &symbol_table) {
  UsedSymbolsCollector collector(symbol_table);
  where.expression_->Accept(collector);
  all_filters_.emplace_back(FilterInfo{where.expression_, collector.symbols_});
  AnalyzeFilter(where.expression_, symbol_table);
}

// Converts a Query to multiple QueryParts. In the process new Ast nodes may be
// created, e.g. filter expressions.
std::vector<QueryPart> CollectQueryParts(SymbolTable &symbol_table,
                                         AstTreeStorage &storage) {
  auto query = storage.query();
  std::vector<QueryPart> query_parts(1);
  auto *query_part = &query_parts.back();
  for (auto &clause : query->clauses_) {
    if (auto *match = dynamic_cast<Match *>(clause)) {
      if (match->optional_) {
        query_part->optional_matching.emplace_back(Matching{});
        AddMatching(*match, symbol_table, storage,
                    query_part->optional_matching.back());
      } else {
        debug_assert(query_part->optional_matching.empty(),
                     "Match clause cannot follow optional match.");
        AddMatching(*match, symbol_table, storage, query_part->matching);
      }
    } else {
      query_part->remaining_clauses.push_back(clause);
      if (auto *merge = dynamic_cast<query::Merge *>(clause)) {
        query_part->merge_matching.emplace_back(Matching{});
        AddMatching({merge->pattern_}, nullptr, symbol_table, storage,
                    query_part->merge_matching.back());
      } else if (dynamic_cast<With *>(clause) ||
                 dynamic_cast<query::Unwind *>(clause)) {
        // This query part is done, continue with a new one.
        query_parts.emplace_back(QueryPart{});
        query_part = &query_parts.back();
      } else if (dynamic_cast<Return *>(clause)) {
        // TODO: Support RETURN UNION ...
        return query_parts;
      }
    }
  }
  return query_parts;
}

}  // namespace query::plan
