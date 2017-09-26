/// @file
#pragma once

#include "gflags/gflags.h"

#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"

DECLARE_int64(query_vertex_count_to_expand_existing);

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
    std::experimental::optional<Bound> lower_bound{};
    std::experimental::optional<Bound> upper_bound{};
  };

  /// Stores additional information for a filter expression.
  struct FilterInfo {
    /// The filter expression which must be satisfied.
    Expression *expression;
    /// Set of used symbols by the filter @c expression.
    std::unordered_set<Symbol> used_symbols;
    /// True if the filter is to be applied on multiple expanding edges.
    /// This is used to inline filtering in an @c ExpandVariable and
    /// @c ExpandBreadthFirst operators.
    bool is_for_multi_expand = false;
  };

  /// List of FilterInfo objects corresponding to all filter expressions that
  /// should be generated.
  auto &all_filters() { return all_filters_; }
  const auto &all_filters() const { return all_filters_; }
  /// Mapping from a symbol to labels that are filtered on it. These should be
  /// used only for generating indexed scans.
  const auto &label_filters() const { return label_filters_; }
  /// Mapping from a symbol to edge types that are filtered on it. These should
  /// be used for generating indexed expansions.
  const auto &edge_type_filters() const { return edge_type_filters_; }
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

  std::vector<FilterInfo> all_filters_;
  std::unordered_map<Symbol, std::unordered_set<GraphDbTypes::Label>>
      label_filters_;
  std::unordered_map<Symbol, std::unordered_set<GraphDbTypes::EdgeType>>
      edge_type_filters_;
  std::unordered_map<Symbol, std::unordered_map<GraphDbTypes::Property,
                                                std::vector<PropertyFilter>>>
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

/// @brief Context which contains variables commonly used during planning.
template <class TDbAccessor>
struct PlanningContext {
  /// @brief SymbolTable is used to determine inputs and outputs of planned
  /// operators.
  ///
  /// Newly created AST nodes may be added to reference existing symbols.
  SymbolTable &symbol_table;
  /// @brief The storage is used to traverse the AST as well as create new nodes
  /// for use in operators.
  AstTreeStorage &ast_storage;
  /// @brief TDbAccessor, which may be used to get some information from the
  /// database to generate better plans. The accessor is required only to live
  /// long enough for the plan generation to finish.
  const TDbAccessor &db;
  /// @brief Symbol set is used to differentiate cycles in pattern matching.
  ///
  /// During planning, symbols will be added as each operator produces values
  /// for them. This way, the operator can be correctly initialized whether to
  /// read a symbol or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and
  /// write) the first `n`, but the latter `n` would only read the already
  /// written information.
  std::unordered_set<Symbol> bound_symbols{};
};

// Contextual information used for generating match operators.
struct MatchContext {
  const Matching &matching;
  const SymbolTable &symbol_table;
  // Already bound symbols, which are used to determine whether the operator
  // should reference them or establish new. This is both read from and written
  // to during generation.
  std::unordered_set<Symbol> &bound_symbols;
  // Determines whether the match should see the new graph state or not.
  GraphView graph_view = GraphView::OLD;
  // All the newly established symbols in match.
  std::vector<Symbol> new_symbols{};
};

/// @brief Convert the AST to multiple @c QueryParts.
///
/// This function will normalize patterns inside @c Match and @c Merge clauses
/// and do some other preprocessing in order to generate multiple @c QueryPart
/// structures. @c AstTreeStorage and @c SymbolTable may be used to create new
/// AST nodes.
std::vector<QueryPart> CollectQueryParts(SymbolTable &, AstTreeStorage &);

namespace impl {

// These functions are an internal implementation of RuleBasedPlanner. To avoid
// writing the whole code inline in this header file, they are declared here and
// defined in the cpp file.

bool BindSymbol(std::unordered_set<Symbol> &bound_symbols,
                const Symbol &symbol);

// Looks for filter expressions, which can be inlined in an ExpandVariable
// operator. Such expressions are merged into one (via `and`) and removed from
// `all_filters`. If the expression uses `expands_to_node`, it is skipped. In
// such a case, we cannot cut variable expand short, since filtering may be
// satisfied by a node deeper in the path.
Expression *ExtractMultiExpandFilter(
    const std::unordered_set<Symbol> &bound_symbols,
    const Symbol &expands_to_node,
    std::vector<Filters::FilterInfo> &all_filters, AstTreeStorage &storage);

LogicalOperator *GenFilters(LogicalOperator *last_op,
                            const std::unordered_set<Symbol> &bound_symbols,
                            std::vector<Filters::FilterInfo> &all_filters,
                            AstTreeStorage &storage);
//
/// For all given `named_paths` checks if the all it's symbols have been bound.
/// If so it creates a logical operator for named path generation, binds it's
/// symbol, removes that path from the collection of unhandled ones and returns
/// the new op. Otherwise it returns nullptr.
LogicalOperator *GenNamedPaths(
    LogicalOperator *last_op, std::unordered_set<Symbol> &bound_symbols,
    std::unordered_map<Symbol, std::vector<Symbol>> &named_paths);

LogicalOperator *GenReturn(Return &ret, LogicalOperator *input_op,
                           SymbolTable &symbol_table, bool is_write,
                           const std::unordered_set<Symbol> &bound_symbols,
                           AstTreeStorage &storage);

LogicalOperator *GenCreateForPattern(Pattern &pattern,
                                     LogicalOperator *input_op,
                                     const SymbolTable &symbol_table,
                                     std::unordered_set<Symbol> &bound_symbols);

LogicalOperator *HandleWriteClause(Clause *clause, LogicalOperator *input_op,
                                   const SymbolTable &symbol_table,
                                   std::unordered_set<Symbol> &bound_symbols);

LogicalOperator *GenWith(With &with, LogicalOperator *input_op,
                         SymbolTable &symbol_table, bool is_write,
                         std::unordered_set<Symbol> &bound_symbols,
                         AstTreeStorage &storage);

template <class TBoolOperator>
Expression *BoolJoin(AstTreeStorage &storage, Expression *expr1,
                     Expression *expr2) {
  if (expr1 && expr2) {
    return storage.Create<TBoolOperator>(expr1, expr2);
  }
  return expr1 ? expr1 : expr2;
}

}  // namespace impl

/// @brief Planner which uses hardcoded rules to produce operators.
///
/// @sa MakeLogicalPlan
template <class TPlanningContext>
class RuleBasedPlanner {
 public:
  explicit RuleBasedPlanner(TPlanningContext &context) : context_(context) {}

  /// @brief The result of plan generation is the root of the generated operator
  /// tree.
  using PlanResult = std::unique_ptr<LogicalOperator>;
  /// @brief Generates the operator tree based on explicitly set rules.
  PlanResult Plan(std::vector<QueryPart> &query_parts) {
    auto &context = context_;
    LogicalOperator *input_op = nullptr;
    // Set to true if a query command writes to the database.
    bool is_write = false;
    for (const auto &query_part : query_parts) {
      MatchContext match_ctx{query_part.matching, context.symbol_table,
                             context.bound_symbols};
      input_op = PlanMatching(match_ctx, input_op);
      for (const auto &matching : query_part.optional_matching) {
        MatchContext opt_ctx{matching, context.symbol_table,
                             context.bound_symbols};
        auto *match_op = PlanMatching(opt_ctx, nullptr);
        if (match_op) {
          input_op = new Optional(std::shared_ptr<LogicalOperator>(input_op),
                                  std::shared_ptr<LogicalOperator>(match_op),
                                  opt_ctx.new_symbols);
        }
      }
      int merge_id = 0;
      for (auto &clause : query_part.remaining_clauses) {
        debug_assert(dynamic_cast<Match *>(clause) == nullptr,
                     "Unexpected Match in remaining clauses");
        if (auto *ret = dynamic_cast<Return *>(clause)) {
          input_op =
              impl::GenReturn(*ret, input_op, context.symbol_table, is_write,
                              context.bound_symbols, context.ast_storage);
        } else if (auto *merge = dynamic_cast<query::Merge *>(clause)) {
          input_op =
              GenMerge(*merge, input_op, query_part.merge_matching[merge_id++]);
          // Treat MERGE clause as write, because we do not know if it will
          // create anything.
          is_write = true;
        } else if (auto *with = dynamic_cast<query::With *>(clause)) {
          input_op =
              impl::GenWith(*with, input_op, context.symbol_table, is_write,
                            context.bound_symbols, context.ast_storage);
          // WITH clause advances the command, so reset the flag.
          is_write = false;
        } else if (auto *op = impl::HandleWriteClause(clause, input_op,
                                                      context.symbol_table,
                                                      context.bound_symbols)) {
          is_write = true;
          input_op = op;
        } else if (auto *unwind = dynamic_cast<query::Unwind *>(clause)) {
          const auto &symbol =
              context.symbol_table.at(*unwind->named_expression_);
          impl::BindSymbol(context.bound_symbols, symbol);
          input_op =
              new plan::Unwind(std::shared_ptr<LogicalOperator>(input_op),
                               unwind->named_expression_->expression_, symbol);
        } else if (auto *create_index =
                       dynamic_cast<query::CreateIndex *>(clause)) {
          debug_assert(!input_op, "Unexpected operator before CreateIndex");
          input_op = new plan::CreateIndex(create_index->label_,
                                           create_index->property_);
        } else {
          throw utils::NotYetImplemented("clause conversion to operator(s)");
        }
      }
    }
    return std::unique_ptr<LogicalOperator>(input_op);
  }

 private:
  TPlanningContext &context_;

  // Finds the label-property combination which has indexed the lowest amount of
  // vertices. `best_label` and `best_property` will be set to that combination
  // and the function will return (`true`, vertex count in index). If the index
  // cannot be found, the function will return (`false`, maximum int64_t), while
  // leaving `best_label` and `best_property` unchanged.
  std::pair<bool, int64_t> FindBestLabelPropertyIndex(
      const std::unordered_set<GraphDbTypes::Label> &labels,
      const std::unordered_map<GraphDbTypes::Property,
                               std::vector<Filters::PropertyFilter>>
          &property_filters,
      const Symbol &symbol, const std::unordered_set<Symbol> &bound_symbols,
      GraphDbTypes::Label &best_label,
      std::pair<GraphDbTypes::Property, Filters::PropertyFilter>
          &best_property) {
    auto are_bound = [&bound_symbols](const auto &used_symbols) {
      for (const auto &used_symbol : used_symbols) {
        if (bound_symbols.find(used_symbol) == bound_symbols.end()) {
          return false;
        }
      }
      return true;
    };
    bool found = false;
    int64_t min_count = std::numeric_limits<int64_t>::max();
    for (const auto &label : labels) {
      for (const auto &prop_pair : property_filters) {
        const auto &property = prop_pair.first;
        if (context_.db.LabelPropertyIndexExists(label, property)) {
          int64_t vertices_count = context_.db.VerticesCount(label, property);
          if (vertices_count < min_count) {
            for (const auto &prop_filter : prop_pair.second) {
              if (prop_filter.used_symbols.find(symbol) !=
                  prop_filter.used_symbols.end()) {
                // Skip filter expressions which use the symbol whose property
                // we are looking up. We cannot scan by such expressions. For
                // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`,
                // so we cannot scan `n` by property index.
                continue;
              }
              if (are_bound(prop_filter.used_symbols)) {
                // Take the first property filter which uses bound symbols.
                best_label = label;
                best_property = {property, prop_filter};
                min_count = vertices_count;
                found = true;
                break;
              }
            }
          }
        }
      }
    }
    return {found, min_count};
  }

  const GraphDbTypes::Label &FindBestLabelIndex(
      const std::unordered_set<GraphDbTypes::Label> &labels) {
    debug_assert(!labels.empty(),
                 "Trying to find the best label without any labels.");
    return *std::min_element(labels.begin(), labels.end(),
                             [this](const auto &label1, const auto &label2) {
                               return context_.db.VerticesCount(label1) <
                                      context_.db.VerticesCount(label2);
                             });
  }

  // Creates a ScanAll by the best possible index for the `node_symbol`. Best
  // index is defined as the index with least number of vertices. If the node
  // does not have at least a label, no indexed lookup can be created and
  // `nullptr` is returned. The operator is chained after `last_op`. Optional
  // `max_vertex_count` controls, whether no operator should be created if the
  // vertex count in the best index exceeds this number. In such a case,
  // `nullptr` is returned and `last_op` is not chained.
  ScanAll *GenScanByIndex(LogicalOperator *last_op, const Symbol &node_symbol,
                          const MatchContext &match_ctx,
                          const std::experimental::optional<int64_t>
                              &max_vertex_count = std::experimental::nullopt) {
    const auto labels =
        utils::FindOr(match_ctx.matching.filters.label_filters(), node_symbol,
                      std::unordered_set<GraphDbTypes::Label>())
            .first;
    if (labels.empty()) {
      // Without labels, we cannot generated any indexed ScanAll.
      return nullptr;
    }
    const auto properties =
        utils::FindOr(
            match_ctx.matching.filters.property_filters(), node_symbol,
            std::unordered_map<GraphDbTypes::Property,
                               std::vector<Filters::PropertyFilter>>())
            .first;
    // First, try to see if we can use label+property index. If not, use just
    // the label index (which ought to exist).
    GraphDbTypes::Label best_label;
    std::pair<GraphDbTypes::Property, Filters::PropertyFilter> best_property;
    auto found_index = FindBestLabelPropertyIndex(
        labels, properties, node_symbol, match_ctx.bound_symbols, best_label,
        best_property);
    if (found_index.first &&
        // Use label+property index if we satisfy max_vertex_count.
        (!max_vertex_count || *max_vertex_count >= found_index.second)) {
      const auto &prop_filter = best_property.second;
      if (prop_filter.lower_bound || prop_filter.upper_bound) {
        return new ScanAllByLabelPropertyRange(
            std::shared_ptr<LogicalOperator>(last_op), node_symbol, best_label,
            best_property.first, prop_filter.lower_bound,
            prop_filter.upper_bound, match_ctx.graph_view);
      } else {
        debug_assert(
            prop_filter.expression,
            "Property filter should either have bounds or an expression.");
        return new ScanAllByLabelPropertyValue(
            std::shared_ptr<LogicalOperator>(last_op), node_symbol, best_label,
            best_property.first, prop_filter.expression, match_ctx.graph_view);
      }
    }
    auto label = FindBestLabelIndex(labels);
    if (max_vertex_count &&
        context_.db.VerticesCount(label) > *max_vertex_count) {
      // Don't create an indexed lookup, since we have more labeled vertices
      // than the allowed count.
      return nullptr;
    }
    return new ScanAllByLabel(std::shared_ptr<LogicalOperator>(last_op),
                              node_symbol, label, match_ctx.graph_view);
  }

  LogicalOperator *PlanMatching(MatchContext &match_context,
                                LogicalOperator *input_op) {
    auto &bound_symbols = match_context.bound_symbols;
    auto &storage = context_.ast_storage;
    const auto &symbol_table = match_context.symbol_table;
    const auto &matching = match_context.matching;
    // Copy all_filters, because we will modify the list as we generate Filters.
    auto all_filters = matching.filters.all_filters();
    // Copy the named_paths for the same reason.
    auto named_paths = matching.named_paths;
    // Try to generate any filters even before the 1st match operator. This
    // optimizes the optional match which filters only on symbols bound in
    // regular match.
    auto *last_op =
        impl::GenFilters(input_op, bound_symbols, all_filters, storage);
    for (const auto &expansion : matching.expansions) {
      const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
      if (impl::BindSymbol(bound_symbols, node1_symbol)) {
        // We have just bound this symbol, so generate ScanAll which fills it.
        if (auto *indexed_scan =
                GenScanByIndex(last_op, node1_symbol, match_context)) {
          // First, try to get an indexed scan.
          last_op = indexed_scan;
        } else {
          // If indexed scan is not possible, we can only generate ScanAll of
          // everything.
          last_op = new ScanAll(std::shared_ptr<LogicalOperator>(last_op),
                                node1_symbol, match_context.graph_view);
        }
        match_context.new_symbols.emplace_back(node1_symbol);
        last_op =
            impl::GenFilters(last_op, bound_symbols, all_filters, storage);
        last_op = impl::GenNamedPaths(last_op, bound_symbols, named_paths);
        last_op =
            impl::GenFilters(last_op, bound_symbols, all_filters, storage);
      }
      // We have an edge, so generate Expand.
      if (expansion.edge) {
        // If the expand symbols were already bound, then we need to indicate
        // that they exist. The Expand will then check whether the pattern holds
        // instead of writing the expansion to symbols.
        const auto &node_symbol =
            symbol_table.at(*expansion.node2->identifier_);
        auto existing_node = false;
        if (!impl::BindSymbol(bound_symbols, node_symbol)) {
          existing_node = true;
        } else {
          match_context.new_symbols.emplace_back(node_symbol);
        }
        const auto &edge_symbol = symbol_table.at(*expansion.edge->identifier_);
        auto existing_edge = false;
        if (!impl::BindSymbol(bound_symbols, edge_symbol)) {
          existing_edge = true;
        } else {
          match_context.new_symbols.emplace_back(edge_symbol);
        }
        const auto edge_types_set =
            utils::FindOr(matching.filters.edge_type_filters(), edge_symbol,
                          std::unordered_set<GraphDbTypes::EdgeType>())
                .first;
        const std::vector<GraphDbTypes::EdgeType> edge_types(
            edge_types_set.begin(), edge_types_set.end());
        if (auto *bf_atom = dynamic_cast<BreadthFirstAtom *>(expansion.edge)) {
          const auto &traversed_edge_symbol =
              symbol_table.at(*bf_atom->traversed_edge_identifier_);
          const auto &next_node_symbol =
              symbol_table.at(*bf_atom->next_node_identifier_);
          // Inline BFS edge filtering together with its filter expression.
          auto *filter_expr = impl::BoolJoin<AndOperator>(
              storage, impl::ExtractMultiExpandFilter(
                           bound_symbols, node_symbol, all_filters, storage),
              bf_atom->filter_expression_);
          last_op = new ExpandBreadthFirst(
              node_symbol, edge_symbol, expansion.direction,
              std::move(edge_types), bf_atom->max_depth_, next_node_symbol,
              traversed_edge_symbol, filter_expr,
              std::shared_ptr<LogicalOperator>(last_op), node1_symbol,
              existing_node, match_context.graph_view);
        } else if (expansion.edge->has_range_) {
          auto *filter_expr = impl::ExtractMultiExpandFilter(
              bound_symbols, node_symbol, all_filters, storage);
          last_op = new ExpandVariable(
              node_symbol, edge_symbol, expansion.direction,
              std::move(edge_types), expansion.is_flipped,
              expansion.edge->lower_bound_, expansion.edge->upper_bound_,
              std::shared_ptr<LogicalOperator>(last_op), node1_symbol,
              existing_node, existing_edge, match_context.graph_view,
              filter_expr);
        } else {
          if (!existing_node) {
            // Try to get better behaviour by creating an indexed scan and then
            // expanding into existing, instead of letting the Expand iterate
            // over all the edges.
            // Currently, just use the maximum vertex count flag, below which we
            // want to replace Expand with index ScanAll + Expand into existing.
            // It would be better to somehow test whether the input vertex
            // degree is larger than the destination vertex index count.
            auto *indexed_scan =
                GenScanByIndex(last_op, node_symbol, match_context,
                               FLAGS_query_vertex_count_to_expand_existing);
            if (indexed_scan) {
              last_op = indexed_scan;
              existing_node = true;
            }
          }
          last_op = new Expand(node_symbol, edge_symbol, expansion.direction,
                               std::move(edge_types),
                               std::shared_ptr<LogicalOperator>(last_op),
                               node1_symbol, existing_node, existing_edge,
                               match_context.graph_view);
        }
        if (!existing_edge) {
          // Ensure Cyphermorphism (different edge symbols always map to
          // different edges).
          for (const auto &edge_symbols : matching.edge_symbols) {
            if (edge_symbols.find(edge_symbol) == edge_symbols.end()) {
              continue;
            }
            std::vector<Symbol> other_symbols;
            for (const auto &symbol : edge_symbols) {
              if (symbol == edge_symbol ||
                  bound_symbols.find(symbol) == bound_symbols.end()) {
                continue;
              }
              other_symbols.push_back(symbol);
            }
            if (!other_symbols.empty()) {
              last_op = new ExpandUniquenessFilter<EdgeAccessor>(
                  std::shared_ptr<LogicalOperator>(last_op), edge_symbol,
                  other_symbols);
            }
          }
        }
        last_op =
            impl::GenFilters(last_op, bound_symbols, all_filters, storage);
        last_op = impl::GenNamedPaths(last_op, bound_symbols, named_paths);
        last_op =
            impl::GenFilters(last_op, bound_symbols, all_filters, storage);
      }
    }
    debug_assert(all_filters.empty(), "Expected to generate all filters");
    return last_op;
  }

  auto GenMerge(query::Merge &merge, LogicalOperator *input_op,
                const Matching &matching) {
    // Copy the bound symbol set, because we don't want to use the updated
    // version when generating the create part.
    std::unordered_set<Symbol> bound_symbols_copy(context_.bound_symbols);
    MatchContext match_ctx{matching, context_.symbol_table, bound_symbols_copy,
                           GraphView::NEW};
    auto on_match = PlanMatching(match_ctx, nullptr);
    // Use the original bound_symbols, so we fill it with new symbols.
    auto on_create = impl::GenCreateForPattern(*merge.pattern_, nullptr,
                                               context_.symbol_table,
                                               context_.bound_symbols);
    for (auto &set : merge.on_create_) {
      on_create = impl::HandleWriteClause(set, on_create, context_.symbol_table,
                                          context_.bound_symbols);
      debug_assert(on_create, "Expected SET in MERGE ... ON CREATE");
    }
    for (auto &set : merge.on_match_) {
      on_match = impl::HandleWriteClause(set, on_match, context_.symbol_table,
                                         context_.bound_symbols);
      debug_assert(on_match, "Expected SET in MERGE ... ON MATCH");
    }
    return new plan::Merge(std::shared_ptr<LogicalOperator>(input_op),
                           std::shared_ptr<LogicalOperator>(on_match),
                           std::shared_ptr<LogicalOperator>(on_create));
  }
};

}  // namespace query::plan
