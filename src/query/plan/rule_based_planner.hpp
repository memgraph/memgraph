/// @file
#pragma once

#include <experimental/optional>

#include "gflags/gflags.h"

#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"

DECLARE_int64(query_vertex_count_to_expand_existing);

namespace query::plan {

/// @brief Context which contains variables commonly used during planning.
template <class TDbAccessor>
struct PlanningContext {
  /// @brief SymbolTable is used to determine inputs and outputs of planned
  /// operators.
  ///
  /// Newly created AST nodes may be added to reference existing symbols.
  SymbolTable *symbol_table{nullptr};
  /// @brief The storage is used to create new AST nodes for use in operators.
  AstStorage *ast_storage{nullptr};
  /// @brief Cypher query to be planned
  CypherQuery *query{nullptr};
  /// @brief TDbAccessor, which may be used to get some information from the
  /// database to generate better plans. The accessor is required only to live
  /// long enough for the plan generation to finish.
  TDbAccessor *db{nullptr};
  /// @brief Symbol set is used to differentiate cycles in pattern matching.
  /// During planning, symbols will be added as each operator produces values
  /// for them. This way, the operator can be correctly initialized whether to
  /// read a symbol or write it. E.g. `MATCH (n) -[r]- (n)` would bind (and
  /// write) the first `n`, but the latter `n` would only read the already
  /// written information.
  std::unordered_set<Symbol> bound_symbols{};
};

template <class TDbAccessor>
auto MakePlanningContext(AstStorage *ast_storage, SymbolTable *symbol_table,
                         CypherQuery *query, TDbAccessor *db) {
  return PlanningContext<TDbAccessor>{symbol_table, ast_storage, query, db};
}

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

namespace impl {

// These functions are an internal implementation of RuleBasedPlanner. To avoid
// writing the whole code inline in this header file, they are declared here and
// defined in the cpp file.

// Iterates over `Filters` joining them in one expression via
// `AndOperator` if symbols they use are bound.. All the joined filters are
// removed from `Filters`.
Expression *ExtractFilters(const std::unordered_set<Symbol> &, Filters &,
                           AstStorage &);

std::unique_ptr<LogicalOperator> GenFilters(std::unique_ptr<LogicalOperator>,
                                            const std::unordered_set<Symbol> &,
                                            Filters &, AstStorage &);

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
  DCHECK(!pattern.atoms_.empty()) << "Missing atoms in pattern";
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  DCHECK(current_node) << "First pattern atom is not a node";
  auto last_res = base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    DCHECK(edge) << "Expected an edge atom in pattern.";
    DCHECK(atoms_it != pattern.atoms_.end())
        << "Edge atom should not end the pattern.";
    auto prev_node = current_node;
    current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    DCHECK(current_node) << "Expected a node atom in pattern.";
    last_res = collect(std::move(last_res), prev_node, edge, current_node);
  }
  return last_res;
}

// For all given `named_paths` checks if all its symbols have been bound.
// If so, it creates a logical operator for named path generation, binds its
// symbol, removes that path from the collection of unhandled ones and returns
// the new op. Otherwise, returns `last_op`.
std::unique_ptr<LogicalOperator> GenNamedPaths(
    std::unique_ptr<LogicalOperator> last_op,
    std::unordered_set<Symbol> &bound_symbols,
    std::unordered_map<Symbol, std::vector<Symbol>> &named_paths);

std::unique_ptr<LogicalOperator> GenReturn(
    Return &ret, std::unique_ptr<LogicalOperator> input_op,
    SymbolTable &symbol_table, bool is_write,
    const std::unordered_set<Symbol> &bound_symbols, AstStorage &storage);

std::unique_ptr<LogicalOperator> GenWith(
    With &with, std::unique_ptr<LogicalOperator> input_op,
    SymbolTable &symbol_table, bool is_write,
    std::unordered_set<Symbol> &bound_symbols, AstStorage &storage);

std::unique_ptr<LogicalOperator> GenUnion(
    const CypherUnion &cypher_union, std::shared_ptr<LogicalOperator> left_op,
    std::shared_ptr<LogicalOperator> right_op, SymbolTable &symbol_table);

template <class TBoolOperator>
Expression *BoolJoin(AstStorage &storage, Expression *expr1,
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
  explicit RuleBasedPlanner(TPlanningContext *context) : context_(context) {}

  /// @brief The result of plan generation is the root of the generated operator
  /// tree.
  using PlanResult = std::unique_ptr<LogicalOperator>;
  /// @brief Generates the operator tree based on explicitly set rules.
  PlanResult Plan(const std::vector<SingleQueryPart> &query_parts) {
    auto &context = *context_;
    std::unique_ptr<LogicalOperator> input_op;
    // Set to true if a query command writes to the database.
    bool is_write = false;
    for (const auto &query_part : query_parts) {
      MatchContext match_ctx{query_part.matching, *context.symbol_table,
                             context.bound_symbols};
      input_op = PlanMatching(match_ctx, std::move(input_op));
      for (const auto &matching : query_part.optional_matching) {
        MatchContext opt_ctx{matching, *context.symbol_table,
                             context.bound_symbols};
        auto match_op = PlanMatching(opt_ctx, nullptr);
        if (match_op) {
          input_op = std::make_unique<Optional>(
              std::move(input_op), std::move(match_op), opt_ctx.new_symbols);
        }
      }
      int merge_id = 0;
      for (auto &clause : query_part.remaining_clauses) {
        DCHECK(!dynamic_cast<Match *>(clause))
            << "Unexpected Match in remaining clauses";
        if (auto *ret = dynamic_cast<Return *>(clause)) {
          input_op = impl::GenReturn(
              *ret, std::move(input_op), *context.symbol_table, is_write,
              context.bound_symbols, *context.ast_storage);
        } else if (auto *merge = dynamic_cast<query::Merge *>(clause)) {
          input_op = GenMerge(*merge, std::move(input_op),
                              query_part.merge_matching[merge_id++]);
          // Treat MERGE clause as write, because we do not know if it will
          // create anything.
          is_write = true;
        } else if (auto *with = dynamic_cast<query::With *>(clause)) {
          input_op = impl::GenWith(*with, std::move(input_op),
                                   *context.symbol_table, is_write,
                                   context.bound_symbols, *context.ast_storage);
          // WITH clause advances the command, so reset the flag.
          is_write = false;
        } else if (auto op = HandleWriteClause(clause, input_op,
                                               *context.symbol_table,
                                               context.bound_symbols)) {
          is_write = true;
          input_op = std::move(op);
        } else if (auto *unwind = dynamic_cast<query::Unwind *>(clause)) {
          const auto &symbol =
              context.symbol_table->at(*unwind->named_expression_);
          context.bound_symbols.insert(symbol);
          input_op = std::make_unique<plan::Unwind>(
              std::move(input_op), unwind->named_expression_->expression_,
              symbol);
        } else {
          throw utils::NotYetImplemented("clause conversion to operator(s)");
        }
      }
    }
    return input_op;
  }

 private:
  TPlanningContext *context_;

  struct LabelPropertyIndex {
    LabelIx label;
    // FilterInfo with PropertyFilter.
    FilterInfo filter;
    int64_t vertex_count;
  };

  storage::Label GetLabel(LabelIx label) {
    return context_->db->Label(context_->ast_storage->labels_[label.ix]);
  }

  storage::Property GetProperty(PropertyIx prop) {
    return context_->db->Property(context_->ast_storage->properties_[prop.ix]);
  }

  storage::EdgeType GetEdgeType(EdgeTypeIx edge_type) {
    return context_->db->EdgeType(
        context_->ast_storage->edge_types_[edge_type.ix]);
  }

  std::unique_ptr<LogicalOperator> GenCreate(
      Create &create, std::unique_ptr<LogicalOperator> input_op,
      const SymbolTable &symbol_table,
      std::unordered_set<Symbol> &bound_symbols) {
    auto last_op = std::move(input_op);
    for (auto pattern : create.patterns_) {
      last_op = GenCreateForPattern(*pattern, std::move(last_op), symbol_table,
                                    bound_symbols);
    }
    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenCreateForPattern(
      Pattern &pattern, std::unique_ptr<LogicalOperator> input_op,
      const SymbolTable &symbol_table,
      std::unordered_set<Symbol> &bound_symbols) {
    auto node_to_creation_info = [&](const NodeAtom &node) {
      const auto &node_symbol = symbol_table.at(*node.identifier_);
      std::vector<storage::Label> labels;
      labels.reserve(node.labels_.size());
      for (const auto &label : node.labels_) {
        labels.push_back(GetLabel(label));
      }
      std::vector<std::pair<storage::Property, Expression *>> properties;
      properties.reserve(node.properties_.size());
      for (const auto &kv : node.properties_) {
        properties.push_back({GetProperty(kv.first), kv.second});
      }
      return NodeCreationInfo{node_symbol, labels, properties};
    };

    auto base = [&](NodeAtom *node) -> std::unique_ptr<LogicalOperator> {
      const auto &node_symbol = symbol_table.at(*node->identifier_);
      if (bound_symbols.insert(node_symbol).second) {
        auto node_info = node_to_creation_info(*node);
        return std::make_unique<CreateNode>(std::move(input_op), node_info);
      } else {
        return std::move(input_op);
      }
    };

    auto collect = [&](std::unique_ptr<LogicalOperator> last_op,
                       NodeAtom *prev_node, EdgeAtom *edge, NodeAtom *node) {
      // Store the symbol from the first node as the input to CreateExpand.
      const auto &input_symbol = symbol_table.at(*prev_node->identifier_);
      // If the expand node was already bound, then we need to indicate this,
      // so that CreateExpand only creates an edge.
      bool node_existing = false;
      if (!bound_symbols.insert(symbol_table.at(*node->identifier_)).second) {
        node_existing = true;
      }
      const auto &edge_symbol = symbol_table.at(*edge->identifier_);
      if (!bound_symbols.insert(edge_symbol).second) {
        LOG(FATAL) << "Symbols used for created edges cannot be redeclared.";
      }
      auto node_info = node_to_creation_info(*node);
      std::vector<std::pair<storage::Property, Expression *>> properties;
      properties.reserve(edge->properties_.size());
      for (const auto &kv : edge->properties_) {
        properties.push_back({GetProperty(kv.first), kv.second});
      }
      CHECK(edge->edge_types_.size() == 1)
          << "Creating an edge with a single type should be required by syntax";
      EdgeCreationInfo edge_info{edge_symbol, properties,
                                 GetEdgeType(edge->edge_types_[0]),
                                 edge->direction_};
      return std::make_unique<CreateExpand>(node_info, edge_info,
                                            std::move(last_op), input_symbol,
                                            node_existing);
    };

    auto last_op = impl::ReducePattern<std::unique_ptr<LogicalOperator>>(
        pattern, base, collect);

    // If the pattern is named, append the path constructing logical operator.
    if (pattern.identifier_->user_declared_) {
      std::vector<Symbol> path_elements;
      for (const PatternAtom *atom : pattern.atoms_)
        path_elements.emplace_back(symbol_table.at(*atom->identifier_));
      last_op = std::make_unique<ConstructNamedPath>(
          std::move(last_op), symbol_table.at(*pattern.identifier_),
          path_elements);
    }

    return last_op;
  }

  // Generate an operator for a clause which writes to the database. Ownership
  // of input_op is transferred to the newly created operator. If the clause
  // isn't handled, returns nullptr and input_op is left as is.
  std::unique_ptr<LogicalOperator> HandleWriteClause(
      Clause *clause, std::unique_ptr<LogicalOperator> &input_op,
      const SymbolTable &symbol_table,
      std::unordered_set<Symbol> &bound_symbols) {
    if (auto *create = dynamic_cast<Create *>(clause)) {
      return GenCreate(*create, std::move(input_op), symbol_table,
                       bound_symbols);
    } else if (auto *del = dynamic_cast<query::Delete *>(clause)) {
      return std::make_unique<plan::Delete>(std::move(input_op),
                                            del->expressions_, del->detach_);
    } else if (auto *set = dynamic_cast<query::SetProperty *>(clause)) {
      return std::make_unique<plan::SetProperty>(
          std::move(input_op), GetProperty(set->property_lookup_->property_),
          set->property_lookup_, set->expression_);
    } else if (auto *set = dynamic_cast<query::SetProperties *>(clause)) {
      auto op = set->update_ ? plan::SetProperties::Op::UPDATE
                             : plan::SetProperties::Op::REPLACE;
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      return std::make_unique<plan::SetProperties>(
          std::move(input_op), input_symbol, set->expression_, op);
    } else if (auto *set = dynamic_cast<query::SetLabels *>(clause)) {
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      std::vector<storage::Label> labels;
      labels.reserve(set->labels_.size());
      for (const auto &label : set->labels_) {
        labels.push_back(GetLabel(label));
      }
      return std::make_unique<plan::SetLabels>(std::move(input_op),
                                               input_symbol, labels);
    } else if (auto *rem = dynamic_cast<query::RemoveProperty *>(clause)) {
      return std::make_unique<plan::RemoveProperty>(
          std::move(input_op), GetProperty(rem->property_lookup_->property_),
          rem->property_lookup_);
    } else if (auto *rem = dynamic_cast<query::RemoveLabels *>(clause)) {
      const auto &input_symbol = symbol_table.at(*rem->identifier_);
      std::vector<storage::Label> labels;
      labels.reserve(rem->labels_.size());
      for (const auto &label : rem->labels_) {
        labels.push_back(GetLabel(label));
      }
      return std::make_unique<plan::RemoveLabels>(std::move(input_op),
                                                  input_symbol, labels);
    }
    return nullptr;
  }

  // Finds the label-property combination which has indexed the lowest amount of
  // vertices. If the index cannot be found, nullopt is returned.
  std::experimental::optional<LabelPropertyIndex> FindBestLabelPropertyIndex(
      const Symbol &symbol, const Filters &filters,
      const std::unordered_set<Symbol> &bound_symbols) {
    auto are_bound = [&bound_symbols](const auto &used_symbols) {
      for (const auto &used_symbol : used_symbols) {
        if (!utils::Contains(bound_symbols, used_symbol)) {
          return false;
        }
      }
      return true;
    };
    std::experimental::optional<LabelPropertyIndex> found;
    for (const auto &label : filters.FilteredLabels(symbol)) {
      for (const auto &filter : filters.PropertyFilters(symbol)) {
        const auto &property = filter.property_filter->property_;
        if (context_->db->LabelPropertyIndexExists(GetLabel(label),
                                                   GetProperty(property))) {
          int64_t vertex_count = context_->db->VerticesCount(
              GetLabel(label), GetProperty(property));
          if (!found || vertex_count < found->vertex_count) {
            if (filter.property_filter->is_symbol_in_value_) {
              // Skip filter expressions which use the symbol whose property
              // we are looking up. We cannot scan by such expressions. For
              // example, in `n.a = 2 + n.b` both sides of `=` refer to `n`,
              // so we cannot scan `n` by property index.
              continue;
            }
            if (are_bound(filter.used_symbols)) {
              // Take the property filter which uses bound symbols.
              found = LabelPropertyIndex{label, filter, vertex_count};
            }
          }
        }
      }
    }
    return found;
  }

  LabelIx FindBestLabelIndex(const std::unordered_set<LabelIx> &labels) {
    CHECK(!labels.empty())
        << "Trying to find the best label without any labels.";
    return *std::min_element(
        labels.begin(), labels.end(),
        [this](const auto &label1, const auto &label2) {
          return context_->db->VerticesCount(GetLabel(label1)) <
                 context_->db->VerticesCount(GetLabel(label2));
        });
  }

  // Creates a ScanAll by the best possible index for the `node_symbol`. Best
  // index is defined as the index with least number of vertices. If the node
  // does not have at least a label, no indexed lookup can be created and
  // `nullptr` is returned. The operator is chained after `last_op`. Optional
  // `max_vertex_count` controls, whether no operator should be created if the
  // vertex count in the best index exceeds this number. In such a case,
  // `nullptr` is returned and `last_op` is not chained.
  std::unique_ptr<ScanAll> GenScanByIndex(
      std::unique_ptr<LogicalOperator> &last_op, const Symbol &node_symbol,
      const MatchContext &match_ctx, Filters &filters,
      const std::experimental::optional<int64_t> &max_vertex_count =
          std::experimental::nullopt) {
    const auto labels = filters.FilteredLabels(node_symbol);
    if (labels.empty()) {
      // Without labels, we cannot generated any indexed ScanAll.
      return nullptr;
    }
    // First, try to see if we can use label+property index. If not, use just
    // the label index (which ought to exist).
    auto found_index = FindBestLabelPropertyIndex(node_symbol, filters,
                                                  match_ctx.bound_symbols);
    if (found_index &&
        // Use label+property index if we satisfy max_vertex_count.
        (!max_vertex_count || *max_vertex_count >= found_index->vertex_count)) {
      // Copy the property filter and then erase it from filters.
      const auto prop_filter = *found_index->filter.property_filter;
      filters.EraseFilter(found_index->filter);
      filters.EraseLabelFilter(node_symbol, found_index->label);
      if (prop_filter.lower_bound_ || prop_filter.upper_bound_) {
        return std::make_unique<ScanAllByLabelPropertyRange>(
            std::move(last_op), node_symbol, GetLabel(found_index->label),
            GetProperty(prop_filter.property_), prop_filter.property_.name,
            prop_filter.lower_bound_, prop_filter.upper_bound_,
            match_ctx.graph_view);
      } else {
        DCHECK(prop_filter.value_) << "Property filter should either have "
                                      "bounds or a value expression.";
        return std::make_unique<ScanAllByLabelPropertyValue>(
            std::move(last_op), node_symbol, GetLabel(found_index->label),
            GetProperty(prop_filter.property_), prop_filter.property_.name,
            prop_filter.value_, match_ctx.graph_view);
      }
    }
    auto label = FindBestLabelIndex(labels);
    if (max_vertex_count &&
        context_->db->VerticesCount(GetLabel(label)) > *max_vertex_count) {
      // Don't create an indexed lookup, since we have more labeled vertices
      // than the allowed count.
      return nullptr;
    }
    filters.EraseLabelFilter(node_symbol, label);
    return std::make_unique<ScanAllByLabel>(
        std::move(last_op), node_symbol, GetLabel(label), match_ctx.graph_view);
  }

  std::unique_ptr<LogicalOperator> PlanMatching(
      MatchContext &match_context, std::unique_ptr<LogicalOperator> input_op) {
    auto &bound_symbols = match_context.bound_symbols;
    auto &storage = *context_->ast_storage;
    const auto &symbol_table = match_context.symbol_table;
    const auto &matching = match_context.matching;
    // Copy filters, because we will modify them as we generate Filters.
    auto filters = matching.filters;
    // Copy the named_paths for the same reason.
    auto named_paths = matching.named_paths;
    // Try to generate any filters even before the 1st match operator. This
    // optimizes the optional match which filters only on symbols bound in
    // regular match.
    auto last_op =
        impl::GenFilters(std::move(input_op), bound_symbols, filters, storage);
    for (const auto &expansion : matching.expansions) {
      const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
      if (bound_symbols.insert(node1_symbol).second) {
        // We have just bound this symbol, so generate ScanAll which fills it.
        if (auto indexed_scan =
                GenScanByIndex(last_op, node1_symbol, match_context, filters)) {
          // First, try to get an indexed scan.
          last_op = std::move(indexed_scan);
        } else {
          // If indexed scan is not possible, we can only generate ScanAll of
          // everything.
          last_op = std::make_unique<ScanAll>(std::move(last_op), node1_symbol,
                                              match_context.graph_view);
        }
        match_context.new_symbols.emplace_back(node1_symbol);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters,
                                   storage);
        last_op =
            impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters,
                                   storage);
      }
      // We have an edge, so generate Expand.
      if (expansion.edge) {
        auto *edge = expansion.edge;
        // If the expand symbols were already bound, then we need to indicate
        // that they exist. The Expand will then check whether the pattern holds
        // instead of writing the expansion to symbols.
        const auto &node_symbol =
            symbol_table.at(*expansion.node2->identifier_);
        auto existing_node = utils::Contains(bound_symbols, node_symbol);
        const auto &edge_symbol = symbol_table.at(*edge->identifier_);
        DCHECK(!utils::Contains(bound_symbols, edge_symbol))
            << "Existing edges are not supported";
        std::vector<storage::EdgeType> edge_types;
        edge_types.reserve(edge->edge_types_.size());
        for (const auto &type : edge->edge_types_) {
          edge_types.push_back(GetEdgeType(type));
        }
        if (edge->IsVariable()) {
          std::experimental::optional<ExpansionLambda> weight_lambda;
          std::experimental::optional<Symbol> total_weight;

          if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH) {
            weight_lambda.emplace(ExpansionLambda{
                symbol_table.at(*edge->weight_lambda_.inner_edge),
                symbol_table.at(*edge->weight_lambda_.inner_node),
                edge->weight_lambda_.expression});

            total_weight.emplace(symbol_table.at(*edge->total_weight_));
          }

          ExpansionLambda filter_lambda;
          filter_lambda.inner_edge_symbol =
              symbol_table.at(*edge->filter_lambda_.inner_edge);
          filter_lambda.inner_node_symbol =
              symbol_table.at(*edge->filter_lambda_.inner_node);
          {
            // Bind the inner edge and node symbols so they're available for
            // inline filtering in ExpandVariable.
            bool inner_edge_bound =
                bound_symbols.insert(filter_lambda.inner_edge_symbol).second;
            bool inner_node_bound =
                bound_symbols.insert(filter_lambda.inner_node_symbol).second;
            DCHECK(inner_edge_bound && inner_node_bound)
                << "An inner edge and node can't be bound from before";
          }
          // Join regular filters with lambda filter expression, so that they
          // are done inline together. Semantic analysis should guarantee that
          // lambda filtering uses bound symbols.
          filter_lambda.expression = impl::BoolJoin<AndOperator>(
              storage, impl::ExtractFilters(bound_symbols, filters, storage),
              edge->filter_lambda_.expression);
          // At this point it's possible we have leftover filters for inline
          // filtering (they use the inner symbols. If they were not collected,
          // we have to remove them manually because no other filter-extraction
          // will ever bind them again.
          filters.erase(
              std::remove_if(
                  filters.begin(), filters.end(),
                  [e = filter_lambda.inner_edge_symbol,
                   n = filter_lambda.inner_node_symbol](FilterInfo &fi) {
                    return utils::Contains(fi.used_symbols, e) ||
                           utils::Contains(fi.used_symbols, n);
                  }),
              filters.end());
          // Unbind the temporarily bound inner symbols for filtering.
          bound_symbols.erase(filter_lambda.inner_edge_symbol);
          bound_symbols.erase(filter_lambda.inner_node_symbol);

          if (total_weight) {
            bound_symbols.insert(*total_weight);
          }

          // TODO: Pass weight lambda.
          CHECK(match_context.graph_view == GraphView::OLD)
              << "ExpandVariable should only be planned with GraphView::OLD";
          last_op = std::make_unique<ExpandVariable>(
              std::move(last_op), node1_symbol, node_symbol, edge_symbol,
              edge->type_, expansion.direction, edge_types,
              expansion.is_flipped, edge->lower_bound_, edge->upper_bound_,
              existing_node, filter_lambda, weight_lambda, total_weight);
        } else {
          if (!existing_node) {
            // Try to get better behaviour by creating an indexed scan and then
            // expanding into existing, instead of letting the Expand iterate
            // over all the edges.
            // Currently, just use the maximum vertex count flag, below which we
            // want to replace Expand with index ScanAll + Expand into existing.
            // It would be better to somehow test whether the input vertex
            // degree is larger than the destination vertex index count.
            auto indexed_scan =
                GenScanByIndex(last_op, node_symbol, match_context, filters,
                               FLAGS_query_vertex_count_to_expand_existing);
            if (indexed_scan) {
              last_op = std::move(indexed_scan);
              existing_node = true;
            }
          }
          last_op = std::make_unique<Expand>(
              std::move(last_op), node1_symbol, node_symbol, edge_symbol,
              expansion.direction, edge_types, existing_node,
              match_context.graph_view);
        }

        // Bind the expanded edge and node.
        bound_symbols.insert(edge_symbol);
        match_context.new_symbols.emplace_back(edge_symbol);
        if (bound_symbols.insert(node_symbol).second) {
          match_context.new_symbols.emplace_back(node_symbol);
        }

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
            last_op = std::make_unique<EdgeUniquenessFilter>(
                std::move(last_op), edge_symbol, other_symbols);
          }
        }
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters,
                                   storage);
        last_op =
            impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters,
                                   storage);
      }
    }
    DCHECK(named_paths.empty()) << "Expected to generate all named paths";
    // We bound all named path symbols, so just add them to new_symbols.
    for (const auto &named_path : matching.named_paths) {
      DCHECK(utils::Contains(bound_symbols, named_path.first))
          << "Expected generated named path to have bound symbol";
      match_context.new_symbols.emplace_back(named_path.first);
    }
    DCHECK(filters.empty()) << "Expected to generate all filters";
    return last_op;
  }

  auto GenMerge(query::Merge &merge, std::unique_ptr<LogicalOperator> input_op,
                const Matching &matching) {
    // Copy the bound symbol set, because we don't want to use the updated
    // version when generating the create part.
    std::unordered_set<Symbol> bound_symbols_copy(context_->bound_symbols);
    MatchContext match_ctx{matching, *context_->symbol_table,
                           bound_symbols_copy, GraphView::NEW};
    auto on_match = PlanMatching(match_ctx, nullptr);
    // Use the original bound_symbols, so we fill it with new symbols.
    auto on_create =
        GenCreateForPattern(*merge.pattern_, nullptr, *context_->symbol_table,
                            context_->bound_symbols);
    for (auto &set : merge.on_create_) {
      on_create = HandleWriteClause(set, on_create, *context_->symbol_table,
                                    context_->bound_symbols);
      DCHECK(on_create) << "Expected SET in MERGE ... ON CREATE";
    }
    for (auto &set : merge.on_match_) {
      on_match = HandleWriteClause(set, on_match, *context_->symbol_table,
                                   context_->bound_symbols);
      DCHECK(on_match) << "Expected SET in MERGE ... ON MATCH";
    }
    return std::make_unique<plan::Merge>(
        std::move(input_op), std::move(on_match), std::move(on_create));
  }
};

}  // namespace query::plan
