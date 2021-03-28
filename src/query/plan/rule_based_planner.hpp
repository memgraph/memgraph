/// @file
#pragma once

#include <optional>

#include "gflags/gflags.h"

#include "query/frontend/ast/ast.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/logging.hpp"

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
auto MakePlanningContext(AstStorage *ast_storage, SymbolTable *symbol_table, CypherQuery *query, TDbAccessor *db) {
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
  storage::View view = storage::View::OLD;
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
Expression *ExtractFilters(const std::unordered_set<Symbol> &, Filters &, AstStorage &);

std::unique_ptr<LogicalOperator> GenFilters(std::unique_ptr<LogicalOperator>, const std::unordered_set<Symbol> &,
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
auto ReducePattern(Pattern &pattern, std::function<T(NodeAtom *)> base,
                   std::function<T(T, NodeAtom *, EdgeAtom *, NodeAtom *)> collect) {
  MG_ASSERT(!pattern.atoms_.empty(), "Missing atoms in pattern");
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = utils::Downcast<NodeAtom>(*atoms_it++);
  MG_ASSERT(current_node, "First pattern atom is not a node");
  auto last_res = base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = utils::Downcast<EdgeAtom>(*atoms_it++);
    MG_ASSERT(edge, "Expected an edge atom in pattern.");
    MG_ASSERT(atoms_it != pattern.atoms_.end(), "Edge atom should not end the pattern.");
    auto prev_node = current_node;
    current_node = utils::Downcast<NodeAtom>(*atoms_it++);
    MG_ASSERT(current_node, "Expected a node atom in pattern.");
    last_res = collect(std::move(last_res), prev_node, edge, current_node);
  }
  return last_res;
}

// For all given `named_paths` checks if all its symbols have been bound.
// If so, it creates a logical operator for named path generation, binds its
// symbol, removes that path from the collection of unhandled ones and returns
// the new op. Otherwise, returns `last_op`.
std::unique_ptr<LogicalOperator> GenNamedPaths(std::unique_ptr<LogicalOperator> last_op,
                                               std::unordered_set<Symbol> &bound_symbols,
                                               std::unordered_map<Symbol, std::vector<Symbol>> &named_paths);

std::unique_ptr<LogicalOperator> GenReturn(Return &ret, std::unique_ptr<LogicalOperator> input_op,
                                           SymbolTable &symbol_table, bool is_write,
                                           const std::unordered_set<Symbol> &bound_symbols, AstStorage &storage);

std::unique_ptr<LogicalOperator> GenWith(With &with, std::unique_ptr<LogicalOperator> input_op,
                                         SymbolTable &symbol_table, bool is_write,
                                         std::unordered_set<Symbol> &bound_symbols, AstStorage &storage);

std::unique_ptr<LogicalOperator> GenUnion(const CypherUnion &cypher_union, std::shared_ptr<LogicalOperator> left_op,
                                          std::shared_ptr<LogicalOperator> right_op, SymbolTable &symbol_table);

template <class TBoolOperator>
Expression *BoolJoin(AstStorage &storage, Expression *expr1, Expression *expr2) {
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
      MatchContext match_ctx{query_part.matching, *context.symbol_table, context.bound_symbols};
      input_op = PlanMatching(match_ctx, std::move(input_op));
      for (const auto &matching : query_part.optional_matching) {
        MatchContext opt_ctx{matching, *context.symbol_table, context.bound_symbols};
        auto match_op = PlanMatching(opt_ctx, nullptr);
        if (match_op) {
          input_op = std::make_unique<Optional>(std::move(input_op), std::move(match_op), opt_ctx.new_symbols);
        }
      }
      uint64_t merge_id = 0;
      for (auto *clause : query_part.remaining_clauses) {
        MG_ASSERT(!utils::IsSubtype(*clause, Match::kType), "Unexpected Match in remaining clauses");
        if (auto *ret = utils::Downcast<Return>(clause)) {
          input_op = impl::GenReturn(*ret, std::move(input_op), *context.symbol_table, is_write, context.bound_symbols,
                                     *context.ast_storage);
        } else if (auto *merge = utils::Downcast<query::Merge>(clause)) {
          input_op = GenMerge(*merge, std::move(input_op), query_part.merge_matching[merge_id++]);
          // Treat MERGE clause as write, because we do not know if it will
          // create anything.
          is_write = true;
        } else if (auto *with = utils::Downcast<query::With>(clause)) {
          input_op = impl::GenWith(*with, std::move(input_op), *context.symbol_table, is_write, context.bound_symbols,
                                   *context.ast_storage);
          // WITH clause advances the command, so reset the flag.
          is_write = false;
        } else if (auto op = HandleWriteClause(clause, input_op, *context.symbol_table, context.bound_symbols)) {
          is_write = true;
          input_op = std::move(op);
        } else if (auto *unwind = utils::Downcast<query::Unwind>(clause)) {
          const auto &symbol = context.symbol_table->at(*unwind->named_expression_);
          context.bound_symbols.insert(symbol);
          input_op =
              std::make_unique<plan::Unwind>(std::move(input_op), unwind->named_expression_->expression_, symbol);
        } else if (auto *call_proc = utils::Downcast<query::CallProcedure>(clause)) {
          std::vector<Symbol> result_symbols;
          result_symbols.reserve(call_proc->result_identifiers_.size());
          for (const auto *ident : call_proc->result_identifiers_) {
            const auto &sym = context.symbol_table->at(*ident);
            context.bound_symbols.insert(sym);
            result_symbols.push_back(sym);
          }
          // TODO: When we add support for write and eager procedures, we will
          // need to plan this operator with Accumulate and pass in
          // storage::View::NEW.
          input_op = std::make_unique<plan::CallProcedure>(
              std::move(input_op), call_proc->procedure_name_, call_proc->arguments_, call_proc->result_fields_,
              result_symbols, call_proc->memory_limit_, call_proc->memory_scale_);
        } else if (auto *load_csv = utils::Downcast<query::LoadCsv>(clause)) {
          const auto &row_sym = context.symbol_table->at(*load_csv->row_var_);
          context.bound_symbols.insert(row_sym);

          input_op =
              std::make_unique<plan::LoadCsv>(std::move(input_op), load_csv->file_, load_csv->with_header_,
                                              load_csv->ignore_bad_, load_csv->delimiter_, load_csv->quote_, row_sym);
        } else {
          throw utils::NotYetImplemented("clause '{}' conversion to operator(s)", clause->GetTypeInfo().name);
        }
      }
    }
    return input_op;
  }

 private:
  TPlanningContext *context_;

  storage::LabelId GetLabel(LabelIx label) { return context_->db->NameToLabel(label.name); }

  storage::PropertyId GetProperty(PropertyIx prop) { return context_->db->NameToProperty(prop.name); }

  storage::EdgeTypeId GetEdgeType(EdgeTypeIx edge_type) { return context_->db->NameToEdgeType(edge_type.name); }

  std::unique_ptr<LogicalOperator> GenCreate(Create &create, std::unique_ptr<LogicalOperator> input_op,
                                             const SymbolTable &symbol_table,
                                             std::unordered_set<Symbol> &bound_symbols) {
    auto last_op = std::move(input_op);
    for (auto pattern : create.patterns_) {
      last_op = GenCreateForPattern(*pattern, std::move(last_op), symbol_table, bound_symbols);
    }
    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenCreateForPattern(Pattern &pattern, std::unique_ptr<LogicalOperator> input_op,
                                                       const SymbolTable &symbol_table,
                                                       std::unordered_set<Symbol> &bound_symbols) {
    auto node_to_creation_info = [&](const NodeAtom &node) {
      const auto &node_symbol = symbol_table.at(*node.identifier_);
      std::vector<storage::LabelId> labels;
      labels.reserve(node.labels_.size());
      for (const auto &label : node.labels_) {
        labels.push_back(GetLabel(label));
      }
      std::vector<std::pair<storage::PropertyId, Expression *>> properties;
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

    auto collect = [&](std::unique_ptr<LogicalOperator> last_op, NodeAtom *prev_node, EdgeAtom *edge, NodeAtom *node) {
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
        LOG_FATAL("Symbols used for created edges cannot be redeclared.");
      }
      auto node_info = node_to_creation_info(*node);
      std::vector<std::pair<storage::PropertyId, Expression *>> properties;
      properties.reserve(edge->properties_.size());
      for (const auto &kv : edge->properties_) {
        properties.push_back({GetProperty(kv.first), kv.second});
      }
      MG_ASSERT(edge->edge_types_.size() == 1, "Creating an edge with a single type should be required by syntax");
      EdgeCreationInfo edge_info{edge_symbol, properties, GetEdgeType(edge->edge_types_[0]), edge->direction_};
      return std::make_unique<CreateExpand>(node_info, edge_info, std::move(last_op), input_symbol, node_existing);
    };

    auto last_op = impl::ReducePattern<std::unique_ptr<LogicalOperator>>(pattern, base, collect);

    // If the pattern is named, append the path constructing logical operator.
    if (pattern.identifier_->user_declared_) {
      std::vector<Symbol> path_elements;
      for (const PatternAtom *atom : pattern.atoms_) path_elements.emplace_back(symbol_table.at(*atom->identifier_));
      last_op = std::make_unique<ConstructNamedPath>(std::move(last_op), symbol_table.at(*pattern.identifier_),
                                                     path_elements);
    }

    return last_op;
  }

  // Generate an operator for a clause which writes to the database. Ownership
  // of input_op is transferred to the newly created operator. If the clause
  // isn't handled, returns nullptr and input_op is left as is.
  std::unique_ptr<LogicalOperator> HandleWriteClause(Clause *clause, std::unique_ptr<LogicalOperator> &input_op,
                                                     const SymbolTable &symbol_table,
                                                     std::unordered_set<Symbol> &bound_symbols) {
    if (auto *create = utils::Downcast<Create>(clause)) {
      return GenCreate(*create, std::move(input_op), symbol_table, bound_symbols);
    } else if (auto *del = utils::Downcast<query::Delete>(clause)) {
      return std::make_unique<plan::Delete>(std::move(input_op), del->expressions_, del->detach_);
    } else if (auto *set = utils::Downcast<query::SetProperty>(clause)) {
      return std::make_unique<plan::SetProperty>(std::move(input_op), GetProperty(set->property_lookup_->property_),
                                                 set->property_lookup_, set->expression_);
    } else if (auto *set = utils::Downcast<query::SetProperties>(clause)) {
      auto op = set->update_ ? plan::SetProperties::Op::UPDATE : plan::SetProperties::Op::REPLACE;
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      return std::make_unique<plan::SetProperties>(std::move(input_op), input_symbol, set->expression_, op);
    } else if (auto *set = utils::Downcast<query::SetLabels>(clause)) {
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      std::vector<storage::LabelId> labels;
      labels.reserve(set->labels_.size());
      for (const auto &label : set->labels_) {
        labels.push_back(GetLabel(label));
      }
      return std::make_unique<plan::SetLabels>(std::move(input_op), input_symbol, labels);
    } else if (auto *rem = utils::Downcast<query::RemoveProperty>(clause)) {
      return std::make_unique<plan::RemoveProperty>(std::move(input_op), GetProperty(rem->property_lookup_->property_),
                                                    rem->property_lookup_);
    } else if (auto *rem = utils::Downcast<query::RemoveLabels>(clause)) {
      const auto &input_symbol = symbol_table.at(*rem->identifier_);
      std::vector<storage::LabelId> labels;
      labels.reserve(rem->labels_.size());
      for (const auto &label : rem->labels_) {
        labels.push_back(GetLabel(label));
      }
      return std::make_unique<plan::RemoveLabels>(std::move(input_op), input_symbol, labels);
    }
    return nullptr;
  }

  std::unique_ptr<LogicalOperator> PlanMatching(MatchContext &match_context,
                                                std::unique_ptr<LogicalOperator> input_op) {
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
    auto last_op = impl::GenFilters(std::move(input_op), bound_symbols, filters, storage);
    for (const auto &expansion : matching.expansions) {
      const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
      if (bound_symbols.insert(node1_symbol).second) {
        // We have just bound this symbol, so generate ScanAll which fills it.
        last_op = std::make_unique<ScanAll>(std::move(last_op), node1_symbol, match_context.view);
        match_context.new_symbols.emplace_back(node1_symbol);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters, storage);
        last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters, storage);
      }
      // We have an edge, so generate Expand.
      if (expansion.edge) {
        auto *edge = expansion.edge;
        // If the expand symbols were already bound, then we need to indicate
        // that they exist. The Expand will then check whether the pattern holds
        // instead of writing the expansion to symbols.
        const auto &node_symbol = symbol_table.at(*expansion.node2->identifier_);
        auto existing_node = utils::Contains(bound_symbols, node_symbol);
        const auto &edge_symbol = symbol_table.at(*edge->identifier_);
        MG_ASSERT(!utils::Contains(bound_symbols, edge_symbol), "Existing edges are not supported");
        std::vector<storage::EdgeTypeId> edge_types;
        edge_types.reserve(edge->edge_types_.size());
        for (const auto &type : edge->edge_types_) {
          edge_types.push_back(GetEdgeType(type));
        }
        if (edge->IsVariable()) {
          std::optional<ExpansionLambda> weight_lambda;
          std::optional<Symbol> total_weight;

          if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH) {
            weight_lambda.emplace(ExpansionLambda{symbol_table.at(*edge->weight_lambda_.inner_edge),
                                                  symbol_table.at(*edge->weight_lambda_.inner_node),
                                                  edge->weight_lambda_.expression});

            total_weight.emplace(symbol_table.at(*edge->total_weight_));
          }

          ExpansionLambda filter_lambda;
          filter_lambda.inner_edge_symbol = symbol_table.at(*edge->filter_lambda_.inner_edge);
          filter_lambda.inner_node_symbol = symbol_table.at(*edge->filter_lambda_.inner_node);
          {
            // Bind the inner edge and node symbols so they're available for
            // inline filtering in ExpandVariable.
            bool inner_edge_bound = bound_symbols.insert(filter_lambda.inner_edge_symbol).second;
            bool inner_node_bound = bound_symbols.insert(filter_lambda.inner_node_symbol).second;
            MG_ASSERT(inner_edge_bound && inner_node_bound, "An inner edge and node can't be bound from before");
          }
          // Join regular filters with lambda filter expression, so that they
          // are done inline together. Semantic analysis should guarantee that
          // lambda filtering uses bound symbols.
          filter_lambda.expression = impl::BoolJoin<AndOperator>(
              storage, impl::ExtractFilters(bound_symbols, filters, storage), edge->filter_lambda_.expression);
          // At this point it's possible we have leftover filters for inline
          // filtering (they use the inner symbols. If they were not collected,
          // we have to remove them manually because no other filter-extraction
          // will ever bind them again.
          filters.erase(std::remove_if(
                            filters.begin(), filters.end(),
                            [e = filter_lambda.inner_edge_symbol, n = filter_lambda.inner_node_symbol](FilterInfo &fi) {
                              return utils::Contains(fi.used_symbols, e) || utils::Contains(fi.used_symbols, n);
                            }),
                        filters.end());
          // Unbind the temporarily bound inner symbols for filtering.
          bound_symbols.erase(filter_lambda.inner_edge_symbol);
          bound_symbols.erase(filter_lambda.inner_node_symbol);

          if (total_weight) {
            bound_symbols.insert(*total_weight);
          }

          // TODO: Pass weight lambda.
          MG_ASSERT(match_context.view == storage::View::OLD,
                    "ExpandVariable should only be planned with storage::View::OLD");
          last_op = std::make_unique<ExpandVariable>(std::move(last_op), node1_symbol, node_symbol, edge_symbol,
                                                     edge->type_, expansion.direction, edge_types, expansion.is_flipped,
                                                     edge->lower_bound_, edge->upper_bound_, existing_node,
                                                     filter_lambda, weight_lambda, total_weight);
        } else {
          last_op = std::make_unique<Expand>(std::move(last_op), node1_symbol, node_symbol, edge_symbol,
                                             expansion.direction, edge_types, existing_node, match_context.view);
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
            if (symbol == edge_symbol || bound_symbols.find(symbol) == bound_symbols.end()) {
              continue;
            }
            other_symbols.push_back(symbol);
          }
          if (!other_symbols.empty()) {
            last_op = std::make_unique<EdgeUniquenessFilter>(std::move(last_op), edge_symbol, other_symbols);
          }
        }
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters, storage);
        last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
        last_op = impl::GenFilters(std::move(last_op), bound_symbols, filters, storage);
      }
    }
    MG_ASSERT(named_paths.empty(), "Expected to generate all named paths");
    // We bound all named path symbols, so just add them to new_symbols.
    for (const auto &named_path : matching.named_paths) {
      MG_ASSERT(utils::Contains(bound_symbols, named_path.first), "Expected generated named path to have bound symbol");
      match_context.new_symbols.emplace_back(named_path.first);
    }
    MG_ASSERT(filters.empty(), "Expected to generate all filters");
    return last_op;
  }

  auto GenMerge(query::Merge &merge, std::unique_ptr<LogicalOperator> input_op, const Matching &matching) {
    // Copy the bound symbol set, because we don't want to use the updated
    // version when generating the create part.
    std::unordered_set<Symbol> bound_symbols_copy(context_->bound_symbols);
    MatchContext match_ctx{matching, *context_->symbol_table, bound_symbols_copy, storage::View::NEW};
    auto on_match = PlanMatching(match_ctx, nullptr);
    // Use the original bound_symbols, so we fill it with new symbols.
    auto on_create = GenCreateForPattern(*merge.pattern_, nullptr, *context_->symbol_table, context_->bound_symbols);
    for (auto &set : merge.on_create_) {
      on_create = HandleWriteClause(set, on_create, *context_->symbol_table, context_->bound_symbols);
      MG_ASSERT(on_create, "Expected SET in MERGE ... ON CREATE");
    }
    for (auto &set : merge.on_match_) {
      on_match = HandleWriteClause(set, on_match, *context_->symbol_table, context_->bound_symbols);
      MG_ASSERT(on_match, "Expected SET in MERGE ... ON MATCH");
    }
    return std::make_unique<plan::Merge>(std::move(input_op), std::move(on_match), std::move(on_create));
  }
};

}  // namespace query::plan
