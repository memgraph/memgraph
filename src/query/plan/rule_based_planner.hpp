// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <cstdint>
#include <optional>
#include <variant>

#include "flags/run_time_configurable.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

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
  bool is_write_query{false};
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
Expression *ExtractFilters(const std::unordered_set<Symbol> &, Filters &, AstStorage &, Filters &);

/// Checks if the filters has all the bound symbols to be included in the current part of the query
bool HasBoundFilterSymbols(const std::unordered_set<Symbol> &bound_symbols, const FilterInfo &filter);

// Returns the set of symbols for the subquery that are actually referenced from the outer scope and
// used in the subquery.
std::unordered_set<Symbol> GetSubqueryBoundSymbols(const std::vector<SingleQueryPart> &single_query_parts,
                                                   SymbolTable &symbol_table, AstStorage &storage);

Symbol GetSymbol(NodeAtom *atom, const SymbolTable &symbol_table);
Symbol GetSymbol(EdgeAtom *atom, const SymbolTable &symbol_table);

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
  PlanResult Plan(const QueryParts &query_parts) {
    auto &context = *context_;
    std::unique_ptr<LogicalOperator> final_plan;
    // procedures need to start from 1
    // due to swapping mechanism of procedure
    // tracking
    uint64_t procedure_id = 1;
    for (const auto &query_part : query_parts.query_parts) {
      std::unique_ptr<LogicalOperator> input_op;

      context.is_write_query = false;
      for (const auto &single_query_part : query_part.single_query_parts) {
        input_op = HandleMatching(std::move(input_op), single_query_part, *context.symbol_table, context.bound_symbols);

        uint64_t merge_id = 0;
        uint64_t subquery_id = 0;

        for (const auto &clause : single_query_part.remaining_clauses) {
          MG_ASSERT(!utils::IsSubtype(*clause, Match::kType), "Unexpected Match in remaining clauses");
          if (auto *ret = utils::Downcast<Return>(clause)) {
            input_op = impl::GenReturn(*ret, std::move(input_op), *context.symbol_table, context.is_write_query,
                                       context.bound_symbols, *context.ast_storage);
          } else if (auto *merge = utils::Downcast<query::Merge>(clause)) {
            input_op = GenMerge(*merge, std::move(input_op), single_query_part.merge_matching[merge_id++]);
            // Treat MERGE clause as write, because we do not know if it will
            // create anything.
            context.is_write_query = true;
          } else if (auto *with = utils::Downcast<query::With>(clause)) {
            input_op = impl::GenWith(*with, std::move(input_op), *context.symbol_table, context.is_write_query,
                                     context.bound_symbols, *context.ast_storage);
            // WITH clause advances the command, so reset the flag.
            context.is_write_query = false;
          } else if (auto op = HandleWriteClause(clause, input_op, *context.symbol_table, context.bound_symbols)) {
            context.is_write_query = true;
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
                result_symbols, call_proc->memory_limit_, call_proc->memory_scale_, call_proc->is_write_,
                procedure_id++, call_proc->void_procedure_);
          } else if (auto *load_csv = utils::Downcast<query::LoadCsv>(clause)) {
            const auto &row_sym = context.symbol_table->at(*load_csv->row_var_);
            context.bound_symbols.insert(row_sym);

            input_op = std::make_unique<plan::LoadCsv>(std::move(input_op), load_csv->file_, load_csv->with_header_,
                                                       load_csv->ignore_bad_, load_csv->delimiter_, load_csv->quote_,
                                                       load_csv->nullif_, row_sym);
          } else if (auto *foreach = utils::Downcast<query::Foreach>(clause)) {
            context.is_write_query = true;
            input_op = HandleForeachClause(foreach, std::move(input_op), *context.symbol_table, context.bound_symbols,
                                           single_query_part, merge_id);
          } else if (auto *call_sub = utils::Downcast<query::CallSubquery>(clause)) {
            input_op = HandleSubquery(std::move(input_op), single_query_part.subqueries[subquery_id++],
                                      *context.symbol_table, *context_->ast_storage);
          } else {
            throw utils::NotYetImplemented("clause '{}' conversion to operator(s)", clause->GetTypeInfo().name);
          }
        }
      }

      // Is this the only situation that should be covered
      if (input_op->OutputSymbols(*context.symbol_table).empty()) {
        input_op = std::make_unique<EmptyResult>(std::move(input_op));
      }

      if (query_part.query_combinator) {
        final_plan = MergeWithCombinator(std::move(input_op), std::move(final_plan), *query_part.query_combinator);
      } else {
        final_plan = std::move(input_op);
      }
    }

    if (query_parts.distinct) {
      final_plan = MakeDistinct(std::move(final_plan));
    }

    return final_plan;
  }

 private:
  TPlanningContext *context_;

  storage::LabelId GetLabel(LabelIx label) { return context_->db->NameToLabel(label.name); }

  storage::PropertyId GetProperty(PropertyIx prop) { return context_->db->NameToProperty(prop.name); }

  storage::EdgeTypeId GetEdgeType(EdgeTypeIx edge_type) { return context_->db->NameToEdgeType(edge_type.name); }

  std::unique_ptr<LogicalOperator> HandleMatching(std::unique_ptr<LogicalOperator> last_op,
                                                  const SingleQueryPart &single_query_part, SymbolTable &symbol_table,
                                                  std::unordered_set<Symbol> &bound_symbols) {
    MatchContext match_ctx{single_query_part.matching, symbol_table, bound_symbols};
    last_op = PlanMatching(match_ctx, std::move(last_op));
    for (const auto &matching : single_query_part.optional_matching) {
      MatchContext opt_ctx{matching, symbol_table, bound_symbols};

      std::vector<Symbol> bound_symbols(context_->bound_symbols.begin(), context_->bound_symbols.end());
      auto once_with_symbols = std::make_unique<Once>(bound_symbols);

      auto match_op = PlanMatching(opt_ctx, std::move(once_with_symbols));
      if (match_op) {
        last_op = std::make_unique<Optional>(std::move(last_op), std::move(match_op), opt_ctx.new_symbols);
      }
    }

    return last_op;
  }

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

      auto properties = std::invoke([&]() -> std::variant<PropertiesMapList, ParameterLookup *> {
        if (const auto *node_properties =
                std::get_if<std::unordered_map<PropertyIx, Expression *>>(&node.properties_)) {
          PropertiesMapList vector_props;
          vector_props.reserve(node_properties->size());
          for (const auto &kv : *node_properties) {
            vector_props.push_back({GetProperty(kv.first), kv.second});
          }
          return std::move(vector_props);
        }
        return std::get<ParameterLookup *>(node.properties_);
      });
      return NodeCreationInfo{node_symbol, labels, properties};
    };

    auto base = [&](NodeAtom *node) -> std::unique_ptr<LogicalOperator> {
      const auto &node_symbol = symbol_table.at(*node->identifier_);
      if (bound_symbols.insert(node_symbol).second) {
        auto node_info = node_to_creation_info(*node);
        return std::make_unique<CreateNode>(std::move(input_op), node_info);
      }
      return std::move(input_op);
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
      auto properties = std::invoke([&]() -> std::variant<PropertiesMapList, ParameterLookup *> {
        if (const auto *edge_properties =
                std::get_if<std::unordered_map<PropertyIx, Expression *>>(&edge->properties_)) {
          PropertiesMapList vector_props;
          vector_props.reserve(edge_properties->size());
          for (const auto &kv : *edge_properties) {
            vector_props.push_back({GetProperty(kv.first), kv.second});
          }
          return std::move(vector_props);
        }
        return std::get<ParameterLookup *>(edge->properties_);
      });

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
    auto last_op = GenFilters(std::move(input_op), bound_symbols, filters, storage, symbol_table);

    last_op = HandleExpansions(std::move(last_op), matching, symbol_table, storage, bound_symbols,
                               match_context.new_symbols, named_paths, filters, match_context.view);

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

    std::vector<Symbol> bound_symbols(context_->bound_symbols.begin(), context_->bound_symbols.end());

    auto once_with_symbols = std::make_unique<Once>(bound_symbols);
    auto on_match = PlanMatching(match_ctx, std::move(once_with_symbols));

    once_with_symbols = std::make_unique<Once>(std::move(bound_symbols));
    // Use the original bound_symbols, so we fill it with new symbols.
    auto on_create = GenCreateForPattern(*merge.pattern_, std::move(once_with_symbols), *context_->symbol_table,
                                         context_->bound_symbols);
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

  std::unique_ptr<LogicalOperator> HandleExpansions(std::unique_ptr<LogicalOperator> last_op, const Matching &matching,
                                                    const SymbolTable &symbol_table, AstStorage &storage,
                                                    std::unordered_set<Symbol> &bound_symbols,
                                                    std::vector<Symbol> &new_symbols,
                                                    std::unordered_map<Symbol, std::vector<Symbol>> &named_paths,
                                                    Filters &filters, storage::View view) {
    if (flags::run_time::GetCartesianProductEnabled()) {
      return HandleExpansionsWithCartesian(std::move(last_op), matching, symbol_table, storage, bound_symbols,
                                           new_symbols, named_paths, filters, view);
    }

    return HandleExpansionsWithoutCartesian(std::move(last_op), matching, symbol_table, storage, bound_symbols,
                                            new_symbols, named_paths, filters, view);
  }

  std::unique_ptr<LogicalOperator> HandleExpansionsWithCartesian(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching, const SymbolTable &symbol_table,
      AstStorage &storage, std::unordered_set<Symbol> &bound_symbols, std::vector<Symbol> &new_symbols,
      std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters, storage::View view) {
    if (matching.expansions.empty()) {
      return last_op;
    }

    std::set<ExpansionGroupId> all_expansion_groups;
    for (const auto &expansion : matching.expansions) {
      all_expansion_groups.insert(expansion.expansion_group_id);
    }

    std::set<ExpansionGroupId> visited_expansion_groups;

    last_op =
        GenerateExpansionOnAlreadySeenSymbols(std::move(last_op), matching, visited_expansion_groups, symbol_table,
                                              storage, bound_symbols, new_symbols, named_paths, filters, view);

    // We want to create separate branches of scan operators for each expansion group group of patterns
    // Whenever there are 2 scan branches, they will be joined with a Cartesian operator

    // New symbols from the opposite branch
    // We need to see what are cross new symbols in order to check for edge uniqueness for cross branch of same matching
    // Since one matching needs to comfort to Cyphermorphism
    std::vector<Symbol> cross_branch_new_symbols;
    bool initial_expansion_done = false;
    for (const auto &expansion : matching.expansions) {
      if (visited_expansion_groups.contains(expansion.expansion_group_id)) {
        continue;
      }

      std::unique_ptr<LogicalOperator> starting_expansion_operator = nullptr;
      if (!initial_expansion_done) {
        starting_expansion_operator = std::move(last_op);
        initial_expansion_done = true;
      }
      std::vector<Symbol> starting_symbols;
      if (starting_expansion_operator) {
        starting_symbols = starting_expansion_operator->ModifiedSymbols(symbol_table);
      }
      std::vector<Symbol> new_expansion_group_symbols;
      std::unordered_set<Symbol> new_bound_symbols{starting_symbols.begin(), starting_symbols.end()};
      std::unique_ptr<LogicalOperator> expansion_group = GenerateExpansionGroup(
          std::move(starting_expansion_operator), matching, symbol_table, storage, new_bound_symbols,
          new_expansion_group_symbols, named_paths, filters, view, expansion.expansion_group_id);

      visited_expansion_groups.insert(expansion.expansion_group_id);

      new_symbols.insert(new_symbols.end(), new_expansion_group_symbols.begin(), new_expansion_group_symbols.end());
      bound_symbols.insert(new_bound_symbols.begin(), new_bound_symbols.end());

      // If we just started and have no beginning operator, make the beginning operator and transfer cross symbols
      // for next iteration
      bool started_matching_operators = !last_op;
      bool has_more_expansions = visited_expansion_groups.size() < all_expansion_groups.size();
      if (started_matching_operators) {
        last_op = std::move(expansion_group);
        if (has_more_expansions) {
          cross_branch_new_symbols = new_expansion_group_symbols;
        }
        continue;
      }

      // if there is already a last operator, then we have 2 branches that we can merge into cartesian
      last_op = GenerateCartesian(std::move(last_op), std::move(expansion_group), symbol_table);

      // additionally, check for Cyphermorphism of the previous branch with new bound symbols
      for (const auto &new_symbol : cross_branch_new_symbols) {
        if (new_symbol.type_ == Symbol::Type::EDGE) {
          last_op = EnsureCyphermorphism(std::move(last_op), new_symbol, matching, new_bound_symbols);
        }
      }

      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);

      // we aggregate all the so far new symbols so we can test them in the next iteration against the new
      // expansion group
      if (has_more_expansions) {
        cross_branch_new_symbols.insert(cross_branch_new_symbols.end(), new_expansion_group_symbols.begin(),
                                        new_expansion_group_symbols.end());
      }
    }

    MG_ASSERT(visited_expansion_groups.size() == all_expansion_groups.size(),
              "Did not create expansions for all expansion group expansions in the planner!");

    return last_op;
  }

  std::unique_ptr<LogicalOperator> HandleExpansionsWithoutCartesian(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching, const SymbolTable &symbol_table,
      AstStorage &storage, std::unordered_set<Symbol> &bound_symbols, std::vector<Symbol> &new_symbols,
      std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters, storage::View view) {
    for (const auto &expansion : matching.expansions) {
      last_op = GenerateOperatorsForExpansion(std::move(last_op), matching, expansion, symbol_table, storage,
                                              bound_symbols, new_symbols, named_paths, filters, view);
    }

    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateExpansionOnAlreadySeenSymbols(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching,
      std::set<ExpansionGroupId> &visited_expansion_groups, SymbolTable symbol_table, AstStorage &storage,
      std::unordered_set<Symbol> &bound_symbols, std::vector<Symbol> &new_symbols,
      std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters, storage::View view) {
    bool added_new_expansions = true;
    while (added_new_expansions) {
      added_new_expansions = false;
      for (const auto &expansion : matching.expansions) {
        // We want to create separate matching branch operators for each expansion group group of patterns
        if (visited_expansion_groups.contains(expansion.expansion_group_id)) {
          continue;
        }

        bool src_node_already_seen = bound_symbols.contains(impl::GetSymbol(expansion.node1, symbol_table));
        bool edge_already_seen =
            expansion.edge && bound_symbols.contains(impl::GetSymbol(expansion.edge, symbol_table));
        bool dest_node_already_seen =
            expansion.edge && bound_symbols.contains(impl::GetSymbol(expansion.node2, symbol_table));

        if (src_node_already_seen || edge_already_seen || dest_node_already_seen) {
          last_op = GenerateExpansionGroup(std::move(last_op), matching, symbol_table, storage, bound_symbols,
                                           new_symbols, named_paths, filters, view, expansion.expansion_group_id);
          visited_expansion_groups.insert(expansion.expansion_group_id);
          added_new_expansions = true;
          break;
        }
      }
    }

    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateExpansionGroup(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching, const SymbolTable &symbol_table,
      AstStorage &storage, std::unordered_set<Symbol> &bound_symbols, std::vector<Symbol> &new_symbols,
      std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters, storage::View view,
      ExpansionGroupId expansion_group_id) {
    for (size_t i = 0, size = matching.expansions.size(); i < size; i++) {
      const auto &expansion = matching.expansions[i];

      if (expansion.expansion_group_id != expansion_group_id) {
        continue;
      }

      // When we picked a pattern to expand, we expand it through the end
      last_op = GenerateOperatorsForExpansion(std::move(last_op), matching, expansion, symbol_table, storage,
                                              bound_symbols, new_symbols, named_paths, filters, view);
    }
    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateOperatorsForExpansion(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching, const Expansion &expansion,
      const SymbolTable &symbol_table, AstStorage &storage, std::unordered_set<Symbol> &bound_symbols,
      std::vector<Symbol> &new_symbols, std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters,
      storage::View view) {
    const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
    if (bound_symbols.insert(node1_symbol).second) {
      // We have just bound this symbol, so generate ScanAll which fills it.
      last_op = std::make_unique<ScanAll>(std::move(last_op), node1_symbol, view);
      new_symbols.emplace_back(node1_symbol);

      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
      last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
    } else if (named_paths.size() == 1U) {
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
      last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
    }

    if (expansion.edge) {
      last_op = GenExpand(std::move(last_op), expansion, symbol_table, bound_symbols, matching, storage, filters,
                          named_paths, new_symbols, view);
    }

    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenExpand(std::unique_ptr<LogicalOperator> last_op, const Expansion &expansion,
                                             const SymbolTable &symbol_table, std::unordered_set<Symbol> &bound_symbols,
                                             const Matching &matching, AstStorage &storage, Filters &filters,
                                             std::unordered_map<Symbol, std::vector<Symbol>> &named_paths,
                                             std::vector<Symbol> &new_symbols, storage::View view) {
    // If the expand symbols were already bound, then we need to indicate
    // that they exist. The Expand will then check whether the pattern holds
    // instead of writing the expansion to symbols.
    const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
    bound_symbols.insert(node1_symbol);

    const auto &node_symbol = symbol_table.at(*expansion.node2->identifier_);
    auto *edge = expansion.edge;

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

      if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
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
      Filters all_filters;
      filter_lambda.expression = impl::BoolJoin<AndOperator>(
          storage, impl::ExtractFilters(bound_symbols, filters, storage, all_filters), edge->filter_lambda_.expression);
      // At this point it's possible we have leftover filters for inline
      // filtering (they use the inner symbols. If they were not collected,
      // we have to remove them manually because no other filter-extraction
      // will ever bind them again.
      filters.erase(
          std::remove_if(filters.begin(), filters.end(),
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
      MG_ASSERT(view == storage::View::OLD, "ExpandVariable should only be planned with storage::View::OLD");
      last_op = std::make_unique<ExpandVariable>(std::move(last_op), node1_symbol, node_symbol, edge_symbol,
                                                 edge->type_, expansion.direction, edge_types, expansion.is_flipped,
                                                 edge->lower_bound_, edge->upper_bound_, existing_node, filter_lambda,
                                                 weight_lambda, total_weight);
    } else {
      last_op = std::make_unique<Expand>(std::move(last_op), node1_symbol, node_symbol, edge_symbol,
                                         expansion.direction, edge_types, existing_node, view);
    }

    // Bind the expanded edge and node.
    bound_symbols.insert(edge_symbol);
    new_symbols.emplace_back(edge_symbol);
    if (bound_symbols.insert(node_symbol).second) {
      new_symbols.emplace_back(node_symbol);
    }

    last_op = EnsureCyphermorphism(std::move(last_op), edge_symbol, matching, bound_symbols);

    last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
    last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
    last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);

    return last_op;
  }

  std::unique_ptr<LogicalOperator> EnsureCyphermorphism(std::unique_ptr<LogicalOperator> last_op,
                                                        const Symbol &edge_symbol, const Matching &matching,
                                                        const std::unordered_set<Symbol> &bound_symbols) {
    // Ensure Cyphermorphism (different edge symbols always map to
    // different edges).
    for (const auto &edge_symbols : matching.edge_symbols) {
      if (edge_symbols.size() <= 1) {
        // nothing to test edge uniqueness with
        continue;
      }
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

    return last_op;
  }

  std::unique_ptr<LogicalOperator> HandleForeachClause(query::Foreach *foreach,
                                                       std::unique_ptr<LogicalOperator> input_op,
                                                       const SymbolTable &symbol_table,
                                                       std::unordered_set<Symbol> &bound_symbols,
                                                       const SingleQueryPart &query_part, uint64_t &merge_id) {
    const auto &symbol = symbol_table.at(*foreach->named_expression_);
    bound_symbols.insert(symbol);
    std::unique_ptr<LogicalOperator> op = std::make_unique<plan::Once>();
    for (auto *clause : foreach->clauses_) {
      if (auto *nested_for_each = utils::Downcast<query::Foreach>(clause)) {
        op = HandleForeachClause(nested_for_each, std::move(op), symbol_table, bound_symbols, query_part, merge_id);
      } else if (auto *merge = utils::Downcast<query::Merge>(clause)) {
        op = GenMerge(*merge, std::move(op), query_part.merge_matching[merge_id++]);
      } else {
        op = HandleWriteClause(clause, op, symbol_table, bound_symbols);
      }
    }
    return std::make_unique<plan::Foreach>(std::move(input_op), std::move(op), foreach->named_expression_->expression_,
                                           symbol);
  }

  std::unique_ptr<LogicalOperator> HandleSubquery(std::unique_ptr<LogicalOperator> last_op,
                                                  std::shared_ptr<QueryParts> subquery, SymbolTable &symbol_table,
                                                  AstStorage &storage) {
    std::unordered_set<Symbol> outer_scope_bound_symbols;
    outer_scope_bound_symbols.insert(std::make_move_iterator(context_->bound_symbols.begin()),
                                     std::make_move_iterator(context_->bound_symbols.end()));

    context_->bound_symbols =
        impl::GetSubqueryBoundSymbols(subquery->query_parts[0].single_query_parts, symbol_table, storage);

    auto subquery_op = Plan(*subquery);

    context_->bound_symbols.clear();
    context_->bound_symbols.insert(std::make_move_iterator(outer_scope_bound_symbols.begin()),
                                   std::make_move_iterator(outer_scope_bound_symbols.end()));

    auto subquery_has_return = true;
    if (subquery_op->GetTypeInfo() == EmptyResult::kType) {
      subquery_has_return = false;
    }

    last_op = std::make_unique<Apply>(std::move(last_op), std::move(subquery_op), subquery_has_return);

    if (context_->is_write_query) {
      last_op = std::make_unique<Accumulate>(std::move(last_op), last_op->ModifiedSymbols(symbol_table), true);
    }

    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateCartesian(std::unique_ptr<LogicalOperator> left,
                                                     std::unique_ptr<LogicalOperator> right,
                                                     const SymbolTable &symbol_table) {
    auto left_symbols = left->ModifiedSymbols(symbol_table);
    auto right_symbols = right->ModifiedSymbols(symbol_table);
    return std::make_unique<Cartesian>(std::move(left), left_symbols, std::move(right), right_symbols);
  }

  std::unique_ptr<LogicalOperator> GenFilters(std::unique_ptr<LogicalOperator> last_op,
                                              const std::unordered_set<Symbol> &bound_symbols, Filters &filters,
                                              AstStorage &storage, const SymbolTable &symbol_table) {
    Filters all_filters{};
    auto pattern_filters = ExtractPatternFilters(filters, symbol_table, storage, bound_symbols);
    auto *filter_expr = impl::ExtractFilters(bound_symbols, filters, storage, all_filters);

    if (filter_expr) {
      last_op =
          std::make_unique<Filter>(std::move(last_op), std::move(pattern_filters), filter_expr, std::move(all_filters));
    }
    return last_op;
  }

  std::unique_ptr<LogicalOperator> MakeExistsFilter(const FilterMatching &matching, const SymbolTable &symbol_table,
                                                    AstStorage &storage,
                                                    const std::unordered_set<Symbol> &bound_symbols) {
    std::vector<Symbol> once_symbols(bound_symbols.begin(), bound_symbols.end());
    std::unique_ptr<LogicalOperator> last_op = std::make_unique<Once>(once_symbols);

    std::vector<Symbol> new_symbols;
    std::unordered_set<Symbol> expand_symbols(bound_symbols.begin(), bound_symbols.end());

    auto filters = matching.filters;

    std::unordered_map<Symbol, std::vector<Symbol>> named_paths;

    last_op = HandleExpansions(std::move(last_op), matching, symbol_table, storage, expand_symbols, new_symbols,
                               named_paths, filters, storage::View::OLD);

    last_op = std::make_unique<Limit>(std::move(last_op), storage.Create<PrimitiveLiteral>(1));

    last_op = std::make_unique<EvaluatePatternFilter>(std::move(last_op), matching.symbol.value());

    return last_op;
  }

  std::vector<std::shared_ptr<LogicalOperator>> ExtractPatternFilters(Filters &filters, const SymbolTable &symbol_table,
                                                                      AstStorage &storage,
                                                                      const std::unordered_set<Symbol> &bound_symbols) {
    std::vector<std::shared_ptr<LogicalOperator>> operators;

    for (const auto &filter : filters) {
      for (const auto &matching : filter.matchings) {
        if (!impl::HasBoundFilterSymbols(bound_symbols, filter)) {
          continue;
        }

        switch (matching.type) {
          case PatternFilterType::EXISTS: {
            operators.push_back(MakeExistsFilter(matching, symbol_table, storage, bound_symbols));
            break;
          }
        }
      }
    }

    return operators;
  }

  std::unique_ptr<LogicalOperator> MergeWithCombinator(std::unique_ptr<LogicalOperator> curr_op,
                                                       std::unique_ptr<LogicalOperator> last_op,
                                                       const Tree &combinator) {
    if (const auto *union_ = utils::Downcast<const CypherUnion>(&combinator)) {
      return impl::GenUnion(*union_, std::move(last_op), std::move(curr_op), *context_->symbol_table);
    }

    throw utils::NotYetImplemented("This type of merging queries is not yet implemented!");
  }

  std::unique_ptr<LogicalOperator> MakeDistinct(std::unique_ptr<LogicalOperator> last_op) {
    return std::make_unique<Distinct>(std::move(last_op), last_op->OutputSymbols(*context_->symbol_table));
  }
};

}  // namespace memgraph::query::plan
