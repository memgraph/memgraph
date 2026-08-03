// Copyright 2026 Memgraph Ltd.
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
#include <ranges>
#include <variant>

#include "flags/run_time_configurable.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/general.hpp"
#include "query/plan/rewrite/range.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::plan {

struct PatternComprehensionData {
  PatternComprehensionData() = default;

  PatternComprehensionData(std::shared_ptr<LogicalOperator> lop, Symbol res_symbol,
                           std::unordered_set<Symbol> exp_syms = {})
      : op(std::move(lop)), result_symbol(res_symbol), expansion_symbols(std::move(exp_syms)) {}

  std::shared_ptr<LogicalOperator> op;
  Symbol result_symbol;
  /// Symbols from the PC pattern (nodes, edges, path).
  /// Used to detect if the PC references outer symbols that won't exist after Aggregate.
  std::unordered_set<Symbol> expansion_symbols;
};

/// Interface for planning correlated-subquery branches, avoiding std::function overhead.
struct SubqueryBranchPlanner {
  virtual ~SubqueryBranchPlanner() = default;
  /// @param extra_bound_symbols Symbols bound by an operator the branch will be planned after, but which are
  /// not yet in the planning context (the output symbols of a WITH/RETURN whose WHERE or ORDER BY holds the
  /// subquery). Has no default: a default argument on a virtual function is bound statically, so it would mean
  /// something different depending on whether the call goes through this interface or through RuleBasedPlanner.
  virtual std::unique_ptr<LogicalOperator> Plan(const PatternComprehensionMatching &matching, storage::View view,
                                                const std::unordered_set<Symbol> &extra_bound_symbols) = 0;
  /// Builds an EXISTS branch for a forced bool fold, i.e. without the deferred fold's
  /// `Limit(1) -> EvaluatePatternFilter` tail.
  virtual std::unique_ptr<LogicalOperator> PlanExistsBranch(const FilterMatching &matching, storage::View view,
                                                            const std::unordered_set<Symbol> &extra_bound_symbols) = 0;
};

/// Passed as @c SubqueryBranchPlanner's @c extra_bound_symbols when the branch is planned in the
/// position its own clause binds, so the planning context's bound symbols are already complete.
inline const std::unordered_set<Symbol> kNoExtraBoundSymbols{};

/// ExpandVariable requires View::OLD, so a branch whose pattern has a variable-length edge must use it.
/// Planning one with View::NEW kills the server.
inline bool HasVariableLengthExpansion(const Matching &matching) {
  return std::ranges::any_of(matching.expansions,
                             [](const auto &expansion) { return expansion.edge && expansion.edge->IsVariable(); });
}

/// The one view rule for a correlated-subquery branch, shared by every splice point. A clause sees the effects of the
/// clauses before it, and within one command there is no AdvanceCommand to fold a write into View::OLD, so a
/// branch planned after a write reads View::NEW - except where ExpandVariable cannot service it.
inline storage::View SubqueryView(const Matching &matching, bool write_occurred) {
  if (HasVariableLengthExpansion(matching)) return storage::View::OLD;
  return write_occurred ? storage::View::NEW : storage::View::OLD;
}

/// Context for on-demand planning of correlated subqueries in RETURN/WITH bodies.
struct SubqueryContext {
  std::unordered_map<Symbol, PatternComprehensionMatching> &pending_comprehensions;
  /// The EXISTS matchings of this query part that a WITH/RETURN body may evaluate, keyed by result symbol.
  const std::unordered_map<Symbol, FilterMatching> &pending_exists;
  SubqueryBranchPlanner *planner;
  /// Whether a write clause has already been planned in this query part; feeds @c SubqueryView.
  bool write_occurred;
};

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
  bool in_exists_subquery{false};
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
                                           const std::unordered_set<Symbol> &bound_symbols, AstStorage &storage,
                                           SubqueryContext &pc_ctx, Expression *commit_frequency,
                                           bool in_exists_subquery);

std::unique_ptr<LogicalOperator> GenWith(With &with, std::unique_ptr<LogicalOperator> input_op,
                                         SymbolTable &symbol_table, bool is_write,
                                         std::unordered_set<Symbol> &bound_symbols, AstStorage &storage,
                                         SubqueryContext &pc_ctx, Expression *commit_frequency,
                                         bool in_exists_subquery);

std::unique_ptr<LogicalOperator> GenUnion(const CypherUnion &cypher_union, std::shared_ptr<LogicalOperator> left_op,
                                          std::shared_ptr<LogicalOperator> right_op, SymbolTable &symbol_table);

template <class TBoolOperator>
Expression *BoolJoin(AstStorage &storage, Expression *expr1, Expression *expr2) {
  if (expr1 && expr2) {
    return storage.Create<TBoolOperator>(expr1, expr2);
  }
  return expr1 ? expr1 : expr2;
}

/// Result symbols of the top-level pattern comprehensions @p clauses evaluate. Used to splice a comprehension into
/// the operator branch that actually reads it, rather than the chain its clause happens to sit on.
std::unordered_set<Symbol> CollectPatternComprehensionSymbols(const std::vector<Clause *> &clauses,
                                                              const SymbolTable &symbol_table);

}  // namespace impl

/// @brief Planner which uses hardcoded rules to produce operators.
///
/// @sa MakeLogicalPlan
template <class TPlanningContext>
class RuleBasedPlanner : public SubqueryBranchPlanner {
 public:
  explicit RuleBasedPlanner(TPlanningContext *context) : context_(context) {}

  /// Implements SubqueryBranchPlanner interface
  std::unique_ptr<LogicalOperator> Plan(const PatternComprehensionMatching &matching, storage::View view,
                                        const std::unordered_set<Symbol> &extra_bound_symbols) override {
    if (extra_bound_symbols.empty()) {
      return PlanPatternComprehension(matching, *context_->symbol_table, context_->bound_symbols, view);
    }
    auto bound_symbols = context_->bound_symbols;
    bound_symbols.insert_range(extra_bound_symbols);
    return PlanPatternComprehension(matching, *context_->symbol_table, bound_symbols, view);
  }

  /// Implements SubqueryBranchPlanner interface
  std::unique_ptr<LogicalOperator> PlanExistsBranch(const FilterMatching &matching, storage::View view,
                                                    const std::unordered_set<Symbol> &extra_bound_symbols) override {
    if (extra_bound_symbols.empty()) {
      return MakeExistsBranch(matching, *context_->symbol_table, *context_->ast_storage, context_->bound_symbols, view);
    }
    auto bound_symbols = context_->bound_symbols;
    bound_symbols.insert_range(extra_bound_symbols);
    return MakeExistsBranch(matching, *context_->symbol_table, *context_->ast_storage, bound_symbols, view);
  }

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
    bool const has_periodic_commit = query_parts.commit_frequency != nullptr;
    bool const is_root_query = !query_parts.is_subquery;

    // in CALL with scoped variables, we immediately have some bound variables to work with
    auto const initial_bound_symbols = context.bound_symbols;
    for (const auto &query_part : query_parts.query_parts) {
      context.bound_symbols = initial_bound_symbols;
      std::unique_ptr<LogicalOperator> input_op;

      context.is_write_query = false;
      for (const auto &single_query_part : query_part.single_query_parts) {
        input_op = HandleMatching(std::move(input_op), single_query_part, *context.symbol_table, context.bound_symbols);

        uint64_t merge_id = 0;
        uint64_t subquery_id = 0;

        // Pattern comprehensions are planned on-demand when visited in RETURN/WITH expressions,
        // or before write clauses for comprehensions not in any expression.
        std::unordered_map<Symbol, PatternComprehensionMatching> pending_comprehensions;
        for (const auto &pc : single_query_part.pattern_comprehension_matchings) {
          pending_comprehensions.emplace(pc.result_symbol, pc);
        }
        // EXISTS is planned only where a splice point exists for it, i.e. from a WITH/RETURN body. Semantic analysis
        // refuses every other position, so an entry left here belongs to a clause with no drain (a MATCH WHERE keeps
        // its own on the FilterInfo and never reaches this map).
        std::unordered_map<Symbol, FilterMatching> pending_exists;
        for (const auto &exists : single_query_part.exists_matchings) {
          pending_exists.emplace(exists.symbol.value(), exists);
        }

        // Compute all symbols that will be bound by this query part (from MATCH, CREATE, MERGE, etc.)
        // This is used to determine which comprehension symbols are external references vs. internal.
        std::unordered_set<Symbol> symbols_bound_by_query_part;
        // Add symbols from MATCH
        symbols_bound_by_query_part.insert(single_query_part.matching.expansion_symbols.begin(),
                                           single_query_part.matching.expansion_symbols.end());
        // Add symbols from optional matches
        for (const auto &opt_matching : single_query_part.optional_matching) {
          symbols_bound_by_query_part.insert(opt_matching.expansion_symbols.begin(),
                                             opt_matching.expansion_symbols.end());
        }
        // Add symbols from merge matchings
        for (const auto &merge_matching : single_query_part.merge_matching) {
          symbols_bound_by_query_part.insert(merge_matching.expansion_symbols.begin(),
                                             merge_matching.expansion_symbols.end());
        }
        auto collect_return_body_symbols = [&](const ReturnBody &body) {
          for (const auto *named_expr : body.named_expressions) {
            symbols_bound_by_query_part.insert(context.symbol_table->at(*named_expr));
          }
        };
        // Add symbols from CREATE, FOREACH and WITH/RETURN clauses
        std::function<void(Clause *)> collect_clause_symbols = [&](Clause *clause) {
          if (auto *create = utils::Downcast<Create>(clause)) {
            for (const auto *pattern : create->patterns_) {
              for (const PatternAtom *atom : pattern->atoms_) {
                symbols_bound_by_query_part.insert(context.symbol_table->at(*atom->identifier_));
              }
            }
          } else if (auto *foreach_clause = utils::Downcast<query::Foreach>(clause)) {
            // Add the FOREACH iteration variable
            symbols_bound_by_query_part.insert(context.symbol_table->at(*foreach_clause->named_expression_));
            // Recursively collect symbols from nested clauses
            for (auto *nested : foreach_clause->clauses_) {
              collect_clause_symbols(nested);
            }
          } else if (auto *ret = utils::Downcast<Return>(clause)) {
            // A WITH/RETURN re-declares its projected names. A comprehension in its WHERE or ORDER BY resolves to
            // those new symbols, so it must not be drained before the clause that binds them - see deps_satisfied.
            collect_return_body_symbols(ret->body_);
          } else if (auto *with = utils::Downcast<query::With>(clause)) {
            collect_return_body_symbols(with->body_);
          }
        };
        for (const auto &clause : single_query_part.remaining_clauses) {
          collect_clause_symbols(clause);
        }

        // Track whether a write operation has occurred - comprehensions planned after writes
        // need to use View::NEW to see the newly created/modified data.
        bool write_occurred = false;

        // Plan and apply the satisfiable comprehensions this clause originates, before the clause itself.
        auto plan_and_apply_comprehensions = [&](const std::unordered_set<Symbol> *only) {
          input_op = SpliceSatisfiedComprehensions(std::move(input_op),
                                                   pending_comprehensions,
                                                   symbols_bound_by_query_part,
                                                   context.bound_symbols,
                                                   write_occurred,
                                                   only);
        };

        for (const auto &clause : single_query_part.remaining_clauses) {
          MG_ASSERT(!utils::IsSubtype(*clause, Match::kType), "Unexpected Match in remaining clauses");

          SubqueryContext pc_ctx{pending_comprehensions, pending_exists, this, write_occurred};

          if (auto *ret = utils::Downcast<Return>(clause)) {
            input_op = impl::GenReturn(*ret,
                                       std::move(input_op),
                                       *context.symbol_table,
                                       context.is_write_query,
                                       context.bound_symbols,
                                       *context.ast_storage,
                                       pc_ctx,
                                       query_parts.commit_frequency,
                                       context.in_exists_subquery);
          } else if (auto *merge = utils::Downcast<query::Merge>(clause)) {
            // The collector descends into ON CREATE / ON MATCH, so those comprehensions also originate here. Subtract
            // them: GenMerge splices each into the branch that reads it, and the main chain must not take them first.
            auto only = OriginatingIn(clause, pending_comprehensions);
            auto branch_clauses = merge->on_create_;
            branch_clauses.insert(branch_clauses.end(), merge->on_match_.begin(), merge->on_match_.end());
            for (const auto &sym : impl::CollectPatternComprehensionSymbols(branch_clauses, *context.symbol_table)) {
              only.erase(sym);
            }
            plan_and_apply_comprehensions(&only);
            input_op = GenMerge(*merge,
                                std::move(input_op),
                                single_query_part.merge_matching[merge_id++],
                                pending_comprehensions,
                                symbols_bound_by_query_part);
            // Treat MERGE clause as write, because we do not know if it will create anything.
            context.is_write_query = true;
            write_occurred = true;
          } else if (auto *with = utils::Downcast<query::With>(clause)) {
            input_op = impl::GenWith(*with,
                                     std::move(input_op),
                                     *context.symbol_table,
                                     context.is_write_query,
                                     context.bound_symbols,
                                     *context.ast_storage,
                                     pc_ctx,
                                     nullptr,
                                     context.in_exists_subquery);
            // WITH clause advances the command, so reset the flag.
            context.is_write_query = false;
          } else if (IsWriteClause(clause)) {
            context.is_write_query = true;
            write_occurred = true;
            const auto only = OriginatingIn(clause, pending_comprehensions);
            plan_and_apply_comprehensions(&only);
            auto op = HandleWriteClause(clause, input_op, *context.symbol_table, context.bound_symbols);
            MG_ASSERT(op, "Expected write clause to be handled");
            input_op = std::move(op);
          } else if (auto *unwind = utils::Downcast<query::Unwind>(clause)) {
            const auto &symbol = context.symbol_table->at(*unwind->named_expression_);
            context.bound_symbols.insert(symbol);
            const auto only = OriginatingIn(clause, pending_comprehensions);
            plan_and_apply_comprehensions(&only);
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
            // A CallProcedure's arguments and its YIELD ... WHERE can both hold a comprehension, and it is a query
            // part boundary, so nothing downstream can drain it. Splice below the operator: the frame slot written
            // per input row survives the procedure's rows and is what the Filter above reads.
            const auto only = OriginatingIn(clause, pending_comprehensions);
            plan_and_apply_comprehensions(&only);
            // TODO: When we add support for write and eager procedures, we will
            // need to plan this operator with Accumulate and pass in
            // storage::View::NEW.
            input_op = std::make_unique<plan::CallProcedure>(std::move(input_op),
                                                             call_proc->procedure_name_,
                                                             call_proc->arguments_,
                                                             call_proc->result_fields_,
                                                             result_symbols,
                                                             call_proc->memory_limit_,
                                                             call_proc->memory_scale_,
                                                             call_proc->is_write_,
                                                             procedure_id++,
                                                             call_proc->void_procedure_);
            if (call_proc->where_) {
              auto *filter_expr = call_proc->where_->expression_;
              Filters where_filters;
              where_filters.CollectFilterExpression(filter_expr, *context.symbol_table);
              input_op = std::make_unique<Filter>(std::move(input_op),
                                                  std::vector<std::shared_ptr<LogicalOperator>>{},
                                                  filter_expr,
                                                  std::move(where_filters));
            }
          } else if (auto *load_csv = utils::Downcast<query::LoadCsv>(clause)) {
            const auto &row_sym = context.symbol_table->at(*load_csv->row_var_);
            context.bound_symbols.insert(row_sym);
            input_op = std::make_unique<plan::LoadCsv>(std::move(input_op),
                                                       load_csv->file_,
                                                       load_csv->configs_,
                                                       load_csv->with_header_,
                                                       load_csv->ignore_bad_,
                                                       load_csv->delimiter_,
                                                       load_csv->quote_,
                                                       load_csv->nullif_,
                                                       row_sym);
          } else if (auto *load_parquet = utils::Downcast<query::LoadParquet>(clause)) {
            const auto &row_sym = context.symbol_table->at(*load_parquet->row_var_);
            context.bound_symbols.insert(row_sym);
            input_op = std::make_unique<plan::LoadParquet>(
                std::move(input_op), load_parquet->file_, load_parquet->configs_, row_sym);
          } else if (auto *load_jsonl = utils::Downcast<query::LoadJsonl>(clause)) {
            const auto &row_sym = context.symbol_table->at(*load_jsonl->row_var_);
            context.bound_symbols.insert(row_sym);
            input_op = std::make_unique<plan::LoadJsonl>(
                std::move(input_op), load_jsonl->file_, load_jsonl->configs_, row_sym);
          } else if (auto *foreach = utils::Downcast<query::Foreach>(clause)) {
            context.is_write_query = true;
            write_occurred = true;
            // Body comprehensions originate at this clause too, so the same set gates both the main chain and the
            // body chain; whichever binds their symbols first takes them.
            const auto only = OriginatingIn(clause, pending_comprehensions);
            plan_and_apply_comprehensions(&only);
            input_op = HandleForeachClause(foreach,
                                           std::move(input_op),
                                           *context.symbol_table,
                                           context.bound_symbols,
                                           single_query_part,
                                           merge_id,
                                           pending_comprehensions,
                                           symbols_bound_by_query_part,
                                           only);
          } else if (auto *call_sub = utils::Downcast<query::CallSubquery>(clause)) {
            auto scoped_variables = std::invoke([&]() -> std::optional<std::unordered_set<Symbol>> {
              if (!call_sub->has_variable_scope_) {
                return std::nullopt;
              }
              if (call_sub->all_variables_scoped_) {
                // `CALL (*) { ... }`: carry every user-declared outer symbol.
                return context.bound_symbols |
                       std::views::filter([](const Symbol &sym) { return sym.user_declared(); }) |
                       std::ranges::to<std::unordered_set<Symbol>>();
              }
              return call_sub->scoped_variables_ | std::views::transform([&](query::NamedExpression *ne) {
                       auto *ident = utils::Downcast<query::Identifier>(ne->expression_);
                       return context.symbol_table->at(*ident);
                     }) |
                     std::ranges::to<std::unordered_set<Symbol>>();
            });
            input_op = HandleSubquery(std::move(input_op),
                                      single_query_part.subqueries[subquery_id++],
                                      *context.symbol_table,
                                      *context_->ast_storage,
                                      pending_comprehensions,
                                      write_occurred,
                                      call_sub->cypher_query_->pre_query_directives_.commit_frequency_,
                                      scoped_variables);
            if (context.is_write_query && !has_periodic_commit) {
              input_op = std::make_unique<Accumulate>(
                  std::move(input_op), input_op->ModifiedSymbols(*context.symbol_table), is_root_query);
            }
          } else {
            throw utils::NotYetImplemented("clause '{}' conversion to operator(s)", clause->GetTypeInfo().name);
          }
        }
      }

      // Is this the only situation that should be covered
      if (input_op->OutputSymbols(*context.symbol_table).empty() && !context.in_exists_subquery) {
        if (has_periodic_commit && is_root_query) {
          // this periodic commit is from USING PERIODIC COMMIT
          input_op = std::make_unique<PeriodicCommit>(std::move(input_op), query_parts.commit_frequency);
        }
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
  /// @brief Recursively plans a pattern comprehension including any nested pattern comprehensions.
  /// For nested pattern comprehensions (e.g., [()--() | [()--() | 1]]), the inner pattern
  /// comprehension is planned first and wrapped with RollUpApply before the outer one's Produce.
  /// @param view The storage view to use - View::NEW if planned after write clauses, View::OLD otherwise.
  std::unique_ptr<LogicalOperator> PlanPatternComprehension(const PatternComprehensionMatching &matching,
                                                            const SymbolTable &symbol_table,
                                                            std::unordered_set<Symbol> &bound_symbols,
                                                            storage::View view = storage::View::OLD) {
    std::unique_ptr<LogicalOperator> new_input;
    // Create a copy of bound_symbols and add external symbols from the pattern comprehension.
    // External symbols are references to variables from outer scope (e.g., FOREACH variable `x`
    // in `[(a)-[r]->(b) WHERE a.id = x | b]`). These must be in bound_symbols for filter extraction
    // to work correctly.
    auto pc_bound_symbols = bound_symbols;
    pc_bound_symbols.insert(matching.external_symbols.begin(), matching.external_symbols.end());

    MatchContext match_ctx{matching, symbol_table, pc_bound_symbols, view};
    new_input = PlanMatching(match_ctx, std::move(new_input));
    new_input = ApplyNestedPatternComprehensions(
        std::move(new_input), matching.nested_pattern_comprehensions, symbol_table, pc_bound_symbols, view);
    new_input = std::make_unique<Produce>(std::move(new_input), std::vector{matching.result_expr});
    return new_input;
  }

  TPlanningContext *context_;

  storage::LabelId GetLabel(const LabelIx &label) { return context_->db->NameToLabel(label.name); }

  storage::PropertyId GetProperty(const PropertyIx &prop) { return context_->db->NameToProperty(prop.name); }

  std::vector<storage::PropertyId> GetProperties(const std::vector<PropertyIx> &props) {
    std::vector<storage::PropertyId> property_ids;
    property_ids.reserve(props.size());
    for (const auto &prop : props) {
      property_ids.push_back(context_->db->NameToProperty(prop.name));
    }
    return property_ids;
  }

  storage::EdgeTypeId GetEdgeType(EdgeTypeIx edge_type) { return context_->db->NameToEdgeType(edge_type.name); }

  std::vector<storage::EdgeTypeId> GetEdgeTypes(const std::vector<QueryEdgeType> &edge_types) {
    std::vector<storage::EdgeTypeId> transformed_edge_types;
    transformed_edge_types.reserve(edge_types.size());

    for (const auto &type : edge_types) {
      if (const auto *edge_type_atom = std::get_if<EdgeTypeIx>(&type)) {
        transformed_edge_types.push_back(GetEdgeType(*edge_type_atom));
      } else {
        throw QueryException(
            "Failed to work with dynamic edge types! Avoid using parameters or variables when creating a new edge. "
            "Also, please contact Memgraph support or submit a GitHub issue, as this scenario should not happen. This "
            "is an unexpected code "
            "path, but we didn't want to just crash Memgraph, help us improve the query planner!");
      }
    }

    return transformed_edge_types;
  }

  std::vector<StorageLabelType> GetLabelIds(const std::vector<QueryLabelType> &labels) {
    std::vector<StorageLabelType> label_ids;
    label_ids.reserve(labels.size());
    for (const auto &label : labels) {
      if (const auto *label_atom = std::get_if<LabelIx>(&label)) {
        label_ids.emplace_back(GetLabel(*label_atom));
      } else {
        label_ids.emplace_back(std::get<Expression *>(label));
      }
    }
    return label_ids;
  }

  std::vector<StorageEdgeType> GetEdgeIds(const std::vector<QueryEdgeType> &edge_types) {
    std::vector<StorageEdgeType> edge_ids;
    edge_ids.reserve(edge_types.size());
    for (const auto &edge_type : edge_types) {
      if (const auto *edge_type_atom = std::get_if<EdgeTypeIx>(&edge_type)) {
        edge_ids.emplace_back(GetEdgeType(*edge_type_atom));
      } else {
        edge_ids.emplace_back(std::get<Expression *>(edge_type));
      }
    }
    return edge_ids;
  }

  std::unique_ptr<LogicalOperator> HandleMatching(std::unique_ptr<LogicalOperator> last_op,
                                                  const SingleQueryPart &single_query_part,
                                                  const SymbolTable &symbol_table,
                                                  std::unordered_set<Symbol> &bound_symbols) {
    MatchContext match_ctx{single_query_part.matching, symbol_table, bound_symbols};
    last_op = PlanMatching(match_ctx, std::move(last_op));
    for (const auto &matching : single_query_part.optional_matching) {
      // Ensure that we have all the symbols from the original match
      // Propagated to the optional match for dynamic indexing
      // We can use the symbols in the optional match either by expanding
      // From existing nodes, or by filtering based on the existing nodes
      std::unordered_set<Symbol> bound_symbols_from_original_match;
      for (const auto &symbol : matching.expansion_symbols) {
        if (bound_symbols.contains(symbol)) {
          bound_symbols_from_original_match.insert(symbol);
        }
      }
      for (const auto &filter : matching.filters) {
        for (const auto &symbol : filter.used_symbols) {
          if (bound_symbols.contains(symbol)) {
            bound_symbols_from_original_match.insert(symbol);
          }
        }
      }

      MatchContext opt_ctx{matching, symbol_table, bound_symbols};
      auto once_with_symbols = std::make_unique<Once>(
          std::vector<Symbol>(bound_symbols_from_original_match.begin(), bound_symbols_from_original_match.end()));
      if (auto match_op = PlanMatching(opt_ctx, std::move(once_with_symbols))) {
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
      return NodeCreationInfo{node_symbol, GetLabelIds(node.labels_), properties};
    };

    auto base = [&](NodeAtom *node) -> std::unique_ptr<LogicalOperator> {
      if (node->label_expression_) {
        throw SemanticException("Label expression not supported in CREATE and MERGE clauses.");
      }
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
      EdgeCreationInfo edge_info{edge_symbol, properties, GetEdgeIds(edge->edge_types_)[0], edge->direction_};
      return std::make_unique<CreateExpand>(node_info, edge_info, std::move(last_op), input_symbol, node_existing);
    };

    auto last_op = impl::ReducePattern<std::unique_ptr<LogicalOperator>>(pattern, base, collect);

    // If the pattern is named, append the path constructing logical operator.
    if (pattern.identifier_->user_declared_) {
      std::vector<Symbol> path_elements;
      for (const PatternAtom *atom : pattern.atoms_) path_elements.emplace_back(symbol_table.at(*atom->identifier_));
      bound_symbols.insert(symbol_table.at(*pattern.identifier_));
      last_op = std::make_unique<ConstructNamedPath>(
          std::move(last_op), symbol_table.at(*pattern.identifier_), path_elements);
    }

    return last_op;
  }

  // Check if a clause is a write clause that HandleWriteClause can process.
  static bool IsWriteClause(Clause *clause) {
    return utils::Downcast<Create>(clause) || utils::Downcast<query::Delete>(clause) ||
           utils::Downcast<query::SetProperty>(clause) || utils::Downcast<query::SetProperties>(clause) ||
           utils::Downcast<query::SetLabels>(clause) || utils::Downcast<query::RemoveProperty>(clause) ||
           utils::Downcast<query::RemoveLabels>(clause);
  }

  // Apply nested pattern comprehensions as RollUpApply nodes.
  // Used when a pattern comprehension's result expression contains other pattern comprehensions.
  std::unique_ptr<LogicalOperator> ApplyNestedPatternComprehensions(
      std::unique_ptr<LogicalOperator> input_op, const std::vector<PatternComprehensionMatching> &nested_comprehensions,
      const SymbolTable &symbol_table, std::unordered_set<Symbol> &bound_symbols, storage::View view) {
    for (const auto &nested : nested_comprehensions) {
      auto nested_op = PlanPatternComprehension(nested, symbol_table, bound_symbols, view);
      auto nested_symbols = nested_op->ModifiedSymbols(symbol_table);
      input_op = std::make_unique<RollUpApply>(
          std::move(input_op), std::move(nested_op), nested_symbols, nested.result_symbol);
    }
    return input_op;
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
      if (!set->property_lookup_->use_nested_property_update_) {
        return std::make_unique<plan::SetProperty>(std::move(input_op),
                                                   GetProperty(set->property_lookup_->property_),
                                                   set->property_lookup_,
                                                   set->expression_);
      } else {
        return std::make_unique<plan::SetNestedProperty>(std::move(input_op),
                                                         GetProperties(set->property_lookup_->property_path_),
                                                         set->property_lookup_,
                                                         set->expression_);
      }
    } else if (auto *set = utils::Downcast<query::SetProperties>(clause)) {
      auto op = set->update_ ? plan::SetProperties::Op::UPDATE : plan::SetProperties::Op::REPLACE;
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      return std::make_unique<plan::SetProperties>(std::move(input_op), input_symbol, set->expression_, op);
    } else if (auto *set = utils::Downcast<query::SetLabels>(clause)) {
      const auto &input_symbol = symbol_table.at(*set->identifier_);
      return std::make_unique<plan::SetLabels>(std::move(input_op), input_symbol, GetLabelIds(set->labels_));
    } else if (auto *rem = utils::Downcast<query::RemoveProperty>(clause)) {
      if (rem->property_lookup_->property_path_.size() == 1) {
        return std::make_unique<plan::RemoveProperty>(
            std::move(input_op), GetProperty(rem->property_lookup_->property_), rem->property_lookup_);
      } else {
        return std::make_unique<plan::RemoveNestedProperty>(
            std::move(input_op), GetProperties(rem->property_lookup_->property_path_), rem->property_lookup_);
      }
    } else if (auto *rem = utils::Downcast<query::RemoveLabels>(clause)) {
      const auto &input_symbol = symbol_table.at(*rem->identifier_);
      return std::make_unique<plan::RemoveLabels>(std::move(input_op), input_symbol, GetLabelIds(rem->labels_));
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

    last_op = HandleExpansions(std::move(last_op),
                               matching,
                               symbol_table,
                               storage,
                               bound_symbols,
                               match_context.new_symbols,
                               named_paths,
                               filters,
                               match_context.view);

    MG_ASSERT(named_paths.empty(), "Expected to generate all named paths");
    // We bound all named path symbols, so just add them to new_symbols.
    for (const auto &named_path : matching.named_paths) {
      MG_ASSERT(bound_symbols.contains(named_path.first), "Expected generated named path to have bound symbol");
      match_context.new_symbols.emplace_back(named_path.first);
    }
    if (!filters.empty()) {
      throw QueryException(
          "Expected to generate all filters! Please contact Memgraph support as this scenario should not happen and is "
          "very likely a bug in the query engine!");
    }
    return last_op;
  }

  auto GenMerge(query::Merge &merge, std::unique_ptr<LogicalOperator> input_op, const Matching &matching,
                std::unordered_map<Symbol, PatternComprehensionMatching> &pending_comprehensions,
                const std::unordered_set<Symbol> &symbols_bound_by_query_part) {
    // Copy the bound symbol set, because we don't want to use the updated
    // version when generating the create part.
    std::unordered_set<Symbol> bound_symbols_copy(context_->bound_symbols);
    MatchContext match_ctx{matching, *context_->symbol_table, bound_symbols_copy, storage::View::NEW};

    std::vector<Symbol> bound_symbols(context_->bound_symbols.begin(), context_->bound_symbols.end());

    auto once_with_symbols = std::make_unique<Once>(bound_symbols);
    auto on_match = PlanMatching(match_ctx, std::move(once_with_symbols));

    // ON CREATE / ON MATCH run inside their own branch, so a comprehension one of them reads has to be spliced into
    // that branch. Splicing it onto the chain the MERGE itself sits on would evaluate it above the Merge, leaving the
    // frame slot unwritten when the SET below reads it. Only the comprehensions that branch actually evaluates are
    // taken, so the two branches cannot steal each other's.
    auto splice_branch_comprehensions = [&](std::unique_ptr<LogicalOperator> branch,
                                            const std::vector<query::Clause *> &sets,
                                            const std::unordered_set<Symbol> &branch_bound_symbols) {
      if (sets.empty() || pending_comprehensions.empty()) return branch;
      const auto wanted = impl::CollectPatternComprehensionSymbols(sets, *context_->symbol_table);
      return SpliceSatisfiedComprehensions(std::move(branch),
                                           pending_comprehensions,
                                           symbols_bound_by_query_part,
                                           branch_bound_symbols,
                                           /*write_occurred=*/true,
                                           &wanted,
                                           branch_bound_symbols);
    };

    on_match = splice_branch_comprehensions(std::move(on_match), merge.on_match_, bound_symbols_copy);

    once_with_symbols = std::make_unique<Once>(std::move(bound_symbols));
    // Use the original bound_symbols, so we fill it with new symbols.
    auto on_create = GenCreateForPattern(
        *merge.pattern_, std::move(once_with_symbols), *context_->symbol_table, context_->bound_symbols);
    on_create = splice_branch_comprehensions(std::move(on_create), merge.on_create_, context_->bound_symbols);
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
      return HandleExpansionsWithCartesian(
          std::move(last_op), matching, symbol_table, storage, bound_symbols, new_symbols, named_paths, filters, view);
    }

    return HandleExpansionsWithoutCartesian(
        std::move(last_op), matching, symbol_table, storage, bound_symbols, new_symbols, named_paths, filters, view);
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

    // We want to create separate branches of scan operators for each expansion group group of patterns
    // Whenever there are 2 scan branches, they will be joined with a Cartesian operator

    // New symbols from the opposite branch
    // We need to see what are cross new symbols in order to check for edge uniqueness for cross branch of same
    // matching Since one matching needs to comfort to Cyphermorphism
    std::vector<Symbol> cross_branch_new_symbols;
    bool initial_expansion_done = false;
    for (const auto &expansion : matching.expansions) {
      if (visited_expansion_groups.contains(expansion.expansion_group_id)) {
        continue;
      }

      last_op = GenerateExpansionOnAlreadySeenSymbols(std::move(last_op),
                                                      matching,
                                                      visited_expansion_groups,
                                                      symbol_table,
                                                      storage,
                                                      bound_symbols,
                                                      new_symbols,
                                                      named_paths,
                                                      filters,
                                                      view);

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
      // For single expansion groups (e.g., pattern comprehensions inside FOREACH), include external bound_symbols
      // so filters referencing those symbols can be extracted.
      // For multiple expansion groups (Cartesian product), each group should only see its own symbols -
      // filters referencing symbols from other groups become join conditions applied after the Cartesian.
      std::unordered_set<Symbol> new_bound_symbols;
      if (all_expansion_groups.size() == 1) {
        new_bound_symbols = bound_symbols;
      }
      new_bound_symbols.insert(starting_symbols.begin(), starting_symbols.end());
      std::unique_ptr<LogicalOperator> expansion_group = GenerateExpansionGroup(std::move(starting_expansion_operator),
                                                                                matching,
                                                                                symbol_table,
                                                                                storage,
                                                                                new_bound_symbols,
                                                                                new_expansion_group_symbols,
                                                                                named_paths,
                                                                                filters,
                                                                                view,
                                                                                expansion.expansion_group_id);

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
        if (new_symbol.type() == Symbol::Type::EDGE) {
          last_op = EnsureCyphermorphism(std::move(last_op), new_symbol, matching, new_bound_symbols);
        }
      }

      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);

      // we aggregate all the so far new symbols so we can test them in the next iteration against the new
      // expansion group
      if (has_more_expansions) {
        cross_branch_new_symbols.insert(
            cross_branch_new_symbols.end(), new_expansion_group_symbols.begin(), new_expansion_group_symbols.end());
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
      last_op = GenerateOperatorsForExpansion(std::move(last_op),
                                              matching,
                                              expansion,
                                              symbol_table,
                                              storage,
                                              bound_symbols,
                                              new_symbols,
                                              named_paths,
                                              filters,
                                              view);
    }

    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateExpansionOnAlreadySeenSymbols(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching,
      std::set<ExpansionGroupId> &visited_expansion_groups, const SymbolTable symbol_table, AstStorage &storage,
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
          last_op = GenerateExpansionGroup(std::move(last_op),
                                           matching,
                                           symbol_table,
                                           storage,
                                           bound_symbols,
                                           new_symbols,
                                           named_paths,
                                           filters,
                                           view,
                                           expansion.expansion_group_id);
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
      last_op = GenerateOperatorsForExpansion(std::move(last_op),
                                              matching,
                                              expansion,
                                              symbol_table,
                                              storage,
                                              bound_symbols,
                                              new_symbols,
                                              named_paths,
                                              filters,
                                              view);
    }
    return last_op;
  }

  std::unique_ptr<LogicalOperator> GenerateOperatorsForExpansion(
      std::unique_ptr<LogicalOperator> last_op, const Matching &matching, const Expansion &expansion,
      const SymbolTable &symbol_table, AstStorage &storage, std::unordered_set<Symbol> &bound_symbols,
      std::vector<Symbol> &new_symbols, std::unordered_map<Symbol, std::vector<Symbol>> &named_paths, Filters &filters,
      storage::View view) {
    const auto &node1_symbol = symbol_table.at(*expansion.node1->identifier_);
    const bool is_unseen_node = bound_symbols.insert(node1_symbol).second;

    // we can just perform scanning from an edge if it's a simple edge
    // we don't take into consideration path expansion as part of edge scanning
    if (is_unseen_node && expansion.expand_from_edge && expansion.edge->type_ == EdgeAtom::Type::SINGLE) {
      const auto &node2_symbol = symbol_table.at(*expansion.node2->identifier_);
      const auto &edge_symbol = symbol_table.at(*expansion.edge->identifier_);
      auto edge_types = GetEdgeTypes(expansion.edge->edge_types_);

      last_op = std::make_unique<ScanAllByEdge>(
          std::move(last_op), edge_symbol, node1_symbol, node2_symbol, expansion.direction, edge_types, view);

      new_symbols.emplace_back(node1_symbol);
      new_symbols.emplace_back(edge_symbol);
      new_symbols.emplace_back(node2_symbol);

      bound_symbols.insert(edge_symbol);
      bound_symbols.insert(node2_symbol);

      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
      last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);

      return last_op;
    }

    if (is_unseen_node) {
      // We have just bound this symbol, so generate ScanAll which fills it.
      last_op = std::make_unique<ScanAll>(std::move(last_op), node1_symbol, view);
      new_symbols.emplace_back(node1_symbol);

      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
      last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
    } else if (!named_paths.empty()) {
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
      last_op = impl::GenNamedPaths(std::move(last_op), bound_symbols, named_paths);
      last_op = GenFilters(std::move(last_op), bound_symbols, filters, storage, symbol_table);
    }

    if (expansion.edge) {
      last_op = GenExpand(std::move(last_op),
                          expansion,
                          symbol_table,
                          bound_symbols,
                          matching,
                          storage,
                          filters,
                          named_paths,
                          new_symbols,
                          view);
    } else if (!last_op) {
      // If we hit here: already seen node + it's not a path or expansion
      last_op = std::make_unique<Once>(std::vector<Symbol>{node1_symbol});
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

    auto existing_node = bound_symbols.contains(node_symbol);
    const auto &edge_symbol = symbol_table.at(*edge->identifier_);
    MG_ASSERT(!bound_symbols.contains(edge_symbol), "Existing edges are not supported");

    auto edge_types = GetEdgeTypes(edge->edge_types_);
    if (edge->IsVariable()) {
      std::optional<ExpansionLambda> weight_lambda;
      std::optional<Symbol> total_weight;

      if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
        weight_lambda.emplace(ExpansionLambda{.inner_edge_symbol = symbol_table.at(*edge->weight_lambda_.inner_edge),
                                              .inner_node_symbol = symbol_table.at(*edge->weight_lambda_.inner_node),
                                              .expression = edge->weight_lambda_.expression});

        total_weight.emplace(symbol_table.at(*edge->total_weight_));
      }

      if (edge->type_ == EdgeAtom::Type::KSHORTEST && !existing_node) {
        throw SemanticException(
            "KSHORTEST expansion requires matched nodes. Try capturing the pair of nodes using a WITH clause.");
      }

      ExpansionLambda filter_lambda;
      filter_lambda.inner_edge_symbol = symbol_table.at(*edge->filter_lambda_.inner_edge);
      filter_lambda.inner_node_symbol = symbol_table.at(*edge->filter_lambda_.inner_node);
      if (edge->filter_lambda_.accumulated_path) {
        filter_lambda.accumulated_path_symbol = symbol_table.at(*edge->filter_lambda_.accumulated_path);

        if (edge->filter_lambda_.accumulated_weight) {
          filter_lambda.accumulated_weight_symbol = symbol_table.at(*edge->filter_lambda_.accumulated_weight);
        }
      }
      {
        // Bind the inner edge and node symbols so they're available for
        // inline filtering in ExpandVariable.
        bool inner_edge_bound = bound_symbols.insert(filter_lambda.inner_edge_symbol).second;
        bool inner_node_bound = bound_symbols.insert(filter_lambda.inner_node_symbol).second;
        MG_ASSERT(inner_edge_bound && inner_node_bound, "An inner edge and node can't be bound from before");
        if (filter_lambda.accumulated_path_symbol) {
          bool accumulated_path_bound = bound_symbols.insert(*filter_lambda.accumulated_path_symbol).second;
          MG_ASSERT(accumulated_path_bound, "The accumulated path can't be bound from before");

          if (filter_lambda.accumulated_weight_symbol) {
            bool accumulated_weight_bound = bound_symbols.insert(*filter_lambda.accumulated_weight_symbol).second;
            MG_ASSERT(accumulated_weight_bound, "The accumulated weight can't be bound from before");
          }
        }
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
      std::vector<Symbol> inner_symbols = {filter_lambda.inner_edge_symbol, filter_lambda.inner_node_symbol};
      if (filter_lambda.accumulated_path_symbol) {
        inner_symbols.emplace_back(*filter_lambda.accumulated_path_symbol);

        if (filter_lambda.accumulated_weight_symbol) {
          inner_symbols.emplace_back(*filter_lambda.accumulated_weight_symbol);
        }
      }

      auto [rem_begin, rem_end] = std::ranges::remove_if(filters, [&inner_symbols](FilterInfo &fi) {
        return std::ranges::any_of(inner_symbols, [&](auto const &symbol) { return fi.used_symbols.contains(symbol); });
      });
      filters.erase(rem_begin, rem_end);

      // Unbind the temporarily bound inner symbols for filtering.
      bound_symbols.erase(filter_lambda.inner_edge_symbol);
      bound_symbols.erase(filter_lambda.inner_node_symbol);
      if (filter_lambda.accumulated_path_symbol) {
        bound_symbols.erase(*filter_lambda.accumulated_path_symbol);

        if (filter_lambda.accumulated_weight_symbol) {
          bound_symbols.erase(*filter_lambda.accumulated_weight_symbol);
        }
      }

      if (total_weight) {
        bound_symbols.insert(*total_weight);
      }

      // TODO: Pass weight lambda.
      MG_ASSERT(view == storage::View::OLD, "ExpandVariable should only be planned with storage::View::OLD");
      last_op = std::make_unique<ExpandVariable>(std::move(last_op),
                                                 node1_symbol,
                                                 node_symbol,
                                                 edge_symbol,
                                                 edge->type_,
                                                 expansion.direction,
                                                 edge_types,
                                                 expansion.is_flipped,
                                                 edge->lower_bound_,
                                                 edge->upper_bound_,
                                                 existing_node,
                                                 filter_lambda,
                                                 weight_lambda,
                                                 total_weight,
                                                 edge->limit_);
    } else {
      last_op = std::make_unique<Expand>(std::move(last_op),
                                         node1_symbol,
                                         node_symbol,
                                         edge_symbol,
                                         expansion.direction,
                                         edge_types,
                                         existing_node,
                                         view);
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
      if (!edge_symbols.contains(edge_symbol)) {
        continue;
      }
      std::vector<Symbol> other_symbols;
      for (const auto &symbol : edge_symbols) {
        if (symbol == edge_symbol || !bound_symbols.contains(symbol)) {
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

  /// Result symbols of the pending comprehensions @p clause originates, i.e. the ones its own expressions evaluate.
  /// A drain restricted to these cannot take a comprehension belonging to a clause further down the chain, which is
  /// what put the RollUpApply below a same-clause write (#4134) and below Accumulate (the Accumulate freeze).
  static std::unordered_set<Symbol> OriginatingIn(
      const query::Clause *clause,
      const std::unordered_map<Symbol, PatternComprehensionMatching> &pending_comprehensions) {
    std::unordered_set<Symbol> symbols;
    for (const auto &[sym, pc] : pending_comprehensions) {
      if (pc.origin_clause == clause) symbols.insert(sym);
    }
    return symbols;
  }

  /// True once every symbol the comprehension references that this query part binds elsewhere is bound. Symbols from
  /// an earlier query part are not waited for. Every site draining `pending_comprehensions` must use this.
  static bool DepsSatisfied(const PatternComprehensionMatching &pc,
                            const std::unordered_set<Symbol> &symbols_bound_by_query_part,
                            const std::unordered_set<Symbol> &bound_symbols) {
    // Symbols referenced from the filter or result expression.
    const bool external_ok = std::ranges::all_of(pc.external_symbols, [&](const Symbol &s) {
      return !symbols_bound_by_query_part.contains(s) || bound_symbols.contains(s);
    });
    if (!external_ok) return false;

    // Symbols the pattern itself references but that are declared elsewhere in this query part, e.g. `a` in
    // `[(a)-->(x)|...]` when `a` comes from a CREATE or a later WITH. Those must be bound first.
    return std::ranges::all_of(pc.expansion_symbols, [&](const Symbol &sym) {
      return !symbols_bound_by_query_part.contains(sym) || bound_symbols.contains(sym);
    });
  }

  /// The one place a pending comprehension turns into a `RollUpApply`. Every operator chain that can evaluate one
  /// drains through here - the main clause chain, a FOREACH body, each MERGE branch, and a CALL ... YIELD - because a
  /// comprehension must be spliced onto the chain that *reads* it, not merely the one its clause sits on. Both defects
  /// this file has had came from that: a drain site whose dependency check had drifted, and a chain with no drain site
  /// at all.
  ///
  /// @param bound_symbols what is bound on @p chain, used for the dependency check and the view.
  /// @param only when non-null, restricts the drain to these result symbols - a MERGE branch takes only what its own
  ///        SET clauses read, so the two branches cannot steal each other's.
  /// @param extra_bound_symbols symbols bound on @p chain that the planning context does not share, e.g. MERGE's
  ///        ON MATCH, which binds its pattern into a copy. Omitting them plans an uncorrelated scan.
  std::unique_ptr<LogicalOperator> SpliceSatisfiedComprehensions(
      std::unique_ptr<LogicalOperator> chain,
      std::unordered_map<Symbol, PatternComprehensionMatching> &pending_comprehensions,
      const std::unordered_set<Symbol> &symbols_bound_by_query_part, const std::unordered_set<Symbol> &bound_symbols,
      bool write_occurred, const std::unordered_set<Symbol> *only = nullptr,
      const std::unordered_set<Symbol> &extra_bound_symbols = kNoExtraBoundSymbols) {
    for (auto it = pending_comprehensions.begin(); it != pending_comprehensions.end();) {
      const auto &[sym, pc] = *it;
      const bool wanted = only == nullptr || only->contains(sym);
      if (!wanted || !DepsSatisfied(pc, symbols_bound_by_query_part, bound_symbols)) {
        ++it;
        continue;
      }
      auto pc_op = Plan(pc, SubqueryView(pc, write_occurred), extra_bound_symbols);
      auto symbols = pc_op->ModifiedSymbols(*context_->symbol_table);
      chain = std::make_unique<RollUpApply>(std::move(chain), std::move(pc_op), symbols, sym);
      it = pending_comprehensions.erase(it);
    }
    return chain;
  }

  std::unique_ptr<LogicalOperator> HandleForeachClause(
      query::Foreach *foreach, std::unique_ptr<LogicalOperator> input_op, const SymbolTable &symbol_table,
      std::unordered_set<Symbol> &bound_symbols, const SingleQueryPart &query_part, uint64_t &merge_id,
      std::unordered_map<Symbol, PatternComprehensionMatching> &pending_comprehensions,
      const std::unordered_set<Symbol> &symbols_bound_by_query_part, const std::unordered_set<Symbol> &only) {
    const auto &symbol = symbol_table.at(*foreach->named_expression_);
    bound_symbols.insert(symbol);
    std::unique_ptr<LogicalOperator> op = std::make_unique<plan::Once>();

    // Every clause a FOREACH body may hold is a write, so anything planned after the first one must read View::NEW.
    bool write_occurred = false;

    // Plan comprehensions whose dependencies are now satisfied, onto the body's own chain. Restricted to the
    // originating FOREACH clause's own set, so a later clause's comprehension is not dragged into the body.
    auto plan_satisfied_comprehensions = [&]() {
      op = SpliceSatisfiedComprehensions(
          std::move(op), pending_comprehensions, symbols_bound_by_query_part, bound_symbols, write_occurred, &only);
    };

    // Plan any comprehensions whose dependencies are now satisfied (e.g., referencing the FOREACH variable)
    plan_satisfied_comprehensions();

    for (auto *clause : foreach->clauses_) {
      if (auto *nested_for_each = utils::Downcast<query::Foreach>(clause)) {
        op = HandleForeachClause(nested_for_each,
                                 std::move(op),
                                 symbol_table,
                                 bound_symbols,
                                 query_part,
                                 merge_id,
                                 pending_comprehensions,
                                 symbols_bound_by_query_part,
                                 only);
      } else if (auto *merge = utils::Downcast<query::Merge>(clause)) {
        op = GenMerge(*merge,
                      std::move(op),
                      query_part.merge_matching[merge_id++],
                      pending_comprehensions,
                      symbols_bound_by_query_part);
      } else {
        op = HandleWriteClause(clause, op, symbol_table, bound_symbols);
      }
      // A body clause can bind the very symbol a pending comprehension expands from, so drain again after each one -
      // exactly as the main clause loop does. Without this the comprehension stays pending and is never planned.
      write_occurred = true;
      plan_satisfied_comprehensions();
    }
    return std::make_unique<plan::Foreach>(
        std::move(input_op), std::move(op), foreach->named_expression_->expression_, symbol);
  }

  std::unique_ptr<LogicalOperator> HandleSubquery(
      std::unique_ptr<LogicalOperator> last_op, std::shared_ptr<QueryParts> subquery, SymbolTable &symbol_table,
      AstStorage &storage, std::unordered_map<Symbol, PatternComprehensionMatching> & /*pending_comprehensions*/,
      bool /*write_occurred*/, Expression *commit_frequency,
      const std::optional<std::unordered_set<Symbol>> &scoped_variables = std::nullopt) {
    std::unordered_set<Symbol> outer_scope_bound_symbols;
    outer_scope_bound_symbols.insert(std::make_move_iterator(context_->bound_symbols.begin()),
                                     std::make_move_iterator(context_->bound_symbols.end()));

    if (scoped_variables) {
      // `CALL (v1, v2, ...) { ... }`: seed the subquery planner with exactly
      // the imported outer symbols. The legacy leading-WITH scan is bypassed.
      context_->bound_symbols = *scoped_variables;
    } else {
      context_->bound_symbols =
          impl::GetSubqueryBoundSymbols(subquery->query_parts[0].single_query_parts, symbol_table, storage);
    }

    auto subquery_op = Plan(*subquery);
    auto subquery_bound_symbols = subquery_op->OutputSymbols(*context_->symbol_table);

    context_->bound_symbols.clear();
    context_->bound_symbols.insert(std::make_move_iterator(outer_scope_bound_symbols.begin()),
                                   std::make_move_iterator(outer_scope_bound_symbols.end()));

    context_->bound_symbols.insert(std::make_move_iterator(subquery_bound_symbols.begin()),
                                   std::make_move_iterator(subquery_bound_symbols.end()));

    auto subquery_has_return = true;
    if (subquery_op->GetTypeInfo() == EmptyResult::kType) {
      subquery_has_return = false;
    }

    bool has_periodic_commit = commit_frequency != nullptr;
    if (!has_periodic_commit) {
      last_op = std::make_unique<Apply>(std::move(last_op), std::move(subquery_op), subquery_has_return);
    } else {
      // this periodic commit is from CALL IN TRANSACTIONS OF x ROWS
      last_op = std::make_unique<PeriodicSubquery>(
          std::move(last_op), std::move(subquery_op), commit_frequency, subquery_has_return);
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
                                              std::unordered_set<Symbol> &bound_symbols, Filters &filters,
                                              AstStorage &storage, const SymbolTable &symbol_table) {
    auto pattern_filters = ExtractPatternFilters(filters, symbol_table, storage, bound_symbols);
    auto *filter_expr = impl::ExtractFilters(bound_symbols, filters, storage);

    if (filter_expr) {
      filter_expr = CompactFilters(filter_expr, storage);  // Can only compact; not delete the whole expression
                                                           // Could do in the future when we have parse-time constants
      Filters operator_filters;
      operator_filters.CollectFilterExpression(filter_expr, symbol_table);
      last_op = std::make_unique<Filter>(
          std::move(last_op), std::move(pattern_filters), filter_expr, std::move(operator_filters));
    }
    return last_op;
  }

  /// The EXISTS branch, without either fold's tail. Both forms are rooted at an `Once(bound_symbols)`, so the branch
  /// correlates through the shared frame; the subquery form gets there via a recursive planner call.
  std::unique_ptr<LogicalOperator> MakeExistsBranch(const FilterMatching &matching, const SymbolTable &symbol_table,
                                                    AstStorage &storage,
                                                    const std::unordered_set<Symbol> &bound_symbols,
                                                    storage::View view) {
    if (matching.type == PatternFilterType::EXISTS_SUBQUERY) {
      // in_exists_subquery drives three behaviours of the recursive plan: the body's RETURN is dropped, the
      // EmptyResult wrapper is suppressed, and GenWith keeps outer-scope vertex/edge symbols across a body WITH.
      const bool old_context_exists_subquery = context_->in_exists_subquery;
      context_->in_exists_subquery = true;
      // Copy first: bound_symbols may alias context_->bound_symbols, and moving out of it would then empty the very
      // set the branch has to correlate against - which shows up as a spurious ScanAll clobbering the outer row.
      auto branch_bound_symbols = bound_symbols;
      auto outer_scope_bound_symbols = std::move(context_->bound_symbols);
      context_->bound_symbols = std::move(branch_bound_symbols);

      std::unique_ptr<LogicalOperator> last_op = Plan(*matching.subquery);

      context_->in_exists_subquery = old_context_exists_subquery;
      context_->bound_symbols = std::move(outer_scope_bound_symbols);
      return last_op;
    }

    std::vector<Symbol> once_symbols(bound_symbols.begin(), bound_symbols.end());
    std::unique_ptr<LogicalOperator> last_op = std::make_unique<Once>(once_symbols);

    std::vector<Symbol> new_symbols;
    std::unordered_set<Symbol> expand_symbols(bound_symbols.begin(), bound_symbols.end());

    auto filters = matching.filters;

    std::unordered_map<Symbol, std::vector<Symbol>> named_paths;

    return HandleExpansions(
        std::move(last_op), matching, symbol_table, storage, expand_symbols, new_symbols, named_paths, filters, view);
  }

  /// The deferred bool fold: the branch plus the tail that installs a closure into the frame. Only usable as a
  /// `Filter` side branch, which is why a projection uses the forced fold instead.
  std::unique_ptr<LogicalOperator> MakeExistsFilter(const FilterMatching &matching, const SymbolTable &symbol_table,
                                                    AstStorage &storage,
                                                    const std::unordered_set<Symbol> &bound_symbols) {
    auto last_op = MakeExistsBranch(matching, symbol_table, storage, bound_symbols, storage::View::OLD);
    last_op = std::make_unique<Limit>(std::move(last_op), storage.Create<PrimitiveLiteral>(1));
    return std::make_unique<EvaluatePatternFilter>(std::move(last_op), matching.symbol.value());
  }

  std::unique_ptr<LogicalOperator> MakePatternComprehensionFilter(const PatternComprehensionMatching &matching,
                                                                  const SymbolTable &symbol_table, AstStorage &storage,
                                                                  std::unordered_set<Symbol> &bound_symbols) {
    std::vector<Symbol> once_symbols(bound_symbols.begin(), bound_symbols.end());
    std::unique_ptr<LogicalOperator> last_op = std::make_unique<Once>(once_symbols);

    auto filters = matching.filters;
    std::vector<Symbol> new_symbols;
    std::unordered_map<Symbol, std::vector<Symbol>> named_paths;

    last_op = HandleExpansions(std::move(last_op),
                               matching,
                               symbol_table,
                               storage,
                               bound_symbols,
                               new_symbols,
                               named_paths,
                               filters,
                               storage::View::OLD);
    last_op = ApplyNestedPatternComprehensions(
        std::move(last_op), matching.nested_pattern_comprehensions, symbol_table, bound_symbols, storage::View::OLD);
    last_op = std::make_unique<Produce>(std::move(last_op), std::vector{matching.result_expr});
    auto list_collection_symbols = last_op->ModifiedSymbols(symbol_table);
    last_op = std::make_unique<RollUpApply>(
        std::make_unique<Once>(), std::move(last_op), list_collection_symbols, matching.result_symbol, true);

    return last_op;
  }

  std::vector<std::shared_ptr<LogicalOperator>> ExtractPatternFilters(Filters &filters, const SymbolTable &symbol_table,
                                                                      AstStorage &storage,
                                                                      std::unordered_set<Symbol> &bound_symbols) {
    std::vector<std::shared_ptr<LogicalOperator>> operators;

    for (const auto &filter : filters) {
      if (!impl::HasBoundFilterSymbols(bound_symbols, filter)) {
        continue;
      }

      for (const auto &matching : filter.matchings) {
        operators.push_back(MakeExistsFilter(matching, symbol_table, storage, bound_symbols));
      }

      for (const auto &matching : filter.pattern_comprehension_matchings) {
        operators.push_back(MakePatternComprehensionFilter(matching, symbol_table, storage, bound_symbols));
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
