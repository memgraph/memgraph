// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/cypher_query_interpreter.hpp"
#include "frontend/ast/ast.hpp"
#include "frontend/semantic/required_privileges.hpp"
#include "frontend/semantic/rw_checker.hpp"
#include "frontend/semantic/symbol_generator.hpp"
#include "plan/read_write_type_checker.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/rule_based_planner.hpp"
#include "query/plan/used_index_checker.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(query_cost_planner, true, "Use the cost-estimating query planner.");
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(query_plan_cache_max_size, 1000, "Maximum number of query plans to cache.",
                       FLAG_IN_RANGE(0, std::numeric_limits<int32_t>::max()));

namespace memgraph::query {
PlanWrapper::PlanWrapper(std::unique_ptr<LogicalPlan> plan) : plan_(std::move(plan)) {}

auto PrepareQueryParameters(frontend::StrippedQuery const &stripped_query, UserParameters const &user_parameters)
    -> Parameters {
  // Copy over the parameters that were introduced during stripping.
  Parameters parameters{stripped_query.literals()};
  // Check that all user-specified parameters are provided.
  for (const auto &[param_index, param_key] : stripped_query.parameters()) {
    auto it = user_parameters.find(param_key);

    if (it == user_parameters.end()) {
      throw UnprovidedParameterError("Parameter ${} not provided.", param_key);
    }

    parameters.Add(param_index, it->second);
  }
  return parameters;
}

ParsedQuery ParseQuery(const std::string &query_string, UserParameters const &user_parameters,
                       utils::SkipList<QueryCacheEntry> *cache, const InterpreterConfig::Query &query_config) {
  // Strip the query for caching purposes. The process of stripping a query
  // "normalizes" it by replacing any literals with new parameters. This
  // results in just the *structure* of the query being taken into account for
  // caching.
  frontend::StrippedQuery stripped_query{query_string};

  // get user-specified parameters
  // ATM we don't need to correctly materise actual PropertyValues exepct Strings
  // passing nullptr here means Enums will be returned as NULL, DO NOT USE during pulls
  auto query_parameters = PrepareQueryParameters(stripped_query, user_parameters);

  // Cache the query's AST if it isn't already.
  auto hash = stripped_query.hash();
  auto accessor = cache->access();
  auto it = accessor.find(hash);
  std::unique_ptr<frontend::opencypher::Parser> parser;

  // Return a copy of both the AST storage and the query.
  CachedQuery result;
  bool is_cacheable = true;

  auto get_information_from_cache = [&](const auto &cached_query) {
    result.ast_storage.properties_ = cached_query.ast_storage.properties_;
    result.ast_storage.labels_ = cached_query.ast_storage.labels_;
    result.ast_storage.edge_types_ = cached_query.ast_storage.edge_types_;

    result.query = cached_query.query->Clone(&result.ast_storage);
    result.required_privileges = cached_query.required_privileges;
    result.is_cypher_read = cached_query.is_cypher_read;
  };

  if (it == accessor.end()) {
    try {
      parser = std::make_unique<frontend::opencypher::Parser>(stripped_query.query());
    } catch (const SyntaxException &e) {
      // There is a syntax exception in the stripped query. Re-run the parser
      // on the original query to get an appropriate error messsage.
      parser = std::make_unique<frontend::opencypher::Parser>(query_string);

      // If an exception was not thrown here, the stripper messed something
      // up.
      LOG_FATAL("The stripped query can't be parsed, but the original can.");
    }

    // Convert the ANTLR4 parse tree into an AST.
    AstStorage ast_storage;
    frontend::ParsingContext context{.is_query_cached = true};
    frontend::CypherMainVisitor visitor(context, &ast_storage, &query_parameters);

    visitor.visit(parser->tree());

    if (visitor.GetQueryInfo().has_load_csv && !query_config.allow_load_csv) {
      throw utils::BasicException("Load CSV not allowed on this instance because it was disabled by a config.");
    }

    auto read_check = [&] {
      query::RWChecker rw_checker;
      if (auto *cypher_query = utils::Downcast<CypherQuery>(visitor.query())) cypher_query->Accept(rw_checker);
      if (auto *profile_query = utils::Downcast<ProfileQuery>(visitor.query()))
        profile_query->cypher_query_->Accept(rw_checker);
      return !rw_checker.IsWrite();
    };

    if (visitor.GetQueryInfo().is_cacheable) {
      CachedQuery cached_query{std::move(ast_storage), visitor.query(), query::GetRequiredPrivileges(visitor.query()),
                               read_check()};
      it = accessor.insert({hash, std::move(cached_query)}).first;

      get_information_from_cache(it->second);
    } else {
      // Carefully use the query we just built, preserving the ast_storage we used to build it
      result.required_privileges = query::GetRequiredPrivileges(visitor.query());
      result.query = visitor.query();
      result.ast_storage = std::move(ast_storage);

      result.is_cypher_read = read_check();
      is_cacheable = false;
    }
  } else {
    get_information_from_cache(it->second);
  }

  return ParsedQuery{
      query_string,
      std::move(stripped_query),
      std::move(result.ast_storage),
      result.query,
      std::move(result.required_privileges),
      result.is_cypher_read,
      is_cacheable,
      user_parameters,
      std::move(query_parameters),
  };
}

std::unique_ptr<LogicalPlan> MakeLogicalPlan(AstStorage ast_storage, CypherQuery *query, const Parameters &parameters,
                                             DbAccessor *db_accessor,
                                             const std::vector<Identifier *> &predefined_identifiers) {
  auto vertex_counts = plan::VertexCountCache(db_accessor);
  auto symbol_table = MakeSymbolTable(query, predefined_identifiers);
  auto planning_context = plan::MakePlanningContext(&ast_storage, &symbol_table, query, &vertex_counts);
  auto [root, cost] = plan::MakeLogicalPlan(&planning_context, parameters, FLAGS_query_cost_planner);
  auto rw_type_checker = plan::ReadWriteTypeChecker();
  rw_type_checker.InferRWType(*root);
  return std::make_unique<SingleNodeLogicalPlan>(std::move(root), cost, std::move(ast_storage), std::move(symbol_table),
                                                 rw_type_checker.type);
}

std::shared_ptr<PlanWrapper> CypherQueryToPlan(frontend::StrippedQuery const &stripped_query, AstStorage ast_storage,
                                               CypherQuery *query, const Parameters &parameters,
                                               PlanCacheLRU *plan_cache, DbAccessor *db_accessor,
                                               const std::vector<Identifier *> &predefined_identifiers) {
  if (plan_cache) {
    auto existing_plan =
        plan_cache->WithLock([&](utils::LRUCache<uint64_t, std::shared_ptr<query::CachedPlanWrapper>> &cache) {
          return cache.get(stripped_query.hash());
        });
    if (existing_plan.has_value() && existing_plan.value()->stripped_query() == stripped_query.query()) {
      // validate the index usage
      auto &ptr = existing_plan.value();
      auto &plan = ptr->plan();

      auto checker = plan::UsedIndexChecker{};
      // G_Lloyd: I am so SORRY, const_cast is BAD, but I'm not fixing Visitable and HierarchicalLogicalOperatorVisitor
      //          ATM to work with a const visitor. This maybe addressed when the planner is redone.
      const_cast<plan::LogicalOperator &>(plan).Accept(checker);

      // TODO: when we are not eagerly collecting all indexes at CreateTransaction we want to Gather rather than Check
      auto all_satisfied = db_accessor->CheckIndicesAreReady(checker.required_indices_);
      if (all_satisfied) {
        return ptr;
      } else {
        plan_cache->WithLock([&](utils::LRUCache<uint64_t, std::shared_ptr<query::CachedPlanWrapper>> &cache) {
          cache.invalidate(stripped_query.hash());
        });
      }
    }
  }

  auto plan = std::make_shared<CachedPlanWrapper>(
      MakeLogicalPlan(std::move(ast_storage), query, parameters, db_accessor, predefined_identifiers),
      stripped_query.query());

  if (plan_cache) {
    plan_cache->WithLock([&](auto &cache) { cache.put(stripped_query.hash(), plan); });
  }

  return plan;
}

SingleNodeLogicalPlan::SingleNodeLogicalPlan(std::unique_ptr<plan::LogicalOperator> root, double cost,
                                             AstStorage storage, SymbolTable symbol_table,
                                             plan::ReadWriteTypeChecker::RWType rw_type)
    : root_(std::move(root)),
      cost_(cost),
      storage_(std::move(storage)),
      symbol_table_(std::move(symbol_table)),
      rw_type_{rw_type} {}

const SymbolTable &SingleNodeLogicalPlan::GetSymbolTable() const { return symbol_table_; }

CachedPlanWrapper::CachedPlanWrapper(std::unique_ptr<LogicalPlan> plan, std::string stripped_query)
    : PlanWrapper(std::move(plan)), stripped_query_(std::move(stripped_query)) {}
}  // namespace memgraph::query
