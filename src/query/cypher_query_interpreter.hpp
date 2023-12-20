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

#pragma once

#include <utility>

#include "query/config.hpp"
#include "query/cypher_query_utils.hpp"
#include "query/frontend/stripped.hpp"
#include "query/plan/planner.hpp"
#include "utils/flag_validation.hpp"
#include "utils/lru_cache.hpp"
#include "utils/synchronized.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(query_cost_planner);
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_int32(query_plan_cache_max_size);

namespace memgraph::query {

ParsedQuery ParseQuery(const std::string &query_string, const std::map<std::string, storage::PropertyValue> &params,
                       utils::SkipList<QueryCacheEntry> *cache, const InterpreterConfig::Query &query_config);

class SingleNodeLogicalPlan final : public LogicalPlan {
 public:
  SingleNodeLogicalPlan(std::unique_ptr<plan::LogicalOperator> root, double cost, AstStorage storage,
                        SymbolTable symbol_table)
      : root_(std::move(root)), cost_(cost), storage_(std::move(storage)), symbol_table_(std::move(symbol_table)) {}

  const plan::LogicalOperator &GetRoot() const override { return *root_; }
  double GetCost() const override { return cost_; }
  const SymbolTable &GetSymbolTable() const override { return symbol_table_; }
  const AstStorage &GetAstStorage() const override { return storage_; }

 private:
  std::unique_ptr<plan::LogicalOperator> root_;
  double cost_;
  AstStorage storage_;
  SymbolTable symbol_table_;
};

using PlanCacheLRU =
    utils::Synchronized<utils::LRUCache<uint64_t, std::shared_ptr<query::PlanWrapper>>, utils::RWSpinLock>;

std::unique_ptr<LogicalPlan> MakeLogicalPlan(AstStorage ast_storage, CypherQuery *query, const Parameters &parameters,
                                             DbAccessor *db_accessor,
                                             const std::vector<Identifier *> &predefined_identifiers);

/**
 * Return the parsed *Cypher* query's AST cached logical plan, or create and
 * cache a fresh one if it doesn't yet exist.
 * @param predefined_identifiers optional identifiers you want to inject into a query.
 * If an identifier is not defined in a scope, we check the predefined identifiers.
 * If an identifier is contained there, we inject it at that place and remove it,
 * because a predefined identifier can be used only in one scope.
 */
std::shared_ptr<PlanWrapper> CypherQueryToPlan(uint64_t hash, AstStorage ast_storage, CypherQuery *query,
                                               const Parameters &parameters, PlanCacheLRU *plan_cache,
                                               DbAccessor *db_accessor,
                                               const std::vector<Identifier *> &predefined_identifiers = {});

}  // namespace memgraph::query
