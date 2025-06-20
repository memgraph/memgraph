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

#pragma once

#include "plan/read_write_type_checker.hpp"
#include "query/config.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/stripped.hpp"
#include "query/parameters.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/lru_cache.hpp"
#include "utils/synchronized.hpp"

#include "gflags/gflags.h"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(query_cost_planner);
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_int32(query_plan_cache_max_size);

namespace memgraph::query {

namespace plan {
class LogicalOperator;
}

class SymbolTable;
class Query;

// TODO: Maybe this should move to query/plan/planner.
/// Interface for accessing the root operator of a logical plan.
class LogicalPlan {
 public:
  explicit LogicalPlan() = default;

  virtual ~LogicalPlan() = default;

  LogicalPlan(const LogicalPlan &) = default;
  LogicalPlan &operator=(const LogicalPlan &) = default;
  LogicalPlan(LogicalPlan &&) = default;
  LogicalPlan &operator=(LogicalPlan &&) = default;

  virtual const plan::LogicalOperator &GetRoot() const = 0;
  virtual double GetCost() const = 0;
  virtual const SymbolTable &GetSymbolTable() const = 0;
  virtual const AstStorage &GetAstStorage() const = 0;
  virtual plan::ReadWriteTypeChecker::RWType RWType() const = 0;
};

using UserParameters = storage::ExternalPropertyValue::map_t;

auto PrepareQueryParameters(frontend::StrippedQuery const &stripped_query, UserParameters const &user_parameters)
    -> Parameters;

class PlanWrapper {
 public:
  explicit PlanWrapper(std::unique_ptr<LogicalPlan> plan);

  auto plan() const -> plan::LogicalOperator const & { return plan_->GetRoot(); }
  double cost() const { return plan_->GetCost(); }
  const auto &symbol_table() const { return plan_->GetSymbolTable(); }
  const auto &ast_storage() const { return plan_->GetAstStorage(); }
  auto rw_type() const { return plan_->RWType(); }

 private:
  std::unique_ptr<LogicalPlan> plan_;
};

struct CachedPlanWrapper : PlanWrapper {
  explicit CachedPlanWrapper(std::unique_ptr<LogicalPlan> plan, std::string stripped_query);
  auto stripped_query() const -> std::string_view { return stripped_query_; }

 private:
  std::string stripped_query_;  // used so we can identify hash collision
};

struct CachedQuery {
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
  bool is_cypher_read{false};
};

struct QueryCacheEntry {
  bool operator==(const QueryCacheEntry &other) const { return first == other.first; }
  bool operator<(const QueryCacheEntry &other) const { return first < other.first; }
  bool operator==(const uint64_t &other) const { return first == other; }
  bool operator<(const uint64_t &other) const { return first < other; }

  uint64_t first;
  // TODO: Maybe store the query string here and use it as a key with the hash
  // so that we eliminate the risk of hash collisions.
  CachedQuery second;
};

/**
 * A container for data related to the parsing of a query.
 */
struct ParsedQuery {
  std::string query_string;
  frontend::StrippedQuery stripped_query;
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
  bool is_cypher_read{false};
  bool is_cacheable{true};
  UserParameters user_parameters;
  Parameters parameters;
};

ParsedQuery ParseQuery(const std::string &query_string, UserParameters const &user_parameters,
                       utils::SkipList<QueryCacheEntry> *cache, const InterpreterConfig::Query &query_config);

class SingleNodeLogicalPlan final : public LogicalPlan {
 public:
  SingleNodeLogicalPlan(std::unique_ptr<plan::LogicalOperator> root, double cost, AstStorage storage,
                        SymbolTable symbol_table, plan::ReadWriteTypeChecker::RWType rw_type);

  const plan::LogicalOperator &GetRoot() const override { return *root_; }
  double GetCost() const override { return cost_; }
  const SymbolTable &GetSymbolTable() const override;
  const AstStorage &GetAstStorage() const override { return storage_; }
  plan::ReadWriteTypeChecker::RWType RWType() const override { return rw_type_; }

 private:
  std::unique_ptr<plan::LogicalOperator> root_;
  double cost_;
  AstStorage storage_;
  SymbolTable symbol_table_;
  plan::ReadWriteTypeChecker::RWType rw_type_;
};

using PlanCacheLRU = utils::Synchronized<utils::LRUCache<uint64_t, std::shared_ptr<query::CachedPlanWrapper>>,
                                         utils::RWSpinLock>;  // TODO: check RW

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
std::shared_ptr<PlanWrapper> CypherQueryToPlan(frontend::StrippedQuery const &stripped_query, AstStorage ast_storage,
                                               CypherQuery *query, const Parameters &parameters,
                                               PlanCacheLRU *plan_cache, DbAccessor *db_accessor,
                                               const std::vector<Identifier *> &predefined_identifiers = {});

}  // namespace memgraph::query
