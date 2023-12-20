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

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/frontend/stripped.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query {

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
};

class PlanWrapper {
 public:
  explicit PlanWrapper(std::unique_ptr<LogicalPlan> plan);

  const auto &plan() const { return plan_->GetRoot(); }
  double cost() const { return plan_->GetCost(); }
  const auto &symbol_table() const { return plan_->GetSymbolTable(); }
  const auto &ast_storage() const { return plan_->GetAstStorage(); }

 private:
  std::unique_ptr<LogicalPlan> plan_;
};

struct CachedQuery {
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
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
  std::map<std::string, storage::PropertyValue> user_parameters;
  Parameters parameters;
  frontend::StrippedQuery stripped_query;
  AstStorage ast_storage;
  Query *query;
  std::vector<AuthQuery::Privilege> required_privileges;
  bool is_cacheable{true};
};
}  // namespace memgraph::query
