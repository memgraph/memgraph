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

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/cypher_query_utils.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/trigger_context.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::query {

enum class TransactionStatus;
struct Trigger {
  explicit Trigger(std::string name, const std::string &query,
                   const std::map<std::string, storage::PropertyValue> &user_parameters, TriggerEventType event_type,
                   utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                   const InterpreterConfig::Query &query_config, std::optional<std::string> owner,
                   const query::AuthChecker *auth_checker);

  void Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down, std::atomic<TransactionStatus> *transaction_status,
               const TriggerContext &context, const AuthChecker *auth_checker) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &Name() const noexcept { return name_; }
  const auto &OriginalStatement() const noexcept { return parsed_statements_.query_string; }
  const auto &Owner() const noexcept { return owner_; }
  auto EventType() const noexcept { return event_type_; }

 private:
  struct TriggerPlan {
    using IdentifierInfo = std::pair<Identifier, TriggerIdentifierTag>;

    explicit TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers);

    PlanWrapper cached_plan;
    std::vector<IdentifierInfo> identifiers;
  };
  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor, const query::AuthChecker *auth_checker) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  TriggerEventType event_type_;

  mutable utils::SpinLock plan_lock_;
  mutable std::shared_ptr<TriggerPlan> trigger_plan_;
  std::optional<std::string> owner_;
};

enum class TriggerPhase : uint8_t { BEFORE_COMMIT, AFTER_COMMIT };

struct TriggerStore {
  explicit TriggerStore(std::filesystem::path directory);

  void RestoreTriggers(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                       const InterpreterConfig::Query &query_config, const query::AuthChecker *auth_checker);

  void AddTrigger(std::string name, const std::string &query,
                  const std::map<std::string, storage::PropertyValue> &user_parameters, TriggerEventType event_type,
                  TriggerPhase phase, utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                  const InterpreterConfig::Query &query_config, std::optional<std::string> owner,
                  const query::AuthChecker *auth_checker);

  void DropTrigger(const std::string &name);

  struct TriggerInfo {
    std::string name;
    std::string statement;
    TriggerEventType event_type;
    TriggerPhase phase;
    std::optional<std::string> owner;
  };

  std::vector<TriggerInfo> GetTriggerInfo() const;

  const auto &BeforeCommitTriggers() const noexcept { return before_commit_triggers_; }
  const auto &AfterCommitTriggers() const noexcept { return after_commit_triggers_; }

  bool HasTriggers() const noexcept { return before_commit_triggers_.size() > 0 || after_commit_triggers_.size() > 0; }
  std::unordered_set<TriggerEventType> GetEventTypes() const;

 private:
  utils::SpinLock store_lock_;
  kvstore::KVStore storage_;

  utils::SkipList<Trigger> before_commit_triggers_;
  utils::SkipList<Trigger> after_commit_triggers_;
};

}  // namespace memgraph::query
