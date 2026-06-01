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

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "dbms/database_protector.hpp"
#include "kvstore/kvstore.hpp"
#include "memory/db_arena_fwd.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/trigger_context.hpp"
#include "trigger_privilege_context.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::query {

struct QueryCacheEntry;

enum class TransactionStatus;

struct Trigger {
  explicit Trigger(std::string name, const std::string &query, const UserParameters &user_parameters,
                   TriggerEventType event_type, utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                   const InterpreterConfig::Query &query_config, std::shared_ptr<QueryUserOrRole> creator,
                   std::string_view db_name,
                   TriggerPrivilegeContext privilege_context = TriggerPrivilegeContext::DEFINER,
                   parameters::Parameters const *server_parameters = nullptr);

  void Execute(DbAccessor *dba, memgraph::dbms::DatabaseAccess db_acc, utils::MemoryResource *execution_memory,
               double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
               std::atomic<TransactionStatus> *transaction_status, const TriggerContext &context, bool is_main,
               std::shared_ptr<QueryUserOrRole> triggering_user, const AuthChecker *auth_checker) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }

  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }

  bool operator==(const std::string &other) const { return name_ == other; }

  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &Name() const noexcept { return name_; }

  const auto &OriginalStatement() const noexcept { return parsed_statements_.query_string; }

  const auto &Creator() const noexcept { return creator_; }

  auto EventType() const noexcept { return event_type_; }

  auto PrivilegeContext() const noexcept { return privilege_context_; }

  // Sentinel for a trigger that has never fired; rendered as null in SHOW TRIGGERS.
  static constexpr int64_t kNeverExecuted = std::numeric_limits<int64_t>::min();

  int64_t LastExecuted() const noexcept { return std::atomic_ref{last_executed_}.load(std::memory_order_relaxed); }

  uint64_t FailureCount() const noexcept { return std::atomic_ref{failure_count_}.load(std::memory_order_relaxed); }

  std::optional<std::string> LastError() const {
    auto guard = std::shared_lock{health_lock_};
    if (last_error_.empty()) return std::nullopt;
    return last_error_;
  }

 private:
  void ExecuteImpl(DbAccessor *dba, memgraph::dbms::DatabaseAccess db_acc, utils::MemoryResource *execution_memory,
                   double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                   std::atomic<TransactionStatus> *transaction_status, const TriggerContext &context, bool is_main,
                   std::shared_ptr<QueryUserOrRole> triggering_user, const AuthChecker *auth_checker) const;

  void RecordExecution() const;
  void RecordFailure(std::string error) const;

  struct TriggerPlan {
    using IdentifierInfo = std::pair<Identifier, TriggerIdentifierTag>;

    explicit TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers);

    PlanWrapper cached_plan;
    std::vector<IdentifierInfo> identifiers;
  };

  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor, std::string_view db_name,
                                       std::shared_ptr<QueryUserOrRole> triggering_user) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  TriggerEventType event_type_;

  mutable utils::RWSpinLock plan_lock_;
  mutable std::shared_ptr<TriggerPlan> trigger_plan_;
  std::shared_ptr<QueryUserOrRole> creator_;
  TriggerPrivilegeContext privilege_context_{TriggerPrivilegeContext::DEFINER};

  // In-memory, non-durable health state. Numeric fields are accessed via atomic_ref (plain storage keeps Trigger
  // movable into the skip list); last_error_ is guarded by health_lock_ and materialized lazily in GetTriggerInfo.
  mutable int64_t last_executed_{kNeverExecuted};
  mutable uint64_t failure_count_{0};
  mutable utils::RWSpinLock health_lock_;
  mutable std::string last_error_;
};

enum class TriggerPhase : uint8_t { BEFORE_COMMIT, AFTER_COMMIT };

struct TriggerStore {
  explicit TriggerStore(std::filesystem::path directory);

  void RestoreTriggers(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                       const InterpreterConfig::Query &query_config, const query::AuthChecker *auth_checker,
                       std::string_view db_name, parameters::Parameters const *server_parameters);

  void AddTrigger(std::string name, const std::string &query, const UserParameters &user_parameters,
                  TriggerEventType event_type, TriggerPhase phase, utils::SkipList<QueryCacheEntry> *query_cache,
                  DbAccessor *db_accessor, const InterpreterConfig::Query &query_config,
                  std::shared_ptr<QueryUserOrRole> creator, std::string_view db_name,
                  TriggerPrivilegeContext privilege_context, parameters::Parameters const *server_parameters,
                  bool if_not_exists = false);

  void DropTrigger(const std::string &name, bool if_exists = false);
  void DropAll();

  struct TriggerInfo {
    std::string name;
    std::string statement;
    TriggerEventType event_type;
    TriggerPhase phase;
    std::optional<std::string> owner;
    TriggerPrivilegeContext privilege_context;
    std::optional<int64_t> last_executed;
    uint64_t failure_count;
    std::optional<std::string> last_error;
  };

  std::vector<TriggerInfo> GetTriggerInfo() const;

  const auto &BeforeCommitTriggers() const noexcept { return before_commit_triggers_; }

  const auto &AfterCommitTriggers() const noexcept { return after_commit_triggers_; }

  bool HasTriggers() const noexcept { return before_commit_triggers_.size() > 0 || after_commit_triggers_.size() > 0; }

  std::unordered_set<TriggerEventType> GetEventTypes() const;

 private:
  void RestoreTrigger(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                      const InterpreterConfig::Query &query_config, const query::AuthChecker *auth_checker,
                      std::string_view trigger_name, std::string_view trigger_data, std::string_view db_name,
                      parameters::Parameters const *server_parameters);

  utils::SpinLock store_lock_;
  kvstore::KVStore storage_;

  utils::SkipListDb<Trigger> before_commit_triggers_;
  utils::SkipListDb<Trigger> after_commit_triggers_;
};

}  // namespace memgraph::query
