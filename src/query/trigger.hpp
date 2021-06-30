#pragma once

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "query/config.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/trigger_context.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"

namespace query {
struct Trigger {
  explicit Trigger(std::string name, const std::string &query,
                   const std::map<std::string, storage::PropertyValue> &user_parameters, TriggerEventType event_type,
                   utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock,
                   const InterpreterConfig::Query &query_config);

  void Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down, const TriggerContext &context) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &Name() const noexcept { return name_; }
  const auto &OriginalStatement() const noexcept { return parsed_statements_.query_string; }
  auto EventType() const noexcept { return event_type_; }

 private:
  struct TriggerPlan {
    using IdentifierInfo = std::pair<Identifier, TriggerIdentifierTag>;

    explicit TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers);

    CachedPlan cached_plan;
    std::vector<IdentifierInfo> identifiers;
  };
  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  TriggerEventType event_type_;

  mutable utils::SpinLock plan_lock_;
  mutable std::shared_ptr<TriggerPlan> trigger_plan_;
};

enum class TriggerPhase : uint8_t { BEFORE_COMMIT, AFTER_COMMIT };

struct TriggerStore {
  explicit TriggerStore(std::filesystem::path directory);

  void RestoreTriggers(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                       utils::SpinLock *antlr_lock, const InterpreterConfig::Query &query_config);

  void AddTrigger(const std::string &name, const std::string &query,
                  const std::map<std::string, storage::PropertyValue> &user_parameters, TriggerEventType event_type,
                  TriggerPhase phase, utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                  utils::SpinLock *antlr_lock, const InterpreterConfig::Query &query_config);

  void DropTrigger(const std::string &name);

  struct TriggerInfo {
    std::string name;
    std::string statement;
    TriggerEventType event_type;
    TriggerPhase phase;
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

}  // namespace query
