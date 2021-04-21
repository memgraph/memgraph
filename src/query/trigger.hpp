#pragma once

#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"

namespace query {

struct Trigger {
  explicit Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *query_cache,
                   utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
               utils::MonotonicBufferResource *execution_memory, double tsc_frequency, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down, std::unordered_map<std::string, TypedValue> context) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const { return name_; }

 private:
  std::shared_ptr<CachedPlan> GetPlan(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  // predefined identifiers
  mutable std::vector<Identifier> identifiers_;
};

struct TriggerContext {
  void RegisterCreatedVertex(VertexAccessor created_vertex);

  // Get each variable that can be used in a query that a triggers
  // executes using the TypedValue type
  std::unordered_map<std::string, TypedValue> GetTypedValues();

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each dirived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

 private:
  std::vector<VertexAccessor> created_vertices_;
};

}  // namespace query
