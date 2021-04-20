#pragma once

#include "query/cypher_query_interpreter.hpp"
#include "query/frontend/ast/ast.hpp"

namespace query {
struct Trigger {
  explicit Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *cache,
                   utils::SpinLock *antlr_lock);

  void Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
               utils::MonotonicBufferResource *execution_memory, double tsc_frequency, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const { return name_; }

 private:
  std::string name_;
  ParsedQuery parsed_statements_;
};
}  // namespace query
