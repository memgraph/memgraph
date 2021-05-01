#pragma once
#include "query/cypher_query_interpreter.hpp"
#include "query/frontend/ast/ast.hpp"

namespace query {

namespace trigger {
enum class IdentifierTag : uint8_t { CREATED_VERTICES, DELETED_VERTICES };
}  // namespace trigger

struct TriggerContext {
  void RegisterCreatedVertex(VertexAccessor created_vertex);
  void RegisterDeletedVertex(VertexAccessor deleted_vertex);

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each derived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  TypedValue GetTypedValue(trigger::IdentifierTag tag) const;

 private:
  std::vector<VertexAccessor> created_vertices_;
  std::vector<VertexAccessor> deleted_vertices_;
};

struct Trigger {
  explicit Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
                   DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, double tsc_frequency,
               double max_execution_time_sec, std::atomic<bool> *is_shutting_down, const TriggerContext &context);

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const noexcept { return name_; }

 private:
  struct TriggerPlan {
    std::optional<CachedPlan> cached_plan;
    std::vector<std::pair<Identifier, trigger::IdentifierTag>> identifiers;
  };
  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor);

  std::string name_;
  ParsedQuery parsed_statements_;

  utils::SpinLock plan_lock_;
  std::shared_ptr<TriggerPlan> trigger_plan_;
};
}  // namespace query
