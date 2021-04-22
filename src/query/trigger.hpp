#pragma once

#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"

namespace query {

namespace trigger {
enum class IdentifierTag : uint8_t { CREATED_VERTICES, DELETED_VERTICES, SET_VERTEX_PROPERTIES, UPDATED_VERTICES };
}  // namespace trigger

struct TriggerContext {
  void RegisterCreatedVertex(VertexAccessor created_vertex);
  void RegisterDeletedVertex(VertexAccessor deleted_vertex);
  void RegisterSetVertexProperty(VertexAccessor vertex, storage::PropertyId key, TypedValue old_value,
                                 TypedValue new_value);

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each dirived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  TypedValue GetTypedValue(trigger::IdentifierTag tag, DbAccessor *dba) const;

  struct SetVertexProperty {
    explicit SetVertexProperty(VertexAccessor vertex, storage::PropertyId key, TypedValue old_value,
                               TypedValue new_value)
        : vertex{vertex}, key{key}, old_value{std::move(old_value)}, new_value{std::move(new_value)} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    VertexAccessor vertex;
    storage::PropertyId key;
    TypedValue old_value;
    TypedValue new_value;
  };

 private:
  std::vector<VertexAccessor> created_vertices_;
  std::vector<VertexAccessor> deleted_vertices_;
  std::vector<SetVertexProperty> set_vertex_properties_;
};

struct Trigger {
  explicit Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *query_cache,
                   utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
               utils::MonotonicBufferResource *execution_memory, double tsc_frequency, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down, const TriggerContext &context) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const noexcept { return name_; }

 private:
  std::shared_ptr<CachedPlan> GetPlan(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  mutable std::vector<std::pair<Identifier, trigger::IdentifierTag>> identifiers_;
};
}  // namespace query
