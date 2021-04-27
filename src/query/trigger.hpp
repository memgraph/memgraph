#pragma once

#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"

namespace query {

namespace trigger {
enum class IdentifierTag : uint8_t {
  CREATED_VERTICES,
  DELETED_VERTICES,
  SET_VERTEX_PROPERTIES,
  REMOVED_VERTEX_PROPERTIES,
  SET_VERTEX_LABELS,
  UPDATED_VERTICES
};
}  // namespace trigger

struct TriggerContext {
  static_assert(std::is_trivially_copy_constructible_v<VertexAccessor>,
                "VertexAccessor is not trivially copy constructible, move it where possible and remove this assert");
  static_assert(std::is_trivially_copy_constructible_v<EdgeAccessor>,
                "EdgeAccessor is not trivially copy constructible, move it where possible and remove this asssert");

  void RegisterCreatedVertex(const VertexAccessor &created_vertex);
  void RegisterDeletedVertex(const VertexAccessor &deleted_vertex);
  void RegisterSetVertexProperty(const VertexAccessor &vertex, storage::PropertyId key, TypedValue old_value,
                                 TypedValue new_value);
  void RegisterRemovedVertexProperty(const VertexAccessor &vertex, storage::PropertyId key, TypedValue old_value);
  void RegisterSetVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id);

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each derived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  TypedValue GetTypedValue(trigger::IdentifierTag tag, DbAccessor *dba) const;

  struct CreatedVertex {
    explicit CreatedVertex(const VertexAccessor &vertex) : vertex{vertex} {}

    bool IsValid() const;

    VertexAccessor vertex;
  };

  struct DeletedVertex {
    explicit DeletedVertex(const VertexAccessor &vertex) : vertex{vertex} {}

    bool IsValid() const;

    VertexAccessor vertex;
  };

  struct SetVertexProperty {
    explicit SetVertexProperty(const VertexAccessor &vertex, storage::PropertyId key, TypedValue old_value,
                               TypedValue new_value)
        : vertex{vertex}, key{key}, old_value{std::move(old_value)}, new_value{std::move(new_value)} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    bool IsValid() const;

    VertexAccessor vertex;
    storage::PropertyId key;
    TypedValue old_value;
    TypedValue new_value;
  };

  struct RemovedVertexProperty {
    explicit RemovedVertexProperty(const VertexAccessor &vertex, storage::PropertyId key, TypedValue old_value)
        : vertex{vertex}, key{key}, old_value{std::move(old_value)} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    bool IsValid() const;

    VertexAccessor vertex;
    storage::PropertyId key;
    TypedValue old_value;
  };

  struct SetVertexLabel {
    explicit SetVertexLabel(const VertexAccessor &vertex, const storage::LabelId label_id)
        : vertex{vertex}, label_id{label_id} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    VertexAccessor vertex;
    storage::LabelId label_id;
  };

 private:
  std::vector<CreatedVertex> created_vertices_;
  std::vector<DeletedVertex> deleted_vertices_;
  std::vector<SetVertexProperty> set_vertex_properties_;
  std::vector<RemovedVertexProperty> removed_vertex_properties_;
  std::vector<SetVertexLabel> set_vertex_labels_;
};

struct Trigger {
  explicit Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
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
