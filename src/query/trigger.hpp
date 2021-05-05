#pragma once
#include <concepts>
#include <type_traits>

#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"
#include "utils/concepts.hpp"

namespace query {

namespace trigger {
enum class IdentifierTag : uint8_t {
  CREATED_VERTICES,
  CREATED_EDGES,
  DELETED_VERTICES,
  DELETED_EDGES,
  SET_VERTEX_PROPERTIES,
  SET_EDGE_PROPERTIES,
  REMOVED_VERTEX_PROPERTIES,
  REMOVED_EDGE_PROPERTIES,
  SET_VERTEX_LABELS,
  REMOVED_VERTEX_LABELS,
  UPDATED_VERTICES,
  UPDATED_EDGES,
  UPDATED_OBJECTS
};
}  // namespace trigger

namespace detail {
template <typename T>
concept ObjectAccessor = utils::SameAsAnyOf<T, VertexAccessor, EdgeAccessor>;

template <ObjectAccessor TAccessor>
const char *ObjectString() {
  if constexpr (std::same_as<TAccessor, VertexAccessor>) {
    return "vertex";
  } else {
    return "edge";
  }
}
}  // namespace detail

struct TriggerContext {
  static_assert(std::is_trivially_copy_constructible_v<VertexAccessor>,
                "VertexAccessor is not trivially copy constructible, move it where possible and remove this assert");
  static_assert(std::is_trivially_copy_constructible_v<EdgeAccessor>,
                "EdgeAccessor is not trivially copy constructible, move it where possible and remove this asssert");

  template <detail::ObjectAccessor TAccessor>
  void RegisterCreatedObject(const TAccessor &created_object) {
    GetRegistry<TAccessor>().created_objects_.emplace_back(created_object);
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterDeletedObject(const TAccessor &deleted_object) {
    GetRegistry<TAccessor>().deleted_objects_.emplace_back(deleted_object);
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterSetObjectProperty(const TAccessor &object, const storage::PropertyId key, TypedValue old_value,
                                 TypedValue new_value) {
    if (new_value.IsNull()) {
      RegisterRemovedObjectProperty(object, key, std::move(old_value));
      return;
    }

    GetRegistry<TAccessor>().set_object_properties_.emplace_back(object, key, std::move(old_value),
                                                                 std::move(new_value));
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterRemovedObjectProperty(const TAccessor &object, const storage::PropertyId key, TypedValue old_value) {
    // property is already removed
    if (old_value.IsNull()) {
      return;
    }

    GetRegistry<TAccessor>().removed_object_properties_.emplace_back(object, key, std::move(old_value));
  }

  void RegisterSetVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id);
  void RegisterRemovedVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id);

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each derived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  TypedValue GetTypedValue(trigger::IdentifierTag tag, DbAccessor *dba) const;

  template <detail::ObjectAccessor TAccessor>
  struct CreatedObject {
    explicit CreatedObject(const TAccessor &object) : object{object} {}

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }

    TAccessor object;
  };

  template <detail::ObjectAccessor TAccessor>
  struct DeletedObject {
    explicit DeletedObject(const TAccessor &object) : object{object} {}

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }

    TAccessor object;
  };

  template <detail::ObjectAccessor TAccessor>
  struct SetObjectProperty {
    explicit SetObjectProperty(const TAccessor &object, storage::PropertyId key, TypedValue old_value,
                               TypedValue new_value)
        : object{object}, key{key}, old_value{std::move(old_value)}, new_value{std::move(new_value)} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const {
      return {{detail::ObjectString<TAccessor>(), TypedValue{object}},
              {"key", TypedValue{dba->PropertyToName(key)}},
              {"old", old_value},
              {"new", new_value}};
    }

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }

    TAccessor object;
    storage::PropertyId key;
    TypedValue old_value;
    TypedValue new_value;
  };

  template <detail::ObjectAccessor TAccessor>
  struct RemovedObjectProperty {
    explicit RemovedObjectProperty(const TAccessor &object, storage::PropertyId key, TypedValue old_value)
        : object{object}, key{key}, old_value{std::move(old_value)} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const {
      return {{detail::ObjectString<TAccessor>(), TypedValue{object}},
              {"key", TypedValue{dba->PropertyToName(key)}},
              {"old", old_value}};
    }

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }

    TAccessor object;
    storage::PropertyId key;
    TypedValue old_value;
  };

  struct SetVertexLabel {
    explicit SetVertexLabel(const VertexAccessor &vertex, const storage::LabelId label_id)
        : object{vertex}, label_id{label_id} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    bool IsValid() const;

    VertexAccessor object;
    storage::LabelId label_id;
  };

  struct RemovedVertexLabel {
    explicit RemovedVertexLabel(const VertexAccessor &vertex, const storage::LabelId label_id)
        : object{vertex}, label_id{label_id} {}

    std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const;
    bool IsValid() const;

    VertexAccessor object;
    storage::LabelId label_id;
  };

 private:
  template <detail::ObjectAccessor TAccessor>
  struct Registry {
    std::vector<CreatedObject<TAccessor>> created_objects_;
    std::vector<DeletedObject<TAccessor>> deleted_objects_;
    std::vector<SetObjectProperty<TAccessor>> set_object_properties_;
    std::vector<RemovedObjectProperty<TAccessor>> removed_object_properties_;
  };

  Registry<VertexAccessor> vertex_registry_;
  Registry<EdgeAccessor> edge_registry_;

  template <detail::ObjectAccessor TAccessor>
  Registry<TAccessor> &GetRegistry() {
    if constexpr (std::same_as<TAccessor, VertexAccessor>) {
      return vertex_registry_;
    } else {
      return edge_registry_;
    }
  }

  std::vector<SetVertexLabel> set_vertex_labels_;
  std::vector<RemovedVertexLabel> removed_vertex_labels_;
};

struct Trigger {
  explicit Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
                   DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, double tsc_frequency,
               double max_execution_time_sec, std::atomic<bool> *is_shutting_down, const TriggerContext &context) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const noexcept { return name_; }

 private:
  struct TriggerPlan {
    using IdentifierInfo = std::pair<Identifier, trigger::IdentifierTag>;

    explicit TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers);

    CachedPlan cached_plan;
    std::vector<IdentifierInfo> identifiers;
  };
  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  mutable utils::SpinLock plan_lock_;
  mutable std::shared_ptr<TriggerPlan> trigger_plan_;
};
}  // namespace query
