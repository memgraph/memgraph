#pragma once
#include <algorithm>
#include <concepts>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

#include "query/cypher_query_interpreter.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"
#include "utils/concepts.hpp"
#include "utils/fnv.hpp"

namespace query {
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

template <ObjectAccessor TAccessor>
struct CreatedObject {
  explicit CreatedObject(const TAccessor &object) : object{object} {}

  bool IsValid() const { return object.IsVisible(storage::View::OLD); }
  std::map<std::string, TypedValue> ToMap([[maybe_unused]] DbAccessor *dba) const {
    return {{ObjectString<TAccessor>(), TypedValue{object}}};
  }

  TAccessor object;
};

template <ObjectAccessor TAccessor>
struct DeletedObject {
  explicit DeletedObject(const TAccessor &object) : object{object} {}

  bool IsValid() const { return object.IsVisible(storage::View::OLD); }
  std::map<std::string, TypedValue> ToMap([[maybe_unused]] DbAccessor *dba) const {
    return {{ObjectString<TAccessor>(), TypedValue{object}}};
  }

  TAccessor object;
};

template <ObjectAccessor TAccessor>
struct SetObjectProperty {
  explicit SetObjectProperty(const TAccessor &object, storage::PropertyId key, TypedValue old_value,
                             TypedValue new_value)
      : object{object}, key{key}, old_value{std::move(old_value)}, new_value{std::move(new_value)} {}

  std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const {
    return {{ObjectString<TAccessor>(), TypedValue{object}},
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

template <ObjectAccessor TAccessor>
struct RemovedObjectProperty {
  explicit RemovedObjectProperty(const TAccessor &object, storage::PropertyId key, TypedValue old_value)
      : object{object}, key{key}, old_value{std::move(old_value)} {}

  std::map<std::string, TypedValue> ToMap(DbAccessor *dba) const {
    return {{ObjectString<TAccessor>(), TypedValue{object}},
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
}  // namespace detail

enum class TriggerIdentifierTag : uint8_t {
  CREATED_VERTICES,
  CREATED_EDGES,
  CREATED_OBJECTS,
  DELETED_VERTICES,
  DELETED_EDGES,
  DELETED_OBJECTS,
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

enum class TriggerEventType : uint8_t {
  ANY,  // Triggers always
  VERTEX_CREATE,
  EDGE_CREATE,
  CREATE,
  VERTEX_DELETE,
  EDGE_DELETE,
  DELETE,
  VERTEX_UPDATE,
  EDGE_UPDATE,
  UPDATE
};

static_assert(std::is_trivially_copy_constructible_v<VertexAccessor>,
              "VertexAccessor is not trivially copy constructible, move it where possible and remove this assert");
static_assert(std::is_trivially_copy_constructible_v<EdgeAccessor>,
              "EdgeAccessor is not trivially copy constructible, move it where possible and remove this asssert");

// Holds the information necessary for triggers
class TriggerContext {
 public:
  TriggerContext() = default;
  TriggerContext(std::vector<detail::CreatedObject<VertexAccessor>> created_vertices,
                 std::vector<detail::DeletedObject<VertexAccessor>> deleted_vertices,
                 std::vector<detail::SetObjectProperty<VertexAccessor>> set_vertex_properties,
                 std::vector<detail::RemovedObjectProperty<VertexAccessor>> removed_vertex_properties,
                 std::vector<detail::SetVertexLabel> set_vertex_labels,
                 std::vector<detail::RemovedVertexLabel> removed_vertex_labels,
                 std::vector<detail::CreatedObject<EdgeAccessor>> created_edges,
                 std::vector<detail::DeletedObject<EdgeAccessor>> deleted_edges,
                 std::vector<detail::SetObjectProperty<EdgeAccessor>> set_edge_properties,
                 std::vector<detail::RemovedObjectProperty<EdgeAccessor>> removed_edge_properties)
      : created_vertices_{std::move(created_vertices)},
        deleted_vertices_{std::move(deleted_vertices)},
        set_vertex_properties_{std::move(set_vertex_properties)},
        removed_vertex_properties_{std::move(removed_vertex_properties)},
        set_vertex_labels_{std::move(set_vertex_labels)},
        removed_vertex_labels_{std::move(removed_vertex_labels)},
        created_edges_{std::move(created_edges)},
        deleted_edges_{std::move(deleted_edges)},
        set_edge_properties_{std::move(set_edge_properties)},
        removed_edge_properties_{std::move(removed_edge_properties)} {}
  TriggerContext(const TriggerContext &) = default;
  TriggerContext(TriggerContext &&) = default;
  TriggerContext &operator=(const TriggerContext &) = default;
  TriggerContext &operator=(TriggerContext &&) = default;

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each derived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  // Get TypedValue for the identifier defined with tag
  TypedValue GetTypedValue(TriggerIdentifierTag tag, DbAccessor *dba) const;
  bool ShouldEventTrigger(TriggerEventType) const;

 private:
  std::vector<detail::CreatedObject<VertexAccessor>> created_vertices_;
  std::vector<detail::DeletedObject<VertexAccessor>> deleted_vertices_;
  std::vector<detail::SetObjectProperty<VertexAccessor>> set_vertex_properties_;
  std::vector<detail::RemovedObjectProperty<VertexAccessor>> removed_vertex_properties_;
  std::vector<detail::SetVertexLabel> set_vertex_labels_;
  std::vector<detail::RemovedVertexLabel> removed_vertex_labels_;

  std::vector<detail::CreatedObject<EdgeAccessor>> created_edges_;
  std::vector<detail::DeletedObject<EdgeAccessor>> deleted_edges_;
  std::vector<detail::SetObjectProperty<EdgeAccessor>> set_edge_properties_;
  std::vector<detail::RemovedObjectProperty<EdgeAccessor>> removed_edge_properties_;
};

// Collects the information necessary for triggers during a single transaction run.
class TriggerContextCollector {
 public:
  template <detail::ObjectAccessor TAccessor>
  void RegisterCreatedObject(const TAccessor &created_object) {
    GetRegistry<TAccessor>().created_objects_.emplace(created_object.Gid(), detail::CreatedObject{created_object});
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterDeletedObject(const TAccessor &deleted_object) {
    auto &registry = GetRegistry<TAccessor>();
    if (registry.created_objects_.count(deleted_object.Gid())) {
      return;
    }

    registry.deleted_objects_.emplace_back(deleted_object);
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterSetObjectProperty(const TAccessor &object, const storage::PropertyId key, TypedValue old_value,
                                 TypedValue new_value) {
    auto &registry = GetRegistry<TAccessor>();
    if (registry.created_objects_.count(object.Gid())) {
      return;
    }

    if (auto it = registry.property_changes_.find({object, key}); it != registry.property_changes_.end()) {
      it->second.new_value = std::move(new_value);
      return;
    }

    registry.property_changes_.emplace(std::make_pair(object, key),
                                       PropertyChangeInfo{std::move(old_value), std::move(new_value)});
  }

  template <detail::ObjectAccessor TAccessor>
  void RegisterRemovedObjectProperty(const TAccessor &object, const storage::PropertyId key, TypedValue old_value) {
    // property is already removed
    if (old_value.IsNull()) {
      return;
    }

    RegisterSetObjectProperty(object, key, std::move(old_value), TypedValue());
  }

  void RegisterSetVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id);
  void RegisterRemovedVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id);
  [[nodiscard]] TriggerContext TransformToTriggerContext() &&;

 private:
  struct HashPair {
    template <detail::ObjectAccessor TAccessor, typename T2>
    size_t operator()(const std::pair<TAccessor, T2> &pair) const {
      using GidType = decltype(std::declval<TAccessor>().Gid());
      return utils::HashCombine<GidType, T2>{}(pair.first.Gid(), pair.second);
    }
  };

  struct PropertyChangeInfo {
    TypedValue old_value;
    TypedValue new_value;
  };

  template <detail::ObjectAccessor TAccessor>
  using PropertyChangesMap =
      std::unordered_map<std::pair<TAccessor, storage::PropertyId>, PropertyChangeInfo, HashPair>;

  template <detail::ObjectAccessor TAccessor>
  using PropertyChangesLists = std::pair<std::vector<detail::SetObjectProperty<TAccessor>>,
                                         std::vector<detail::RemovedObjectProperty<TAccessor>>>;

  template <detail::ObjectAccessor TAccessor>
  struct Registry {
    using ChangesSummary =
        std::tuple<std::vector<detail::CreatedObject<TAccessor>>, std::vector<detail::DeletedObject<TAccessor>>,
                   std::vector<detail::SetObjectProperty<TAccessor>>,
                   std::vector<detail::RemovedObjectProperty<TAccessor>>>;

    [[nodiscard]] static PropertyChangesLists<TAccessor> PropertyMapToList(PropertyChangesMap<TAccessor> &&map) {
      std::vector<detail::SetObjectProperty<TAccessor>> set_object_properties;
      std::vector<detail::RemovedObjectProperty<TAccessor>> removed_object_properties;

      for (auto it = map.begin(); it != map.end(); it = map.erase(it)) {
        const auto &[key, property_change_info] = *it;
        if (property_change_info.old_value.IsNull() && property_change_info.new_value.IsNull()) {
          // no change happened on the transaction level
          continue;
        }

        if (const auto is_equal = property_change_info.old_value == property_change_info.new_value;
            is_equal.IsBool() && is_equal.ValueBool()) {
          // no change happened on the transaction level
          continue;
        }

        if (property_change_info.new_value.IsNull()) {
          removed_object_properties.emplace_back(key.first, key.second /* property_id */,
                                                 std::move(property_change_info.old_value));
        } else {
          set_object_properties.emplace_back(key.first, key.second, std::move(property_change_info.old_value),
                                             std::move(property_change_info.new_value));
        }
      }

      return PropertyChangesLists<TAccessor>{std::move(set_object_properties), std::move(removed_object_properties)};
    }

    [[nodiscard]] ChangesSummary Summarize() && {
      auto [set_object_properties, removed_object_properties] = PropertyMapToList(std::move(property_changes_));
      std::vector<detail::CreatedObject<TAccessor>> created_objects_vec;
      created_objects_vec.reserve(created_objects_.size());
      std::transform(created_objects_.begin(), created_objects_.end(), std::back_inserter(created_objects_vec),
                     [](const auto &gid_and_created_object) { return gid_and_created_object.second; });
      created_objects_.clear();

      return {std::move(created_objects_vec), std::move(deleted_objects_), std::move(set_object_properties),
              std::move(removed_object_properties)};
    }

    std::unordered_map<storage::Gid, detail::CreatedObject<TAccessor>> created_objects_;
    std::vector<detail::DeletedObject<TAccessor>> deleted_objects_;
    // During the transaction, a single property on a single object could be changed multiple times.
    // We want to register only the global change, at the end of the transaction. The change consists of
    // the value before the transaction start, and the latest value assigned throughout the transaction.
    PropertyChangesMap<TAccessor> property_changes_;
  };

  template <detail::ObjectAccessor TAccessor>
  Registry<TAccessor> &GetRegistry() {
    if constexpr (std::same_as<TAccessor, VertexAccessor>) {
      return vertex_registry_;
    } else {
      return edge_registry_;
    }
  }

  using LabelChangesMap = std::unordered_map<std::pair<VertexAccessor, storage::LabelId>, int8_t, HashPair>;
  using LabelChangesLists = std::pair<std::vector<detail::SetVertexLabel>, std::vector<detail::RemovedVertexLabel>>;

  enum class LabelChange : int8_t { REMOVE = -1, ADD = 1 };

  static int8_t LabelChangeToInt(LabelChange change);

  [[nodiscard]] static LabelChangesLists LabelMapToList(LabelChangesMap &&label_changes);

  void UpdateLabelMap(VertexAccessor vertex, storage::LabelId label_id, LabelChange change);

  Registry<VertexAccessor> vertex_registry_;
  Registry<EdgeAccessor> edge_registry_;
  // During the transaction, a single label on a single vertex could be added and removed multiple times.
  // We want to register only the global change, at the end of the transaction. The change consists of
  // the state of the label before the transaction start, and the latest state assigned throughout the transaction.
  LabelChangesMap label_changes_;
};

struct Trigger {
  explicit Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
                   DbAccessor *db_accessor, utils::SpinLock *antlr_lock,
                   TriggerEventType event_type = TriggerEventType::ANY);

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
}  // namespace query
