#pragma once
#include <concepts>
#include <type_traits>

#include "kvstore/kvstore.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/concepts.hpp"
#include "utils/exceptions.hpp"

namespace query {

namespace trigger {
enum class IdentifierTag : uint8_t {
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

enum class EventType : uint8_t {
  ANY = 1,
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

const char *EventTypeToString(EventType event_type);
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

// Collects and holds the information necessary for triggers during a single transaction run
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
    auto &property_changes = GetRegistry<TAccessor>().property_changes_;

    auto property_changes_map = std::get_if<PropertyChangesMap<TAccessor>>(&property_changes);
    MG_ASSERT(property_changes_map, "Invalid state of trigger context");

    if (auto it = property_changes_map->find({object, key}); it != property_changes_map->end()) {
      it->second.new_value = std::move(new_value);
      return;
    }

    property_changes_map->emplace(std::make_pair(object, key),
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

  // Adapt the TriggerContext object inplace for a different DbAccessor
  // (each derived accessor, e.g. VertexAccessor, gets adapted
  // to the sent DbAccessor so they can be used safely)
  void AdaptForAccessor(DbAccessor *accessor);

  // Get TypedValue for the identifier defined with tag
  TypedValue GetTypedValue(trigger::IdentifierTag tag, DbAccessor *dba) const;
  bool ShouldEvenTrigger(trigger::EventType) const;

  template <detail::ObjectAccessor TAccessor>
  struct CreatedObject {
    explicit CreatedObject(const TAccessor &object) : object{object} {}

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }
    std::map<std::string, TypedValue> ToMap([[maybe_unused]] DbAccessor *dba) const {
      return {{"object", TypedValue{object}}};
    }

    TAccessor object;
  };

  template <detail::ObjectAccessor TAccessor>
  struct DeletedObject {
    explicit DeletedObject(const TAccessor &object) : object{object} {}

    bool IsValid() const { return object.IsVisible(storage::View::OLD); }
    std::map<std::string, TypedValue> ToMap([[maybe_unused]] DbAccessor *dba) const {
      return {{"object", TypedValue{object}}};
    }

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
  struct PropertyChangeInfo {
    TypedValue old_value;
    TypedValue new_value;
  };

  struct HashPair {
    template <detail::ObjectAccessor TAccessor, typename T2>
    size_t operator()(const std::pair<TAccessor, T2> &pair) const {
      using GidType = decltype(std::declval<TAccessor>().Gid());
      return std::hash<GidType>{}(pair.first.Gid()) ^ std::hash<T2>{}(pair.second);
    }
  };

  template <detail::ObjectAccessor TAccessor>
  using PropertyChangesMap =
      std::unordered_map<std::pair<TAccessor, storage::PropertyId>, PropertyChangeInfo, HashPair>;

  template <detail::ObjectAccessor TAccessor>
  using PropertyChangesList =
      std::pair<std::vector<SetObjectProperty<TAccessor>>, std::vector<RemovedObjectProperty<TAccessor>>>;

  template <detail::ObjectAccessor TAccessor>
  struct Registry {
    std::vector<CreatedObject<TAccessor>> created_objects_;
    std::vector<DeletedObject<TAccessor>> deleted_objects_;

    // We split the map into property changes lists (if it's not already split).
    // Each list contains specific type of change.
    // This method should be called only after the transaction finished running (during Commit or Rollback).
    void PropertyMapToList() {
      auto *map = std::get_if<PropertyChangesMap<TAccessor>>(&property_changes_);
      if (!map) {
        return;
      }

      std::vector<SetObjectProperty<TAccessor>> set_object_properties;
      std::vector<RemovedObjectProperty<TAccessor>> removed_object_properties;

      for (auto it = map->begin(); it != map->end(); it = map->erase(it)) {
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
      property_changes_ =
          PropertyChangesList<TAccessor>{std::move(set_object_properties), std::move(removed_object_properties)};
    }

    // While the transaction is being run, collect the information inside a map.
    // During the transaction, a single property on a single object could be changed multiple times.
    // We want to register only the global change, at the end of the transaction. The change consists of
    // the value before the transaction start, and the latest value assigned throughout the transaction.
    std::variant<PropertyChangesMap<TAccessor>, PropertyChangesList<TAccessor>> property_changes_;
  };

  // we don't need locks because a single trigger_contex is only used in one thread
  mutable Registry<VertexAccessor> vertex_registry_;
  mutable Registry<EdgeAccessor> edge_registry_;

  template <detail::ObjectAccessor TAccessor>
  Registry<TAccessor> &GetRegistry() {
    if constexpr (std::same_as<TAccessor, VertexAccessor>) {
      return vertex_registry_;
    } else {
      return edge_registry_;
    }
  }

  using LabelChangesMap = std::unordered_map<std::pair<VertexAccessor, storage::LabelId>, int8_t, HashPair>;
  using LabelChangesList = std::pair<std::vector<SetVertexLabel>, std::vector<RemovedVertexLabel>>;
  // While the transaction is being run, collect the information inside a map.
  // During the transaction, a single label on a single object could be added and removed multiple times.
  // We want to register only the global change, at the end of the transaction. The change consists of
  // the state of the label before the transaction start, and the latest state assigned throughout the transaction.
  mutable std::variant<LabelChangesMap, LabelChangesList> label_changes_;

  enum class LabelChange : int8_t { REMOVE = -1, ADD = 1 };

  void UpdateLabelMap(VertexAccessor vertex, storage::LabelId label_id, LabelChange change);
  // We split the map into property changes lists (if it's not already split).
  // Each list contains specific type of change.
  // This method should be called only after the transaction finished running (during Commit or Rollback).
  void LabelMapToList() const;
};

struct Trigger {
  explicit Trigger(std::string name, const std::string &query,
                   const std::map<std::string, storage::PropertyValue> &user_parameters, trigger::EventType event_type,
                   utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, double tsc_frequency,
               double max_execution_time_sec, std::atomic<bool> *is_shutting_down, const TriggerContext &context) const;

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
    using IdentifierInfo = std::pair<Identifier, trigger::IdentifierTag>;

    explicit TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers);

    CachedPlan cached_plan;
    std::vector<IdentifierInfo> identifiers;
  };
  std::shared_ptr<TriggerPlan> GetPlan(DbAccessor *db_accessor) const;

  std::string name_;
  ParsedQuery parsed_statements_;

  trigger::EventType event_type_;

  mutable utils::SpinLock plan_lock_;
  mutable std::shared_ptr<TriggerPlan> trigger_plan_;
};

struct TriggerStore {
  explicit TriggerStore(std::filesystem::path directory, utils::SkipList<QueryCacheEntry> *query_cache,
                        DbAccessor *db_accessor, utils::SpinLock *antlr_lock);

  void AddTrigger(const std::string &name, const std::string &query,
                  const std::map<std::string, storage::PropertyValue> &user_parameters, trigger::EventType event_type,
                  bool before_commit, utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                  utils::SpinLock *antlr_lock);

  void DropTrigger(const std::string &name);

  struct TriggerInfo {
    std::string name;
    std::string statement;
    trigger::EventType event_type;
    bool before_commit;
  };

  std::vector<TriggerInfo> GetTriggerInfo() const;

  const auto &BeforeCommitTriggers() const noexcept { return before_commit_triggers_; }
  const auto &AfterCommitTriggers() const noexcept { return after_commit_triggers_; }

  bool HasTriggers() const noexcept { return before_commit_triggers_.size() > 0 || after_commit_triggers_.size() > 0; }

 private:
  utils::SpinLock store_lock_;
  kvstore::KVStore storage_;

  utils::SkipList<Trigger> before_commit_triggers_;
  utils::SkipList<Trigger> after_commit_triggers_;
};

}  // namespace query
