#include <concepts>

#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/memory.hpp"

namespace query {

namespace trigger {
const char *EventTypeToString(const EventType event_type) {
  switch (event_type) {
    case EventType::ANY:
      return "ANY";

    case EventType::CREATE:
      return "CREATE";

    case EventType::VERTEX_CREATE:
      return "() CREATE";

    case EventType::EDGE_CREATE:
      return "--> CREATE";

    case EventType::DELETE:
      return "DELETE";

    case EventType::VERTEX_DELETE:
      return "() DELETE";

    case EventType::EDGE_DELETE:
      return "--> DELETE";

    case EventType::UPDATE:
      return "UPDATE";

    case EventType::VERTEX_UPDATE:
      return "() UPDATE";

    case EventType::EDGE_UPDATE:
      return "--> UPDATE";
  }
}
}  // namespace trigger

namespace {

auto IdentifierString(const trigger::IdentifierTag tag) noexcept {
  switch (tag) {
    case trigger::IdentifierTag::CREATED_VERTICES:
      return "createdVertices";

    case trigger::IdentifierTag::CREATED_EDGES:
      return "createdEdges";

    case trigger::IdentifierTag::CREATED_OBJECTS:
      return "createdObjects";

    case trigger::IdentifierTag::DELETED_VERTICES:
      return "deletedVertices";

    case trigger::IdentifierTag::DELETED_EDGES:
      return "deletedEdges";

    case trigger::IdentifierTag::DELETED_OBJECTS:
      return "deletedObjects";

    case trigger::IdentifierTag::SET_VERTEX_PROPERTIES:
      return "assignedVertexProperties";

    case trigger::IdentifierTag::SET_EDGE_PROPERTIES:
      return "assignedEdgeProperties";

    case trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES:
      return "removedVertexProperties";

    case trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES:
      return "removedEdgeProperties";

    case trigger::IdentifierTag::SET_VERTEX_LABELS:
      return "assignedVertexLabels";

    case trigger::IdentifierTag::REMOVED_VERTEX_LABELS:
      return "removedVertexLabels";

    case trigger::IdentifierTag::UPDATED_VERTICES:
      return "updatedVertices";

    case trigger::IdentifierTag::UPDATED_EDGES:
      return "updatedEdges";

    case trigger::IdentifierTag::UPDATED_OBJECTS:
      return "updatedObjects";
  }
}

std::vector<std::pair<Identifier, trigger::IdentifierTag>> TagsToIdentifiers(
    const std::same_as<trigger::IdentifierTag> auto... args) {
  std::vector<std::pair<Identifier, trigger::IdentifierTag>> identifiers;
  identifiers.reserve(sizeof...(args));

  auto add_identifier = [&identifiers](const auto tag) {
    identifiers.emplace_back(Identifier{IdentifierString(tag), false}, tag);
  };

  (add_identifier(args), ...);

  return identifiers;
};

std::vector<std::pair<Identifier, trigger::IdentifierTag>> GetPredefinedIdentifiers(
    const trigger::EventType event_type) {
  using IdentifierTag = trigger::IdentifierTag;
  using EventType = trigger::EventType;

  switch (event_type) {
    case EventType::ANY:
      return {};

    case EventType::CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_OBJECTS);

    case EventType::VERTEX_CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_VERTICES);

    case EventType::EDGE_CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_EDGES);

    case EventType::DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_OBJECTS);

    case EventType::VERTEX_DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_VERTICES);

    case EventType::EDGE_DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_EDGES);

    case EventType::UPDATE:
      return TagsToIdentifiers(IdentifierTag::UPDATED_OBJECTS);

    case EventType::VERTEX_UPDATE:
      return TagsToIdentifiers(IdentifierTag::SET_VERTEX_PROPERTIES, IdentifierTag::REMOVED_VERTEX_PROPERTIES,
                               IdentifierTag::SET_VERTEX_LABELS, IdentifierTag::REMOVED_VERTEX_LABELS,
                               IdentifierTag::UPDATED_VERTICES);

    case EventType::EDGE_UPDATE:
      return TagsToIdentifiers(IdentifierTag::SET_EDGE_PROPERTIES, IdentifierTag::REMOVED_EDGE_PROPERTIES,
                               IdentifierTag::UPDATED_EDGES);
  }
}

template <typename T>
concept WithToMap = requires(const T value, DbAccessor *dba) {
  { value.ToMap(dba) }
  ->std::same_as<std::map<std::string, TypedValue>>;
};

TypedValue ToTypedValue(const WithToMap auto &value, DbAccessor *dba) { return TypedValue{value.ToMap(dba)}; }

template <detail::ObjectAccessor TAccessor>
TypedValue ToTypedValue(const TriggerContext::CreatedObject<TAccessor> &created_object,
                        [[maybe_unused]] DbAccessor *dba) {
  return TypedValue{created_object.object};
}

template <detail::ObjectAccessor TAccessor>
TypedValue ToTypedValue(const TriggerContext::DeletedObject<TAccessor> &deleted_object,
                        [[maybe_unused]] DbAccessor *dba) {
  return TypedValue{deleted_object.object};
}

template <typename T>
concept WithIsValid = requires(const T value) {
  { value.IsValid() }
  ->std::same_as<bool>;
};

template <typename T>
concept ConvertableToTypedValue = requires(T value, DbAccessor *dba) {
  { ToTypedValue(value, dba) }
  ->std::same_as<TypedValue>;
}
&&WithIsValid<T>;

template <typename T>
concept LabelUpdateContext = utils::SameAsAnyOf<T, TriggerContext::SetVertexLabel, TriggerContext::RemovedVertexLabel>;

template <LabelUpdateContext TContext>
TypedValue ToTypedValue(const std::vector<TContext> &values, DbAccessor *dba) {
  std::unordered_map<storage::LabelId, std::vector<TypedValue>> vertices_by_labels;

  for (const auto &value : values) {
    if (value.IsValid()) {
      vertices_by_labels[value.label_id].emplace_back(value.object);
    }
  }

  std::map<std::string, TypedValue> typed_values;
  for (auto &[label_id, vertices] : vertices_by_labels) {
    typed_values.emplace(dba->LabelToName(label_id), TypedValue(std::move(vertices)));
  }

  return TypedValue(std::move(typed_values));
}

template <ConvertableToTypedValue T>
TypedValue ToTypedValue(const std::vector<T> &values, DbAccessor *dba) requires(!LabelUpdateContext<T>) {
  std::vector<TypedValue> typed_values;
  typed_values.reserve(values.size());

  for (const auto &value : values) {
    if (value.IsValid()) {
      typed_values.push_back(ToTypedValue(value, dba));
    }
  }

  return TypedValue(std::move(typed_values));
}

template <typename T>
const char *TypeToString() {
  if constexpr (std::same_as<T, TriggerContext::CreatedObject<VertexAccessor>>) {
    return "created_vertex";
  } else if constexpr (std::same_as<T, TriggerContext::CreatedObject<EdgeAccessor>>) {
    return "created_edge";
  } else if constexpr (std::same_as<T, TriggerContext::DeletedObject<VertexAccessor>>) {
    return "deleted_vertex";
  } else if constexpr (std::same_as<T, TriggerContext::DeletedObject<EdgeAccessor>>) {
    return "deleted_edge";
  } else if constexpr (std::same_as<T, TriggerContext::SetObjectProperty<VertexAccessor>>) {
    return "set_vertex_property";
  } else if constexpr (std::same_as<T, TriggerContext::SetObjectProperty<EdgeAccessor>>) {
    return "set_edge_property";
  } else if constexpr (std::same_as<T, TriggerContext::RemovedObjectProperty<VertexAccessor>>) {
    return "removed_vertex_property";
  } else if constexpr (std::same_as<T, TriggerContext::RemovedObjectProperty<EdgeAccessor>>) {
    return "removed_edge_property";
  } else if constexpr (std::same_as<T, TriggerContext::SetVertexLabel>) {
    return "set_vertex_label";
  } else if constexpr (std::same_as<T, TriggerContext::RemovedVertexLabel>) {
    return "removed_vertex_label";
  }
}

template <typename T>
concept ContextInfo = WithToMap<T> &&WithIsValid<T>;

template <ContextInfo... Args>
TypedValue Concatenate(DbAccessor *dba, const std::vector<Args> &...args) {
  const auto size = (args.size() + ...);
  std::vector<TypedValue> concatenated;
  concatenated.reserve(size);

  const auto add_to_concatenated = [&]<ContextInfo T>(const std::vector<T> &values) {
    for (const auto &value : values) {
      if (value.IsValid()) {
        auto map = value.ToMap(dba);
        map["event_type"] = TypeToString<T>();
        concatenated.emplace_back(std::move(map));
      }
    }
  };

  (add_to_concatenated(args), ...);

  return TypedValue(std::move(concatenated));
}

template <typename T>
concept WithSize = requires(const T value) {
  { value.size() }
  ->std::same_as<size_t>;
};

bool AnyContainsValue(const WithSize auto &...value_containers) { return (!value_containers.empty() || ...); }

}  // namespace

bool TriggerContext::SetVertexLabel::IsValid() const { return object.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::SetVertexLabel::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{object}}, {"label", TypedValue{dba->LabelToName(label_id)}}};
}

bool TriggerContext::RemovedVertexLabel::IsValid() const { return object.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::RemovedVertexLabel::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{object}}, {"label", TypedValue{dba->LabelToName(label_id)}}};
}

void TriggerContext::UpdateLabelMap(const VertexAccessor vertex, const storage::LabelId label_id,
                                    const LabelChange change) {
  auto *label_changes_map = std::get_if<LabelChangesMap>(&label_changes_);
  MG_ASSERT(label_changes_map, "Invalid state of trigger context");

  if (auto it = label_changes_map->find({vertex, label_id}); it != label_changes_map->end()) {
    it->second = std::clamp(it->second + static_cast<int8_t>(change), -1, 1);
    return;
  }

  label_changes_map->emplace(std::make_pair(vertex, label_id), static_cast<int8_t>(change));
}

void TriggerContext::RegisterSetVertexLabel(const VertexAccessor &vertex, const storage::LabelId label_id) {
  UpdateLabelMap(vertex, label_id, LabelChange::ADD);
}

void TriggerContext::RegisterRemovedVertexLabel(const VertexAccessor &vertex, const storage::LabelId label_id) {
  UpdateLabelMap(vertex, label_id, LabelChange::REMOVE);
}

void TriggerContext::LabelMapToList() const {
  auto *map = std::get_if<LabelChangesMap>(&label_changes_);
  if (!map) {
    return;
  }

  std::vector<SetVertexLabel> set_vertex_labels;
  std::vector<RemovedVertexLabel> removed_vertex_labels;

  for (auto it = map->begin(); it != map->end(); it = map->erase(it)) {
    const auto &[key, label_state] = *it;

    if (label_state == 1) {
      set_vertex_labels.emplace_back(key.first, key.second);
    } else if (label_state == -1) {
      removed_vertex_labels.emplace_back(key.first, key.second);
    }
  }

  label_changes_ = LabelChangesList{std::move(set_vertex_labels), std::move(removed_vertex_labels)};
}

TypedValue TriggerContext::GetTypedValue(const trigger::IdentifierTag tag, DbAccessor *dba) const {
  vertex_registry_.PropertyMapToList();
  const auto &[created_vertices, deleted_vertices, vertex_property_changes] = vertex_registry_;
  const auto &[set_vertex_properties, removed_vertex_properties] =
      *std::get_if<PropertyChangesList<VertexAccessor>>(&vertex_property_changes);
  LabelMapToList();
  const auto &[set_vertex_labels, removed_vertex_labels] = *std::get_if<LabelChangesList>(&label_changes_);

  edge_registry_.PropertyMapToList();
  const auto &[created_edges, deleted_edges, edge_property_changes] = edge_registry_;
  const auto &[set_edge_properties, removed_edge_properties] =
      *std::get_if<PropertyChangesList<EdgeAccessor>>(&edge_property_changes);

  switch (tag) {
    case trigger::IdentifierTag::CREATED_VERTICES:
      return ToTypedValue(created_vertices, dba);

    case trigger::IdentifierTag::CREATED_EDGES:
      return ToTypedValue(created_edges, dba);

    case trigger::IdentifierTag::CREATED_OBJECTS:
      return Concatenate(dba, created_vertices, created_edges);

    case trigger::IdentifierTag::DELETED_VERTICES:
      return ToTypedValue(deleted_vertices, dba);

    case trigger::IdentifierTag::DELETED_EDGES:
      return ToTypedValue(deleted_edges, dba);

    case trigger::IdentifierTag::DELETED_OBJECTS:
      return Concatenate(dba, deleted_vertices, deleted_edges);

    case trigger::IdentifierTag::SET_VERTEX_PROPERTIES:
      return ToTypedValue(set_vertex_properties, dba);

    case trigger::IdentifierTag::SET_EDGE_PROPERTIES:
      return ToTypedValue(set_edge_properties, dba);

    case trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES:
      return ToTypedValue(removed_vertex_properties, dba);

    case trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES:
      return ToTypedValue(removed_edge_properties, dba);

    case trigger::IdentifierTag::SET_VERTEX_LABELS:
      return ToTypedValue(set_vertex_labels, dba);

    case trigger::IdentifierTag::REMOVED_VERTEX_LABELS:
      return ToTypedValue(removed_vertex_labels, dba);

    case trigger::IdentifierTag::UPDATED_VERTICES:
      return Concatenate(dba, set_vertex_properties, removed_vertex_properties, set_vertex_labels,
                         removed_vertex_labels);

    case trigger::IdentifierTag::UPDATED_EDGES:
      return Concatenate(dba, set_edge_properties, removed_edge_properties);

    case trigger::IdentifierTag::UPDATED_OBJECTS:
      return Concatenate(dba, set_vertex_properties, set_edge_properties, removed_vertex_properties,
                         removed_edge_properties, set_vertex_labels, removed_vertex_labels);
  }
}

bool TriggerContext::ShouldEvenTrigger(const trigger::EventType event_type) const {
  vertex_registry_.PropertyMapToList();
  const auto &[created_vertices, deleted_vertices, vertex_property_changes] = vertex_registry_;
  const auto &[set_vertex_properties, removed_vertex_properties] =
      *std::get_if<PropertyChangesList<VertexAccessor>>(&vertex_property_changes);
  LabelMapToList();
  const auto &[set_vertex_labels, removed_vertex_labels] = *std::get_if<LabelChangesList>(&label_changes_);

  edge_registry_.PropertyMapToList();
  const auto &[created_edges, deleted_edges, edge_property_changes] = edge_registry_;
  const auto &[set_edge_properties, removed_edge_properties] =
      *std::get_if<PropertyChangesList<EdgeAccessor>>(&edge_property_changes);

  using EventType = trigger::EventType;
  switch (event_type) {
    case EventType::ANY:
      return true;

    case EventType::CREATE:
      return AnyContainsValue(created_vertices, created_edges);

    case EventType::VERTEX_CREATE:
      return AnyContainsValue(created_vertices);

    case EventType::EDGE_CREATE:
      return AnyContainsValue(created_edges);

    case EventType::DELETE:
      return AnyContainsValue(deleted_vertices, deleted_edges);

    case EventType::VERTEX_DELETE:
      return AnyContainsValue(deleted_vertices);

    case EventType::EDGE_DELETE:
      return AnyContainsValue(deleted_edges);

    case EventType::UPDATE:
      return AnyContainsValue(set_vertex_properties, set_edge_properties, removed_vertex_properties,
                              removed_edge_properties, set_vertex_labels, removed_vertex_labels);

    case EventType::VERTEX_UPDATE:
      return AnyContainsValue(set_vertex_properties, removed_vertex_properties, set_vertex_labels,
                              removed_vertex_labels);

    case EventType::EDGE_UPDATE:
      return AnyContainsValue(set_edge_properties, removed_edge_properties);
  }
}

void TriggerContext::AdaptForAccessor(DbAccessor *accessor) {
  vertex_registry_.PropertyMapToList();
  auto &[created_vertices, deleted_vertices, vertex_property_changes] = vertex_registry_;
  auto &[set_vertex_properties, removed_vertex_properties] =
      *std::get_if<PropertyChangesList<VertexAccessor>>(&vertex_property_changes);
  LabelMapToList();
  auto &[set_vertex_labels, removed_vertex_labels] = *std::get_if<LabelChangesList>(&label_changes_);

  // adapt created_vertices_
  {
    auto it = created_vertices.begin();
    for (const auto &created_vertex : created_vertices) {
      if (auto maybe_vertex = accessor->FindVertex(created_vertex.object.Gid(), storage::View::OLD); maybe_vertex) {
        *it = CreatedObject{*maybe_vertex};
        ++it;
      }
    }
    created_vertices.erase(it, created_vertices.end());
  }

  // deleted_vertices_ should keep the transaction context of the transaction which deleted it
  // because no other transaction can modify an object after it's deleted so it should be the
  // latest state of the object

  const auto adapt_context_with_vertex = [accessor](auto *values) {
    auto it = values->begin();
    for (auto &value : *values) {
      if (auto maybe_vertex = accessor->FindVertex(value.object.Gid(), storage::View::OLD); maybe_vertex) {
        *it = std::move(value);
        it->object = *maybe_vertex;
        ++it;
      }
    }
    values->erase(it, values->end());
  };

  adapt_context_with_vertex(&set_vertex_properties);
  adapt_context_with_vertex(&removed_vertex_properties);
  adapt_context_with_vertex(&set_vertex_labels);
  adapt_context_with_vertex(&removed_vertex_labels);

  edge_registry_.PropertyMapToList();
  auto &[created_edges, deleted_edges, edge_property_changes] = edge_registry_;
  auto &[set_edge_properties, removed_edge_properties] =
      *std::get_if<PropertyChangesList<EdgeAccessor>>(&edge_property_changes);
  // adapt created_edges
  {
    auto it = created_edges.begin();
    for (const auto &created_edge : created_edges) {
      if (auto maybe_vertex = accessor->FindVertex(created_edge.object.From().Gid(), storage::View::OLD);
          maybe_vertex) {
        auto maybe_out_edges = maybe_vertex->OutEdges(storage::View::OLD);
        MG_ASSERT(maybe_out_edges.HasValue());
        for (const auto &edge : *maybe_out_edges) {
          if (edge.Gid() == created_edge.object.Gid()) {
            *it = CreatedObject{edge};
            ++it;
            break;
          }
        }
      }
    }
    created_edges.erase(it, created_edges.end());
  }

  // deleted_edges_ should keep the transaction context of the transaction which deleted it
  // because no other transaction can modify an object after it's deleted so it should be the
  // latest state of the object

  const auto adapt_context_with_edge = [accessor](auto *values) {
    auto it = values->begin();
    for (const auto &value : *values) {
      if (auto maybe_vertex = accessor->FindVertex(value.object.From().Gid(), storage::View::OLD); maybe_vertex) {
        auto maybe_out_edges = maybe_vertex->OutEdges(storage::View::OLD);
        MG_ASSERT(maybe_out_edges.HasValue());
        for (const auto &edge : *maybe_out_edges) {
          if (edge.Gid() == value.object.Gid()) {
            *it = std::move(value);
            it->object = edge;
            ++it;
            break;
          }
        }
      }
    }
    values->erase(it, values->end());
  };

  adapt_context_with_edge(&set_edge_properties);
  adapt_context_with_edge(&removed_edge_properties);
}

Trigger::Trigger(std::string name, const std::string &query,
                 const std::map<std::string, storage::PropertyValue> &user_parameters,
                 const trigger::EventType event_type, utils::SkipList<QueryCacheEntry> *query_cache,
                 DbAccessor *db_accessor, utils::SpinLock *antlr_lock)
    : name_{std::move(name)},
      parsed_statements_{ParseQuery(query, user_parameters, query_cache, antlr_lock)},
      event_type_{event_type} {
  // We check immediately if the query is valid by trying to create a plan.
  GetPlan(db_accessor);
}

Trigger::TriggerPlan::TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers)
    : cached_plan(std::move(logical_plan)), identifiers(std::move(identifiers)) {}

std::shared_ptr<Trigger::TriggerPlan> Trigger::GetPlan(DbAccessor *db_accessor) const {
  std::lock_guard plan_guard{plan_lock_};
  if (parsed_statements_.is_cacheable && trigger_plan_ && !trigger_plan_->cached_plan.IsExpired()) {
    return trigger_plan_;
  }

  auto identifiers = GetPredefinedIdentifiers(event_type_);

  AstStorage ast_storage;
  ast_storage.properties_ = parsed_statements_.ast_storage.properties_;
  ast_storage.labels_ = parsed_statements_.ast_storage.labels_;
  ast_storage.edge_types_ = parsed_statements_.ast_storage.edge_types_;

  std::vector<Identifier *> predefined_identifiers;
  predefined_identifiers.reserve(identifiers.size());
  std::transform(identifiers.begin(), identifiers.end(), std::back_inserter(predefined_identifiers),
                 [](auto &identifier) { return &identifier.first; });

  auto logical_plan = MakeLogicalPlan(std::move(ast_storage), utils::Downcast<CypherQuery>(parsed_statements_.query),
                                      parsed_statements_.parameters, db_accessor, predefined_identifiers);

  trigger_plan_ = std::make_shared<TriggerPlan>(std::move(logical_plan), std::move(identifiers));
  return trigger_plan_;
}

void Trigger::Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, const double tsc_frequency,
                      const double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                      const TriggerContext &context) const {
  if (!context.ShouldEvenTrigger(event_type_)) {
    return;
  }

  spdlog::debug("Executing trigger '{}'", name_);
  auto trigger_plan = GetPlan(dba);
  MG_ASSERT(trigger_plan, "Invalid trigger plan received");
  auto &[plan, identifiers] = *trigger_plan;

  ExecutionContext ctx;
  ctx.db_accessor = dba;
  ctx.symbol_table = plan.symbol_table();
  ctx.evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  ctx.evaluation_context.parameters = parsed_statements_.parameters;
  ctx.evaluation_context.properties = NamesToProperties(plan.ast_storage().properties_, dba);
  ctx.evaluation_context.labels = NamesToLabels(plan.ast_storage().labels_, dba);
  ctx.execution_tsc_timer = utils::TSCTimer(tsc_frequency);
  ctx.max_execution_time_sec = max_execution_time_sec;
  ctx.is_shutting_down = is_shutting_down;
  ctx.is_profile_query = false;

  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  constexpr size_t stack_size = 256 * 1024;
  char stack_data[stack_size];

  // We can throw on every query because a simple queries for deleting will use only
  // the stack allocated buffer.
  // Also, we want to throw only when the query engine requests more memory and not the storage
  // so we add the exception to the allocator.
  utils::ResourceWithOutOfMemoryException resource_with_exception;
  utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size, &resource_with_exception);
  // TODO (mferencevic): Tune the parameters accordingly.
  utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
  ctx.evaluation_context.memory = &pool_memory;

  auto cursor = plan.plan().MakeCursor(execution_memory);
  Frame frame{plan.symbol_table().max_position(), execution_memory};
  for (const auto &[identifier, tag] : identifiers) {
    if (identifier.symbol_pos_ == -1) {
      continue;
    }

    frame[plan.symbol_table().at(identifier)] = context.GetTypedValue(tag, dba);
  }

  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
}

namespace {
constexpr uint64_t kVersion{1};

inline nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value);

nlohmann::json SerializeVector(const std::vector<storage::PropertyValue> &values) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto &value : values) {
    array.push_back(SerializePropertyValue(value));
  }
  return array;
}

nlohmann::json SerializeMap(const std::map<std::string, storage::PropertyValue> &parameters) {
  nlohmann::json data = nlohmann::json::object();

  for (const auto &[key, value] : parameters) {
    data[key] = SerializePropertyValue(value);
  }

  return data;
}

nlohmann::json SerializePropertyValue(const storage::PropertyValue &property_value) {
  using Type = storage::PropertyValue::Type;
  switch (property_value.type()) {
    case Type::Null:
      return {};
    case Type::Bool:
      return property_value.ValueBool();
    case Type::Int:
      return property_value.ValueInt();
    case Type::Double:
      return property_value.ValueDouble();
    case Type::String:
      return property_value.ValueString();
    case Type::List:
      return SerializeVector(property_value.ValueList());
    case Type::Map:
      return SerializeMap(property_value.ValueMap());
  }
}

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data);

auto DeserializePropertyValueList(const nlohmann::json::array_t &data) {
  std::vector<storage::PropertyValue> property_values;
  property_values.reserve(data.size());
  for (const auto &value : data) {
    property_values.emplace_back(DeserializePropertyValue(value));
  }

  return property_values;
}

auto DeserializePropertyValueMap(const nlohmann::json::object_t &data) {
  std::map<std::string, storage::PropertyValue> property_values;

  for (const auto &[key, value] : data) {
    property_values.emplace(key, DeserializePropertyValue(value));
  }

  return property_values;
}

storage::PropertyValue DeserializePropertyValue(const nlohmann::json &data) {
  if (data.is_null()) {
    return storage::PropertyValue();
  }

  if (data.is_boolean()) {
    return storage::PropertyValue(static_cast<bool>(data));
  }

  if (data.is_number_integer()) {
    return storage::PropertyValue(static_cast<int64_t>(data));
  }

  if (data.is_number_float()) {
    return storage::PropertyValue(static_cast<double>(data));
  }

  if (data.is_string()) {
    return storage::PropertyValue(std::string{data});
  }

  if (data.is_array()) {
    return storage::PropertyValue(DeserializePropertyValueList(data));
  }

  MG_ASSERT(data.is_object(), "Unknown type found in the trigger storage");
  return storage::PropertyValue(DeserializePropertyValueMap(data));
}

}  // namespace

TriggerStore::TriggerStore(std::filesystem::path directory, utils::SkipList<QueryCacheEntry> *query_cache,
                           DbAccessor *db_accessor, utils::SpinLock *antlr_lock)
    : storage_{std::move(directory)} {
  spdlog::info("Loading triggers...");

  for (const auto &[trigger_name, trigger_data] : storage_) {
    spdlog::debug("Loading trigger '{}'", trigger_name);
    auto json_trigger_data = nlohmann::json::parse(trigger_data);

    if (!json_trigger_data["version"].is_number_unsigned()) {
      spdlog::debug("Invalid state of the trigger data.");
      continue;
    }
    if (json_trigger_data["version"] != kVersion) {
      spdlog::debug("Invalid version of the trigger data. Got {}");
      continue;
    }

    if (!json_trigger_data["statement"].is_string()) {
      spdlog::debug("Invalid state of the trigger data");
      continue;
    }
    std::string statement = json_trigger_data["statement"];

    if (!json_trigger_data["before_commit"].is_boolean()) {
      spdlog::debug("Invalid state of the trigger data");
      continue;
    }
    const bool before_commit = json_trigger_data["before_commit"];

    if (!json_trigger_data["event_type"].is_number_integer()) {
      spdlog::debug("Invalid state of the trigger data");
      continue;
    }
    const auto event_type = static_cast<trigger::EventType>(json_trigger_data["event_type"]);

    if (!json_trigger_data["user_parameters"].is_object()) {
      spdlog::debug("Invalid state of the trigger data");
      continue;
    }
    const auto user_parameters = DeserializePropertyValueMap(json_trigger_data["user_parameters"]);

    std::optional<Trigger> trigger;
    try {
      trigger.emplace(trigger_name, statement, user_parameters, event_type, query_cache, db_accessor, antlr_lock);
    } catch (const utils::BasicException &e) {
      spdlog::debug("Failed to create a trigger '{}' because: {}", trigger_name, e.what());
      continue;
    }

    auto triggers_acc = before_commit ? before_commit_triggers_.access() : after_commit_triggers_.access();
    triggers_acc.insert(std::move(*trigger));

    spdlog::debug("Trigger loaded successfully!");
  }
}

void TriggerStore::AddTrigger(const std::string &name, const std::string &query,
                              const std::map<std::string, storage::PropertyValue> &user_parameters,
                              trigger::EventType event_type, bool before_commit,
                              utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                              utils::SpinLock *antlr_lock) {
  std::unique_lock store_guard{store_lock_};
  if (storage_.Get(name)) {
    throw utils::BasicException("Trigger with the same name already exists.");
  }

  std::optional<Trigger> trigger;
  try {
    trigger.emplace(name, query, user_parameters, event_type, query_cache, db_accessor, antlr_lock);
  } catch (const utils::BasicException &e) {
    const auto identifiers = GetPredefinedIdentifiers(event_type);
    std::stringstream identifier_names_stream;
    utils::PrintIterable(identifier_names_stream, identifiers, ", ",
                         [](auto &stream, const auto &identifier) { stream << identifier.first.name_; });

    throw utils::BasicException(
        "Failed creating the trigger.\nError message: '{}'\nThe error was mostly likely generated because of the wrong "
        "statement that this trigger executes.\nMake sure all predefined variables used are present for the specified "
        "event.\nAllowed variables for event '{}' are: {}",
        e.what(), EventTypeToString(event_type), identifier_names_stream.str());
  }

  nlohmann::json data = nlohmann::json::object();
  data["statement"] = query;
  data["user_parameters"] = SerializeMap(user_parameters);
  data["event_type"] = static_cast<std::underlying_type_t<trigger::EventType>>(event_type);
  data["before_commit"] = before_commit;
  data["version"] = kVersion;
  storage_.Put(name, data.dump());
  store_guard.unlock();

  auto triggers_acc = before_commit ? before_commit_triggers_.access() : after_commit_triggers_.access();
  triggers_acc.insert(std::move(*trigger));
}

void TriggerStore::DropTrigger(const std::string &name) {
  std::unique_lock store_guard{store_lock_};
  const auto maybe_trigger_data = storage_.Get(name);
  if (!maybe_trigger_data) {
    throw utils::BasicException("Trigger with name '{}' doesn't exist", name);
  }

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*maybe_trigger_data);
  } catch (const nlohmann::json::parse_error &e) {
    throw utils::BasicException("Couldn't load trigger data!");
  }

  if (!data.is_object()) {
    throw utils::BasicException("Couldn't load trigger data!");
  }

  if (!data["before_commit"].is_boolean()) {
    throw utils::BasicException("Invalid type loaded inside the trigger data!");
  }

  auto triggers_acc = data["before_commit"] ? before_commit_triggers_.access() : after_commit_triggers_.access();
  triggers_acc.remove(name);
  storage_.Delete(name);
}

std::vector<TriggerStore::TriggerInfo> TriggerStore::GetTriggerInfo() const {
  std::vector<TriggerInfo> info;
  info.reserve(before_commit_triggers_.size() + after_commit_triggers_.size());

  const auto add_info = [&](const utils::SkipList<Trigger> &trigger_list, bool before_commit) {
    for (const auto &trigger : trigger_list.access()) {
      info.push_back({trigger.Name(), trigger.OriginalStatement(), trigger.EventType(), before_commit});
    }
  };

  add_info(before_commit_triggers_, true);
  add_info(after_commit_triggers_, false);

  return info;
}
}  // namespace query
