#include "query/trigger.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "utils/concept.hpp"
#include "utils/memory.hpp"

namespace query {

namespace {
std::vector<std::pair<Identifier, trigger::IdentifierTag>> GetPredefinedIdentifiers() {
  return {{{"createdVertices", false}, trigger::IdentifierTag::CREATED_VERTICES},
          {{"createdEdges", false}, trigger::IdentifierTag::CREATED_EDGES},
          {{"deletedVertices", false}, trigger::IdentifierTag::DELETED_VERTICES},
          {{"assignedVertexProperties", false}, trigger::IdentifierTag::SET_VERTEX_PROPERTIES},
          {{"removedVertexProperties", false}, trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES},
          {{"assignedVertexLabels", false}, trigger::IdentifierTag::SET_VERTEX_LABELS},
          {{"removedVertexLabels", false}, trigger::IdentifierTag::REMOVED_VERTEX_LABELS},
          {{"updatedVertices", false}, trigger::IdentifierTag::UPDATED_VERTICES}};
}

template <typename T>
concept WithToMap = requires(const T value, DbAccessor *dba) {
  { value.ToMap(dba) }
  ->utils::SameAs<std::map<std::string, TypedValue>>;
};

template <WithToMap T>
TypedValue ToTypedValue(const T &value, DbAccessor *dba) {
  return TypedValue{value.ToMap(dba)};
}

template <ObjectAccessor TAccessor>
TypedValue ToTypedValue(const TriggerContext::CreatedObject<TAccessor> &created_object,
                        [[maybe_unused]] DbAccessor *dba) {
  return TypedValue{created_object.object};
}

TypedValue ToTypedValue(const TriggerContext::DeletedVertex &deleted_vertex, [[maybe_unused]] DbAccessor *dba) {
  return TypedValue{deleted_vertex.vertex};
}

template <typename T>
concept WithIsValid = requires(const T value) {
  { value.IsValid() }
  ->utils::SameAs<bool>;
};

template <typename T>
concept ConvertableToTypedValue = requires(T value, DbAccessor *dba) {
  { ToTypedValue(value, dba) }
  ->utils::SameAs<TypedValue>;
  WithIsValid<T>;
};

template <typename T>
concept LabelUpdateContext = utils::SameAsAnyOf<T, TriggerContext::SetVertexLabel, TriggerContext::RemovedVertexLabel>;

template <LabelUpdateContext TContext>
TypedValue ToTypedValue(const std::vector<TContext> &values, DbAccessor *dba) {
  std::unordered_map<storage::LabelId, std::vector<TypedValue>> vertices_by_labels;

  for (const auto &value : values) {
    if (value.IsValid()) {
      vertices_by_labels[value.label_id].emplace_back(value.vertex);
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
  if constexpr (utils::SameAs<T, TriggerContext::SetVertexProperty>) {
    return "set_vertex_property";
  } else if constexpr (utils::SameAs<T, TriggerContext::RemovedVertexProperty>) {
    return "removed_vertex_property";
  } else if constexpr (utils::SameAs<T, TriggerContext::SetVertexLabel>) {
    return "set_vertex_label";
  } else if constexpr (utils::SameAs<T, TriggerContext::RemovedVertexLabel>) {
    return "removed_vertex_label";
  }
}

template <typename T>
concept UpdateContext = WithToMap<T> &&WithIsValid<T>;

template <UpdateContext... Args>
TypedValue Updated(DbAccessor *dba, const std::vector<Args> &...args) {
  const auto size = (args.size() + ...);
  std::vector<TypedValue> updated;
  updated.reserve(size);

  const auto add_to_updated = [&]<UpdateContext T>(const std::vector<T> &values) {
    for (const auto &value : values) {
      if (value.IsValid()) {
        auto map = value.ToMap(dba);
        map["type"] = TypeToString<T>();
        updated.emplace_back(std::move(map));
      }
    }
  };

  (add_to_updated(args), ...);

  return TypedValue(std::move(updated));
}

}  // namespace
bool TriggerContext::DeletedVertex::IsValid() const { return vertex.IsVisible(storage::View::OLD); }

bool TriggerContext::SetVertexProperty::IsValid() const { return vertex.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::SetVertexProperty::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{vertex}},
          {"key", TypedValue{dba->PropertyToName(key)}},
          {"old", old_value},
          {"new", new_value}};
}

bool TriggerContext::RemovedVertexProperty::IsValid() const { return vertex.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::RemovedVertexProperty::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{vertex}}, {"key", TypedValue{dba->PropertyToName(key)}}, {"old", old_value}};
}

bool TriggerContext::SetVertexLabel::IsValid() const { return vertex.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::SetVertexLabel::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{vertex}}, {"label", TypedValue{dba->LabelToName(label_id)}}};
}

bool TriggerContext::RemovedVertexLabel::IsValid() const { return vertex.IsVisible(storage::View::OLD); }

std::map<std::string, TypedValue> TriggerContext::RemovedVertexLabel::ToMap(DbAccessor *dba) const {
  return {{"vertex", TypedValue{vertex}}, {"label", TypedValue{dba->LabelToName(label_id)}}};
}

void TriggerContext::RegisterDeletedVertex(const VertexAccessor &deleted_vertex) {
  deleted_vertices_.emplace_back(deleted_vertex);
}

void TriggerContext::RegisterSetVertexProperty(const VertexAccessor &vertex, const storage::PropertyId key,
                                               TypedValue old_value, TypedValue new_value) {
  if (new_value.IsNull()) {
    RegisterRemovedVertexProperty(vertex, key, std::move(old_value));
    return;
  }

  set_vertex_properties_.emplace_back(vertex, key, std::move(old_value), std::move(new_value));
}

void TriggerContext::RegisterRemovedVertexProperty(const VertexAccessor &vertex, const storage::PropertyId key,
                                                   TypedValue old_value) {
  // vertex is already removed
  if (old_value.IsNull()) {
    return;
  }

  removed_vertex_properties_.emplace_back(vertex, key, std::move(old_value));
}

void TriggerContext::RegisterSetVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id) {
  set_vertex_labels_.emplace_back(vertex, label_id);
}

void TriggerContext::RegisterRemovedVertexLabel(const VertexAccessor &vertex, storage::LabelId label_id) {
  removed_vertex_labels_.emplace_back(vertex, label_id);
}

TypedValue TriggerContext::GetTypedValue(const trigger::IdentifierTag tag, DbAccessor *dba) const {
  switch (tag) {
    case trigger::IdentifierTag::CREATED_VERTICES:
      return ToTypedValue(created_vertices_, dba);
    case trigger::IdentifierTag::CREATED_EDGES:
      return ToTypedValue(created_edges_, dba);
    case trigger::IdentifierTag::DELETED_VERTICES:
      return ToTypedValue(deleted_vertices_, dba);
    case trigger::IdentifierTag::SET_VERTEX_PROPERTIES:
      return ToTypedValue(set_vertex_properties_, dba);
    case trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES:
      return ToTypedValue(removed_vertex_properties_, dba);
    case trigger::IdentifierTag::SET_VERTEX_LABELS:
      return ToTypedValue(set_vertex_labels_, dba);
    case trigger::IdentifierTag::REMOVED_VERTEX_LABELS:
      return ToTypedValue(removed_vertex_labels_, dba);
    case trigger::IdentifierTag::UPDATED_VERTICES:
      return Updated(dba, set_vertex_properties_, removed_vertex_properties_, set_vertex_labels_,
                     removed_vertex_labels_);
  }
}

void TriggerContext::AdaptForAccessor(DbAccessor *accessor) {
  // adapt created_vertices_
  {
    auto it = created_vertices_.begin();
    for (const auto &created_vertex : created_vertices_) {
      if (auto maybe_vertex = accessor->FindVertex(created_vertex.object.Gid(), storage::View::OLD); maybe_vertex) {
        *it = CreatedObject{*maybe_vertex};
        ++it;
      }
    }
    created_vertices_.erase(it, created_vertices_.end());
  }

  // deleted_vertices_ should keep the transaction context of the transaction which deleted it
  // because no other transaction can modify an object after it's deleted so it should be the
  // latest state of the object

  const auto adapt_context_with_vertex = [accessor](auto *values) {
    auto it = values->begin();
    for (auto &value : *values) {
      if (auto maybe_vertex = accessor->FindVertex(value.vertex.Gid(), storage::View::OLD); maybe_vertex) {
        *it = std::move(value);
        it->vertex = *maybe_vertex;
        ++it;
      }
    }
  };

  adapt_context_with_vertex(&set_vertex_properties_);
  adapt_context_with_vertex(&removed_vertex_properties_);
  adapt_context_with_vertex(&set_vertex_labels_);
  adapt_context_with_vertex(&removed_vertex_labels_);
}

Trigger::Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
                 utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock)
    : name_(std::move(name)),
      parsed_statements_{ParseQuery(query, {}, query_cache, antlr_lock)},
      identifiers_{GetPredefinedIdentifiers()} {
  // We check immediately if the query is valid by trying to create a plan.
  GetPlan(plan_cache, db_accessor);
}

std::shared_ptr<CachedPlan> Trigger::GetPlan(utils::SkipList<PlanCacheEntry> *plan_cache,
                                             DbAccessor *db_accessor) const {
  AstStorage ast_storage;
  ast_storage.properties_ = parsed_statements_.ast_storage.properties_;
  ast_storage.labels_ = parsed_statements_.ast_storage.labels_;
  ast_storage.edge_types_ = parsed_statements_.ast_storage.edge_types_;

  std::vector<Identifier *> predefined_identifiers;
  predefined_identifiers.reserve(identifiers_.size());
  std::transform(identifiers_.begin(), identifiers_.end(), std::back_inserter(predefined_identifiers),
                 [](auto &identifier) { return &identifier.first; });

  return CypherQueryToPlan(utils::Fnv(name_), std::move(ast_storage),
                           utils::Downcast<CypherQuery>(parsed_statements_.query), parsed_statements_.parameters,
                           plan_cache, db_accessor, parsed_statements_.is_cacheable, predefined_identifiers);
}

void Trigger::Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
                      utils::MonotonicBufferResource *execution_memory, const double tsc_frequency,
                      const double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                      const TriggerContext &context) const {
  auto plan = GetPlan(plan_cache, dba);

  ExecutionContext ctx;
  ctx.db_accessor = dba;
  ctx.symbol_table = plan->symbol_table();
  ctx.evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  ctx.evaluation_context.parameters = parsed_statements_.parameters;
  ctx.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, dba);
  ctx.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, dba);
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

  auto cursor = plan->plan().MakeCursor(execution_memory);
  Frame frame{plan->symbol_table().max_position(), execution_memory};
  for (const auto &[identifier, tag] : identifiers_) {
    if (identifier.symbol_pos_ == -1) {
      continue;
    }

    frame[plan->symbol_table().at(identifier)] = context.GetTypedValue(tag, dba);
  }

  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
}
}  // namespace query
