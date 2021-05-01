#include "query/trigger.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "utils/memory.hpp"

namespace query {

namespace {
std::vector<std::pair<Identifier, trigger::IdentifierTag>> GetPredefinedIdentifiers() {
  return {{{"createdVertices", false}, trigger::IdentifierTag::CREATED_VERTICES},
          {{"deletedVertices", false}, trigger::IdentifierTag::DELETED_VERTICES}};
}

template <typename T>
concept ConvertableToTypedValue = requires(T value) {
  {TypedValue{value}};
};

template <ConvertableToTypedValue T>
TypedValue ToTypedValue(const std::vector<T> &values) {
  std::vector<TypedValue> typed_values;
  typed_values.reserve(values.size());
  std::transform(std::begin(values), std::end(values), std::back_inserter(typed_values),
                 [](const auto &accessor) { return TypedValue(accessor); });
  return TypedValue(typed_values);
}

}  // namespace

void TriggerContext::RegisterCreatedVertex(const VertexAccessor created_vertex) {
  created_vertices_.push_back(created_vertex);
}

void TriggerContext::RegisterDeletedVertex(const VertexAccessor deleted_vertex) {
  deleted_vertices_.push_back(deleted_vertex);
}

TypedValue TriggerContext::GetTypedValue(const trigger::IdentifierTag tag) const {
  switch (tag) {
    case trigger::IdentifierTag::CREATED_VERTICES:
      return ToTypedValue(created_vertices_);
    case trigger::IdentifierTag::DELETED_VERTICES:
      return ToTypedValue(deleted_vertices_);
  }
}

void TriggerContext::AdaptForAccessor(DbAccessor *accessor) {
  // adapt created_vertices_
  auto it = created_vertices_.begin();
  for (const auto &created_vertex : created_vertices_) {
    if (auto maybe_vertex = accessor->FindVertex(created_vertex.Gid(), storage::View::OLD); maybe_vertex) {
      *it = *maybe_vertex;
      ++it;
    }
  }
  created_vertices_.erase(it, created_vertices_.end());

  // deleted_vertices_ should keep the transaction context of the transaction which deleted it
  // because no other transaction can modify an object after it's deleted so it should be the
  // latest state of the object
}

Trigger::Trigger(std::string name, const std::string &query, utils::SkipList<QueryCacheEntry> *query_cache,
                 DbAccessor *db_accessor, utils::SpinLock *antlr_lock)
    : name_(std::move(name)), parsed_statements_{ParseQuery(query, {}, query_cache, antlr_lock)} {
  // We check immediately if the query is valid by trying to create a plan.
  GetPlan(db_accessor);
}

std::shared_ptr<Trigger::TriggerPlan> Trigger::GetPlan(DbAccessor *db_accessor) {
  std::lock_guard plan_guard{plan_lock_};
  if (trigger_plan_ && !trigger_plan_->cached_plan->IsExpired()) {
    return trigger_plan_;
  }

  trigger_plan_ = std::make_shared<TriggerPlan>();
  auto &[cached_plan, identifiers] = *trigger_plan_;
  identifiers = GetPredefinedIdentifiers();

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
  cached_plan.emplace(std::move(logical_plan));

  return trigger_plan_;
}

void Trigger::Execute(DbAccessor *dba, utils::MonotonicBufferResource *execution_memory, const double tsc_frequency,
                      const double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                      const TriggerContext &context) {
  auto trigger_plan = GetPlan(dba);
  MG_ASSERT(trigger_plan, "Invalid trigger plan received");
  auto &[plan, identifiers] = *trigger_plan;

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
  for (const auto &[identifier, tag] : identifiers) {
    if (identifier.symbol_pos_ == -1) {
      continue;
    }

    frame[plan->symbol_table().at(identifier)] = context.GetTypedValue(tag);
  }

  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
}
}  // namespace query
