#include "query/trigger.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "utils/memory.hpp"

namespace query {

namespace {
constexpr const char *kCreatedVertices = "createdVertices";

std::vector<Identifier> GetPredefinedIdentifiers() { return {{kCreatedVertices, false}}; }
}  // namespace

Trigger::Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *query_cache,
                 utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *db_accessor, utils::SpinLock *antlr_lock)
    : name_(std::move(name)),
      parsed_statements_{ParseQuery(query, {}, query_cache, antlr_lock)},
      identifiers_{GetPredefinedIdentifiers()} {
  GetPlan(plan_cache, db_accessor);
}

std::shared_ptr<CachedPlan> Trigger::GetPlan(utils::SkipList<PlanCacheEntry> *plan_cache,
                                             DbAccessor *db_accessor) const {
  AstStorage ast_storage;
  ast_storage.properties_ = parsed_statements_.ast_storage.properties_;
  ast_storage.labels_ = parsed_statements_.ast_storage.labels_;
  ast_storage.edge_types_ = parsed_statements_.ast_storage.edge_types_;

  std::unordered_map<std::string, Identifier *> predefined_identifiers;
  for (auto &identifier : identifiers_) {
    predefined_identifiers.emplace(identifier.name_, &identifier);
  }

  return CypherQueryToPlan(parsed_statements_.stripped_query.hash(), std::move(ast_storage),
                           utils::Downcast<CypherQuery>(parsed_statements_.query), parsed_statements_.parameters,
                           plan_cache, db_accessor, parsed_statements_.is_cacheable, std::move(predefined_identifiers));
}

void Trigger::Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
                      utils::MonotonicBufferResource *execution_memory, const double tsc_frequency,
                      const double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                      std::unordered_map<std::string, TypedValue> context) const {
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
  for (const auto &identifier : identifiers_) {
    if (identifier.symbol_pos_ == -1) {
      continue;
    }

    if (auto it = context.find(identifier.name_); it != context.end()) {
      frame[plan->symbol_table().at(identifier)] = std::move(it->second);
    }
  }

  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
}

void TriggerContext::RegisterCreatedVertex(const VertexAccessor created_vertex) {
  created_vertices_.push_back(created_vertex);
}

std::unordered_map<std::string, TypedValue> TriggerContext::GetTypedValues() {
  std::unordered_map<std::string, TypedValue> typed_values;

  std::vector<TypedValue> typed_created_vertices;
  typed_created_vertices.reserve(created_vertices_.size());
  std::transform(std::begin(created_vertices_), std::end(created_vertices_), std::back_inserter(typed_created_vertices),
                 [](const auto &accessor) { return TypedValue(accessor); });
  typed_values.emplace(kCreatedVertices, std::move(typed_created_vertices));

  return typed_values;
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
}
}  // namespace query
