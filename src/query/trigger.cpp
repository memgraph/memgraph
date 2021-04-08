#include "query/trigger.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "utils/memory.hpp"

namespace query {
Trigger::Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *cache,
                 utils::SpinLock *antlr_lock)
    : name_(std::move(name)),
      parsed_statements_{ParseQuery(query, {} /* this should contain the predefined parameters */, cache, antlr_lock)} {
}

void Trigger::Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
                      utils::MonotonicBufferResource *execution_memory, const double tsc_frequency,
                      std::atomic<bool> *is_shutting_down) const {
  AstStorage ast_storage;
  ast_storage.properties_ = parsed_statements_.ast_storage.properties_;
  ast_storage.labels_ = parsed_statements_.ast_storage.labels_;
  ast_storage.edge_types_ = parsed_statements_.ast_storage.edge_types_;

  auto plan = CypherQueryToPlan(parsed_statements_.stripped_query.hash(), std::move(ast_storage),
                                utils::Downcast<CypherQuery>(parsed_statements_.query), parsed_statements_.parameters,
                                plan_cache, dba, parsed_statements_.is_cacheable);
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
  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
}
}  // namespace query
