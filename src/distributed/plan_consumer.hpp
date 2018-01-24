#pragma once

#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/plan_rpc_messages.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace distributed {

/** Handles plan consumption from master. Creates and holds a local cache of
 * plans. Worker side.
 */
class PlanConsumer {
 public:
  explicit PlanConsumer(communication::rpc::System &system);

  /**
   * Return cached plan and symbol table for a given plan id.
   */
  std::pair<std::shared_ptr<query::plan::LogicalOperator>, SymbolTable>
  PlanForId(int64_t plan_id);

 private:
  communication::rpc::Server server_;
  mutable ConcurrentMap<
      int64_t,
      std::pair<std::shared_ptr<query::plan::LogicalOperator>, SymbolTable>>
      plan_cache_;
};

}  // namespace distributed
