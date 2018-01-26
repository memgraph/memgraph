#pragma once

#include "distributed/coordination.hpp"
#include "distributed/plan_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace distributed {

/** Handles plan dispatching to all workers. Uses MasterCoordination to
 * acomplish that. Master side.
 */
class PlanDispatcher {
 public:
  explicit PlanDispatcher(Coordination &coordination);

  /**
   * Synchronously dispatch a plan to all workers and wait for their
   * acknowledgement.
   */
  void DispatchPlan(int64_t plan_id,
                    std::shared_ptr<query::plan::LogicalOperator> plan,
                    const SymbolTable &symbol_table);

 private:
  RpcWorkerClients clients_;
};

}  // namespace distributed
