#include <distributed/plan_dispatcher.hpp>

namespace distributed {

PlanDispatcher::PlanDispatcher(Coordination *coordination) : coordination_(coordination) {}

void PlanDispatcher::DispatchPlan(
    int64_t plan_id, std::shared_ptr<query::plan::LogicalOperator> plan,
    const query::SymbolTable &symbol_table) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      0, [plan_id, plan, symbol_table](
             int worker_id, communication::rpc::ClientPool &client_pool) {
        client_pool.Call<DispatchPlanRpc>(plan_id, plan, symbol_table);
      });

  for (auto &future : futures) {
    future.get();
  }
}

void PlanDispatcher::RemovePlan(int64_t plan_id) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      0, [plan_id](int worker_id, communication::rpc::ClientPool &client_pool) {
        client_pool.Call<RemovePlanRpc>(plan_id);
      });

  for (auto &future : futures) {
    future.get();
  }
}

}  // namespace distributed
