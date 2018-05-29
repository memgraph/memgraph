#include <distributed/plan_dispatcher.hpp>

namespace distributed {

PlanDispatcher::PlanDispatcher(RpcWorkerClients &clients) : clients_(clients) {}

void PlanDispatcher::DispatchPlan(
    int64_t plan_id, std::shared_ptr<query::plan::LogicalOperator> plan,
    const SymbolTable &symbol_table) {
  auto futures = clients_.ExecuteOnWorkers<void>(
      0, [plan_id, plan, symbol_table](
             int worker_id, communication::rpc::ClientPool &client_pool) {
        auto result =
            client_pool.Call<DistributedPlanRpc>(plan_id, plan, symbol_table);
        CHECK(result) << "DistributedPlanRpc failed";
      });

  for (auto &future : futures) {
    future.wait();
  }
}

void PlanDispatcher::RemovePlan(int64_t plan_id) {
  auto futures = clients_.ExecuteOnWorkers<void>(
      0, [plan_id](int worker_id, communication::rpc::ClientPool &client_pool) {
        auto result = client_pool.Call<RemovePlanRpc>(plan_id);
        CHECK(result) << "Failed to remove plan from worker";
      });

  for (auto &future : futures) {
    future.wait();
  }
}

}  // namespace distributed
