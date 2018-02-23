#include <distributed/plan_dispatcher.hpp>

namespace distributed {

PlanDispatcher::PlanDispatcher(Coordination &coordination)
    : clients_(coordination) {}

void PlanDispatcher::DispatchPlan(
    int64_t plan_id, std::shared_ptr<query::plan::LogicalOperator> plan,
    const SymbolTable &symbol_table) {
  auto futures = clients_.ExecuteOnWorkers<void>(
      0, [plan_id, plan,
          symbol_table](communication::rpc::ClientPool &client_pool) {
        auto result =
            client_pool.Call<DistributedPlanRpc>(plan_id, plan, symbol_table);
        CHECK(result) << "Failed to dispatch plan to worker";
      });

  for (auto &future : futures) {
    future.wait();
  }
}

}  // namespace distributed
