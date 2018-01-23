#include <distributed/plan_dispatcher.hpp>

namespace distributed {

PlanDispatcher::PlanDispatcher(communication::messaging::System &system,
                               Coordination &coordination)
    : clients_(system, coordination, kDistributedPlanServerName) {}

void PlanDispatcher::DispatchPlan(
    int64_t plan_id, std::shared_ptr<query::plan::LogicalOperator> plan,
    SymbolTable &symbol_table) {
  auto futures = clients_.ExecuteOnWorkers<void>(
      0, [plan_id, &plan, &symbol_table](communication::rpc::Client &client) {
        auto result =
            client.Call<DistributedPlanRpc>(300ms, plan_id, plan, symbol_table);
        CHECK(result) << "Failed to dispatch plan to worker";
      });
}

}  // namespace distributed
