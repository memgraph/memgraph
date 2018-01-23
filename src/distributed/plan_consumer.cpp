#include "distributed/plan_consumer.hpp"

namespace distributed {

PlanConsumer::PlanConsumer(communication::messaging::System &system)
    : server_(system, kDistributedPlanServerName) {
  server_.Register<DistributedPlanRpc>([this](const DispatchPlanReq &req) {
    plan_cache_.access().insert(req.plan_id_,
                                std::make_pair(req.plan_, req.symbol_table_));
    return std::make_unique<ConsumePlanRes>(true);
  });
}

std::pair<std::shared_ptr<query::plan::LogicalOperator>, SymbolTable>
PlanConsumer::PlanForId(int64_t plan_id) {
  auto accessor = plan_cache_.access();
  auto found = accessor.find(plan_id);
  CHECK(found != accessor.end())
      << "Missing plan and symbol table for plan id!";
  return found->second;
}

}  // namespace distributed
