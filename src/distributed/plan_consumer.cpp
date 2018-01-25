#include "distributed/plan_consumer.hpp"

namespace distributed {

PlanConsumer::PlanConsumer(communication::rpc::System &system)
    : server_(system, kDistributedPlanServerName) {
  server_.Register<DistributedPlanRpc>([this](const DispatchPlanReq &req) {
    plan_cache_.access().insert(
        req.plan_id_,
        std::make_unique<PlanPack>(
            req.plan_, req.symbol_table_,
            std::move(const_cast<DispatchPlanReq &>(req).storage_)));
    return std::make_unique<ConsumePlanRes>(true);
  });
}

PlanConsumer::PlanPack &PlanConsumer::PlanForId(int64_t plan_id) const {
  auto accessor = plan_cache_.access();
  auto found = accessor.find(plan_id);
  CHECK(found != accessor.end())
      << "Missing plan and symbol table for plan id!";
  return *found->second;
}

}  // namespace distributed
