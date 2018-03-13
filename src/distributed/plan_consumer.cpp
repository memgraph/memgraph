#include "distributed/plan_consumer.hpp"

namespace distributed {

PlanConsumer::PlanConsumer(communication::rpc::Server &server)
    : server_(server) {
  server_.Register<DistributedPlanRpc>([this](const DispatchPlanReq &req) {
    plan_cache_.access().insert(
        req.plan_id_,
        std::make_unique<PlanPack>(
            req.plan_, req.symbol_table_,
            std::move(const_cast<DispatchPlanReq &>(req).storage_)));
    return std::make_unique<DispatchPlanRes>();
  });

  server_.Register<RemovePlanRpc>([this](const RemovePlanReq &req) {
    plan_cache_.access().remove(req.member);
    return std::make_unique<RemovePlanRes>();
  });
}

PlanConsumer::PlanPack &PlanConsumer::PlanForId(int64_t plan_id) const {
  auto accessor = plan_cache_.access();
  auto found = accessor.find(plan_id);
  CHECK(found != accessor.end())
      << "Missing plan and symbol table for plan id!";
  return *found->second;
}

std::vector<int64_t> PlanConsumer::CachedPlanIds() const {
  std::vector<int64_t> plan_ids;
  auto access = plan_cache_.access();
  plan_ids.reserve(access.size());
  for (auto &kv : access) plan_ids.emplace_back(kv.first);

  return plan_ids;
}

}  // namespace distributed
