#include "distributed/plan_consumer.hpp"

namespace distributed {

PlanConsumer::PlanConsumer(distributed::Coordination *coordination) {
  coordination->Register<DispatchPlanRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        DispatchPlanReq req;
        Load(&req, req_reader);
        plan_cache_.access().insert(
            req.plan_id, std::make_unique<PlanPack>(req.plan, req.symbol_table,
                                                    std::move(req.storage)));
        DispatchPlanRes res;
        Save(res, res_builder);
      });

  coordination->Register<RemovePlanRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        plan_cache_.access().remove(req_reader.getMember());
      });
}

PlanConsumer::PlanPack &PlanConsumer::PlanForId(int64_t plan_id) const {
  auto accessor = plan_cache_.access();
  auto found = accessor.find(plan_id);
  CHECK(found != accessor.end())
      << "Missing plan and symbol table for plan id: " << plan_id;
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
