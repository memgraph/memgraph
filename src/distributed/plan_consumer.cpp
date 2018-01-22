#include "distributed/plan_consumer.hpp"

namespace distributed {

PlanConsumer::PlanConsumer(communication::messaging::System &system)
    : server_(system, kDistributedPlanServerName) {
  // TODO
}

pair<std::shared_ptr<query::plan::LogicalOperator>, SymbolTable>
PlanConsumer::PlanForId(int64_t plan_id) {
  auto accessor = plan_cache_.access();
  auto found = accessor.find(plan_id);
  CHECK(found != accessor.end())
      << "Missing plan and symbol table for plan id!";
  return found->second;
}

bool PlanConsumer::ConsumePlan(int64_t,
                               std::shared_ptr<query::plan::LogicalOperator>,
                               SymbolTable) {
  // TODO
  return false;
}

}  // namespace distributed
