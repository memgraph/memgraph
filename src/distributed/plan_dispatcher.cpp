#include <distributed/plan_dispatcher.hpp>

namespace distributed {

PlanDispatcher::PlanDispatcher(communication::messaging::System &system,
                               Coordination &coordination)
    : clients_(system, coordination, kDistributedPlanServerName) {}

void PlanDispatcher::DispatchPlan(int64_t,
                                  std::shared_ptr<query::plan::LogicalOperator>,
                                  SymbolTable &) {
  // TODO
  // NOTE: skip id 0 from clients_, it's the master id
}

}  // namespace distributed
