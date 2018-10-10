#pragma once

#include "query/interpreter.hpp"

namespace database {
class Master;
}

namespace distributed {
class PlanDispatcher;
}

namespace query {

class DistributedInterpreter final : public Interpreter {
 public:
  DistributedInterpreter(database::Master *db);

 private:
  std::unique_ptr<LogicalPlan> MakeLogicalPlan(Query *, AstStorage,
                                               Context *) override;

  std::atomic<int64_t> next_plan_id_{0};
  distributed::PlanDispatcher *plan_dispatcher_{nullptr};
};

}  // namespace query
