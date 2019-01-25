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

  Results operator()(const std::string &, database::GraphDbAccessor &,
                     const std::map<std::string, PropertyValue> &,
                     bool in_explicit_transaction) override;

 private:
  std::unique_ptr<LogicalPlan> MakeLogicalPlan(
      CypherQuery *, AstStorage, const Parameters &,
      database::GraphDbAccessor *) override;

  void PrettyPrintPlan(const database::GraphDbAccessor &,
                       const plan::LogicalOperator *, std::ostream *) override;

  std::string PlanToJson(const database::GraphDbAccessor &,
                         const plan::LogicalOperator *) override;

  std::atomic<int64_t> next_plan_id_{0};
  distributed::PlanDispatcher *plan_dispatcher_{nullptr};
};

}  // namespace query
