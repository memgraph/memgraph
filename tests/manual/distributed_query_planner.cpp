#include "interactive_planning.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/distributed/distributed_graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "query/plan/distributed.hpp"
#include "query/plan/distributed_pretty_print.hpp"

DECLARE_int32(min_log_level);

DEFCOMMAND(ShowDistributed) {
  int64_t plan_ix = 0;
  std::stringstream ss(args[0]);
  ss >> plan_ix;
  if (ss.fail() || !ss.eof() || plan_ix >= plans.size()) return;
  const auto &plan = plans[plan_ix].first;
  std::atomic<int64_t> plan_id{0};
  std::vector<storage::Property> properties_by_ix =
      query::NamesToProperties(ast_storage.properties_, &dba);
  auto distributed_plan = MakeDistributedPlan(ast_storage, *plan, symbol_table,
                                              plan_id, properties_by_ix);
  {
    std::cout << "---- Master Plan ---- " << std::endl;
    query::plan::DistributedPrettyPrint(dba,
                                        distributed_plan.master_plan.get());
    std::cout << std::endl;
  }
  for (size_t i = 0; i < distributed_plan.worker_plans.size(); ++i) {
    int64_t id;
    std::shared_ptr<query::plan::LogicalOperator> worker_plan;
    std::tie(id, worker_plan) = distributed_plan.worker_plans[i];
    std::cout << "---- Worker Plan #" << id << " ---- " << std::endl;
    query::plan::DistributedPrettyPrint(dba, worker_plan.get());
    std::cout << std::endl;
  }
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);
  AddCommand("show-distributed",
             {ShowDistributedCommand, 1,
              "Show the Nth plan as for distributed execution"});
  database::Master db;
  db.Start();
  auto dba = db.Access();
  RunInteractivePlanning(dba.get());
  db.Shutdown();
  db.AwaitShutdown();
  return 0;
}
