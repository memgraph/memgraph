#include "query/distributed_interpreter.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/plan/distributed.hpp"
#include "query/plan/distributed_pretty_print.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/rule_based_planner.hpp"
#include "query/plan/vertex_count_cache.hpp"

namespace query {

namespace {

class DistributedLogicalPlan final : public LogicalPlan {
 public:
  DistributedLogicalPlan(plan::DistributedPlan plan, double cost,
                         distributed::PlanDispatcher *plan_dispatcher)
      : plan_(std::move(plan)), plan_dispatcher_(plan_dispatcher), cost_(cost) {
    CHECK(plan_dispatcher_);
    for (const auto &plan_pair : plan_.worker_plans) {
      const auto &plan_id = plan_pair.first;
      const auto &worker_plan = plan_pair.second;
      plan_dispatcher_->DispatchPlan(plan_id, worker_plan, plan_.symbol_table);
    }
  }

  ~DistributedLogicalPlan() {
    for (const auto &plan_pair : plan_.worker_plans) {
      const auto &plan_id = plan_pair.first;
      try {
        plan_dispatcher_->RemovePlan(plan_id);
      } catch (const communication::rpc::RpcFailedException &) {
        // We ignore RPC exceptions here because the other side can be possibly
        // shutting down. TODO: If that is not the case then something is really
        // wrong with the cluster!
      }
    }
  }

  const plan::LogicalOperator &GetRoot() const override {
    return *plan_.master_plan;
  }
  double GetCost() const override { return cost_; }
  const SymbolTable &GetSymbolTable() const override {
    return plan_.symbol_table;
  }
  const AstStorage &GetAstStorage() const override {
    return plan_.ast_storage;
  }

 private:
  plan::DistributedPlan plan_;
  distributed::PlanDispatcher *plan_dispatcher_{nullptr};
  double cost_;
};

class DistributedPostProcessor final {
  // Original plan before rewrite, needed only for temporary cost estimation
  // implementation.
  std::unique_ptr<plan::LogicalOperator> original_plan_;
  std::atomic<int64_t> *next_plan_id_;
  Parameters parameters_;

 public:
  using ProcessedPlan = plan::DistributedPlan;

  DistributedPostProcessor(const Parameters &parameters,
                           std::atomic<int64_t> *next_plan_id)
      : next_plan_id_(next_plan_id), parameters_(parameters) {}

  template <class TPlanningContext>
  plan::DistributedPlan Rewrite(std::unique_ptr<plan::LogicalOperator> plan,
                                TPlanningContext *context) {
    plan::PostProcessor post_processor(parameters_);
    original_plan_ = post_processor.Rewrite(std::move(plan), context);
    const auto &property_names = context->ast_storage->properties_;
    std::vector<storage::Property> properties_by_ix;
    properties_by_ix.reserve(property_names.size());
    for (const auto &name : property_names) {
      properties_by_ix.push_back(context->db->Property(name));
    }
    return MakeDistributedPlan(*context->ast_storage, *original_plan_,
                               *context->symbol_table, *next_plan_id_,
                               properties_by_ix);
  }

  template <class TVertexCounts>
  double EstimatePlanCost(const plan::DistributedPlan &plan,
                          TVertexCounts *vertex_counts) {
    // TODO: Make cost estimation work with distributed plan.
    return ::query::plan::EstimatePlanCost(vertex_counts, parameters_,
                                           *original_plan_);
  }

  template <class TPlanningContext>
  plan::DistributedPlan MergeWithCombinator(plan::DistributedPlan curr_plan,
                                            plan::DistributedPlan last_plan,
                                            const Tree &combinator,
                                            TPlanningContext *context) {
    throw utils::NotYetImplemented("query combinator");
  }

  template <class TPlanningContext>
  plan::DistributedPlan MakeDistinct(plan::DistributedPlan last_op,
                                     TPlanningContext *context) {
    throw utils::NotYetImplemented("query combinator");
  }
};

}  // namespace

DistributedInterpreter::DistributedInterpreter(database::Master *db)
    : plan_dispatcher_(&db->plan_dispatcher()) {}

std::unique_ptr<LogicalPlan> DistributedInterpreter::MakeLogicalPlan(
    CypherQuery *query, AstStorage ast_storage, const Parameters &parameters,
    database::GraphDbAccessor *db_accessor) {
  auto vertex_counts = plan::MakeVertexCountCache(db_accessor);
  auto symbol_table = MakeSymbolTable(query);
  auto planning_context = plan::MakePlanningContext(&ast_storage, &symbol_table,
                                                    query, &vertex_counts);
  DistributedPostProcessor distributed_post_processor(parameters,
                                                      &next_plan_id_);
  plan::DistributedPlan plan;
  double cost;
  std::tie(plan, cost) = plan::MakeLogicalPlan(
      &planning_context, &distributed_post_processor, FLAGS_query_cost_planner);
  VLOG(10) << "[Interpreter] Created plan for distributed execution "
           << next_plan_id_ - 1;
  return std::make_unique<DistributedLogicalPlan>(std::move(plan), cost,
                                                  plan_dispatcher_);
}

Interpreter::Results DistributedInterpreter::operator()(
    const std::string &query_string, database::GraphDbAccessor &db_accessor,
    const std::map<std::string, PropertyValue> &params,
    bool in_explicit_transaction) {
  AstStorage ast_storage;
  Parameters parameters;

  auto queries = StripAndParseQuery(query_string, &parameters, &ast_storage,
                                    &db_accessor, params);
  ParsedQuery &parsed_query = queries.second;

  if (utils::IsSubtype(*parsed_query.query, ProfileQuery::kType)) {
    throw utils::NotYetImplemented("PROFILE in a distributed query");
  }

  return Interpreter::operator()(query_string, db_accessor, params,
                                 in_explicit_transaction);
}

void DistributedInterpreter::PrettyPrintPlan(
    const database::GraphDbAccessor &dba,
    const plan::LogicalOperator *plan_root, std::ostream *out) {
  plan::DistributedPrettyPrint(dba, plan_root, out);
}

std::string DistributedInterpreter::PlanToJson(
    const database::GraphDbAccessor &dba,
    const plan::LogicalOperator *plan_root) {
  return plan::DistributedPlanToJson(dba, plan_root).dump();
}

}  // namespace query
