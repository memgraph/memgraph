#include "query/plan/distributed.hpp"

#include <memory>

// TODO: Remove these includes for hacked cloning of logical operators via boost
// serialization when proper cloning is added.
#include <sstream>
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"

#include "query/plan/operator.hpp"
#include "utils/exceptions.hpp"

namespace query::plan {

namespace {

std::pair<std::unique_ptr<LogicalOperator>, AstTreeStorage> Clone(
    const LogicalOperator &original_plan) {
  // TODO: Add a proper Clone method to LogicalOperator
  std::stringstream stream;
  {
    boost::archive::binary_oarchive out_archive(stream);
    out_archive << &original_plan;
  }
  boost::archive::binary_iarchive in_archive(stream);
  LogicalOperator *plan_copy = nullptr;
  in_archive >> plan_copy;
  return {std::unique_ptr<LogicalOperator>(plan_copy),
          std::move(in_archive.template get_helper<AstTreeStorage>(
              AstTreeStorage::kHelperId))};
}

class DistributedPlanner : public HierarchicalLogicalOperatorVisitor {
 public:
  DistributedPlanner(DistributedPlan &distributed_plan)
      : distributed_plan_(distributed_plan) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;

  // ScanAll are all done on each machine locally.
  bool PreVisit(ScanAll &) override { return true; }
  bool PostVisit(ScanAll &) override {
    RaiseIfCartesian();
    RaiseIfHasWorkerPlan();
    has_scan_all_ = true;
    return true;
  }
  bool PreVisit(ScanAllByLabel &) override { return true; }
  bool PostVisit(ScanAllByLabel &) override {
    RaiseIfCartesian();
    RaiseIfHasWorkerPlan();
    has_scan_all_ = true;
    return true;
  }
  bool PreVisit(ScanAllByLabelPropertyRange &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyRange &) override {
    RaiseIfCartesian();
    RaiseIfHasWorkerPlan();
    has_scan_all_ = true;
    return true;
  }
  bool PreVisit(ScanAllByLabelPropertyValue &) override { return true; }
  bool PostVisit(ScanAllByLabelPropertyValue &) override {
    RaiseIfCartesian();
    RaiseIfHasWorkerPlan();
    has_scan_all_ = true;
    return true;
  }

  // Expand is done locally on each machine with RPC calls for worker-boundary
  // crossing edges.
  bool PreVisit(Expand &) override { return true; }
  // TODO: ExpandVariable

  // The following operators filter the frame or put something on it. They
  // should be worker local.
  bool PreVisit(ConstructNamedPath &) override { return true; }
  bool PreVisit(Filter &) override { return true; }
  bool PreVisit(ExpandUniquenessFilter<VertexAccessor> &) override {
    return true;
  }
  bool PreVisit(ExpandUniquenessFilter<EdgeAccessor> &) override {
    return true;
  }
  bool PreVisit(Optional &) override { return true; }

  // Skip needs to skip only the first N results from *all* of the results.
  // Therefore, the earliest (deepest in the plan tree) encountered Skip will
  // break the plan in 2 parts.
  //  1) Master plan with Skip and everything above it.
  //  2) Worker plan with operators below Skip, but without Skip itself.
  bool PreVisit(Skip &) override { return true; }
  bool PostVisit(Skip &skip) override {
    if (!distributed_plan_.worker_plan) {
      auto input = skip.input();
      distributed_plan_.worker_plan = input;
      skip.set_input(std::make_shared<PullRemote>(
          input, distributed_plan_.plan_id,
          input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // Limit, like Skip, needs to see *all* of the results, so we split the plan.
  // Unlike Skip, we can also do the operator locally on each machine. This may
  // improve the execution speed of workers. So, the 2 parts of the plan are:
  //  1) Master plan with Limit and everything above.
  //  2) Worker plan with operators below Limit, but including Limit itself.
  bool PreVisit(Limit &) override { return true; }
  bool PostVisit(Limit &limit) override {
    if (!distributed_plan_.worker_plan) {
      // Shallow copy Limit
      distributed_plan_.worker_plan = std::make_shared<Limit>(limit);
      auto input = limit.input();
      limit.set_input(std::make_shared<PullRemote>(
          input, distributed_plan_.plan_id,
          input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // OrderBy is an associative operator, this means we can do ordering
  // on workers and then merge the results on master. This requires a more
  // involved solution, so for now treat OrderBy just like Split.
  bool PreVisit(OrderBy &) override { return true; }
  bool PostVisit(OrderBy &order_by) override {
    // TODO: Associative combination of OrderBy
    if (!distributed_plan_.worker_plan) {
      auto input = order_by.input();
      distributed_plan_.worker_plan = input;
      order_by.set_input(std::make_shared<PullRemote>(
          input, distributed_plan_.plan_id,
          input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // Treat Distinct just like Limit.
  bool PreVisit(Distinct &) override { return true; }
  bool PostVisit(Distinct &distinct) override {
    if (!distributed_plan_.worker_plan) {
      // Shallow copy Distinct
      distributed_plan_.worker_plan = std::make_shared<Distinct>(distinct);
      auto input = distinct.input();
      distinct.set_input(std::make_shared<PullRemote>(
          input, distributed_plan_.plan_id,
          input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // TODO: Union

  // For purposes of distribution, aggregation comes in 2 flavors:
  //  * associative and
  //  * non-associative.
  //
  // Associative aggregation can be done locally on workers, and then the
  // results merged on master. Similarly to how OrderBy can be distributed. For
  // this type of aggregation, master will need to have an aggregation merging
  // operator. This need not be a new LogicalOperator, it can be a new
  // Aggregation with different Expressions.
  //
  // Non-associative aggregation needs to see all of the results and is
  // completely done on master.
  bool PreVisit(Aggregate &) override { return true; }
  bool PostVisit(Aggregate &aggr_op) override {
    if (distributed_plan_.worker_plan) {
      // We have already split the plan, so the aggregation we are visiting is
      // on master.
      return true;
    }
    auto is_associative = [&aggr_op]() {
      for (const auto &aggr : aggr_op.aggregations()) {
        switch (aggr.op) {
          case Aggregation::Op::COUNT:
          case Aggregation::Op::MIN:
          case Aggregation::Op::MAX:
          case Aggregation::Op::SUM:
            break;
          default:
            return false;
        }
      }
      return true;
    };
    if (!is_associative()) {
      auto input = aggr_op.input();
      distributed_plan_.worker_plan = input;
      aggr_op.set_input(std::make_shared<PullRemote>(
          input, distributed_plan_.plan_id,
          input->OutputSymbols(distributed_plan_.symbol_table)));
      return true;
    }
    // Aggregate uses associative operation(s), so split the work across master
    // and workers.
    auto make_merge_aggregation = [this](auto op, const auto &worker_sym) {
      auto *worker_ident =
          distributed_plan_.ast_storage.Create<Identifier>(worker_sym.name());
      distributed_plan_.symbol_table[*worker_ident] = worker_sym;
      auto merge_name =
          Aggregation::OpToString(op) + std::to_string(worker_ident->uid());
      auto merge_sym = distributed_plan_.symbol_table.CreateSymbol(
          merge_name, false, Symbol::Type::Number);
      return Aggregate::Element{worker_ident, nullptr, op, merge_sym};
    };
    std::vector<Aggregate::Element> master_aggrs;
    master_aggrs.reserve(aggr_op.aggregations().size());
    for (const auto &aggr : aggr_op.aggregations()) {
      switch (aggr.op) {
        // Count, like sum, only needs to sum all of the results on master.
        case Aggregation::Op::COUNT:
        case Aggregation::Op::SUM:
          master_aggrs.emplace_back(
              make_merge_aggregation(Aggregation::Op::SUM, aggr.output_sym));
          break;
        case Aggregation::Op::MIN:
        case Aggregation::Op::MAX:
          master_aggrs.emplace_back(
              make_merge_aggregation(aggr.op, aggr.output_sym));
          break;
        default:
          throw utils::NotYetImplemented("distributed planning");
      }
    }
    // Rewiring is done in PostVisit(Produce), so just store our results.
    master_aggrs_ = master_aggrs;
    return true;
  }

  bool PreVisit(Produce &) override { return true; }
  bool PostVisit(Produce &produce) override {
    if (master_aggrs_.empty()) return true;
    // We have to rewire master/worker aggregation.
    DCHECK(!distributed_plan_.worker_plan);
    DCHECK(std::dynamic_pointer_cast<Aggregate>(produce.input()));
    auto aggr_op = std::static_pointer_cast<Aggregate>(produce.input());
    std::vector<Symbol> pull_symbols;
    pull_symbols.reserve(aggr_op->aggregations().size() +
                         aggr_op->remember().size());
    for (const auto &aggr : aggr_op->aggregations())
      pull_symbols.push_back(aggr.output_sym);
    for (const auto &sym : aggr_op->remember()) pull_symbols.push_back(sym);
    distributed_plan_.worker_plan = aggr_op;
    auto pull_op = std::make_shared<PullRemote>(
        aggr_op, distributed_plan_.plan_id, pull_symbols);
    auto master_aggr_op = std::make_shared<Aggregate>(
        pull_op, master_aggrs_, aggr_op->group_by(), aggr_op->remember());
    // Create a Produce operator which only moves the final results from new
    // symbols into old aggregation symbols, because expressions following the
    // aggregation expect the result in old symbols.
    std::vector<NamedExpression *> produce_exprs;
    produce_exprs.reserve(aggr_op->aggregations().size());
    for (int i = 0; i < aggr_op->aggregations().size(); ++i) {
      const auto &merge_result_sym = master_aggrs_[i].output_sym;
      const auto &original_result_sym = aggr_op->aggregations()[i].output_sym;
      auto *ident = distributed_plan_.ast_storage.Create<Identifier>(
          merge_result_sym.name());
      distributed_plan_.symbol_table[*ident] = merge_result_sym;
      auto *nexpr = distributed_plan_.ast_storage.Create<NamedExpression>(
          original_result_sym.name(), ident);
      distributed_plan_.symbol_table[*nexpr] = original_result_sym;
      produce_exprs.emplace_back(nexpr);
    }
    // Wire our master Produce into Produce + Aggregate
    produce.set_input(std::make_shared<Produce>(master_aggr_op, produce_exprs));
    master_aggrs_.clear();
    return true;
  }

  bool Visit(Once &) override { return true; }

  bool Visit(CreateIndex &) override {
    throw utils::NotYetImplemented("distributed planning");
  }

  // TODO: Write operators, accumulate and unwind

 protected:
  bool DefaultPreVisit() override {
    throw utils::NotYetImplemented("distributed planning");
  }

 private:
  DistributedPlan &distributed_plan_;
  // Used for rewiring the master/worker aggregation in PostVisit(Produce)
  std::vector<Aggregate::Element> master_aggrs_;
  bool has_scan_all_ = false;

  void RaiseIfCartesian() {
    if (has_scan_all_)
      throw utils::NotYetImplemented("Cartesian product distributed planning");
  }

  void RaiseIfHasWorkerPlan() {
    if (distributed_plan_.worker_plan)
      throw utils::NotYetImplemented("distributed planning");
  }
};

}  // namespace

DistributedPlan MakeDistributedPlan(const LogicalOperator &original_plan,
                                    const SymbolTable &symbol_table,
                                    std::atomic<int64_t> &next_plan_id) {
  DistributedPlan distributed_plan;
  // If we will generate multiple worker plans, we will need to increment the
  // next_plan_id for each one.
  distributed_plan.plan_id = next_plan_id++;
  distributed_plan.symbol_table = symbol_table;
  std::tie(distributed_plan.master_plan, distributed_plan.ast_storage) =
      Clone(original_plan);
  DistributedPlanner planner(distributed_plan);
  distributed_plan.master_plan->Accept(planner);
  if (!distributed_plan.worker_plan) {
    // We haven't split the plan, this means that it should be the same on
    // master and worker. We only need to prepend PullRemote to master plan.
    distributed_plan.worker_plan = std::move(distributed_plan.master_plan);
    distributed_plan.master_plan = std::make_unique<PullRemote>(
        distributed_plan.worker_plan, distributed_plan.plan_id,
        distributed_plan.worker_plan->OutputSymbols(
            distributed_plan.symbol_table));
  }
  return distributed_plan;
}

}  // namespace query::plan
