#include "query/plan/distributed.hpp"

#include <memory>

// TODO: Remove these includes for hacked cloning of logical operators via boost
// serialization when proper cloning is added.
#include <sstream>
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"

#include "query/plan/operator.hpp"
#include "query/plan/preprocess.hpp"
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

int64_t AddWorkerPlan(DistributedPlan &distributed_plan,
                      std::atomic<int64_t> &next_plan_id,
                      const std::shared_ptr<LogicalOperator> &worker_plan) {
  int64_t plan_id = next_plan_id++;
  distributed_plan.worker_plans.emplace_back(plan_id, worker_plan);
  return plan_id;
}

class DistributedPlanner : public HierarchicalLogicalOperatorVisitor {
 public:
  DistributedPlanner(DistributedPlan &distributed_plan,
                     std::atomic<int64_t> &next_plan_id)
      : distributed_plan_(distributed_plan), next_plan_id_(next_plan_id) {}

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;

  // Returns true if the plan should be run on master and workers. Note, that
  // false is returned if the plan is already split.
  bool ShouldSplit() const { return should_split_; }

  bool NeedsSynchronize() const { return needs_synchronize_; }

  // ScanAll are all done on each machine locally.
  // We need special care when multiple ScanAll operators appear, this means we
  // need a Cartesian product. Both the left and the right side of Cartesian
  // should be sent as standalone plans to each worker. Then, the master
  // execution should use PullRemote to wire them into Cartesian.  In case of
  // multiple Cartesians, we send each ScanAll part to workers and chain them
  // into multiple Cartesians on master.
  //
  // For example, `ScanAll(n) > ScanAll(m) > ScanAll(l)` is transformed to:
  //
  //    workers       |   master
  //
  //  * ScanAll(n) - - - - - - - - - \
  //                                  + PullRemote(n) \
  //  * ScanAll(m) \                                   + Cartesian
  //                +- PullRemote(m) \                /
  //                                  + Cartesian - -
  //                +- PullRemote(n) /
  //  * ScanAll(l) /
  //
  // Things get more complicated if any branch of the Cartesian has a Filter
  // operator which depends on the result from another branch.
  //
  // For example, `ScanAll (n) > ScanAll (m) > Filter (m.prop = n.prop)`:
  //
  //              workers                         |    master
  //
  //  * ScanAll(n) - - - - - - - - - - - - - -  \
  //                                             + PullRemote(n) \
  //                                                              + Cartesian
  //                                             + PullRemote(m) /
  //  * ScanAll(m) - - Filter (m.prop = n.prop) /
  //
  // Since the Filter depends on the first ScanAll branch, we can either:
  //  * enforce the first branch is evaluated before and data sent back to
  //    workers to evaluate the second; or
  //  * move the Filter after the Cartesian (or maybe inline it inside).
  //
  // Inserting the Cartesian operator is done through PlanCartesian while
  // post-visiting Produce, Aggregate or write operators.
  //
  // TODO: Consider planning Cartesian during regular planning in
  // RuleBasedPlanner.
  // TODO: Finish Cartesian planning:
  //  * checking Filters can be used;
  //  * checking ExpandUniquenessFilter can be used;
  //  * checking indexed ScanAll can be used;
  //  * checking Expand into existing can be used;
  //  * allowing Cartesian after Produce (i.e. after WITH clause);
  //  * ...
  std::shared_ptr<Cartesian> PlanCartesian(
      const std::shared_ptr<LogicalOperator> &rhs_input) {
    std::shared_ptr<Cartesian> cartesian;
    auto pull_id = AddWorkerPlan(rhs_input);
    std::shared_ptr<LogicalOperator> right_op = std::make_shared<PullRemote>(
        rhs_input, pull_id,
        rhs_input->ModifiedSymbols(distributed_plan_.symbol_table));
    // We use this ordering of operators, so that left hand side can be
    // accumulated without having whole product accumulations. This (obviously)
    // relies on the fact that Cartesian accumulation strategy accumulates the
    // left operator input.
    while (!left_cartesians_.empty()) {
      auto left_op = left_cartesians_.back();
      left_cartesians_.pop_back();
      cartesian = std::make_shared<Cartesian>(
          left_op, left_op->ModifiedSymbols(distributed_plan_.symbol_table),
          right_op, right_op->ModifiedSymbols(distributed_plan_.symbol_table));
      right_op = cartesian;
    }
    return cartesian;
  };

  bool PreVisit(ScanAll &scan) override {
    prev_ops_.push_back(&scan);
    return true;
  }
  bool PostVisit(ScanAll &scan) override {
    prev_ops_.pop_back();
    should_split_ = true;
    if (has_scan_all_) {
      // Prepare for Cartesian planning
      auto id = AddWorkerPlan(scan.input());
      auto left_op = std::make_shared<PullRemote>(
          scan.input(), id,
          scan.input()->ModifiedSymbols(distributed_plan_.symbol_table));
      left_cartesians_.push_back(left_op);
      scan.set_input(std::make_shared<Once>());
    }
    has_scan_all_ = true;
    return true;
  }

  bool PreVisit(ScanAllByLabel &scan) override {
    prev_ops_.push_back(&scan);
    return true;
  }
  bool PostVisit(ScanAllByLabel &scan) override {
    prev_ops_.pop_back();
    should_split_ = true;
    if (has_scan_all_) {
      // Prepare for Cartesian planning
      auto id = AddWorkerPlan(scan.input());
      auto left_op = std::make_shared<PullRemote>(
          scan.input(), id,
          scan.input()->ModifiedSymbols(distributed_plan_.symbol_table));
      left_cartesians_.push_back(left_op);
      scan.set_input(std::make_shared<Once>());
    }
    has_scan_all_ = true;
    return true;
  }
  bool PreVisit(ScanAllByLabelPropertyRange &scan) override {
    prev_ops_.push_back(&scan);
    return true;
  }
  bool PostVisit(ScanAllByLabelPropertyRange &scan) override {
    prev_ops_.pop_back();
    should_split_ = true;
    if (has_scan_all_) {
      // Prepare for Cartesian planning
      auto id = AddWorkerPlan(scan.input());
      auto left_op = std::make_shared<PullRemote>(
          scan.input(), id,
          scan.input()->ModifiedSymbols(distributed_plan_.symbol_table));
      left_cartesians_.push_back(left_op);
      scan.set_input(std::make_shared<Once>());
    }
    has_scan_all_ = true;
    return true;
  }
  bool PreVisit(ScanAllByLabelPropertyValue &scan) override {
    prev_ops_.push_back(&scan);
    return true;
  }
  bool PostVisit(ScanAllByLabelPropertyValue &scan) override {
    prev_ops_.pop_back();
    should_split_ = true;
    if (has_scan_all_) {
      // Prepare for Cartesian planning
      auto id = AddWorkerPlan(scan.input());
      auto left_op = std::make_shared<PullRemote>(
          scan.input(), id,
          scan.input()->ModifiedSymbols(distributed_plan_.symbol_table));
      left_cartesians_.push_back(left_op);
      scan.set_input(std::make_shared<Once>());
    }
    has_scan_all_ = true;
    return true;
  }

  // Expand is done locally on each machine with RPC calls for worker-boundary
  // crossing edges.
  bool PreVisit(Expand &exp) override {
    prev_ops_.push_back(&exp);
    return true;
  }

  bool PreVisit(ExpandVariable &exp) override {
    prev_ops_.push_back(&exp);
    return true;
  }

  // The following operators filter the frame or put something on it. They
  // should be worker local.
  bool PreVisit(ConstructNamedPath &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PreVisit(Filter &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PreVisit(ExpandUniquenessFilter<VertexAccessor> &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PreVisit(ExpandUniquenessFilter<EdgeAccessor> &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PreVisit(Optional &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  // Skip needs to skip only the first N results from *all* of the results.
  // Therefore, the earliest (deepest in the plan tree) encountered Skip will
  // break the plan in 2 parts.
  //  1) Master plan with Skip and everything above it.
  //  2) Worker plan with operators below Skip, but without Skip itself.
  bool PreVisit(Skip &skip) override {
    prev_ops_.push_back(&skip);
    return true;
  }
  bool PostVisit(Skip &skip) override {
    prev_ops_.pop_back();
    if (ShouldSplit()) {
      auto input = skip.input();
      auto pull_id = AddWorkerPlan(input);
      Split(skip, std::make_shared<PullRemote>(
                      input, pull_id,
                      input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // Limit, like Skip, needs to see *all* of the results, so we split the plan.
  // Unlike Skip, we can also do the operator locally on each machine. This may
  // improve the execution speed of workers. So, the 2 parts of the plan are:
  //  1) Master plan with Limit and everything above.
  //  2) Worker plan with operators below Limit, but including Limit itself.
  bool PreVisit(Limit &limit) override {
    prev_ops_.push_back(&limit);
    return true;
  }
  bool PostVisit(Limit &limit) override {
    prev_ops_.pop_back();
    if (ShouldSplit()) {
      // Shallow copy Limit
      auto pull_id = AddWorkerPlan(std::make_shared<Limit>(limit));
      auto input = limit.input();
      Split(limit, std::make_shared<PullRemote>(
                       input, pull_id,
                       input->OutputSymbols(distributed_plan_.symbol_table)));
    }
    return true;
  }

  // OrderBy is an associative operator, this means we can do ordering
  // on workers and then merge the results on master.
  bool PreVisit(OrderBy &order_by) override {
    prev_ops_.push_back(&order_by);
    return true;
  }
  bool PostVisit(OrderBy &order_by) override {
    prev_ops_.pop_back();
    // TODO: Associative combination of OrderBy
    if (ShouldSplit()) {
      std::unordered_set<Symbol> pull_symbols(order_by.output_symbols().begin(),
                                              order_by.output_symbols().end());
      // Pull symbols need to also include those used in order by expressions.
      // For example, `RETURN n AS m ORDER BY n.prop`, output symbols will
      // contain `m`, while we also need to pull `n`.
      // TODO: Consider creating a virtual symbol for expressions like `n.prop`
      // and sending them instead. It's possible that the evaluated expression
      // requires less network traffic than sending the value of the used symbol
      // `n` itself.
      for (const auto &expr : order_by.order_by()) {
        UsedSymbolsCollector collector(distributed_plan_.symbol_table);
        expr->Accept(collector);
        pull_symbols.insert(collector.symbols_.begin(),
                            collector.symbols_.end());
      }
      // Create a copy of OrderBy but with added symbols used in expressions, so
      // that they can be pulled.
      std::vector<std::pair<Ordering, Expression *>> ordering;
      ordering.reserve(order_by.order_by().size());
      for (int i = 0; i < order_by.order_by().size(); ++i) {
        ordering.emplace_back(order_by.compare().ordering()[i],
                              order_by.order_by()[i]);
      }
      auto worker_plan = std::make_shared<OrderBy>(
          order_by.input(), ordering,
          std::vector<Symbol>(pull_symbols.begin(), pull_symbols.end()));
      auto pull_id = AddWorkerPlan(worker_plan);
      auto merge_op = std::make_unique<PullRemoteOrderBy>(
          worker_plan, pull_id, ordering,
          std::vector<Symbol>(pull_symbols.begin(), pull_symbols.end()));
      SplitOnPrevious(std::move(merge_op));
    }
    return true;
  }

  // Treat Distinct just like Limit.
  bool PreVisit(Distinct &distinct) override {
    prev_ops_.push_back(&distinct);
    return true;
  }
  bool PostVisit(Distinct &distinct) override {
    prev_ops_.pop_back();
    if (ShouldSplit()) {
      // Shallow copy Distinct
      auto pull_id = AddWorkerPlan(std::make_shared<Distinct>(distinct));
      auto input = distinct.input();
      Split(distinct,
            std::make_shared<PullRemote>(
                input, pull_id,
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
  bool PreVisit(Aggregate &aggr_op) override {
    prev_ops_.push_back(&aggr_op);
    return true;
  }
  bool PostVisit(Aggregate &aggr_op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(aggr_op, PlanCartesian(aggr_op.input()));
      return true;
    }
    if (!ShouldSplit()) {
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
          case Aggregation::Op::AVG:
            break;
          default:
            return false;
        }
      }
      return true;
    };
    if (!is_associative()) {
      auto input = aggr_op.input();
      auto pull_id = AddWorkerPlan(input);
      std::unordered_set<Symbol> pull_symbols(aggr_op.remember().begin(),
                                              aggr_op.remember().end());
      for (const auto &elem : aggr_op.aggregations()) {
        UsedSymbolsCollector collector(distributed_plan_.symbol_table);
        elem.value->Accept(collector);
        if (elem.key) elem.key->Accept(collector);
        pull_symbols.insert(collector.symbols_.begin(),
                            collector.symbols_.end());
      }
      Split(aggr_op,
            std::make_shared<PullRemote>(
                input, pull_id,
                std::vector<Symbol>(pull_symbols.begin(), pull_symbols.end())));
      return true;
    }
    auto make_ident = [this](const auto &symbol) {
      auto *ident =
          distributed_plan_.ast_storage.Create<Identifier>(symbol.name());
      distributed_plan_.symbol_table[*ident] = symbol;
      return ident;
    };
    auto make_named_expr = [&](const auto &in_sym, const auto &out_sym) {
      auto *nexpr = distributed_plan_.ast_storage.Create<NamedExpression>(
          out_sym.name(), make_ident(in_sym));
      distributed_plan_.symbol_table[*nexpr] = out_sym;
      return nexpr;
    };
    auto make_merge_aggregation = [&](auto op, const auto &worker_sym) {
      auto *worker_ident = make_ident(worker_sym);
      auto merge_name = Aggregation::OpToString(op) +
                        std::to_string(worker_ident->uid()) + "<-" +
                        worker_sym.name();
      auto merge_sym = distributed_plan_.symbol_table.CreateSymbol(
          merge_name, false, Symbol::Type::Number);
      return Aggregate::Element{worker_ident, nullptr, op, merge_sym};
    };
    // Aggregate uses associative operation(s), so split the work across master
    // and workers.
    std::vector<Aggregate::Element> master_aggrs;
    master_aggrs.reserve(aggr_op.aggregations().size());
    std::vector<Aggregate::Element> worker_aggrs;
    worker_aggrs.reserve(aggr_op.aggregations().size());
    // We will need to create a Produce operator which moves the final results
    // from new (merge) symbols into old aggregation symbols, because
    // expressions following the aggregation expect the result in old symbols.
    std::vector<NamedExpression *> produce_exprs;
    produce_exprs.reserve(aggr_op.aggregations().size());
    for (const auto &aggr : aggr_op.aggregations()) {
      switch (aggr.op) {
        // Count, like sum, only needs to sum all of the results on master.
        case Aggregation::Op::COUNT:
        case Aggregation::Op::SUM: {
          worker_aggrs.emplace_back(aggr);
          auto merge_aggr =
              make_merge_aggregation(Aggregation::Op::SUM, aggr.output_sym);
          master_aggrs.emplace_back(merge_aggr);
          produce_exprs.emplace_back(
              make_named_expr(merge_aggr.output_sym, aggr.output_sym));
          break;
        }
        case Aggregation::Op::MIN:
        case Aggregation::Op::MAX: {
          worker_aggrs.emplace_back(aggr);
          auto merge_aggr = make_merge_aggregation(aggr.op, aggr.output_sym);
          master_aggrs.emplace_back(merge_aggr);
          produce_exprs.emplace_back(
              make_named_expr(merge_aggr.output_sym, aggr.output_sym));
          break;
        }
        // AVG is split into:
        //  * workers: SUM(xpr), COUNT(expr)
        //  * master: SUM(worker_sum) / toFloat(SUM(worker_count)) AS avg
        case Aggregation::Op::AVG: {
          auto worker_sum_sym = distributed_plan_.symbol_table.CreateSymbol(
              aggr.output_sym.name() + "_SUM", false, Symbol::Type::Number);
          Aggregate::Element worker_sum{aggr.value, aggr.key,
                                        Aggregation::Op::SUM, worker_sum_sym};
          worker_aggrs.emplace_back(worker_sum);
          auto worker_count_sym = distributed_plan_.symbol_table.CreateSymbol(
              aggr.output_sym.name() + "_COUNT", false, Symbol::Type::Number);
          Aggregate::Element worker_count{
              aggr.value, aggr.key, Aggregation::Op::COUNT, worker_count_sym};
          worker_aggrs.emplace_back(worker_count);
          auto master_sum =
              make_merge_aggregation(Aggregation::Op::SUM, worker_sum_sym);
          master_aggrs.emplace_back(master_sum);
          auto master_count =
              make_merge_aggregation(Aggregation::Op::SUM, worker_count_sym);
          master_aggrs.emplace_back(master_count);
          auto *master_sum_ident = make_ident(master_sum.output_sym);
          auto *master_count_ident = make_ident(master_count.output_sym);
          auto *to_float = distributed_plan_.ast_storage.Create<Function>(
              "TOFLOAT", std::vector<Expression *>{master_count_ident});
          auto *div_expr =
              distributed_plan_.ast_storage.Create<DivisionOperator>(
                  master_sum_ident, to_float);
          auto *as_avg = distributed_plan_.ast_storage.Create<NamedExpression>(
              aggr.output_sym.name(), div_expr);
          distributed_plan_.symbol_table[*as_avg] = aggr.output_sym;
          produce_exprs.emplace_back(as_avg);
          break;
        }
        default:
          throw utils::NotYetImplemented("distributed planning");
      }
    }
    // Rewire master/worker aggregation.
    auto worker_plan = std::make_shared<Aggregate>(
        aggr_op.input(), worker_aggrs, aggr_op.group_by(), aggr_op.remember());
    auto pull_id = AddWorkerPlan(worker_plan);
    std::vector<Symbol> pull_symbols;
    pull_symbols.reserve(worker_aggrs.size() + aggr_op.remember().size());
    for (const auto &aggr : worker_aggrs)
      pull_symbols.push_back(aggr.output_sym);
    for (const auto &sym : aggr_op.remember()) pull_symbols.push_back(sym);
    auto pull_op =
        std::make_shared<PullRemote>(worker_plan, pull_id, pull_symbols);
    auto master_aggr_op = std::make_shared<Aggregate>(
        pull_op, master_aggrs, aggr_op.group_by(), aggr_op.remember());
    // Make our master Aggregate into Produce + Aggregate
    auto master_plan = std::make_unique<Produce>(master_aggr_op, produce_exprs);
    SplitOnPrevious(std::move(master_plan));
    return true;
  }

  bool PreVisit(Produce &produce) override {
    prev_ops_.push_back(&produce);
    return true;
  }
  bool PostVisit(Produce &produce) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      // TODO: It might be better to plan Cartesians later if this Produce isn't
      // the last one and is not followed by an operator which requires a merge
      // point (Skip, OrderBy, etc.).
      if (!on_master_) {
        Split(produce, PlanCartesian(produce.input()));
      } else {
        // We are on master, so our produce input must come on the left hand
        // side.
        throw utils::NotYetImplemented("distributed planning");
      }
    }
    return true;
  }

  bool PreVisit(Unwind &op) override {
    prev_ops_.push_back(&op);
    return true;
  }

  bool Visit(Once &) override { return true; }

  bool Visit(CreateIndex &) override { return true; }

  // Accumulate is used only if the query performs any writes. In such a case,
  // we need to synchronize the work done on master and all workers.
  // Synchronization will force applying changes to distributed storage, and
  // then we can continue with the rest of the plan. Currently, the remainder of
  // the plan is executed on master. In the future, when we support Cartesian
  // products after the WITH clause, we will need to split the plan in more
  // subparts to be executed on workers.
  bool PreVisit(Accumulate &acc) override {
    prev_ops_.push_back(&acc);
    return true;
  }
  bool PostVisit(Accumulate &acc) override {
    prev_ops_.pop_back();
    DCHECK(needs_synchronize_)
        << "Expected Accumulate to follow a write operator";
    // Create a synchronization point. Use pull remote to fetch accumulated
    // symbols from workers. Accumulation is done through Synchronize, so we
    // don't need the Accumulate operator itself. Local input operations are the
    // same as on workers.
    std::shared_ptr<PullRemote> pull_remote;
    if (ShouldSplit()) {
      auto pull_id = AddWorkerPlan(acc.input());
      pull_remote =
          std::make_shared<PullRemote>(nullptr, pull_id, acc.symbols());
    }
    auto sync = std::make_unique<Synchronize>(acc.input(), pull_remote,
                                              acc.advance_command());
    SetOnPrevious(std::move(sync));
    on_master_ = true;
    needs_synchronize_ = false;
    return true;
  }

  // CRUD operators follow

  bool PreVisit(CreateNode &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(CreateNode &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    // Creation needs to be modified if running on master, so as to distribute
    // node creation to workers.
    if (!ShouldSplit()) {
      op.set_on_random_worker(true);
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(CreateExpand &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(CreateExpand &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(Delete &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(Delete &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(SetProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetProperty &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(SetProperties &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetProperties &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(SetLabels &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(SetLabels &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(RemoveProperty &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(RemoveProperty &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

  bool PreVisit(RemoveLabels &op) override {
    prev_ops_.push_back(&op);
    return true;
  }
  bool PostVisit(RemoveLabels &op) override {
    prev_ops_.pop_back();
    if (!left_cartesians_.empty()) {
      Split(op, PlanCartesian(op.input()));
    }
    needs_synchronize_ = true;
    return true;
  }

 protected:
  bool DefaultPreVisit() override {
    throw utils::NotYetImplemented("distributed planning");
  }

  bool DefaultPostVisit() override {
    prev_ops_.pop_back();
    return true;
  }

 private:
  DistributedPlan &distributed_plan_;
  std::atomic<int64_t> &next_plan_id_;
  std::vector<LogicalOperator *> prev_ops_;
  // Left side operators that still need to be wired into Cartesian.
  std::vector<std::shared_ptr<LogicalOperator>> left_cartesians_;
  bool has_scan_all_ = false;
  bool needs_synchronize_ = false;
  bool should_split_ = false;
  // True if we have added a worker merge point on master, i.e. the rest of the
  // plan is executing on master.
  bool on_master_ = false;

  // Sets the master_op input to be merge_op. Afterwards, on_master_ is true.
  template <class TOp>
  void Split(TOp &master_op, std::shared_ptr<LogicalOperator> merge_op) {
    if (on_master_) throw utils::NotYetImplemented("distributed planning");
    on_master_ = true;
    master_op.set_input(merge_op);
  }

  void SplitOnPrevious(std::unique_ptr<LogicalOperator> merge_op) {
    if (on_master_) throw utils::NotYetImplemented("distributed planning");
    on_master_ = true;
    if (prev_ops_.empty()) {
      distributed_plan_.master_plan = std::move(merge_op);
      return;
    }
    SetOnPrevious(std::move(merge_op));
  }

  void SetOnPrevious(std::unique_ptr<LogicalOperator> input_op) {
    auto *prev_op = prev_ops_.back();
    DCHECK(prev_op)
        << "SetOnPrevious should only be called when there is a previously "
           "visited operation";
    if (!prev_op->HasSingleInput())
      throw utils::NotYetImplemented("distributed planning");
    prev_op->set_input(std::move(input_op));
  }

  int64_t AddWorkerPlan(const std::shared_ptr<LogicalOperator> &worker_plan) {
    should_split_ = false;
    return ::query::plan::AddWorkerPlan(distributed_plan_, next_plan_id_,
                                        worker_plan);
  }
};

}  // namespace

DistributedPlan MakeDistributedPlan(const LogicalOperator &original_plan,
                                    const SymbolTable &symbol_table,
                                    std::atomic<int64_t> &next_plan_id) {
  DistributedPlan distributed_plan;
  // If we will generate multiple worker plans, we will need to increment the
  // next_plan_id for each one.
  distributed_plan.master_plan_id = next_plan_id++;
  distributed_plan.symbol_table = symbol_table;
  std::tie(distributed_plan.master_plan, distributed_plan.ast_storage) =
      Clone(original_plan);
  DistributedPlanner planner(distributed_plan, next_plan_id);
  distributed_plan.master_plan->Accept(planner);
  if (planner.ShouldSplit()) {
    // We haven't split the plan, this means that it should be the same on
    // master and worker. We only need to prepend PullRemote to master plan.
    std::shared_ptr<LogicalOperator> worker_plan(
        std::move(distributed_plan.master_plan));
    auto pull_id = AddWorkerPlan(distributed_plan, next_plan_id, worker_plan);
    // If the plan performs writes, we need to finish with Synchronize.
    if (planner.NeedsSynchronize()) {
      auto pull_remote = std::make_shared<PullRemote>(
          nullptr, pull_id,
          worker_plan->OutputSymbols(distributed_plan.symbol_table));
      distributed_plan.master_plan =
          std::make_unique<Synchronize>(worker_plan, pull_remote, false);
    } else {
      distributed_plan.master_plan = std::make_unique<PullRemote>(
          worker_plan, pull_id,
          worker_plan->OutputSymbols(distributed_plan.symbol_table));
    }
  } else if (planner.NeedsSynchronize()) {
    // If the plan performs writes on master, we still need to Synchronize, even
    // though we don't split the plan.
    distributed_plan.master_plan = std::make_unique<Synchronize>(
        std::move(distributed_plan.master_plan), nullptr, false);
  }
  return distributed_plan;
}

}  // namespace query::plan
