#include <iostream>
#include <list>
#include <sstream>
#include <tuple>
#include <typeinfo>
#include <unordered_set>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/distributed.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/planner.hpp"

#include "query_common.hpp"

namespace query {
::std::ostream &operator<<(::std::ostream &os, const Symbol &sym) {
  return os << "Symbol{\"" << sym.name() << "\" [" << sym.position() << "] "
            << Symbol::TypeToString(sym.type()) << "}";
}
}  // namespace query

using namespace query::plan;
using query::AstTreeStorage;
using query::SingleQuery;
using query::Symbol;
using query::SymbolGenerator;
using query::SymbolTable;
using Direction = query::EdgeAtom::Direction;
using Bound = ScanAllByLabelPropertyRange::Bound;

namespace {

class BaseOpChecker {
 public:
  virtual ~BaseOpChecker() {}

  virtual void CheckOp(LogicalOperator &, const SymbolTable &) = 0;
};

class PlanChecker : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  PlanChecker(const std::list<std::unique_ptr<BaseOpChecker>> &checkers,
              const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {
    for (const auto &checker : checkers) checkers_.emplace_back(checker.get());
  }

  PlanChecker(const std::list<BaseOpChecker *> &checkers,
              const SymbolTable &symbol_table)
      : checkers_(checkers), symbol_table_(symbol_table) {}

#define PRE_VISIT(TOp)              \
  bool PreVisit(TOp &op) override { \
    CheckOp(op);                    \
    return true;                    \
  }

  PRE_VISIT(CreateNode);
  PRE_VISIT(CreateExpand);
  PRE_VISIT(Delete);
  PRE_VISIT(ScanAll);
  PRE_VISIT(ScanAllByLabel);
  PRE_VISIT(ScanAllByLabelPropertyValue);
  PRE_VISIT(ScanAllByLabelPropertyRange);
  PRE_VISIT(Expand);
  PRE_VISIT(ExpandVariable);
  PRE_VISIT(Filter);
  PRE_VISIT(ConstructNamedPath);
  PRE_VISIT(Produce);
  PRE_VISIT(SetProperty);
  PRE_VISIT(SetProperties);
  PRE_VISIT(SetLabels);
  PRE_VISIT(RemoveProperty);
  PRE_VISIT(RemoveLabels);
  PRE_VISIT(ExpandUniquenessFilter<VertexAccessor>);
  PRE_VISIT(ExpandUniquenessFilter<EdgeAccessor>);
  PRE_VISIT(Accumulate);
  PRE_VISIT(Aggregate);
  PRE_VISIT(Skip);
  PRE_VISIT(Limit);
  PRE_VISIT(OrderBy);
  bool PreVisit(Merge &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  bool PreVisit(Optional &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }
  PRE_VISIT(Unwind);
  PRE_VISIT(Distinct);

  bool Visit(Once &) override {
    // Ignore checking Once, it is implicitly at the end.
    return true;
  }

  bool Visit(CreateIndex &op) override {
    CheckOp(op);
    return true;
  }

  PRE_VISIT(PullRemote);

  bool PreVisit(Synchronize &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }

  bool PreVisit(Cartesian &op) override {
    CheckOp(op);
    return false;
  }

  PRE_VISIT(PullRemoteOrderBy);
#undef PRE_VISIT

  std::list<BaseOpChecker *> checkers_;

 private:
  void CheckOp(LogicalOperator &op) {
    ASSERT_FALSE(checkers_.empty());
    checkers_.back()->CheckOp(op, symbol_table_);
    checkers_.pop_back();
  }

  const SymbolTable &symbol_table_;
};

template <class TOp>
class OpChecker : public BaseOpChecker {
 public:
  void CheckOp(LogicalOperator &op, const SymbolTable &symbol_table) override {
    auto *expected_op = dynamic_cast<TOp *>(&op);
    ASSERT_TRUE(expected_op);
    ExpectOp(*expected_op, symbol_table);
  }

  virtual void ExpectOp(TOp &, const SymbolTable &) {}
};

using ExpectCreateExpand = OpChecker<CreateExpand>;
using ExpectDelete = OpChecker<Delete>;
using ExpectScanAll = OpChecker<ScanAll>;
using ExpectScanAllByLabel = OpChecker<ScanAllByLabel>;
using ExpectExpand = OpChecker<Expand>;
using ExpectFilter = OpChecker<Filter>;
using ExpectConstructNamedPath = OpChecker<ConstructNamedPath>;
using ExpectProduce = OpChecker<Produce>;
using ExpectSetProperty = OpChecker<SetProperty>;
using ExpectSetProperties = OpChecker<SetProperties>;
using ExpectSetLabels = OpChecker<SetLabels>;
using ExpectRemoveProperty = OpChecker<RemoveProperty>;
using ExpectRemoveLabels = OpChecker<RemoveLabels>;
template <class TAccessor>
using ExpectExpandUniquenessFilter =
    OpChecker<ExpandUniquenessFilter<TAccessor>>;
using ExpectSkip = OpChecker<Skip>;
using ExpectLimit = OpChecker<Limit>;
using ExpectOrderBy = OpChecker<OrderBy>;
using ExpectUnwind = OpChecker<Unwind>;
using ExpectDistinct = OpChecker<Distinct>;

class ExpectExpandVariable : public OpChecker<ExpandVariable> {
 public:
  void ExpectOp(ExpandVariable &op, const SymbolTable &) override {
    EXPECT_EQ(op.type(), query::EdgeAtom::Type::DEPTH_FIRST);
  }
};

class ExpectExpandBreadthFirst : public OpChecker<ExpandVariable> {
 public:
  void ExpectOp(ExpandVariable &op, const SymbolTable &) override {
    EXPECT_EQ(op.type(), query::EdgeAtom::Type::BREADTH_FIRST);
  }
};

class ExpectAccumulate : public OpChecker<Accumulate> {
 public:
  explicit ExpectAccumulate(const std::unordered_set<Symbol> &symbols)
      : symbols_(symbols) {}

  void ExpectOp(Accumulate &op, const SymbolTable &) override {
    std::unordered_set<Symbol> got_symbols(op.symbols().begin(),
                                           op.symbols().end());
    EXPECT_EQ(symbols_, got_symbols);
  }

 private:
  const std::unordered_set<Symbol> symbols_;
};

class ExpectAggregate : public OpChecker<Aggregate> {
 public:
  ExpectAggregate(bool is_master,
                  const std::vector<query::Aggregation *> &aggregations,
                  const std::unordered_set<query::Expression *> &group_by)
      : is_master_(is_master),
        aggregations_(aggregations),
        group_by_(group_by) {}
  ExpectAggregate(const std::vector<query::Aggregation *> &aggregations,
                  const std::unordered_set<query::Expression *> &group_by)
      : is_master_(false), aggregations_(aggregations), group_by_(group_by) {}

  void ExpectOp(Aggregate &op, const SymbolTable &symbol_table) override {
    auto aggr_it = aggregations_.begin();
    for (const auto &aggr_elem : op.aggregations()) {
      ASSERT_NE(aggr_it, aggregations_.end());
      auto aggr = *aggr_it++;
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(aggr_elem.value).hash_code(),
                typeid(aggr->expression1_).hash_code());
      EXPECT_EQ(typeid(aggr_elem.key).hash_code(),
                typeid(aggr->expression2_).hash_code());
      EXPECT_EQ(aggr_elem.op, aggr->op_);
      if (!is_master_) {
        // Skip checking virtual merge aggregation symbol when the plan is
        // distributed.
        EXPECT_EQ(aggr_elem.output_sym, symbol_table.at(*aggr));
      }
    }
    EXPECT_EQ(aggr_it, aggregations_.end());
    // TODO: Proper group by expression equality
    std::unordered_set<size_t> got_group_by;
    for (auto *expr : op.group_by())
      got_group_by.insert(typeid(*expr).hash_code());
    std::unordered_set<size_t> expected_group_by;
    for (auto *expr : group_by_)
      expected_group_by.insert(typeid(*expr).hash_code());
    EXPECT_EQ(got_group_by, expected_group_by);
  }

 private:
  bool is_master_ = false;
  std::vector<query::Aggregation *> aggregations_;
  std::unordered_set<query::Expression *> group_by_;
};

auto ExpectMasterAggregate(
    const std::vector<query::Aggregation *> &aggregations,
    const std::unordered_set<query::Expression *> &group_by) {
  return ExpectAggregate(true, aggregations, group_by);
}

class ExpectMerge : public OpChecker<Merge> {
 public:
  ExpectMerge(const std::list<BaseOpChecker *> &on_match,
              const std::list<BaseOpChecker *> &on_create)
      : on_match_(on_match), on_create_(on_create) {}

  void ExpectOp(Merge &merge, const SymbolTable &symbol_table) override {
    PlanChecker check_match(on_match_, symbol_table);
    merge.merge_match()->Accept(check_match);
    PlanChecker check_create(on_create_, symbol_table);
    merge.merge_create()->Accept(check_create);
  }

 private:
  const std::list<BaseOpChecker *> &on_match_;
  const std::list<BaseOpChecker *> &on_create_;
};

class ExpectOptional : public OpChecker<Optional> {
 public:
  explicit ExpectOptional(const std::list<BaseOpChecker *> &optional)
      : optional_(optional) {}

  ExpectOptional(const std::vector<Symbol> &optional_symbols,
                 const std::list<BaseOpChecker *> &optional)
      : optional_symbols_(optional_symbols), optional_(optional) {}

  void ExpectOp(Optional &optional, const SymbolTable &symbol_table) override {
    if (!optional_symbols_.empty()) {
      EXPECT_THAT(optional.optional_symbols(),
                  testing::UnorderedElementsAreArray(optional_symbols_));
    }
    PlanChecker check_optional(optional_, symbol_table);
    optional.optional()->Accept(check_optional);
  }

 private:
  std::vector<Symbol> optional_symbols_;
  const std::list<BaseOpChecker *> &optional_;
};

class ExpectScanAllByLabelPropertyValue
    : public OpChecker<ScanAllByLabelPropertyValue> {
 public:
  ExpectScanAllByLabelPropertyValue(
      storage::Label label,
      const std::pair<std::string, storage::Property> &prop_pair,
      query::Expression *expression)
      : label_(label), property_(prop_pair.second), expression_(expression) {}

  void ExpectOp(ScanAllByLabelPropertyValue &scan_all,
                const SymbolTable &) override {
    EXPECT_EQ(scan_all.label(), label_);
    EXPECT_EQ(scan_all.property(), property_);
    // TODO: Proper expression equality
    EXPECT_EQ(typeid(scan_all.expression()).hash_code(),
              typeid(expression_).hash_code());
  }

 private:
  storage::Label label_;
  storage::Property property_;
  query::Expression *expression_;
};

class ExpectScanAllByLabelPropertyRange
    : public OpChecker<ScanAllByLabelPropertyRange> {
 public:
  ExpectScanAllByLabelPropertyRange(
      storage::Label label, storage::Property property,
      std::experimental::optional<Bound> lower_bound,
      std::experimental::optional<Bound> upper_bound)
      : label_(label),
        property_(property),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound) {}

  void ExpectOp(ScanAllByLabelPropertyRange &scan_all,
                const SymbolTable &) override {
    EXPECT_EQ(scan_all.label(), label_);
    EXPECT_EQ(scan_all.property(), property_);
    if (lower_bound_) {
      ASSERT_TRUE(scan_all.lower_bound());
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(scan_all.lower_bound()->value()).hash_code(),
                typeid(lower_bound_->value()).hash_code());
      EXPECT_EQ(scan_all.lower_bound()->type(), lower_bound_->type());
    }
    if (upper_bound_) {
      ASSERT_TRUE(scan_all.upper_bound());
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(scan_all.upper_bound()->value()).hash_code(),
                typeid(upper_bound_->value()).hash_code());
      EXPECT_EQ(scan_all.upper_bound()->type(), upper_bound_->type());
    }
  }

 private:
  storage::Label label_;
  storage::Property property_;
  std::experimental::optional<Bound> lower_bound_;
  std::experimental::optional<Bound> upper_bound_;
};

class ExpectCreateIndex : public OpChecker<CreateIndex> {
 public:
  ExpectCreateIndex(storage::Label label, storage::Property property)
      : label_(label), property_(property) {}

  void ExpectOp(CreateIndex &create_index, const SymbolTable &) override {
    EXPECT_EQ(create_index.label(), label_);
    EXPECT_EQ(create_index.property(), property_);
  }

 private:
  storage::Label label_;
  storage::Property property_;
};

class ExpectPullRemote : public OpChecker<PullRemote> {
 public:
  ExpectPullRemote() {}
  ExpectPullRemote(const std::vector<Symbol> &symbols) : symbols_(symbols) {}

  void ExpectOp(PullRemote &op, const SymbolTable &) override {
    EXPECT_THAT(op.symbols(), testing::UnorderedElementsAreArray(symbols_));
  }

 private:
  std::vector<Symbol> symbols_;
};

class ExpectSynchronize : public OpChecker<Synchronize> {
 public:
  explicit ExpectSynchronize(bool advance_command)
      : has_pull_(false), advance_command_(advance_command) {}
  ExpectSynchronize(const std::vector<Symbol> &symbols = {},
                    bool advance_command = false)
      : expect_pull_(symbols),
        has_pull_(true),
        advance_command_(advance_command) {}

  void ExpectOp(Synchronize &op, const SymbolTable &symbol_table) override {
    if (has_pull_) {
      ASSERT_TRUE(op.pull_remote());
      expect_pull_.ExpectOp(*op.pull_remote(), symbol_table);
    } else {
      EXPECT_FALSE(op.pull_remote());
    }
    EXPECT_EQ(op.advance_command(), advance_command_);
  }

 private:
  ExpectPullRemote expect_pull_;
  bool has_pull_ = true;
  bool advance_command_ = false;
};

class ExpectCartesian : public OpChecker<Cartesian> {
 public:
  ExpectCartesian(const std::list<std::unique_ptr<BaseOpChecker>> &left,
                  const std::list<std::unique_ptr<BaseOpChecker>> &right)
      : left_(left), right_(right) {}

  void ExpectOp(Cartesian &op, const SymbolTable &symbol_table) override {
    ASSERT_TRUE(op.left_op());
    PlanChecker left_checker(left_, symbol_table);
    op.left_op()->Accept(left_checker);
    ASSERT_TRUE(op.right_op());
    PlanChecker right_checker(right_, symbol_table);
    op.right_op()->Accept(right_checker);
  }

 private:
  const std::list<std::unique_ptr<BaseOpChecker>> &left_;
  const std::list<std::unique_ptr<BaseOpChecker>> &right_;
};

class ExpectCreateNode : public OpChecker<CreateNode> {
 public:
  ExpectCreateNode(bool on_random_worker = false)
      : on_random_worker_(on_random_worker) {}

  void ExpectOp(CreateNode &op, const SymbolTable &) override {
    EXPECT_EQ(op.on_random_worker(), on_random_worker_);
  }

 private:
  bool on_random_worker_ = false;
};

class ExpectPullRemoteOrderBy : public OpChecker<PullRemoteOrderBy> {
 public:
  ExpectPullRemoteOrderBy(const std::vector<Symbol> symbols)
      : symbols_(symbols) {}

  void ExpectOp(PullRemoteOrderBy &op, const SymbolTable &) override {
    EXPECT_THAT(op.symbols(), testing::UnorderedElementsAreArray(symbols_));
  }

 private:
  std::vector<Symbol> symbols_;
};

auto MakeSymbolTable(query::Query &query) {
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(symbol_table);
  query.Accept(symbol_generator);
  return symbol_table;
}

class Planner {
 public:
  Planner(std::vector<SingleQueryPart> single_query_parts,
          PlanningContext<database::GraphDbAccessor> &context) {
    plan_ = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(single_query_parts,
                                                            context);
  }

  auto &plan() { return *plan_; }

 private:
  std::unique_ptr<LogicalOperator> plan_;
};

class SerializedPlanner {
 public:
  SerializedPlanner(std::vector<SingleQueryPart> single_query_parts,
                    PlanningContext<database::GraphDbAccessor> &context) {
    std::stringstream stream;
    {
      auto original_plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(
          single_query_parts, context);
      boost::archive::binary_oarchive out_archive(stream);
      out_archive << original_plan;
    }
    {
      boost::archive::binary_iarchive in_archive(stream);
      std::tie(plan_, ast_storage_) = LoadPlan(in_archive);
    }
  }

  auto &plan() { return *plan_; }

 private:
  AstTreeStorage ast_storage_;
  std::unique_ptr<LogicalOperator> plan_;
};

template <class TPlanner>
TPlanner MakePlanner(database::MasterBase &master_db, AstTreeStorage &storage,
                     SymbolTable &symbol_table) {
  database::GraphDbAccessor dba(master_db);
  auto planning_context = MakePlanningContext(storage, symbol_table, dba);
  auto query_parts = CollectQueryParts(symbol_table, storage);
  auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
  return TPlanner(single_query_parts, planning_context);
}

template <class... TChecker>
auto CheckPlan(LogicalOperator &plan, const SymbolTable &symbol_table,
               TChecker... checker) {
  std::list<BaseOpChecker *> checkers{&checker...};
  PlanChecker plan_checker(checkers, symbol_table);
  plan.Accept(plan_checker);
  EXPECT_TRUE(plan_checker.checkers_.empty());
}

template <class TPlanner, class... TChecker>
auto CheckPlan(AstTreeStorage &storage, TChecker... checker) {
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TPlanner>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, checker...);
}

struct ExpectedDistributedPlan {
  std::list<std::unique_ptr<BaseOpChecker>> master_checkers;
  std::vector<std::list<std::unique_ptr<BaseOpChecker>>> worker_checkers;
};

template <class TPlanner>
DistributedPlan MakeDistributedPlan(query::AstTreeStorage &storage) {
  database::Master db;
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TPlanner>(db, storage, symbol_table);
  std::atomic<int64_t> next_plan_id{0};
  return MakeDistributedPlan(planner.plan(), symbol_table, next_plan_id);
}

void CheckDistributedPlan(DistributedPlan &distributed_plan,
                          ExpectedDistributedPlan &expected) {
  PlanChecker plan_checker(expected.master_checkers,
                           distributed_plan.symbol_table);
  distributed_plan.master_plan->Accept(plan_checker);
  EXPECT_TRUE(plan_checker.checkers_.empty());
  if (expected.worker_checkers.empty()) {
    EXPECT_TRUE(distributed_plan.worker_plans.empty());
  } else {
    ASSERT_EQ(distributed_plan.worker_plans.size(),
              expected.worker_checkers.size());
    for (size_t i = 0; i < expected.worker_checkers.size(); ++i) {
      PlanChecker plan_checker(expected.worker_checkers[i],
                               distributed_plan.symbol_table);
      auto worker_plan = distributed_plan.worker_plans[i].second;
      worker_plan->Accept(plan_checker);
      EXPECT_TRUE(plan_checker.checkers_.empty());
    }
  }
}

void CheckDistributedPlan(const LogicalOperator &plan,
                          const SymbolTable &symbol_table,
                          ExpectedDistributedPlan &expected_distributed_plan) {
  std::atomic<int64_t> next_plan_id{0};
  auto distributed_plan = MakeDistributedPlan(plan, symbol_table, next_plan_id);
  EXPECT_EQ(next_plan_id - 1, distributed_plan.worker_plans.size());
  CheckDistributedPlan(distributed_plan, expected_distributed_plan);
}

template <class TPlanner>
void CheckDistributedPlan(AstTreeStorage &storage,
                          ExpectedDistributedPlan &expected_distributed_plan) {
  auto distributed_plan = MakeDistributedPlan<TPlanner>(storage);
  CheckDistributedPlan(distributed_plan, expected_distributed_plan);
}

template <class T>
std::list<std::unique_ptr<BaseOpChecker>> MakeCheckers(T arg) {
  std::list<std::unique_ptr<BaseOpChecker>> l;
  l.emplace_back(std::make_unique<T>(arg));
  return l;
}

template <class T, class... Rest>
std::list<std::unique_ptr<BaseOpChecker>> MakeCheckers(T arg, Rest &&... rest) {
  auto l = MakeCheckers(std::forward<Rest>(rest)...);
  l.emplace_front(std::make_unique<T>(arg));
  return std::move(l);
}

ExpectedDistributedPlan ExpectDistributed(
    std::list<std::unique_ptr<BaseOpChecker>> master_checker) {
  return ExpectedDistributedPlan{std::move(master_checker)};
}

ExpectedDistributedPlan ExpectDistributed(
    std::list<std::unique_ptr<BaseOpChecker>> master_checker,
    std::list<std::unique_ptr<BaseOpChecker>> worker_checker) {
  ExpectedDistributedPlan expected{std::move(master_checker)};
  expected.worker_checkers.emplace_back(std::move(worker_checker));
  return expected;
}

void AddWorkerCheckers(
    ExpectedDistributedPlan &expected,
    std::list<std::unique_ptr<BaseOpChecker>> worker_checker) {
  expected.worker_checkers.emplace_back(std::move(worker_checker));
}

template <class... Rest>
void AddWorkerCheckers(ExpectedDistributedPlan &expected,
                       std::list<std::unique_ptr<BaseOpChecker>> worker_checker,
                       Rest &&... rest) {
  expected.worker_checkers.emplace_back(std::move(worker_checker));
  AddWorkerCheckers(expected, std::forward<Rest>(rest)...);
}

template <class... Rest>
ExpectedDistributedPlan ExpectDistributed(
    std::list<std::unique_ptr<BaseOpChecker>> master_checker,
    std::list<std::unique_ptr<BaseOpChecker>> worker_checker, Rest &&... rest) {
  ExpectedDistributedPlan expected{std::move(master_checker)};
  expected.worker_checkers.emplace_back(std::move(worker_checker));
  AddWorkerCheckers(expected, std::forward<Rest>(rest)...);
  return expected;
}

template <class T>
class TestPlanner : public ::testing::Test {};

using PlannerTypes = ::testing::Types<Planner, SerializedPlanner>;

TYPED_TEST_CASE(TestPlanner, PlannerTypes);

TYPED_TEST(TestPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n
  AstTreeStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN(as_n)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::Master db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query =
      QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), RETURN(ident_n, AS("n"))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(), acc,
            ExpectProduce());
  {
    auto expected = ExpectDistributed(MakeCheckers(
        ExpectCreateNode(true), ExpectSynchronize(false), ExpectProduce()));
    std::atomic<int64_t> next_plan_id{0};
    auto distributed_plan =
        MakeDistributedPlan(planner.plan(), symbol_table, next_plan_id);
    CheckDistributedPlan(distributed_plan, expected);
  }
}

TYPED_TEST(TestPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("relationship");
  QUERY(SINGLE_QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")))));
  CheckPlan<TypeParam>(storage, ExpectCreateNode(), ExpectCreateExpand());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectCreateExpand(),
                   ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m")))));
  CheckPlan<TypeParam>(storage, ExpectCreateNode(), ExpectCreateNode());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectCreateNode(true),
                   ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("rel");
  QUERY(SINGLE_QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")),
      PATTERN(NODE("l")))));
  CheckPlan<TypeParam>(storage, ExpectCreateNode(), ExpectCreateExpand(),
                       ExpectCreateNode());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectCreateExpand(),
                   ExpectCreateNode(true), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, CreateNamedPattern) {
  // Test CREATE p = (n) -[r :rel]-> (m)
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("rel");
  QUERY(SINGLE_QUERY(CREATE(NAMED_PATTERN(
      "p", NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")))));
  CheckPlan<TypeParam>(storage, ExpectCreateNode(), ExpectCreateExpand(),
                       ExpectConstructNamedPath());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectCreateExpand(),
                   ExpectConstructNamedPath(), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("relationship");
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      CREATE(PATTERN(NODE("n"), EDGE("r", Direction::OUT, {relationship}),
                     NODE("m")))));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectCreateExpand());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectCreateExpand(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectCreateExpand()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))), RETURN(as_n)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAllByLabel(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAllByLabel(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAllByLabel(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("relationship");
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", Direction::BOTH, {relationship}),
                    NODE("m"))),
      RETURN(as_n)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchNamedPatternReturn) {
  // Test MATCH p = (n) -[r :relationship]- (m) RETURN p
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("relationship");
  auto *as_p = NEXPR("p", IDENT("p"));
  QUERY(SINGLE_QUERY(
      MATCH(NAMED_PATTERN("p", NODE("n"),
                          EDGE("r", Direction::BOTH, {relationship}),
                          NODE("m"))),
      RETURN(as_p)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectConstructNamedPath(), ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_p)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectConstructNamedPath(),
                   ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectConstructNamedPath(),
                   ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchNamedPatternWithPredicateReturn) {
  // Test MATCH p = (n) -[r :relationship]- (m) WHERE 2 = p RETURN p
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("relationship");
  auto *as_p = NEXPR("p", IDENT("p"));
  QUERY(SINGLE_QUERY(
      MATCH(NAMED_PATTERN("p", NODE("n"),
                          EDGE("r", Direction::BOTH, {relationship}),
                          NODE("m"))),
      WHERE(EQ(LITERAL(2), IDENT("p"))), RETURN(as_p)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectConstructNamedPath(), ExpectFilter(), ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_p)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectConstructNamedPath(),
                   ExpectFilter(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectConstructNamedPath(),
                   ExpectFilter(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, OptionalMatchNamedPatternReturn) {
  // Test OPTIONAL MATCH p = (n) -[r]- (m) RETURN p
  database::SingleNode db;
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto edge = EDGE("r");
  auto node_m = NODE("m");
  auto pattern = NAMED_PATTERN("p", node_n, edge, node_m);
  auto as_p = AS("p");
  QUERY(SINGLE_QUERY(OPTIONAL_MATCH(pattern), RETURN("p", as_p)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  std::list<BaseOpChecker *> optional{new ExpectScanAll(), new ExpectExpand(),
                                      new ExpectConstructNamedPath()};
  auto get_symbol = [&symbol_table](const auto *ast_node) {
    return symbol_table.at(*ast_node->identifier_);
  };
  std::vector<Symbol> optional_symbols{get_symbol(pattern), get_symbol(node_n),
                                       get_symbol(edge), get_symbol(node_m)};
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table,
            ExpectOptional(optional_symbols, optional), ExpectProduce());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectOptional(optional_symbols, optional), ExpectProduce(),
                   ExpectPullRemote({symbol_table.at(*as_p)})),
      MakeCheckers(ExpectOptional(optional_symbols, optional),
                   ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto property = dba.Property("property");
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", property), LITERAL(42))),
                     RETURN(as_n)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectFilter(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n"))));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectDelete());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDelete(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectDelete()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchNodeSet) {
  // Test MATCH (n) SET n.prop = 42, n = n, n :label
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  auto label = dba.Label("label");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     SET(PROPERTY_LOOKUP("n", prop), LITERAL(42)),
                     SET("n", IDENT("n")), SET("n", {label})));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectSetProperty(),
                       ExpectSetProperties(), ExpectSetLabels());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectSetProperty(), ExpectSetProperties(),
                   ExpectSetLabels(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectSetProperty(), ExpectSetProperties(),
                   ExpectSetLabels()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchRemove) {
  // Test MATCH (n) REMOVE n.prop REMOVE n :label
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  auto label = dba.Label("label");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     REMOVE(PROPERTY_LOOKUP("n", prop)), REMOVE("n", {label})));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectRemoveProperty(),
                       ExpectRemoveLabels());
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectRemoveProperty(),
                                     ExpectRemoveLabels(), ExpectSynchronize()),
                        MakeCheckers(ExpectScanAll(), ExpectRemoveProperty(),
                                     ExpectRemoveLabels()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchMultiPattern) {
  // Test MATCH (n) -[r]- (m), (j) -[e]- (i) RETURN n
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("j"), EDGE("e"), NODE("i"))),
                     RETURN("n")));
  // We expect the expansions after the first to have a uniqueness filter in a
  // single MATCH clause.
  CheckPlan<TypeParam>(
      storage, ExpectScanAll(), ExpectExpand(), ExpectScanAll(), ExpectExpand(),
      ExpectExpandUniquenessFilter<EdgeAccessor>(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchMultiPatternSameStart) {
  // Test MATCH (n), (n) -[e]- (m) RETURN n
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n")), PATTERN(NODE("n"), EDGE("e"), NODE("m"))),
      RETURN("n")));
  // We expect the second pattern to generate only an Expand, since another
  // ScanAll would be redundant.
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpand(),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchMultiPatternSameExpandStart) {
  // Test MATCH (n) -[r]- (m), (m) -[e]- (l) RETURN n
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m")),
                           PATTERN(NODE("m"), EDGE("e"), NODE("l"))),
                     RETURN("n")));
  // We expect the second pattern to generate only an Expand. Another
  // ScanAll would be redundant, as it would generate the nodes obtained from
  // expansion. Additionally, a uniqueness filter is expected.
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpand(), ExpectExpand(),
                       ExpectExpandUniquenessFilter<EdgeAccessor>(),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MultiMatch) {
  // Test MATCH (n) -[r]- (m) MATCH (j) -[e]- (i) -[f]- (h) RETURN n
  AstTreeStorage storage;
  auto *node_n = NODE("n");
  auto *edge_r = EDGE("r");
  auto *node_m = NODE("m");
  auto *node_j = NODE("j");
  auto *edge_e = EDGE("e");
  auto *node_i = NODE("i");
  auto *edge_f = EDGE("f");
  auto *node_h = NODE("h");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, edge_r, node_m)),
                     MATCH(PATTERN(node_j, edge_e, node_i, edge_f, node_h)),
                     RETURN("n")));
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  // Multiple MATCH clauses form a Cartesian product, so the uniqueness should
  // not cross MATCH boundaries.
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectScanAll(), ExpectExpand(), ExpectExpand(),
            ExpectExpandUniquenessFilter<EdgeAccessor>(), ExpectProduce());
  auto get_symbol = [&symbol_table](const auto *atom_node) {
    return symbol_table.at(*atom_node->identifier_);
  };
  ExpectPullRemote left_pull(
      {get_symbol(node_n), get_symbol(edge_r), get_symbol(node_m)});
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectExpand(), left_pull);
  ExpectPullRemote right_pull({get_symbol(node_j), get_symbol(edge_e),
                               get_symbol(node_i), get_symbol(edge_f),
                               get_symbol(node_h)});
  auto right_cart =
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectExpand(),
                   ExpectExpandUniquenessFilter<EdgeAccessor>(), right_pull);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectCartesian(std::move(left_cart), std::move(right_cart)),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectExpand()),
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectExpand(),
                   ExpectExpandUniquenessFilter<EdgeAccessor>()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MultiMatchSameStart) {
  // Test MATCH (n) MATCH (n) -[r]- (m) RETURN n
  AstTreeStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     RETURN(as_n)));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectExpand(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchWithReturn) {
  // Test MATCH (old) WITH old AS new RETURN new
  AstTreeStorage storage;
  auto *as_new = NEXPR("new", IDENT("new"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new")),
                     RETURN(as_new)));
  // No accumulation since we only do reads.
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectProduce(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_new)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MatchWithWhereReturn) {
  // Test MATCH (old) WITH old AS new WHERE new.prop < 42 RETURN new
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto *as_new = NEXPR("new", IDENT("new"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))), WITH("old", AS("new")),
                     WHERE(LESS(PROPERTY_LOOKUP("new", prop), LITERAL(42))),
                     RETURN(as_new)));
  // No accumulation since we only do reads.
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectProduce(),
            ExpectFilter(), ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_new)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(),
                                     ExpectFilter(), ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectProduce(),
                                     ExpectFilter(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto r = dba.EdgeType("r");
  auto p = dba.EdgeType("p");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(
      CREATE(PATTERN(NODE("n"), EDGE("r", Direction::OUT, {r}), NODE("m")),
             PATTERN(NODE("n"), EDGE("p", Direction::OUT, {p}), NODE("l")))));
  CheckPlan<TypeParam>(storage, ExpectCreateNode(), ExpectCreateExpand(),
                       ExpectCreateExpand());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectCreateExpand(),
                   ExpectCreateExpand(), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchWithSumWhereReturn) {
  // Test MATCH (n) WITH SUM(n.prop) + 42 AS sum WHERE sum < 42
  //      RETURN sum AS result
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto literal = LITERAL(42);
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), WITH(ADD(sum, literal), AS("sum")),
      WHERE(LESS(IDENT("sum"), LITERAL(42))), RETURN("sum", AS("result"))));
  auto aggr = ExpectAggregate({sum}, {literal});
  CheckPlan<TypeParam>(storage, ExpectScanAll(), aggr, ExpectProduce(),
                       ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchReturnSum) {
  // Test MATCH (n) RETURN SUM(n.prop1) AS sum, n.prop2 AS group
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop1 = dba.Property("prop1");
  auto prop2 = dba.Property("prop2");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop1));
  auto n_prop2 = PROPERTY_LOOKUP("n", prop2);
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     RETURN(sum, AS("sum"), n_prop2, AS("group"))));
  auto aggr = ExpectAggregate({sum}, {n_prop2});
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), aggr,
            ExpectProduce());
  {
    std::atomic<int64_t> next_plan_id{0};
    auto distributed_plan =
        MakeDistributedPlan(planner.plan(), symbol_table, next_plan_id);
    auto merge_sum = SUM(IDENT("worker_sum"));
    auto master_aggr = ExpectMasterAggregate({merge_sum}, {n_prop2});
    ExpectPullRemote pull(
        {symbol_table.at(*sum), symbol_table.at(*n_prop2->expression_)});
    auto expected =
        ExpectDistributed(MakeCheckers(ExpectScanAll(), aggr, pull, master_aggr,
                                       ExpectProduce(), ExpectProduce()),
                          MakeCheckers(ExpectScanAll(), aggr));
    CheckDistributedPlan(distributed_plan, expected);
  }
}

TYPED_TEST(TestPlanner, CreateWithSum) {
  // Test CREATE (n) WITH SUM(n.prop) AS sum
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto sum = SUM(n_prop);
  auto query =
      QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), WITH(sum, AS("sum"))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*n_prop->expression_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  // We expect both the accumulation and aggregation because the part before
  // WITH updates the database.
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(), acc, aggr,
            ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchWithCreate) {
  // Test MATCH (n) WITH n AS a CREATE (a) -[r :r]-> (b)
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto r_type = dba.EdgeType("r");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), WITH("n", AS("a")),
      CREATE(
          PATTERN(NODE("a"), EDGE("r", Direction::OUT, {r_type}), NODE("b")))));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectProduce(),
                       ExpectCreateExpand());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectCreateExpand(),
                   ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectCreateExpand()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchReturnSkipLimit) {
  // Test MATCH (n) RETURN n SKIP 2 LIMIT 1
  AstTreeStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     RETURN(as_n, SKIP(LITERAL(2)), LIMIT(LITERAL(1)))));
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectProduce(),
            ExpectSkip(), ExpectLimit());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(), pull,
                                     ExpectSkip(), ExpectLimit()),
                        MakeCheckers(ExpectScanAll(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, CreateWithSkipReturnLimit) {
  // Test CREATE (n) WITH n AS m SKIP 2 RETURN m LIMIT 1
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))),
                                  WITH(ident_n, AS("m"), SKIP(LITERAL(2))),
                                  RETURN("m", LIMIT(LITERAL(1)))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  // Since we have a write query, we need to have Accumulate. This is a bit
  // different than Neo4j 3.0, which optimizes WITH followed by RETURN as a
  // single RETURN clause and then moves Skip and Limit before Accumulate. This
  // causes different behaviour. A newer version of Neo4j does the same thing as
  // us here (but who knows if they change it again).
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(), acc,
            ExpectProduce(), ExpectSkip(), ExpectProduce(), ExpectLimit());
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectCreateNode(true), ExpectSynchronize(true),
                   ExpectProduce(), ExpectSkip(), ExpectProduce(),
                   ExpectLimit()),
      {}};
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, CreateReturnSumSkipLimit) {
  // Test CREATE (n) RETURN SUM(n.prop) AS s SKIP 2 LIMIT 1
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto n_prop = PROPERTY_LOOKUP("n", prop);
  auto sum = SUM(n_prop);
  auto query = QUERY(
      SINGLE_QUERY(CREATE(PATTERN(NODE("n"))),
                   RETURN(sum, AS("s"), SKIP(LITERAL(2)), LIMIT(LITERAL(1)))));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*n_prop->expression_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(), acc, aggr,
            ExpectProduce(), ExpectSkip(), ExpectLimit());
}

TYPED_TEST(TestPlanner, MatchReturnOrderBy) {
  // Test MATCH (n) RETURN n AS m ORDER BY n.prop
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto *as_m = NEXPR("m", IDENT("n"));
  auto *node_n = NODE("n");
  auto ret = RETURN(as_m, ORDER_BY(PROPERTY_LOOKUP("n", prop)));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), ret));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectProduce(),
            ExpectOrderBy());
  ExpectPullRemoteOrderBy pull_order_by(
      {symbol_table.at(*as_m), symbol_table.at(*node_n->identifier_)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectOrderBy(),
                   pull_order_by),
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectOrderBy()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
  // Even though last operator pulls and orders by `m` and `n`, we expect only
  // `m` as the output of the query execution.
  EXPECT_THAT(planner.plan().OutputSymbols(symbol_table),
              testing::UnorderedElementsAre(symbol_table.at(*as_m)));
}

TYPED_TEST(TestPlanner, CreateWithOrderByWhere) {
  // Test CREATE (n) -[r :r]-> (m)
  //      WITH n AS new ORDER BY new.prop, r.prop WHERE m.prop < 42
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  auto r_type = dba.EdgeType("r");
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto new_prop = PROPERTY_LOOKUP("new", prop);
  auto r_prop = PROPERTY_LOOKUP("r", prop);
  auto m_prop = PROPERTY_LOOKUP("m", prop);
  auto query = QUERY(SINGLE_QUERY(
      CREATE(
          PATTERN(NODE("n"), EDGE("r", Direction::OUT, {r_type}), NODE("m"))),
      WITH(ident_n, AS("new"), ORDER_BY(new_prop, r_prop)),
      WHERE(LESS(m_prop, LITERAL(42)))));
  auto symbol_table = MakeSymbolTable(*query);
  // Since this is a write query, we expect to accumulate to old used symbols.
  auto acc = ExpectAccumulate({
      symbol_table.at(*ident_n),              // `n` in WITH
      symbol_table.at(*r_prop->expression_),  // `r` in ORDER BY
      symbol_table.at(*m_prop->expression_),  // `m` in WHERE
  });
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(),
            ExpectCreateExpand(), acc, ExpectProduce(), ExpectOrderBy(),
            ExpectFilter());
  auto expected = ExpectDistributed(MakeCheckers(
      ExpectCreateNode(true), ExpectCreateExpand(), ExpectSynchronize(true),
      ExpectProduce(), ExpectOrderBy(), ExpectFilter()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, ReturnAddSumCountOrderBy) {
  // Test RETURN SUM(1) + COUNT(2) AS result ORDER BY result
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(1));
  auto count = COUNT(LITERAL(2));
  QUERY(SINGLE_QUERY(
      RETURN(ADD(sum, count), AS("result"), ORDER_BY(IDENT("result")))));
  auto aggr = ExpectAggregate({sum, count}, {});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce(), ExpectOrderBy());
  auto expected =
      ExpectDistributed(MakeCheckers(aggr, ExpectProduce(), ExpectOrderBy()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, MatchMerge) {
  // Test MATCH (n) MERGE (n) -[r :r]- (m)
  //      ON MATCH SET n.prop = 42 ON CREATE SET m = n
  //      RETURN n AS n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto r_type = dba.EdgeType("r");
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      MERGE(PATTERN(NODE("n"), EDGE("r", Direction::BOTH, {r_type}), NODE("m")),
            ON_MATCH(SET(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
            ON_CREATE(SET("m", IDENT("n")))),
      RETURN(ident_n, AS("n"))));
  std::list<BaseOpChecker *> on_match{new ExpectExpand(),
                                      new ExpectSetProperty()};
  std::list<BaseOpChecker *> on_create{new ExpectCreateExpand(),
                                       new ExpectSetProperties()};
  auto symbol_table = MakeSymbolTable(*query);
  // We expect Accumulate after Merge, because it is considered as a write.
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(),
            ExpectMerge(on_match, on_create), acc, ExpectProduce());
  for (auto &op : on_match) delete op;
  on_match.clear();
  for (auto &op : on_create) delete op;
  on_create.clear();
}

TYPED_TEST(TestPlanner, MatchOptionalMatchWhereReturn) {
  // Test MATCH (n) OPTIONAL MATCH (n) -[r]- (m) WHERE m.prop < 42 RETURN r
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     OPTIONAL_MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     WHERE(LESS(PROPERTY_LOOKUP("m", prop), LITERAL(42))),
                     RETURN("r")));
  std::list<BaseOpChecker *> optional{new ExpectScanAll(), new ExpectExpand(),
                                      new ExpectFilter()};
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectOptional(optional),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchUnwindReturn) {
  // Test MATCH (n) UNWIND [1,2,3] AS x RETURN n, x
  AstTreeStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *as_x = NEXPR("x", IDENT("x"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("x")),
                     RETURN(as_n, as_x)));
  auto symbol_table = MakeSymbolTable(*storage.query());
  database::SingleNode db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectUnwind(),
            ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n), symbol_table.at(*as_x)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectUnwind(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectUnwind(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, ReturnDistinctOrderBySkipLimit) {
  // Test RETURN DISTINCT 1 ORDER BY 1 SKIP 1 LIMIT 1
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(RETURN_DISTINCT(LITERAL(1), AS("1"), ORDER_BY(LITERAL(1)),
                                     SKIP(LITERAL(1)), LIMIT(LITERAL(1)))));
  CheckPlan<TypeParam>(storage, ExpectProduce(), ExpectDistinct(),
                       ExpectOrderBy(), ExpectSkip(), ExpectLimit());
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectProduce(), ExpectDistinct(), ExpectOrderBy(),
                   ExpectSkip(), ExpectLimit()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, CreateWithDistinctSumWhereReturn) {
  // Test CREATE (n) WITH DISTINCT SUM(n.prop) AS s WHERE s < 42 RETURN s
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto query =
      QUERY(SINGLE_QUERY(CREATE(PATTERN(node_n)), WITH_DISTINCT(sum, AS("s")),
                         WHERE(LESS(IDENT("s"), LITERAL(42))), RETURN("s")));
  auto symbol_table = MakeSymbolTable(*query);
  auto acc = ExpectAccumulate({symbol_table.at(*node_n->identifier_)});
  auto aggr = ExpectAggregate({sum}, {});
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectCreateNode(), acc, aggr,
            ExpectProduce(), ExpectDistinct(), ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchCrossReferenceVariable) {
  // Test MATCH (n {prop: m.prop}), (m {prop: n.prop}) RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto node_n = NODE("n");
  auto m_prop = PROPERTY_LOOKUP("m", prop.second);
  node_n->properties_[prop] = m_prop;
  auto node_m = NODE("m");
  auto n_prop = PROPERTY_LOOKUP("n", prop.second);
  node_m->properties_[prop] = n_prop;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n), PATTERN(node_m)), RETURN("n")));
  // We expect both ScanAll to come before filters (2 are joined into one),
  // because they need to populate the symbol values.
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectScanAll(),
                       ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchWhereBeforeExpand) {
  // Test MATCH (n) -[r]- (m) WHERE n.prop < 42 RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
                     RETURN(as_n)));
  // We expect Fitler to come immediately after ScanAll, since it only uses `n`.
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectFilter(),
            ExpectExpand(), ExpectProduce());
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectFilter(),
                                     ExpectExpand(), ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectFilter(),
                                     ExpectExpand(), ExpectProduce()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, MultiMatchWhere) {
  // Test MATCH (n) -[r]- (m) MATCH (l) WHERE n.prop < 42 RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     MATCH(PATTERN(NODE("l"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
                     RETURN("n")));
  // Even though WHERE is in the second MATCH clause, we expect Filter to come
  // before second ScanAll, since it only uses the value from first ScanAll.
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectFilter(), ExpectExpand(),
                       ExpectScanAll(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchOptionalMatchWhere) {
  // Test MATCH (n) -[r]- (m) OPTIONAL MATCH (l) WHERE n.prop < 42 RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
                     OPTIONAL_MATCH(PATTERN(NODE("l"))),
                     WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))),
                     RETURN("n")));
  // Even though WHERE is in the second MATCH clause, and it uses the value from
  // first ScanAll, it must remain part of the Optional. It should come before
  // optional ScanAll.
  std::list<BaseOpChecker *> optional{new ExpectFilter(), new ExpectScanAll()};
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpand(),
                       ExpectOptional(optional), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchReturnAsterisk) {
  // Test MATCH (n) -[e]- (m) RETURN *, m.prop
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto ret = RETURN(PROPERTY_LOOKUP("m", prop), AS("m.prop"));
  ret->body_.all_identifiers = true;
  auto query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("e"), NODE("m"))), ret));
  auto symbol_table = MakeSymbolTable(*query);
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
            ExpectProduce());
  std::vector<std::string> output_names;
  for (const auto &output_symbol : planner.plan().OutputSymbols(symbol_table)) {
    output_names.emplace_back(output_symbol.name());
  }
  std::vector<std::string> expected_names{"e", "m", "n", "m.prop"};
  EXPECT_EQ(output_names, expected_names);
}

TYPED_TEST(TestPlanner, MatchReturnAsteriskSum) {
  // Test MATCH (n) RETURN *, SUM(n.prop) AS s
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  AstTreeStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto ret = RETURN(sum, AS("s"));
  ret->body_.all_identifiers = true;
  auto query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), ret));
  auto symbol_table = MakeSymbolTable(*query);
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  auto *produce = dynamic_cast<Produce *>(&planner.plan());
  ASSERT_TRUE(produce);
  const auto &named_expressions = produce->named_expressions();
  ASSERT_EQ(named_expressions.size(), 2);
  auto *expanded_ident =
      dynamic_cast<query::Identifier *>(named_expressions[0]->expression_);
  ASSERT_TRUE(expanded_ident);
  auto aggr = ExpectAggregate({sum}, {expanded_ident});
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), aggr,
            ExpectProduce());
  std::vector<std::string> output_names;
  for (const auto &output_symbol : planner.plan().OutputSymbols(symbol_table)) {
    output_names.emplace_back(output_symbol.name());
  }
  std::vector<std::string> expected_names{"n", "s"};
  EXPECT_EQ(output_names, expected_names);
}

TYPED_TEST(TestPlanner, UnwindMergeNodeProperty) {
  // Test UNWIND [1] AS i MERGE (n {prop: i})
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  AstTreeStorage storage;
  auto node_n = NODE("n");
  node_n->properties_[PROPERTY_PAIR("prop")] = IDENT("i");
  QUERY(
      SINGLE_QUERY(UNWIND(LIST(LITERAL(1)), AS("i")), MERGE(PATTERN(node_n))));
  std::list<BaseOpChecker *> on_match{new ExpectScanAll(), new ExpectFilter()};
  std::list<BaseOpChecker *> on_create{new ExpectCreateNode()};
  CheckPlan<TypeParam>(storage, ExpectUnwind(),
                       ExpectMerge(on_match, on_create));
  for (auto &op : on_match) delete op;
  for (auto &op : on_create) delete op;
}

TYPED_TEST(TestPlanner, MultipleOptionalMatchReturn) {
  // Test OPTIONAL MATCH (n) OPTIONAL MATCH (m) RETURN n
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(OPTIONAL_MATCH(PATTERN(NODE("n"))),
                     OPTIONAL_MATCH(PATTERN(NODE("m"))), RETURN("n")));
  std::list<BaseOpChecker *> optional{new ExpectScanAll()};
  CheckPlan<TypeParam>(storage, ExpectOptional(optional),
                       ExpectOptional(optional), ExpectProduce());
}

TYPED_TEST(TestPlanner, FunctionAggregationReturn) {
  // Test RETURN sqrt(SUM(2)) AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(
      RETURN(FN("sqrt", sum), AS("result"), group_by_literal, AS("group_by"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
  auto expected = ExpectDistributed(MakeCheckers(aggr, ExpectProduce()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, FunctionWithoutArguments) {
  // Test RETURN pi() AS pi
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(RETURN(FN("pi"), AS("pi"))));
  CheckPlan<TypeParam>(storage, ExpectProduce());
  auto expected = ExpectDistributed(MakeCheckers(ExpectProduce()));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, ListLiteralAggregationReturn) {
  // Test RETURN [SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(
      RETURN(LIST(sum), AS("result"), group_by_literal, AS("group_by"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, MapLiteralAggregationReturn) {
  // Test RETURN {sum: SUM(2)} AS result, 42 AS group_by
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(RETURN(MAP({PROPERTY_PAIR("sum"), sum}), AS("result"),
                            group_by_literal, AS("group_by"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, EmptyListIndexAggregation) {
  // Test RETURN [][SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto empty_list = LIST();
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(
      RETURN(storage.Create<query::ListMapIndexingOperator>(empty_list, sum),
             AS("result"), group_by_literal, AS("group_by"))));
  // We expect to group by '42' and the empty list, because it is a
  // sub-expression of a binary operator which contains an aggregation. This is
  // similar to grouping by '1' in `RETURN 1 + SUM(2)`.
  auto aggr = ExpectAggregate({sum}, {empty_list, group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, ListSliceAggregationReturn) {
  // Test RETURN [1, 2][0..SUM(2)] AS result, 42 AS group_by
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto list = LIST(LITERAL(1), LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(RETURN(SLICE(list, LITERAL(0), sum), AS("result"),
                            group_by_literal, AS("group_by"))));
  // Similarly to EmptyListIndexAggregation test, we expect grouping by list and
  // '42', because slicing is an operator.
  auto aggr = ExpectAggregate({sum}, {list, group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, ListWithAggregationAndGroupBy) {
  // Test RETURN [sum(2), 42]
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(RETURN(LIST(sum, group_by_literal), AS("result"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, AggregatonWithListWithAggregationAndGroupBy) {
  // Test RETURN sum(2), [sum(3), 42]
  AstTreeStorage storage;
  auto sum2 = SUM(LITERAL(2));
  auto sum3 = SUM(LITERAL(3));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(
      RETURN(sum2, AS("sum2"), LIST(sum3, group_by_literal), AS("list"))));
  auto aggr = ExpectAggregate({sum2, sum3}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, MapWithAggregationAndGroupBy) {
  // Test RETURN {lit: 42, sum: sum(2)}
  database::SingleNode db;
  AstTreeStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  QUERY(SINGLE_QUERY(RETURN(MAP({PROPERTY_PAIR("sum"), sum},
                                {PROPERTY_PAIR("lit"), group_by_literal}),
                            AS("result"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, CreateIndex) {
  // Test CREATE INDEX ON :Label(property)
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = dba.Property("property");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(CREATE_INDEX_ON(label, property)));
  CheckPlan<TypeParam>(storage, ExpectCreateIndex(label, property));
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectCreateIndex(label, property)));
  CheckDistributedPlan<TypeParam>(storage, expected);
}

TYPED_TEST(TestPlanner, AtomIndexedLabelProperty) {
  // Test MATCH (n :label {property: 42, not_indexed: 0}) RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = PROPERTY_PAIR("property");
  auto not_indexed = PROPERTY_PAIR("not_indexed");
  auto vertex = dba.InsertVertex();
  vertex.add_label(label);
  vertex.PropsSet(property.second, 42);
  dba.Commit();
  database::GraphDbAccessor(db).BuildIndex(label, property.second);
  {
    auto node = NODE("n", label);
    auto lit_42 = LITERAL(42);
    node->properties_[property] = lit_42;
    node->properties_[not_indexed] = LITERAL(0);
    QUERY(SINGLE_QUERY(MATCH(PATTERN(node)), RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(planner.plan(), symbol_table,
              ExpectScanAllByLabelPropertyValue(label, property, lit_42),
              ExpectFilter(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, AtomPropertyWhereLabelIndexing) {
  // Test MATCH (n {property: 42}) WHERE n.not_indexed AND n:label RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = PROPERTY_PAIR("property");
  auto not_indexed = PROPERTY_PAIR("not_indexed");
  dba.BuildIndex(label, property.second);
  {
    auto node = NODE("n");
    auto lit_42 = LITERAL(42);
    node->properties_[property] = lit_42;
    QUERY(SINGLE_QUERY(
        MATCH(PATTERN(node)),
        WHERE(AND(PROPERTY_LOOKUP("n", not_indexed),
                  storage.Create<query::LabelsTest>(
                      IDENT("n"), std::vector<storage::Label>{label}))),
        RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(planner.plan(), symbol_table,
              ExpectScanAllByLabelPropertyValue(label, property, lit_42),
              ExpectFilter(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, WhereIndexedLabelProperty) {
  // Test MATCH (n :label) WHERE n.property = 42 RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = PROPERTY_PAIR("property");
  dba.BuildIndex(label, property.second);
  {
    auto lit_42 = LITERAL(42);
    QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))),
                       WHERE(EQ(PROPERTY_LOOKUP("n", property), lit_42)),
                       RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(planner.plan(), symbol_table,
              ExpectScanAllByLabelPropertyValue(label, property, lit_42),
              ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, BestPropertyIndexed) {
  // Test MATCH (n :label) WHERE n.property = 1 AND n.better = 42 RETURN n
  AstTreeStorage storage;
  database::SingleNode db;
  auto label = database::GraphDbAccessor(db).Label("label");
  auto property = database::GraphDbAccessor(db).Property("property");
  {
    database::GraphDbAccessor(db).BuildIndex(label, property);
    database::GraphDbAccessor dba(db);
    // Add a vertex with :label+property combination, so that the best
    // :label+better remains empty and thus better choice.
    auto vertex = dba.InsertVertex();
    vertex.add_label(label);
    vertex.PropsSet(property, 1);
    dba.Commit();
  }
  ASSERT_EQ(database::GraphDbAccessor(db).VerticesCount(label, property), 1);
  auto better = PROPERTY_PAIR("better");
  database::GraphDbAccessor(db).BuildIndex(label, better.second);
  {
    database::GraphDbAccessor dba(db);
    ASSERT_EQ(dba.VerticesCount(label, better.second), 0);
    auto lit_42 = LITERAL(42);
    QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))),
                       WHERE(AND(EQ(PROPERTY_LOOKUP("n", property), LITERAL(1)),
                                 EQ(PROPERTY_LOOKUP("n", better), lit_42))),
                       RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(planner.plan(), symbol_table,
              ExpectScanAllByLabelPropertyValue(label, better, lit_42),
              ExpectFilter(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, MultiPropertyIndexScan) {
  // Test MATCH (n :label1), (m :label2) WHERE n.prop1 = 1 AND m.prop2 = 2
  //      RETURN n, m
  database::SingleNode db;
  auto label1 = database::GraphDbAccessor(db).Label("label1");
  auto label2 = database::GraphDbAccessor(db).Label("label2");
  auto prop1 = PROPERTY_PAIR("prop1");
  auto prop2 = PROPERTY_PAIR("prop2");
  database::GraphDbAccessor(db).BuildIndex(label1, prop1.second);
  database::GraphDbAccessor(db).BuildIndex(label2, prop2.second);
  AstTreeStorage storage;
  auto lit_1 = LITERAL(1);
  auto lit_2 = LITERAL(2);
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n", label1)), PATTERN(NODE("m", label2))),
      WHERE(AND(EQ(PROPERTY_LOOKUP("n", prop1), lit_1),
                EQ(PROPERTY_LOOKUP("m", prop2), lit_2))),
      RETURN("n", "m")));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckPlan(planner.plan(), symbol_table,
            ExpectScanAllByLabelPropertyValue(label1, prop1, lit_1),
            ExpectScanAllByLabelPropertyValue(label2, prop2, lit_2),
            ExpectProduce());
}

TYPED_TEST(TestPlanner, WhereIndexedLabelPropertyRange) {
  // Test MATCH (n :label) WHERE n.property REL_OP 42 RETURN n
  // REL_OP is one of: `<`, `<=`, `>`, `>=`
  database::SingleNode db;
  auto label = database::GraphDbAccessor(db).Label("label");
  auto property = database::GraphDbAccessor(db).Property("property");
  database::GraphDbAccessor(db).BuildIndex(label, property);
  AstTreeStorage storage;
  auto lit_42 = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP("n", property);
  auto check_planned_range = [&label, &property, &db](const auto &rel_expr,
                                                      auto lower_bound,
                                                      auto upper_bound) {
    // Shadow the first storage, so that the query is created in this one.
    AstTreeStorage storage;
    QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))), WHERE(rel_expr),
                       RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(planner.plan(), symbol_table,
              ExpectScanAllByLabelPropertyRange(label, property, lower_bound,
                                                upper_bound),
              ExpectProduce());
  };
  {
    // Test relation operators which form an upper bound for range.
    std::vector<std::pair<query::Expression *, Bound::Type>> upper_bound_rel_op{
        std::make_pair(LESS(n_prop, lit_42), Bound::Type::EXCLUSIVE),
        std::make_pair(LESS_EQ(n_prop, lit_42), Bound::Type::INCLUSIVE),
        std::make_pair(GREATER(lit_42, n_prop), Bound::Type::EXCLUSIVE),
        std::make_pair(GREATER_EQ(lit_42, n_prop), Bound::Type::INCLUSIVE)};
    for (const auto &rel_op : upper_bound_rel_op) {
      check_planned_range(rel_op.first, std::experimental::nullopt,
                          Bound(lit_42, rel_op.second));
    }
  }
  {
    // Test relation operators which form a lower bound for range.
    std::vector<std::pair<query::Expression *, Bound::Type>> lower_bound_rel_op{
        std::make_pair(LESS(lit_42, n_prop), Bound::Type::EXCLUSIVE),
        std::make_pair(LESS_EQ(lit_42, n_prop), Bound::Type::INCLUSIVE),
        std::make_pair(GREATER(n_prop, lit_42), Bound::Type::EXCLUSIVE),
        std::make_pair(GREATER_EQ(n_prop, lit_42), Bound::Type::INCLUSIVE)};
    for (const auto &rel_op : lower_bound_rel_op) {
      check_planned_range(rel_op.first, Bound(lit_42, rel_op.second),
                          std::experimental::nullopt);
    }
  }
}

TYPED_TEST(TestPlanner, UnableToUsePropertyIndex) {
  // Test MATCH (n: label) WHERE n.property = n.property RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = dba.Property("property");
  dba.BuildIndex(label, property);
  {
    AstTreeStorage storage;
    QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))),
                       WHERE(EQ(PROPERTY_LOOKUP("n", property),
                                PROPERTY_LOOKUP("n", property))),
                       RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    // We can only get ScanAllByLabelIndex, because we are comparing properties
    // with those on the same node.
    CheckPlan(planner.plan(), symbol_table, ExpectScanAllByLabel(),
              ExpectFilter(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, SecondPropertyIndex) {
  // Test MATCH (n :label), (m :label) WHERE m.property = n.property RETURN n
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  auto property = PROPERTY_PAIR("property");
  dba.BuildIndex(label, dba.Property("property"));
  {
    AstTreeStorage storage;
    auto n_prop = PROPERTY_LOOKUP("n", property);
    auto m_prop = PROPERTY_LOOKUP("m", property);
    QUERY(SINGLE_QUERY(
        MATCH(PATTERN(NODE("n", label)), PATTERN(NODE("m", label))),
        WHERE(EQ(m_prop, n_prop)), RETURN("n")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    CheckPlan(
        planner.plan(), symbol_table, ExpectScanAllByLabel(),
        // Note: We are scanning for m, therefore property should equal n_prop.
        ExpectScanAllByLabelPropertyValue(label, property, n_prop),
        ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, ReturnSumGroupByAll) {
  // Test RETURN sum([1,2,3]), all(x in [1] where x = 1)
  AstTreeStorage storage;
  auto sum = SUM(LIST(LITERAL(1), LITERAL(2), LITERAL(3)));
  auto *all = ALL("x", LIST(LITERAL(1)), WHERE(EQ(IDENT("x"), LITERAL(1))));
  QUERY(SINGLE_QUERY(RETURN(sum, AS("sum"), all, AS("all"))));
  auto aggr = ExpectAggregate({sum}, {all});
  CheckPlan<TypeParam>(storage, aggr, ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchExpandVariable) {
  // Test MATCH (n) -[r *..3]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r");
  edge->upper_bound_ = LITERAL(3);
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpandVariable(),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchExpandVariableNoBounds) {
  // Test MATCH (n) -[r *]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpandVariable(),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchExpandVariableInlinedFilter) {
  // Test MATCH (n) -[r :type * {prop: 42}]-> (m) RETURN r
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto type = dba.EdgeType("type");
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r", Direction::BOTH, {type});
  edge->properties_[prop] = LITERAL(42);
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(
      storage, ExpectScanAll(),
      ExpectExpandVariable(),  // Filter is both inlined and post-expand
      ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchExpandVariableNotInlinedFilter) {
  // Test MATCH (n) -[r :type * {prop: m.prop}]-> (m) RETURN r
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto type = dba.EdgeType("type");
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r", Direction::BOTH, {type});
  edge->properties_[prop] = EQ(PROPERTY_LOOKUP("m", prop), LITERAL(42));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpandVariable(),
                       ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, UnwindMatchVariable) {
  // Test UNWIND [1,2,3] AS depth MATCH (n) -[r*d]-> (m) RETURN r
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r", Direction::OUT);
  edge->lower_bound_ = IDENT("d");
  edge->upper_bound_ = IDENT("d");
  QUERY(SINGLE_QUERY(UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("d")),
                     MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(storage, ExpectUnwind(), ExpectScanAll(),
                       ExpectExpandVariable(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchBreadthFirst) {
  // Test MATCH (n) -[r:type *..10 (r, n|n)]-> (m) RETURN r
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto edge_type = dba.EdgeType("type");
  AstTreeStorage storage;
  auto *bfs = storage.Create<query::EdgeAtom>(
      IDENT("r"), query::EdgeAtom::Type::BREADTH_FIRST, Direction::OUT,
      std::vector<storage::EdgeType>{edge_type});
  bfs->filter_lambda_.inner_edge = IDENT("r");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = IDENT("n");
  bfs->upper_bound_ = LITERAL(10);
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r")));
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectExpandBreadthFirst(),
                       ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchDoubleScanToExpandExisting) {
  // Test MATCH (n) -[r]- (m :label) RETURN r
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto label = dba.Label("label");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m", label))),
                     RETURN("r")));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  // We expect 2x ScanAll and then Expand, since we are guessing that is
  // faster (due to low label index vertex count).
  CheckPlan(planner.plan(), symbol_table, ExpectScanAll(),
            ExpectScanAllByLabel(), ExpectExpand(), ExpectProduce());
}

TYPED_TEST(TestPlanner, MatchScanToExpand) {
  // Test MATCH (n) -[r]- (m :label {property: 1}) RETURN r
  database::SingleNode db;
  auto label = database::GraphDbAccessor(db).Label("label");
  auto property = database::GraphDbAccessor(db).Property("property");
  database::GraphDbAccessor(db).BuildIndex(label, property);
  database::GraphDbAccessor dba(db);
  // Fill vertices to the max.
  for (int64_t i = 0; i < FLAGS_query_vertex_count_to_expand_existing; ++i) {
    auto vertex = dba.InsertVertex();
    vertex.PropsSet(property, 1);
    vertex.add_label(label);
  }
  // Add one more above the max.
  auto vertex = dba.InsertVertex();
  vertex.add_label(label);
  vertex.PropsSet(property, 1);
  dba.Commit();
  {
    AstTreeStorage storage;
    auto node_m = NODE("m", label);
    node_m->properties_[std::make_pair("property", property)] = LITERAL(1);
    QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), node_m)),
                       RETURN("r")));
    auto symbol_table = MakeSymbolTable(*storage.query());
    auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
    // We expect 1x ScanAllByLabel and then Expand, since we are guessing that
    // is faster (due to high label index vertex count).
    CheckPlan(planner.plan(), symbol_table, ExpectScanAll(), ExpectExpand(),
              ExpectFilter(), ExpectProduce());
  }
}

TYPED_TEST(TestPlanner, MatchWhereAndSplit) {
  // Test MATCH (n) -[r]- (m) WHERE n.prop AND r.prop RETURN m
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  auto prop = PROPERTY_PAIR("prop");
  AstTreeStorage storage;
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
      WHERE(AND(PROPERTY_LOOKUP("n", prop), PROPERTY_LOOKUP("r", prop))),
      RETURN("m")));
  // We expect `n.prop` filter right after scanning `n`.
  CheckPlan<TypeParam>(storage, ExpectScanAll(), ExpectFilter(), ExpectExpand(),
                       ExpectFilter(), ExpectProduce());
}

TYPED_TEST(TestPlanner, ReturnAsteriskOmitsLambdaSymbols) {
  // Test MATCH (n) -[r* (ie, in | true)]- (m) RETURN *
  database::SingleNode db;
  AstTreeStorage storage;
  auto edge = EDGE_VARIABLE("r", Direction::BOTH);
  edge->filter_lambda_.inner_edge = IDENT("ie");
  edge->filter_lambda_.inner_node = IDENT("in");
  edge->filter_lambda_.expression = LITERAL(true);
  auto ret = storage.Create<query::Return>();
  ret->body_.all_identifiers = true;
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), ret));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  auto *produce = dynamic_cast<Produce *>(&planner.plan());
  ASSERT_TRUE(produce);
  std::vector<std::string> outputs;
  for (const auto &output_symbol : produce->OutputSymbols(symbol_table)) {
    outputs.emplace_back(output_symbol.name());
  }
  // We expect `*` expanded to `n`, `r` and `m`.
  EXPECT_EQ(outputs.size(), 3);
  for (const auto &name : {"n", "r", "m"}) {
    EXPECT_TRUE(utils::Contains(outputs, name));
  }
}

TYPED_TEST(TestPlanner, DistributedAvg) {
  // Test MATCH (n) RETURN AVG(n.prop) AS res
  AstTreeStorage storage;
  database::Master db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                     RETURN(AVG(PROPERTY_LOOKUP("n", prop)), AS("res"))));
  auto distributed_plan = MakeDistributedPlan<TypeParam>(storage);
  auto &symbol_table = distributed_plan.symbol_table;
  auto worker_sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto worker_count = COUNT(PROPERTY_LOOKUP("n", prop));
  {
    ASSERT_EQ(distributed_plan.worker_plans.size(), 1U);
    auto worker_plan = distributed_plan.worker_plans.back().second;
    auto worker_aggr_op = std::dynamic_pointer_cast<Aggregate>(worker_plan);
    ASSERT_TRUE(worker_aggr_op);
    ASSERT_EQ(worker_aggr_op->aggregations().size(), 2U);
    symbol_table[*worker_sum] = worker_aggr_op->aggregations()[0].output_sym;
    symbol_table[*worker_count] = worker_aggr_op->aggregations()[1].output_sym;
  }
  auto worker_aggr = ExpectAggregate({worker_sum, worker_count}, {});
  auto merge_sum = SUM(IDENT("worker_sum"));
  auto merge_count = SUM(IDENT("worker_count"));
  auto master_aggr = ExpectMasterAggregate({merge_sum, merge_count}, {});
  ExpectPullRemote pull(
      {symbol_table.at(*worker_sum), symbol_table.at(*worker_count)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), worker_aggr, pull, master_aggr,
                   ExpectProduce(), ExpectProduce()),
      MakeCheckers(ExpectScanAll(), worker_aggr));
  CheckDistributedPlan(distributed_plan, expected);
}

TYPED_TEST(TestPlanner, DistributedCollectList) {
  // Test MATCH (n) RETURN COLLECT(n.prop) AS res
  AstTreeStorage storage;
  database::Master db;
  database::GraphDbAccessor dba(db);
  auto prop = dba.Property("prop");
  auto node_n = NODE("n");
  auto collect = COLLECT_LIST(PROPERTY_LOOKUP("n", prop));
  QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), RETURN(collect, AS("res"))));
  auto distributed_plan = MakeDistributedPlan<TypeParam>(storage);
  auto &symbol_table = distributed_plan.symbol_table;
  auto aggr = ExpectAggregate({collect}, {});
  ExpectPullRemote pull({symbol_table.at(*node_n->identifier_)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), pull, aggr, ExpectProduce()),
      MakeCheckers(ExpectScanAll()));
  CheckDistributedPlan(distributed_plan, expected);
}

TYPED_TEST(TestPlanner, DistributedMatchCreateReturn) {
  // Test MATCH (n) CREATE (m) RETURN m
  AstTreeStorage storage;
  auto *ident_m = IDENT("m");
  QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("m"))),
                     RETURN(ident_m, AS("m"))));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto acc = ExpectAccumulate({symbol_table.at(*ident_m)});
  database::Master db;
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectCreateNode(),
                   ExpectSynchronize({symbol_table.at(*ident_m)}),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectCreateNode()));
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianCreate) {
  // Test MATCH (a), (b) CREATE (a)-[e:r]->(b)
  AstTreeStorage storage;
  database::Master db;
  database::GraphDbAccessor dba(db);
  auto relationship = dba.EdgeType("r");
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b)),
      CREATE(PATTERN(NODE("a"), EDGE("e", Direction::OUT, {relationship}),
                     NODE("b")))));
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto left_cart =
      MakeCheckers(ExpectScanAll(),
                   ExpectPullRemote({symbol_table.at(*node_a->identifier_)}));
  auto right_cart =
      MakeCheckers(ExpectScanAll(),
                   ExpectPullRemote({symbol_table.at(*node_b->identifier_)}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectCartesian(std::move(left_cart), std::move(right_cart)),
                   ExpectCreateExpand(), ExpectSynchronize(false)),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  auto planner = MakePlanner<TypeParam>(db, storage, symbol_table);
  CheckDistributedPlan(planner.plan(), symbol_table, expected);
}

}  // namespace
