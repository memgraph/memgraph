#include "query_plan_checker.hpp"

#include <memory>
#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>

#include "database/distributed/graph_db.hpp"
#include "distributed/coordination.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/data_rpc_clients.hpp"
#include "distributed/data_rpc_server.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed_common.hpp"
#include "io/network/endpoint.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpreter.hpp"
#include "query/plan/distributed.hpp"
#include "query/plan/distributed_ops.hpp"
#include "query/plan/planner.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"

DECLARE_int32(query_execution_time_sec);

using namespace distributed;
using namespace database;
using namespace std::literals::chrono_literals;
using Direction = query::EdgeAtom::Direction;

ExpandTuple MakeDistributedExpand(
    AstStorage &storage, SymbolTable &symbol_table,
    std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
    const std::string &edge_identifier, EdgeAtom::Direction direction,
    const std::vector<storage::EdgeType> &edge_types,
    const std::string &node_identifier, bool existing_node,
    GraphView graph_view) {
  auto edge = EDGE(edge_identifier, direction);
  auto edge_sym = symbol_table.CreateSymbol(edge_identifier, true);
  edge->identifier_->MapTo(edge_sym);

  auto node = NODE(node_identifier);
  auto node_sym = symbol_table.CreateSymbol(node_identifier, true);
  node->identifier_->MapTo(node_sym);

  auto op = std::make_shared<DistributedExpand>(input, input_symbol, node_sym,
                                                edge_sym, direction, edge_types,
                                                existing_node, graph_view);

  return ExpandTuple{edge, edge_sym, node, node_sym, op};
}

class DistributedQueryPlan : public DistributedGraphDbTest {
 protected:
  DistributedQueryPlan() : DistributedGraphDbTest("query_plan") {}
};

TEST_F(DistributedQueryPlan, PullProduceRpc) {
  auto dba = master().Access();
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;

  // Query plan for: UNWIND [42, true, "bla", 1, 2] as x RETURN x
  using namespace query;
  auto list =
      LIST(LITERAL(42), LITERAL(true), LITERAL("bla"), LITERAL(1), LITERAL(2));
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, list, x);
  auto x_expr = IDENT("x")->MapTo(x);
  auto x_ne =
      NEXPR("x", x_expr)->MapTo(symbol_table.CreateSymbol("x_ne", true));
  auto produce = MakeProduce(unwind, x_ne);

  // Test that the plan works locally.
  auto ctx = MakeContext(storage, symbol_table, dba.get());
  auto results = CollectProduce(*produce, &ctx);
  ASSERT_EQ(results.size(), 5);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, ctx.symbol_table);

  tx::CommandId command_id = dba->transaction().cid();
  auto &evaluation_context = ctx.evaluation_context;
  std::vector<query::Symbol> symbols{ctx.symbol_table.at(*x_ne)};
  auto remote_pull = [this, &command_id, &evaluation_context, &symbols](
                         GraphDbAccessor &dba, int worker_id) {
    return master().pull_clients().Pull(&dba, worker_id, plan_id, command_id,
                                        evaluation_context, symbols, false, 3);
  };
  auto expect_first_batch = [](auto &batch) {
    EXPECT_EQ(batch.pull_state, distributed::PullState::CURSOR_IN_PROGRESS);
    ASSERT_EQ(batch.frames.size(), 3);
    ASSERT_EQ(batch.frames[0].size(), 1);
    EXPECT_EQ(batch.frames[0][0].ValueInt(), 42);
    EXPECT_EQ(batch.frames[1][0].ValueBool(), true);
    EXPECT_EQ(batch.frames[2][0].ValueString(), "bla");
  };
  auto expect_second_batch = [](auto &batch) {
    EXPECT_EQ(batch.pull_state, distributed::PullState::CURSOR_EXHAUSTED);
    ASSERT_EQ(batch.frames.size(), 2);
    ASSERT_EQ(batch.frames[0].size(), 1);
    EXPECT_EQ(batch.frames[0][0].ValueInt(), 1);
    EXPECT_EQ(batch.frames[1][0].ValueInt(), 2);
  };

  auto dba_1 = master().Access();
  auto dba_2 = master().Access();
  for (int worker_id : {1, 2}) {
    // TODO flor, proper test async here.
    auto tx1_batch1 = remote_pull(*dba_1, worker_id).get();
    expect_first_batch(tx1_batch1);
    auto tx2_batch1 = remote_pull(*dba_2, worker_id).get();
    expect_first_batch(tx2_batch1);
    auto tx2_batch2 = remote_pull(*dba_2, worker_id).get();
    expect_second_batch(tx2_batch2);
    auto tx1_batch2 = remote_pull(*dba_1, worker_id).get();
    expect_second_batch(tx1_batch2);
  }
}

TEST_F(DistributedQueryPlan, PullProduceRpcWithGraphElements) {
  // Create some data on the master and both workers. Eeach edge (3 of them) and
  // vertex (6 of them) will be uniquely identified with their worker id and
  // sequence ID, so we can check we retrieved all.
  storage::Property prop;
  {
    auto dba = master().Access();
    prop = dba->Property("prop");
    auto create_data = [prop](GraphDbAccessor &dba, int worker_id) {
      auto v1 = dba.InsertVertex();
      v1.PropsSet(prop, worker_id * 10);
      auto v2 = dba.InsertVertex();
      v2.PropsSet(prop, worker_id * 10 + 1);
      auto e12 = dba.InsertEdge(v1, v2, dba.EdgeType("et"));
      e12.PropsSet(prop, worker_id * 10 + 2);
    };
    create_data(*dba, 0);
    auto dba_w1 = worker(1).Access(dba->transaction_id());
    create_data(*dba_w1, 1);
    auto dba_w2 = worker(2).Access(dba->transaction_id());
    create_data(*dba_w2, 2);
    dba->Commit();
  }

  auto dba = master().Access();
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;

  // Query plan for: MATCH p = (n)-[r]->(m) return [n, r], m, p
  // Use this query to test graph elements are transferred correctly in
  // collections too.
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeDistributedExpand(storage, symbol_table, n.op_, n.sym_, "r",
                                   EdgeAtom::Direction::OUT, {}, "m", false,
                                   GraphView::OLD);
  auto p_sym = symbol_table.CreateSymbol("p", true);
  auto p = std::make_shared<query::plan::ConstructNamedPath>(
      r_m.op_, p_sym,
      std::vector<Symbol>{n.sym_, r_m.edge_sym_, r_m.node_sym_});
  auto return_n = IDENT("n")->MapTo(n.sym_);
  auto return_r = IDENT("r")->MapTo(r_m.edge_sym_);
  auto return_n_r = NEXPR("[n, r]", LIST(return_n, return_r))
                        ->MapTo(symbol_table.CreateSymbol("", true));
  auto return_m = NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
                      ->MapTo(symbol_table.CreateSymbol("", true));
  auto return_p = NEXPR("p", IDENT("p")->MapTo(p_sym))
                      ->MapTo(symbol_table.CreateSymbol("", true));
  auto produce = MakeProduce(p, return_n_r, return_m, return_p);

  auto check_result = [prop](int worker_id,
                             const std::vector<std::vector<query::TypedValue>>
                                 &frames) {
    int offset = worker_id * 10;
    ASSERT_EQ(frames.size(), 1);
    auto &row = frames[0];
    ASSERT_EQ(row.size(), 3);
    auto &list = row[0].ValueList();
    ASSERT_EQ(list.size(), 2);
    ASSERT_EQ(list[0].ValueVertex().PropsAt(prop).Value<int64_t>(), offset);
    ASSERT_EQ(list[1].ValueEdge().PropsAt(prop).Value<int64_t>(), offset + 2);
    ASSERT_EQ(row[1].ValueVertex().PropsAt(prop).Value<int64_t>(), offset + 1);
    auto &path = row[2].ValuePath();
    ASSERT_EQ(path.size(), 1);
    ASSERT_EQ(path.vertices()[0].PropsAt(prop).Value<int64_t>(), offset);
    ASSERT_EQ(path.edges()[0].PropsAt(prop).Value<int64_t>(), offset + 2);
    ASSERT_EQ(path.vertices()[1].PropsAt(prop).Value<int64_t>(), offset + 1);
  };

  // Test that the plan works locally.
  auto ctx = MakeContext(storage, symbol_table, dba.get());
  auto results = CollectProduce(*produce, &ctx);
  check_result(0, results);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, ctx.symbol_table);

  tx::CommandId command_id = dba->transaction().cid();
  auto &evaluation_context = ctx.evaluation_context;
  std::vector<query::Symbol> symbols{ctx.symbol_table.at(*return_n_r),
                                     ctx.symbol_table.at(*return_m), p_sym};
  auto remote_pull = [this, &command_id, &evaluation_context, &symbols](
                         GraphDbAccessor &dba, int worker_id) {
    return master().pull_clients().Pull(&dba, worker_id, plan_id, command_id,
                                        evaluation_context, symbols, false, 3);
  };
  auto future_w1_results = remote_pull(*dba, 1);
  auto future_w2_results = remote_pull(*dba, 2);
  check_result(1, future_w1_results.get().frames);
  check_result(2, future_w2_results.get().frames);
}

TEST_F(DistributedQueryPlan, Synchronize) {
  auto from = InsertVertex(worker(1));
  auto to = InsertVertex(worker(2));
  InsertEdge(from, to, "et");

  // Query: MATCH (n)--(m) SET m.prop = 2 RETURN n.prop
  // This query ensures that a remote update gets applied and the local stuff
  // gets reconstructed.
  auto dba_ptr = master().Access();
  auto &dba = *dba_ptr;
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;
  // MATCH
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeDistributedExpand(storage, symbol_table, n.op_, n.sym_, "r",
                                   EdgeAtom::Direction::BOTH, {}, "m", false,
                                   GraphView::OLD);

  // SET
  auto literal = LITERAL(42);
  auto prop = PROPERTY_PAIR("prop");
  auto m_p = PROPERTY_LOOKUP(IDENT("m")->MapTo(r_m.node_sym_), prop);
  auto set_m_p =
      std::make_shared<plan::SetProperty>(r_m.op_, prop.second, m_p, literal);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, set_m_p, symbol_table);

  // Master-side PullRemote, Synchronize
  auto pull_remote = std::make_shared<query::plan::PullRemote>(
      nullptr, plan_id, std::vector<Symbol>{n.sym_});
  auto synchronize =
      std::make_shared<query::plan::Synchronize>(set_m_p, pull_remote, true);

  // RETURN
  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto return_n_p =
      NEXPR("n.prop", n_p)->MapTo(symbol_table.CreateSymbol("n.p", true));
  auto produce = MakeProduce(synchronize, return_n_p);
  auto ctx = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &ctx);
  ASSERT_EQ(results.size(), 2);
  ASSERT_EQ(results[0].size(), 1);
  EXPECT_EQ(results[0][0].ValueInt(), 42);
  ASSERT_EQ(results[1].size(), 1);
  EXPECT_EQ(results[1][0].ValueInt(), 42);

  // TODO test without advance command?
}

TEST_F(DistributedQueryPlan, Create) {
  // Query: UNWIND range(0, 1000) as x CREATE ()
  auto dba = master().Access();
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;
  auto range = FN("range", LITERAL(0), LITERAL(1000));
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, range, x);
  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  auto create =
      std::make_shared<query::plan::DistributedCreateNode>(unwind, node, true);
  auto context = MakeContext(storage, symbol_table, dba.get());
  PullAll(*create, &context);
  dba->Commit();
  EXPECT_GT(VertexCount(master()), 200);
  EXPECT_GT(VertexCount(worker(1)), 200);
  EXPECT_GT(VertexCount(worker(2)), 200);
}

TEST_F(DistributedQueryPlan, PullRemoteOrderBy) {
  // Create some data on the master and both workers.
  storage::Property prop;
  {
    auto dba = master().Access();
    auto tx_id = dba->transaction_id();
    auto dba1 = worker(1).Access(tx_id);
    auto dba2 = worker(2).Access(tx_id);
    prop = dba->Property("prop");
    auto add_data = [prop](GraphDbAccessor &dba, int value) {
      dba.InsertVertex().PropsSet(prop, value);
    };

    std::vector<int> data;
    for (int i = 0; i < 300; ++i) data.push_back(i);
    std::random_shuffle(data.begin(), data.end());

    for (int i = 0; i < 100; ++i) add_data(*dba, data[i]);
    for (int i = 100; i < 200; ++i) add_data(*dba1, data[i]);
    for (int i = 200; i < 300; ++i) add_data(*dba2, data[i]);

    dba->Commit();
  }

  auto dba_ptr = master().Access();
  auto &dba = *dba_ptr;
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;

  // Query plan for:  MATCH (n) RETURN n.prop ORDER BY n.prop;
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
  auto order_by = std::make_shared<plan::OrderBy>(
      n.op_, std::vector<SortItem>{{Ordering::ASC, n_p}},
      std::vector<Symbol>{n.sym_});

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, order_by, symbol_table);

  auto pull_remote_order_by = std::make_shared<plan::PullRemoteOrderBy>(
      order_by, plan_id, std::vector<SortItem>{{Ordering::ASC, n_p}},
      std::vector<Symbol>{n.sym_});

  auto n_p_ne =
      NEXPR("n.prop", n_p)->MapTo(symbol_table.CreateSymbol("n.prop", true));
  auto produce = MakeProduce(pull_remote_order_by, n_p_ne);
  auto ctx = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &ctx);

  ASSERT_EQ(results.size(), 300);
  for (int j = 0; j < 300; ++j) {
    EXPECT_TRUE(TypedValue::BoolEqual{}(results[j][0], j));
  }
}

class DistributedTransactionTimeout : public DistributedGraphDbTest {
 protected:
  DistributedTransactionTimeout()
      : DistributedGraphDbTest("transaction_timeout") {}
  int QueryExecutionTimeSec(int) override { return 1; }
};

TEST_F(DistributedTransactionTimeout, Timeout) {
  InsertVertex(worker(1));
  InsertVertex(worker(1));

  auto dba = master().Access();
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator{symbol_table};
  AstStorage storage;

  // Make distributed plan for MATCH (n) RETURN n
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto output =
      NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(scan_all.op_, output);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, symbol_table);
  tx::CommandId command_id = dba->transaction().cid();

  EvaluationContext evaluation_context;
  evaluation_context.properties =
      NamesToProperties(storage.properties_, dba.get());
  evaluation_context.labels = NamesToLabels(storage.labels_, dba.get());
  std::vector<query::Symbol> symbols{symbol_table.at(*output)};
  auto remote_pull = [this, &command_id, &evaluation_context, &symbols,
                      &dba]() {
    return master()
        .pull_clients()
        .Pull(dba.get(), 1, plan_id, command_id, evaluation_context, symbols,
              false, 1)
        .get()
        .pull_state;
  };
  ASSERT_EQ(remote_pull(), distributed::PullState::CURSOR_IN_PROGRESS);
  // Sleep here so the remote gets a hinted error.
  std::this_thread::sleep_for(2s);
  EXPECT_EQ(remote_pull(), distributed::PullState::HINTED_ABORT_ERROR);
}

class DistributedPlanChecker : public PlanChecker,
                               public DistributedOperatorVisitor {
 public:
  using PlanChecker::PlanChecker;
  using PlanChecker::PostVisit;
  using PlanChecker::PreVisit;
  using PlanChecker::Visit;

#define PRE_VISIT(TOp)              \
  bool PreVisit(TOp &op) override { \
    CheckOp(op);                    \
    return true;                    \
  }

  PRE_VISIT(PullRemote);
  PRE_VISIT(PullRemoteOrderBy);
  PRE_VISIT(DistributedExpand);
  PRE_VISIT(DistributedExpandBfs);
  PRE_VISIT(DistributedCreateNode);
  PRE_VISIT(DistributedCreateExpand);

  bool PreVisit(Synchronize &op) override {
    CheckOp(op);
    op.input()->Accept(*this);
    return false;
  }

#undef PRE_VISIT
};

using ExpectDistributedExpand = OpChecker<DistributedExpand>;
using ExpectDistributedExpandBfs = OpChecker<DistributedExpandBfs>;
using ExpectDistributedCreateExpand = OpChecker<DistributedCreateExpand>;

class ExpectDistributedOptional : public OpChecker<Optional> {
 public:
  explicit ExpectDistributedOptional(const std::list<BaseOpChecker *> &optional)
      : optional_(optional) {}

  ExpectDistributedOptional(const std::vector<Symbol> &optional_symbols,
                            const std::list<BaseOpChecker *> &optional)
      : optional_symbols_(optional_symbols), optional_(optional) {}

  void ExpectOp(Optional &optional, const SymbolTable &symbol_table) override {
    if (!optional_symbols_.empty()) {
      EXPECT_THAT(optional.optional_symbols_,
                  testing::UnorderedElementsAreArray(optional_symbols_));
    }
    DistributedPlanChecker check_optional(optional_, symbol_table);
    optional.optional_->Accept(check_optional);
  }

 private:
  std::vector<Symbol> optional_symbols_;
  const std::list<BaseOpChecker *> &optional_;
};

class ExpectDistributedCartesian : public OpChecker<Cartesian> {
 public:
  ExpectDistributedCartesian(
      const std::list<std::unique_ptr<BaseOpChecker>> &left,
      const std::list<std::unique_ptr<BaseOpChecker>> &right)
      : left_(left), right_(right) {}

  void ExpectOp(Cartesian &op, const SymbolTable &symbol_table) override {
    ASSERT_TRUE(op.left_op_);
    DistributedPlanChecker left_checker(left_, symbol_table);
    op.left_op_->Accept(left_checker);
    ASSERT_TRUE(op.right_op_);
    DistributedPlanChecker right_checker(right_, symbol_table);
    op.right_op_->Accept(right_checker);
  }

 private:
  const std::list<std::unique_ptr<BaseOpChecker>> &left_;
  const std::list<std::unique_ptr<BaseOpChecker>> &right_;
};

class ExpectMasterAggregate : public OpChecker<Aggregate> {
 public:
  ExpectMasterAggregate(const std::vector<query::Aggregation *> &aggregations,
                        const std::unordered_set<query::Expression *> &group_by)
      : aggregations_(aggregations), group_by_(group_by) {}

  void ExpectOp(Aggregate &op, const SymbolTable &symbol_table) override {
    auto aggr_it = aggregations_.begin();
    for (const auto &aggr_elem : op.aggregations_) {
      ASSERT_NE(aggr_it, aggregations_.end());
      auto aggr = *aggr_it++;
      // TODO: Proper expression equality
      EXPECT_EQ(typeid(aggr_elem.value).hash_code(),
                typeid(aggr->expression1_).hash_code());
      EXPECT_EQ(typeid(aggr_elem.key).hash_code(),
                typeid(aggr->expression2_).hash_code());
      EXPECT_EQ(aggr_elem.op, aggr->op_);
      // Skip checking virtual merge aggregation symbol when the plan is
      // distributed.
      // EXPECT_EQ(aggr_elem.output_sym, symbol_table.at(*aggr));
    }
    EXPECT_EQ(aggr_it, aggregations_.end());
    // TODO: Proper group by expression equality
    std::unordered_set<size_t> got_group_by;
    for (auto *expr : op.group_by_)
      got_group_by.insert(typeid(*expr).hash_code());
    std::unordered_set<size_t> expected_group_by;
    for (auto *expr : group_by_)
      expected_group_by.insert(typeid(*expr).hash_code());
    EXPECT_EQ(got_group_by, expected_group_by);
  }

 private:
  std::vector<query::Aggregation *> aggregations_;
  std::unordered_set<query::Expression *> group_by_;
};

class ExpectPullRemote : public OpChecker<PullRemote> {
 public:
  ExpectPullRemote() {}
  ExpectPullRemote(const std::vector<Symbol> &symbols) : symbols_(symbols) {}

  void ExpectOp(PullRemote &op, const SymbolTable &) override {
    EXPECT_THAT(op.symbols_, testing::UnorderedElementsAreArray(symbols_));
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
      ASSERT_TRUE(op.pull_remote_);
      expect_pull_.ExpectOp(*op.pull_remote_, symbol_table);
    } else {
      EXPECT_FALSE(op.pull_remote_);
    }
    EXPECT_EQ(op.advance_command_, advance_command_);
  }

 private:
  ExpectPullRemote expect_pull_;
  bool has_pull_ = true;
  bool advance_command_ = false;
};

class ExpectDistributedCreateNode : public OpChecker<DistributedCreateNode> {
 public:
  ExpectDistributedCreateNode(bool on_random_worker = false)
      : on_random_worker_(on_random_worker) {}

  void ExpectOp(DistributedCreateNode &op, const SymbolTable &) override {
    EXPECT_EQ(op.on_random_worker_, on_random_worker_);
  }

 private:
  bool on_random_worker_ = false;
};

class ExpectPullRemoteOrderBy : public OpChecker<PullRemoteOrderBy> {
 public:
  ExpectPullRemoteOrderBy(const std::vector<Symbol> symbols)
      : symbols_(symbols) {}

  void ExpectOp(PullRemoteOrderBy &op, const SymbolTable &) override {
    EXPECT_THAT(op.symbols_, testing::UnorderedElementsAreArray(symbols_));
  }

 private:
  std::vector<Symbol> symbols_;
};

void SavePlan(const LogicalOperator &plan, ::capnp::MessageBuilder *message) {
  auto builder = message->initRoot<query::plan::capnp::LogicalOperator>();
  LogicalOperator::SaveHelper helper;
  Save(plan, &builder, &helper);
}

auto LoadPlan(const ::query::plan::capnp::LogicalOperator::Reader &reader) {
  std::unique_ptr<LogicalOperator> plan;
  LogicalOperator::LoadHelper helper;
  Load(&plan, reader, &helper);
  return std::make_pair(std::move(plan), std::move(helper.ast_storage));
}

class CapnpPlanner {
 public:
  template <class TDbAccessor>
  CapnpPlanner(std::vector<SingleQueryPart> single_query_parts,
               PlanningContext<TDbAccessor> context) {
    ::capnp::MallocMessageBuilder message;
    {
      auto original_plan = MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(
          single_query_parts, &context);
      SavePlan(*original_plan, &message);
    }
    {
      auto reader = message.getRoot<query::plan::capnp::LogicalOperator>();
      std::tie(plan_, ast_storage_) = LoadPlan(reader);
    }
  }

  auto &plan() { return *plan_; }

 private:
  AstStorage ast_storage_;
  std::unique_ptr<LogicalOperator> plan_;
};

struct ExpectedDistributedPlan {
  std::list<std::unique_ptr<BaseOpChecker>> master_checkers;
  std::vector<std::list<std::unique_ptr<BaseOpChecker>>> worker_checkers;
};

template <class TPlanner>
DistributedPlan MakeDistributedPlan(query::CypherQuery *query,
                                    query::AstStorage &storage) {
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TPlanner>(&dba, storage, symbol_table, query);
  std::atomic<int64_t> next_plan_id{0};
  return MakeDistributedPlan(storage, planner.plan(), symbol_table,
                             next_plan_id, {});
}

void CheckDistributedPlan(DistributedPlan &distributed_plan,
                          ExpectedDistributedPlan &expected) {
  DistributedPlanChecker plan_checker(expected.master_checkers,
                                      distributed_plan.symbol_table);
  distributed_plan.master_plan->Accept(plan_checker);
  EXPECT_TRUE(plan_checker.checkers_.empty());
  if (expected.worker_checkers.empty()) {
    EXPECT_TRUE(distributed_plan.worker_plans.empty());
  } else {
    ASSERT_EQ(distributed_plan.worker_plans.size(),
              expected.worker_checkers.size());
    for (size_t i = 0; i < expected.worker_checkers.size(); ++i) {
      DistributedPlanChecker plan_checker(expected.worker_checkers[i],
                                          distributed_plan.symbol_table);
      auto worker_plan = distributed_plan.worker_plans[i].second;
      worker_plan->Accept(plan_checker);
      EXPECT_TRUE(plan_checker.checkers_.empty());
    }
  }
}

void CheckDistributedPlan(
    const AstStorage &ast_storage, const LogicalOperator &plan,
    const SymbolTable &symbol_table,
    const std::vector<storage::Property> &properties_by_ix,
    ExpectedDistributedPlan &expected_distributed_plan) {
  std::atomic<int64_t> next_plan_id{0};
  auto distributed_plan = MakeDistributedPlan(ast_storage, plan, symbol_table,
                                              next_plan_id, properties_by_ix);
  EXPECT_EQ(next_plan_id - 1, distributed_plan.worker_plans.size());
  CheckDistributedPlan(distributed_plan, expected_distributed_plan);
}

template <class TPlanner>
void CheckDistributedPlan(query::CypherQuery *query, AstStorage &storage,
                          ExpectedDistributedPlan &expected_distributed_plan) {
  auto distributed_plan = MakeDistributedPlan<TPlanner>(query, storage);
  CheckDistributedPlan(distributed_plan, expected_distributed_plan);
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

class Planner {
 public:
  template <class TDbAccessor>
  Planner(std::vector<SingleQueryPart> single_query_parts,
          PlanningContext<TDbAccessor> context)
      : plan_(MakeLogicalPlanForSingleQuery<RuleBasedPlanner>(
            single_query_parts, &context)) {}

  auto &plan() { return *plan_; }

 private:
  std::unique_ptr<LogicalOperator> plan_;
};

template <class T>
class TestPlanner : public ::testing::Test {};

using PlannerTypes = ::testing::Types<Planner, CapnpPlanner>;

TYPED_TEST_CASE(TestPlanner, PlannerTypes);

TYPED_TEST(TestPlanner, MatchNodeReturn) {
  // Test MATCH (n) RETURN n
  AstStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN(as_n)));
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, CreateNodeReturn) {
  // Test CREATE (n) RETURN n AS n
  AstStorage storage;
  auto ident_n = IDENT("n");
  auto query =
      QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))), RETURN(ident_n, AS("n"))));
  auto symbol_table = query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCreateNode(true), ExpectSynchronize(false),
                   ExpectProduce()));
  std::atomic<int64_t> next_plan_id{0};
  auto distributed_plan = MakeDistributedPlan(storage, planner.plan(),
                                              symbol_table, next_plan_id, {});
  CheckDistributedPlan(distributed_plan, expected);
}

TYPED_TEST(TestPlanner, CreateExpand) {
  // Test CREATE (n) -[r :rel1]-> (m)
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "relationship";
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(
      NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")))));
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateExpand(), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, CreateMultipleNode) {
  // Test CREATE (n), (m)
  AstStorage storage;
  auto *query =
      QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n")), PATTERN(NODE("m")))));
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateNode(true), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, CreateNodeExpandNode) {
  // Test CREATE (n) -[r :rel]-> (m), (l)
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "rel";
  auto *query = QUERY(SINGLE_QUERY(CREATE(
      PATTERN(NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")),
      PATTERN(NODE("l")))));
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateExpand(),
                   ExpectDistributedCreateNode(true), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, CreateNamedPattern) {
  // Test CREATE p = (n) -[r :rel]-> (m)
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "rel";
  auto *query = QUERY(SINGLE_QUERY(CREATE(NAMED_PATTERN(
      "p", NODE("n"), EDGE("r", Direction::OUT, {relationship}), NODE("m")))));
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateExpand(), ExpectConstructNamedPath(),
                   ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchCreateExpand) {
  // Test MATCH (n) CREATE (n) -[r :rel1]-> (m)
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "relationship";
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      CREATE(PATTERN(NODE("n"), EDGE("r", Direction::OUT, {relationship}),
                     NODE("m")))));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateExpand(),
                   ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateExpand()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchLabeledNodes) {
  // Test MATCH (n :label) RETURN n
  AstStorage storage;
  FakeDbAccessor dba;
  auto label = "label";
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n", label))), RETURN(as_n)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAllByLabel(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAllByLabel(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchPathReturn) {
  // Test MATCH (n) -[r :relationship]- (m) RETURN n
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "relationship";
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", Direction::BOTH, {relationship}),
                    NODE("m"))),
      RETURN(as_n)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchNamedPatternReturn) {
  // Test MATCH p = (n) -[r :relationship]- (m) RETURN p
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "relationship";
  auto *as_p = NEXPR("p", IDENT("p"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(NAMED_PATTERN("p", NODE("n"),
                          EDGE("r", Direction::BOTH, {relationship}),
                          NODE("m"))),
      RETURN(as_p)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_p)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                   ExpectConstructNamedPath(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                   ExpectConstructNamedPath(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchNamedPatternWithPredicateReturn) {
  // Test MATCH p = (n) -[r :relationship]- (m) WHERE 2 = p RETURN p
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "relationship";
  auto *as_p = NEXPR("p", IDENT("p"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(NAMED_PATTERN("p", NODE("n"),
                          EDGE("r", Direction::BOTH, {relationship}),
                          NODE("m"))),
      WHERE(EQ(LITERAL(2), IDENT("p"))), RETURN(as_p)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_p)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectConstructNamedPath(), ExpectFilter(),
                                     ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectConstructNamedPath(), ExpectFilter(),
                                     ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, OptionalMatchNamedPatternReturn) {
  // Test OPTIONAL MATCH p = (n) -[r]- (m) RETURN p
  AstStorage storage;
  auto node_n = NODE("n");
  auto edge = EDGE("r");
  auto node_m = NODE("m");
  auto pattern = NAMED_PATTERN("p", node_n, edge, node_m);
  auto as_p = AS("p");
  auto *query = QUERY(SINGLE_QUERY(OPTIONAL_MATCH(pattern), RETURN("p", as_p)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto get_symbol = [&symbol_table](const auto *ast_node) {
    return symbol_table.at(*ast_node->identifier_);
  };
  std::vector<Symbol> optional_symbols{get_symbol(pattern), get_symbol(node_n),
                                       get_symbol(edge), get_symbol(node_m)};
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::list<BaseOpChecker *> optional{
      new ExpectScanAll(), new ExpectDistributedExpand(),
      new ExpectConstructNamedPath(), new ExpectPullRemote(optional_symbols)};
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedOptional(optional_symbols, optional),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                   ExpectConstructNamedPath()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchWhereReturn) {
  // Test MATCH (n) WHERE n.property < 42 RETURN n
  AstStorage storage;
  FakeDbAccessor dba;
  auto property = dba.Property("property");
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      WHERE(LESS(PROPERTY_LOOKUP("n", property), LITERAL(42))), RETURN(as_n)));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchDelete) {
  // Test MATCH (n) DELETE n
  AstStorage storage;
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n"))));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDelete(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectDelete()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchNodeSet) {
  // Test MATCH (n) SET n.prop = 42, n = n, n :label
  AstStorage storage;
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  auto label = "label";
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                   SET(PROPERTY_LOOKUP("n", prop), LITERAL(42)),
                                   SET("n", IDENT("n")), SET("n", {label})));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectSetProperty(), ExpectSetProperties(),
                   ExpectSetLabels(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectSetProperty(), ExpectSetProperties(),
                   ExpectSetLabels()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchRemove) {
  // Test MATCH (n) REMOVE n.prop REMOVE n :label
  AstStorage storage;
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  auto label = "label";
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                   REMOVE(PROPERTY_LOOKUP("n", prop)),
                                   REMOVE("n", {label})));
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectRemoveProperty(),
                                     ExpectRemoveLabels(), ExpectSynchronize()),
                        MakeCheckers(ExpectScanAll(), ExpectRemoveProperty(),
                                     ExpectRemoveLabels()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MultiMatch) {
  // Test MATCH (n) -[r]- (m) MATCH (j) -[e]- (i) -[f]- (h) RETURN n
  AstStorage storage;
  auto *node_n = NODE("n");
  auto *edge_r = EDGE("r");
  auto *node_m = NODE("m");
  auto *node_j = NODE("j");
  auto *edge_e = EDGE("e");
  auto *node_i = NODE("i");
  auto *edge_f = EDGE("f");
  auto *node_h = NODE("h");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_n, edge_r, node_m)),
      MATCH(PATTERN(node_j, edge_e, node_i, edge_f, node_h)), RETURN("n")));
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  auto get_symbol = [&symbol_table](const auto *atom_node) {
    return symbol_table.at(*atom_node->identifier_);
  };
  ExpectPullRemote left_pull(
      {get_symbol(node_n), get_symbol(edge_r), get_symbol(node_m)});
  auto left_cart =
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(), left_pull);
  ExpectPullRemote right_pull({get_symbol(node_j), get_symbol(edge_e),
                               get_symbol(node_i), get_symbol(edge_f),
                               get_symbol(node_h)});
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                 ExpectDistributedExpand(),
                                 ExpectEdgeUniquenessFilter(), right_pull);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                   ExpectDistributedExpand(), ExpectEdgeUniquenessFilter()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MultiMatchSameStart) {
  // Test MATCH (n) MATCH (n) -[r]- (m) RETURN n
  AstStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))),
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))), RETURN(as_n)));
  // Similar to MatchMultiPatternSameStart, we expect only Expand from second
  // MATCH clause.
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                     ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchWithReturn) {
  // Test MATCH (old) WITH old AS new RETURN new
  AstStorage storage;
  auto *as_new = NEXPR("new", IDENT("new"));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("old"))),
                                   WITH("old", AS("new")), RETURN(as_new)));
  // No accumulation since we only do reads.
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_new)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchWithWhereReturn) {
  // Test MATCH (old) WITH old AS new WHERE new.prop < 42 RETURN new
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  AstStorage storage;
  auto *as_new = NEXPR("new", IDENT("new"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("old"))), WITH("old", AS("new")),
      WHERE(LESS(PROPERTY_LOOKUP("new", prop), LITERAL(42))), RETURN(as_new)));
  // No accumulation since we only do reads.
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_new)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(),
                                     ExpectFilter(), ExpectProduce(), pull),
                        MakeCheckers(ExpectScanAll(), ExpectProduce(),
                                     ExpectFilter(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, CreateMultiExpand) {
  // Test CREATE (n) -[r :r]-> (m), (n) - [p :p]-> (l)
  FakeDbAccessor dba;
  auto r = "r";
  auto p = "p";
  AstStorage storage;
  auto *query = QUERY(SINGLE_QUERY(
      CREATE(PATTERN(NODE("n"), EDGE("r", Direction::OUT, {r}), NODE("m")),
             PATTERN(NODE("n"), EDGE("p", Direction::OUT, {p}), NODE("l")))));
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateExpand(),
                   ExpectDistributedCreateExpand(), ExpectSynchronize(false)),
      {}};
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchReturnSum) {
  // Test MATCH (n) RETURN SUM(n.prop1) AS sum, n.prop2 AS group
  FakeDbAccessor dba;
  auto prop1 = dba.Property("prop1");
  auto prop2 = dba.Property("prop2");
  AstStorage storage;
  auto sum = SUM(PROPERTY_LOOKUP("n", prop1));
  auto n_prop2 = PROPERTY_LOOKUP("n", prop2);
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), RETURN(sum, AS("sum"), n_prop2, AS("group"))));
  auto aggr = ExpectAggregate({sum}, {n_prop2});
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::atomic<int64_t> next_plan_id{0};
  auto distributed_plan = MakeDistributedPlan(storage, planner.plan(),
                                              symbol_table, next_plan_id, {});
  auto merge_sum = SUM(IDENT("worker_sum"));
  auto master_aggr = ExpectMasterAggregate({merge_sum}, {n_prop2});
  ExpectPullRemote pull(
      {symbol_table.at(*sum),
       symbol_table.at(*dynamic_cast<Identifier *>(n_prop2->expression_))});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), aggr, pull, master_aggr,
                                     ExpectProduce(), ExpectProduce()),
                        MakeCheckers(ExpectScanAll(), aggr));
  CheckDistributedPlan(distributed_plan, expected);
}

TYPED_TEST(TestPlanner, MatchWithCreate) {
  // Test MATCH (n) WITH n AS a CREATE (a) -[r :r]-> (b)
  FakeDbAccessor dba;
  auto r_type = "r";
  AstStorage storage;
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"))), WITH("n", AS("a")),
      CREATE(
          PATTERN(NODE("a"), EDGE("r", Direction::OUT, {r_type}), NODE("b")))));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(),
                   ExpectDistributedCreateExpand(), ExpectSynchronize()),
      MakeCheckers(ExpectScanAll(), ExpectProduce(),
                   ExpectDistributedCreateExpand()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchReturnSkipLimit) {
  // Test MATCH (n) RETURN n SKIP 2 LIMIT 1
  AstStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         RETURN(as_n, SKIP(LITERAL(2)), LIMIT(LITERAL(1)))));
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected =
      ExpectDistributed(MakeCheckers(ExpectScanAll(), ExpectProduce(), pull,
                                     ExpectSkip(), ExpectLimit()),
                        MakeCheckers(ExpectScanAll(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, CreateWithSkipReturnLimit) {
  // Test CREATE (n) WITH n AS m SKIP 2 RETURN m LIMIT 1
  AstStorage storage;
  auto ident_n = IDENT("n");
  auto query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n"))),
                                  WITH(ident_n, AS("m"), SKIP(LITERAL(2))),
                                  RETURN("m", LIMIT(LITERAL(1)))));
  auto symbol_table = query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_n)});
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectedDistributedPlan expected{
      MakeCheckers(ExpectDistributedCreateNode(true), ExpectSynchronize(true),
                   ExpectProduce(), ExpectSkip(), ExpectProduce(),
                   ExpectLimit()),
      {}};
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, MatchReturnOrderBy) {
  // Test MATCH (n) RETURN n AS m ORDER BY n.prop
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  AstStorage storage;
  auto *as_m = NEXPR("m", IDENT("n"));
  auto *node_n = NODE("n");
  auto ret = RETURN(as_m, ORDER_BY(PROPERTY_LOOKUP("n", prop)));
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), ret));
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemoteOrderBy pull_order_by(
      {symbol_table.at(*as_m), symbol_table.at(*node_n->identifier_)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectOrderBy(),
                   pull_order_by),
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectOrderBy()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
  // Even though last operator pulls and orders by `m` and `n`, we expect only
  // `m` as the output of the query execution.
  EXPECT_THAT(planner.plan().OutputSymbols(symbol_table),
              testing::UnorderedElementsAre(symbol_table.at(*as_m)));
}

TYPED_TEST(TestPlanner, CreateWithOrderByWhere) {
  // Test CREATE (n) -[r :r]-> (m)
  //      WITH n AS new ORDER BY new.prop, r.prop WHERE m.prop < 42
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  auto r_type = "r";
  AstStorage storage;
  auto ident_n = IDENT("n");
  auto ident_r = IDENT("r");
  auto ident_m = IDENT("m");
  auto new_prop = PROPERTY_LOOKUP("new", prop);
  auto r_prop = PROPERTY_LOOKUP(ident_r, prop);
  auto m_prop = PROPERTY_LOOKUP(ident_m, prop);
  auto query = QUERY(SINGLE_QUERY(
      CREATE(
          PATTERN(NODE("n"), EDGE("r", Direction::OUT, {r_type}), NODE("m"))),
      WITH(ident_n, AS("new"), ORDER_BY(new_prop, r_prop)),
      WHERE(LESS(m_prop, LITERAL(42)))));
  auto symbol_table = query::MakeSymbolTable(query);
  // Since this is a write query, we expect to accumulate to old used symbols.
  auto acc = ExpectAccumulate({
      symbol_table.at(*ident_n),  // `n` in WITH
      symbol_table.at(*ident_r),  // `r` in ORDER BY
      symbol_table.at(*ident_m),  // `m` in WHERE
  });
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCreateNode(true),
                   ExpectDistributedCreateExpand(), ExpectSynchronize(true),
                   ExpectProduce(), ExpectOrderBy(), ExpectFilter()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, ReturnAddSumCountOrderBy) {
  // Test RETURN SUM(1) + COUNT(2) AS result ORDER BY result
  AstStorage storage;
  auto sum = SUM(LITERAL(1));
  auto count = COUNT(LITERAL(2));
  auto *query = QUERY(SINGLE_QUERY(
      RETURN(ADD(sum, count), AS("result"), ORDER_BY(IDENT("result")))));
  auto aggr = ExpectAggregate({sum, count}, {});
  auto expected =
      ExpectDistributed(MakeCheckers(aggr, ExpectProduce(), ExpectOrderBy()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchUnwindReturn) {
  // Test MATCH (n) UNWIND [1,2,3] AS x RETURN n, x
  AstStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *as_x = NEXPR("x", IDENT("x"));
  auto *query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                   UNWIND(LIST(LITERAL(1), LITERAL(2), LITERAL(3)), AS("x")),
                   RETURN(as_n, as_x)));
  auto symbol_table = query::MakeSymbolTable(query);
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n), symbol_table.at(*as_x)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectUnwind(), ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectUnwind(), ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, ReturnDistinctOrderBySkipLimit) {
  // Test RETURN DISTINCT 1 ORDER BY 1 SKIP 1 LIMIT 1
  AstStorage storage;
  auto *query = QUERY(
      SINGLE_QUERY(RETURN_DISTINCT(LITERAL(1), AS("1"), ORDER_BY(LITERAL(1)),
                                   SKIP(LITERAL(1)), LIMIT(LITERAL(1)))));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectProduce(), ExpectDistinct(), ExpectOrderBy(),
                   ExpectSkip(), ExpectLimit()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchWhereBeforeExpand) {
  // Test MATCH (n) -[r]- (m) WHERE n.prop < 42 RETURN n
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  AstStorage storage;
  auto *as_n = NEXPR("n", IDENT("n"));
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
      WHERE(LESS(PROPERTY_LOOKUP("n", prop), LITERAL(42))), RETURN(as_n)));
  // We expect Filter to come immediately after ScanAll, since it only uses `n`.
  auto symbol_table = query::MakeSymbolTable(query);
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  ExpectPullRemote pull({symbol_table.at(*as_n)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectDistributedExpand(),
                   ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectDistributedExpand(),
                   ExpectProduce()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, FunctionAggregationReturn) {
  // Test RETURN sqrt(SUM(2)) AS result, 42 AS group_by
  AstStorage storage;
  auto sum = SUM(LITERAL(2));
  auto group_by_literal = LITERAL(42);
  auto *query = QUERY(SINGLE_QUERY(
      RETURN(FN("sqrt", sum), AS("result"), group_by_literal, AS("group_by"))));
  auto aggr = ExpectAggregate({sum}, {group_by_literal});
  auto expected = ExpectDistributed(MakeCheckers(aggr, ExpectProduce()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, FunctionWithoutArguments) {
  // Test RETURN pi() AS pi
  AstStorage storage;
  auto *query = QUERY(SINGLE_QUERY(RETURN(FN("pi"), AS("pi"))));
  auto expected = ExpectDistributed(MakeCheckers(ExpectProduce()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, MatchBfs) {
  // Test MATCH (n) -[r:type *..10 (r, n|n)]-> (m) RETURN r
  FakeDbAccessor dba;
  AstStorage storage;
  auto edge_type = storage.GetEdgeTypeIx("type");
  auto *bfs = storage.Create<query::EdgeAtom>(
      IDENT("r"), query::EdgeAtom::Type::BREADTH_FIRST, Direction::OUT,
      std::vector<EdgeTypeIx>{edge_type});
  bfs->filter_lambda_.inner_edge = IDENT("r");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = IDENT("n");
  bfs->upper_bound_ = LITERAL(10);
  auto *as_r = NEXPR("r", IDENT("r"));
  auto *query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN(as_r)));
  auto symbol_table = query::MakeSymbolTable(query);
  ExpectPullRemote pull({symbol_table.at(*as_r)});
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpandBfs(),
                   ExpectProduce(), pull),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpandBfs(),
                   ExpectProduce()));
  CheckDistributedPlan<TypeParam>(query, storage, expected);
}

TYPED_TEST(TestPlanner, DistributedAvg) {
  // Test MATCH (n) RETURN AVG(n.prop) AS res
  AstStorage storage;
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         RETURN(AVG(PROPERTY_LOOKUP("n", prop)), AS("res"))));
  auto distributed_plan = MakeDistributedPlan<TypeParam>(query, storage);
  auto &symbol_table = distributed_plan.symbol_table;
  auto worker_sum = SUM(PROPERTY_LOOKUP("n", prop));
  auto worker_count = COUNT(PROPERTY_LOOKUP("n", prop));
  {
    ASSERT_EQ(distributed_plan.worker_plans.size(), 1U);
    auto worker_plan = distributed_plan.worker_plans.back().second;
    auto worker_aggr_op = std::dynamic_pointer_cast<Aggregate>(worker_plan);
    ASSERT_TRUE(worker_aggr_op);
    ASSERT_EQ(worker_aggr_op->aggregations_.size(), 2U);
    worker_sum->MapTo(worker_aggr_op->aggregations_[0].output_sym);
    worker_count->MapTo(worker_aggr_op->aggregations_[1].output_sym);
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
  AstStorage storage;
  FakeDbAccessor dba;
  auto prop = dba.Property("prop");
  auto node_n = NODE("n");
  auto collect = COLLECT_LIST(PROPERTY_LOOKUP("n", prop));
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n)), RETURN(collect, AS("res"))));
  auto distributed_plan = MakeDistributedPlan<TypeParam>(query, storage);
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
  AstStorage storage;
  auto *ident_m = IDENT("m");
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), CREATE(PATTERN(NODE("m"))),
                         RETURN(ident_m, AS("m"))));
  auto symbol_table = query::MakeSymbolTable(query);
  auto acc = ExpectAccumulate({symbol_table.at(*ident_m)});
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateNode(),
                   ExpectSynchronize({symbol_table.at(*ident_m)}),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateNode()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianCreateExpand) {
  // Test MATCH (a), (b) CREATE (a)-[e:r]->(b) RETURN e
  AstStorage storage;
  FakeDbAccessor dba;
  auto relationship = "r";
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b)),
      CREATE(PATTERN(NODE("a"), EDGE("e", Direction::OUT, {relationship}),
                     NODE("b"))),
      RETURN("e")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto left_cart =
      MakeCheckers(ExpectScanAll(),
                   ExpectPullRemote({symbol_table.at(*node_a->identifier_)}));
  auto right_cart =
      MakeCheckers(ExpectScanAll(),
                   ExpectPullRemote({symbol_table.at(*node_b->identifier_)}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectDistributedCreateExpand(), ExpectSynchronize(false),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianExpand) {
  // Test MATCH (a), (b)-[e]-(c) RETURN c
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *edge_e = EDGE("e");
  auto *node_c = NODE("c");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b, edge_e, node_c)), RETURN("c")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto sym_e = symbol_table.at(*edge_e->identifier_);
  auto sym_c = symbol_table.at(*node_c->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectDistributedExpand(),
                                 ExpectPullRemote({sym_b, sym_e, sym_c}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll()),
      MakeCheckers(ExpectScanAll(), ExpectDistributedExpand()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianExpandToExisting) {
  // Test MATCH (a), (b)-[e]-(a) RETURN e
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b, EDGE("e"), NODE("a"))),
      RETURN("e")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectDistributedExpand(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianExpandFromExisting) {
  // Test MATCH (a), (b), (a)-[e]-(b) RETURN e
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(node_a), PATTERN(node_b),
                               PATTERN(NODE("a"), EDGE("e"), NODE("b"))),
                         RETURN("e")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectDistributedExpand(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianFilter) {
  // Test MATCH (a), (b), (c) WHERE a = 42 AND b = a AND c = b RETURN c
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *node_c = NODE("c");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b), PATTERN(node_c)),
      WHERE(AND(AND(EQ(IDENT("a"), LITERAL(42)), EQ(IDENT("b"), IDENT("a"))),
                EQ(IDENT("c"), IDENT("b")))),
      RETURN("c")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto sym_c = symbol_table.at(*node_c->identifier_);
  auto left_cart =
      MakeCheckers(ExpectScanAll(), ExpectFilter(), ExpectPullRemote({sym_a}));
  auto mid_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_c}));
  auto mid_right_cart =
      MakeCheckers(ExpectDistributedCartesian(mid_cart, right_cart));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, mid_right_cart),
                   ExpectFilter(), ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectFilter()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianIndexedScanByProperty) {
  // Test MATCH (a), (b :label) WHERE b.prop = a RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label_name = "label";
  auto label = dba.Label(label_name);
  auto prop = dba.Property("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  auto *node_a = NODE("a");
  auto *node_b = NODE("b", label_name);
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b)),
      WHERE(EQ(PROPERTY_LOOKUP("b", prop), IDENT("a"))), RETURN("b")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect only indexed lookup by label because property depends on
  // Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAllByLabel()));
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianIndexedScanByLowerBound) {
  // Test MATCH (a), (b :label) WHERE a < b.prop RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label_name = "label";
  auto label = dba.Label(label_name);
  auto prop = dba.Property("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  auto *node_a = NODE("a");
  auto *node_b = NODE("b", label_name);
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b)),
      WHERE(LESS(IDENT("a"), PROPERTY_LOOKUP("b", prop))), RETURN("b")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect only indexed lookup by label because lower bound depends on
  // Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAllByLabel()));
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianIndexedScanByUpperBound) {
  // Test MATCH (a), (b :label) WHERE a > b.prop RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label_name = "label";
  auto label = dba.Label(label_name);
  auto prop = dba.Property("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  auto *node_a = NODE("a");
  auto *node_b = NODE("b", label_name);
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a), PATTERN(node_b)),
      WHERE(GREATER(IDENT("a"), PROPERTY_LOOKUP("b", prop))), RETURN("b")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect only indexed lookup by label because upper bound depends on
  // Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAllByLabel()));
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TEST(TestPlanner, DistributedCartesianIndexedScanByBothBounds) {
  // Test MATCH (a), (b :label) WHERE a > b.prop > a RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label = dba.Label("label");
  auto prop = dba.Property("prop");
  storage.GetPropertyIx("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  SymbolTable symbol_table;
  auto sym_a = symbol_table.CreateSymbol("a", true);
  auto scan_a = std::make_shared<ScanAll>(nullptr, sym_a);
  auto sym_b = symbol_table.CreateSymbol("b", true);
  query::Expression *lower_expr = IDENT("a")->MapTo(sym_a);
  auto lower_bound = utils::MakeBoundExclusive(lower_expr);
  query::Expression *upper_expr = IDENT("a")->MapTo(sym_a);
  auto upper_bound = utils::MakeBoundExclusive(upper_expr);
  auto scan_b = std::make_shared<ScanAllByLabelPropertyRange>(
      scan_a, sym_b, label, prop, "prop", lower_bound, upper_bound);
  auto ident_b = IDENT("b")->MapTo(sym_b);
  auto as_b = NEXPR("b", ident_b);
  auto produce = std::make_shared<Produce>(
      scan_b, std::vector<query::NamedExpression *>{as_b});
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect only indexed lookup by label because both bounds depend on
  // Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAllByLabel()));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, *produce, symbol_table, properties_by_ix,
                       expected);
}

TEST(TestPlanner, DistributedCartesianIndexedScanByLowerWithBothBounds) {
  // Test MATCH (a), (b :label) WHERE a > b.prop > 42 RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label = dba.Label("label");
  auto prop = dba.Property("prop");
  storage.GetPropertyIx("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  SymbolTable symbol_table;
  auto sym_a = symbol_table.CreateSymbol("a", true);
  auto scan_a = std::make_shared<ScanAll>(nullptr, sym_a);
  auto sym_b = symbol_table.CreateSymbol("b", true);
  query::Expression *lower_expr = LITERAL(42);
  auto lower_bound = utils::MakeBoundExclusive(lower_expr);
  query::Expression *upper_expr = IDENT("a")->MapTo(sym_a);
  auto upper_bound = utils::MakeBoundExclusive(upper_expr);
  auto scan_b = std::make_shared<ScanAllByLabelPropertyRange>(
      scan_a, sym_b, label, prop, "prop", lower_bound, upper_bound);
  auto ident_b = IDENT("b")->MapTo(sym_b);
  auto as_b = NEXPR("b", ident_b);
  auto produce = std::make_shared<Produce>(
      scan_b, std::vector<query::NamedExpression *>{as_b});
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect indexed lookup by label property range above lower bound,
  // because upper bound depends on Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabelPropertyRange(
                       label, prop, lower_bound, std::experimental::nullopt),
                   ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()),
      MakeCheckers(ExpectScanAllByLabelPropertyRange(
          label, prop, lower_bound, std::experimental::nullopt)));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, *produce, symbol_table, properties_by_ix,
                       expected);
}

TEST(TestPlanner, DistributedCartesianIndexedScanByUpperWithBothBounds) {
  // Test MATCH (a), (b :label) WHERE 42 > b.prop > a RETURN b
  AstStorage storage;
  FakeDbAccessor dba;
  auto label = dba.Label("label");
  auto prop = dba.Property("prop");
  storage.GetPropertyIx("prop");
  // Set indexes so that lookup by property is preferred.
  dba.SetIndexCount(label, 1024);
  dba.SetIndexCount(label, prop, 0);
  SymbolTable symbol_table;
  auto sym_a = symbol_table.CreateSymbol("a", true);
  auto scan_a = std::make_shared<ScanAll>(nullptr, sym_a);
  auto sym_b = symbol_table.CreateSymbol("b", true);
  query::Expression *lower_expr = IDENT("a")->MapTo(sym_a);
  auto lower_bound = utils::MakeBoundExclusive(lower_expr);
  query::Expression *upper_expr = LITERAL(42);
  auto upper_bound = utils::MakeBoundExclusive(upper_expr);
  auto scan_b = std::make_shared<ScanAllByLabelPropertyRange>(
      scan_a, sym_b, label, prop, "prop", lower_bound, upper_bound);
  auto ident_b = IDENT("b")->MapTo(sym_b);
  auto as_b = NEXPR("b", ident_b);
  auto produce = std::make_shared<Produce>(
      scan_b, std::vector<query::NamedExpression *>{as_b});
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  // We still expect indexed lookup by label property range below upper bound,
  // because lower bound depends on Cartesian branch.
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabelPropertyRange(
                       label, prop, std::experimental::nullopt, upper_bound),
                   ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()),
      MakeCheckers(ExpectScanAllByLabelPropertyRange(
          label, prop, std::experimental::nullopt, upper_bound)));
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, *produce, symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianProduce) {
  // Test MATCH (a) WITH a MATCH (b) WHERE b = a RETURN b;
  AstStorage storage;
  auto *with_a = WITH("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("a"))), with_a, MATCH(PATTERN(node_b)),
                   WHERE(EQ(IDENT("b"), IDENT("a"))), RETURN("b")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*with_a->body_.named_expressions[0]);
  auto left_cart =
      MakeCheckers(ExpectScanAll(), ExpectProduce(), ExpectPullRemote({sym_a}));
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAll(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianUnwind) {
  // Test MATCH (a), (b) UNWIND a AS x RETURN x
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_a), PATTERN(node_b)),
                                   UNWIND(IDENT("a"), AS("x")), RETURN("x")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}));
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectUnwind(), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianMatchCreateNode) {
  // Test MATCH (a) CREATE (b) WITH b MATCH (c) CREATE (d)
  AstStorage storage;
  auto *node_b = NODE("b");
  auto *node_c = NODE("c");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(NODE("a"))), CREATE(PATTERN(node_b)), WITH("b"),
      MATCH(PATTERN(node_c)), CREATE(PATTERN(NODE("d")))));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto left_cart =
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateNode(),
                   ExpectSynchronize({sym_b}, true), ExpectProduce());
  auto sym_c = symbol_table.at(*node_c->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_c}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectDistributedCreateNode(true), ExpectSynchronize(false)),
      MakeCheckers(ExpectScanAll(), ExpectDistributedCreateNode()),
      MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianCreateNode) {
  // Test CREATE (a) WITH a MATCH (b) RETURN b
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(node_a)), WITH("a"),
                                   MATCH(PATTERN(node_b)), RETURN("b")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto left_cart = MakeCheckers(ExpectDistributedCreateNode(true),
                                ExpectSynchronize(true), ExpectProduce());
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, right_cart),
                   ExpectProduce()),
      MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedOptionalExpand) {
  // Test MATCH (n) OPTIONAL MATCH (n)-[e]-(m) RETURN e;
  AstStorage storage;
  auto *node_n = NODE("n");
  auto *edge_e = EDGE("e");
  auto *node_m = NODE("m");
  auto *ret_e = RETURN("e");
  auto *query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(node_n)),
                   OPTIONAL_MATCH(PATTERN(node_n, edge_e, node_m)), ret_e));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_e = symbol_table.at(*ret_e->body_.named_expressions[0]);
  std::list<BaseOpChecker *> optional{new ExpectDistributedExpand()};
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectDistributedOptional(optional),
                   ExpectProduce(), ExpectPullRemote({sym_e})),
      MakeCheckers(ExpectScanAll(), ExpectDistributedOptional(optional),
                   ExpectProduce()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedOptionalCartesian) {
  // Test MATCH (a) OPTIONAL MATCH (b), (c) WHERE b > a RETURN c;
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *node_c = NODE("c");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a)), OPTIONAL_MATCH(PATTERN(node_b), PATTERN(node_c)),
      WHERE(GREATER(node_b->identifier_, node_a->identifier_)), RETURN("c")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  auto sym_c = symbol_table.at(*node_c->identifier_);
  auto left_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_b}));
  auto right_cart = MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_c}));
  std::list<BaseOpChecker *> optional{
      new ExpectDistributedCartesian(left_cart, right_cart),
      new ExpectFilter()};
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}),
                   ExpectDistributedOptional(optional), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()),
      MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedCartesianTransitiveDependency) {
  // Test MATCH (n:L)-[a]-(m:L)-[b]-(l:L) RETURN l;
  AstStorage storage;
  FakeDbAccessor dba;
  auto label_name = "L";
  auto label = dba.Label(label_name);
  // Set indexes so that multiple scans and expanding to existing is preferred.
  dba.SetIndexCount(label, 1);
  auto *node_n = NODE("n", label_name);
  auto *node_m = NODE("m", label_name);
  auto *node_l = NODE("l", label_name);
  auto *edge_a = EDGE("a");
  auto *edge_b = EDGE("b");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_n, edge_a, node_m, edge_b, node_l)), RETURN("l")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*edge_a->identifier_);
  auto sym_b = symbol_table.at(*edge_b->identifier_);
  auto sym_n = symbol_table.at(*node_n->identifier_);
  auto sym_m = symbol_table.at(*node_m->identifier_);
  auto sym_l = symbol_table.at(*node_l->identifier_);
  auto right_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_l}));
  auto mid_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_m}));
  auto left_cart =
      MakeCheckers(ExpectScanAllByLabel(), ExpectPullRemote({sym_n}));
  auto mid_right_cart =
      MakeCheckers(ExpectDistributedCartesian(mid_cart, right_cart));
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectDistributedCartesian(left_cart, mid_right_cart),
                   // This expand depends on Cartesian branches.
                   ExpectDistributedExpand(),
                   // This expand depends on the previous one.
                   ExpectDistributedExpand(),
                   // UniquenessFilter depends on both expands.
                   ExpectEdgeUniquenessFilter(), ExpectProduce()),
      MakeCheckers(ExpectScanAllByLabel()),
      MakeCheckers(ExpectScanAllByLabel()),
      MakeCheckers(ExpectScanAllByLabel()));
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TYPED_TEST(TestPlanner, DistributedOptionalScanExpandExisting) {
  // Test MATCH (a) OPTIONAL MATCH (b)-[e]-(a) RETURN e;
  AstStorage storage;
  auto *node_a = NODE("a");
  auto *node_b = NODE("b");
  auto *query = QUERY(SINGLE_QUERY(
      MATCH(PATTERN(node_a)),
      OPTIONAL_MATCH(PATTERN(node_b, EDGE("e"), NODE("a"))), RETURN("e")));
  auto symbol_table = query::MakeSymbolTable(query);
  auto sym_a = symbol_table.at(*node_a->identifier_);
  auto sym_b = symbol_table.at(*node_b->identifier_);
  std::list<BaseOpChecker *> optional{new ExpectScanAll(),
                                      new ExpectPullRemote({sym_b}),
                                      new ExpectDistributedExpand()};
  auto expected = ExpectDistributed(
      MakeCheckers(ExpectScanAll(), ExpectPullRemote({sym_a}),
                   ExpectDistributedOptional(optional), ExpectProduce()),
      MakeCheckers(ExpectScanAll()), MakeCheckers(ExpectScanAll()));
  FakeDbAccessor dba;
  auto planner = MakePlanner<TypeParam>(&dba, storage, symbol_table, query);
  std::vector<storage::Property> properties_by_ix;
  for (const auto &prop : storage.properties_) {
    properties_by_ix.push_back(dba.Property(prop));
  }
  CheckDistributedPlan(storage, planner.plan(), symbol_table, properties_by_ix,
                       expected);
}

TEST(CapnpSerial, Union) {
  std::vector<Symbol> left_symbols{
      Symbol("symbol", 1, true, Symbol::Type::EDGE)};
  std::vector<Symbol> right_symbols{
      Symbol("symbol", 3, true, Symbol::Type::ANY)};
  auto union_symbols = right_symbols;
  auto union_op = std::make_unique<Union>(nullptr, nullptr, union_symbols,
                                          left_symbols, right_symbols);
  std::unique_ptr<LogicalOperator> loaded_plan;
  ::capnp::MallocMessageBuilder message;
  SavePlan(*union_op, &message);
  AstStorage new_storage;
  std::tie(loaded_plan, new_storage) =
      LoadPlan(message.getRoot<query::plan::capnp::LogicalOperator>());
  ASSERT_TRUE(loaded_plan);
  auto *loaded_op = dynamic_cast<Union *>(loaded_plan.get());
  ASSERT_TRUE(loaded_op);
  EXPECT_FALSE(loaded_op->left_op_);
  EXPECT_FALSE(loaded_op->right_op_);
  EXPECT_EQ(loaded_op->left_symbols_, left_symbols);
  EXPECT_EQ(loaded_op->right_symbols_, right_symbols);
  EXPECT_EQ(loaded_op->union_symbols_, union_symbols);
}

TEST(CapnpSerial, Cartesian) {
  std::vector<Symbol> left_symbols{
      Symbol("left_symbol", 1, true, Symbol::Type::EDGE)};
  std::vector<Symbol> right_symbols{
      Symbol("right_symbol", 3, true, Symbol::Type::ANY)};
  auto cartesian = std::make_unique<Cartesian>(nullptr, left_symbols, nullptr,
                                               right_symbols);
  std::unique_ptr<LogicalOperator> loaded_plan;
  ::capnp::MallocMessageBuilder message;
  SavePlan(*cartesian, &message);
  AstStorage new_storage;
  std::tie(loaded_plan, new_storage) =
      LoadPlan(message.getRoot<query::plan::capnp::LogicalOperator>());
  ASSERT_TRUE(loaded_plan);
  auto *loaded_op = dynamic_cast<Cartesian *>(loaded_plan.get());
  ASSERT_TRUE(loaded_op);
  EXPECT_FALSE(loaded_op->left_op_);
  EXPECT_FALSE(loaded_op->right_op_);
  EXPECT_EQ(loaded_op->left_symbols_, left_symbols);
  EXPECT_EQ(loaded_op->right_symbols_, right_symbols);
}

TEST(CapnpSerial, Synchronize) {
  auto synchronize = std::make_unique<Synchronize>(nullptr, nullptr, true);
  std::unique_ptr<LogicalOperator> loaded_plan;
  ::capnp::MallocMessageBuilder message;
  SavePlan(*synchronize, &message);
  AstStorage new_storage;
  std::tie(loaded_plan, new_storage) =
      LoadPlan(message.getRoot<query::plan::capnp::LogicalOperator>());
  ASSERT_TRUE(loaded_plan);
  auto *loaded_op = dynamic_cast<Synchronize *>(loaded_plan.get());
  ASSERT_TRUE(loaded_op);
  EXPECT_FALSE(loaded_op->input());
  EXPECT_FALSE(loaded_op->pull_remote_);
  EXPECT_TRUE(loaded_op->advance_command_);
}

TEST(CapnpSerial, PullRemote) {
  std::vector<Symbol> symbols{Symbol("symbol", 1, true, Symbol::Type::EDGE)};
  auto pull_remote = std::make_unique<PullRemote>(nullptr, 42, symbols);
  std::unique_ptr<LogicalOperator> loaded_plan;
  ::capnp::MallocMessageBuilder message;
  SavePlan(*pull_remote, &message);
  AstStorage new_storage;
  std::tie(loaded_plan, new_storage) =
      LoadPlan(message.getRoot<query::plan::capnp::LogicalOperator>());
  ASSERT_TRUE(loaded_plan);
  auto *loaded_op = dynamic_cast<PullRemote *>(loaded_plan.get());
  ASSERT_TRUE(loaded_op);
  EXPECT_FALSE(loaded_op->input());
  EXPECT_EQ(loaded_op->plan_id_, 42);
  EXPECT_EQ(loaded_op->symbols_, symbols);
}

TEST(CapnpSerial, PullRemoteOrderBy) {
  auto once = std::make_shared<Once>();
  AstStorage storage;
  std::vector<Symbol> symbols{
      Symbol("my_symbol", 2, true, Symbol::Type::VERTEX, 3)};
  std::vector<query::SortItem> order_by{
      {query::Ordering::ASC, IDENT("my_symbol")}};
  auto pull_remote_order_by =
      std::make_unique<PullRemoteOrderBy>(once, 42, order_by, symbols);
  std::unique_ptr<LogicalOperator> loaded_plan;
  ::capnp::MallocMessageBuilder message;
  SavePlan(*pull_remote_order_by, &message);
  AstStorage new_storage;
  std::tie(loaded_plan, new_storage) =
      LoadPlan(message.getRoot<query::plan::capnp::LogicalOperator>());
  ASSERT_TRUE(loaded_plan);
  auto *loaded_op = dynamic_cast<PullRemoteOrderBy *>(loaded_plan.get());
  ASSERT_TRUE(loaded_op);
  ASSERT_TRUE(std::dynamic_pointer_cast<Once>(loaded_op->input()));
  EXPECT_EQ(loaded_op->plan_id_, 42);
  EXPECT_EQ(loaded_op->symbols_, symbols);
  ASSERT_EQ(loaded_op->order_by_.size(), 1);
  EXPECT_TRUE(dynamic_cast<query::Identifier *>(loaded_op->order_by_[0]));
  ASSERT_EQ(loaded_op->compare_.ordering().size(), 1);
  EXPECT_EQ(loaded_op->compare_.ordering()[0], query::Ordering::ASC);
}
