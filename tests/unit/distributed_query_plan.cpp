#include <memory>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
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
#include "query/plan/planner.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"
#include "transactions/engine_master.hpp"

DECLARE_int32(query_execution_time_sec);

using namespace distributed;
using namespace database;

TEST_F(DistributedGraphDbTest, PullProduceRpc) {
  GraphDbAccessor dba{master()};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;

  // Query plan for: UNWIND [42, true, "bla", 1, 2] as x RETURN x
  using namespace query;
  auto list =
      LIST(LITERAL(42), LITERAL(true), LITERAL("bla"), LITERAL(1), LITERAL(2));
  auto x = ctx.symbol_table_.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, list, x);
  auto x_expr = IDENT("x");
  ctx.symbol_table_[*x_expr] = x;
  auto x_ne = NEXPR("x", x_expr);
  ctx.symbol_table_[*x_ne] = ctx.symbol_table_.CreateSymbol("x_ne", true);
  auto produce = MakeProduce(unwind, x_ne);

  // Test that the plan works locally.
  auto results = CollectProduce(produce.get(), ctx.symbol_table_, dba);
  ASSERT_EQ(results.size(), 5);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, ctx.symbol_table_);

  tx::CommandId command_id = dba.transaction().cid();
  Parameters params;
  std::vector<query::Symbol> symbols{ctx.symbol_table_[*x_ne]};
  auto remote_pull = [this, &command_id, &params, &symbols](
      GraphDbAccessor &dba, int worker_id) {
    return master().pull_clients().Pull(dba, worker_id, plan_id, command_id,
                                        params, symbols, false, 3);
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

  GraphDbAccessor dba_1{master()};
  GraphDbAccessor dba_2{master()};
  for (int worker_id : {1, 2}) {
    // TODO flor, proper test async here.
    auto tx1_batch1 = remote_pull(dba_1, worker_id).get();
    expect_first_batch(tx1_batch1);
    auto tx2_batch1 = remote_pull(dba_2, worker_id).get();
    expect_first_batch(tx2_batch1);
    auto tx2_batch2 = remote_pull(dba_2, worker_id).get();
    expect_second_batch(tx2_batch2);
    auto tx1_batch2 = remote_pull(dba_1, worker_id).get();
    expect_second_batch(tx1_batch2);
  }
}

TEST_F(DistributedGraphDbTest, PullProduceRpcWithGraphElements) {
  // Create some data on the master and both workers. Eeach edge (3 of them) and
  // vertex (6 of them) will be uniquely identified with their worker id and
  // sequence ID, so we can check we retrieved all.
  storage::Property prop;
  {
    GraphDbAccessor dba{master()};
    prop = dba.Property("prop");
    auto create_data = [prop](GraphDbAccessor &dba, int worker_id) {
      auto v1 = dba.InsertVertex();
      v1.PropsSet(prop, worker_id * 10);
      auto v2 = dba.InsertVertex();
      v2.PropsSet(prop, worker_id * 10 + 1);
      auto e12 = dba.InsertEdge(v1, v2, dba.EdgeType("et"));
      e12.PropsSet(prop, worker_id * 10 + 2);
    };
    create_data(dba, 0);
    GraphDbAccessor dba_w1{worker(1), dba.transaction_id()};
    create_data(dba_w1, 1);
    GraphDbAccessor dba_w2{worker(2), dba.transaction_id()};
    create_data(dba_w2, 2);
    dba.Commit();
  }

  GraphDbAccessor dba{master()};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;

  // Query plan for: MATCH p = (n)-[r]->(m) return [n, r], m, p
  // Use this query to test graph elements are transferred correctly in
  // collections too.
  auto n = MakeScanAll(storage, ctx.symbol_table_, "n");
  auto r_m =
      MakeExpand(storage, ctx.symbol_table_, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  auto p_sym = ctx.symbol_table_.CreateSymbol("p", true);
  auto p = std::make_shared<query::plan::ConstructNamedPath>(
      r_m.op_, p_sym,
      std::vector<Symbol>{n.sym_, r_m.edge_sym_, r_m.node_sym_});
  auto return_n = IDENT("n");
  ctx.symbol_table_[*return_n] = n.sym_;
  auto return_r = IDENT("r");
  ctx.symbol_table_[*return_r] = r_m.edge_sym_;
  auto return_n_r = NEXPR("[n, r]", LIST(return_n, return_r));
  ctx.symbol_table_[*return_n_r] = ctx.symbol_table_.CreateSymbol("", true);
  auto return_m = NEXPR("m", IDENT("m"));
  ctx.symbol_table_[*return_m->expression_] = r_m.node_sym_;
  ctx.symbol_table_[*return_m] = ctx.symbol_table_.CreateSymbol("", true);
  auto return_p = NEXPR("p", IDENT("p"));
  ctx.symbol_table_[*return_p->expression_] = p_sym;
  ctx.symbol_table_[*return_p] = ctx.symbol_table_.CreateSymbol("", true);
  auto produce = MakeProduce(p, return_n_r, return_m, return_p);

  auto check_result = [prop](
      int worker_id,
      const std::vector<std::vector<query::TypedValue>> &frames) {
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
  auto results = CollectProduce(produce.get(), ctx.symbol_table_, dba);
  check_result(0, results);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, ctx.symbol_table_);

  tx::CommandId command_id = dba.transaction().cid();
  Parameters params;
  std::vector<query::Symbol> symbols{ctx.symbol_table_[*return_n_r],
                                     ctx.symbol_table_[*return_m], p_sym};
  auto remote_pull = [this, &command_id, &params, &symbols](
      GraphDbAccessor &dba, int worker_id) {
    return master().pull_clients().Pull(dba, worker_id, plan_id, command_id,
                                        params, symbols, false, 3);
  };
  auto future_w1_results = remote_pull(dba, 1);
  auto future_w2_results = remote_pull(dba, 2);
  check_result(1, future_w1_results.get().frames);
  check_result(2, future_w2_results.get().frames);
}

TEST_F(DistributedGraphDbTest, Synchronize) {
  auto from = InsertVertex(worker(1));
  auto to = InsertVertex(worker(2));
  InsertEdge(from, to, "et");

  // Query: MATCH (n)--(m) SET m.prop = 2 RETURN n.prop
  // This query ensures that a remote update gets applied and the local stuff
  // gets reconstructed.
  auto &db = master();
  GraphDbAccessor dba{db};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;
  // MATCH
  auto n = MakeScanAll(storage, ctx.symbol_table_, "n");
  auto r_m =
      MakeExpand(storage, ctx.symbol_table_, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);

  // SET
  auto literal = LITERAL(42);
  auto prop = PROPERTY_PAIR("prop");
  auto m_p = PROPERTY_LOOKUP("m", prop);
  ctx.symbol_table_[*m_p->expression_] = r_m.node_sym_;
  auto set_m_p = std::make_shared<plan::SetProperty>(r_m.op_, m_p, literal);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, set_m_p, ctx.symbol_table_);

  // Master-side PullRemote, Synchronize
  auto pull_remote = std::make_shared<query::plan::PullRemote>(
      nullptr, plan_id, std::vector<Symbol>{n.sym_});
  auto synchronize =
      std::make_shared<query::plan::Synchronize>(set_m_p, pull_remote, true);

  // RETURN
  auto n_p =
      storage.Create<PropertyLookup>(storage.Create<Identifier>("n"), prop);
  ctx.symbol_table_[*n_p->expression_] = n.sym_;
  auto return_n_p = NEXPR("n.prop", n_p);
  auto return_n_p_sym = ctx.symbol_table_.CreateSymbol("n.p", true);
  ctx.symbol_table_[*return_n_p] = return_n_p_sym;
  auto produce = MakeProduce(synchronize, return_n_p);

  auto results = CollectProduce(produce.get(), ctx.symbol_table_, dba);
  ASSERT_EQ(results.size(), 2);
  ASSERT_EQ(results[0].size(), 1);
  EXPECT_EQ(results[0][0].ValueInt(), 42);
  ASSERT_EQ(results[1].size(), 1);
  EXPECT_EQ(results[1][0].ValueInt(), 42);

  // TODO test without advance command?
}

TEST_F(DistributedGraphDbTest, Create) {
  // Query: UNWIND range(0, 1000) as x CREATE ()
  auto &db = master();
  GraphDbAccessor dba{db};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;
  auto range = FN("range", LITERAL(0), LITERAL(1000));
  auto x = ctx.symbol_table_.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, range, x);
  auto node = NODE("n");
  ctx.symbol_table_[*node->identifier_] =
      ctx.symbol_table_.CreateSymbol("n", true);
  auto create = std::make_shared<query::plan::CreateNode>(unwind, node, true);
  PullAll(create, dba, ctx.symbol_table_);
  dba.Commit();

  EXPECT_GT(VertexCount(master()), 200);
  EXPECT_GT(VertexCount(worker(1)), 200);
  EXPECT_GT(VertexCount(worker(2)), 200);
}

TEST_F(DistributedGraphDbTest, PullRemoteOrderBy) {
  // Create some data on the master and both workers.
  storage::Property prop;
  {
    GraphDbAccessor dba{master()};
    auto tx_id = dba.transaction_id();
    GraphDbAccessor dba1{worker(1), tx_id};
    GraphDbAccessor dba2{worker(2), tx_id};
    prop = dba.Property("prop");
    auto add_data = [prop](GraphDbAccessor &dba, int value) {
      dba.InsertVertex().PropsSet(prop, value);
    };

    std::vector<int> data;
    for (int i = 0; i < 300; ++i) data.push_back(i);
    std::random_shuffle(data.begin(), data.end());

    for (int i = 0; i < 100; ++i) add_data(dba, data[i]);
    for (int i = 100; i < 200; ++i) add_data(dba1, data[i]);
    for (int i = 200; i < 300; ++i) add_data(dba2, data[i]);

    dba.Commit();
  }

  auto &db = master();
  GraphDbAccessor dba{db};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;

  // Query plan for:  MATCH (n) RETURN n.prop ORDER BY n.prop;
  auto n = MakeScanAll(storage, ctx.symbol_table_, "n");
  auto n_p = PROPERTY_LOOKUP("n", prop);
  ctx.symbol_table_[*n_p->expression_] = n.sym_;
  auto order_by = std::make_shared<plan::OrderBy>(
      n.op_,
      std::vector<std::pair<Ordering, Expression *>>{{Ordering::ASC, n_p}},
      std::vector<Symbol>{n.sym_});

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, order_by, ctx.symbol_table_);

  auto pull_remote_order_by = std::make_shared<plan::PullRemoteOrderBy>(
      order_by, plan_id,
      std::vector<std::pair<Ordering, Expression *>>{{Ordering::ASC, n_p}},
      std::vector<Symbol>{n.sym_});

  auto n_p_ne = NEXPR("n.prop", n_p);
  ctx.symbol_table_[*n_p_ne] = ctx.symbol_table_.CreateSymbol("n.prop", true);
  auto produce = MakeProduce(pull_remote_order_by, n_p_ne);
  auto results = CollectProduce(produce.get(), ctx.symbol_table_, dba);

  ASSERT_EQ(results.size(), 300);
  for (int j = 0; j < 300; ++j) {
    EXPECT_TRUE(TypedValue::BoolEqual{}(results[j][0], j));
  }
}

class DistributedTransactionTimeout : public DistributedGraphDbTest {
 protected:
  int QueryExecutionTimeSec(int) override { return 1; }
};

TEST_F(DistributedTransactionTimeout, Timeout) {
  InsertVertex(worker(1));
  InsertVertex(worker(1));

  GraphDbAccessor dba{master()};
  Context ctx{dba};
  SymbolGenerator symbol_generator{ctx.symbol_table_};
  AstTreeStorage storage;

  // Make distributed plan for MATCH (n) RETURN n
  auto scan_all = MakeScanAll(storage, ctx.symbol_table_, "n");
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all.op_, output);
  ctx.symbol_table_[*output->expression_] = scan_all.sym_;
  ctx.symbol_table_[*output] =
      ctx.symbol_table_.CreateSymbol("named_expression_1", true);

  const int plan_id = 42;
  master().plan_dispatcher().DispatchPlan(plan_id, produce, ctx.symbol_table_);
  tx::CommandId command_id = dba.transaction().cid();

  Parameters params;
  std::vector<query::Symbol> symbols{ctx.symbol_table_[*output]};
  auto remote_pull = [this, &command_id, &params, &symbols, &dba]() {
    return master()
        .pull_clients()
        .Pull(dba, 1, plan_id, command_id, params, symbols, false, 1)
        .get()
        .pull_state;
  };
  ASSERT_EQ(remote_pull(), distributed::PullState::CURSOR_IN_PROGRESS);
  // Sleep here so the remote gets a hinted error.
  std::this_thread::sleep_for(2s);
  EXPECT_EQ(remote_pull(), distributed::PullState::HINTED_ABORT_ERROR);
}
