#include <memory>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "distributed/coordination.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "distributed/remote_data_rpc_server.hpp"
#include "distributed/remote_pull_rpc_clients.hpp"
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

using namespace distributed;
using namespace database;

TEST_F(DistributedGraphDbTest, Coordination) {
  EXPECT_NE(master().endpoint().port(), 0);
  EXPECT_NE(worker(1).endpoint().port(), 0);
  EXPECT_NE(worker(2).endpoint().port(), 0);

  EXPECT_EQ(master().GetEndpoint(1), worker(1).endpoint());
  EXPECT_EQ(master().GetEndpoint(2), worker(2).endpoint());
  EXPECT_EQ(worker(1).GetEndpoint(0), master().endpoint());
  EXPECT_EQ(worker(1).GetEndpoint(2), worker(2).endpoint());
  EXPECT_EQ(worker(2).GetEndpoint(0), master().endpoint());
  EXPECT_EQ(worker(2).GetEndpoint(1), worker(1).endpoint());
}

TEST_F(DistributedGraphDbTest, TxEngine) {
  auto *tx1 = master_tx_engine().Begin();
  auto *tx2 = master_tx_engine().Begin();
  EXPECT_EQ(tx2->snapshot().size(), 1);
  EXPECT_EQ(
      worker(1).tx_engine().RunningTransaction(tx1->id_)->snapshot().size(), 0);
  EXPECT_EQ(worker(2).tx_engine().RunningTransaction(tx2->id_)->snapshot(),
            tx2->snapshot());

  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(worker(2).tx_engine().RunningTransaction(123), "");
}

template <typename TType>
using mapper_vec =
    std::vector<std::reference_wrapper<storage::ConcurrentIdMapper<TType>>>;

TEST_F(DistributedGraphDbTest, StorageTypes) {
  auto test_mappers = [](auto mappers, auto ids) {
    for (size_t i = 0; i < mappers.size(); ++i) {
      ids.emplace_back(
          mappers[i].get().value_to_id("value" + std::to_string(i)));
    }
    EXPECT_GT(ids.size(), 0);
    for (size_t i = 0; i < mappers.size(); ++i) {
      for (size_t j = 0; j < ids.size(); ++j) {
        EXPECT_EQ(mappers[i].get().id_to_value(ids[j]),
                  "value" + std::to_string(j));
      }
    }
  };

  test_mappers(mapper_vec<storage::Label>{master().label_mapper(),
                                          worker(1).label_mapper(),
                                          worker(2).label_mapper()},
               std::vector<storage::Label>{});
  test_mappers(mapper_vec<storage::EdgeType>{master().edge_type_mapper(),
                                             worker(1).edge_type_mapper(),
                                             worker(2).edge_type_mapper()},
               std::vector<storage::EdgeType>{});
  test_mappers(mapper_vec<storage::Property>{master().property_mapper(),
                                             worker(1).property_mapper(),
                                             worker(2).property_mapper()},
               std::vector<storage::Property>{});
}

TEST_F(DistributedGraphDbTest, Counters) {
  EXPECT_EQ(master().counters().Get("a"), 0);
  EXPECT_EQ(worker(1).counters().Get("a"), 1);
  EXPECT_EQ(worker(2).counters().Get("a"), 2);

  EXPECT_EQ(worker(1).counters().Get("b"), 0);
  EXPECT_EQ(worker(2).counters().Get("b"), 1);
  EXPECT_EQ(master().counters().Get("b"), 2);
}

TEST_F(DistributedGraphDbTest, DispatchPlan) {
  auto kRPCWaitTime = 600ms;
  int64_t plan_id = 5;
  SymbolTable symbol_table;
  AstTreeStorage storage;

  auto scan_all = MakeScanAll(storage, symbol_table, "n");

  master().plan_dispatcher().DispatchPlan(plan_id, scan_all.op_, symbol_table);
  std::this_thread::sleep_for(kRPCWaitTime);

  auto check_for_worker = [plan_id, &symbol_table](auto &worker) {
    auto &cached = worker.plan_consumer().PlanForId(plan_id);
    EXPECT_NE(dynamic_cast<query::plan::ScanAll *>(cached.plan.get()), nullptr);
    EXPECT_EQ(cached.symbol_table.max_position(), symbol_table.max_position());
    EXPECT_EQ(cached.symbol_table.table(), symbol_table.table());
  };
  check_for_worker(worker(1));
  check_for_worker(worker(2));
}

TEST_F(DistributedGraphDbTest, RemotePullProduceRpc) {
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

  Parameters params;
  std::vector<query::Symbol> symbols{ctx.symbol_table_[*x_ne]};
  auto remote_pull = [this, &params, &symbols](GraphDbAccessor &dba,
                                               int worker_id) {
    return master().remote_pull_clients().RemotePull(dba, worker_id, plan_id,
                                                     params, symbols, 3);
  };
  auto expect_first_batch = [](auto &batch) {
    EXPECT_EQ(batch.pull_state,
              distributed::RemotePullState::CURSOR_IN_PROGRESS);
    ASSERT_EQ(batch.frames.size(), 3);
    ASSERT_EQ(batch.frames[0].size(), 1);
    EXPECT_EQ(batch.frames[0][0].ValueInt(), 42);
    EXPECT_EQ(batch.frames[1][0].ValueBool(), true);
    EXPECT_EQ(batch.frames[2][0].ValueString(), "bla");
  };
  auto expect_second_batch = [](auto &batch) {
    EXPECT_EQ(batch.pull_state, distributed::RemotePullState::CURSOR_EXHAUSTED);
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
  for (auto tx_id : {dba_1.transaction_id(), dba_2.transaction_id()})
    master().remote_pull_clients().EndAllRemotePulls(tx_id, plan_id);
}

TEST_F(DistributedGraphDbTest, RemotePullProduceRpcWithGraphElements) {
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

  Parameters params;
  std::vector<query::Symbol> symbols{ctx.symbol_table_[*return_n_r],
                                     ctx.symbol_table_[*return_m], p_sym};
  auto remote_pull = [this, &params, &symbols](GraphDbAccessor &dba,
                                               int worker_id) {
    return master().remote_pull_clients().RemotePull(dba, worker_id, plan_id,
                                                     params, symbols, 3);
  };
  auto future_w1_results = remote_pull(dba, 1);
  auto future_w2_results = remote_pull(dba, 2);
  check_result(1, future_w1_results.get().frames);
  check_result(2, future_w2_results.get().frames);

  master().remote_pull_clients().EndAllRemotePulls(dba.transaction_id(),
                                                   plan_id);
}

TEST_F(DistributedGraphDbTest, BuildIndexDistributed) {
  storage::Label label;
  storage::Property property;

  {
    GraphDbAccessor dba0{master()};
    label = dba0.Label("label");
    property = dba0.Property("property");
    auto tx_id = dba0.transaction_id();

    GraphDbAccessor dba1{worker(1), tx_id};
    GraphDbAccessor dba2{worker(2), tx_id};
    auto add_vertex = [label, property](GraphDbAccessor &dba) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(property, 1);
    };
    for (int i = 0; i < 100; ++i) add_vertex(dba0);
    for (int i = 0; i < 50; ++i) add_vertex(dba1);
    for (int i = 0; i < 300; ++i) add_vertex(dba2);
    dba0.Commit();
  }

  {
    GraphDbAccessor dba{master()};
    dba.BuildIndex(label, property);
    EXPECT_TRUE(dba.LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba.Vertices(label, property, false)), 100);
  }

  GraphDbAccessor dba_master{master()};

  {
    GraphDbAccessor dba{worker(1), dba_master.transaction_id()};
    EXPECT_TRUE(dba.LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba.Vertices(label, property, false)), 50);
  }

  {
    GraphDbAccessor dba{worker(2), dba_master.transaction_id()};
    EXPECT_TRUE(dba.LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba.Vertices(label, property, false)), 300);
  }
}

TEST_F(DistributedGraphDbTest, WorkerOwnedDbAccessors) {
  GraphDbAccessor dba_w1(worker(1));
  auto v = dba_w1.InsertVertex();
  auto prop = dba_w1.Property("p");
  v.PropsSet(prop, 42);
  auto v_ga = v.GlobalAddress();
  dba_w1.Commit();

  GraphDbAccessor dba_w2(worker(2));
  VertexAccessor v_in_w2{v_ga, dba_w2};
  EXPECT_EQ(v_in_w2.PropsAt(prop).Value<int64_t>(), 42);
}
