#include <memory>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

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
#include "query/plan/planner.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"
#include "transactions/distributed/engine_master.hpp"

using database::GraphDbAccessor;
using namespace distributed;
using namespace std::literals::chrono_literals;

class DistributedGraphDb : public DistributedGraphDbTest {
 public:
  DistributedGraphDb() : DistributedGraphDbTest("distributed_graph") {}
};

TEST_F(DistributedGraphDb, Coordination) {
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

TEST_F(DistributedGraphDb, TxEngine) {
  auto *tx1 = master().tx_engine().Begin();
  auto *tx2 = master().tx_engine().Begin();
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

TEST_F(DistributedGraphDb, StorageTypes) {
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

TEST_F(DistributedGraphDb, DispatchPlan) {
  auto kRPCWaitTime = 600ms;
  int64_t plan_id = 5;
  SymbolTable symbol_table;
  AstStorage storage;

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

  master().plan_dispatcher().RemovePlan(plan_id);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(check_for_worker(worker(1)), "Missing plan*");
}

TEST_F(DistributedGraphDb, BuildIndexDistributed) {
  storage::Label label;
  storage::Property property;

  {
    auto dba0 = master().Access();
    label = dba0->Label("label");
    property = dba0->Property("property");
    auto tx_id = dba0->transaction_id();

    auto dba1 = worker(1).Access(tx_id);
    auto dba2 = worker(2).Access(tx_id);
    auto add_vertex = [label, property](GraphDbAccessor &dba) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(property, 1);
    };
    for (int i = 0; i < 100; ++i) add_vertex(*dba0);
    for (int i = 0; i < 50; ++i) add_vertex(*dba1);
    for (int i = 0; i < 300; ++i) add_vertex(*dba2);
    dba0->Commit();
  }

  {
    auto dba = master().Access();
    dba->BuildIndex(label, property);
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba->Vertices(label, property, false)), 100);
  }

  auto dba_master = master().Access();

  {
    auto dba = worker(1).Access(dba_master->transaction_id());
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba->Vertices(label, property, false)), 50);
  }

  {
    auto dba = worker(2).Access(dba_master->transaction_id());
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba->Vertices(label, property, false)), 300);
  }
}

TEST_F(DistributedGraphDb, BuildIndexConcurrentInsert) {
  storage::Label label;
  storage::Property property;

  auto dba0 = master().Access();
  label = dba0->Label("label");
  property = dba0->Property("property");

  int cnt = 0;
  auto add_vertex = [label, property, &cnt](GraphDbAccessor &dba) {
    auto vertex = dba.InsertVertex();
    vertex.add_label(label);
    vertex.PropsSet(property, ++cnt);
  };
  dba0->Commit();

  auto worker_insert = std::thread([this, &add_vertex]() {
    for (int i = 0; i < 10000; ++i) {
      auto dba1 = worker(1).Access();
      add_vertex(*dba1);
      dba1->Commit();
    }
  });

  std::this_thread::sleep_for(0.5s);
  {
    auto dba = master().Access();
    dba->BuildIndex(label, property);
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
  }

  worker_insert.join();
  {
    auto dba = worker(1).Access();
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba->Vertices(label, property, false)), 10000);
  }
}

TEST_F(DistributedGraphDb, WorkerOwnedDbAccessors) {
  auto dba_w1 = worker(1).Access();
  auto v = dba_w1->InsertVertex();
  auto prop = dba_w1->Property("p");
  v.PropsSet(prop, 42);
  auto v_ga = v.GlobalAddress();
  dba_w1->Commit();

  auto dba_w2 = worker(2).Access();
  VertexAccessor v_in_w2{v_ga, *dba_w2};
  EXPECT_EQ(v_in_w2.PropsAt(prop).Value<int64_t>(), 42);
}
