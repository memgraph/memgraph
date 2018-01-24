#include <experimental/optional>
#include <thread>

#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"
#include "database/graph_db.hpp"
#include "distributed/coordination.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "distributed/remote_data_rpc_server.hpp"
#include "io/network/endpoint.hpp"
#include "query_plan_common.hpp"
#include "transactions/engine_master.hpp"

template <typename T>
using optional = std::experimental::optional<T>;

using namespace distributed;

class DistributedGraphDbTest : public ::testing::Test {
  const std::string kLocal = "127.0.0.1";
  class WorkerInThread {
   public:
    WorkerInThread(database::Config config) : worker_(config) {
      thread_ = std::thread([this, config] { worker_.WaitForShutdown(); });
    }

    ~WorkerInThread() {
      if (thread_.joinable()) thread_.join();
    }

    database::Worker worker_;
    std::thread thread_;
  };

 protected:
  void SetUp() override {
    const auto kInitTime = 200ms;

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_.emplace(master_config);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
      return config;
    };

    worker1_.emplace(worker_config(1));
    std::this_thread::sleep_for(kInitTime);
    worker2_.emplace(worker_config(2));
    std::this_thread::sleep_for(kInitTime);
  }

  void TearDown() override {
    // Kill master first because it will expect a shutdown response from the
    // workers.
    master_ = std::experimental::nullopt;

    worker2_ = std::experimental::nullopt;
    worker1_ = std::experimental::nullopt;
  }

  database::Master &master() { return *master_; }
  auto &master_tx_engine() {
    return dynamic_cast<tx::MasterEngine &>(master_->tx_engine());
  }
  database::Worker &worker1() { return worker1_->worker_; }
  database::Worker &worker2() { return worker2_->worker_; }

 private:
  optional<database::Master> master_;
  optional<WorkerInThread> worker1_;
  optional<WorkerInThread> worker2_;
};

TEST_F(DistributedGraphDbTest, Coordination) {
  EXPECT_NE(master().endpoint().port(), 0);
  EXPECT_NE(worker1().endpoint().port(), 0);
  EXPECT_NE(worker2().endpoint().port(), 0);

  EXPECT_EQ(master().GetEndpoint(1), worker1().endpoint());
  EXPECT_EQ(master().GetEndpoint(2), worker2().endpoint());
  EXPECT_EQ(worker1().GetEndpoint(0), master().endpoint());
  EXPECT_EQ(worker1().GetEndpoint(2), worker2().endpoint());
  EXPECT_EQ(worker2().GetEndpoint(0), master().endpoint());
  EXPECT_EQ(worker2().GetEndpoint(1), worker1().endpoint());
}

TEST_F(DistributedGraphDbTest, TxEngine) {
  auto *tx1 = master_tx_engine().Begin();
  auto *tx2 = master_tx_engine().Begin();
  EXPECT_EQ(tx2->snapshot().size(), 1);
  EXPECT_EQ(
      worker1().tx_engine().RunningTransaction(tx1->id_)->snapshot().size(), 0);
  EXPECT_EQ(worker2().tx_engine().RunningTransaction(tx2->id_)->snapshot(),
            tx2->snapshot());

  ::testing::FLAGS_gtest_death_test_style = "fast";
  EXPECT_DEATH(worker2().tx_engine().RunningTransaction(123), "");
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
                                          worker1().label_mapper(),
                                          worker2().label_mapper()},
               std::vector<storage::Label>{});
  test_mappers(mapper_vec<storage::EdgeType>{master().edge_type_mapper(),
                                             worker1().edge_type_mapper(),
                                             worker2().edge_type_mapper()},
               std::vector<storage::EdgeType>{});
  test_mappers(mapper_vec<storage::Property>{master().property_mapper(),
                                             worker1().property_mapper(),
                                             worker2().property_mapper()},
               std::vector<storage::Property>{});
}

TEST_F(DistributedGraphDbTest, Counters) {
  EXPECT_EQ(master().counters().Get("a"), 0);
  EXPECT_EQ(worker1().counters().Get("a"), 1);
  EXPECT_EQ(worker2().counters().Get("a"), 2);

  EXPECT_EQ(worker1().counters().Get("b"), 0);
  EXPECT_EQ(worker2().counters().Get("b"), 1);
  EXPECT_EQ(master().counters().Get("b"), 2);
}

TEST_F(DistributedGraphDbTest, RemoteDataGetting) {
  using GraphDbAccessor = database::GraphDbAccessor;
  // Only old data is visible remotely, so create and commit some data.
  gid::Gid v1_id, v2_id, e1_id;

  {
    GraphDbAccessor dba{master()};
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    auto e1 = dba.InsertEdge(v1, v2, dba.EdgeType("et"));

    // Set some data so we see we're getting the right stuff.
    v1.PropsSet(dba.Property("p1"), 42);
    v1.add_label(dba.Label("label"));
    v2.PropsSet(dba.Property("p2"), "value");
    e1.PropsSet(dba.Property("p3"), true);

    v1_id = v1.gid();
    v2_id = v2.gid();
    e1_id = e1.gid();

    dba.Commit();
  }

  // The master must start a transaction before workers can work in it.
  database::GraphDbAccessor master_dba{master()};

  {
    database::GraphDbAccessor w1_dba{worker1(), master_dba.transaction_id()};
    VertexAccessor v1_in_w1{{v1_id, 0}, w1_dba};
    EXPECT_NE(v1_in_w1.GetOld(), nullptr);
    EXPECT_EQ(v1_in_w1.GetNew(), nullptr);
    EXPECT_EQ(v1_in_w1.PropsAt(w1_dba.Property("p1")).Value<int64_t>(), 42);
    EXPECT_TRUE(v1_in_w1.has_label(w1_dba.Label("label")));
  }

  {
    database::GraphDbAccessor w2_dba{worker2(), master_dba.transaction_id()};
    VertexAccessor v2_in_w2{{v2_id, 0}, w2_dba};
    EXPECT_NE(v2_in_w2.GetOld(), nullptr);
    EXPECT_EQ(v2_in_w2.GetNew(), nullptr);
    EXPECT_EQ(v2_in_w2.PropsAt(w2_dba.Property("p2")).Value<std::string>(),
              "value");
    EXPECT_FALSE(v2_in_w2.has_label(w2_dba.Label("label")));

    VertexAccessor v1_in_w2{{v1_id, 0}, w2_dba};
    EdgeAccessor e1_in_w2{{e1_id, 0}, w2_dba};
    EXPECT_EQ(e1_in_w2.from(), v1_in_w2);
    EXPECT_EQ(e1_in_w2.to(), v2_in_w2);
    EXPECT_EQ(e1_in_w2.EdgeType(), w2_dba.EdgeType("et"));
    EXPECT_EQ(e1_in_w2.PropsAt(w2_dba.Property("p3")).Value<bool>(), true);
  }
}

TEST_F(DistributedGraphDbTest, DispatchPlan) {
  auto kRPCWaitTime = 600ms;
  int64_t plan_id = 5;
  SymbolTable symbol_table;
  AstTreeStorage storage;

  auto scan_all = MakeScanAll(storage, symbol_table, "n");

  master().plan_dispatcher().DispatchPlan(plan_id, scan_all.op_, symbol_table);
  std::this_thread::sleep_for(kRPCWaitTime);

  {
    auto cached = worker1().plan_consumer().PlanForId(plan_id);
    EXPECT_NE(dynamic_cast<query::plan::ScanAll *>(cached.first.get()),
              nullptr);
    EXPECT_EQ(cached.second.max_position(), symbol_table.max_position());
    EXPECT_EQ(cached.second.table(), symbol_table.table());
  }
  {
    auto cached = worker2().plan_consumer().PlanForId(plan_id);
    EXPECT_NE(dynamic_cast<query::plan::ScanAll *>(cached.first.get()),
              nullptr);
    EXPECT_EQ(cached.second.max_position(), symbol_table.max_position());
    EXPECT_EQ(cached.second.table(), symbol_table.table());
  }
}
