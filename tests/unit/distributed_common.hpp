#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "storage/address_types.hpp"
#include "transactions/engine_master.hpp"

class DistributedGraphDbTest : public ::testing::Test {
  const std::string kLocal = "127.0.0.1";
  const int kWorkerCount = 2;

  class WorkerInThread {
   public:
    explicit WorkerInThread(database::Config config) : worker_(config) {
      thread_ = std::thread([this, config] { worker_.WaitForShutdown(); });
    }

    ~WorkerInThread() {
      if (thread_.joinable()) thread_.join();
    }

    database::Worker worker_;
    std::thread thread_;
  };

 protected:
  virtual int QueryExecutionTimeSec(int) { return 180; }

  void SetUp() override {
    const auto kInitTime = 200ms;

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_config.query_execution_time_sec = QueryExecutionTimeSec(0);
    master_ = std::make_unique<database::Master>(master_config);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
      config.query_execution_time_sec = QueryExecutionTimeSec(worker_id);
      return config;
    };

    for (int i = 0; i < kWorkerCount; ++i) {
      workers_.emplace_back(
          std::make_unique<WorkerInThread>(worker_config(i + 1)));
      std::this_thread::sleep_for(kInitTime);
    }
  }

  void TearDown() override {
    // Kill master first because it will expect a shutdown response from the
    // workers.
    master_ = nullptr;
    for (int i = kWorkerCount - 1; i >= 0; --i) workers_[i] = nullptr;
  }

  database::Master &master() { return *master_; }
  auto &master_tx_engine() {
    return dynamic_cast<tx::MasterEngine &>(master_->tx_engine());
  }

  database::Worker &worker(int worker_id) {
    return workers_[worker_id - 1]->worker_;
  }

  /// Inserts a vertex and returns it's global address. Does it in a new
  /// transaction.
  storage::VertexAddress InsertVertex(database::GraphDb &db) {
    database::GraphDbAccessor dba{db};
    auto r_val = dba.InsertVertex().GlobalAddress();
    dba.Commit();
    return r_val;
  }

  /// Inserts an edge (on the 'from' side) and returns it's global address.
  auto InsertEdge(storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr,
                  const std::string &edge_type_name) {
    CHECK(from_addr.is_remote() && to_addr.is_remote())
        << "Distributed test InsertEdge only takes global addresses";
    database::GraphDbAccessor dba{master()};
    VertexAccessor from{from_addr, dba};
    VertexAccessor to{to_addr, dba};
    auto r_val =
        dba.InsertEdge(from, to, dba.EdgeType(edge_type_name)).GlobalAddress();
    master().updates_server().Apply(dba.transaction_id());
    worker(1).updates_server().Apply(dba.transaction_id());
    worker(2).updates_server().Apply(dba.transaction_id());
    dba.Commit();
    return r_val;
  }

  auto VertexCount(database::GraphDb &db) {
    database::GraphDbAccessor dba{db};
    auto vertices = dba.Vertices(false);
    return std::distance(vertices.begin(), vertices.end());
  };

  auto EdgeCount(database::GraphDb &db) {
    database::GraphDbAccessor dba(db);
    auto edges = dba.Edges(false);
    return std::distance(edges.begin(), edges.end());
  };

 private:
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
};
