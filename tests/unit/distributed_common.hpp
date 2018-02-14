#include <memory>

#include <gtest/gtest.h>

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
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
  void SetUp() override {
    const auto kInitTime = 200ms;

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_ = std::make_unique<database::Master>(master_config);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
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
  auto InsertVertex(database::GraphDb &db) {
    database::GraphDbAccessor dba{db};
    auto r_val = dba.InsertVertex().GlobalAddress();
    dba.Commit();
    return r_val;
  }

  /// Inserts an edge (on the 'from' side) and returns it's global address.
  auto InsertEdge(Edges::VertexAddress from, Edges::VertexAddress to,
                  const std::string &edge_type_name) {
    database::GraphDbAccessor dba{worker(from.worker_id())};
    auto from_v = dba.FindVertexChecked(from.gid(), false);
    auto edge_type = dba.EdgeType(edge_type_name);

    // If 'to' is on the same worker as 'from', create and send local.
    if (to.worker_id() == from.worker_id()) {
      auto to_v = dba.FindVertexChecked(to.gid(), false);
      auto r_val = dba.InsertEdge(from_v, to_v, edge_type).GlobalAddress();
      dba.Commit();
      return r_val;
    }

    // 'to' is not on the same worker as 'from'
    auto edge_ga = dba.InsertOnlyEdge(from, to, edge_type,
                                      dba.db().storage().EdgeGenerator().Next())
                       .GlobalAddress();
    from_v.update().out_.emplace(to, edge_ga, edge_type);
    database::GraphDbAccessor dba_to{worker(to.worker_id()),
                                     dba.transaction_id()};
    auto to_v = dba_to.FindVertexChecked(to.gid(), false);
    to_v.update().in_.emplace(from, edge_ga, edge_type);

    dba.Commit();
    return edge_ga;
  }

 private:
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
};
