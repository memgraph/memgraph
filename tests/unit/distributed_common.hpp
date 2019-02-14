#include <experimental/filesystem>
#include <memory>
#include <thread>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "database/distributed/graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "storage/distributed/address_types.hpp"
#include "transactions/distributed/engine_master.hpp"
#include "utils/file.hpp"

DECLARE_string(durability_directory);

namespace fs = std::experimental::filesystem;

class WorkerInThread {
 public:
  explicit WorkerInThread(database::Config config) : worker_(config) {
    thread_ = std::thread([this, config] {
      worker_.Start();
      EXPECT_TRUE(worker_.AwaitShutdown());
    });
  }

  ~WorkerInThread() {
    if (thread_.joinable()) thread_.join();
  }

  database::Worker *db() { return &worker_; }

  database::Worker worker_;
  std::thread thread_;
};

class DistributedGraphDbTest : public ::testing::Test {
 public:
  const std::string kLocal = "127.0.0.1";
  const int kWorkerCount = 2;

 protected:
  virtual int QueryExecutionTimeSec(int) { return 180; }

  void Initialize(
      std::function<database::Config(database::Config config)> modify_config) {
    using namespace std::literals::chrono_literals;
    const auto kInitTime = 200ms;

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_config.query_execution_time_sec = QueryExecutionTimeSec(0);
    master_config.durability_directory = GetDurabilityDirectory(0);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(0);
    // This is semantically wrong since this is not a cluster of size 1 but of
    // size kWorkerCount+1, but it's hard to wait here for workers to recover
    // and simultaneously assign the port to which the workers must connect.
    // TODO (buda): Fix sometime in the future - not mission critical.
    master_config.recovering_cluster_size = 1;
    master_ = std::make_unique<database::Master>(modify_config(master_config));
    master_->Start();

    std::this_thread::sleep_for(kInitTime);
    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.durability_directory = GetDurabilityDirectory(worker_id);
      config.worker_endpoint = {kLocal, 0};
      config.query_execution_time_sec = QueryExecutionTimeSec(worker_id);
      return config;
    };

    for (int i = 0; i < kWorkerCount; ++i) {
      // Flag needs to be updated due to props on disk storage.
      FLAGS_durability_directory = GetDurabilityDirectory(i + 1);
      workers_.emplace_back(std::make_unique<WorkerInThread>(
          modify_config(worker_config(i + 1))));
      std::this_thread::sleep_for(kInitTime);
    }

    // Wait for the whole cluster to be up and running.
    std::this_thread::sleep_for(kInitTime);
    while (master_->GetWorkerIds().size() < kWorkerCount + 1) {
      std::this_thread::sleep_for(kInitTime);
    }
    for (int i = 0; i < kWorkerCount; ++i) {
      while (workers_[i]->worker_.GetWorkerIds().size() < kWorkerCount + 1) {
        std::this_thread::sleep_for(kInitTime);
      }
    }
    std::this_thread::sleep_for(kInitTime);
  }

  void SetUp() override {
    Initialize([](database::Config config) { return config; });
  }

  void ShutDown() {
    // Shutdown the master. It will send a shutdown signal to the workers.
    master_->Shutdown();
    EXPECT_TRUE(master_->AwaitShutdown());
    // Wait for all workers to finish shutting down.
    workers_.clear();
  }

  fs::path GetDurabilityDirectory(int worker_id) {
    if (worker_id == 0) return tmp_dir_ / "master";
    return tmp_dir_ / fmt::format("worker{}", worker_id);
  }

  void CleanDurability() {
    if (fs::exists(tmp_dir_)) fs::remove_all(tmp_dir_);
  }

  void TearDown() override {
    ShutDown();
    CleanDurability();
  }

  database::Master &master() { return *master_; }

  database::Worker &worker(int worker_id) {
    return workers_[worker_id - 1]->worker_;
  }

  /// Inserts a vertex and returns it's global address. Does it in a new
  /// transaction.
  storage::VertexAddress InsertVertex(database::GraphDb &db) {
    auto dba = db.Access();
    auto r_val = dba->InsertVertex().GlobalAddress();
    dba->Commit();
    return r_val;
  }

  /// Inserts an edge (on the 'from' side) and returns it's global address.
  auto InsertEdge(storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr,
                  const std::string &edge_type_name) {
    CHECK(from_addr.is_remote() && to_addr.is_remote())
        << "Distributed test InsertEdge only takes global addresses";
    auto dba = master().Access();
    VertexAccessor from{from_addr, *dba};
    VertexAccessor to{to_addr, *dba};
    auto r_val = dba->InsertEdge(from, to, dba->EdgeType(edge_type_name))
                     .GlobalAddress();
    master().updates_server().Apply(dba->transaction_id());
    worker(1).updates_server().Apply(dba->transaction_id());
    worker(2).updates_server().Apply(dba->transaction_id());
    dba->Commit();
    return r_val;
  }

  auto VertexCount(database::GraphDb &db) {
    auto dba = db.Access();
    auto vertices = dba->Vertices(false);
    return std::distance(vertices.begin(), vertices.end());
  };

  auto EdgeCount(database::GraphDb &db) {
    auto dba = db.Access();
    auto edges = dba->Edges(false);
    return std::distance(edges.begin(), edges.end());
  };

  fs::path tmp_dir_{fs::temp_directory_path() / "MG_test_unit_durability"};

 public:
  // Each test has to specify its own durability suffix to avoid conflicts
  DistributedGraphDbTest() = delete;

  explicit DistributedGraphDbTest(const std::string &dir_suffix)
      : dir_suffix_(dir_suffix) {
    tmp_dir_ =
        fs::temp_directory_path() / ("MG_test_unit_durability_" + dir_suffix_);
  }

 private:
  std::string dir_suffix_{""};
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
};

class Cluster {
 public:
  Cluster(int num_workers, const std::string &test_name) {
    using namespace std::literals::chrono_literals;
    tmp_dir_ = fs::temp_directory_path() / "MG_test_unit_distributed_common_" /
               test_name;
    EXPECT_TRUE(utils::EnsureDir(tmp_dir_));

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_config.durability_directory = GetDurabilityDirectory(0);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(0);

    master_ = std::make_unique<database::Master>(master_config);
    master_->Start();
    auto master_endpoint = master_->endpoint();

    const auto kInitTime = 200ms;
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this, master_endpoint](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_endpoint;
      config.worker_endpoint = {kLocal, 0};
      config.durability_directory = GetDurabilityDirectory(worker_id);
      return config;
    };

    for (int i = 0; i < num_workers; ++i) {
      // Flag needs to be updated due to props on disk storage.
      FLAGS_durability_directory = GetDurabilityDirectory(i + 1);
      workers_.emplace_back(
          std::make_unique<WorkerInThread>(worker_config(i + 1)));
      std::this_thread::sleep_for(kInitTime);
    }

    // Wait for the whole cluster to be up and running.
    std::this_thread::sleep_for(kInitTime);
    while (master_->GetWorkerIds().size() < num_workers + 1) {
      std::this_thread::sleep_for(kInitTime);
    }
    for (int i = 0; i < num_workers; ++i) {
      while (workers_[i]->worker_.GetWorkerIds().size() < num_workers + 1) {
        std::this_thread::sleep_for(kInitTime);
      }
    }
    std::this_thread::sleep_for(kInitTime);
  }

  Cluster(const Cluster &) = delete;
  Cluster(Cluster &&) = delete;
  Cluster &operator=(const Cluster &) = delete;
  Cluster &operator=(Cluster &&) = delete;

  ~Cluster() {
    master_->Shutdown();
    EXPECT_TRUE(master_->AwaitShutdown());
    workers_.clear();
    if (fs::exists(tmp_dir_)) fs::remove_all(tmp_dir_);
  }

  auto *master() { return master_.get(); }
  auto workers() {
    return iter::imap([](auto &worker) { return worker->db(); }, workers_);
  }

  void ClearCache(tx::TransactionId tx_id) {
    master()->data_manager().ClearCacheForSingleTransaction(tx_id);
    for (auto member : workers()) {
      member->data_manager().ClearCacheForSingleTransaction(tx_id);
    }
  }

  void ApplyUpdates(tx::TransactionId tx_id) {
    master()->updates_server().Apply(tx_id);
    for (auto member : workers()) {
      member->updates_server().Apply(tx_id);
    }
    ClearCache(tx_id);
  }

  void AdvanceCommand(tx::TransactionId tx_id) {
    ApplyUpdates(tx_id);
    master()->tx_engine().Advance(tx_id);
    for (auto worker : workers()) worker->tx_engine().UpdateCommand(tx_id);
    ClearCache(tx_id);
  }

  fs::path GetDurabilityDirectory(int worker_id) {
    if (worker_id == 0) return tmp_dir_ / "master";
    return tmp_dir_ / fmt::format("worker{}", worker_id);
  }

 private:
  const std::string kLocal = "127.0.0.1";

  fs::path tmp_dir_{fs::temp_directory_path() /
                    "MG_test_unit_distributed_common"};

  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
};
