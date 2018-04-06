#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "database/counters.hpp"
#include "database/storage.hpp"
#include "database/storage_gc.hpp"
#include "durability/wal.hpp"
#include "io/network/endpoint.hpp"
#include "storage/concurrent_id_mapper.hpp"
#include "storage/types.hpp"
#include "transactions/engine.hpp"
#include "utils/scheduler.hpp"

namespace distributed {
class DataRpcServer;
class DataRpcClients;
class PlanDispatcher;
class PlanConsumer;
class PullRpcClients;
class ProduceRpcServer;
class UpdatesRpcServer;
class UpdatesRpcClients;
class DataManager;
class IndexRpcClients;
}  // namespace distributed

namespace database {

/// Database configuration. Initialized from flags, but modifiable.
struct Config {
  Config();
  // Durability flags.
  bool durability_enabled;
  std::string durability_directory;
  bool db_recover_on_startup;
  int snapshot_cycle_sec;
  int snapshot_max_retained;
  int snapshot_on_exit;

  // Misc flags.
  int gc_cycle_sec;
  int query_execution_time_sec;
  int rpc_num_workers;

  // Distributed master/worker flags.
  int worker_id;
  io::network::Endpoint master_endpoint;
  io::network::Endpoint worker_endpoint;
};

/**
 * An abstract base class for a SingleNode/Master/Worker graph db.
 *
 * Always be sure that GraphDb object is destructed before main exits, i. e.
 * GraphDb object shouldn't be part of global/static variable, except if its
 * destructor is explicitly called before main exits. Consider code:
 *
 * GraphDb db;  // KeyIndex is created as a part of database::Storage
 * int main() {
 *   GraphDbAccessor dba(db);
 *   auto v = dba.InsertVertex();
 *   v.add_label(dba.Label(
 *       "Start"));  // New SkipList is created in KeyIndex for LabelIndex.
 *                   // That SkipList creates SkipListGc which
 *                   // initialises static Executor object.
 *   return 0;
 * }
 *
 * After main exits: 1. Executor is destructed, 2. KeyIndex is destructed.
 * Destructor of KeyIndex calls delete on created SkipLists which destroy
 * SkipListGc that tries to use Excutioner object that doesn't exist anymore.
 * -> CRASH
 */
class GraphDb {
 public:
  enum class Type { SINGLE_NODE, DISTRIBUTED_MASTER, DISTRIBUTED_WORKER };

  GraphDb() {}
  virtual ~GraphDb() {}

  virtual Type type() const = 0;
  virtual Storage &storage() = 0;
  virtual durability::WriteAheadLog &wal() = 0;
  virtual tx::Engine &tx_engine() = 0;
  virtual storage::ConcurrentIdMapper<storage::Label> &label_mapper() = 0;
  virtual storage::ConcurrentIdMapper<storage::EdgeType>
      &edge_type_mapper() = 0;
  virtual storage::ConcurrentIdMapper<storage::Property> &property_mapper() = 0;
  virtual database::Counters &counters() = 0;
  virtual void CollectGarbage() = 0;
  virtual int WorkerId() const = 0;
  virtual std::vector<int> GetWorkerIds() const = 0;

  // Supported only in distributed master and worker, not in single-node.
  virtual distributed::DataRpcServer &data_server() = 0;
  virtual distributed::DataRpcClients &data_clients() = 0;
  virtual distributed::UpdatesRpcServer &updates_server() = 0;
  virtual distributed::UpdatesRpcClients &updates_clients() = 0;
  virtual distributed::DataManager &data_manager() = 0;

  // Supported only in distributed master.
  virtual distributed::PullRpcClients &pull_clients() = 0;
  virtual distributed::PlanDispatcher &plan_dispatcher() = 0;
  virtual distributed::IndexRpcClients &index_rpc_clients() = 0;

  // Supported only in distributed worker.
  // TODO remove once end2end testing is possible.
  virtual distributed::ProduceRpcServer &produce_server() = 0;
  virtual distributed::PlanConsumer &plan_consumer() = 0;

  // Makes a snapshot from the visibility of the given accessor
  virtual bool MakeSnapshot(GraphDbAccessor &accessor) = 0;

  GraphDb(const GraphDb &) = delete;
  GraphDb(GraphDb &&) = delete;
  GraphDb &operator=(const GraphDb &) = delete;
  GraphDb &operator=(GraphDb &&) = delete;
};

namespace impl {
// Private GraphDb implementations all inherit `PrivateBase`.
// Public GraphDb implementations  all inherit `PublicBase`.
class PrivateBase;

// Base class for all GraphDb implementations exposes to the client programmer.
// Encapsulates an instance of a private implementation of GraphDb and performs
// initialization and cleanup.
class PublicBase : public GraphDb {
 public:
  Type type() const override;
  Storage &storage() override;
  durability::WriteAheadLog &wal() override;
  tx::Engine &tx_engine() override;
  storage::ConcurrentIdMapper<storage::Label> &label_mapper() override;
  storage::ConcurrentIdMapper<storage::EdgeType> &edge_type_mapper() override;
  storage::ConcurrentIdMapper<storage::Property> &property_mapper() override;
  database::Counters &counters() override;
  void CollectGarbage() override;
  int WorkerId() const override;
  std::vector<int> GetWorkerIds() const override;
  distributed::DataRpcServer &data_server() override;
  distributed::DataRpcClients &data_clients() override;
  distributed::PlanDispatcher &plan_dispatcher() override;
  distributed::IndexRpcClients &index_rpc_clients() override;
  distributed::PlanConsumer &plan_consumer() override;
  distributed::PullRpcClients &pull_clients() override;
  distributed::ProduceRpcServer &produce_server() override;
  distributed::UpdatesRpcServer &updates_server() override;
  distributed::UpdatesRpcClients &updates_clients() override;
  distributed::DataManager &data_manager() override;

  bool is_accepting_transactions() const { return is_accepting_transactions_; }
  bool MakeSnapshot(GraphDbAccessor &accessor) override;

 protected:
  explicit PublicBase(std::unique_ptr<PrivateBase> impl);
  ~PublicBase();

  std::unique_ptr<PrivateBase> impl_;

 private:
  /** When this is false, no new transactions should be created. */
  std::atomic<bool> is_accepting_transactions_{true};
  Scheduler transaction_killer_;
};
}  // namespace impl

class MasterBase : public impl::PublicBase {
 public:
  MasterBase(std::unique_ptr<impl::PrivateBase> impl);
  ~MasterBase();

 private:
  std::unique_ptr<Scheduler> snapshot_creator_;
};

class SingleNode : public MasterBase {
 public:
  explicit SingleNode(Config config = Config());
};

class Master : public MasterBase {
 public:
  explicit Master(Config config = Config());
  /** Gets this master's endpoint. */
  io::network::Endpoint endpoint() const;
  /** Gets the endpoint of the worker with the given id. */
  // TODO make const once Coordination::GetEndpoint is const.
  io::network::Endpoint GetEndpoint(int worker_id);
};

class Worker : public impl::PublicBase {
 public:
  explicit Worker(Config config = Config());
  /** Gets this worker's endpoint. */
  io::network::Endpoint endpoint() const;
  /** Gets the endpoint of the worker with the given id. */
  // TODO make const once Coordination::GetEndpoint is const.
  io::network::Endpoint GetEndpoint(int worker_id);
  void WaitForShutdown();
};
}  // namespace database
