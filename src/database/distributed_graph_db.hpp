#pragma once

#include "database/graph_db.hpp"
#include "durability/recovery.hpp"

namespace distributed {
class BfsRpcServer;
class BfsRpcClients;
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

namespace impl {
class Master;
class Worker;
}  // namespace impl

/// Abstract base class for concrete distributed versions of GraphDb
class DistributedGraphDb : public GraphDb {
 public:
  virtual distributed::BfsRpcClients &bfs_subcursor_clients() = 0;
  virtual distributed::DataRpcClients &data_clients() = 0;
  virtual distributed::UpdatesRpcServer &updates_server() = 0;
  virtual distributed::UpdatesRpcClients &updates_clients() = 0;
  virtual distributed::DataManager &data_manager() = 0;
};

class Master final : public DistributedGraphDb {
 public:
  explicit Master(Config config = Config());
  ~Master();

  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_MASTER;
  }

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
  bool MakeSnapshot(GraphDbAccessor &accessor) override;
  void ReinitializeStorage() override;

  /** Gets this master's endpoint. */
  io::network::Endpoint endpoint() const;
  /** Gets the endpoint of the worker with the given id. */
  // TODO make const once Coordination::GetEndpoint is const.
  io::network::Endpoint GetEndpoint(int worker_id);

  distributed::BfsRpcClients &bfs_subcursor_clients() override;
  distributed::DataRpcClients &data_clients() override;
  distributed::UpdatesRpcServer &updates_server() override;
  distributed::UpdatesRpcClients &updates_clients() override;
  distributed::DataManager &data_manager() override;

  distributed::PullRpcClients &pull_clients();
  distributed::PlanDispatcher &plan_dispatcher();
  distributed::IndexRpcClients &index_rpc_clients();

 private:
  std::unique_ptr<impl::Master> impl_;

  utils::Scheduler transaction_killer_;
  std::unique_ptr<utils::Scheduler> snapshot_creator_;
};

class Worker final : public DistributedGraphDb {
 public:
  explicit Worker(Config config = Config());
  ~Worker();

  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_WORKER;
  }

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
  bool MakeSnapshot(GraphDbAccessor &accessor) override;
  void ReinitializeStorage() override;
  void RecoverWalAndIndexes(durability::RecoveryData *recovery_data);

  /** Gets this worker's endpoint. */
  io::network::Endpoint endpoint() const;
  /** Gets the endpoint of the worker with the given id. */
  // TODO make const once Coordination::GetEndpoint is const.
  io::network::Endpoint GetEndpoint(int worker_id);
  void WaitForShutdown();

  distributed::BfsRpcClients &bfs_subcursor_clients() override;
  distributed::DataRpcClients &data_clients() override;
  distributed::UpdatesRpcServer &updates_server() override;
  distributed::UpdatesRpcClients &updates_clients() override;
  distributed::DataManager &data_manager() override;

  distributed::PlanConsumer &plan_consumer();

 private:
  std::unique_ptr<impl::Worker> impl_;

  utils::Scheduler transaction_killer_;
};

}  // namespace database
