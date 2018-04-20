#include <experimental/optional>

#include "glog/logging.h"

#include "communication/rpc/server.hpp"
#include "database/graph_db.hpp"
#include "database/storage_gc_master.hpp"
#include "database/storage_gc_single_node.hpp"
#include "database/storage_gc_worker.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/data_rpc_clients.hpp"
#include "distributed/data_rpc_server.hpp"
#include "distributed/durability_rpc_clients.hpp"
#include "distributed/durability_rpc_messages.hpp"
#include "distributed/durability_rpc_server.hpp"
#include "distributed/index_rpc_server.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/produce_rpc_server.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed/transactional_cache_cleaner.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_single_node.hpp"
#include "transactions/engine_worker.hpp"
#include "utils/flag_validation.hpp"

using namespace storage;

namespace database {
namespace impl {

class PrivateBase : public GraphDb {
 public:
  explicit PrivateBase(const Config &config) : config_(config) {}
  virtual ~PrivateBase() {}

  const Config config_;

  Storage &storage() override { return *storage_; }
  durability::WriteAheadLog &wal() override { return wal_; }
  int WorkerId() const override { return config_.worker_id; }

  // Makes a local snapshot from the visibility of accessor
  bool MakeSnapshot(GraphDbAccessor &accessor) override {
    const bool status = durability::MakeSnapshot(
        *this, accessor, fs::path(config_.durability_directory),
        config_.snapshot_max_retained);
    if (status) {
      LOG(INFO) << "Snapshot created successfully." << std::endl;
    } else {
      LOG(ERROR) << "Snapshot creation failed!" << std::endl;
    }
    return status;
  }

  void ReinitializeStorage() override {
    storage_ = std::make_unique<Storage>(WorkerId());
  }

  distributed::PullRpcClients &pull_clients() override {
    LOG(FATAL) << "Remote pull clients only available in master.";
  }
  distributed::ProduceRpcServer &produce_server() override {
    LOG(FATAL) << "Remote produce server only available in worker.";
  }
  distributed::PlanConsumer &plan_consumer() override {
    LOG(FATAL) << "Plan consumer only available in distributed worker.";
  }
  distributed::PlanDispatcher &plan_dispatcher() override {
    LOG(FATAL) << "Plan dispatcher only available in distributed master.";
  }
  distributed::IndexRpcClients &index_rpc_clients() override {
    LOG(FATAL) << "Index RPC clients only available in distributed master.";
  }

 protected:
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};
};

template <template <typename TId> class TMapper>
struct TypemapPack {
  template <typename... TMapperArgs>
  explicit TypemapPack(TMapperArgs &... args)
      : label(args...), edge_type(args...), property(args...) {}
  // TODO this should also be garbage collected
  TMapper<Label> label;
  TMapper<EdgeType> edge_type;
  TMapper<Property> property;
};

#define IMPL_GETTERS                                            \
  tx::Engine &tx_engine() override { return tx_engine_; }       \
  ConcurrentIdMapper<Label> &label_mapper() override {          \
    return typemap_pack_.label;                                 \
  }                                                             \
  ConcurrentIdMapper<EdgeType> &edge_type_mapper() override {   \
    return typemap_pack_.edge_type;                             \
  }                                                             \
  ConcurrentIdMapper<Property> &property_mapper() override {    \
    return typemap_pack_.property;                              \
  }                                                             \
  database::Counters &counters() override { return counters_; } \
  void CollectGarbage() override { storage_gc_->CollectGarbage(); }

class SingleNode : public PrivateBase {
 public:
  explicit SingleNode(const Config &config) : PrivateBase(config) {}
  GraphDb::Type type() const override { return GraphDb::Type::SINGLE_NODE; }
  IMPL_GETTERS

  tx::SingleNodeEngine tx_engine_{&wal_};
  std::unique_ptr<StorageGcSingleNode> storage_gc_ =
      std::make_unique<StorageGcSingleNode>(*storage_, tx_engine_,
                                            config_.gc_cycle_sec);
  TypemapPack<SingleNodeConcurrentIdMapper> typemap_pack_;
  database::SingleNodeCounters counters_;
  std::vector<int> GetWorkerIds() const override { return {0}; }
  distributed::DataRpcServer &data_server() override {
    LOG(FATAL) << "Remote data server not available in single-node.";
  }
  distributed::DataRpcClients &data_clients() override {
    LOG(FATAL) << "Remote data clients not available in single-node.";
  }
  distributed::PlanDispatcher &plan_dispatcher() override {
    LOG(FATAL) << "Plan Dispatcher not available in single-node.";
  }
  distributed::PlanConsumer &plan_consumer() override {
    LOG(FATAL) << "Plan Consumer not available in single-node.";
  }
  distributed::UpdatesRpcServer &updates_server() override {
    LOG(FATAL) << "Remote updates server not available in single-node.";
  }
  distributed::UpdatesRpcClients &updates_clients() override {
    LOG(FATAL) << "Remote updates clients not available in single-node.";
  }
  distributed::DataManager &data_manager() override {
    LOG(FATAL) << "Remote data manager not available in single-node.";
  }
  void ReinitializeStorage() override {
    // Release gc scheduler to stop it from touching storage
    storage_gc_ = nullptr;
    PrivateBase::ReinitializeStorage();
    storage_gc_ = std::make_unique<StorageGcSingleNode>(*storage_, tx_engine_,
                                                        config_.gc_cycle_sec);
  }
};

#define IMPL_DISTRIBUTED_GETTERS                                              \
  std::vector<int> GetWorkerIds() const override {                            \
    return coordination_.GetWorkerIds();                                      \
  }                                                                           \
  distributed::DataRpcServer &data_server() override { return data_server_; } \
  distributed::DataRpcClients &data_clients() override {                      \
    return data_clients_;                                                     \
  }                                                                           \
  distributed::UpdatesRpcServer &updates_server() override {                  \
    return updates_server_;                                                   \
  }                                                                           \
  distributed::UpdatesRpcClients &updates_clients() override {                \
    return updates_clients_;                                                  \
  }                                                                           \
  distributed::DataManager &data_manager() override { return data_manager_; }

class Master : public PrivateBase {
 public:
  explicit Master(const Config &config) : PrivateBase(config) {}

  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_MASTER;
  }

  // Makes a local snapshot and forces the workers to do the same. Snapshot is
  // written here only if workers sucesfully created their own snapshot
  bool MakeSnapshot(GraphDbAccessor &accessor) override {
    auto workers_snapshot =
        durability_rpc_clients_.MakeSnapshot(accessor.transaction_id());
    if (!workers_snapshot.get()) return false;
    // This can be further optimized by creating master snapshot at the same
    // time as workers snapshots but this forces us to delete the master
    // snapshot if we succeed in creating it and workers somehow fail. Because
    // we have an assumption that every snapshot that exists on master with some
    // tx_id visibility also exists on workers
    return PrivateBase::MakeSnapshot(accessor);
  }

  IMPL_GETTERS
  IMPL_DISTRIBUTED_GETTERS
  distributed::PlanDispatcher &plan_dispatcher() override {
    return plan_dispatcher_;
  }
  distributed::PullRpcClients &pull_clients() override { return pull_clients_; }
  distributed::IndexRpcClients &index_rpc_clients() override {
    return index_rpc_clients_;
  }

  void ReinitializeStorage() override {
    // Release gc scheduler to stop it from touching storage
    storage_gc_ = nullptr;
    PrivateBase::ReinitializeStorage();
    storage_gc_ = std::make_unique<StorageGcMaster>(
        *storage_, tx_engine_, config_.gc_cycle_sec, server_, coordination_);
  }

  communication::rpc::Server server_{
      config_.master_endpoint, static_cast<size_t>(config_.rpc_num_workers)};
  tx::MasterEngine tx_engine_{server_, rpc_worker_clients_, &wal_};
  distributed::MasterCoordination coordination_{server_.endpoint()};
  std::unique_ptr<StorageGcMaster> storage_gc_ =
      std::make_unique<StorageGcMaster>(
          *storage_, tx_engine_, config_.gc_cycle_sec, server_, coordination_);
  distributed::RpcWorkerClients rpc_worker_clients_{coordination_};
  TypemapPack<MasterConcurrentIdMapper> typemap_pack_{server_};
  database::MasterCounters counters_{server_};
  distributed::DurabilityRpcClients durability_rpc_clients_{
      rpc_worker_clients_};
  distributed::DataRpcServer data_server_{*this, server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanDispatcher plan_dispatcher_{rpc_worker_clients_};
  distributed::PullRpcClients pull_clients_{rpc_worker_clients_};
  distributed::IndexRpcClients index_rpc_clients_{rpc_worker_clients_};
  distributed::UpdatesRpcServer updates_server_{*this, server_};
  distributed::UpdatesRpcClients updates_clients_{rpc_worker_clients_};
  distributed::DataManager data_manager_{*this, data_clients_};
  distributed::TransactionalCacheCleaner cache_cleaner_{
      tx_engine_, updates_server_, data_manager_};
  distributed::ClusterDiscoveryMaster cluster_discovery_{server_, coordination_,
                                                         rpc_worker_clients_};
};

class Worker : public PrivateBase {
 public:
  explicit Worker(const Config &config) : PrivateBase(config) {
    cluster_discovery_.RegisterWorker(config.worker_id);
  }

  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_WORKER;
  }
  IMPL_GETTERS
  IMPL_DISTRIBUTED_GETTERS
  distributed::PlanConsumer &plan_consumer() override { return plan_consumer_; }
  distributed::ProduceRpcServer &produce_server() override {
    return produce_server_;
  }

  void ReinitializeStorage() override {
    // Release gc scheduler to stop it from touching storage
    storage_gc_ = nullptr;
    PrivateBase::ReinitializeStorage();
    storage_gc_ = std::make_unique<StorageGcWorker>(
        *storage_, tx_engine_, config_.gc_cycle_sec,
        rpc_worker_clients_.GetClientPool(0), config_.worker_id);
  }

  communication::rpc::Server server_{
      config_.worker_endpoint, static_cast<size_t>(config_.rpc_num_workers)};
  distributed::WorkerCoordination coordination_{server_,
                                                config_.master_endpoint};
  distributed::RpcWorkerClients rpc_worker_clients_{coordination_};
  tx::WorkerEngine tx_engine_{rpc_worker_clients_.GetClientPool(0)};
  std::unique_ptr<StorageGcWorker> storage_gc_ =
      std::make_unique<StorageGcWorker>(
          *storage_, tx_engine_, config_.gc_cycle_sec,
          rpc_worker_clients_.GetClientPool(0), config_.worker_id);
  TypemapPack<WorkerConcurrentIdMapper> typemap_pack_{
      rpc_worker_clients_.GetClientPool(0)};
  database::WorkerCounters counters_{rpc_worker_clients_.GetClientPool(0)};
  distributed::DataRpcServer data_server_{*this, server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanConsumer plan_consumer_{server_};
  distributed::ProduceRpcServer produce_server_{*this, tx_engine_, server_,
                                                plan_consumer_};
  distributed::IndexRpcServer index_rpc_server_{*this, server_};
  distributed::UpdatesRpcServer updates_server_{*this, server_};
  distributed::UpdatesRpcClients updates_clients_{rpc_worker_clients_};
  distributed::DataManager data_manager_{*this, data_clients_};
  distributed::WorkerTransactionalCacheCleaner cache_cleaner_{
      tx_engine_, server_, produce_server_, updates_server_, data_manager_};
  distributed::DurabilityRpcServer durability_rpc_server_{*this, server_};
  distributed::ClusterDiscoveryWorker cluster_discovery_{
      server_, coordination_, rpc_worker_clients_.GetClientPool(0)};
};

#undef IMPL_GETTERS

PublicBase::PublicBase(std::unique_ptr<PrivateBase> impl)
    : impl_(std::move(impl)) {
  if (impl_->config_.durability_enabled)
    durability::CheckDurabilityDir(impl_->config_.durability_directory);

  // Durability recovery.
  {
    auto db_type = impl_->type();

    // What we should recover.
    std::experimental::optional<durability::RecoveryInfo>
        required_recovery_info;
    if (db_type == Type::DISTRIBUTED_WORKER) {
      required_recovery_info = dynamic_cast<impl::Worker *>(impl_.get())
                                   ->cluster_discovery_.recovery_info();
    }

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    // Recover only if necessary.
    if ((db_type != Type::DISTRIBUTED_WORKER &&
         impl_->config_.db_recover_on_startup) ||
        (db_type == Type::DISTRIBUTED_WORKER && required_recovery_info)) {
      recovery_info = durability::Recover(impl_->config_.durability_directory,
                                          *impl_, required_recovery_info);
    }

    // Post-recovery setup and checking.
    switch (db_type) {
      case Type::DISTRIBUTED_MASTER:
        dynamic_cast<impl::Master *>(impl_.get())
            ->coordination_.SetRecoveryInfo(recovery_info);
        break;
      case Type::DISTRIBUTED_WORKER:
        if (required_recovery_info != recovery_info)
          LOG(FATAL) << "Memgraph worker failed to recover the database state "
                        "recovered on the master";
        break;
      case Type::SINGLE_NODE:
        break;
    }
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal().Enable();
  }

  // Start transaction killer.
  if (impl_->config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        "TX killer",
        std::chrono::seconds(std::max(
            1, std::min(5, impl_->config_.query_execution_time_sec / 4))),
        [this]() {
          impl_->tx_engine().LocalForEachActiveTransaction(
              [this](tx::Transaction &t) {
                if (t.creation_time() +
                        std::chrono::seconds(
                            impl_->config_.query_execution_time_sec) <
                    std::chrono::steady_clock::now()) {
                  t.set_should_abort();
                };
              });
        });
  }
}

PublicBase::~PublicBase() {
  is_accepting_transactions_ = false;
  tx_engine().LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });

  // If we are not a worker we can do a snapshot on exit if it's enabled. Doing
  // this on the master forces workers to do the same through rpcs
  if (impl_->config_.snapshot_on_exit &&
      impl_->type() != Type::DISTRIBUTED_WORKER) {
    GraphDbAccessor dba(*this);
    MakeSnapshot(dba);
  }
}

GraphDb::Type PublicBase::type() const { return impl_->type(); }
Storage &PublicBase::storage() { return impl_->storage(); }
durability::WriteAheadLog &PublicBase::wal() { return impl_->wal(); }
tx::Engine &PublicBase::tx_engine() { return impl_->tx_engine(); }
ConcurrentIdMapper<Label> &PublicBase::label_mapper() {
  return impl_->label_mapper();
}
ConcurrentIdMapper<EdgeType> &PublicBase::edge_type_mapper() {
  return impl_->edge_type_mapper();
}
ConcurrentIdMapper<Property> &PublicBase::property_mapper() {
  return impl_->property_mapper();
}
database::Counters &PublicBase::counters() { return impl_->counters(); }
void PublicBase::CollectGarbage() { impl_->CollectGarbage(); }
int PublicBase::WorkerId() const { return impl_->WorkerId(); }
std::vector<int> PublicBase::GetWorkerIds() const {
  return impl_->GetWorkerIds();
}
distributed::DataRpcServer &PublicBase::data_server() {
  return impl_->data_server();
}
distributed::DataRpcClients &PublicBase::data_clients() {
  return impl_->data_clients();
}
distributed::PlanDispatcher &PublicBase::plan_dispatcher() {
  return impl_->plan_dispatcher();
}
distributed::IndexRpcClients &PublicBase::index_rpc_clients() {
  return impl_->index_rpc_clients();
}
distributed::PlanConsumer &PublicBase::plan_consumer() {
  return impl_->plan_consumer();
}
distributed::PullRpcClients &PublicBase::pull_clients() {
  return impl_->pull_clients();
}
distributed::ProduceRpcServer &PublicBase::produce_server() {
  return impl_->produce_server();
}
distributed::UpdatesRpcServer &PublicBase::updates_server() {
  return impl_->updates_server();
}
distributed::UpdatesRpcClients &PublicBase::updates_clients() {
  return impl_->updates_clients();
}
distributed::DataManager &PublicBase::data_manager() {
  return impl_->data_manager();
}

bool PublicBase::MakeSnapshot(GraphDbAccessor &accessor) {
  return impl_->MakeSnapshot(accessor);
}

void PublicBase::ReinitializeStorage() { impl_->ReinitializeStorage(); }

}  // namespace impl

MasterBase::MasterBase(std::unique_ptr<impl::PrivateBase> impl)
    : PublicBase(std::move(impl)) {
  if (impl_->config_.durability_enabled) {
    impl_->wal().Enable();
    snapshot_creator_ = std::make_unique<Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] {
          GraphDbAccessor dba(*this);
          impl_->MakeSnapshot(dba);
        });
  }
}

MasterBase::~MasterBase() { snapshot_creator_ = nullptr; }

SingleNode::SingleNode(Config config)
    : MasterBase(std::make_unique<impl::SingleNode>(config)) {}

Master::Master(Config config)
    : MasterBase(std::make_unique<impl::Master>(config)) {}

io::network::Endpoint Master::endpoint() const {
  return dynamic_cast<impl::Master *>(impl_.get())->server_.endpoint();
}

io::network::Endpoint Master::GetEndpoint(int worker_id) {
  return dynamic_cast<impl::Master *>(impl_.get())
      ->coordination_.GetEndpoint(worker_id);
}

Worker::Worker(Config config)
    : PublicBase(std::make_unique<impl::Worker>(config)) {}

io::network::Endpoint Worker::endpoint() const {
  return dynamic_cast<impl::Worker *>(impl_.get())->server_.endpoint();
}

io::network::Endpoint Worker::GetEndpoint(int worker_id) {
  return dynamic_cast<impl::Worker *>(impl_.get())
      ->coordination_.GetEndpoint(worker_id);
}

void Worker::WaitForShutdown() {
  dynamic_cast<impl::Worker *>(impl_.get())->coordination_.WaitForShutdown();
}
}  // namespace database
