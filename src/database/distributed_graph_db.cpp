#include "database/distributed_graph_db.hpp"

#include "database/storage_gc_master.hpp"
#include "database/storage_gc_worker.hpp"
#include "distributed/bfs_rpc_clients.hpp"
#include "distributed/bfs_rpc_server.hpp"
#include "distributed/bfs_subcursor.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/data_rpc_server.hpp"
#include "distributed/durability_rpc_clients.hpp"
#include "distributed/durability_rpc_server.hpp"
#include "distributed/index_rpc_server.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed/token_sharing_rpc_server.hpp"
#include "distributed/transactional_cache_cleaner.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "durability/snapshooter.hpp"
#include "storage/concurrent_id_mapper.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "transactions/engine_master.hpp"
#include "utils/file.hpp"

using namespace std::literals::chrono_literals;

namespace database {

namespace impl {

template <template <typename TId> class TMapper>
struct TypemapPack {
  template <typename... TMapperArgs>
  explicit TypemapPack(TMapperArgs &... args)
      : label(args...), edge_type(args...), property(args...) {}
  // TODO this should also be garbage collected
  TMapper<storage::Label> label;
  TMapper<storage::EdgeType> edge_type;
  TMapper<storage::Property> property;
};

// Master

class Master {
 public:
  explicit Master(const Config &config, DistributedGraphDb *self)
      : config_(config), self_(self) {}

  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};

  // TODO: Some things may depend on order of construction/destruction. We also
  // have a lot of circular pointers among members. It would be a good idea to
  // clean the mess. Also, be careful of virtual calls to `self_` in
  // constructors of members.
  DistributedGraphDb *self_{nullptr};
  communication::rpc::Server server_{
      config_.master_endpoint, static_cast<size_t>(config_.rpc_num_workers)};
  tx::MasterEngine tx_engine_{server_, rpc_worker_clients_, &wal_};
  distributed::MasterCoordination coordination_{server_.endpoint()};
  std::unique_ptr<StorageGcMaster> storage_gc_ =
      std::make_unique<StorageGcMaster>(
          *storage_, tx_engine_, config_.gc_cycle_sec, server_, coordination_);
  distributed::RpcWorkerClients rpc_worker_clients_{coordination_};
  TypemapPack<storage::MasterConcurrentIdMapper> typemap_pack_{server_};
  database::MasterCounters counters_{server_};
  distributed::BfsSubcursorStorage subcursor_storage_{self_,
                                                      &bfs_subcursor_clients_};
  distributed::BfsRpcServer bfs_subcursor_server_{self_, &server_,
                                                  &subcursor_storage_};
  distributed::BfsRpcClients bfs_subcursor_clients_{
      self_, &subcursor_storage_, &rpc_worker_clients_, &data_manager_};
  distributed::DurabilityRpcClients durability_rpc_clients_{
      rpc_worker_clients_};
  distributed::DataRpcServer data_server_{*self_, server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanDispatcher plan_dispatcher_{rpc_worker_clients_};
  distributed::PullRpcClients pull_clients_{rpc_worker_clients_};
  distributed::IndexRpcClients index_rpc_clients_{rpc_worker_clients_};
  distributed::UpdatesRpcServer updates_server_{*self_, server_};
  distributed::UpdatesRpcClients updates_clients_{rpc_worker_clients_};
  distributed::DataManager data_manager_{*self_, data_clients_};
  distributed::TransactionalCacheCleaner cache_cleaner_{
      tx_engine_, updates_server_, data_manager_};
  distributed::ClusterDiscoveryMaster cluster_discovery_{server_, coordination_,
                                                         rpc_worker_clients_};
  distributed::TokenSharingRpcClients token_sharing_clients_{
      &rpc_worker_clients_};
  distributed::TokenSharingRpcServer token_sharing_server_{
      self_, config_.worker_id, &coordination_, &server_,
      &token_sharing_clients_};
};

}  // namespace impl

Master::Master(Config config)
    : impl_(std::make_unique<impl::Master>(config, this)) {
  if (impl_->config_.durability_enabled)
    utils::CheckDir(impl_->config_.durability_directory);

  // Durability recovery.
  {
    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    // Recover only if necessary.
    if (impl_->config_.db_recover_on_startup) {
      recovery_info = durability::Recover(impl_->config_.durability_directory,
                                          *this, std::experimental::nullopt);
    }

    // Post-recovery setup and checking.
    impl_->coordination_.SetRecoveryInfo(recovery_info);
    if (recovery_info) {
      CHECK(impl_->config_.recovering_cluster_size > 0)
          << "Invalid cluster recovery size flag. Recovered cluster size "
             "should be at least 1";
      while (impl_->coordination_.CountRecoveredWorkers() !=
             impl_->config_.recovering_cluster_size - 1) {
        LOG(INFO) << "Waiting for workers to finish recovering..";
        std::this_thread::sleep_for(2s);
      }
    }
  }
  // Start the dynamic graph partitioner inside token sharing server
  if (impl_->config_.dynamic_graph_partitioner_enabled) {
    impl_->token_sharing_server_.StartTokenSharing();
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal_.Enable();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] {
          GraphDbAccessor dba(*this);
          MakeSnapshot(dba);
        });
  }

  // Start transaction killer.
  if (impl_->config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        "TX killer",
        std::chrono::seconds(std::max(
            1, std::min(5, impl_->config_.query_execution_time_sec / 4))),
        [this]() {
          impl_->tx_engine_.LocalForEachActiveTransaction(
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

Master::~Master() {
  snapshot_creator_ = nullptr;

  is_accepting_transactions_ = false;
  impl_->tx_engine_.LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });

  // We are not a worker, so we can do a snapshot on exit if it's enabled. Doing
  // this on the master forces workers to do the same through rpcs
  if (impl_->config_.snapshot_on_exit) {
    GraphDbAccessor dba(*this);
    MakeSnapshot(dba);
  }
}

Storage &Master::storage() { return *impl_->storage_; }

durability::WriteAheadLog &Master::wal() { return impl_->wal_; }

tx::Engine &Master::tx_engine() { return impl_->tx_engine_; }

storage::ConcurrentIdMapper<storage::Label> &Master::label_mapper() {
  return impl_->typemap_pack_.label;
}

storage::ConcurrentIdMapper<storage::EdgeType> &Master::edge_type_mapper() {
  return impl_->typemap_pack_.edge_type;
}

storage::ConcurrentIdMapper<storage::Property> &Master::property_mapper() {
  return impl_->typemap_pack_.property;
}

database::Counters &Master::counters() { return impl_->counters_; }

void Master::CollectGarbage() { impl_->storage_gc_->CollectGarbage(); }

int Master::WorkerId() const { return impl_->config_.worker_id; }

std::vector<int> Master::GetWorkerIds() const {
  return impl_->coordination_.GetWorkerIds();
}

// Makes a local snapshot and forces the workers to do the same. Snapshot is
// written here only if workers sucesfully created their own snapshot
bool Master::MakeSnapshot(GraphDbAccessor &accessor) {
  auto workers_snapshot =
      impl_->durability_rpc_clients_.MakeSnapshot(accessor.transaction_id());
  if (!workers_snapshot.get()) return false;
  // This can be further optimized by creating master snapshot at the same
  // time as workers snapshots but this forces us to delete the master
  // snapshot if we succeed in creating it and workers somehow fail. Because
  // we have an assumption that every snapshot that exists on master with
  // some tx_id visibility also exists on workers
  const bool status = durability::MakeSnapshot(
      *this, accessor, fs::path(impl_->config_.durability_directory),
      impl_->config_.snapshot_max_retained);
  if (status) {
    LOG(INFO) << "Snapshot created successfully.";
  } else {
    LOG(ERROR) << "Snapshot creation failed!";
  }
  return status;
}

void Master::ReinitializeStorage() {
  // Release gc scheduler to stop it from touching storage
  impl_->storage_gc_ = nullptr;
  impl_->storage_ = std::make_unique<Storage>(
      impl_->config_.worker_id, impl_->config_.properties_on_disk);
  impl_->storage_gc_ = std::make_unique<StorageGcMaster>(
      *impl_->storage_, impl_->tx_engine_, impl_->config_.gc_cycle_sec,
      impl_->server_, impl_->coordination_);
}

io::network::Endpoint Master::endpoint() const {
  return impl_->server_.endpoint();
}

io::network::Endpoint Master::GetEndpoint(int worker_id) {
  return impl_->coordination_.GetEndpoint(worker_id);
}

distributed::BfsRpcClients &Master::bfs_subcursor_clients() {
  return impl_->bfs_subcursor_clients_;
}

distributed::DataRpcClients &Master::data_clients() {
  return impl_->data_clients_;
}

distributed::UpdatesRpcServer &Master::updates_server() {
  return impl_->updates_server_;
}

distributed::UpdatesRpcClients &Master::updates_clients() {
  return impl_->updates_clients_;
}

distributed::DataManager &Master::data_manager() {
  return impl_->data_manager_;
}

distributed::PullRpcClients &Master::pull_clients() {
  return impl_->pull_clients_;
}

distributed::PlanDispatcher &Master::plan_dispatcher() {
  return impl_->plan_dispatcher_;
}

distributed::IndexRpcClients &Master::index_rpc_clients() {
  return impl_->index_rpc_clients_;
}

// Worker

namespace impl {

class Worker {
 public:
  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};

  explicit Worker(const Config &config, DistributedGraphDb *self)
      : config_(config), self_(self) {
    cluster_discovery_.RegisterWorker(config.worker_id);
  }

  // TODO: Some things may depend on order of construction/destruction. We also
  // have a lot of circular pointers among members. It would be a good idea to
  // clean the mess. Also, be careful of virtual calls to `self_` in
  // constructors of members.
  DistributedGraphDb *self_{nullptr};
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
  TypemapPack<storage::WorkerConcurrentIdMapper> typemap_pack_{
      rpc_worker_clients_.GetClientPool(0)};
  database::WorkerCounters counters_{rpc_worker_clients_.GetClientPool(0)};
  distributed::BfsSubcursorStorage subcursor_storage_{self_,
                                                      &bfs_subcursor_clients_};
  distributed::BfsRpcServer bfs_subcursor_server_{self_, &server_,
                                                  &subcursor_storage_};
  distributed::BfsRpcClients bfs_subcursor_clients_{
      self_, &subcursor_storage_, &rpc_worker_clients_, &data_manager_};
  distributed::DataRpcServer data_server_{*self_, server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanConsumer plan_consumer_{server_};
  distributed::ProduceRpcServer produce_server_{*self_, tx_engine_, server_,
                                                plan_consumer_, &data_manager_};
  distributed::IndexRpcServer index_rpc_server_{*self_, server_};
  distributed::UpdatesRpcServer updates_server_{*self_, server_};
  distributed::UpdatesRpcClients updates_clients_{rpc_worker_clients_};
  distributed::DataManager data_manager_{*self_, data_clients_};
  distributed::WorkerTransactionalCacheCleaner cache_cleaner_{
      tx_engine_,      &wal_,           server_,
      produce_server_, updates_server_, data_manager_};
  distributed::DurabilityRpcServer durability_rpc_server_{*self_, server_};
  distributed::ClusterDiscoveryWorker cluster_discovery_{
      server_, coordination_, rpc_worker_clients_.GetClientPool(0)};
  distributed::TokenSharingRpcClients token_sharing_clients_{
      &rpc_worker_clients_};
  distributed::TokenSharingRpcServer token_sharing_server_{
      self_, config_.worker_id, &coordination_, &server_,
      &token_sharing_clients_};
};

}  // namespace impl

Worker::Worker(Config config)
    : impl_(std::make_unique<impl::Worker>(config, this)) {
  if (impl_->config_.durability_enabled)
    utils::CheckDir(impl_->config_.durability_directory);

  // Durability recovery.
  {
    // What we should recover.
    std::experimental::optional<durability::RecoveryInfo>
        required_recovery_info(impl_->cluster_discovery_.recovery_info());

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    // Recover only if necessary.
    if (required_recovery_info) {
      recovery_info = durability::Recover(impl_->config_.durability_directory,
                                          *this, required_recovery_info);
    }

    // Post-recovery setup and checking.
    if (required_recovery_info != recovery_info)
      LOG(FATAL) << "Memgraph worker failed to recover the database state "
                    "recovered on the master";
    impl_->cluster_discovery_.NotifyWorkerRecovered();
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal_.Enable();
  }

  // Start transaction killer.
  if (impl_->config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        "TX killer",
        std::chrono::seconds(std::max(
            1, std::min(5, impl_->config_.query_execution_time_sec / 4))),
        [this]() {
          impl_->tx_engine_.LocalForEachActiveTransaction(
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

Worker::~Worker() {
  is_accepting_transactions_ = false;
  impl_->tx_engine_.LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });
}

Storage &Worker::storage() { return *impl_->storage_; }

durability::WriteAheadLog &Worker::wal() { return impl_->wal_; }

tx::Engine &Worker::tx_engine() { return impl_->tx_engine_; }

storage::ConcurrentIdMapper<storage::Label> &Worker::label_mapper() {
  return impl_->typemap_pack_.label;
}

storage::ConcurrentIdMapper<storage::EdgeType> &Worker::edge_type_mapper() {
  return impl_->typemap_pack_.edge_type;
}

storage::ConcurrentIdMapper<storage::Property> &Worker::property_mapper() {
  return impl_->typemap_pack_.property;
}

database::Counters &Worker::counters() { return impl_->counters_; }

void Worker::CollectGarbage() { return impl_->storage_gc_->CollectGarbage(); }

int Worker::WorkerId() const { return impl_->config_.worker_id; }

std::vector<int> Worker::GetWorkerIds() const {
  return impl_->coordination_.GetWorkerIds();
}

bool Worker::MakeSnapshot(GraphDbAccessor &accessor) {
  // Makes a local snapshot from the visibility of accessor
  const bool status = durability::MakeSnapshot(
      *this, accessor, fs::path(impl_->config_.durability_directory),
      impl_->config_.snapshot_max_retained);
  if (status) {
    LOG(INFO) << "Snapshot created successfully.";
  } else {
    LOG(ERROR) << "Snapshot creation failed!";
  }
  return status;
}

void Worker::ReinitializeStorage() {
  // Release gc scheduler to stop it from touching storage
  impl_->storage_gc_ = nullptr;
  impl_->storage_ = std::make_unique<Storage>(
      impl_->config_.worker_id, impl_->config_.properties_on_disk);
  impl_->storage_gc_ = std::make_unique<StorageGcWorker>(
      *impl_->storage_, impl_->tx_engine_, impl_->config_.gc_cycle_sec,
      impl_->rpc_worker_clients_.GetClientPool(0), impl_->config_.worker_id);
}

io::network::Endpoint Worker::endpoint() const {
  return impl_->server_.endpoint();
}

io::network::Endpoint Worker::GetEndpoint(int worker_id) {
  return impl_->coordination_.GetEndpoint(worker_id);
}

void Worker::WaitForShutdown() {
  return impl_->coordination_.WaitForShutdown();
}

distributed::BfsRpcClients &Worker::bfs_subcursor_clients() {
  return impl_->bfs_subcursor_clients_;
}

distributed::DataRpcClients &Worker::data_clients() {
  return impl_->data_clients_;
}

distributed::UpdatesRpcServer &Worker::updates_server() {
  return impl_->updates_server_;
}

distributed::UpdatesRpcClients &Worker::updates_clients() {
  return impl_->updates_clients_;
}

distributed::DataManager &Worker::data_manager() {
  return impl_->data_manager_;
}

distributed::PlanConsumer &Worker::plan_consumer() {
  return impl_->plan_consumer_;
}

}  // namespace database
