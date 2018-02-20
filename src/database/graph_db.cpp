#include "glog/logging.h"

#include "communication/rpc/server.hpp"
#include "database/graph_db.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/index_rpc_server.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/remote_data_manager.hpp"
#include "distributed/remote_data_rpc_clients.hpp"
#include "distributed/remote_data_rpc_server.hpp"
#include "distributed/remote_produce_rpc_server.hpp"
#include "distributed/remote_pull_rpc_clients.hpp"
#include "distributed/remote_updates_rpc_clients.hpp"
#include "distributed/remote_updates_rpc_server.hpp"
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

  Storage &storage() override { return storage_; }
  durability::WriteAheadLog &wal() override { return wal_; }
  int WorkerId() const override { return config_.worker_id; }

  distributed::RemotePullRpcClients &remote_pull_clients() override {
    LOG(FATAL) << "Remote pull clients only available in master.";
  }
  distributed::RemoteProduceRpcServer &remote_produce_server() override {
    LOG(FATAL) << "Remote produce server only available in worker.";
  }
  distributed::PlanConsumer &plan_consumer() override {
    LOG(FATAL) << "Plan consumer only available in distributed worker.";
  }
  distributed::PlanDispatcher &plan_dispatcher() override {
    LOG(FATAL) << "Plan dispatcher only available in distributed master.";
  }
  distributed::RpcWorkerClients &index_rpc_clients() override {
    LOG(FATAL) << "Index RPC clients only available in distributed master.";
  }

 protected:
  Storage storage_{config_.worker_id};
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
  void CollectGarbage() override { storage_gc_.CollectGarbage(); }

class SingleNode : public PrivateBase {
 public:
  explicit SingleNode(const Config &config) : PrivateBase(config) {}
  GraphDb::Type type() const override { return GraphDb::Type::SINGLE_NODE; }
  IMPL_GETTERS

  tx::SingleNodeEngine tx_engine_{&wal_};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  TypemapPack<SingleNodeConcurrentIdMapper> typemap_pack_;
  database::SingleNodeCounters counters_;
  std::vector<int> GetWorkerIds() const override { return {0}; }
  distributed::RemoteDataRpcServer &remote_data_server() override {
    LOG(FATAL) << "Remote data server not available in single-node.";
  }
  distributed::RemoteDataRpcClients &remote_data_clients() override {
    LOG(FATAL) << "Remote data clients not available in single-node.";
  }
  distributed::PlanDispatcher &plan_dispatcher() override {
    LOG(FATAL) << "Plan Dispatcher not available in single-node.";
  }
  distributed::PlanConsumer &plan_consumer() override {
    LOG(FATAL) << "Plan Consumer not available in single-node.";
  }
  distributed::RemoteUpdatesRpcServer &remote_updates_server() override {
    LOG(FATAL) << "Remote updates server not available in single-node.";
  }
  distributed::RemoteUpdatesRpcClients &remote_updates_clients() override {
    LOG(FATAL) << "Remote updates clients not available in single-node.";
  }
  distributed::RemoteDataManager &remote_data_manager() override {
    LOG(FATAL) << "Remote data manager not available in single-node.";
  }
};

#define IMPL_DISTRIBUTED_GETTERS                                            \
  std::vector<int> GetWorkerIds() const override {                          \
    return coordination_.GetWorkerIds();                                    \
  }                                                                         \
  distributed::RemoteDataRpcServer &remote_data_server() override {         \
    return remote_data_server_;                                             \
  }                                                                         \
  distributed::RemoteDataRpcClients &remote_data_clients() override {       \
    return remote_data_clients_;                                            \
  }                                                                         \
  distributed::RemoteUpdatesRpcServer &remote_updates_server() override {   \
    return remote_updates_server_;                                          \
  }                                                                         \
  distributed::RemoteUpdatesRpcClients &remote_updates_clients() override { \
    return remote_updates_clients_;                                         \
  }                                                                         \
  distributed::RemoteDataManager &remote_data_manager() override {          \
    return remote_data_manager_;                                            \
  }

class Master : public PrivateBase {
 public:
  explicit Master(const Config &config) : PrivateBase(config) {}
  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_MASTER;
  }
  IMPL_GETTERS
  IMPL_DISTRIBUTED_GETTERS
  distributed::PlanDispatcher &plan_dispatcher() override {
    return plan_dispatcher_;
  }
  distributed::RemotePullRpcClients &remote_pull_clients() override {
    return remote_pull_clients_;
  }
  distributed::RpcWorkerClients &index_rpc_clients() override {
    return index_rpc_clients_;
  }

  communication::rpc::System system_{config_.master_endpoint};
  tx::MasterEngine tx_engine_{system_, &wal_};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  distributed::MasterCoordination coordination_{system_};
  TypemapPack<MasterConcurrentIdMapper> typemap_pack_{system_};
  database::MasterCounters counters_{system_};
  distributed::RemoteDataRpcServer remote_data_server_{*this, system_};
  distributed::RemoteDataRpcClients remote_data_clients_{coordination_};
  distributed::PlanDispatcher plan_dispatcher_{coordination_};
  distributed::RemotePullRpcClients remote_pull_clients_{coordination_};
  distributed::RpcWorkerClients index_rpc_clients_{coordination_,
                                                   distributed::kIndexRpcName};
  distributed::RemoteUpdatesRpcServer remote_updates_server_{*this, tx_engine_,
                                                             system_};
  distributed::RemoteUpdatesRpcClients remote_updates_clients_{coordination_};
  distributed::RemoteDataManager remote_data_manager_{tx_engine_,
                                                      remote_data_clients_};
};

class Worker : public PrivateBase {
 public:
  explicit Worker(const Config &config) : PrivateBase(config) {
    coordination_.RegisterWorker(config.worker_id);
  }

  GraphDb::Type type() const override {
    return GraphDb::Type::DISTRIBUTED_WORKER;
  }
  IMPL_GETTERS
  IMPL_DISTRIBUTED_GETTERS
  distributed::PlanConsumer &plan_consumer() override { return plan_consumer_; }
  distributed::RemoteProduceRpcServer &remote_produce_server() override {
    return remote_produce_server_;
  }

  communication::rpc::System system_{config_.worker_endpoint};
  distributed::WorkerCoordination coordination_{system_,
                                                config_.master_endpoint};
  tx::WorkerEngine tx_engine_{config_.master_endpoint};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  TypemapPack<WorkerConcurrentIdMapper> typemap_pack_{config_.master_endpoint};
  database::WorkerCounters counters_{config_.master_endpoint};
  distributed::RemoteDataRpcServer remote_data_server_{*this, system_};
  distributed::RemoteDataRpcClients remote_data_clients_{coordination_};
  distributed::PlanConsumer plan_consumer_{system_};
  distributed::RemoteProduceRpcServer remote_produce_server_{
      *this, tx_engine_, system_, plan_consumer_};
  distributed::IndexRpcServer index_rpc_server_{*this, system_};
  distributed::RemoteUpdatesRpcServer remote_updates_server_{*this, tx_engine_,
                                                             system_};
  distributed::RemoteUpdatesRpcClients remote_updates_clients_{coordination_};
  distributed::RemoteDataManager remote_data_manager_{tx_engine_,
                                                      remote_data_clients_};
};

#undef IMPL_GETTERS

PublicBase::PublicBase(std::unique_ptr<PrivateBase> impl)
    : impl_(std::move(impl)) {
  if (impl_->config_.durability_enabled)
    durability::CheckDurabilityDir(impl_->config_.durability_directory);

  if (impl_->config_.db_recover_on_startup)
    durability::Recover(impl_->config_.durability_directory, *impl_);
  if (impl_->config_.durability_enabled) {
    impl_->wal().Enable();
    snapshot_creator_ = std::make_unique<Scheduler>();
    snapshot_creator_->Run(
        std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] { MakeSnapshot(); });
  }
}

PublicBase::~PublicBase() {
  snapshot_creator_.release();
  if (impl_->config_.snapshot_on_exit) MakeSnapshot();
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
distributed::RemoteDataRpcServer &PublicBase::remote_data_server() {
  return impl_->remote_data_server();
}
distributed::RemoteDataRpcClients &PublicBase::remote_data_clients() {
  return impl_->remote_data_clients();
}
distributed::PlanDispatcher &PublicBase::plan_dispatcher() {
  return impl_->plan_dispatcher();
}
distributed::RpcWorkerClients &PublicBase::index_rpc_clients() {
  return impl_->index_rpc_clients();
}
distributed::PlanConsumer &PublicBase::plan_consumer() {
  return impl_->plan_consumer();
}
distributed::RemotePullRpcClients &PublicBase::remote_pull_clients() {
  return impl_->remote_pull_clients();
}
distributed::RemoteProduceRpcServer &PublicBase::remote_produce_server() {
  return impl_->remote_produce_server();
}
distributed::RemoteUpdatesRpcServer &PublicBase::remote_updates_server() {
  return impl_->remote_updates_server();
}
distributed::RemoteUpdatesRpcClients &PublicBase::remote_updates_clients() {
  return impl_->remote_updates_clients();
}
distributed::RemoteDataManager &PublicBase::remote_data_manager() {
  return impl_->remote_data_manager();
}

void PublicBase::MakeSnapshot() {
  const bool status = durability::MakeSnapshot(
      *impl_, fs::path(impl_->config_.durability_directory),
      impl_->config_.snapshot_max_retained);
  if (status) {
    LOG(INFO) << "Snapshot created successfully." << std::endl;
  } else {
    LOG(ERROR) << "Snapshot creation failed!" << std::endl;
  }
}
}  // namespace impl

MasterBase::MasterBase(std::unique_ptr<impl::PrivateBase> impl)
    : PublicBase(std::move(impl)) {
  if (impl_->config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
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

MasterBase::~MasterBase() {
  is_accepting_transactions_ = false;
  tx_engine().LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });
}

SingleNode::SingleNode(Config config)
    : MasterBase(std::make_unique<impl::SingleNode>(config)) {}

Master::Master(Config config)
    : MasterBase(std::make_unique<impl::Master>(config)) {}

io::network::Endpoint Master::endpoint() const {
  return dynamic_cast<impl::Master *>(impl_.get())->system_.endpoint();
}

io::network::Endpoint Master::GetEndpoint(int worker_id) {
  return dynamic_cast<impl::Master *>(impl_.get())
      ->coordination_.GetEndpoint(worker_id);
}

Worker::Worker(Config config)
    : PublicBase(std::make_unique<impl::Worker>(config)) {}

io::network::Endpoint Worker::endpoint() const {
  return dynamic_cast<impl::Worker *>(impl_.get())->system_.endpoint();
}

io::network::Endpoint Worker::GetEndpoint(int worker_id) {
  return dynamic_cast<impl::Worker *>(impl_.get())
      ->coordination_.GetEndpoint(worker_id);
}

void Worker::WaitForShutdown() {
  dynamic_cast<impl::Worker *>(impl_.get())->coordination_.WaitForShutdown();
}
}  // namespace database
