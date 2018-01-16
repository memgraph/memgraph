#include "database/graph_db.hpp"

#include "communication/messaging/distributed.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
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

class Base {
 public:
  explicit Base(const Config &config) : config_(config) {}
  virtual ~Base() {}

  const Config config_;

  virtual Storage &storage() = 0;
  virtual StorageGc &storage_gc() = 0;
  virtual durability::WriteAheadLog &wal() = 0;
  virtual tx::Engine &tx_engine() = 0;
  virtual ConcurrentIdMapper<Label> &label_mapper() = 0;
  virtual ConcurrentIdMapper<EdgeType> &edge_type_mapper() = 0;
  virtual ConcurrentIdMapper<Property> &property_mapper() = 0;
  virtual database::Counters &counters() = 0;

  Base(const Base &) = delete;
  Base(Base &&) = delete;
  Base &operator=(const Base &) = delete;
  Base &operator=(Base &&) = delete;
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

#define IMPL_GETTERS                                          \
  Storage &storage() override { return storage_; }            \
  StorageGc &storage_gc() override { return storage_gc_; }    \
  durability::WriteAheadLog &wal() override { return wal_; }  \
  tx::Engine &tx_engine() override { return tx_engine_; }     \
  ConcurrentIdMapper<Label> &label_mapper() override {        \
    return typemap_pack_.label;                               \
  }                                                           \
  ConcurrentIdMapper<EdgeType> &edge_type_mapper() override { \
    return typemap_pack_.edge_type;                           \
  }                                                           \
  ConcurrentIdMapper<Property> &property_mapper() override {  \
    return typemap_pack_.property;                            \
  }                                                           \
  database::Counters &counters() override { return counters_; }

class SingleNode : public Base {
 public:
  explicit SingleNode(const Config &config) : Base(config) {}
  IMPL_GETTERS

 private:
  Storage storage_{0};
  durability::WriteAheadLog wal_{config_.durability_directory,
                                 config_.durability_enabled};
  tx::SingleNodeEngine tx_engine_{&wal_};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  TypemapPack<SingleNodeConcurrentIdMapper> typemap_pack_;
  database::SingleNodeCounters counters_;
};

class Master : public Base {
 public:
  explicit Master(const Config &config) : Base(config) {}
  IMPL_GETTERS

 private:
  communication::messaging::System system_{config_.master_endpoint};
  Storage storage_{0};
  durability::WriteAheadLog wal_{config_.durability_directory,
                                 config_.durability_enabled};
  tx::MasterEngine tx_engine_{system_, &wal_};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  distributed::MasterCoordination coordination{system_};
  TypemapPack<MasterConcurrentIdMapper> typemap_pack_{system_};
  database::MasterCounters counters_{system_};
};

class Worker : public Base {
 public:
  explicit Worker(const Config &config) : Base(config) {}
  IMPL_GETTERS
  void WaitForShutdown() { coordination_.WaitForShutdown(); }

 private:
  communication::messaging::System system_{config_.worker_endpoint};
  distributed::WorkerCoordination coordination_{system_,
                                                config_.master_endpoint};
  tx::WorkerEngine tx_engine_{system_, config_.master_endpoint};
  Storage storage_{config_.worker_id};
  StorageGc storage_gc_{storage_, tx_engine_, config_.gc_cycle_sec};
  durability::WriteAheadLog wal_{config_.durability_directory,
                                 config_.durability_enabled};
  TypemapPack<WorkerConcurrentIdMapper> typemap_pack_{system_,
                                                      config_.master_endpoint};
  database::WorkerCounters counters_{system_, config_.master_endpoint};
};

#undef IMPL_GETTERS

}  // namespace impl

GraphDb::GraphDb(std::unique_ptr<impl::Base> impl) : impl_(std::move(impl)) {
  if (impl_->config_.durability_enabled)
    durability::CheckDurabilityDir(impl_->config_.durability_directory);

  if (impl_->config_.db_recover_on_startup)
    durability::Recover(impl_->config_.durability_directory, *this);
  if (impl_->config_.durability_enabled) {
    wal().Enable();
    snapshot_creator_ = std::make_unique<Scheduler>();
    snapshot_creator_->Run(
        std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] { MakeSnapshot(); });
  }
}

GraphDb::~GraphDb() {
  snapshot_creator_.release();
  if (impl_->config_.snapshot_on_exit) MakeSnapshot();
}

Storage &GraphDb::storage() { return impl_->storage(); }

durability::WriteAheadLog &GraphDb::wal() { return impl_->wal(); }

tx::Engine &GraphDb::tx_engine() { return impl_->tx_engine(); }

ConcurrentIdMapper<Label> &GraphDb::label_mapper() {
  return impl_->label_mapper();
}

ConcurrentIdMapper<EdgeType> &GraphDb::edge_type_mapper() {
  return impl_->edge_type_mapper();
}

ConcurrentIdMapper<Property> &GraphDb::property_mapper() {
  return impl_->property_mapper();
}

database::Counters &GraphDb::counters() { return impl_->counters(); }

void GraphDb::CollectGarbage() { impl_->storage_gc().CollectGarbage(); }

void GraphDb::MakeSnapshot() {
  const bool status = durability::MakeSnapshot(
      *this, fs::path(impl_->config_.durability_directory),
      impl_->config_.snapshot_max_retained);
  if (status) {
    LOG(INFO) << "Snapshot created successfully." << std::endl;
  } else {
    LOG(ERROR) << "Snapshot creation failed!" << std::endl;
  }
}

MasterBase::MasterBase(std::unique_ptr<impl::Base> impl)
    : GraphDb(std::move(impl)) {
  if (impl_->config_.query_execution_time_sec != -1) {
    transaction_killer_.Run(
        std::chrono::seconds(std::max(
            1, std::min(5, impl_->config_.query_execution_time_sec / 4))),
        [this]() {
          tx_engine().LocalForEachActiveTransaction([this](tx::Transaction &t) {
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

SingleNode::SingleNode(Config config)
    : MasterBase(std::make_unique<impl::SingleNode>(config)) {}

Master::Master(Config config)
    : MasterBase(std::make_unique<impl::Master>(config)) {}

Worker::Worker(Config config)
    : GraphDb(std::make_unique<impl::Worker>(config)) {}

void Worker::WaitForShutdown() {
  dynamic_cast<impl::Worker *>(impl_.get())->WaitForShutdown();
}
}  // namespace database
