#include <experimental/optional>

#include "glog/logging.h"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/storage_gc_single_node.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"
#include "transactions/engine_single_node.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"

using namespace std::literals::chrono_literals;
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
    storage_ =
        std::make_unique<Storage>(WorkerId(), config_.properties_on_disk);
  }

 protected:
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
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
  IMPL_GETTERS

  tx::SingleNodeEngine tx_engine_{&wal_};
  std::unique_ptr<StorageGcSingleNode> storage_gc_ =
      std::make_unique<StorageGcSingleNode>(*storage_, tx_engine_,
                                            config_.gc_cycle_sec);
  TypemapPack<SingleNodeConcurrentIdMapper> typemap_pack_{
      storage_->PropertiesOnDisk()};
  database::SingleNodeCounters counters_;
  std::vector<int> GetWorkerIds() const override { return {0}; }
  void ReinitializeStorage() override {
    // Release gc scheduler to stop it from touching storage
    storage_gc_ = nullptr;
    PrivateBase::ReinitializeStorage();
    storage_gc_ = std::make_unique<StorageGcSingleNode>(*storage_, tx_engine_,
                                                        config_.gc_cycle_sec);
  }
};

PublicBase::PublicBase(std::unique_ptr<PrivateBase> impl)
    : impl_(std::move(impl)) {
  if (impl_->config_.durability_enabled)
    utils::CheckDir(impl_->config_.durability_directory);

  // Durability recovery.
  {
    // What we should recover.
    std::experimental::optional<durability::RecoveryInfo>
        required_recovery_info;

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    // Recover only if necessary.
    if (impl_->config_.db_recover_on_startup) {
      recovery_info = durability::Recover(impl_->config_.durability_directory,
                                          *impl_, required_recovery_info);
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
  if (impl_->config_.snapshot_on_exit) {
    GraphDbAccessor dba(*this);
    MakeSnapshot(dba);
  }
}

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

bool PublicBase::MakeSnapshot(GraphDbAccessor &accessor) {
  return impl_->MakeSnapshot(accessor);
}

void PublicBase::ReinitializeStorage() { impl_->ReinitializeStorage(); }

}  // namespace impl

MasterBase::MasterBase(std::unique_ptr<impl::PrivateBase> impl)
    : PublicBase(std::move(impl)) {
  if (impl_->config_.durability_enabled) {
    impl_->wal().Enable();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
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

}  // namespace database
