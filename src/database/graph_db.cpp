#include "database/graph_db.hpp"

#include <experimental/optional>

#include <glog/logging.h>

#include "database/graph_db_accessor.hpp"
#include "database/storage_gc_single_node.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"
#include "transactions/engine_single_node.hpp"
#include "utils/file.hpp"

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

class SingleNode {
 public:
  explicit SingleNode(const Config &config) : config_(config) {}

  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};

  tx::SingleNodeEngine tx_engine_{&wal_};
  std::unique_ptr<StorageGcSingleNode> storage_gc_ =
      std::make_unique<StorageGcSingleNode>(*storage_, tx_engine_,
                                            config_.gc_cycle_sec);
  TypemapPack<storage::SingleNodeConcurrentIdMapper> typemap_pack_{
      storage_->PropertiesOnDisk()};
  database::SingleNodeCounters counters_;
};

}  // namespace impl

SingleNode::SingleNode(Config config)
    : impl_(std::make_unique<impl::SingleNode>(config)) {
  if (impl_->config_.durability_enabled)
    utils::CheckDir(impl_->config_.durability_directory);

  // Durability recovery.
  {
    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    durability::RecoveryData recovery_data;
    // Recover only if necessary.
    if (impl_->config_.db_recover_on_startup) {
      recovery_info = durability::RecoverOnlySnapshot(
          impl_->config_.durability_directory, this, &recovery_data,
          std::experimental::nullopt);
    }

    // Post-recovery setup and checking.
    if (recovery_info) {
      recovery_data.wal_tx_to_recover = recovery_info->wal_recovered;
      durability::RecoverWalAndIndexes(impl_->config_.durability_directory,
                                       this, &recovery_data);
    }
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal_.Enable();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] {
          GraphDbAccessor dba(*this);
          this->MakeSnapshot(dba);
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

SingleNode::~SingleNode() {
  snapshot_creator_ = nullptr;

  is_accepting_transactions_ = false;
  impl_->tx_engine_.LocalForEachActiveTransaction(
      [](auto &t) { t.set_should_abort(); });

  if (impl_->config_.snapshot_on_exit) {
    GraphDbAccessor dba(*this);
    MakeSnapshot(dba);
  }
}

Storage &SingleNode::storage() { return *impl_->storage_; }

durability::WriteAheadLog &SingleNode::wal() { return impl_->wal_; }

tx::Engine &SingleNode::tx_engine() { return impl_->tx_engine_; }

storage::ConcurrentIdMapper<storage::Label> &SingleNode::label_mapper() {
  return impl_->typemap_pack_.label;
}

storage::ConcurrentIdMapper<storage::EdgeType> &SingleNode::edge_type_mapper() {
  return impl_->typemap_pack_.edge_type;
}

storage::ConcurrentIdMapper<storage::Property> &SingleNode::property_mapper() {
  return impl_->typemap_pack_.property;
}

database::Counters &SingleNode::counters() { return impl_->counters_; }

void SingleNode::CollectGarbage() { impl_->storage_gc_->CollectGarbage(); }

int SingleNode::WorkerId() const { return impl_->config_.worker_id; }

std::vector<int> SingleNode::GetWorkerIds() const { return {0}; }

bool SingleNode::MakeSnapshot(GraphDbAccessor &accessor) {
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

void SingleNode::ReinitializeStorage() {
  // Release gc scheduler to stop it from touching storage
  impl_->storage_gc_ = nullptr;
  impl_->storage_ = std::make_unique<Storage>(
      impl_->config_.worker_id, impl_->config_.properties_on_disk);
  impl_->storage_gc_ = std::make_unique<StorageGcSingleNode>(
      *impl_->storage_, impl_->tx_engine_, impl_->config_.gc_cycle_sec);
}

}  // namespace database
