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

namespace {

//////////////////////////////////////////////////////////////////////
// RecordAccessor and GraphDbAccessor implementations
//////////////////////////////////////////////////////////////////////

template <class TRecord>
class SingleNodeRecordAccessor final {
 public:
  typename RecordAccessor<TRecord>::AddressT GlobalAddress(
      const RecordAccessor<TRecord> &record_accessor) {
    // TODO: This is still coupled to distributed storage, albeit loosely.
    int worker_id = 0;
    CHECK(record_accessor.is_local());
    return storage::Address<mvcc::VersionList<TRecord>>(record_accessor.gid(),
                                                        worker_id);
  }

  void SetOldNew(const RecordAccessor<TRecord> &record_accessor, TRecord **old,
                 TRecord **newr) {
    auto &dba = record_accessor.db_accessor();
    const auto &address = record_accessor.address();
    CHECK(record_accessor.is_local());
    address.local()->find_set_old_new(dba.transaction(), *old, *newr);
  }

  TRecord *FindNew(const RecordAccessor<TRecord> &record_accessor) {
    const auto &address = record_accessor.address();
    auto &dba = record_accessor.db_accessor();
    CHECK(address.is_local());
    return address.local()->update(dba.transaction());
  }

  void ProcessDelta(const RecordAccessor<TRecord> &record_accessor,
                    const database::StateDelta &delta) {
    CHECK(record_accessor.is_local());
    record_accessor.db_accessor().wal().Emplace(delta);
  }
};

class VertexAccessorImpl final : public ::VertexAccessor::Impl {
  SingleNodeRecordAccessor<Vertex> accessor_;

 public:
  typename RecordAccessor<Vertex>::AddressT GlobalAddress(
      const RecordAccessor<Vertex> &ra) override {
    return accessor_.GlobalAddress(ra);
  }

  void SetOldNew(const RecordAccessor<Vertex> &ra, Vertex **old_record,
                 Vertex **new_record) override {
    return accessor_.SetOldNew(ra, old_record, new_record);
  }

  Vertex *FindNew(const RecordAccessor<Vertex> &ra) override {
    return accessor_.FindNew(ra);
  }

  void ProcessDelta(const RecordAccessor<Vertex> &ra,
                    const database::StateDelta &delta) override {
    return accessor_.ProcessDelta(ra, delta);
  }

  void AddLabel(const VertexAccessor &va,
                const storage::Label &label) override {
    CHECK(va.is_local());
    auto &dba = va.db_accessor();
    auto delta = StateDelta::AddLabel(dba.transaction_id(), va.gid(), label,
                                      dba.LabelName(label));
    Vertex &vertex = va.update();
    // not a duplicate label, add it
    if (!utils::Contains(vertex.labels_, label)) {
      vertex.labels_.emplace_back(label);
      dba.wal().Emplace(delta);
      dba.UpdateLabelIndices(label, va, &vertex);
    }
  }

  void RemoveLabel(const VertexAccessor &va,
                   const storage::Label &label) override {
    CHECK(va.is_local());
    auto &dba = va.db_accessor();
    auto delta = StateDelta::RemoveLabel(dba.transaction_id(), va.gid(), label,
                                         dba.LabelName(label));
    Vertex &vertex = va.update();
    if (utils::Contains(vertex.labels_, label)) {
      auto &labels = vertex.labels_;
      auto found = std::find(labels.begin(), labels.end(), delta.label);
      std::swap(*found, labels.back());
      labels.pop_back();
      dba.wal().Emplace(delta);
    }
  }
};

class EdgeAccessorImpl final : public ::RecordAccessor<Edge>::Impl {
  SingleNodeRecordAccessor<Edge> accessor_;

 public:
  typename RecordAccessor<Edge>::AddressT GlobalAddress(
      const RecordAccessor<Edge> &ra) override {
    return accessor_.GlobalAddress(ra);
  }

  void SetOldNew(const RecordAccessor<Edge> &ra, Edge **old_record,
                 Edge **new_record) override {
    return accessor_.SetOldNew(ra, old_record, new_record);
  }

  Edge *FindNew(const RecordAccessor<Edge> &ra) override {
    return accessor_.FindNew(ra);
  }

  void ProcessDelta(const RecordAccessor<Edge> &ra,
                    const database::StateDelta &delta) override {
    return accessor_.ProcessDelta(ra, delta);
  }
};

class SingleNodeAccessor : public GraphDbAccessor {
  // Shared implementations of record accessors.
  static VertexAccessorImpl vertex_accessor_;
  static EdgeAccessorImpl edge_accessor_;

 public:
  explicit SingleNodeAccessor(GraphDb &db) : GraphDbAccessor(db) {}
  SingleNodeAccessor(GraphDb &db, tx::TransactionId tx_id)
      : GraphDbAccessor(db, tx_id) {}

  ::VertexAccessor::Impl *GetVertexImpl() override { return &vertex_accessor_; }

  ::RecordAccessor<Edge>::Impl *GetEdgeImpl() override {
    return &edge_accessor_;
  }
};

VertexAccessorImpl SingleNodeAccessor::vertex_accessor_;
EdgeAccessorImpl SingleNodeAccessor::edge_accessor_;

}  // namespace

//////////////////////////////////////////////////////////////////////
// SingleNode GraphDb implementation
//////////////////////////////////////////////////////////////////////

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

namespace impl {

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
  CHECK(config.worker_id == 0)
      << "Worker ID should only be set in distributed GraphDb";
  if (impl_->config_.durability_enabled)
    utils::CheckDir(impl_->config_.durability_directory);

  // Durability recovery.
  if (impl_->config_.db_recover_on_startup) {
    CHECK(durability::VersionConsistency(impl_->config_.durability_directory))
        << "Contents of durability directory are not compatible with the "
           "current version of Memgraph binary!";

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;
    durability::RecoveryData recovery_data;

    recovery_info = durability::RecoverOnlySnapshot(
        impl_->config_.durability_directory, this, &recovery_data,
        std::experimental::nullopt, 0);

    // Post-recovery setup and checking.
    if (recovery_info) {
      recovery_data.wal_tx_to_recover = recovery_info->wal_recovered;
      SingleNodeRecoveryTransanctions recovery_transactions(this);
      durability::RecoverWalAndIndexes(impl_->config_.durability_directory,
                                       this, &recovery_data,
                                       &recovery_transactions);
    }
  }

  if (impl_->config_.durability_enabled) {
    // move any existing snapshots or wal files to a deprecated folder.
    if (!impl_->config_.db_recover_on_startup &&
        durability::ContainsDurabilityFiles(
            impl_->config_.durability_directory)) {
      durability::MoveToBackup(impl_->config_.durability_directory);
      LOG(WARNING) << "Since Memgraph was not supposed to recover on startup "
                      "and durability is enabled, your current durability "
                      "files will likely be overriden. To prevent important "
                      "data loss, Memgraph has stored those files into a "
                      ".backup directory inside durability directory";
    }
    impl_->wal_.Enable();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] {
          auto dba = this->Access();
          this->MakeSnapshot(*dba);
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
    auto dba = this->Access();
    MakeSnapshot(*dba);
  }
}

std::unique_ptr<GraphDbAccessor> SingleNode::Access() {
  // NOTE: We are doing a heap allocation to allow polymorphism. If this poses
  // performance issues, we may want to have a stack allocated GraphDbAccessor
  // which is constructed with a pointer to some global implementation struct
  // which contains only pure functions (without any state).
  return std::make_unique<SingleNodeAccessor>(*this);
}

std::unique_ptr<GraphDbAccessor> SingleNode::Access(tx::TransactionId tx_id) {
  return std::make_unique<SingleNodeAccessor>(*this, tx_id);
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

bool SingleNode::MakeSnapshot(GraphDbAccessor &accessor) {
  const bool status = durability::MakeSnapshot(
      *this, accessor, 0, fs::path(impl_->config_.durability_directory),
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

SingleNodeRecoveryTransanctions::SingleNodeRecoveryTransanctions(SingleNode *db)
    : db_(db) {}

SingleNodeRecoveryTransanctions::~SingleNodeRecoveryTransanctions() {}

void SingleNodeRecoveryTransanctions::Begin(const tx::TransactionId &tx_id) {
  CHECK(accessors_.find(tx_id) == accessors_.end())
      << "Double transaction start";
  accessors_.emplace(tx_id, db_->Access());
}

GraphDbAccessor *GetAccessor(
    const std::unordered_map<tx::TransactionId,
                             std::unique_ptr<GraphDbAccessor>> &accessors,
    const tx::TransactionId &tx_id) {
  auto found = accessors.find(tx_id);
  CHECK(found != accessors.end()) << "Accessor does not exist for transaction: "
                                  << tx_id;
  return found->second.get();
}

void SingleNodeRecoveryTransanctions::Abort(const tx::TransactionId &tx_id) {
  GetAccessor(accessors_, tx_id)->Abort();
  accessors_.erase(accessors_.find(tx_id));
}

void SingleNodeRecoveryTransanctions::Commit(const tx::TransactionId &tx_id) {
  GetAccessor(accessors_, tx_id)->Commit();
  accessors_.erase(accessors_.find(tx_id));
}

void SingleNodeRecoveryTransanctions::Apply(const database::StateDelta &delta) {
  delta.Apply(*GetAccessor(accessors_, delta.transaction_id));
}

}  // namespace database
