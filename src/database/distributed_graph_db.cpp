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
#include "distributed/durability_rpc_master.hpp"
#include "distributed/durability_rpc_worker.hpp"
#include "distributed/index_rpc_server.hpp"
#include "distributed/plan_dispatcher.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed/token_sharing_rpc_server.hpp"
#include "distributed/transactional_cache_cleaner.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "durability/snapshooter.hpp"
// TODO: Why do we depend on query here?
#include "query/exceptions.hpp"
#include "storage/concurrent_id_mapper.hpp"
#include "storage/concurrent_id_mapper_master.hpp"
#include "storage/concurrent_id_mapper_worker.hpp"
#include "transactions/engine_master.hpp"
#include "utils/file.hpp"

using namespace std::literals::chrono_literals;

namespace database {

namespace {

//////////////////////////////////////////////////////////////////////
// RecordAccessors implementations
//////////////////////////////////////////////////////////////////////

// RecordAccessor implementation is shared among different RecordAccessors to
// avoid heap allocations. Therefore, we are constructing this implementation in
// each DistributedGraphDb and pass it to DistributedAccessor.
template <class TRecord>
class DistributedRecordAccessor final {
  // These should never be changed, because this implementation may be shared
  // among multiple RecordAccessors.
  int worker_id_;
  distributed::DataManager *data_manager_;
  distributed::UpdatesRpcClients *updates_clients_;

 public:
  DistributedRecordAccessor(int worker_id,
                            distributed::DataManager *data_manager,
                            distributed::UpdatesRpcClients *updates_clients)
      : worker_id_(worker_id),
        data_manager_(data_manager),
        updates_clients_(updates_clients) {
    CHECK(data_manager_ && updates_clients_);
  }

  typename RecordAccessor<TRecord>::AddressT GlobalAddress(
      const RecordAccessor<TRecord> &record_accessor) {
    return record_accessor.is_local()
               ? storage::Address<mvcc::VersionList<TRecord>>(
                     record_accessor.gid(), worker_id_)
               : record_accessor.address();
  }

  void SetOldNew(const RecordAccessor<TRecord> &record_accessor, TRecord **old,
                 TRecord **newr) {
    auto &dba = record_accessor.db_accessor();
    const auto &address = record_accessor.address();
    if (record_accessor.is_local()) {
      address.local()->find_set_old_new(dba.transaction(), old, newr);
      return;
    }
    // It's not possible that we have a global address for a graph element
    // that's local, because that is resolved in the constructor.
    // TODO in write queries it's possible the command has been advanced and
    // we need to invalidate the Cache and really get the latest stuff.
    // But only do that after the command has been advanced.
    data_manager_->FindSetOldNew(dba.transaction_id(), address.worker_id(),
                                 address.gid(), old, newr);
  }

  TRecord *FindNew(const RecordAccessor<TRecord> &record_accessor) {
    const auto &address = record_accessor.address();
    auto &dba = record_accessor.db_accessor();
    if (address.is_local()) {
      return address.local()->update(dba.transaction());
    }
    return data_manager_->FindNew<TRecord>(dba.transaction_id(),
                                           address.gid());
  }

  void ProcessDelta(const RecordAccessor<TRecord> &record_accessor,
                    const database::StateDelta &delta) {
    if (record_accessor.is_local()) {
      record_accessor.db_accessor().wal().Emplace(delta);
    } else {
      SendDelta(record_accessor, delta);
    }
  }

  void SendDelta(const RecordAccessor<TRecord> &record_accessor,
                 const database::StateDelta &delta) {
    auto result =
        updates_clients_->Update(record_accessor.address().worker_id(), delta);
    switch (result) {
      case distributed::UpdateResult::DONE:
        break;
      case distributed::UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR:
        throw query::RemoveAttachedVertexException();
      case distributed::UpdateResult::SERIALIZATION_ERROR:
        throw mvcc::SerializationError();
      case distributed::UpdateResult::UPDATE_DELETED_ERROR:
        throw RecordDeletedError();
      case distributed::UpdateResult::LOCK_TIMEOUT_ERROR:
        throw utils::LockTimeoutException("Lock timeout on remote worker");
    }
  }
};

class DistributedEdgeAccessor final : public ::RecordAccessor<Edge>::Impl {
  DistributedRecordAccessor<Edge> distributed_accessor_;

 public:
  DistributedEdgeAccessor(int worker_id, distributed::DataManager *data_manager,
                          distributed::UpdatesRpcClients *updates_clients)
      : distributed_accessor_(worker_id, data_manager, updates_clients) {}

  typename RecordAccessor<Edge>::AddressT GlobalAddress(
      const RecordAccessor<Edge> &ra) override {
    return distributed_accessor_.GlobalAddress(ra);
  }

  void SetOldNew(const RecordAccessor<Edge> &ra, Edge **old_record,
                 Edge **new_record) override {
    return distributed_accessor_.SetOldNew(ra, old_record, new_record);
  }

  Edge *FindNew(const RecordAccessor<Edge> &ra) override {
    return distributed_accessor_.FindNew(ra);
  }

  void ProcessDelta(const RecordAccessor<Edge> &ra,
                    const database::StateDelta &delta) override {
    return distributed_accessor_.ProcessDelta(ra, delta);
  }
};

class DistributedVertexAccessor final : public ::VertexAccessor::Impl {
  DistributedRecordAccessor<Vertex> distributed_accessor_;

 public:
  DistributedVertexAccessor(int worker_id,
                            distributed::DataManager *data_manager,
                            distributed::UpdatesRpcClients *updates_clients)
      : distributed_accessor_(worker_id, data_manager, updates_clients) {}

  typename RecordAccessor<Vertex>::AddressT GlobalAddress(
      const RecordAccessor<Vertex> &ra) override {
    return distributed_accessor_.GlobalAddress(ra);
  }

  void SetOldNew(const RecordAccessor<Vertex> &ra, Vertex **old_record,
                 Vertex **new_record) override {
    return distributed_accessor_.SetOldNew(ra, old_record, new_record);
  }

  Vertex *FindNew(const RecordAccessor<Vertex> &ra) override {
    return distributed_accessor_.FindNew(ra);
  }

  void ProcessDelta(const RecordAccessor<Vertex> &ra,
                    const database::StateDelta &delta) override {
    return distributed_accessor_.ProcessDelta(ra, delta);
  }

  void AddLabel(const VertexAccessor &va,
                const storage::Label &label) override {
    auto &dba = va.db_accessor();
    auto delta = StateDelta::AddLabel(dba.transaction_id(), va.gid(), label,
                                      dba.LabelName(label));
    Vertex &vertex = va.update();
    // not a duplicate label, add it
    if (!utils::Contains(vertex.labels_, label)) {
      vertex.labels_.emplace_back(label);
      if (va.is_local()) {
        dba.wal().Emplace(delta);
        dba.UpdateLabelIndices(label, va, &vertex);
      }
    }

    if (!va.is_local()) distributed_accessor_.SendDelta(va, delta);
  }

  void RemoveLabel(const VertexAccessor &va,
                   const storage::Label &label) override {
    auto &dba = va.db_accessor();
    auto delta = StateDelta::RemoveLabel(dba.transaction_id(), va.gid(), label,
                                         dba.LabelName(label));
    Vertex &vertex = va.update();
    if (utils::Contains(vertex.labels_, label)) {
      auto &labels = vertex.labels_;
      auto found = std::find(labels.begin(), labels.end(), delta.label);
      std::swap(*found, labels.back());
      labels.pop_back();
      if (va.is_local()) {
        dba.wal().Emplace(delta);
      }
    }

    if (!va.is_local()) distributed_accessor_.SendDelta(va, delta);
  }
};

//////////////////////////////////////////////////////////////////////
// GraphDbAccessor implementations
//////////////////////////////////////////////////////////////////////

class DistributedAccessor : public GraphDbAccessor {
  distributed::UpdatesRpcClients *updates_clients_{nullptr};
  distributed::DataManager *data_manager_{nullptr};
  // Shared implementations of record accessors.
  DistributedVertexAccessor *vertex_accessor_;
  DistributedEdgeAccessor *edge_accessor_;

 protected:
  DistributedAccessor(DistributedGraphDb *db, tx::TransactionId tx_id,
                      DistributedVertexAccessor *vertex_accessor,
                      DistributedEdgeAccessor *edge_accessor)
      : GraphDbAccessor(*db, tx_id),
        updates_clients_(&db->updates_clients()),
        data_manager_(&db->data_manager()),
        vertex_accessor_(vertex_accessor),
        edge_accessor_(edge_accessor) {}

  DistributedAccessor(DistributedGraphDb *db,
                      DistributedVertexAccessor *vertex_accessor,
                      DistributedEdgeAccessor *edge_accessor)
      : GraphDbAccessor(*db),
        updates_clients_(&db->updates_clients()),
        data_manager_(&db->data_manager()),
        vertex_accessor_(vertex_accessor),
        edge_accessor_(edge_accessor) {}

 public:
  ::VertexAccessor::Impl *GetVertexImpl() override { return vertex_accessor_; }

  ::RecordAccessor<Edge>::Impl *GetEdgeImpl() override {
    return edge_accessor_;
  }

  bool RemoveVertex(VertexAccessor &vertex_accessor,
                    bool check_empty = true) override {
    if (!vertex_accessor.is_local()) {
      auto address = vertex_accessor.address();
      updates_clients_->RemoveVertex(address.worker_id(), transaction_id(),
                                     address.gid(), check_empty);
      // We can't know if we are going to be able to remove vertex until
      // deferred updates on a remote worker are executed
      return true;
    }
    return GraphDbAccessor::RemoveVertex(vertex_accessor, check_empty);
  }

  void RemoveEdge(EdgeAccessor &edge, bool remove_out_edge = true,
                  bool remove_in_edge = true) override {
    if (edge.is_local()) {
      return GraphDbAccessor::RemoveEdge(edge, remove_out_edge, remove_in_edge);
    }
    auto edge_addr = edge.GlobalAddress();
    auto from_addr = db().storage().GlobalizedAddress(edge.from_addr());
    CHECK(edge_addr.worker_id() == from_addr.worker_id())
        << "Edge and it's 'from' vertex not on the same worker";
    auto to_addr = db().storage().GlobalizedAddress(edge.to_addr());
    updates_clients_->RemoveEdge(transaction_id(), edge_addr.worker_id(),
                                 edge_addr.gid(), from_addr.gid(), to_addr);
    // Another RPC is necessary only if the first did not handle vertices on
    // both sides.
    if (edge_addr.worker_id() != to_addr.worker_id()) {
      updates_clients_->RemoveInEdge(transaction_id(), to_addr.worker_id(),
                                     to_addr.gid(), edge_addr);
    }
  }

  storage::EdgeAddress InsertEdgeOnFrom(
      VertexAccessor *from, VertexAccessor *to,
      const storage::EdgeType &edge_type,
      const std::experimental::optional<gid::Gid> &requested_gid,
      const std::experimental::optional<int64_t> &cypher_id) override {
    if (from->is_local()) {
      return GraphDbAccessor::InsertEdgeOnFrom(from, to, edge_type,
                                               requested_gid, cypher_id);
    }
    auto edge_address =
        updates_clients_->CreateEdge(transaction_id(), *from, *to, edge_type);
    auto *from_updated =
        data_manager_->FindNew<Vertex>(transaction_id(), from->gid());
    // Create an Edge and insert it into the Cache so we see it locally.
    data_manager_->Emplace<Edge>(
        transaction_id(), edge_address.gid(), nullptr,
        std::make_unique<Edge>(from->address(), to->address(), edge_type));
    from_updated->out_.emplace(
        db().storage().LocalizedAddressIfPossible(to->address()), edge_address,
        edge_type);
    return edge_address;
  }

  void InsertEdgeOnTo(VertexAccessor *from, VertexAccessor *to,
                      const storage::EdgeType &edge_type,
                      const storage::EdgeAddress &edge_address) override {
    if (to->is_local()) {
      return GraphDbAccessor::InsertEdgeOnTo(from, to, edge_type, edge_address);
    }
    // The RPC call for the `to` side is already handled if `from` is not
    // local.
    if (from->is_local() ||
        from->address().worker_id() != to->address().worker_id()) {
      updates_clients_->AddInEdge(
          transaction_id(), *from,
          db().storage().GlobalizedAddress(edge_address), *to, edge_type);
    }
    auto *to_updated =
        data_manager_->FindNew<Vertex>(transaction_id(), to->gid());
    to_updated->in_.emplace(
        db().storage().LocalizedAddressIfPossible(from->address()),
        edge_address, edge_type);
  }
};

class MasterAccessor final : public DistributedAccessor {
  distributed::IndexRpcClients *index_rpc_clients_{nullptr};
  int worker_id_{0};

 public:
  MasterAccessor(Master *db, distributed::IndexRpcClients *index_rpc_clients,
                 DistributedVertexAccessor *vertex_accessor,
                 DistributedEdgeAccessor *edge_accessor)
      : DistributedAccessor(db, vertex_accessor, edge_accessor),
        index_rpc_clients_(index_rpc_clients),
        worker_id_(db->WorkerId()) {}

  MasterAccessor(Master *db, tx::TransactionId tx_id,
                 distributed::IndexRpcClients *index_rpc_clients,
                 DistributedVertexAccessor *vertex_accessor,
                 DistributedEdgeAccessor *edge_accessor)
      : DistributedAccessor(db, tx_id, vertex_accessor, edge_accessor),
        index_rpc_clients_(index_rpc_clients),
        worker_id_(db->WorkerId()) {}

  void PostCreateIndex(const LabelPropertyIndex::Key &key) override {
    std::experimental::optional<std::vector<utils::Future<bool>>>
        index_rpc_completions;

    // Notify all workers to create the index
    index_rpc_completions.emplace(index_rpc_clients_->GetCreateIndexFutures(
        key.label_, key.property_, worker_id_));

    if (index_rpc_completions) {
      // Wait first, check later - so that every thread finishes and none
      // terminates - this can probably be optimized in case we fail early so
      // that we notify other workers to stop building indexes
      for (auto &index_built : *index_rpc_completions) index_built.wait();
      for (auto &index_built : *index_rpc_completions) {
        if (!index_built.get()) {
          db().storage().label_property_index().DeleteIndex(key);
          throw IndexCreationOnWorkerException("Index exists on a worker");
        }
      }
    }
  }

  void PopulateIndexFromBuildIndex(
      const LabelPropertyIndex::Key &key) override {
    // Notify all workers to start populating an index if we are the master
    // since they don't have to wait anymore
    std::experimental::optional<std::vector<utils::Future<bool>>>
        index_rpc_completions;
    index_rpc_completions.emplace(index_rpc_clients_->GetPopulateIndexFutures(
        key.label_, key.property_, transaction_id(), worker_id_));

    // Populate our own storage
    GraphDbAccessor::PopulateIndexFromBuildIndex(key);

    // Check if all workers successfully built their indexes and after this we
    // can set the index as built
    if (index_rpc_completions) {
      // Wait first, check later - so that every thread finishes and none
      // terminates - this can probably be optimized in case we fail early so
      // that we notify other workers to stop building indexes
      for (auto &index_built : *index_rpc_completions) index_built.wait();
      for (auto &index_built : *index_rpc_completions) {
        if (!index_built.get()) {
          db().storage().label_property_index().DeleteIndex(key);
          throw IndexCreationOnWorkerException("Index exists on a worker");
        }
      }
    }
  }
};

class WorkerAccessor final : public DistributedAccessor {
 public:
  WorkerAccessor(Worker *db, DistributedVertexAccessor *vertex_accessor,
                 DistributedEdgeAccessor *edge_accessor)
      : DistributedAccessor(db, vertex_accessor, edge_accessor) {}

  WorkerAccessor(Worker *db, tx::TransactionId tx_id,
                 DistributedVertexAccessor *vertex_accessor,
                 DistributedEdgeAccessor *edge_accessor)
      : DistributedAccessor(db, tx_id, vertex_accessor, edge_accessor) {}

  void BuildIndex(storage::Label, storage::Property) override {
    // TODO: Rethink BuildIndex API or inheritance. It's rather strange that a
    // derived type blocks this functionality.
    LOG(FATAL) << "BuildIndex invoked on worker.";
  }
};

//////////////////////////////////////////////////////////////////////
// RecoveryTransactions implementations
//////////////////////////////////////////////////////////////////////

class DistributedRecoveryTransanctions
    : public durability::RecoveryTransactions {
 public:
  explicit DistributedRecoveryTransanctions(DistributedGraphDb *db) : db_(db) {}

  void Begin(const tx::TransactionId &tx_id) override {
    CHECK(accessors_.find(tx_id) == accessors_.end())
        << "Double transaction start";
    accessors_.emplace(tx_id, db_->Access());
  }

  void Abort(const tx::TransactionId &tx_id) final {
    GetAccessor(tx_id)->Abort();
    accessors_.erase(accessors_.find(tx_id));
  }

  void Commit(const tx::TransactionId &tx_id) final {
    GetAccessor(tx_id)->Commit();
    accessors_.erase(accessors_.find(tx_id));
  }

  void Apply(const database::StateDelta &delta) final {
    delta.Apply(*GetAccessor(delta.transaction_id));
  }

 protected:
  virtual GraphDbAccessor *GetAccessor(const tx::TransactionId &tx_id) {
    auto found = accessors_.find(tx_id);
    // Currently accessors are created on transaction_begin, but since workers
    // don't have a transaction begin, the accessors are not created.
    if (db_->type() == database::GraphDb::Type::DISTRIBUTED_WORKER &&
        found == accessors_.end()) {
      std::tie(found, std::ignore) = accessors_.emplace(tx_id, db_->Access());
    }

    CHECK(found != accessors_.end())
        << "Accessor does not exist for transaction: " << tx_id;
    return found->second.get();
  }

  DistributedGraphDb *db_;
  std::unordered_map<tx::TransactionId, std::unique_ptr<GraphDbAccessor>>
      accessors_;
};

class WorkerRecoveryTransactions final
    : public DistributedRecoveryTransanctions {
 public:
  explicit WorkerRecoveryTransactions(Worker *db)
      : DistributedRecoveryTransanctions(db) {}

  void Begin(const tx::TransactionId &tx_id) override {
    LOG(FATAL) << "Unexpected transaction begin on worker recovery.";
  }

 protected:
  GraphDbAccessor *GetAccessor(const tx::TransactionId &tx_id) override {
    auto found = accessors_.find(tx_id);
    // Currently accessors are created on transaction_begin, but since workers
    // don't have a transaction begin, the accessors are not created.
    if (found == accessors_.end()) {
      std::tie(found, std::ignore) = accessors_.emplace(tx_id, db_->Access());
    }
    return found->second.get();
  }
};

}  // namespace

//////////////////////////////////////////////////////////////////////
// GraphDb implementations
//////////////////////////////////////////////////////////////////////

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

//////////////////////////////////////////////////////////////////////
// Master
//////////////////////////////////////////////////////////////////////

class Master {
 public:
  explicit Master(const Config &config, database::Master *self)
      : config_(config), self_(self) {}

  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};
  // Shared implementations for all RecordAccessor in this Db.
  DistributedEdgeAccessor edge_accessor_{config_.worker_id, &data_manager_,
                                         &updates_clients_};
  DistributedVertexAccessor vertex_accessor_{config_.worker_id, &data_manager_,
                                             &updates_clients_};

  // TODO: Some things may depend on order of construction/destruction. We also
  // have a lot of circular pointers among members. It would be a good idea to
  // clean the mess. Also, be careful of virtual calls to `self_` in
  // constructors of members.
  database::Master *self_{nullptr};
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
  distributed::DurabilityRpcMaster durability_rpc_{rpc_worker_clients_};
  distributed::DataRpcServer data_server_{self_, &server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanDispatcher plan_dispatcher_{rpc_worker_clients_};
  distributed::PullRpcClients pull_clients_{&rpc_worker_clients_,
                                            &data_manager_};
  distributed::IndexRpcClients index_rpc_clients_{rpc_worker_clients_};
  distributed::UpdatesRpcServer updates_server_{self_, &server_};
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

    durability::RecoveryData recovery_data;
    // Recover only if necessary.
    if (impl_->config_.db_recover_on_startup) {
      recovery_info = durability::RecoverOnlySnapshot(
          impl_->config_.durability_directory, this, &recovery_data,
          std::experimental::nullopt, config.worker_id);
    }

    // Post-recovery setup and checking.
    impl_->coordination_.SetRecoveredSnapshot(
        recovery_info
            ? std::experimental::make_optional(recovery_info->snapshot_tx_id)
            : std::experimental::nullopt);

    // Wait till workers report back their recoverable wal txs
    if (recovery_info) {
      CHECK(impl_->config_.recovering_cluster_size > 0)
          << "Invalid cluster recovery size flag. Recovered cluster size "
             "should be at least 1";
      while (impl_->coordination_.CountRecoveredWorkers() !=
             impl_->config_.recovering_cluster_size - 1) {
        LOG(INFO) << "Waiting for workers to finish recovering..";
        std::this_thread::sleep_for(2s);
      }

      // Get the intersection of recoverable transactions from wal on
      // workers and on master
      recovery_data.wal_tx_to_recover =
          impl_->coordination_.CommonWalTransactions(*recovery_info);
      DistributedRecoveryTransanctions recovery_transactions(this);
      durability::RecoverWalAndIndexes(impl_->config_.durability_directory,
                                       this, &recovery_data,
                                       &recovery_transactions);
      auto workers_recovered_wal =
          impl_->durability_rpc_.RecoverWalAndIndexes(&recovery_data);
      workers_recovered_wal.get();
    }
  }

  // Start the dynamic graph partitioner inside token sharing server
  if (impl_->config_.dynamic_graph_partitioner_enabled) {
    impl_->token_sharing_server_.StartTokenSharing();
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal_.Init();
    snapshot_creator_ = std::make_unique<utils::Scheduler>();
    snapshot_creator_->Run(
        "Snapshot", std::chrono::seconds(impl_->config_.snapshot_cycle_sec),
        [this] {
          auto dba = this->Access();
          MakeSnapshot(*dba);
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
    auto dba = Access();
    MakeSnapshot(*dba);
  }
}

std::unique_ptr<GraphDbAccessor> Master::Access() {
  return std::make_unique<MasterAccessor>(this, &impl_->index_rpc_clients_,
                                          &impl_->vertex_accessor_,
                                          &impl_->edge_accessor_);
}

std::unique_ptr<GraphDbAccessor> Master::Access(tx::TransactionId tx_id) {
  return std::make_unique<MasterAccessor>(
      this, tx_id, &impl_->index_rpc_clients_, &impl_->vertex_accessor_,
      &impl_->edge_accessor_);
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
      impl_->durability_rpc_.MakeSnapshot(accessor.transaction_id());
  if (!workers_snapshot.get()) return false;
  // This can be further optimized by creating master snapshot at the same
  // time as workers snapshots but this forces us to delete the master
  // snapshot if we succeed in creating it and workers somehow fail. Because
  // we have an assumption that every snapshot that exists on master with
  // some tx_id visibility also exists on workers
  const bool status =
      durability::MakeSnapshot(*this, accessor, impl_->config_.worker_id,
                               fs::path(impl_->config_.durability_directory),
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

VertexAccessor InsertVertexIntoRemote(
    GraphDbAccessor *dba, int worker_id,
    const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, query::TypedValue>
        &properties) {
  // TODO: Replace this with virtual call or some other mechanism.
  auto *distributed_db =
      dynamic_cast<database::DistributedGraphDb *>(&dba->db());
  CHECK(distributed_db);
  CHECK(worker_id != distributed_db->WorkerId())
      << "Not allowed to call InsertVertexIntoRemote for local worker";
  auto *updates_clients = &distributed_db->updates_clients();
  auto *data_manager = &distributed_db->data_manager();
  CHECK(updates_clients && data_manager);
  gid::Gid gid = updates_clients->CreateVertex(worker_id, dba->transaction_id(),
                                               labels, properties);
  auto vertex = std::make_unique<Vertex>();
  vertex->labels_ = labels;
  for (auto &kv : properties) vertex->properties_.set(kv.first, kv.second);
  data_manager->Emplace<Vertex>(dba->transaction_id(), gid, nullptr, std::move(vertex));
  return VertexAccessor({gid, worker_id}, *dba);
}

//////////////////////////////////////////////////////////////////////
// Worker
//////////////////////////////////////////////////////////////////////

namespace impl {

class Worker {
 public:
  Config config_;
  std::unique_ptr<Storage> storage_ =
      std::make_unique<Storage>(config_.worker_id, config_.properties_on_disk);
  durability::WriteAheadLog wal_{config_.worker_id,
                                 config_.durability_directory,
                                 config_.durability_enabled};
  // Shared implementations for all RecordAccessor in this Db.
  DistributedEdgeAccessor edge_accessor_{config_.worker_id, &data_manager_,
                                         &updates_clients_};
  DistributedVertexAccessor vertex_accessor_{config_.worker_id, &data_manager_,
                                             &updates_clients_};

  explicit Worker(const Config &config, database::Worker *self)
      : config_(config), self_(self) {
    cluster_discovery_.RegisterWorker(config.worker_id);
  }

  // TODO: Some things may depend on order of construction/destruction. We also
  // have a lot of circular pointers among members. It would be a good idea to
  // clean the mess. Also, be careful of virtual calls to `self_` in
  // constructors of members.
  database::Worker *self_{nullptr};
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
  distributed::DataRpcServer data_server_{self_, &server_};
  distributed::DataRpcClients data_clients_{rpc_worker_clients_};
  distributed::PlanConsumer plan_consumer_{server_};
  distributed::ProduceRpcServer produce_server_{self_, &tx_engine_, server_,
                                                plan_consumer_, &data_manager_};
  distributed::IndexRpcServer index_rpc_server_{*self_, server_};
  distributed::UpdatesRpcServer updates_server_{self_, &server_};
  distributed::UpdatesRpcClients updates_clients_{rpc_worker_clients_};
  distributed::DataManager data_manager_{*self_, data_clients_};
  distributed::WorkerTransactionalCacheCleaner cache_cleaner_{
      tx_engine_,      &wal_,           server_,
      produce_server_, updates_server_, data_manager_};
  distributed::DurabilityRpcWorker durability_rpc_{self_, &server_};
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
    auto snapshot_to_recover = impl_->cluster_discovery_.snapshot_to_recover();

    // What we recover.
    std::experimental::optional<durability::RecoveryInfo> recovery_info;

    durability::RecoveryData recovery_data;
    // Recover only if necessary.
    if (snapshot_to_recover) {
      recovery_info = durability::RecoverOnlySnapshot(
          impl_->config_.durability_directory, this, &recovery_data,
          snapshot_to_recover, config.worker_id);
    }

    // Post-recovery setup and checking.
    if (snapshot_to_recover &&
        (!recovery_info ||
         snapshot_to_recover != recovery_info->snapshot_tx_id))
      LOG(FATAL) << "Memgraph worker failed to recover the database state "
                    "recovered on the master";
    impl_->cluster_discovery_.NotifyWorkerRecovered(recovery_info);
  }

  if (impl_->config_.durability_enabled) {
    impl_->wal_.Init();
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

std::unique_ptr<GraphDbAccessor> Worker::Access() {
  return std::make_unique<WorkerAccessor>(this, &impl_->vertex_accessor_,
                                          &impl_->edge_accessor_);
}

std::unique_ptr<GraphDbAccessor> Worker::Access(tx::TransactionId tx_id) {
  return std::make_unique<WorkerAccessor>(this, tx_id, &impl_->vertex_accessor_,
                                          &impl_->edge_accessor_);
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
  const bool status =
      durability::MakeSnapshot(*this, accessor, impl_->config_.worker_id,
                               fs::path(impl_->config_.durability_directory),
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

void Worker::RecoverWalAndIndexes(durability::RecoveryData *recovery_data) {
  WorkerRecoveryTransactions recovery_transactions(this);
  durability::RecoverWalAndIndexes(impl_->config_.durability_directory, this,
                                   recovery_data, &recovery_transactions);
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
