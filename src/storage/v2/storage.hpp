#pragma once

#include <atomic>
#include <filesystem>
#include <optional>
#include <shared_mutex>

#include "storage/v2/commit_log.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/file_locker.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

#ifdef MG_ENTERPRISE
#include "rpc/server.hpp"
#include "storage/v2/replication/replication.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/replication/serialization.hpp"
#endif

namespace storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

/// Iterable for iterating through all vertices of a Storage.
///
/// An instance of this will be usually be wrapped inside VerticesIterable for
/// generic, public use.
class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipList<Vertex>::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it);

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const {
      return self_ == other.self_ && it_ == other.it_;
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor,
                      Transaction *transaction, View view, Indices *indices,
                      Constraints *constraints, Config::Items config)
      : vertices_accessor_(std::move(vertices_accessor)),
        transaction_(transaction),
        view_(view),
        indices_(indices),
        constraints_(constraints),
        config_(config) {}

  Iterator begin() { return Iterator(this, vertices_accessor_.begin()); }
  Iterator end() { return Iterator(this, vertices_accessor_.end()); }
};

/// Generic access to different kinds of vertex iterations.
///
/// This class should be the primary type used by the client code to iterate
/// over vertices inside a Storage instance.
class VerticesIterable final {
  enum class Type { ALL, BY_LABEL, BY_LABEL_PROPERTY };

  Type type_;
  union {
    AllVerticesIterable all_vertices_;
    LabelIndex::Iterable vertices_by_label_;
    LabelPropertyIndex::Iterable vertices_by_label_property_;
  };

 public:
  explicit VerticesIterable(AllVerticesIterable);
  explicit VerticesIterable(LabelIndex::Iterable);
  explicit VerticesIterable(LabelPropertyIndex::Iterable);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesIterable::Iterator all_it_;
      LabelIndex::Iterable::Iterator by_label_it_;
      LabelPropertyIndex::Iterable::Iterator by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(AllVerticesIterable::Iterator);
    explicit Iterator(LabelIndex::Iterable::Iterator);
    explicit Iterator(LabelPropertyIndex::Iterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  Iterator begin();
  Iterator end();
};

/// Structure used to return information about existing indices in the storage.
struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<std::pair<LabelId, PropertyId>> label_property;
};

/// Structure used to return information about existing constraints in the
/// storage.
struct ConstraintsInfo {
  std::vector<std::pair<LabelId, PropertyId>> existence;
  std::vector<std::pair<LabelId, std::set<PropertyId>>> unique;
};

/// Structure used to return information about the storage.
struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
  uint64_t disk_usage;
};

#ifdef MG_ENTERPRISE
enum class ReplicationRole : uint8_t { MAIN, REPLICA };
#endif

class Storage final {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit Storage(Config config = Config());

  ~Storage();

  class Accessor final {
   private:
    friend class Storage;

    explicit Accessor(Storage *storage);

   public:
    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;
    Accessor &operator=(Accessor &&other) = delete;

    // NOTE: After the accessor is moved, all objects derived from it (accessors
    // and iterators) are *invalid*. You have to get all derived objects again.
    Accessor(Accessor &&other) noexcept;

    ~Accessor();

    /// @throw std::bad_alloc
    VertexAccessor CreateVertex();

    std::optional<VertexAccessor> FindVertex(Gid gid, View view);

    VerticesIterable Vertices(View view) {
      return VerticesIterable(
          AllVerticesIterable(storage_->vertices_.access(), &transaction_, view,
                              &storage_->indices_, &storage_->constraints_,
                              storage_->config_.items));
    }

    VerticesIterable Vertices(LabelId label, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const PropertyValue &value, View view);

    VerticesIterable Vertices(
        LabelId label, PropertyId property,
        const std::optional<utils::Bound<PropertyValue>> &lower_bound,
        const std::optional<utils::Bound<PropertyValue>> &upper_bound,
        View view);

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount() const {
      return storage_->vertices_.size();
    }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label) const {
      return storage_->indices_.label_index.ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(
          label, property);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const PropertyValue &value) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(
          label, property, value);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    int64_t ApproximateVertexCount(
        LabelId label, PropertyId property,
        const std::optional<utils::Bound<PropertyValue>> &lower,
        const std::optional<utils::Bound<PropertyValue>> &upper) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(
          label, property, lower, upper);
    }

    /// @throw std::bad_alloc
    Result<bool> DeleteVertex(VertexAccessor *vertex);

    /// @throw std::bad_alloc
    Result<bool> DetachDeleteVertex(VertexAccessor *vertex);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                    EdgeTypeId edge_type);

    /// @throw std::bad_alloc
    Result<bool> DeleteEdge(EdgeAccessor *edge);

    const std::string &LabelToName(LabelId label) const;
    const std::string &PropertyToName(PropertyId property) const;
    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

    /// @throw std::bad_alloc if unable to insert a new mapping
    LabelId NameToLabel(const std::string_view &name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    PropertyId NameToProperty(const std::string_view &name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    EdgeTypeId NameToEdgeType(const std::string_view &name);

    bool LabelIndexExists(LabelId label) const {
      return storage_->indices_.label_index.IndexExists(label);
    }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const {
      return storage_->indices_.label_property_index.IndexExists(label,
                                                                 property);
    }

    IndicesInfo ListAllIndices() const {
      return {storage_->indices_.label_index.ListIndices(),
              storage_->indices_.label_property_index.ListIndices()};
    }

    ConstraintsInfo ListAllConstraints() const {
      return {ListExistenceConstraints(storage_->constraints_),
              storage_->constraints_.unique_constraints.ListConstraints()};
    }

    void AdvanceCommand();

    /// Commit returns `ConstraintViolation` if the changes made by this
    /// transaction violate an existence or unique constraint. In that case the
    /// transaction is automatically aborted. Otherwise, void is returned.
    /// @throw std::bad_alloc
    utils::BasicResult<ConstraintViolation, void> Commit();

    /// @throw std::bad_alloc
    void Abort();

   private:
#ifdef MG_ENTERPRISE
    /// @throw std::bad_alloc
    VertexAccessor CreateVertex(storage::Gid gid);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                    EdgeTypeId edge_type, storage::Gid gid);

    /// @throw std::bad_alloc
    utils::BasicResult<ConstraintViolation, void> Commit(
        std::optional<uint64_t> desired_commit_timestamp);
#endif

    Storage *storage_;
    std::shared_lock<utils::RWLock> storage_guard_;
    Transaction transaction_;
    bool is_transaction_active_;
    Config::Items config_;
  };

  Accessor Access() { return Accessor{this}; }

  const std::string &LabelToName(LabelId label) const;
  const std::string &PropertyToName(PropertyId property) const;
  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

  /// @throw std::bad_alloc if unable to insert a new mapping
  LabelId NameToLabel(const std::string_view &name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  PropertyId NameToProperty(const std::string_view &name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  EdgeTypeId NameToEdgeType(const std::string_view &name);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property);

  bool DropIndex(LabelId label);

  bool DropIndex(LabelId label, PropertyId property);

  IndicesInfo ListAllIndices() const;

  /// Creates an existence constraint. Returns true if the constraint was
  /// successfuly added, false if it already exists and a `ConstraintViolation`
  /// if there is an existing vertex violating the constraint.
  ///
  /// @throw std::bad_alloc
  /// @throw std::length_error
  utils::BasicResult<ConstraintViolation, bool> CreateExistenceConstraint(
      LabelId label, PropertyId property);

  /// Removes an existence constraint. Returns true if the constraint was
  /// removed, and false if it doesn't exist.
  bool DropExistenceConstraint(LabelId label, PropertyId property);

  /// Creates a unique constraint. In the case of two vertices violating the
  /// constraint, it returns `ConstraintViolation`. Otherwise returns a
  /// `UniqueConstraints::CreationStatus` enum with the following possibilities:
  ///     * `SUCCESS` if the constraint was successfully created,
  ///     * `ALREADY_EXISTS` if the constraint already existed,
  ///     * `EMPTY_PROPERTIES` if the property set is empty, or
  //      * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the
  //        limit of maximum number of properties.
  ///
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus>
  CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties);

  /// Removes a unique constraint. Returns `UniqueConstraints::DeletionStatus`
  /// enum with the following possibilities:
  ///     * `SUCCESS` if constraint was successfully removed,
  ///     * `NOT_FOUND` if the specified constraint was not found,
  ///     * `EMPTY_PROPERTIES` if the property set is empty, or
  ///     * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the
  //        limit of maximum number of properties.
  UniqueConstraints::DeletionStatus DropUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties);

  ConstraintsInfo ListAllConstraints() const;

  StorageInfo GetInfo() const;

#ifdef MG_ENTERPRISE
  template <ReplicationRole role, typename... Args>
  void SetReplicationRole(Args &&...args) {
    if (replication_role_.load() == role) {
      return;
    }

    std::unique_lock<utils::RWLock> replication_guard(replication_lock_);

    if constexpr (role == ReplicationRole::REPLICA) {
      ConfigureReplica(std::forward<Args>(args)...);
    } else if (role == ReplicationRole::MAIN) {
      // Main instance does not need replication server
      replication_server_.reset();
    }

    replication_role_.store(role);
  }

  void RegisterReplica(std::string name, io::network::Endpoint endpoint,
                       replication::ReplicationMode replication_mode =
                           replication::ReplicationMode::SYNC);
  void UnregisterReplica(std::string_view name);

  std::optional<replication::ReplicaState> ReplicaState(std::string_view name);
#endif

 private:
  Transaction CreateTransaction();

  /// @throw std::system_error
  /// @throw std::bad_alloc
  void CollectGarbage();

  bool InitializeWalFile();
  void FinalizeWalFile();

  void AppendToWal(const Transaction &transaction,
                   uint64_t final_commit_timestamp);
  void AppendToWal(durability::StorageGlobalOperation operation, LabelId label,
                   const std::set<PropertyId> &properties,
                   uint64_t final_commit_timestamp);

  void CreateSnapshot();

#ifdef MG_ENTERPRISE
  void ConfigureReplica(io::network::Endpoint endpoint);

  std::pair<durability::WalInfo, std::filesystem::path> LoadWal(
      replication::Decoder *decoder,
      durability::RecoveredIndicesAndConstraints *indices_constraints);
#endif

  // Main storage lock.
  //
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  mutable utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};
  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated.
  std::atomic<uint64_t> edge_count_{0};

  NameIdMapper name_id_mapper_;

  Constraints constraints_;
  Indices indices_;

  // Transaction engine
  utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  CommitLog commit_log_;

  utils::Synchronized<std::list<Transaction>, utils::SpinLock>
      committed_transactions_;

  Config config_;
  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;

  // Undo buffers that were unlinked and now are waiting to be freed.
  utils::Synchronized<std::list<std::pair<uint64_t, std::list<Delta>>>,
                      utils::SpinLock>
      garbage_undo_buffers_;

  // Vertices that are logically deleted but still have to be removed from
  // indices before removing them from the main storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_vertices_;

  // Vertices that are logically deleted and removed from indices and now wait
  // to be removed from the main storage.
  std::list<std::pair<uint64_t, Gid>> garbage_vertices_;

  // Edges that are logically deleted and wait to be removed from the main
  // storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_edges_;

  // Durability
  std::filesystem::path snapshot_directory_;
  std::filesystem::path wal_directory_;
  std::filesystem::path lock_file_path_;
  utils::OutputFile lock_file_handle_;

  utils::Scheduler snapshot_runner_;

  // UUID used to distinguish snapshots and to link snapshots to WALs
  std::string uuid_;
  // Sequence number used to keep track of the chain of WALs.
  uint64_t wal_seq_num_{0};

  std::optional<durability::WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};

  utils::FileRetainer file_retainer_;

  // Replication
#ifdef MG_ENTERPRISE
  std::atomic<uint64_t> last_commit_timestamp_{kTimestampInitialId};
  utils::RWLock replication_lock_{utils::RWLock::Priority::WRITE};

  struct ReplicationServer {
    std::optional<communication::ServerContext> rpc_server_context;
    std::optional<rpc::Server> rpc_server;

    explicit ReplicationServer() = default;
    ReplicationServer(const ReplicationServer &) = delete;
    ReplicationServer(ReplicationServer &&) = delete;
    ReplicationServer &operator=(const ReplicationServer &) = delete;
    ReplicationServer &operator=(ReplicationServer &&) = delete;

    ~ReplicationServer() {
      if (rpc_server) {
        rpc_server->Shutdown();
        rpc_server->AwaitShutdown();
      }
    }
  };

  using ReplicationClientList =
      utils::Synchronized<std::list<replication::ReplicationClient>,
                          utils::SpinLock>;

  std::optional<ReplicationServer> replication_server_;
  ReplicationClientList replication_clients_;

  std::atomic<ReplicationRole> replication_role_{ReplicationRole::MAIN};
#endif
};

}  // namespace storage
