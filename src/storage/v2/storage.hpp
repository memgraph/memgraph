#pragma once

#include <atomic>
#include <filesystem>
#include <optional>
#include <shared_mutex>

#include "io/network/endpoint.hpp"
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
#include "utils/on_scope_exit.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

#ifdef MG_ENTERPRISE
#include "rpc/server.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
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

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Transaction *transaction, View view,
                      Indices *indices, Constraints *constraints, Config::Items config)
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
      return VerticesIterable(AllVerticesIterable(storage_->vertices_.access(), &transaction_, view,
                                                  &storage_->indices_, &storage_->constraints_,
                                                  storage_->config_.items));
    }

    VerticesIterable Vertices(LabelId label, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view);

    /// Return approximate number of all vertices in the database.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount() const { return storage_->vertices_.size(); }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label) const {
      return storage_->indices_.label_index.ApproximateVertexCount(label);
    }

    /// Return approximate number of vertices with the given label and property.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(label, property);
    }

    /// Return approximate number of vertices with the given label and the given
    /// value for the given property. Note that this is always an over-estimate
    /// and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(label, property, value);
    }

    /// Return approximate number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                   const std::optional<utils::Bound<PropertyValue>> &lower,
                                   const std::optional<utils::Bound<PropertyValue>> &upper) const {
      return storage_->indices_.label_property_index.ApproximateVertexCount(label, property, lower, upper);
    }

    /// @throw std::bad_alloc
    Result<bool> DeleteVertex(VertexAccessor *vertex);

    /// @throw std::bad_alloc
    Result<bool> DetachDeleteVertex(VertexAccessor *vertex);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type);

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

    bool LabelIndexExists(LabelId label) const { return storage_->indices_.label_index.IndexExists(label); }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const {
      return storage_->indices_.label_property_index.IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const {
      return {storage_->indices_.label_index.ListIndices(), storage_->indices_.label_property_index.ListIndices()};
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
    utils::BasicResult<ConstraintViolation, void> Commit(std::optional<uint64_t> desired_commit_timestamp = {});

    /// @throw std::bad_alloc
    void Abort();

   private:
#ifdef MG_ENTERPRISE
    /// @throw std::bad_alloc
    VertexAccessor CreateVertex(storage::Gid gid);

    /// @throw std::bad_alloc
    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type, storage::Gid gid);
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
  bool CreateIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  IndicesInfo ListAllIndices() const;

  /// Creates an existence constraint. Returns true if the constraint was
  /// successfuly added, false if it already exists and a `ConstraintViolation`
  /// if there is an existing vertex violating the constraint.
  ///
  /// @throw std::bad_alloc
  /// @throw std::length_error
  utils::BasicResult<ConstraintViolation, bool> CreateExistenceConstraint(
      LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  /// Removes an existence constraint. Returns true if the constraint was
  /// removed, and false if it doesn't exist.
  bool DropExistenceConstraint(LabelId label, PropertyId property,
                               std::optional<uint64_t> desired_commit_timestamp = {});

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
  utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus> CreateUniqueConstraint(
      LabelId label, const std::set<PropertyId> &properties, std::optional<uint64_t> desired_commit_timestamp = {});

  /// Removes a unique constraint. Returns `UniqueConstraints::DeletionStatus`
  /// enum with the following possibilities:
  ///     * `SUCCESS` if constraint was successfully removed,
  ///     * `NOT_FOUND` if the specified constraint was not found,
  ///     * `EMPTY_PROPERTIES` if the property set is empty, or
  ///     * `PROPERTIES_SIZE_LIMIT_EXCEEDED` if the property set exceeds the
  //        limit of maximum number of properties.
  UniqueConstraints::DeletionStatus DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                                         std::optional<uint64_t> desired_commit_timestamp = {});

  ConstraintsInfo ListAllConstraints() const;

  StorageInfo GetInfo() const;

  bool LockPath();
  bool UnlockPath();

#if MG_ENTERPRISE
  bool SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config = {});

  bool SetMainReplicationRole();

  enum class RegisterReplicaError : uint8_t { NAME_EXISTS, CONNECTION_FAILED };

  /// @pre The instance should have a MAIN role
  /// @pre Timeout can only be set for SYNC replication
  utils::BasicResult<RegisterReplicaError, void> RegisterReplica(
      std::string name, io::network::Endpoint endpoint, replication::ReplicationMode replication_mode,
      const replication::ReplicationClientConfig &config = {});
  /// @pre The instance should have a MAIN role
  bool UnregisterReplica(std::string_view name);

  std::optional<replication::ReplicaState> GetReplicaState(std::string_view name);

  ReplicationRole GetReplicationRole() const;

  struct ReplicaInfo {
    std::string name;
    replication::ReplicationMode mode;
    std::optional<double> timeout;
    io::network::Endpoint endpoint;
    replication::ReplicaState state;
  };

  std::vector<ReplicaInfo> ReplicasInfo();
#endif

  void FreeMemory();

 private:
  Transaction CreateTransaction();

  /// @throw std::system_error
  /// @throw std::bad_alloc
  template <bool force>
  void CollectGarbage() {
    if constexpr (force) {
      // We take the unique lock on the main storage lock so we can forcefully clean
      // everything we can
      if (!main_lock_.try_lock()) {
        CollectGarbage<false>();
        return;
      }
    } else {
      // Because the garbage collector iterates through the indices and constraints
      // to clean them up, it must take the main lock for reading to make sure that
      // the indices and constraints aren't concurrently being modified.
      main_lock_.lock_shared();
    }

    utils::OnScopeExit lock_releaser{[&] {
      if constexpr (force) {
        main_lock_.unlock();
      } else {
        main_lock_.unlock_shared();
      }
    }};

    // Garbage collection must be performed in two phases. In the first phase,
    // deltas that won't be applied by any transaction anymore are unlinked from
    // the version chains. They cannot be deleted immediately, because there
    // might be a transaction that still needs them to terminate the version
    // chain traversal. They are instead marked for deletion and will be deleted
    // in the second GC phase in this GC iteration or some of the following
    // ones.
    std::unique_lock<std::mutex> gc_guard(gc_lock_, std::try_to_lock);
    if (!gc_guard.owns_lock()) {
      return;
    }

    uint64_t oldest_active_start_timestamp = commit_log_->OldestActive();
    // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
    // list immediately, because we would have to repeatedly take
    // garbage_undo_buffers lock.
    std::list<std::pair<uint64_t, std::list<Delta>>> unlinked_undo_buffers;

    // We will only free vertices deleted up until now in this GC cycle, and we
    // will do it after cleaning-up the indices. That way we are sure that all
    // vertices that appear in an index also exist in main storage.
    std::list<Gid> current_deleted_edges;
    std::list<Gid> current_deleted_vertices;
    deleted_vertices_->swap(current_deleted_vertices);
    deleted_edges_->swap(current_deleted_edges);

    // Flag that will be used to determine whether the Index GC should be run. It
    // should be run when there were any items that were cleaned up (there were
    // updates between this run of the GC and the previous run of the GC). This
    // eliminates high CPU usage when the GC doesn't have to clean up anything.
    bool run_index_cleanup = !committed_transactions_->empty() || !garbage_undo_buffers_->empty();

    while (true) {
      // We don't want to hold the lock on commited transactions for too long,
      // because that prevents other transactions from committing.
      Transaction *transaction;
      {
        auto committed_transactions_ptr = committed_transactions_.Lock();
        if (committed_transactions_ptr->empty()) {
          break;
        }
        transaction = &committed_transactions_ptr->front();
      }

      auto commit_timestamp = transaction->commit_timestamp->load(std::memory_order_acquire);
      if (commit_timestamp >= oldest_active_start_timestamp) {
        break;
      }

      // When unlinking a delta which is the first delta in its version chain,
      // special care has to be taken to avoid the following race condition:
      //
      // [Vertex] --> [Delta A]
      //
      //    GC thread: Delta A is the first in its chain, it must be unlinked from
      //               vertex and marked for deletion
      //    TX thread: Update vertex and add Delta B with Delta A as next
      //
      // [Vertex] --> [Delta B] <--> [Delta A]
      //
      //    GC thread: Unlink delta from Vertex
      //
      // [Vertex] --> (nullptr)
      //
      // When processing a delta that is the first one in its chain, we
      // obtain the corresponding vertex or edge lock, and then verify that this
      // delta still is the first in its chain.
      // When processing a delta that is in the middle of the chain we only
      // process the final delta of the given transaction in that chain. We
      // determine the owner of the chain (either a vertex or an edge), obtain the
      // corresponding lock, and then verify that this delta is still in the same
      // position as it was before taking the lock.
      //
      // Even though the delta chain is lock-free (both `next` and `prev`) the
      // chain should not be modified without taking the lock from the object that
      // owns the chain (either a vertex or an edge). Modifying the chain without
      // taking the lock will cause subtle race conditions that will leave the
      // chain in a broken state.
      // The chain can be only read without taking any locks.

      for (Delta &delta : transaction->deltas) {
        while (true) {
          auto prev = delta.prev.Get();
          switch (prev.type) {
            case PreviousPtr::Type::VERTEX: {
              Vertex *vertex = prev.vertex;
              std::lock_guard<utils::SpinLock> vertex_guard(vertex->lock);
              if (vertex->delta != &delta) {
                // Something changed, we're not the first delta in the chain
                // anymore.
                continue;
              }
              vertex->delta = nullptr;
              if (vertex->deleted) {
                current_deleted_vertices.push_back(vertex->gid);
              }
              break;
            }
            case PreviousPtr::Type::EDGE: {
              Edge *edge = prev.edge;
              std::lock_guard<utils::SpinLock> edge_guard(edge->lock);
              if (edge->delta != &delta) {
                // Something changed, we're not the first delta in the chain
                // anymore.
                continue;
              }
              edge->delta = nullptr;
              if (edge->deleted) {
                current_deleted_edges.push_back(edge->gid);
              }
              break;
            }
            case PreviousPtr::Type::DELTA: {
              if (prev.delta->timestamp->load(std::memory_order_acquire) == commit_timestamp) {
                // The delta that is newer than this one is also a delta from this
                // transaction. We skip the current delta and will remove it as a
                // part of the suffix later.
                break;
              }
              std::unique_lock<utils::SpinLock> guard;
              {
                // We need to find the parent object in order to be able to use
                // its lock.
                auto parent = prev;
                while (parent.type == PreviousPtr::Type::DELTA) {
                  parent = parent.delta->prev.Get();
                }
                switch (parent.type) {
                  case PreviousPtr::Type::VERTEX:
                    guard = std::unique_lock<utils::SpinLock>(parent.vertex->lock);
                    break;
                  case PreviousPtr::Type::EDGE:
                    guard = std::unique_lock<utils::SpinLock>(parent.edge->lock);
                    break;
                  case PreviousPtr::Type::DELTA:
                    LOG_FATAL("Invalid database state!");
                }
              }
              if (delta.prev.Get() != prev) {
                // Something changed, we could now be the first delta in the
                // chain.
                continue;
              }
              Delta *prev_delta = prev.delta;
              prev_delta->next.store(nullptr, std::memory_order_release);
              break;
            }
          }
          break;
        }
      }

      committed_transactions_.WithLock([&](auto &committed_transactions) {
        unlinked_undo_buffers.emplace_back(0, std::move(transaction->deltas));
        committed_transactions.pop_front();
      });
    }

    // After unlinking deltas from vertices, we refresh the indices. That way
    // we're sure that none of the vertices from `current_deleted_vertices`
    // appears in an index, and we can safely remove the from the main storage
    // after the last currently active transaction is finished.
    if (run_index_cleanup) {
      // This operation is very expensive as it traverses through all of the items
      // in every index every time.
      RemoveObsoleteEntries(&indices_, oldest_active_start_timestamp);
      constraints_.unique_constraints.RemoveObsoleteEntries(oldest_active_start_timestamp);
    }

    {
      std::unique_lock<utils::SpinLock> guard(engine_lock_);
      uint64_t mark_timestamp = timestamp_;
      // Take garbage_undo_buffers lock while holding the engine lock to make
      // sure that entries are sorted by mark timestamp in the list.
      garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
        // Release engine lock because we don't have to hold it anymore and
        // this could take a long time.
        guard.unlock();
        // TODO(mtomic): holding garbage_undo_buffers_ lock here prevents
        // transactions from aborting until we're done marking, maybe we should
        // add them one-by-one or something
        for (auto &[timestamp, undo_buffer] : unlinked_undo_buffers) {
          timestamp = mark_timestamp;
        }
        garbage_undo_buffers.splice(garbage_undo_buffers.end(), unlinked_undo_buffers);
      });
      for (auto vertex : current_deleted_vertices) {
        garbage_vertices_.emplace_back(mark_timestamp, vertex);
      }
    }

    while (true) {
      auto garbage_undo_buffers_ptr = garbage_undo_buffers_.Lock();
      if (garbage_undo_buffers_ptr->empty() ||
          garbage_undo_buffers_ptr->front().first > oldest_active_start_timestamp) {
        break;
      }
      garbage_undo_buffers_ptr->pop_front();
    }

    {
      auto vertex_acc = vertices_.access();
      if constexpr (force) {
        while (!garbage_vertices_.empty()) {
          MG_ASSERT(vertex_acc.remove(garbage_vertices_.front().second), "Invalid database state!");
          garbage_vertices_.pop_front();
        }
      } else {
        while (!garbage_vertices_.empty() && garbage_vertices_.front().first < oldest_active_start_timestamp) {
          MG_ASSERT(vertex_acc.remove(garbage_vertices_.front().second), "Invalid database state!");
          garbage_vertices_.pop_front();
        }
      }
    }
    {
      auto edge_acc = edges_.access();
      for (auto edge : current_deleted_edges) {
        MG_ASSERT(edge_acc.remove(edge), "Invalid database state!");
      }
    }
  }

  bool InitializeWalFile();
  void FinalizeWalFile();

  void AppendToWal(const Transaction &transaction, uint64_t final_commit_timestamp);
  void AppendToWal(durability::StorageGlobalOperation operation, LabelId label, const std::set<PropertyId> &properties,
                   uint64_t final_commit_timestamp);

  void CreateSnapshot();

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

#ifdef MG_ENTERPRISE
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
  std::optional<CommitLog> commit_log_;

  utils::Synchronized<std::list<Transaction>, utils::SpinLock> committed_transactions_;

  Config config_;
  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;

  // Undo buffers that were unlinked and now are waiting to be freed.
  utils::Synchronized<std::list<std::pair<uint64_t, std::list<Delta>>>, utils::SpinLock> garbage_undo_buffers_;

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

  // UUID to distinguish different main instance runs for replication process
  // on SAME storage.
  // Multiple instances can have same storage UUID and be MAIN at the same time.
  // We cannot compare commit timestamps of those instances if one of them
  // becomes the replica of the other so we use epoch_id_ as additional
  // discriminating property.
  // Example of this:
  // We have 2 instances of the same storage, S1 and S2.
  // S1 and S2 are MAIN and accept their own commits and write them to the WAL.
  // At the moment when S1 commited a transaction with timestamp 20, and S2
  // a different transaction with timestamp 15, we change S2's role to REPLICA
  // and register it on S1.
  // Without using the epoch_id, we don't know that S1 and S2 have completely
  // different transactions, we think that the S2 is behind only by 5 commits.
  std::string epoch_id_;
  // History of the previous epoch ids.
  // Each value consists of the epoch id along the last commit belonging to that
  // epoch.
  std::deque<std::pair<std::string, uint64_t>> epoch_history_;

  std::optional<durability::WalFile> wal_file_;
  uint64_t wal_unsynced_transactions_{0};

  utils::FileRetainer file_retainer_;

  // Global locker that is used for clients file locking
  utils::FileRetainer::FileLocker global_locker_;

  // Replication
#ifdef MG_ENTERPRISE
  // Last commited timestamp
  std::atomic<uint64_t> last_commit_timestamp_{kTimestampInitialId};

  class ReplicationServer;
  std::unique_ptr<ReplicationServer> replication_server_{nullptr};

  class ReplicationClient;
  // We create ReplicationClient using unique_ptr so we can move
  // newly created client into the vector.
  // We cannot move the client directly because it contains ThreadPool
  // which cannot be moved. Also, the move is necessary because
  // we don't want to create the client directly inside the vector
  // because that would require the lock on the list putting all
  // commits (they iterate list of clients) to halt.
  // This way we can initialize client in main thread which means
  // that we can immediately notify the user if the initialization
  // failed.
  using ReplicationClientList = utils::Synchronized<std::vector<std::unique_ptr<ReplicationClient>>, utils::SpinLock>;
  ReplicationClientList replication_clients_;

  std::atomic<ReplicationRole> replication_role_{ReplicationRole::MAIN};
#endif
};

}  // namespace storage
