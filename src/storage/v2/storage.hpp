#pragma once

#include <optional>
#include <shared_mutex>

#include "storage/v2/commit_log.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"

namespace storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

/// Pass this class to the \ref Storage constructor to set the behavior of the
/// garbage control.
///
/// There are three options:
//    1. NONE - No GC at all, only useful for benchmarking.
//    2. PERIODIC - A separate thread performs GC periodically with given
//                  interval (this is the default, with 1 second interval).
//    3. ON_FINISH - Whenever a transaction commits or aborts, GC is performed
//                   on the same thread.
struct StorageGcConfig {
  enum class Type { NONE, PERIODIC, ON_FINISH };
  Type type;
  std::chrono::milliseconds interval;
};

inline static constexpr StorageGcConfig DefaultGcConfig = {
    .type = StorageGcConfig::Type::PERIODIC,
    .interval = std::chrono::milliseconds(1000)};

/// Iterable for iterating through all vertices of a Storage.
///
/// An instance of this will be usually be wrapped inside VerticesIterable for
/// generic, public use.
class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;

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
                      Transaction *transaction, View view, Indices *indices)
      : vertices_accessor_(std::move(vertices_accessor)),
        transaction_(transaction),
        view_(view),
        indices_(indices) {}

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

    /// @throw std::bad_alloc raised in
    ///        LabelPropertyIndex::Iterable::Iterator::operator++
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
};

class Storage final {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit Storage(StorageGcConfig gc_config = DefaultGcConfig);

  ~Storage();

  class Accessor final {
   private:
    friend class Storage;

    Accessor(Storage *storage, uint64_t transaction_id,
             uint64_t start_timestamp);

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
      return VerticesIterable(AllVerticesIterable(storage_->vertices_.access(),
                                                  &transaction_, view,
                                                  &storage_->indices_));
    }

    /// @throw std::bad_alloc raised in Index::Vertices
    VerticesIterable Vertices(LabelId label, View view);

    /// @throw std::bad_alloc raised in Index::Vertices
    VerticesIterable Vertices(LabelId label, PropertyId property, View view);

    /// @throw std::bad_alloc raised in Index::Vertices
    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const PropertyValue &value, View view);

    /// @throw std::bad_alloc raised in Index::Vertices
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
    LabelId NameToLabel(const std::string &name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    PropertyId NameToProperty(const std::string &name);

    /// @throw std::bad_alloc if unable to insert a new mapping
    EdgeTypeId NameToEdgeType(const std::string &name);

    void AdvanceCommand();

    /// Commit returns `ExistenceConstraintViolation` if the changes made by
    /// this transaction violate an existence constraint. In that case the
    /// transaction is automatically aborted. Otherwise, void is returned.
    /// @throw std::bad_alloc
    utils::BasicResult<ExistenceConstraintViolation, void> Commit();

    /// @throw std::bad_alloc
    void Abort();

   private:
    Storage *storage_;
    Transaction transaction_;
    bool is_transaction_active_;

    std::shared_lock<utils::RWLock> storage_guard_;
  };

  Accessor Access();

  const std::string &LabelToName(LabelId label) const;
  const std::string &PropertyToName(PropertyId property) const;
  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

  /// @throw std::bad_alloc if unable to insert a new mapping
  LabelId NameToLabel(const std::string &name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  PropertyId NameToProperty(const std::string &name);

  /// @throw std::bad_alloc if unable to insert a new mapping
  EdgeTypeId NameToEdgeType(const std::string &name);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property) {
    std::unique_lock<utils::RWLock> storage_guard(main_lock_);
    return indices_.label_property_index.CreateIndex(label, property,
                                                     vertices_.access());
  }

  bool DropIndex(LabelId label, PropertyId property) {
    std::unique_lock<utils::RWLock> storage_guard(main_lock_);
    return indices_.label_property_index.DropIndex(label, property);
  }

  bool LabelPropertyIndexExists(LabelId label, PropertyId property) {
    return indices_.label_property_index.IndexExists(label, property);
  }

  IndicesInfo ListAllIndices() const {
    return {indices_.label_index.ListIndices(),
            indices_.label_property_index.ListIndices()};
  }

  /// Creates a unique constraint`. Returns true if the constraint was
  /// successfuly added, false if it already exists and an
  /// `ExistenceConstraintViolation` if there is an existing vertex violating
  /// the constraint.
  ///
  /// @throw std::bad_alloc
  /// @throw std::length_error
  utils::BasicResult<ExistenceConstraintViolation, bool>
  CreateExistenceConstraint(LabelId label, PropertyId property) {
    std::unique_lock<utils::RWLock> storage_guard(main_lock_);
    return ::storage::CreateExistenceConstraint(&constraints_, label, property,
                                                vertices_.access());
  }

  /// Removes a unique constraint. Returns true if the constraint was removed,
  /// and false if it doesn't exist.
  bool DropExistenceConstraint(LabelId label, PropertyId property) {
    std::unique_lock<utils::RWLock> storage_guard(main_lock_);
    return ::storage::DropExistenceConstraint(&constraints_, label, property);
  }

  ConstraintsInfo ListAllConstraints() const {
    return {ListExistenceConstraints(constraints_)};
  }

 private:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  void CollectGarbage();

  // Main storage lock.
  //
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when doing
  // operations on storage that affect the global state, for example index
  // creation.
  utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  NameIdMapper name_id_mapper_;

  Indices indices_;
  Constraints constraints_;

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

  StorageGcConfig gc_config_;
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
};

}  // namespace storage
