#pragma once

#include <optional>
#include <shared_mutex>

#include "storage/v2/commit_log.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
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

class Storage final {
 public:
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

    VertexAccessor CreateVertex();

    std::optional<VertexAccessor> FindVertex(Gid gid, View view);

    Result<bool> DeleteVertex(VertexAccessor *vertex);

    Result<bool> DetachDeleteVertex(VertexAccessor *vertex);

    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                    EdgeTypeId edge_type);

    Result<bool> DeleteEdge(EdgeAccessor *edge);

    const std::string &LabelToName(LabelId label);
    const std::string &PropertyToName(PropertyId property);
    const std::string &EdgeTypeToName(EdgeTypeId edge_type);

    LabelId NameToLabel(const std::string &name);
    PropertyId NameToProperty(const std::string &name);
    EdgeTypeId NameToEdgeType(const std::string &name);

    void AdvanceCommand();

    void Commit();

    void Abort();

   private:
    Storage *storage_;
    Transaction transaction_;
    bool is_transaction_starter_;
    bool is_transaction_active_;

    std::shared_lock<utils::RWLock> storage_guard_;
  };

  Accessor Access();

 private:
  void CollectGarbage();

  // Main storage lock.
  //
  // Accessors take a shared lock when starting, so it is possible to block
  // creation of new accessors by taking a unique lock. This is used when
  // building a label-property index because it is much simpler to do when there
  // are no parallel reads and writes.
  utils::RWLock main_lock_{utils::RWLock::Priority::WRITE};

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  NameIdMapper name_id_mapper_;

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

  // Vertices that are logically deleted and now are waiting to be removed from
  // the main storage.
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_vertices_;
  utils::Synchronized<std::list<Gid>, utils::SpinLock> deleted_edges_;
};

}  // namespace storage
