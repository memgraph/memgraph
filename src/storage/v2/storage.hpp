#pragma once

#include <optional>

#include "storage/v2/commit_log.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"

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
   public:
    explicit Accessor(Storage *storage);

    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;

    Accessor(Accessor &&other) noexcept;

    // This operator isn't `noexcept` because the `Abort` function isn't
    // `noexcept`.
    Accessor &operator=(Accessor &&other);

    ~Accessor();

    VertexAccessor CreateVertex();

    std::optional<VertexAccessor> FindVertex(Gid gid, View view);

    Result<bool> DeleteVertex(VertexAccessor *vertex);

    Result<bool> DetachDeleteVertex(VertexAccessor *vertex);

    Result<EdgeAccessor> CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                    uint64_t edge_type);

    Result<bool> DeleteEdge(EdgeAccessor *edge);

    void AdvanceCommand();

    void Commit();

    void Abort();

   private:
    Storage *storage_;
    // TODO: when we are able to move Transaction objects without breaking the
    // pointers in Delta, we can get rid of the unique pointer here
    std::unique_ptr<Transaction> transaction_;
    bool is_transaction_starter_;
  };

  Accessor Access() { return Accessor{this}; }

 private:
  void CollectGarbage();

  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  // Transaction engine
  utils::SpinLock engine_lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  // TODO: This isn't really a commit log, it doesn't even care if a
  // transaction commited or aborted. We could probably combine this with
  // `timestamp_` in a sensible unit, something like TransactionClock or
  // whatever.
  CommitLog commit_log_;

  utils::SpinLock committed_transactions_lock_;
  // TODO: when we are able to move Transaction objects without breaking the
  // pointers in Delta, we can get rid of the unique pointer here
  std::list<std::unique_ptr<Transaction>> committed_transactions_;

  utils::SpinLock aborted_undo_buffers_lock_;
  std::list<std::pair<uint64_t, std::list<Delta>>> aborted_undo_buffers_;

  StorageGcConfig gc_config_;
  utils::Scheduler gc_runner_;
  std::mutex gc_lock_;
  std::list<std::pair<uint64_t, std::list<Delta>>> marked_undo_buffers_;
};

}  // namespace storage
