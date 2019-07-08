#pragma once

#include <optional>

#include "utils/skip_list.hpp"

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

class Storage final {
 public:
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
    Transaction *transaction_;
    bool is_transaction_starter_;
  };

  Accessor Access() { return Accessor{this}; }

 private:
  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  utils::SkipList<storage::Edge> edges_;
  std::atomic<uint64_t> vertex_id_{0};
  std::atomic<uint64_t> edge_id_{0};

  // Transaction engine
  utils::SpinLock lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  utils::SkipList<Transaction> transactions_;
};

}  // namespace storage
