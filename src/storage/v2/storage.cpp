#include "storage/v2/storage.hpp"

#include <memory>

#include <glog/logging.h>

#include "storage/v2/mvcc.hpp"

namespace storage {

Storage::Storage(StorageGcConfig gc_config) : gc_config_(gc_config) {
  if (gc_config.type == StorageGcConfig::Type::PERIODIC) {
    gc_runner_.Run("Storage GC", gc_config.interval,
                   [this] { this->CollectGarbage(); });
  }
}

Storage::~Storage() {
  if (gc_config_.type == StorageGcConfig::Type::PERIODIC) {
    gc_runner_.Stop();
  }
}

Storage::Accessor::Accessor(Storage *storage)
    : storage_(storage), is_transaction_starter_(true) {
  // We acquire the transaction engine lock here because we access (and
  // modify) the transaction engine variables (`transaction_id` and
  // `timestamp`) below.
  uint64_t transaction_id;
  uint64_t start_timestamp;
  {
    std::lock_guard<utils::SpinLock> guard(storage_->engine_lock_);
    transaction_id = storage_->transaction_id_++;
    start_timestamp = storage_->timestamp_++;
  }
  transaction_ = std::make_unique<Transaction>(transaction_id, start_timestamp);
}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      transaction_(std::move(other.transaction_)),
      is_transaction_starter_(true) {
  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;
}

// This operator isn't `noexcept` because the `Abort` function isn't
// `noexcept`.
Storage::Accessor &Storage::Accessor::operator=(Accessor &&other) {
  if (this == &other) return *this;

  if (is_transaction_starter_ && transaction_) {
    Abort();
  }

  storage_ = other.storage_;
  transaction_ = std::move(other.transaction_);
  is_transaction_starter_ = true;

  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;

  return *this;
}

Storage::Accessor::~Accessor() {
  if (is_transaction_starter_ && transaction_) {
    Abort();
  }
}

VertexAccessor Storage::Accessor::CreateVertex() {
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(transaction_.get());
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The vertex must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Vertex accessor!";
  delta->prev.Set(&*it);
  return VertexAccessor{&*it, transaction_.get()};
}

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, transaction_.get(), view);
}

Result<bool> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == transaction_.get())
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(transaction_.get(), vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_ptr->deleted) return Result<bool>{false};

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty())
    return Result<bool>{Error::VERTEX_HAS_EDGES};

  CreateAndLinkDelta(transaction_.get(), vertex_ptr,
                     Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<bool> Storage::Accessor::DetachDeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == transaction_.get())
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> in_edges;
  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(transaction_.get(), vertex_ptr))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_ptr->deleted) return Result<bool>{false};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, from_vertex, vertex_ptr,
                   transaction_.get()};
    auto ret = DeleteEdge(&e);
    if (ret.IsError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, vertex_ptr, to_vertex, transaction_.get()};
    auto ret = DeleteEdge(&e);
    if (ret.IsError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  // We need to check again for serialization errors because we unlocked the
  // vertex. Some other transaction could have modified the vertex in the
  // meantime if we didn't have any edges to delete.

  if (!PrepareForWrite(transaction_.get(), vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  CHECK(!vertex_ptr->deleted) << "Invalid database state!";

  CreateAndLinkDelta(transaction_.get(), vertex_ptr,
                     Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<EdgeAccessor> Storage::Accessor::CreateEdge(VertexAccessor *from,
                                                   VertexAccessor *to,
                                                   uint64_t edge_type) {
  CHECK(from->transaction_ == to->transaction_)
      << "VertexAccessors must be from the same transaction when creating "
         "an edge!";
  CHECK(from->transaction_ == transaction_.get())
      << "VertexAccessors must be from the same transaction in when "
         "creating an edge!";

  auto from_vertex = from->vertex_;
  auto to_vertex = to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock,
                                               std::defer_lock);
  std::unique_lock<utils::SpinLock> guard_to(to_vertex->lock, std::defer_lock);
  if (from_vertex->gid < to_vertex->gid) {
    guard_from.lock();
    guard_to.lock();
  } else if (from_vertex->gid > to_vertex->gid) {
    guard_to.lock();
    guard_from.lock();
  } else {
    // The vertices are the same vertex, only lock one.
    guard_from.lock();
  }

  if (!PrepareForWrite(transaction_.get(), from_vertex))
    return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_.get(), to_vertex))
      return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  auto gid = storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->edges_.access();
  auto delta = CreateDeleteObjectDelta(transaction_.get());
  auto [it, inserted] = acc.insert(Edge{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The edge must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Edge accessor!";
  auto edge = &*it;
  delta->prev.Set(&*it);

  CreateAndLinkDelta(transaction_.get(), from_vertex, Delta::RemoveOutEdgeTag(),
                     edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(transaction_.get(), to_vertex, Delta::RemoveInEdgeTag(),
                     edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  return Result<EdgeAccessor>{EdgeAccessor{edge, edge_type, from_vertex,
                                           to_vertex, transaction_.get()}};
}

Result<bool> Storage::Accessor::DeleteEdge(EdgeAccessor *edge) {
  CHECK(edge->transaction_ == transaction_.get())
      << "EdgeAccessor must be from the same transaction as the storage "
         "accessor when deleting an edge!";
  auto edge_ptr = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::lock_guard<utils::SpinLock> guard(edge_ptr->lock);

  if (!PrepareForWrite(transaction_.get(), edge_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (edge_ptr->deleted) return Result<bool>{false};

  auto from_vertex = edge->from_vertex_;
  auto to_vertex = edge->to_vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock,
                                               std::defer_lock);
  std::unique_lock<utils::SpinLock> guard_to(to_vertex->lock, std::defer_lock);
  if (from_vertex->gid < to_vertex->gid) {
    guard_from.lock();
    guard_to.lock();
  } else if (from_vertex->gid > to_vertex->gid) {
    guard_to.lock();
    guard_from.lock();
  } else {
    // The vertices are the same vertex, only lock one.
    guard_from.lock();
  }

  if (!PrepareForWrite(transaction_.get(), from_vertex))
    return Result<bool>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_.get(), to_vertex))
      return Result<bool>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  CreateAndLinkDelta(transaction_.get(), edge_ptr, Delta::RecreateObjectTag());
  edge_ptr->deleted = true;

  CreateAndLinkDelta(transaction_.get(), from_vertex, Delta::AddOutEdgeTag(),
                     edge_type, to_vertex, edge_ptr);
  {
    std::tuple<uint64_t, Vertex *, Edge *> link{edge_type, to_vertex, edge_ptr};
    auto it = std::find(from_vertex->out_edges.begin(),
                        from_vertex->out_edges.end(), link);
    CHECK(it != from_vertex->out_edges.end()) << "Invalid database state!";
    std::swap(*it, *from_vertex->out_edges.rbegin());
    from_vertex->out_edges.pop_back();
  }

  CreateAndLinkDelta(transaction_.get(), to_vertex, Delta::AddInEdgeTag(),
                     edge_type, from_vertex, edge_ptr);
  {
    std::tuple<uint64_t, Vertex *, Edge *> link{edge_type, from_vertex,
                                                edge_ptr};
    auto it =
        std::find(to_vertex->in_edges.begin(), to_vertex->in_edges.end(), link);
    CHECK(it != to_vertex->in_edges.end()) << "Invalid database state!";
    std::swap(*it, *to_vertex->in_edges.rbegin());
    to_vertex->in_edges.pop_back();
  }

  return Result<bool>{true};
}

void Storage::Accessor::AdvanceCommand() { ++transaction_->command_id; }

void Storage::Accessor::Commit() {
  CHECK(transaction_) << "The transaction is already terminated!";
  CHECK(!transaction_->must_abort) << "The transaction can't be committed!";

  if (transaction_->deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    storage_->commit_log_.MarkFinished(transaction_->start_timestamp);
    transaction_ = nullptr;
  } else {
    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_->start_timestamp;
    uint64_t commit_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp = storage_->timestamp_++;

      // Take committed_transactions lock while holding the engine lock to
      // make sure that committed transactions are sorted by the commit
      // timestamp in the list.
      std::lock_guard<utils::SpinLock> committed_transactions_guard(
          storage_->committed_transactions_lock_);
      // TODO: release lock, and update all deltas to have a local copy
      // of the commit timestamp
      transaction_->commit_timestamp.store(commit_timestamp,
                                           std::memory_order_release);
      // Release engine lock because we don't have to hold it anymore and
      // emplace back could take a long time.
      engine_guard.unlock();
      storage_->committed_transactions_.emplace_back(std::move(transaction_));
    }

    storage_->commit_log_.MarkFinished(start_timestamp);
    storage_->commit_log_.MarkFinished(commit_timestamp);
  }
  if (storage_->gc_config_.type == StorageGcConfig::Type::ON_FINISH) {
    storage_->CollectGarbage();
  }
}

void Storage::Accessor::Abort() {
  CHECK(transaction_) << "The transaction is already terminated!";
  for (const auto &delta : transaction_->deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto vertex = prev.vertex;
        std::unique_lock<utils::SpinLock> guard(vertex->lock);
        Delta *current = vertex->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) ==
                   transaction_->transaction_id) {
          switch (current->action) {
            case Delta::Action::REMOVE_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                                  current->label);
              CHECK(it != vertex->labels.end()) << "Invalid database state!";
              std::swap(*it, *vertex->labels.rbegin());
              vertex->labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                                  current->label);
              CHECK(it == vertex->labels.end()) << "Invalid database state!";
              vertex->labels.push_back(current->label);
              break;
            }
            case Delta::Action::SET_PROPERTY: {
              auto it = vertex->properties.find(current->property.key);
              if (it != vertex->properties.end()) {
                if (current->property.value.IsNull()) {
                  // remove the property
                  vertex->properties.erase(it);
                } else {
                  // set the value
                  it->second = current->property.value;
                }
              } else if (!current->property.value.IsNull()) {
                vertex->properties.emplace(current->property.key,
                                           current->property.value);
              }
              break;
            }
            case Delta::Action::ADD_IN_EDGE: {
              std::tuple<uint64_t, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(),
                                  vertex->in_edges.end(), link);
              CHECK(it == vertex->in_edges.end()) << "Invalid database state!";
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              std::tuple<uint64_t, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(),
                                  vertex->out_edges.end(), link);
              CHECK(it == vertex->out_edges.end()) << "Invalid database state!";
              vertex->out_edges.push_back(link);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              std::tuple<uint64_t, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(),
                                  vertex->in_edges.end(), link);
              CHECK(it != vertex->in_edges.end()) << "Invalid database state!";
              std::swap(*it, *vertex->in_edges.rbegin());
              vertex->in_edges.pop_back();
              break;
            }
            case Delta::Action::REMOVE_OUT_EDGE: {
              std::tuple<uint64_t, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(),
                                  vertex->out_edges.end(), link);
              CHECK(it != vertex->out_edges.end()) << "Invalid database state!";
              std::swap(*it, *vertex->out_edges.rbegin());
              vertex->out_edges.pop_back();
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              auto acc = storage_->vertices_.access();
              // We must unlock the guard here. Otherwise it might try to access
              // freed memory in its destructor because the vertex is deleted
              // along with its lock.
              guard.unlock();
              CHECK(acc.remove(vertex->gid)) << "Invalid database state!";
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              vertex->deleted = false;
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
        }
        vertex->delta = current;
        if (current != nullptr) {
          current->prev.Set(vertex);
        }

        break;
      }
      case PreviousPtr::Type::EDGE: {
        auto edge = prev.edge;
        std::unique_lock<utils::SpinLock> guard(edge->lock);
        Delta *current = edge->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) ==
                   transaction_->transaction_id) {
          switch (current->action) {
            case Delta::Action::SET_PROPERTY: {
              auto it = edge->properties.find(current->property.key);
              if (it != edge->properties.end()) {
                if (current->property.value.IsNull()) {
                  // remove the property
                  edge->properties.erase(it);
                } else {
                  // set the value
                  it->second = current->property.value;
                }
              } else if (!current->property.value.IsNull()) {
                edge->properties.emplace(current->property.key,
                                         current->property.value);
              }
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              auto acc = storage_->edges_.access();
              // We must unlock the guard here. Otherwise it might try to access
              // freed memory in its destructor because the edge is deleted
              // along with its lock.
              guard.unlock();
              CHECK(acc.remove(edge->gid)) << "Invalid database state!";
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              edge->deleted = false;
              break;
            }
            case Delta::Action::REMOVE_LABEL:
            case Delta::Action::ADD_LABEL:
            case Delta::Action::ADD_IN_EDGE:
            case Delta::Action::ADD_OUT_EDGE:
            case Delta::Action::REMOVE_IN_EDGE:
            case Delta::Action::REMOVE_OUT_EDGE: {
              LOG(FATAL) << "Invalid database state!";
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
        }
        edge->delta = current;
        if (current != nullptr) {
          current->prev.Set(edge);
        }

        break;
      }
      case PreviousPtr::Type::DELTA:
        break;
    }
  }

  {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    uint64_t mark_timestamp = storage_->timestamp_;
    // Take aborted_undo_buffers lock while holding the engine lock to make
    // sure that entries are sorted by mark timestamp in the list.
    std::lock_guard<utils::SpinLock> aborted_undo_buffers_guard(
        storage_->aborted_undo_buffers_lock_);
    // Release engine lock because we don't have to hold it anymore and
    // emplace back could take a long time.
    engine_guard.unlock();
    storage_->aborted_undo_buffers_.emplace_back(
        mark_timestamp, std::move(transaction_->deltas));
  }

  storage_->commit_log_.MarkFinished(transaction_->start_timestamp);
  transaction_ = nullptr;
  if (storage_->gc_config_.type == StorageGcConfig::Type::ON_FINISH) {
    storage_->CollectGarbage();
  }
}

void Storage::CollectGarbage() {
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

  uint64_t oldest_active_start_timestamp = commit_log_.OldestActive();
  // We don't move undo buffers of unlinked transactions to
  // marked_undo_buffers list immediately, because we would have to repeatedly
  // take transaction engine lock.
  std::list<std::unique_ptr<Transaction>> unlinked;

  while (true) {
    // We don't want to hold the lock on commited transactions for too long,
    // because that prevents other transactions from committing.
    Transaction *transaction;

    {
      std::lock_guard<utils::SpinLock> guard(committed_transactions_lock_);
      if (committed_transactions_.empty()) {
        break;
      }

      transaction = committed_transactions_.front().get();
    }

    if (transaction->commit_timestamp >= oldest_active_start_timestamp) {
      break;
    }

    // When unlinking a delta which is a first delta in its version chain,
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

    for (Delta &delta : transaction->deltas) {
      while (true) {
        auto prev = delta.prev.Get();
        switch (prev.type) {
          case PreviousPtr::Type::VERTEX: {
            Vertex *vertex = prev.vertex;
            std::unique_lock<utils::SpinLock> vertex_guard(vertex->lock);
            if (vertex->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            vertex->delta = nullptr;
            if (vertex->deleted) {
              // We must unlock the guard now because the lock it is holding
              // might be destroyed when vertex is deleted.
              vertex_guard.unlock();
              auto acc = vertices_.access();
              acc.remove(vertex->gid);
            }
            break;
          }
          case PreviousPtr::Type::EDGE: {
            Edge *edge = prev.edge;
            std::unique_lock<utils::SpinLock> edge_guard(edge->lock);
            if (edge->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            if (edge->deleted) {
              // We must unlock the guard now because the lock it is holding
              // might be destroyed when vertex is deleted.
              edge_guard.unlock();
              auto acc = edges_.access();

              acc.remove(edge->gid);
            }
            break;
          }
          case PreviousPtr::Type::DELTA: {
            Delta *prev_delta = prev.delta;
            prev_delta->next.store(nullptr, std::memory_order_release);
            break;
          }
        }
        break;
      }
    }

    {
      std::lock_guard<utils::SpinLock> guard(committed_transactions_lock_);
      // We use splice here to avoid allocating storage for new list nodes.
      unlinked.splice(unlinked.begin(), committed_transactions_,
                      committed_transactions_.begin());
    }
  }

  uint64_t mark_timestamp;
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    mark_timestamp = timestamp_;
  }

  for (auto &transaction : unlinked) {
    marked_undo_buffers_.emplace_back(mark_timestamp,
                                      std::move(transaction->deltas));
  }

  while (!marked_undo_buffers_.empty() &&
         marked_undo_buffers_.front().first < oldest_active_start_timestamp) {
    marked_undo_buffers_.pop_front();
  }

  while (true) {
    std::lock_guard<utils::SpinLock> aborted_undo_buffers_guard(
        aborted_undo_buffers_lock_);
    if (aborted_undo_buffers_.empty() ||
        aborted_undo_buffers_.front().first >= oldest_active_start_timestamp) {
      break;
    }
    aborted_undo_buffers_.pop_front();
  }
}

}  // namespace storage
