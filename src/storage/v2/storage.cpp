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

Storage::Accessor::Accessor(Storage *storage, uint64_t transaction_id,
                            uint64_t start_timestamp)
    : storage_(storage),
      transaction_(transaction_id, start_timestamp),
      is_transaction_starter_(true),
      is_transaction_active_(true),
      storage_guard_(storage_->main_lock_) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      transaction_(std::move(other.transaction_)),
      is_transaction_starter_(true),
      is_transaction_active_(other.is_transaction_active_) {
  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;
}

Storage::Accessor::~Accessor() {
  if (is_transaction_starter_ && is_transaction_active_) {
    Abort();
  }
}

VertexAccessor Storage::Accessor::CreateVertex() {
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The vertex must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Vertex accessor!";
  delta->prev.Set(&*it);
  return VertexAccessor{&*it, &transaction_};
}

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, &transaction_, view);
}

Result<bool> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == &transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(&transaction_, vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_ptr->deleted) return Result<bool>{false};

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty())
    return Result<bool>{Error::VERTEX_HAS_EDGES};

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<bool> Storage::Accessor::DetachDeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == &transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<EdgeTypeId, Vertex *, Edge *>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, Edge *>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(&transaction_, vertex_ptr))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_ptr->deleted) return Result<bool>{false};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, from_vertex, vertex_ptr, &transaction_};
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, vertex_ptr, to_vertex, &transaction_};
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  // We need to check again for serialization errors because we unlocked the
  // vertex. Some other transaction could have modified the vertex in the
  // meantime if we didn't have any edges to delete.

  if (!PrepareForWrite(&transaction_, vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  CHECK(!vertex_ptr->deleted) << "Invalid database state!";

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<EdgeAccessor> Storage::Accessor::CreateEdge(VertexAccessor *from,
                                                   VertexAccessor *to,
                                                   EdgeTypeId edge_type) {
  CHECK(from->transaction_ == to->transaction_)
      << "VertexAccessors must be from the same transaction when creating "
         "an edge!";
  CHECK(from->transaction_ == &transaction_)
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

  if (!PrepareForWrite(&transaction_, from_vertex))
    return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex))
      return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  auto gid = storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->edges_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Edge{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The edge must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Edge accessor!";
  auto edge = &*it;
  delta->prev.Set(&*it);

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(),
                     edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(),
                     edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  return Result<EdgeAccessor>{
      EdgeAccessor{edge, edge_type, from_vertex, to_vertex, &transaction_}};
}

Result<bool> Storage::Accessor::DeleteEdge(EdgeAccessor *edge) {
  CHECK(edge->transaction_ == &transaction_)
      << "EdgeAccessor must be from the same transaction as the storage "
         "accessor when deleting an edge!";
  auto edge_ptr = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::lock_guard<utils::SpinLock> guard(edge_ptr->lock);

  if (!PrepareForWrite(&transaction_, edge_ptr))
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

  if (!PrepareForWrite(&transaction_, from_vertex))
    return Result<bool>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex))
      return Result<bool>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  CreateAndLinkDelta(&transaction_, edge_ptr, Delta::RecreateObjectTag());
  edge_ptr->deleted = true;

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(),
                     edge_type, to_vertex, edge_ptr);
  {
    std::tuple<EdgeTypeId, Vertex *, Edge *> link{edge_type, to_vertex,
                                                  edge_ptr};
    auto it = std::find(from_vertex->out_edges.begin(),
                        from_vertex->out_edges.end(), link);
    CHECK(it != from_vertex->out_edges.end()) << "Invalid database state!";
    std::swap(*it, *from_vertex->out_edges.rbegin());
    from_vertex->out_edges.pop_back();
  }

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type,
                     from_vertex, edge_ptr);
  {
    std::tuple<EdgeTypeId, Vertex *, Edge *> link{edge_type, from_vertex,
                                                  edge_ptr};
    auto it =
        std::find(to_vertex->in_edges.begin(), to_vertex->in_edges.end(), link);
    CHECK(it != to_vertex->in_edges.end()) << "Invalid database state!";
    std::swap(*it, *to_vertex->in_edges.rbegin());
    to_vertex->in_edges.pop_back();
  }

  return Result<bool>{true};
}

const std::string &Storage::Accessor::LabelToName(LabelId label) {
  return storage_->name_id_mapper_.IdToName(label.AsUint());
}
const std::string &Storage::Accessor::PropertyToName(PropertyId property) {
  return storage_->name_id_mapper_.IdToName(property.AsUint());
}
const std::string &Storage::Accessor::EdgeTypeToName(EdgeTypeId edge_type) {
  return storage_->name_id_mapper_.IdToName(edge_type.AsUint());
}

LabelId Storage::Accessor::NameToLabel(const std::string &name) {
  return LabelId::FromUint(storage_->name_id_mapper_.NameToId(name));
}
PropertyId Storage::Accessor::NameToProperty(const std::string &name) {
  return PropertyId::FromUint(storage_->name_id_mapper_.NameToId(name));
}
EdgeTypeId Storage::Accessor::NameToEdgeType(const std::string &name) {
  return EdgeTypeId::FromUint(storage_->name_id_mapper_.NameToId(name));
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

void Storage::Accessor::Commit() {
  CHECK(is_transaction_active_) << "The transaction is already terminated!";
  CHECK(!transaction_.must_abort) << "The transaction can't be committed!";

  if (transaction_.deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    storage_->commit_log_.MarkFinished(transaction_.start_timestamp);
  } else {
    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;
    uint64_t commit_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp = storage_->timestamp_++;

      // Take committed_transactions lock while holding the engine lock to
      // make sure that committed transactions are sorted by the commit
      // timestamp in the list.
      storage_->committed_transactions_.WithLock(
          [&](auto &committed_transactions) {
            // TODO: release lock, and update all deltas to have a local copy
            // of the commit timestamp
            CHECK(transaction_.commit_timestamp != nullptr)
                << "Invalid database state!";
            transaction_.commit_timestamp->store(commit_timestamp,
                                                 std::memory_order_release);
            // Release engine lock because we don't have to hold it anymore and
            // emplace back could take a long time.
            engine_guard.unlock();
            committed_transactions.emplace_back(std::move(transaction_));
          });
    }

    storage_->commit_log_.MarkFinished(start_timestamp);
    storage_->commit_log_.MarkFinished(commit_timestamp);
  }
  is_transaction_active_ = false;
  if (storage_->gc_config_.type == StorageGcConfig::Type::ON_FINISH) {
    storage_->CollectGarbage();
  }
}

void Storage::Accessor::Abort() {
  CHECK(is_transaction_active_) << "The transaction is already terminated!";

  // We collect vertices and edges we've created here and then splice them into
  // `deleted_vertices_` and `deleted_edges_` lists, instead of adding them one
  // by one and acquiring lock every time.
  std::list<Gid> my_deleted_vertices;
  std::list<Gid> my_deleted_edges;

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto vertex = prev.vertex;
        std::lock_guard<utils::SpinLock> guard(vertex->lock);
        Delta *current = vertex->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) ==
                   transaction_.transaction_id) {
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
              std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(),
                                  vertex->in_edges.end(), link);
              CHECK(it == vertex->in_edges.end()) << "Invalid database state!";
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, Edge *> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(),
                                  vertex->out_edges.end(), link);
              CHECK(it == vertex->out_edges.end()) << "Invalid database state!";
              vertex->out_edges.push_back(link);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, Edge *> link{
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
              std::tuple<EdgeTypeId, Vertex *, Edge *> link{
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
              vertex->deleted = true;
              my_deleted_vertices.push_back(vertex->gid);
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
        std::lock_guard<utils::SpinLock> guard(edge->lock);
        Delta *current = edge->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) ==
                   transaction_.transaction_id) {
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
              edge->deleted = true;
              my_deleted_edges.push_back(edge->gid);
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
    // Take garbage_undo_buffers lock while holding the engine lock to make
    // sure that entries are sorted by mark timestamp in the list.
    storage_->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
      // Release engine lock because we don't have to hold it anymore and
      // emplace back could take a long time.
      engine_guard.unlock();
      garbage_undo_buffers.emplace_back(mark_timestamp,
                                        std::move(transaction_.deltas));
    });
    storage_->deleted_vertices_.WithLock([&](auto &deleted_vertices) {
      deleted_vertices.splice(deleted_vertices.begin(), my_deleted_vertices);
    });
    storage_->deleted_edges_.WithLock([&](auto &deleted_edges) {
      deleted_edges.splice(deleted_edges.begin(), my_deleted_edges);
    });
  }

  storage_->commit_log_.MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
  if (storage_->gc_config_.type == StorageGcConfig::Type::ON_FINISH) {
    storage_->CollectGarbage();
  }
}

Storage::Accessor Storage::Access() {
  // We acquire the transaction engine lock here because we access (and
  // modify) the transaction engine variables (`transaction_id` and
  // `timestamp`) below.
  uint64_t transaction_id;
  uint64_t start_timestamp;
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    transaction_id = transaction_id_++;
    start_timestamp = timestamp_++;
  }
  return Accessor{this, transaction_id, start_timestamp};
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
  // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
  // list immediately, because we would have to repeatedly take
  // garbage_undo_buffers lock.
  std::list<std::pair<uint64_t, std::list<Delta>>> unlinked_undo_buffers;

  // We will free only vertices deleted up until now in this GC cycle.
  // Otherwise, GC cycle might be prolonged by aborted transactions adding new
  // edges and vertices to the lists over and over again.
  std::list<Gid> current_deleted_edges;
  std::list<Gid> current_deleted_vertices;
  deleted_vertices_->swap(current_deleted_vertices);
  deleted_edges_->swap(current_deleted_edges);

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

    if (transaction->commit_timestamp->load(std::memory_order_acquire) >=
        oldest_active_start_timestamp) {
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
              current_deleted_vertices.push_back(vertex->gid);
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
              current_deleted_edges.push_back(edge->gid);
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

    committed_transactions_.WithLock([&](auto &committed_transactions) {
      unlinked_undo_buffers.emplace_back(0, std::move(transaction->deltas));
      committed_transactions.pop_front();
    });
  }

  {
    uint64_t mark_timestamp;
    std::unique_lock<utils::SpinLock> guard(engine_lock_);
    mark_timestamp = timestamp_;
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
      garbage_undo_buffers.splice(garbage_undo_buffers.end(),
                                  unlinked_undo_buffers);
    });
  }

  while (true) {
    auto garbage_undo_buffers_ptr = garbage_undo_buffers_.Lock();
    if (garbage_undo_buffers_ptr->empty() ||
        garbage_undo_buffers_ptr->front().first >=
            oldest_active_start_timestamp) {
      break;
    }
    garbage_undo_buffers_ptr->pop_front();
  }

  {
    auto vertex_acc = vertices_.access();
    for (auto vertex : current_deleted_vertices) {
      CHECK(vertex_acc.remove(vertex)) << "Invalid database state!";
    }
  }
  {
    auto edge_acc = edges_.access();
    for (auto edge : current_deleted_edges) {
      CHECK(edge_acc.remove(edge)) << "Invalid database state!";
    }
  }
}

}  // namespace storage
