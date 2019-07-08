#include "storage/v2/storage.hpp"

#include <memory>

#include <glog/logging.h>

#include "storage/v2/mvcc.hpp"

namespace storage {

Storage::Accessor::Accessor(Storage *storage)
    : storage_(storage), is_transaction_starter_(true) {
  // We acquire the storage lock here because we access (and modify) the
  // transaction engine variables (`transaction_id` and `timestamp`) below.
  std::lock_guard<utils::SpinLock> guard(storage_->lock_);
  auto acc = storage_->transactions_.access();
  auto [it, inserted] = acc.insert(
      Transaction{storage_->transaction_id_++, storage_->timestamp_++});
  CHECK(inserted) << "The Transaction must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Transaction iterator!";
  transaction_ = &*it;
}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      transaction_(other.transaction_),
      is_transaction_starter_(true) {
  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;
}

// This operator isn't `noexcept` because the `Abort` function isn't
// `noexcept`.
Storage::Accessor &Storage::Accessor::operator=(Accessor &&other) {
  if (this == &other) return *this;

  if (is_transaction_starter_ && transaction_->is_active) {
    Abort();
  }

  storage_ = other.storage_;
  transaction_ = other.transaction_;
  is_transaction_starter_ = true;

  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;

  return *this;
}

Storage::Accessor::~Accessor() {
  if (is_transaction_starter_ && transaction_->is_active) {
    Abort();
  }
}

VertexAccessor Storage::Accessor::CreateVertex() {
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The vertex must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Vertex accessor!";
  transaction_->modified_vertices.push_back(&*it);
  return VertexAccessor{&*it, transaction_};
}

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, transaction_, view);
}

Result<bool> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(transaction_, vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_ptr->deleted) return Result<bool>{false};

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty())
    return Result<bool>{Error::VERTEX_HAS_EDGES};

  CreateAndLinkDelta(transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<bool> Storage::Accessor::DetachDeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> in_edges;
  std::vector<std::tuple<uint64_t, Vertex *, Edge *>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(transaction_, vertex_ptr))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_ptr->deleted) return Result<bool>{false};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, from_vertex, vertex_ptr, transaction_};
    auto ret = DeleteEdge(&e);
    if (ret.IsError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e{edge, edge_type, vertex_ptr, to_vertex, transaction_};
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

  if (!PrepareForWrite(transaction_, vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  CHECK(!vertex_ptr->deleted) << "Invalid database state!";

  CreateAndLinkDelta(transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

Result<EdgeAccessor> Storage::Accessor::CreateEdge(VertexAccessor *from,
                                                   VertexAccessor *to,
                                                   uint64_t edge_type) {
  CHECK(from->transaction_ == to->transaction_)
      << "VertexAccessors must be from the same transaction when creating "
         "an edge!";
  CHECK(from->transaction_ == transaction_)
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

  if (!PrepareForWrite(transaction_, from_vertex))
    return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_, to_vertex))
      return Result<EdgeAccessor>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  auto gid = storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->edges_.access();
  auto delta = CreateDeleteObjectDelta(transaction_);
  auto [it, inserted] = acc.insert(Edge{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The edge must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Edge accessor!";
  auto edge = &*it;
  transaction_->modified_edges.push_back(edge);

  CreateAndLinkDelta(transaction_, from_vertex, Delta::RemoveOutEdgeTag(),
                     edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(transaction_, to_vertex, Delta::RemoveInEdgeTag(),
                     edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  return Result<EdgeAccessor>{
      EdgeAccessor{edge, edge_type, from_vertex, to_vertex, transaction_}};
}

Result<bool> Storage::Accessor::DeleteEdge(EdgeAccessor *edge) {
  CHECK(edge->transaction_ == transaction_)
      << "EdgeAccessor must be from the same transaction as the storage "
         "accessor when deleting an edge!";
  auto edge_ptr = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::lock_guard<utils::SpinLock> guard(edge_ptr->lock);

  if (!PrepareForWrite(transaction_, edge_ptr))
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

  if (!PrepareForWrite(transaction_, from_vertex))
    return Result<bool>{Error::SERIALIZATION_ERROR};
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_, to_vertex))
      return Result<bool>{Error::SERIALIZATION_ERROR};
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  CreateAndLinkDelta(transaction_, edge_ptr, Delta::RecreateObjectTag());
  edge_ptr->deleted = true;

  CreateAndLinkDelta(transaction_, from_vertex, Delta::AddOutEdgeTag(),
                     edge_type, to_vertex, edge_ptr);
  {
    std::tuple<uint64_t, Vertex *, Edge *> link{edge_type, to_vertex, edge_ptr};
    auto it = std::find(from_vertex->out_edges.begin(),
                        from_vertex->out_edges.end(), link);
    CHECK(it != from_vertex->out_edges.end()) << "Invalid database state!";
    std::swap(*it, *from_vertex->out_edges.rbegin());
    from_vertex->out_edges.pop_back();
  }

  CreateAndLinkDelta(transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type,
                     from_vertex, edge_ptr);
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
  CHECK(!transaction_->must_abort) << "The transaction can't be committed!";
  CHECK(transaction_->is_active) << "The transaction is already terminated!";
  if (transaction_->deltas.empty()) {
    transaction_->commit_timestamp.store(transaction_->start_timestamp,
                                         std::memory_order_release);
  } else {
    std::lock_guard<utils::SpinLock> guard(storage_->lock_);
    transaction_->commit_timestamp.store(storage_->timestamp_++,
                                         std::memory_order_release);
    // TODO: release lock, and update all deltas to have an in-memory copy
    // of the commit id
  }
  transaction_->is_active = false;
}

void Storage::Accessor::Abort() {
  CHECK(transaction_->is_active) << "The transaction is already terminated!";
  for (Vertex *vertex : transaction_->modified_vertices) {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
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
          auto it =
              std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
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
          auto it =
              std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
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
  }
  for (Edge *edge : transaction_->modified_edges) {
    std::lock_guard<utils::SpinLock> guard(edge->lock);
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
  }
  transaction_->is_active = false;
}

}  // namespace storage
