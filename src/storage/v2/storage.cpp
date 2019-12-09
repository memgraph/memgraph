#include "storage/v2/storage.hpp"

#include <algorithm>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "storage/v2/mvcc.hpp"
#include "utils/stat.hpp"

namespace storage {

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::Iterator it,
                            utils::SkipList<Vertex>::Iterator end,
                            std::optional<VertexAccessor> *vertex,
                            Transaction *tx, View view, Indices *indices,
                            Config::Items config) {
  while (it != end) {
    *vertex = VertexAccessor::Create(&*it, tx, indices, config, view);
    if (!*vertex) {
      ++it;
      continue;
    }
    break;
  }
  return it;
}

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self,
                                        utils::SkipList<Vertex>::Iterator it)
    : self_(self),
      it_(AdvanceToVisibleVertex(it, self->vertices_accessor_.end(),
                                 &self->vertex_, self->transaction_,
                                 self->view_, self->indices_, self->config_)) {}

VertexAccessor AllVerticesIterable::Iterator::operator*() const {
  return *self_->vertex_;
}

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(),
                               &self_->vertex_, self_->transaction_,
                               self_->view_, self_->indices_, self_->config_);
  return *this;
}

VerticesIterable::VerticesIterable(AllVerticesIterable vertices)
    : type_(Type::ALL) {
  new (&all_vertices_) AllVerticesIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(LabelIndex::Iterable vertices)
    : type_(Type::BY_LABEL) {
  new (&vertices_by_label_) LabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(LabelPropertyIndex::Iterable vertices)
    : type_(Type::BY_LABEL_PROPERTY) {
  new (&vertices_by_label_property_)
      LabelPropertyIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(VerticesIterable &&other) noexcept
    : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL:
      new (&vertices_by_label_)
          LabelIndex::Iterable(std::move(other.vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&vertices_by_label_property_) LabelPropertyIndex::Iterable(
          std::move(other.vertices_by_label_property_));
      break;
  }
}

VerticesIterable &VerticesIterable::operator=(
    VerticesIterable &&other) noexcept {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL:
      vertices_by_label_.LabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY:
      vertices_by_label_property_.LabelPropertyIndex::Iterable::~Iterable();
      break;
  }
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL:
      new (&vertices_by_label_)
          LabelIndex::Iterable(std::move(other.vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&vertices_by_label_property_) LabelPropertyIndex::Iterable(
          std::move(other.vertices_by_label_property_));
      break;
  }
  return *this;
}

VerticesIterable::~VerticesIterable() {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL:
      vertices_by_label_.LabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY:
      vertices_by_label_property_.LabelPropertyIndex::Iterable::~Iterable();
      break;
  }
}

VerticesIterable::Iterator VerticesIterable::begin() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.begin());
    case Type::BY_LABEL:
      return Iterator(vertices_by_label_.begin());
    case Type::BY_LABEL_PROPERTY:
      return Iterator(vertices_by_label_property_.begin());
  }
}

VerticesIterable::Iterator VerticesIterable::end() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.end());
    case Type::BY_LABEL:
      return Iterator(vertices_by_label_.end());
    case Type::BY_LABEL_PROPERTY:
      return Iterator(vertices_by_label_property_.end());
  }
}

VerticesIterable::Iterator::Iterator(AllVerticesIterable::Iterator it)
    : type_(Type::ALL) {
  new (&all_it_) AllVerticesIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(LabelIndex::Iterable::Iterator it)
    : type_(Type::BY_LABEL) {
  new (&by_label_it_) LabelIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(LabelPropertyIndex::Iterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY) {
  new (&by_label_property_it_)
      LabelPropertyIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(const VerticesIterable::Iterator &other)
    : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_)
          LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(
    const VerticesIterable::Iterator &other) {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_)
          LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
  return *this;
}

VerticesIterable::Iterator::Iterator(
    VerticesIterable::Iterator &&other) noexcept
    : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
      break;
    case Type::BY_LABEL:
      new (&by_label_it_)
          LabelIndex::Iterable::Iterator(std::move(other.by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(
          std::move(other.by_label_property_it_));
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(
    VerticesIterable::Iterator &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
      break;
    case Type::BY_LABEL:
      new (&by_label_it_)
          LabelIndex::Iterable::Iterator(std::move(other.by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(
          std::move(other.by_label_property_it_));
      break;
  }
  return *this;
}

VerticesIterable::Iterator::~Iterator() { Destroy(); }

void VerticesIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::ALL:
      all_it_.AllVerticesIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL:
      by_label_it_.LabelIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY:
      by_label_property_it_.LabelPropertyIndex::Iterable::Iterator::~Iterator();
      break;
  }
}

VertexAccessor VerticesIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::ALL:
      return *all_it_;
    case Type::BY_LABEL:
      return *by_label_it_;
    case Type::BY_LABEL_PROPERTY:
      return *by_label_property_it_;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator++() {
  switch (type_) {
    case Type::ALL:
      ++all_it_;
      break;
    case Type::BY_LABEL:
      ++by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY:
      ++by_label_property_it_;
      break;
  }
  return *this;
}

bool VerticesIterable::Iterator::operator==(const Iterator &other) const {
  switch (type_) {
    case Type::ALL:
      return all_it_ == other.all_it_;
    case Type::BY_LABEL:
      return by_label_it_ == other.by_label_it_;
    case Type::BY_LABEL_PROPERTY:
      return by_label_property_it_ == other.by_label_property_it_;
  }
}

Storage::Storage(Config config)
    : indices_(config.items),
      config_(config),
      durability_(config.durability, &vertices_, &edges_, &name_id_mapper_,
                  &edge_count_, &indices_, &constraints_, config.items) {
  auto info = durability_.Initialize([this](auto callback) {
    // Take master RW lock (for reading).
    std::shared_lock<utils::RWLock> storage_guard(main_lock_);

    // Create the transaction used to create the snapshot.
    auto transaction = CreateTransaction();

    // Create snapshot.
    callback(&transaction);

    // Finalize snapshot transaction.
    commit_log_.MarkFinished(transaction.start_timestamp);
  });
  if (info) {
    vertex_id_ = info->next_vertex_id;
    edge_id_ = info->next_edge_id;
    timestamp_ = std::max(timestamp_, info->next_timestamp);
  }
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Run("Storage GC", config_.gc.interval,
                   [this] { this->CollectGarbage(); });
  }
}

Storage::~Storage() {
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Stop();
  }
  durability_.Finalize();
}

Storage::Accessor::Accessor(Storage *storage)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction()),
      is_transaction_active_(true),
      config_(storage->config_.items) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      transaction_(std::move(other.transaction_)),
      is_transaction_active_(other.is_transaction_active_),
      config_(other.config_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
}

Storage::Accessor::~Accessor() {
  if (is_transaction_active_) {
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
  return VertexAccessor(&*it, &transaction_, &storage_->indices_, config_);
}

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, &transaction_, &storage_->indices_,
                                config_, view);
}

Result<bool> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == &transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(&transaction_, vertex_ptr))
    return Error::SERIALIZATION_ERROR;

  if (vertex_ptr->deleted) return false;

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty())
    return Error::VERTEX_HAS_EDGES;

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return true;
}

Result<bool> Storage::Accessor::DetachDeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == &transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(&transaction_, vertex_ptr))
      return Error::SERIALIZATION_ERROR;

    if (vertex_ptr->deleted) return false;

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, from_vertex, vertex_ptr, &transaction_,
                   &storage_->indices_, config_);
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      CHECK(ret.GetError() == Error::SERIALIZATION_ERROR)
          << "Invalid database state!";
      return ret;
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, vertex_ptr, to_vertex, &transaction_,
                   &storage_->indices_, config_);
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
    return Error::SERIALIZATION_ERROR;

  CHECK(!vertex_ptr->deleted) << "Invalid database state!";

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return true;
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
    return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex))
      return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  auto gid = storage::Gid::FromUint(
      storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = storage_->edges_.access();
    auto delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    CHECK(inserted) << "The edge must be inserted here!";
    CHECK(it != acc.end()) << "Invalid Edge accessor!";
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(),
                     edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(),
                     edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_,
                      &storage_->indices_, config_);
}

Result<bool> Storage::Accessor::DeleteEdge(EdgeAccessor *edge) {
  CHECK(edge->transaction_ == &transaction_)
      << "EdgeAccessor must be from the same transaction as the storage "
         "accessor when deleting an edge!";
  auto edge_ref = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::unique_lock<utils::SpinLock> guard;
  if (config_.properties_on_edges) {
    auto edge_ptr = edge_ref.ptr;
    guard = std::unique_lock<utils::SpinLock>(edge_ptr->lock);

    if (!PrepareForWrite(&transaction_, edge_ptr))
      return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return false;
  }

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
    return Error::SERIALIZATION_ERROR;
  CHECK(!from_vertex->deleted) << "Invalid database state!";

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex))
      return Error::SERIALIZATION_ERROR;
    CHECK(!to_vertex->deleted) << "Invalid database state!";
  }

  auto delete_edge_from_storage = [&edge_type, &edge_ref, this](auto *vertex,
                                                                auto *edges) {
    std::tuple<EdgeTypeId, Vertex *, EdgeRef> link(edge_type, vertex, edge_ref);
    auto it = std::find(edges->begin(), edges->end(), link);
    if (config_.properties_on_edges) {
      CHECK(it != edges->end()) << "Invalid database state!";
    } else if (it == edges->end()) {
      return false;
    }
    std::swap(*it, *edges->rbegin());
    edges->pop_back();
    return true;
  };

  auto op1 = delete_edge_from_storage(to_vertex, &from_vertex->out_edges);
  auto op2 = delete_edge_from_storage(from_vertex, &to_vertex->in_edges);

  if (config_.properties_on_edges) {
    CHECK((op1 && op2)) << "Invalid database state!";
  } else {
    CHECK((op1 && op2) || (!op1 && !op2)) << "Invalid database state!";
    if (!op1 && !op2) {
      // The edge is already deleted.
      return false;
    }
  }

  if (config_.properties_on_edges) {
    auto edge_ptr = edge_ref.ptr;
    CreateAndLinkDelta(&transaction_, edge_ptr, Delta::RecreateObjectTag());
    edge_ptr->deleted = true;
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(),
                     edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type,
                     from_vertex, edge_ref);

  // Decrement edge count.
  storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);

  return true;
}

const std::string &Storage::Accessor::LabelToName(LabelId label) const {
  return storage_->LabelToName(label);
}

const std::string &Storage::Accessor::PropertyToName(
    PropertyId property) const {
  return storage_->PropertyToName(property);
}

const std::string &Storage::Accessor::EdgeTypeToName(
    EdgeTypeId edge_type) const {
  return storage_->EdgeTypeToName(edge_type);
}

LabelId Storage::Accessor::NameToLabel(const std::string &name) {
  return storage_->NameToLabel(name);
}

PropertyId Storage::Accessor::NameToProperty(const std::string &name) {
  return storage_->NameToProperty(name);
}

EdgeTypeId Storage::Accessor::NameToEdgeType(const std::string &name) {
  return storage_->NameToEdgeType(name);
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

utils::BasicResult<ExistenceConstraintViolation, void>
Storage::Accessor::Commit() {
  CHECK(is_transaction_active_) << "The transaction is already terminated!";
  CHECK(!transaction_.must_abort) << "The transaction can't be committed!";

  if (transaction_.deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    storage_->commit_log_.MarkFinished(transaction_.start_timestamp);
  } else {
    // Validate that existence constraints are satisfied for all modified
    // vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      auto validation_result =
          ValidateExistenceConstraints(prev.vertex, &storage_->constraints_);
      if (validation_result) {
        Abort();
        return *validation_result;
      }
    }

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;
    uint64_t commit_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp = storage_->timestamp_++;

      // Write transaction to WAL while holding the engine lock to make sure
      // that committed transactions are sorted by the commit timestamp in the
      // WAL files. We supply the new commit timestamp to the function so that
      // it knows what will be the final commit timestamp. The WAL must be
      // written before actually committing the transaction (before setting the
      // commit timestamp) so that no other transaction can see the
      // modifications before they are written to disk.
      storage_->durability_.AppendToWal(transaction_, commit_timestamp);

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

  return {};
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
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(),
                                  vertex->in_edges.end(), link);
              CHECK(it == vertex->in_edges.end()) << "Invalid database state!";
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(),
                                  vertex->out_edges.end(), link);
              CHECK(it == vertex->out_edges.end()) << "Invalid database state!";
              vertex->out_edges.push_back(link);
              // Increment edge count. We only increment the count here because
              // the information in `ADD_IN_EDGE` and `Edge/RECREATE_OBJECT` is
              // redundant. Also, `Edge/RECREATE_OBJECT` isn't available when
              // edge properties are disabled.
              storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
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
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                  current->vertex_edge.edge_type, current->vertex_edge.vertex,
                  current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(),
                                  vertex->out_edges.end(), link);
              CHECK(it != vertex->out_edges.end()) << "Invalid database state!";
              std::swap(*it, *vertex->out_edges.rbegin());
              vertex->out_edges.pop_back();
              // Decrement edge count. We only decrement the count here because
              // the information in `REMOVE_IN_EDGE` and `Edge/DELETE_OBJECT` is
              // redundant. Also, `Edge/DELETE_OBJECT` isn't available when edge
              // properties are disabled.
              storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);
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
}

const std::string &Storage::LabelToName(LabelId label) const {
  return name_id_mapper_.IdToName(label.AsUint());
}

const std::string &Storage::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

const std::string &Storage::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

LabelId Storage::NameToLabel(const std::string &name) {
  return LabelId::FromUint(name_id_mapper_.NameToId(name));
}

PropertyId Storage::NameToProperty(const std::string &name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

EdgeTypeId Storage::NameToEdgeType(const std::string &name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

bool Storage::CreateIndex(LabelId label) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_index.CreateIndex(label, vertices_.access()))
    return false;
  // Here it is safe to use `timestamp_` as the final commit timestamp of this
  // operation even though this operation isn't transactional. The `timestamp_`
  // variable holds the next timestamp that will be used. Because the above
  // `storage_guard` ensures that no transactions are currently active, the
  // value of `timestamp_` is guaranteed to be used as a start timestamp for the
  // next regular transaction after this operation. This prevents collisions of
  // commit timestamps between non-transactional operations and transactional
  // operations.
  durability_.AppendToWal(StorageGlobalOperation::LABEL_INDEX_CREATE, label,
                          std::nullopt, timestamp_);
  return true;
}

bool Storage::CreateIndex(LabelId label, PropertyId property) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index.CreateIndex(label, property,
                                                 vertices_.access()))
    return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  durability_.AppendToWal(StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE,
                          label, property, timestamp_);
  return true;
}

bool Storage::DropIndex(LabelId label) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_index.DropIndex(label)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  durability_.AppendToWal(StorageGlobalOperation::LABEL_INDEX_DROP, label,
                          std::nullopt, timestamp_);
  return true;
}

bool Storage::DropIndex(LabelId label, PropertyId property) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index.DropIndex(label, property)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  durability_.AppendToWal(StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP,
                          label, property, timestamp_);
  return true;
}

IndicesInfo Storage::ListAllIndices() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {indices_.label_index.ListIndices(),
          indices_.label_property_index.ListIndices()};
}

utils::BasicResult<ExistenceConstraintViolation, bool>
Storage::CreateExistenceConstraint(LabelId label, PropertyId property) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = ::storage::CreateExistenceConstraint(&constraints_, label,
                                                  property, vertices_.access());
  if (ret.HasError() || !ret.GetValue()) return ret;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  durability_.AppendToWal(StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE,
                          label, property, timestamp_);
  return true;
}

bool Storage::DropExistenceConstraint(LabelId label, PropertyId property) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!::storage::DropExistenceConstraint(&constraints_, label, property))
    return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  durability_.AppendToWal(StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP,
                          label, property, timestamp_);
  return true;
}

ConstraintsInfo Storage::ListAllConstraints() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {ListExistenceConstraints(constraints_)};
}

StorageInfo Storage::GetInfo() const {
  auto vertex_count = vertices_.size();
  auto edge_count = edge_count_.load(std::memory_order_acquire);
  double average_degree = 0.0;
  if (vertex_count) {
    average_degree = 2.0 * static_cast<double>(edge_count) / vertex_count;
  }
  return {vertex_count, edge_count, average_degree, utils::GetMemoryUsage(),
          utils::GetDirDiskUsage(config_.durability.storage_directory)};
}

VerticesIterable Storage::Accessor::Vertices(LabelId label, View view) {
  return VerticesIterable(
      storage_->indices_.label_index.Vertices(label, view, &transaction_));
}

VerticesIterable Storage::Accessor::Vertices(LabelId label, PropertyId property,
                                             View view) {
  return VerticesIterable(storage_->indices_.label_property_index.Vertices(
      label, property, std::nullopt, std::nullopt, view, &transaction_));
}

VerticesIterable Storage::Accessor::Vertices(LabelId label, PropertyId property,
                                             const PropertyValue &value,
                                             View view) {
  return VerticesIterable(storage_->indices_.label_property_index.Vertices(
      label, property, utils::MakeBoundInclusive(value),
      utils::MakeBoundInclusive(value), view, &transaction_));
}

VerticesIterable Storage::Accessor::Vertices(
    LabelId label, PropertyId property,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  return VerticesIterable(storage_->indices_.label_property_index.Vertices(
      label, property, lower_bound, upper_bound, view, &transaction_));
}

Transaction Storage::CreateTransaction() {
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
  return {transaction_id, start_timestamp};
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
  bool run_index_cleanup =
      !committed_transactions_->empty() || !garbage_undo_buffers_->empty();

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

    auto commit_timestamp =
        transaction->commit_timestamp->load(std::memory_order_acquire);
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
            if (prev.delta->timestamp->load(std::memory_order_release) ==
                commit_timestamp) {
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
                  guard =
                      std::unique_lock<utils::SpinLock>(parent.vertex->lock);
                  break;
                case PreviousPtr::Type::EDGE:
                  guard = std::unique_lock<utils::SpinLock>(parent.edge->lock);
                  break;
                case PreviousPtr::Type::DELTA:
                  LOG(FATAL) << "Invalid database state!";
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
      garbage_undo_buffers.splice(garbage_undo_buffers.end(),
                                  unlinked_undo_buffers);
    });
    for (auto vertex : current_deleted_vertices) {
      garbage_vertices_.emplace_back(mark_timestamp, vertex);
    }
  }

  while (true) {
    auto garbage_undo_buffers_ptr = garbage_undo_buffers_.Lock();
    if (garbage_undo_buffers_ptr->empty() ||
        garbage_undo_buffers_ptr->front().first >
            oldest_active_start_timestamp) {
      break;
    }
    garbage_undo_buffers_ptr->pop_front();
  }

  {
    auto vertex_acc = vertices_.access();
    while (!garbage_vertices_.empty() &&
           garbage_vertices_.front().first < oldest_active_start_timestamp) {
      CHECK(vertex_acc.remove(garbage_vertices_.front().second))
          << "Invalid database state!";
      garbage_vertices_.pop_front();
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
