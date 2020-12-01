#include "storage/v2/storage.hpp"

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <variant>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "io/network/endpoint.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "utils/file.hpp"
#include "utils/rw_lock.hpp"
#include "utils/spin_lock.hpp"
#include "utils/stat.hpp"
#include "utils/uuid.hpp"

#ifdef MG_ENTERPRISE
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/rpc.hpp"
#endif

#ifdef MG_ENTERPRISE
DEFINE_bool(main, false, "Set to true to be the main");
DEFINE_bool(replica, false, "Set to true to be the replica");
DEFINE_bool(async_replica, false, "Set to true to be the replica");
#endif

namespace storage {

namespace {
constexpr uint16_t kEpochHistoryRetention = 1000;
}  // namespace

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::Iterator it,
                            utils::SkipList<Vertex>::Iterator end,
                            std::optional<VertexAccessor> *vertex,
                            Transaction *tx, View view, Indices *indices,
                            Constraints *constraints, Config::Items config) {
  while (it != end) {
    *vertex =
        VertexAccessor::Create(&*it, tx, indices, constraints, config, view);
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
                                 self->view_, self->indices_,
                                 self_->constraints_, self->config_)) {}

VertexAccessor AllVerticesIterable::Iterator::operator*() const {
  return *self_->vertex_;
}

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(),
                               &self_->vertex_, self_->transaction_,
                               self_->view_, self_->indices_,
                               self_->constraints_, self_->config_);
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
    : indices_(&constraints_, config.items),
      config_(config),
      snapshot_directory_(config_.durability.storage_directory /
                          durability::kSnapshotDirectory),
      wal_directory_(config_.durability.storage_directory /
                     durability::kWalDirectory),
      lock_file_path_(config_.durability.storage_directory /
                      durability::kLockFile),
      uuid_(utils::GenerateUUID()),
      epoch_id_(utils::GenerateUUID()) {
  if (config_.durability.snapshot_wal_mode !=
          Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit ||
      config_.durability.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(
        config_.durability.storage_directory);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_.Open(lock_file_path_,
                           utils::OutputFile::Mode::OVERWRITE_EXISTING);
    CHECK(lock_file_handle_.AcquireLock())
        << "Couldn't acquire lock on the storage directory "
        << config_.durability.storage_directory
        << "!\nAnother Memgraph process is currently running with the same "
           "storage directory, please stop it first before starting this "
           "process!";
  }
  if (config_.durability.recover_on_startup) {
    auto info = durability::RecoverData(
        snapshot_directory_, wal_directory_, &uuid_, &epoch_id_,
        &epoch_history_, &vertices_, &edges_, &edge_count_, &name_id_mapper_,
        &indices_, &constraints_, config_.items, &wal_seq_num_);
    if (info) {
      vertex_id_ = info->next_vertex_id;
      edge_id_ = info->next_edge_id;
      timestamp_ = std::max(timestamp_, info->next_timestamp);
#if MG_ENTERPRISE
      // After we finished the recovery, the info->next_timestamp will
      // basically be
      // `std::max(latest_snapshot.start_timestamp + 1, latest_wal.to_timestamp
      // + 1)` So the last commited transaction is one before that.
      last_commit_timestamp_ = timestamp_ - 1;
#endif
    }
  } else if (config_.durability.snapshot_wal_mode !=
                 Config::Durability::SnapshotWalMode::DISABLED ||
             config_.durability.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root =
        config_.durability.storage_directory / durability::kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(snapshot_directory_, durability::kSnapshotDirectory,
                          "snapshot"),
          std::make_tuple(wal_directory_, durability::kWalDirectory, "WAL")}) {
      if (!utils::DirExists(path)) continue;
      auto backup_curr = backup_root / dirname;
      std::error_code error_code;
      for (const auto &item :
           std::filesystem::directory_iterator(path, error_code)) {
        utils::EnsureDirOrDie(backup_root);
        utils::EnsureDirOrDie(backup_curr);
        std::error_code item_error_code;
        std::filesystem::rename(
            item.path(), backup_curr / item.path().filename(), item_error_code);
        CHECK(!item_error_code)
            << "Couldn't move " << what << " file " << item.path()
            << " because of: " << item_error_code.message();
        files_moved = true;
      }
      CHECK(!error_code) << "Couldn't backup " << what
                         << " files because of: " << error_code.message();
    }
    LOG_IF(WARNING, files_moved)
        << "Since Memgraph was not supposed to recover on startup and "
           "durability is enabled, your current durability files will likely "
           "be overridden. To prevent important data loss, Memgraph has stored "
           "those files into a .backup directory inside the storage directory.";
  }
  if (config_.durability.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval,
                         [this] { this->CreateSnapshot(); });
  }
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Run("Storage GC", config_.gc.interval,
                   [this] { this->CollectGarbage(); });
  }

#ifdef MG_ENTERPRISE
  // For testing purposes until we can define the instance type from
  // a query.
  if (FLAGS_main) {
    RegisterReplica("REPLICA_SYNC", io::network::Endpoint{"127.0.0.1", 10000});
    RegisterReplica("REPLICA_ASYNC", io::network::Endpoint{"127.0.0.1", 10002});
  } else if (FLAGS_replica) {
    SetReplicationRole<ReplicationRole::REPLICA>(
        io::network::Endpoint{"127.0.0.1", 10000});
  } else if (FLAGS_async_replica) {
    SetReplicationRole<ReplicationRole::REPLICA>(
        io::network::Endpoint{"127.0.0.1", 10002});
  }
#endif
}

Storage::~Storage() {
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Stop();
  }
#ifdef MG_ENTERPRISE
  {
    // Clear replication data
    replication_server_.reset();
    replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
  }
#endif
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
  }
  if (config_.durability.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Stop();
  }
  if (config_.durability.snapshot_on_exit) {
    CreateSnapshot();
  }
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
  return VertexAccessor(&*it, &transaction_, &storage_->indices_,
                        &storage_->constraints_, config_);
}

#ifdef MG_ENTERPRISE
VertexAccessor Storage::Accessor::CreateVertex(storage::Gid gid) {
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  storage_->vertex_id_.store(
      std::max(storage_->vertex_id_.load(std::memory_order_acquire),
               gid.AsUint() + 1),
      std::memory_order_release);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  CHECK(inserted) << "The vertex must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Vertex accessor!";
  delta->prev.Set(&*it);
  return VertexAccessor(&*it, &transaction_, &storage_->indices_,
                        &storage_->constraints_, config_);
}
#endif

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, &transaction_, &storage_->indices_,
                                &storage_->constraints_, config_, view);
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
                   &storage_->indices_, &storage_->constraints_, config_);
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
                   &storage_->indices_, &storage_->constraints_, config_);
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
                      &storage_->indices_, &storage_->constraints_, config_);
}

#ifdef MG_ENTERPRISE
Result<EdgeAccessor> Storage::Accessor::CreateEdge(VertexAccessor *from,
                                                   VertexAccessor *to,
                                                   EdgeTypeId edge_type,
                                                   storage::Gid gid) {
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

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  storage_->edge_id_.store(
      std::max(storage_->edge_id_.load(std::memory_order_acquire),
               gid.AsUint() + 1),
      std::memory_order_release);

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
                      &storage_->indices_, &storage_->constraints_, config_);
}
#endif

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

LabelId Storage::Accessor::NameToLabel(const std::string_view &name) {
  return storage_->NameToLabel(name);
}

PropertyId Storage::Accessor::NameToProperty(const std::string_view &name) {
  return storage_->NameToProperty(name);
}

EdgeTypeId Storage::Accessor::NameToEdgeType(const std::string_view &name) {
  return storage_->NameToEdgeType(name);
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

utils::BasicResult<ConstraintViolation, void> Storage::Accessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
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
          ValidateExistenceConstraints(*prev.vertex, storage_->constraints_);
      if (validation_result) {
        Abort();
        return *validation_result;
      }
    }

    // Result of validating the vertex against unqiue constraints. It has to be
    // declared outside of the critical section scope because its value is
    // tested for Abort call which has to be done out of the scope.
    std::optional<ConstraintViolation> unique_constraint_violation;

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;
    uint64_t commit_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp = storage_->CommitTimestamp(desired_commit_timestamp);

      // Before committing and validating vertices against unique constraints,
      // we have to update unique constraints with the vertices that are going
      // to be validated/committed.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }
        storage_->constraints_.unique_constraints.UpdateBeforeCommit(
            prev.vertex, transaction_);
      }

      // Validate that unique constraints are satisfied for all modified
      // vertices.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }

        // No need to take any locks here because we modified this vertex and no
        // one else can touch it until we commit.
        unique_constraint_violation =
            storage_->constraints_.unique_constraints.Validate(
                *prev.vertex, transaction_, commit_timestamp);
        if (unique_constraint_violation) {
          break;
        }
      }

      if (!unique_constraint_violation) {
        // Write transaction to WAL while holding the engine lock to make sure
        // that committed transactions are sorted by the commit timestamp in the
        // WAL files. We supply the new commit timestamp to the function so that
        // it knows what will be the final commit timestamp. The WAL must be
        // written before actually committing the transaction (before setting
        // the commit timestamp) so that no other transaction can see the
        // modifications before they are written to disk.
#ifdef MG_ENTERPRISE
        // Replica can log only the write transaction received from Main
        // so the Wal files are consistent
        if (storage_->replication_role_ == ReplicationRole::MAIN ||
            desired_commit_timestamp.has_value()) {
          storage_->AppendToWal(transaction_, commit_timestamp);
        }
#else

        storage_->AppendToWal(transaction_, commit_timestamp);
#endif

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
#ifdef MG_ENTERPRISE
              // Replica can only update the last commit timestamp with
              // the commits received from main.
              if (storage_->replication_role_ == ReplicationRole::MAIN ||
                  desired_commit_timestamp.has_value()) {
                // Update the last commit timestamp
                storage_->last_commit_timestamp_.store(commit_timestamp);
              }
#endif
              // Release engine lock because we don't have to hold it anymore
              // and emplace back could take a long time.
              engine_guard.unlock();
              committed_transactions.emplace_back(std::move(transaction_));
            });

        storage_->commit_log_.MarkFinished(start_timestamp);
        storage_->commit_log_.MarkFinished(commit_timestamp);
      }
    }

    if (unique_constraint_violation) {
      Abort();
      storage_->commit_log_.MarkFinished(commit_timestamp);
      return *unique_constraint_violation;
    }
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
              vertex->properties.SetProperty(current->property.key,
                                             current->property.value);
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
              edge->properties.SetProperty(current->property.key,
                                           current->property.value);
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

LabelId Storage::NameToLabel(const std::string_view &name) {
  return LabelId::FromUint(name_id_mapper_.NameToId(name));
}

PropertyId Storage::NameToProperty(const std::string_view &name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

EdgeTypeId Storage::NameToEdgeType(const std::string_view &name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

bool Storage::CreateIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
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
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_CREATE, label, {},
              commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

bool Storage::CreateIndex(
    LabelId label, PropertyId property,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index.CreateIndex(label, property,
                                                 vertices_.access()))
    return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE,
              label, {property}, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

bool Storage::DropIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_index.DropIndex(label)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {},
              commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

bool Storage::DropIndex(
    LabelId label, PropertyId property,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index.DropIndex(label, property)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP,
              label, {property}, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

IndicesInfo Storage::ListAllIndices() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {indices_.label_index.ListIndices(),
          indices_.label_property_index.ListIndices()};
}

utils::BasicResult<ConstraintViolation, bool>
Storage::CreateExistenceConstraint(
    LabelId label, PropertyId property,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = ::storage::CreateExistenceConstraint(&constraints_, label,
                                                  property, vertices_.access());
  if (ret.HasError() || !ret.GetValue()) return ret;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE,
              label, {property}, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

bool Storage::DropExistenceConstraint(
    LabelId label, PropertyId property,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!::storage::DropExistenceConstraint(&constraints_, label, property))
    return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP,
              label, {property}, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return true;
}

utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus>
Storage::CreateUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = constraints_.unique_constraints.CreateConstraint(
      label, properties, vertices_.access());
  if (ret.HasError() ||
      ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS) {
    return ret;
  }
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE,
              label, properties, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus Storage::DropUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties,
    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = constraints_.unique_constraints.DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP, label,
              properties, commit_timestamp);
  commit_log_.MarkFinished(commit_timestamp);
#ifdef MG_ENTERPRISE
  last_commit_timestamp_ = commit_timestamp;
#endif
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

ConstraintsInfo Storage::ListAllConstraints() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {ListExistenceConstraints(constraints_),
          constraints_.unique_constraints.ListConstraints()};
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
  // Because the garbage collector iterates through the indices and constraints
  // to clean them up, it must take the main lock for reading to make sure that
  // the indices and constraints aren't concurrently being modified.
  std::shared_lock<utils::RWLock> main_guard(main_lock_);

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
            if (prev.delta->timestamp->load(std::memory_order_acquire) ==
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
    constraints_.unique_constraints.RemoveObsoleteEntries(
        oldest_active_start_timestamp);
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

bool Storage::InitializeWalFile() {
  if (config_.durability.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, epoch_id_, config_.items,
                      &name_id_mapper_, wal_seq_num_++, &file_retainer_);
  }
  return true;
}

void Storage::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >=
      config_.durability.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >=
      config_.durability.wal_file_size_kibibytes) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
    wal_unsynced_transactions_ = 0;
  } else {
    // Try writing the internal buffer if possible, if not
    // the data should be written as soon as it's possible
    // (triggered by the new transaction commit, or some
    // reading thread EnabledFlushing)
    wal_file_->TryFlushing();
  }
}

void Storage::AppendToWal(const Transaction &transaction,
                          uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp =
      transaction.commit_timestamp->load(std::memory_order_acquire);

#ifdef MG_ENTERPRISE
  if (replication_role_.load() == ReplicationRole::MAIN) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(wal_file_->SequenceNumber());
      }
    });
  }
#endif

  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent,
                                   auto filter) {
    while (true) {
      auto older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr ||
          older->timestamp->load(std::memory_order_acquire) !=
              current_commit_timestamp)
        break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
#ifdef MG_ENTERPRISE
        replication_clients_.WithLock([&](auto &clients) {
          for (auto &client : clients) {
            client->IfStreamingTransaction([&](auto &stream) {
              stream.AppendDelta(*delta, parent, final_commit_timestamp);
            });
          }
        });
#endif
      }
      auto prev = delta->prev.Get();
      if (prev.type != PreviousPtr::Type::DELTA) break;
      delta = prev.delta;
    }
  };

  // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
  // don't traverse them in that order. That is because for each delta we need
  // information about the vertex or edge they belong to and that information
  // isn't stored in the deltas themselves. In order to find out information
  // about the corresponding vertex or edge it is necessary to traverse the
  // delta chain for each delta until a vertex or edge is encountered. This
  // operation is very expensive as the chain grows.
  // Instead, we traverse the edges until we find a vertex or edge and traverse
  // their delta chains. This approach has a drawback because we lose the
  // correct order of the operations. Because of that, we need to traverse the
  // deltas several times and we have to manually ensure that the stored deltas
  // will be ordered correctly.

  // 1. Process all Vertex deltas and store all operations that create vertices
  // and modify vertex data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
          return true;

        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 2. Process all Vertex deltas and store all operations that create edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          return false;
      }
    });
  }
  // 3. Process all Edge deltas and store all operations that modify edge data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, *prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 4. Process all Vertex deltas and store all operations that delete edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 5. Process all Vertex deltas and store all operations that delete vertices.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.
  wal_file_->AppendTransactionEnd(final_commit_timestamp);

  FinalizeWalFile();

#ifdef MG_ENTERPRISE
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) {
        stream.AppendTransactionEnd(final_commit_timestamp);
      });
      client->FinalizeTransactionReplication();
    }
  });
#endif
}

void Storage::AppendToWal(durability::StorageGlobalOperation operation,
                          LabelId label, const std::set<PropertyId> &properties,
                          uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  wal_file_->AppendOperation(operation, label, properties,
                             final_commit_timestamp);
#ifdef MG_ENTERPRISE
  {
    if (replication_role_.load() == ReplicationRole::MAIN) {
      replication_clients_.WithLock([&](auto &clients) {
        for (auto &client : clients) {
          client->StartTransactionReplication(wal_file_->SequenceNumber());
          client->IfStreamingTransaction([&](auto &stream) {
            stream.AppendOperation(operation, label, properties,
                                   final_commit_timestamp);
          });
          client->FinalizeTransactionReplication();
        }
      });
    }
  }
#endif
  FinalizeWalFile();
}

void Storage::CreateSnapshot() {
#ifdef MG_ENTERPRISE
  if (replication_role_.load() != ReplicationRole::MAIN) {
    LOG(WARNING) << "Snapshots are disabled for replicas!";
    return;
  }
#endif

  // Take master RW lock (for reading).
  std::shared_lock<utils::RWLock> storage_guard(main_lock_);

  // Create the transaction used to create the snapshot.
  auto transaction = CreateTransaction();

  // Create snapshot.
  durability::CreateSnapshot(&transaction, snapshot_directory_, wal_directory_,
                             config_.durability.snapshot_retention_count,
                             &vertices_, &edges_, &name_id_mapper_, &indices_,
                             &constraints_, config_.items, uuid_, epoch_id_,
                             epoch_history_, &file_retainer_);

  // Finalize snapshot transaction.
  commit_log_.MarkFinished(transaction.start_timestamp);
}

uint64_t Storage::CommitTimestamp(
    const std::optional<uint64_t> desired_commit_timestamp) {
#ifdef MG_ENTERPRISE
  if (!desired_commit_timestamp) {
    return timestamp_++;
  } else {
    const auto commit_timestamp = *desired_commit_timestamp;
    timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
    return commit_timestamp;
  }
#else
  return timestamp_++;
#endif
}

#ifdef MG_ENTERPRISE
void Storage::ConfigureReplica(io::network::Endpoint endpoint) {
  replication_server_ =
      std::make_unique<ReplicationServer>(this, std::move(endpoint));
}

void Storage::ConfigureMain() {
  // Main instance does not need replication server
  // This should be always called first so we finalize everything
  replication_server_.reset(nullptr);

  std::unique_lock engine_guard{engine_lock_};
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }

  // Generate new epoch id and save the last one to the history.
  if (epoch_history_.size() == kEpochHistoryRetention) {
    epoch_history_.pop_front();
  }
  epoch_history_.emplace_back(std::move(epoch_id_), last_commit_timestamp_);
  epoch_id_ = utils::GenerateUUID();
}

void Storage::RegisterReplica(
    std::string name, io::network::Endpoint endpoint,
    const replication::ReplicationMode replication_mode) {
  // TODO (antonio2368): This shouldn't stop the main instance
  CHECK(replication_role_.load() == ReplicationRole::MAIN)
      << "Only main instance can register a replica!";

  replication_clients_.WithLock([&](auto &clients) {
    if (std::any_of(clients.begin(), clients.end(),
                    [&](auto &client) { return client->Name() == name; })) {
      throw utils::BasicException("Replica with a same name already exists!");
    }
  });

  auto client = std::make_unique<ReplicationClient>(
      std::move(name), this, endpoint, false, replication_mode);

  replication_clients_.WithLock(
      [&](auto &clients) { clients.push_back(std::move(client)); });
}

void Storage::UnregisterReplica(const std::string_view name) {
  CHECK(replication_role_.load() == ReplicationRole::MAIN)
      << "Only main instance can unregister a replica!";
  replication_clients_.WithLock([&](auto &clients) {
    std::erase_if(clients,
                  [&](const auto &client) { return client->Name() == name; });
  });
}

std::optional<replication::ReplicaState> Storage::GetReplicaState(
    const std::string_view name) {
  return replication_clients_.WithLock(
      [&](auto &clients) -> std::optional<replication::ReplicaState> {
        const auto client_it = std::find_if(
            clients.cbegin(), clients.cend(),
            [name](auto &client) { return client->Name() == name; });
        if (client_it == clients.cend()) {
          return std::nullopt;
        }
        return (*client_it)->State();
      });
}
#endif

}  // namespace storage
