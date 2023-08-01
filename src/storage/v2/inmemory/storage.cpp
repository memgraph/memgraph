// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/stat.hpp"

/// REPLICATION ///
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/storage_error.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {
inline constexpr uint16_t kEpochHistoryRetention = 1000;

std::string RegisterReplicaErrorToString(InMemoryStorage::RegisterReplicaError error) {
  switch (error) {
    case InMemoryStorage::RegisterReplicaError::NAME_EXISTS:
      return "NAME_EXISTS";
    case InMemoryStorage::RegisterReplicaError::END_POINT_EXISTS:
      return "END_POINT_EXISTS";
    case InMemoryStorage::RegisterReplicaError::CONNECTION_FAILED:
      return "CONNECTION_FAILED";
    case InMemoryStorage::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
      return "COULD_NOT_BE_PERSISTED";
  }
}
}  // namespace

InMemoryStorage::InMemoryStorage(Config config)
    : Storage(config, StorageMode::IN_MEMORY_TRANSACTIONAL),
      snapshot_directory_(config.durability.storage_directory / durability::kSnapshotDirectory),
      lock_file_path_(config.durability.storage_directory / durability::kLockFile),
      wal_directory_(config.durability.storage_directory / durability::kWalDirectory),
      uuid_(utils::GenerateUUID()),
      epoch_id_(utils::GenerateUUID()),
      global_locker_(file_retainer_.AddLocker()) {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit || config_.durability.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(config_.durability.storage_directory);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_.Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    MG_ASSERT(lock_file_handle_.AcquireLock(),
              "Couldn't acquire lock on the storage directory {}"
              "!\nAnother Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              config_.durability.storage_directory);
  }
  if (config_.durability.recover_on_startup) {
    auto info = durability::RecoverData(snapshot_directory_, wal_directory_, &uuid_, &epoch_id_, &epoch_history_,
                                        &vertices_, &edges_, &edge_count_, name_id_mapper_.get(), &indices_,
                                        &constraints_, config_, &wal_seq_num_);
    if (info) {
      vertex_id_ = info->next_vertex_id;
      edge_id_ = info->next_edge_id;
      timestamp_ = std::max(timestamp_, info->next_timestamp);
      if (info->last_commit_timestamp) {
        last_commit_timestamp_ = *info->last_commit_timestamp;
      }
    }
  } else if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
             config_.durability.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root = config_.durability.storage_directory / durability::kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(snapshot_directory_, durability::kSnapshotDirectory, "snapshot"),
          std::make_tuple(wal_directory_, durability::kWalDirectory, "WAL")}) {
      if (!utils::DirExists(path)) continue;
      auto backup_curr = backup_root / dirname;
      std::error_code error_code;
      for (const auto &item : std::filesystem::directory_iterator(path, error_code)) {
        utils::EnsureDirOrDie(backup_root);
        utils::EnsureDirOrDie(backup_curr);
        std::error_code item_error_code;
        std::filesystem::rename(item.path(), backup_curr / item.path().filename(), item_error_code);
        MG_ASSERT(!item_error_code, "Couldn't move {} file {} because of: {}", what, item.path(),
                  item_error_code.message());
        files_moved = true;
      }
      MG_ASSERT(!error_code, "Couldn't backup {} files because of: {}", what, error_code.message());
    }
    if (files_moved) {
      spdlog::warn(
          "Since Memgraph was not supposed to recover on startup and "
          "durability is enabled, your current durability files will likely "
          "be overridden. To prevent important data loss, Memgraph has stored "
          "those files into a .backup directory inside the storage directory.");
    }
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval, [this] {
      if (auto maybe_error = this->CreateSnapshot({true}); maybe_error.HasError()) {
        switch (maybe_error.GetError()) {
          case CreateSnapshotError::DisabledForReplica:
            spdlog::warn(
                utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
            break;
          case CreateSnapshotError::DisabledForAnalyticsPeriodicCommit:
            spdlog::warn(utils::MessageWithLink("Periodic snapshots are disabled for analytical mode.",
                                                "https://memgr.ph/durability"));
            break;
          case storage::InMemoryStorage::CreateSnapshotError::ReachedMaxNumTries:
            spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
            break;
        }
      }
    });
  }
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Run("Storage GC", config_.gc.interval, [this] { this->CollectGarbage<false>(); });
  }

  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }

  if (config_.durability.restore_replication_state_on_startup) {
    spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
    utils::EnsureDirOrDie(config_.durability.storage_directory / durability::kReplicationDirectory);
    storage_ =
        std::make_unique<kvstore::KVStore>(config_.durability.storage_directory / durability::kReplicationDirectory);

    RestoreReplicationRole();

    if (replication_role_ == replication::ReplicationRole::MAIN) {
      RestoreReplicas();
    }
  } else {
    spdlog::warn(
        "Replication configuration will NOT be stored. When the server restarts, replication state will be "
        "forgotten.");
  }

  if (config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::DISABLED &&
      replication_role_ == replication::ReplicationRole::MAIN) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }
}

InMemoryStorage::~InMemoryStorage() {
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Stop();
  }
  {
    // Clear replication data
    replication_server_.reset();
    replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
  }
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Stop();
  }
  if (config_.durability.snapshot_on_exit) {
    if (auto maybe_error = this->CreateSnapshot({false}); maybe_error.HasError()) {
      switch (maybe_error.GetError()) {
        case CreateSnapshotError::DisabledForReplica:
          spdlog::warn(utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
          break;
        case CreateSnapshotError::DisabledForAnalyticsPeriodicCommit:
          spdlog::warn(utils::MessageWithLink("Periodic snapshots are disabled for analytical mode.",
                                              "https://memgr.ph/replication"));
          break;
        case storage::InMemoryStorage::CreateSnapshotError::ReachedMaxNumTries:
          spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
          break;
      }
    }
  }
}

InMemoryStorage::InMemoryAccessor::InMemoryAccessor(InMemoryStorage *storage, IsolationLevel isolation_level,
                                                    StorageMode storage_mode)
    : Accessor(storage, isolation_level, storage_mode), config_(storage->config_.items) {}
InMemoryStorage::InMemoryAccessor::InMemoryAccessor(InMemoryAccessor &&other) noexcept
    : Accessor(std::move(other)), config_(other.config_) {}

InMemoryStorage::InMemoryAccessor::~InMemoryAccessor() {
  if (is_transaction_active_) {
    Abort();
  }

  FinalizeTransaction();
}

VertexAccessor InMemoryStorage::InMemoryAccessor::CreateVertex() {
  OOMExceptionEnabler oom_exception;
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto gid = mem_storage->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = mem_storage->vertices_.access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");

  if (delta) {
    delta->prev.Set(&*it);
  }
  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

VertexAccessor InMemoryStorage::InMemoryAccessor::CreateVertex(storage::Gid gid) {
  OOMExceptionEnabler oom_exception;
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  mem_storage->vertex_id_.store(std::max(mem_storage->vertex_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                                std::memory_order_release);
  auto acc = mem_storage->vertices_.access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  if (delta) {
    delta->prev.Set(&*it);
  }
  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

std::optional<VertexAccessor> InMemoryStorage::InMemoryAccessor::FindVertex(Gid gid, View view) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto acc = mem_storage->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_, view);
}

Result<std::optional<VertexAccessor>> InMemoryStorage::InMemoryAccessor::DeleteVertex(VertexAccessor *vertex) {
  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  if (vertex_ptr->deleted) {
    return std::optional<VertexAccessor>{};
  }

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty()) return Error::VERTEX_HAS_EDGES;

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  // Need to inform the next CollectGarbage call that there are some
  // non-transactional deletions that need to be collected
  if (transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
    mem_storage->gc_full_scan_vertices_delete_ = true;
  }

  return std::make_optional<VertexAccessor>(vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_,
                                            config_, true);
}

Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
InMemoryStorage::InMemoryAccessor::DetachDeleteVertex(VertexAccessor *vertex) {
  using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

    if (vertex_ptr->deleted) return std::optional<ReturnType>{};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  std::vector<EdgeAccessor> deleted_edges;
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, from_vertex, vertex_ptr, &transaction_, &storage_->indices_,
                   &storage_->constraints_, config_);
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, vertex_ptr, to_vertex, &transaction_, &storage_->indices_, &storage_->constraints_,
                   config_);
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  // We need to check again for serialization errors because we unlocked the
  // vertex. Some other transaction could have modified the vertex in the
  // meantime if we didn't have any edges to delete.

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  // Need to inform the next CollectGarbage call that there are some
  // non-transactional deletions that need to be collected
  if (transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
    mem_storage->gc_full_scan_vertices_delete_ = true;
  }

  return std::make_optional<ReturnType>(
      VertexAccessor{vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_, config_, true},
      std::move(deleted_edges));
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                   EdgeTypeId edge_type) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock, std::defer_lock);
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

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto gid = storage::Gid::FromUint(mem_storage->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = mem_storage->edges_.access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    if (delta) {
      delta->prev.Set(&*it);
    }
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                      &storage_->constraints_, config_);
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                   EdgeTypeId edge_type, storage::Gid gid) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock, std::defer_lock);
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

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  mem_storage->edge_id_.store(std::max(mem_storage->edge_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                              std::memory_order_release);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = mem_storage->edges_.access();

    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    if (delta) {
      delta->prev.Set(&*it);
    }
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                      &storage_->constraints_, config_);
}

Result<std::optional<EdgeAccessor>> InMemoryStorage::InMemoryAccessor::DeleteEdge(EdgeAccessor *edge) {
  MG_ASSERT(edge->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when deleting an edge!");
  auto edge_ref = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::unique_lock<utils::SpinLock> guard;
  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    guard = std::unique_lock<utils::SpinLock>(edge_ptr->lock);

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return std::optional<EdgeAccessor>{};
  }

  auto *from_vertex = edge->from_vertex_;
  auto *to_vertex = edge->to_vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock, std::defer_lock);
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

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!from_vertex->deleted, "Invalid database state!");

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    MG_ASSERT(!to_vertex->deleted, "Invalid database state!");
  }

  auto delete_edge_from_storage = [&edge_type, &edge_ref, this](auto *vertex, auto *edges) {
    std::tuple<EdgeTypeId, Vertex *, EdgeRef> link(edge_type, vertex, edge_ref);
    auto it = std::find(edges->begin(), edges->end(), link);
    if (config_.properties_on_edges) {
      MG_ASSERT(it != edges->end(), "Invalid database state!");
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
    MG_ASSERT((op1 && op2), "Invalid database state!");
  } else {
    MG_ASSERT((op1 && op2) || (!op1 && !op2), "Invalid database state!");
    if (!op1 && !op2) {
      // The edge is already deleted.
      return std::optional<EdgeAccessor>{};
    }
  }

  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    CreateAndLinkDelta(&transaction_, edge_ptr, Delta::RecreateObjectTag());
    edge_ptr->deleted = true;

    // Need to inform the next CollectGarbage call that there are some
    // non-transactional deletions that need to be collected
    if (transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      mem_storage->gc_full_scan_edges_delete_ = true;
    }
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(), edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type, from_vertex, edge_ref);

  // Decrement edge count.
  storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);

  return std::make_optional<EdgeAccessor>(edge_ref, edge_type, from_vertex, to_vertex, &transaction_,
                                          &storage_->indices_, &storage_->constraints_, config_, true);
}

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageDataManipulationError, void> InMemoryStorage::InMemoryAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto could_replicate_all_sync_replicas = true;

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  if (transaction_.deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  } else {
    // Validate that existence constraints are satisfied for all modified
    // vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      auto validation_result = storage_->constraints_.existence_constraints_->Validate(*prev.vertex);
      if (validation_result) {
        Abort();
        return StorageDataManipulationError{*validation_result};
      }
    }

    // Result of validating the vertex against unqiue constraints. It has to be
    // declared outside of the critical section scope because its value is
    // tested for Abort call which has to be done out of the scope.
    std::optional<ConstraintViolation> unique_constraint_violation;

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      auto *mem_unique_constraints =
          static_cast<InMemoryUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
      commit_timestamp_.emplace(mem_storage->CommitTimestamp(desired_commit_timestamp));

      // Before committing and validating vertices against unique constraints,
      // we have to update unique constraints with the vertices that are going
      // to be validated/committed.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }
        mem_unique_constraints->UpdateBeforeCommit(prev.vertex, transaction_);
      }

      // Validate that unique constraints are satisfied for all modified
      // vertices.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }

        // No need to take any locks here because we modified this vertex and no
        // one else can touch it until we commit.
        unique_constraint_violation = mem_unique_constraints->Validate(*prev.vertex, transaction_, *commit_timestamp_);
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
        // Replica can log only the write transaction received from Main
        // so the Wal files are consistent
        if (mem_storage->replication_role_ == replication::ReplicationRole::MAIN ||
            desired_commit_timestamp.has_value()) {
          could_replicate_all_sync_replicas =
              mem_storage->AppendToWalDataManipulation(transaction_, *commit_timestamp_);
        }

        // Take committed_transactions lock while holding the engine lock to
        // make sure that committed transactions are sorted by the commit
        // timestamp in the list.
        mem_storage->committed_transactions_.WithLock([&](auto & /*committed_transactions*/) {
          // TODO: release lock, and update all deltas to have a local copy
          // of the commit timestamp
          MG_ASSERT(transaction_.commit_timestamp != nullptr, "Invalid database state!");
          transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);
          // Replica can only update the last commit timestamp with
          // the commits received from main.
          if (mem_storage->replication_role_ == replication::ReplicationRole::MAIN ||
              desired_commit_timestamp.has_value()) {
            // Update the last commit timestamp
            mem_storage->last_commit_timestamp_.store(*commit_timestamp_);
          }
          // Release engine lock because we don't have to hold it anymore
          // and emplace back could take a long time.
          engine_guard.unlock();
        });

        mem_storage->commit_log_->MarkFinished(start_timestamp);
      }
    }

    if (unique_constraint_violation) {
      Abort();
      return StorageDataManipulationError{*unique_constraint_violation};
    }
  }
  is_transaction_active_ = false;

  if (!could_replicate_all_sync_replicas) {
    return StorageDataManipulationError{ReplicationError{}};
  }

  return {};
}

void InMemoryStorage::InMemoryAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");

  // We collect vertices and edges we've created here and then splice them into
  // `deleted_vertices_` and `deleted_edges_` lists, instead of adding them one
  // by one and acquiring lock every time.
  std::list<Gid> my_deleted_vertices;
  std::list<Gid> my_deleted_edges;

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
        std::lock_guard<utils::SpinLock> guard(vertex->lock);
        Delta *current = vertex->delta;
        while (current != nullptr && current->timestamp->load(std::memory_order_acquire) ==
                                         transaction_.transaction_id.load(std::memory_order_acquire)) {
          switch (current->action) {
            case Delta::Action::REMOVE_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it != vertex->labels.end(), "Invalid database state!");
              std::swap(*it, *vertex->labels.rbegin());
              vertex->labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it == vertex->labels.end(), "Invalid database state!");
              vertex->labels.push_back(current->label);
              break;
            }
            case Delta::Action::SET_PROPERTY: {
              vertex->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::ADD_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it == vertex->in_edges.end(), "Invalid database state!");
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it == vertex->out_edges.end(), "Invalid database state!");
              vertex->out_edges.push_back(link);
              // Increment edge count. We only increment the count here because
              // the information in `ADD_IN_EDGE` and `Edge/RECREATE_OBJECT` is
              // redundant. Also, `Edge/RECREATE_OBJECT` isn't available when
              // edge properties are disabled.
              storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it != vertex->in_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->in_edges.rbegin());
              vertex->in_edges.pop_back();
              break;
            }
            case Delta::Action::REMOVE_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it != vertex->out_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->out_edges.rbegin());
              vertex->out_edges.pop_back();
              // Decrement edge count. We only decrement the count here because
              // the information in `REMOVE_IN_EDGE` and `Edge/DELETE_OBJECT` is
              // redundant. Also, `Edge/DELETE_OBJECT` isn't available when edge
              // properties are disabled.
              storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
        auto *edge = prev.edge;
        std::lock_guard<utils::SpinLock> guard(edge->lock);
        Delta *current = edge->delta;
        while (current != nullptr && current->timestamp->load(std::memory_order_acquire) ==
                                         transaction_.transaction_id.load(std::memory_order_acquire)) {
          switch (current->action) {
            case Delta::Action::SET_PROPERTY: {
              edge->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
              LOG_FATAL("Invalid database state!");
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
      // pointer probably couldn't be set because allocation failed
      case PreviousPtr::Type::NULLPTR:
        break;
    }
  }

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    uint64_t mark_timestamp = storage_->timestamp_;
    // Take garbage_undo_buffers lock while holding the engine lock to make
    // sure that entries are sorted by mark timestamp in the list.
    mem_storage->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
      // Release engine lock because we don't have to hold it anymore and
      // emplace back could take a long time.
      engine_guard.unlock();
      garbage_undo_buffers.emplace_back(mark_timestamp, std::move(transaction_.deltas));
    });
    mem_storage->deleted_vertices_.WithLock(
        [&](auto &deleted_vertices) { deleted_vertices.splice(deleted_vertices.begin(), my_deleted_vertices); });
    mem_storage->deleted_edges_.WithLock(
        [&](auto &deleted_edges) { deleted_edges.splice(deleted_edges.begin(), my_deleted_edges); });
  }

  mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
}

void InMemoryStorage::InMemoryAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
    mem_storage->commit_log_->MarkFinished(*commit_timestamp_);
    mem_storage->committed_transactions_.WithLock(
        [&](auto &committed_transactions) { committed_transactions.emplace_back(std::move(transaction_)); });
    commit_timestamp_.reset();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::CreateIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(indices_.label_index_.get());
  if (!mem_label_index->CreateIndex(label, vertices_.access(), std::nullopt)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  const auto success =
      AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_INDEX_CREATE, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelIndices);

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::CreateIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto *mem_label_property_index = static_cast<InMemoryLabelPropertyIndex *>(indices_.label_property_index_.get());
  if (!mem_label_property_index->CreateIndex(label, property, vertices_.access(), std::nullopt)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::DropIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_index_->DropIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success =
      AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelIndices);

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::DropIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index_->DropIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void> InMemoryStorage::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  if (constraints_.existence_constraints_->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }

  if (auto violation = ExistenceConstraints::ValidateVerticesOnConstraint(vertices_.access(), label, property);
      violation.has_value()) {
    return StorageExistenceConstraintDefinitionError{violation.value()};
  }

  constraints_.existence_constraints_->InsertConstraint(label, property);

  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageExistenceConstraintDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void> InMemoryStorage::DropExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!constraints_.existence_constraints_->DropConstraint(label, property)) {
    return StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageExistenceConstraintDroppingError{ReplicationError{}};
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
InMemoryStorage::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                        const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get());
  auto ret = mem_unique_constraints->CreateConstraint(label, properties, vertices_.access());
  if (ret.HasError()) {
    return StorageUniqueConstraintDefinitionError{ret.GetError()};
  }
  if (ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS) {
    return ret.GetValue();
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE, label,
                                           properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return UniqueConstraints::CreationStatus::SUCCESS;
  }

  return StorageUniqueConstraintDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
InMemoryStorage::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                      const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = constraints_.unique_constraints_->DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP, label,
                                           properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }

  return StorageUniqueConstraintDroppingError{ReplicationError{}};
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, View view) {
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get());
  return VerticesIterable(mem_label_index->Vertices(label, view, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, PropertyId property, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(
      mem_label_property_index->Vertices(label, property, std::nullopt, std::nullopt, view, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, PropertyId property,
                                                             const PropertyValue &value, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(mem_label_property_index->Vertices(label, property, utils::MakeBoundInclusive(value),
                                                             utils::MakeBoundInclusive(value), view, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(
    LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(
      mem_label_property_index->Vertices(label, property, lower_bound, upper_bound, view, &transaction_));
}

Transaction InMemoryStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) {
  // We acquire the transaction engine lock here because we access (and
  // modify) the transaction engine variables (`transaction_id` and
  // `timestamp`) below.
  uint64_t transaction_id = 0;
  uint64_t start_timestamp = 0;
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    transaction_id = transaction_id_++;
    // Replica should have only read queries and the write queries
    // can come from main instance with any past timestamp.
    // To preserve snapshot isolation we set the start timestamp
    // of any query on replica to the last commited transaction
    // which is timestamp_ as only commit of transaction with writes
    // can change the value of it.
    if (replication_role_ == replication::ReplicationRole::REPLICA) {
      start_timestamp = timestamp_;
    } else {
      start_timestamp = timestamp_++;
    }
  }
  return {transaction_id, start_timestamp, isolation_level, storage_mode};
}

template <bool force>
void InMemoryStorage::CollectGarbage(std::unique_lock<utils::RWLock> main_guard) {
  // NOTE: You do not need to consider cleanup of deleted object that occurred in
  // different storage modes within the same CollectGarbage call. This is because
  // SetStorageMode will ensure CollectGarbage is called before any new transactions
  // with the new storage mode can start.

  // SetStorageMode will pass its unique_lock of main_lock_. We will use that lock,
  // as reacquiring the lock would cause  deadlock. Otherwise, we need to get our own
  // lock.
  if (!main_guard.owns_lock()) {
    if constexpr (force) {
      // We take the unique lock on the main storage lock, so we can forcefully clean
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
  } else {
    MG_ASSERT(main_guard.mutex() == std::addressof(main_lock_), "main_guard should be only for the main_lock_");
  }

  utils::OnScopeExit lock_releaser{[&] {
    if (!main_guard.owns_lock()) {
      if constexpr (force) {
        main_lock_.unlock();
      } else {
        main_lock_.unlock_shared();
      }
    } else {
      main_guard.unlock();
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

  auto const need_full_scan_vertices = gc_full_scan_vertices_delete_.exchange(false);
  auto const need_full_scan_edges = gc_full_scan_edges_delete_.exchange(false);

  // Flag that will be used to determine whether the Index GC should be run. It
  // should be run when there were any items that were cleaned up (there were
  // updates between this run of the GC and the previous run of the GC). This
  // eliminates high CPU usage when the GC doesn't have to clean up anything.
  bool run_index_cleanup = !committed_transactions_->empty() || !garbage_undo_buffers_->empty() ||
                           need_full_scan_vertices || need_full_scan_edges;

  while (true) {
    // We don't want to hold the lock on committed transactions for too long,
    // because that prevents other transactions from committing.
    Transaction *transaction = nullptr;
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
                case PreviousPtr::Type::NULLPTR:
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
          case PreviousPtr::Type::NULLPTR: {
            LOG_FATAL("Invalid pointer!");
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
    indices_.RemoveObsoleteEntries(oldest_active_start_timestamp);
    auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get());
    mem_unique_constraints->RemoveObsoleteEntries(oldest_active_start_timestamp);
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

  garbage_undo_buffers_.WithLock([&](auto &undo_buffers) {
    // if force is set to true we can simply delete all the leftover undos because
    // no transaction is active
    if constexpr (force) {
      undo_buffers.clear();
    } else {
      while (!undo_buffers.empty() && undo_buffers.front().first <= oldest_active_start_timestamp) {
        undo_buffers.pop_front();
      }
    }
  });

  {
    auto vertex_acc = vertices_.access();
    if constexpr (force) {
      // if force is set to true, then we have unique_lock and no transactions are active
      // so we can clean all of the deleted vertices
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

  // EXPENSIVE full scan, is only run if an IN_MEMORY_ANALYTICAL transaction involved any deletions
  // TODO: implement a fast internal iteration inside the skip_list (to avoid unnecessary find_node calls),
  //  accessor.remove_if([](auto const & item){ return item.delta == nullptr && item.deleted;});
  //  alternatively, an auxiliary data structure within skip_list to track these, hence a full scan wouldn't be needed
  //  we will wait for evidence that this is needed before doing so.
  if (need_full_scan_vertices) {
    auto vertex_acc = vertices_.access();
    for (auto &vertex : vertex_acc) {
      // a deleted vertex which as no deltas must have come from IN_MEMORY_ANALYTICAL deletion
      if (vertex.delta == nullptr && vertex.deleted) {
        vertex_acc.remove(vertex);
      }
    }
  }

  // EXPENSIVE full scan, is only run if an IN_MEMORY_ANALYTICAL transaction involved any deletions
  if (need_full_scan_edges) {
    auto edge_acc = edges_.access();
    for (auto &edge : edge_acc) {
      // a deleted edge which as no deltas must have come from IN_MEMORY_ANALYTICAL deletion
      if (edge.delta == nullptr && edge.deleted) {
        edge_acc.remove(edge);
      }
    }
  }
}

// tell the linker he can find the CollectGarbage definitions here
template void InMemoryStorage::CollectGarbage<true>(std::unique_lock<utils::RWLock>);
template void InMemoryStorage::CollectGarbage<false>(std::unique_lock<utils::RWLock>);

StorageInfo InMemoryStorage::GetInfo() const {
  auto vertex_count = vertices_.size();
  auto edge_count = edge_count_.load(std::memory_order_acquire);
  double average_degree = 0.0;
  if (vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    average_degree = 2.0 * static_cast<double>(edge_count) / vertex_count;
  }
  return {vertex_count, edge_count, average_degree, utils::GetMemoryUsage(),
          utils::GetDirDiskUsage(config_.durability.storage_directory)};
}

bool InMemoryStorage::InitializeWalFile() {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, epoch_id_, config_.items, name_id_mapper_.get(), wal_seq_num_++,
                      &file_retainer_);
  }
  return true;
}

void InMemoryStorage::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >= config_.durability.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >= config_.durability.wal_file_size_kibibytes) {
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

bool InMemoryStorage::AppendToWalDataManipulation(const Transaction &transaction, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) {
    return true;
  }
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

  if (replication_role_.load() == replication::ReplicationRole::MAIN) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(wal_file_->SequenceNumber());
      }
    });
  }

  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent, auto filter) {
    while (true) {
      auto *older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
        replication_clients_.WithLock([&](auto &clients) {
          for (auto &client : clients) {
            client->IfStreamingTransaction(
                [&](auto &stream) { stream.AppendDelta(*delta, parent, final_commit_timestamp); });
          }
        });
      }
      auto prev = delta->prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
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
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, *prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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

  auto finalized_on_all_replicas = true;
  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(final_commit_timestamp); });
      const auto finalized = client->FinalizeTransactionReplication();

      if (client->Mode() == replication::ReplicationMode::SYNC) {
        finalized_on_all_replicas = finalized && finalized_on_all_replicas;
      }
    }
  });

  return finalized_on_all_replicas;
}

bool InMemoryStorage::AppendToWalDataDefinition(durability::StorageGlobalOperation operation, LabelId label,
                                                const std::set<PropertyId> &properties,
                                                uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) {
    return true;
  }

  auto finalized_on_all_replicas = true;
  wal_file_->AppendOperation(operation, label, properties, final_commit_timestamp);
  {
    if (replication_role_.load() == replication::ReplicationRole::MAIN) {
      replication_clients_.WithLock([&](auto &clients) {
        for (auto &client : clients) {
          client->StartTransactionReplication(wal_file_->SequenceNumber());
          client->IfStreamingTransaction(
              [&](auto &stream) { stream.AppendOperation(operation, label, properties, final_commit_timestamp); });

          const auto finalized = client->FinalizeTransactionReplication();
          if (client->Mode() == replication::ReplicationMode::SYNC) {
            finalized_on_all_replicas = finalized && finalized_on_all_replicas;
          }
        }
      });
    }
  }
  FinalizeWalFile();
  return finalized_on_all_replicas;
}

utils::BasicResult<InMemoryStorage::CreateSnapshotError> InMemoryStorage::CreateSnapshot(
    std::optional<bool> is_periodic) {
  if (replication_role_.load() != replication::ReplicationRole::MAIN) {
    return CreateSnapshotError::DisabledForReplica;
  }

  auto snapshot_creator = [this]() {
    utils::Timer timer;

    auto transaction = CreateTransaction(IsolationLevel::SNAPSHOT_ISOLATION, storage_mode_);
    // Create snapshot.
    durability::CreateSnapshot(&transaction, snapshot_directory_, wal_directory_,
                               config_.durability.snapshot_retention_count, &vertices_, &edges_, name_id_mapper_.get(),
                               &indices_, &constraints_, config_, uuid_, epoch_id_, epoch_history_, &file_retainer_);
    // Finalize snapshot transaction.
    commit_log_->MarkFinished(transaction.start_timestamp);

    memgraph::metrics::Measure(memgraph::metrics::SnapshotCreationLatency_us,
                               std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed()).count());
  };

  std::lock_guard snapshot_guard(snapshot_lock_);

  auto should_try_shared{true};
  auto max_num_tries{10};
  while (max_num_tries) {
    if (should_try_shared) {
      std::shared_lock<utils::RWLock> storage_guard(main_lock_);
      if (storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
        snapshot_creator();
        return {};
      }
    } else {
      std::unique_lock main_guard{main_lock_};
      if (storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) {
        if (is_periodic && *is_periodic) {
          return CreateSnapshotError::DisabledForAnalyticsPeriodicCommit;
        }
        snapshot_creator();
        return {};
      }
    }
    should_try_shared = !should_try_shared;
    max_num_tries--;
  }

  return CreateSnapshotError::ReachedMaxNumTries;
}

void InMemoryStorage::FreeMemory(std::unique_lock<utils::RWLock> main_guard) {
  CollectGarbage<true>(std::move(main_guard));

  // SkipList is already threadsafe
  vertices_.run_gc();
  edges_.run_gc();

  static_cast<InMemoryLabelIndex *>(indices_.label_index_.get())->RunGC();
  static_cast<InMemoryLabelPropertyIndex *>(indices_.label_property_index_.get())->RunGC();
}

uint64_t InMemoryStorage::CommitTimestamp(const std::optional<uint64_t> desired_commit_timestamp) {
  if (!desired_commit_timestamp) {
    return timestamp_++;
  }
  timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
  return *desired_commit_timestamp;
}

bool InMemoryStorage::SetReplicaRole(io::network::Endpoint endpoint,
                                     const replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (replication_role_ == replication::ReplicationRole::REPLICA) {
    return false;
  }

  auto port = endpoint.port;  // assigning because we will move the endpoint
  replication_server_ = std::make_unique<ReplicationServer>(this, std::move(endpoint), config);

  if (ShouldStoreAndRestoreReplicationState()) {
    // Only thing that matters here is the role saved as REPLICA and the listening port
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = replication::kReservedReplicationRoleName,
                                       .ip_address = "",
                                       .port = port,
                                       .sync_mode = replication::ReplicationMode::SYNC,
                                       .replica_check_frequency = std::chrono::seconds(0),
                                       .ssl = std::nullopt,
                                       .role = replication::ReplicationRole::REPLICA});

    if (!storage_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving REPLICA replication role in settings.");
      return false;
    }
  }

  replication_role_.store(replication::ReplicationRole::REPLICA);
  return true;
}

bool InMemoryStorage::SetMainReplicationRole() {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  if (replication_role_ == replication::ReplicationRole::MAIN) {
    return false;
  }

  // Main instance does not need replication server
  // This should be always called first so we finalize everything
  replication_server_.reset(nullptr);

  {
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

  if (ShouldStoreAndRestoreReplicationState()) {
    // Only thing that matters here is the role saved as MAIN
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = replication::kReservedReplicationRoleName,
                                       .ip_address = "",
                                       .port = 0,
                                       .sync_mode = replication::ReplicationMode::SYNC,
                                       .replica_check_frequency = std::chrono::seconds(0),
                                       .ssl = std::nullopt,
                                       .role = replication::ReplicationRole::MAIN});

    if (!storage_->Put(replication::kReservedReplicationRoleName, data.dump())) {
      spdlog::error("Error when saving MAIN replication role in settings.");
      return false;
    }
  }

  replication_role_.store(replication::ReplicationRole::MAIN);
  return true;
}

utils::BasicResult<InMemoryStorage::RegisterReplicaError> InMemoryStorage::RegisterReplica(
    std::string name, io::network::Endpoint endpoint, const replication::ReplicationMode replication_mode,
    const replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config) {
  MG_ASSERT(replication_role_.load() == replication::ReplicationRole::MAIN,
            "Only main instance can register a replica!");

  const bool name_exists = replication_clients_.WithLock([&](auto &clients) {
    return std::any_of(clients.begin(), clients.end(), [&name](const auto &client) { return client->Name() == name; });
  });

  if (name_exists) {
    return RegisterReplicaError::NAME_EXISTS;
  }

  const auto end_point_exists = replication_clients_.WithLock([&endpoint](auto &clients) {
    return std::any_of(clients.begin(), clients.end(),
                       [&endpoint](const auto &client) { return client->Endpoint() == endpoint; });
  });

  if (end_point_exists) {
    return RegisterReplicaError::END_POINT_EXISTS;
  }

  if (ShouldStoreAndRestoreReplicationState()) {
    auto data = replication::ReplicationStatusToJSON(
        replication::ReplicationStatus{.name = name,
                                       .ip_address = endpoint.address,
                                       .port = endpoint.port,
                                       .sync_mode = replication_mode,
                                       .replica_check_frequency = config.replica_check_frequency,
                                       .ssl = config.ssl,
                                       .role = replication::ReplicationRole::REPLICA});
    if (!storage_->Put(name, data.dump())) {
      spdlog::error("Error when saving replica {} in settings.", name);
      return RegisterReplicaError::COULD_NOT_BE_PERSISTED;
    }
  }

  auto client = std::make_unique<ReplicationClient>(std::move(name), this, endpoint, replication_mode, config);

  if (client->State() == replication::ReplicaState::INVALID) {
    if (replication::RegistrationMode::CAN_BE_INVALID != registration_mode) {
      return RegisterReplicaError::CONNECTION_FAILED;
    }

    spdlog::warn("Connection failed when registering replica {}. Replica will still be registered.", client->Name());
  }

  return replication_clients_.WithLock([&](auto &clients) -> utils::BasicResult<InMemoryStorage::RegisterReplicaError> {
    // Another thread could have added a client with same name while
    // we were connecting to this client.
    if (std::any_of(clients.begin(), clients.end(),
                    [&](const auto &other_client) { return client->Name() == other_client->Name(); })) {
      return RegisterReplicaError::NAME_EXISTS;
    }

    if (std::any_of(clients.begin(), clients.end(),
                    [&client](const auto &other_client) { return client->Endpoint() == other_client->Endpoint(); })) {
      return RegisterReplicaError::END_POINT_EXISTS;
    }

    clients.push_back(std::move(client));
    return {};
  });
}

bool InMemoryStorage::UnregisterReplica(const std::string &name) {
  MG_ASSERT(replication_role_.load() == replication::ReplicationRole::MAIN,
            "Only main instance can unregister a replica!");
  if (ShouldStoreAndRestoreReplicationState()) {
    if (!storage_->Delete(name)) {
      spdlog::error("Error when removing replica {} from settings.", name);
      return false;
    }
  }

  return replication_clients_.WithLock([&](auto &clients) {
    return std::erase_if(clients, [&](const auto &client) { return client->Name() == name; });
  });
}

std::optional<replication::ReplicaState> InMemoryStorage::GetReplicaState(const std::string_view name) {
  return replication_clients_.WithLock([&](auto &clients) -> std::optional<replication::ReplicaState> {
    const auto client_it =
        std::find_if(clients.cbegin(), clients.cend(), [name](auto &client) { return client->Name() == name; });
    if (client_it == clients.cend()) {
      return std::nullopt;
    }
    return (*client_it)->State();
  });
}

replication::ReplicationRole InMemoryStorage::GetReplicationRole() const { return replication_role_; }

std::vector<InMemoryStorage::ReplicaInfo> InMemoryStorage::ReplicasInfo() {
  return replication_clients_.WithLock([](auto &clients) {
    std::vector<InMemoryStorage::ReplicaInfo> replica_info;
    replica_info.reserve(clients.size());
    std::transform(
        clients.begin(), clients.end(), std::back_inserter(replica_info), [](const auto &client) -> ReplicaInfo {
          return {client->Name(), client->Mode(), client->Endpoint(), client->State(), client->GetTimestampInfo()};
        });
    return replica_info;
  });
}

void InMemoryStorage::RestoreReplicationRole() {
  if (!ShouldStoreAndRestoreReplicationState()) {
    return;
  }

  spdlog::info("Restoring replication role.");
  uint16_t port = replication::kDefaultReplicationPort;

  const auto replication_data = storage_->Get(replication::kReservedReplicationRoleName);
  if (!replication_data.has_value()) {
    spdlog::debug("Cannot find data needed for restore replication role in persisted metadata.");
    return;
  }

  const auto maybe_replication_status = replication::JSONToReplicationStatus(nlohmann::json::parse(*replication_data));
  if (!maybe_replication_status.has_value()) {
    LOG_FATAL("Cannot parse previously saved configuration of replication role {}.",
              replication::kReservedReplicationRoleName);
  }

  const auto replication_status = *maybe_replication_status;
  if (!replication_status.role.has_value()) {
    replication_role_.store(replication::ReplicationRole::MAIN);
  } else {
    replication_role_.store(*replication_status.role);
    port = replication_status.port;
  }

  if (replication_role_ == replication::ReplicationRole::REPLICA) {
    io::network::Endpoint endpoint(replication::kDefaultReplicationServerIp, port);
    replication_server_ =
        std::make_unique<ReplicationServer>(this, std::move(endpoint), replication::ReplicationServerConfig{});
  }

  spdlog::info("Replication role restored to {}.",
               replication_role_ == replication::ReplicationRole::MAIN ? "MAIN" : "REPLICA");
}

void InMemoryStorage::RestoreReplicas() {
  if (!ShouldStoreAndRestoreReplicationState()) {
    return;
  }
  spdlog::info("Restoring replicas.");

  for (const auto &[replica_name, replica_data] : *storage_) {
    spdlog::info("Restoring replica {}.", replica_name);

    const auto maybe_replica_status = replication::JSONToReplicationStatus(nlohmann::json::parse(replica_data));
    if (!maybe_replica_status.has_value()) {
      LOG_FATAL("Cannot parse previously saved configuration of replica {}.", replica_name);
    }

    auto replica_status = *maybe_replica_status;
    MG_ASSERT(replica_status.name == replica_name, "Expected replica name is '{}', but got '{}'", replica_status.name,
              replica_name);

    if (replica_name == replication::kReservedReplicationRoleName) {
      continue;
    }

    auto ret =
        RegisterReplica(std::move(replica_status.name), {std::move(replica_status.ip_address), replica_status.port},
                        replica_status.sync_mode, replication::RegistrationMode::CAN_BE_INVALID,
                        {
                            .replica_check_frequency = replica_status.replica_check_frequency,
                            .ssl = replica_status.ssl,
                        });

    if (ret.HasError()) {
      MG_ASSERT(RegisterReplicaError::CONNECTION_FAILED != ret.GetError());
      LOG_FATAL("Failure when restoring replica {}: {}.", replica_name, RegisterReplicaErrorToString(ret.GetError()));
    }
    spdlog::info("Replica {} restored.", replica_name);
  }
}

bool InMemoryStorage::ShouldStoreAndRestoreReplicationState() const { return nullptr != storage_; }

utils::FileRetainer::FileLockerAccessor::ret_type InMemoryStorage::IsPathLocked() {
  auto locker_accessor = global_locker_.Access();
  return locker_accessor.IsPathLocked(config_.durability.storage_directory);
}

utils::FileRetainer::FileLockerAccessor::ret_type InMemoryStorage::LockPath() {
  auto locker_accessor = global_locker_.Access();
  return locker_accessor.AddPath(config_.durability.storage_directory);
}

utils::FileRetainer::FileLockerAccessor::ret_type InMemoryStorage::UnlockPath() {
  {
    auto locker_accessor = global_locker_.Access();
    const auto ret = locker_accessor.RemovePath(config_.durability.storage_directory);
    if (ret.HasError() || !ret.GetValue()) {
      // Exit without cleaning the queue
      return ret;
    }
  }
  // We use locker accessor in seperate scope so we don't produce deadlock
  // after we call clean queue.
  file_retainer_.CleanQueue();
  return true;
}

}  // namespace memgraph::storage
