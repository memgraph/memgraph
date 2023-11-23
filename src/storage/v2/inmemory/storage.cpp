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
#include "dbms/constants.hpp"
#include "memory/global_memory_control.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/metadata_delta.hpp"

/// REPLICATION ///
#include "dbms/inmemory/replication_handlers.hpp"
#include "storage/v2/inmemory/replication/recovery.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "utils/resource_lock.hpp"
#include "utils/stat.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

InMemoryStorage::InMemoryStorage(Config config, StorageMode storage_mode)
    : Storage(config, storage_mode),
      snapshot_directory_(config.durability.storage_directory / durability::kSnapshotDirectory),
      lock_file_path_(config.durability.storage_directory / durability::kLockFile),
      wal_directory_(config.durability.storage_directory / durability::kWalDirectory),
      uuid_(utils::GenerateUUID()),
      global_locker_(file_retainer_.AddLocker()) {
  MG_ASSERT(storage_mode != StorageMode::ON_DISK_TRANSACTIONAL,
            "Invalid storage mode sent to InMemoryStorage constructor!");
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
    auto info =
        durability::RecoverData(snapshot_directory_, wal_directory_, &uuid_, repl_storage_state_, &vertices_, &edges_,
                                &edge_count_, name_id_mapper_.get(), &indices_, &constraints_, config_, &wal_seq_num_);
    if (info) {
      vertex_id_ = info->next_vertex_id;
      edge_id_ = info->next_edge_id;
      timestamp_ = std::max(timestamp_, info->next_timestamp);
      if (info->last_commit_timestamp) {
        repl_storage_state_.last_commit_timestamp_ = *info->last_commit_timestamp;
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

  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    // TODO: move out of storage have one global gc_runner_
    gc_runner_.Run("Storage GC", config_.gc.interval, [this] {
      this->FreeMemory(std::unique_lock<utils::ResourceLock>{main_lock_, std::defer_lock});
    });
    gc_jemalloc_runner_.Run("Jemalloc GC", config_.gc.interval, [] { memory::PurgeUnusedMemory(); });
  }
  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }
}

InMemoryStorage::InMemoryStorage(Config config) : InMemoryStorage(config, StorageMode::IN_MEMORY_TRANSACTIONAL) {}

InMemoryStorage::~InMemoryStorage() {
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Stop();
    gc_jemalloc_runner_.Stop();
  }
  {
    // Stop replication (Stop all clients or stop the REPLICA server)
    repl_storage_state_.Reset();
  }
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Stop();
  }
  if (config_.durability.snapshot_on_exit && this->create_snapshot_handler) {
    create_snapshot_handler();
  }
  committed_transactions_.WithLock([](auto &transactions) { transactions.clear(); });
}

InMemoryStorage::InMemoryAccessor::InMemoryAccessor(auto tag, InMemoryStorage *storage, IsolationLevel isolation_level,
                                                    StorageMode storage_mode, bool is_main)
    : Accessor(tag, storage, isolation_level, storage_mode, is_main), config_(storage->config_.items) {}
InMemoryStorage::InMemoryAccessor::InMemoryAccessor(InMemoryAccessor &&other) noexcept
    : Accessor(std::move(other)), config_(other.config_) {}

InMemoryStorage::InMemoryAccessor::~InMemoryAccessor() {
  if (is_transaction_active_) {
    Abort();
    // We didn't actually commit
    commit_timestamp_.reset();
  }

  FinalizeTransaction();
}

VertexAccessor InMemoryStorage::InMemoryAccessor::CreateVertex() {
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
  return {&*it, storage_, &transaction_};
}

VertexAccessor InMemoryStorage::InMemoryAccessor::CreateVertexEx(storage::Gid gid) {
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
  return {&*it, storage_, &transaction_};
}

std::optional<VertexAccessor> InMemoryStorage::InMemoryAccessor::FindVertex(Gid gid, View view) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto acc = mem_storage->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, storage_, &transaction_, view);
}

Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
InMemoryStorage::InMemoryAccessor::DetachDelete(std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges,
                                                bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

  auto maybe_result = Storage::Accessor::DetachDelete(nodes, edges, detach);

  if (maybe_result.HasError()) {
    return maybe_result.GetError();
  }

  auto value = maybe_result.GetValue();

  if (!value) {
    return std::make_optional<ReturnType>();
  }

  auto &[deleted_vertices, deleted_edges] = *value;

  // Need to inform the next CollectGarbage call that there are some
  // non-transactional deletions that need to be collected

  auto const inform_gc_vertex_deletion = utils::OnScopeExit{[this, &deleted_vertices = deleted_vertices]() {
    if (!deleted_vertices.empty() && transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      mem_storage->gc_full_scan_vertices_delete_ = true;
    }
  }};

  auto const inform_gc_edge_deletion = utils::OnScopeExit{[this, &deleted_edges = deleted_edges]() {
    if (!deleted_edges.empty() && transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      mem_storage->gc_full_scan_edges_delete_ = true;
    }
  }};

  for (auto const &vertex : deleted_vertices) {
    transaction_.manyDeltasCache.Invalidate(vertex.vertex_);
  }

  for (const auto &edge : deleted_edges) {
    transaction_.manyDeltasCache.Invalidate(edge.from_vertex_, edge.edge_type_, EdgeDirection::OUT);
    transaction_.manyDeltasCache.Invalidate(edge.to_vertex_, edge.edge_type_, EdgeDirection::IN);
  }

  return maybe_result;
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                   EdgeTypeId edge_type) {
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  auto guard_from = std::unique_lock{from_vertex->lock, std::defer_lock};
  auto guard_to = std::unique_lock{to_vertex->lock, std::defer_lock};
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

  transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdgeEx(VertexAccessor *from, VertexAccessor *to,
                                                                     EdgeTypeId edge_type, storage::Gid gid) {
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  auto guard_from = std::unique_lock{from_vertex->lock, std::defer_lock};
  auto guard_to = std::unique_lock{to_vertex->lock, std::defer_lock};
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

  transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::EdgeSetFrom(EdgeAccessor *edge, VertexAccessor *new_from) {
  MG_ASSERT(edge->transaction_ == new_from->transaction_,
            "EdgeAccessor must be from the same transaction as the new from vertex "
            "accessor when deleting an edge!");
  MG_ASSERT(edge->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when changing an edge!");

  auto *old_from_vertex = edge->from_vertex_;
  auto *new_from_vertex = new_from->vertex_;
  auto *to_vertex = edge->to_vertex_;

  if (old_from_vertex->gid == new_from_vertex->gid) return *edge;

  auto edge_ref = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::unique_lock<utils::RWSpinLock> guard;
  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    guard = std::unique_lock{edge_ptr->lock};

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return Error::DELETED_OBJECT;
  }

  std::unique_lock<utils::RWSpinLock> guard_old_from(old_from_vertex->lock, std::defer_lock);
  std::unique_lock<utils::RWSpinLock> guard_new_from(new_from_vertex->lock, std::defer_lock);
  std::unique_lock<utils::RWSpinLock> guard_to(to_vertex->lock, std::defer_lock);

  // lock in increasing gid order, if two vertices have the same gid need to only lock once
  std::vector<memgraph::storage::Vertex *> vertices{old_from_vertex, new_from_vertex, to_vertex};
  std::sort(vertices.begin(), vertices.end(), [](auto x, auto y) { return x->gid < y->gid; });
  vertices.erase(std::unique(vertices.begin(), vertices.end(), [](auto x, auto y) { return x->gid == y->gid; }),
                 vertices.end());

  for (auto *vertex : vertices) {
    if (vertex == old_from_vertex) {
      guard_old_from.lock();
    } else if (vertex == new_from_vertex) {
      guard_new_from.lock();
    } else if (vertex == to_vertex) {
      guard_to.lock();
    } else {
      return Error::NONEXISTENT_OBJECT;
    }
  }

  if (!PrepareForWrite(&transaction_, old_from_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!old_from_vertex->deleted, "Invalid database state!");

  if (!PrepareForWrite(&transaction_, new_from_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!new_from_vertex->deleted, "Invalid database state!");

  if (to_vertex != old_from_vertex && to_vertex != new_from_vertex) {
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

  auto op1 = delete_edge_from_storage(to_vertex, &old_from_vertex->out_edges);
  auto op2 = delete_edge_from_storage(old_from_vertex, &to_vertex->in_edges);

  if (config_.properties_on_edges) {
    MG_ASSERT((op1 && op2), "Invalid database state!");
  } else {
    MG_ASSERT((op1 && op2) || (!op1 && !op2), "Invalid database state!");
    if (!op1 && !op2) {
      // The edge is already deleted.
      return Error::DELETED_OBJECT;
    }
  }

  CreateAndLinkDelta(&transaction_, old_from_vertex, Delta::AddOutEdgeTag(), edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type, old_from_vertex, edge_ref);

  CreateAndLinkDelta(&transaction_, new_from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge_ref);
  new_from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, new_from_vertex, edge_ref);
  to_vertex->in_edges.emplace_back(edge_type, new_from_vertex, edge_ref);

  transaction_.manyDeltasCache.Invalidate(new_from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(old_from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

  return EdgeAccessor(edge_ref, edge_type, new_from_vertex, to_vertex, storage_, &transaction_);
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::EdgeSetTo(EdgeAccessor *edge, VertexAccessor *new_to) {
  MG_ASSERT(edge->transaction_ == new_to->transaction_,
            "EdgeAccessor must be from the same transaction as the new to vertex "
            "accessor when deleting an edge!");
  MG_ASSERT(edge->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when deleting an edge!");

  auto *from_vertex = edge->from_vertex_;
  auto *old_to_vertex = edge->to_vertex_;
  auto *new_to_vertex = new_to->vertex_;

  if (old_to_vertex->gid == new_to_vertex->gid) return *edge;

  auto &edge_ref = edge->edge_;
  auto &edge_type = edge->edge_type_;

  std::unique_lock<utils::RWSpinLock> guard;
  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    guard = std::unique_lock{edge_ptr->lock};

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return Error::DELETED_OBJECT;
  }

  std::unique_lock<utils::RWSpinLock> guard_from(from_vertex->lock, std::defer_lock);
  std::unique_lock<utils::RWSpinLock> guard_old_to(old_to_vertex->lock, std::defer_lock);
  std::unique_lock<utils::RWSpinLock> guard_new_to(new_to_vertex->lock, std::defer_lock);

  // lock in increasing gid order, if two vertices have the same gid need to only lock once
  std::vector<memgraph::storage::Vertex *> vertices{from_vertex, old_to_vertex, new_to_vertex};
  std::sort(vertices.begin(), vertices.end(), [](auto x, auto y) { return x->gid < y->gid; });
  vertices.erase(std::unique(vertices.begin(), vertices.end(), [](auto x, auto y) { return x->gid == y->gid; }),
                 vertices.end());

  for (auto *vertex : vertices) {
    if (vertex == from_vertex) {
      guard_from.lock();
    } else if (vertex == old_to_vertex) {
      guard_old_to.lock();
    } else if (vertex == new_to_vertex) {
      guard_new_to.lock();
    } else {
      return Error::NONEXISTENT_OBJECT;
    }
  }

  if (!PrepareForWrite(&transaction_, old_to_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!old_to_vertex->deleted, "Invalid database state!");

  if (!PrepareForWrite(&transaction_, new_to_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!new_to_vertex->deleted, "Invalid database state!");

  if (from_vertex != old_to_vertex && from_vertex != new_to_vertex) {
    if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
    MG_ASSERT(!from_vertex->deleted, "Invalid database state!");
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

  auto op1 = delete_edge_from_storage(old_to_vertex, &from_vertex->out_edges);
  auto op2 = delete_edge_from_storage(from_vertex, &old_to_vertex->in_edges);

  if (config_.properties_on_edges) {
    MG_ASSERT((op1 && op2), "Invalid database state!");
  } else {
    MG_ASSERT((op1 && op2) || (!op1 && !op2), "Invalid database state!");
    if (!op1 && !op2) {
      // The edge is already deleted.
      return Error::DELETED_OBJECT;
    }
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(), edge_type, old_to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, old_to_vertex, Delta::AddInEdgeTag(), edge_type, from_vertex, edge_ref);

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, new_to_vertex, edge_ref);
  from_vertex->out_edges.emplace_back(edge_type, new_to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, new_to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge_ref);
  new_to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge_ref);

  transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(old_to_vertex, edge_type, EdgeDirection::IN);
  transaction_.manyDeltasCache.Invalidate(new_to_vertex, edge_type, EdgeDirection::IN);

  return EdgeAccessor(edge_ref, edge_type, from_vertex, new_to_vertex, storage_, &transaction_);
}

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::EdgeChangeType(EdgeAccessor *edge, EdgeTypeId new_edge_type) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(&transaction_ == edge->transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when changing the edge type!");

  auto &edge_ref = edge->edge_;
  auto &edge_type = edge->edge_type_;

  std::unique_lock<utils::RWSpinLock> guard;
  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    guard = std::unique_lock{edge_ptr->lock};

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;
    if (edge_ptr->deleted) return Error::DELETED_OBJECT;
  }

  auto *from_vertex = edge->from_vertex_;
  auto *to_vertex = edge->to_vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  auto guard_from = std::unique_lock{from_vertex->lock, std::defer_lock};
  auto guard_to = std::unique_lock{to_vertex->lock, std::defer_lock};
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

  if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!to_vertex->deleted, "Invalid database state!");

  auto change_edge_type_in_storage = [&edge_type, &edge_ref, &new_edge_type, this](auto *vertex, auto *edges) {
    std::tuple<EdgeTypeId, Vertex *, EdgeRef> link(edge_type, vertex, edge_ref);
    auto it = std::find(edges->begin(), edges->end(), link);
    if (config_.properties_on_edges) {
      MG_ASSERT(it != edges->end(), "Invalid database state!");
    } else if (it == edges->end()) {
      return false;
    }
    *it = std::tuple<EdgeTypeId, Vertex *, EdgeRef>{new_edge_type, vertex, edge_ref};
    return true;
  };

  auto op1 = change_edge_type_in_storage(to_vertex, &from_vertex->out_edges);
  auto op2 = change_edge_type_in_storage(from_vertex, &to_vertex->in_edges);

  MG_ASSERT((op1 && op2), "Invalid database state!");

  // "deleting" old edge
  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(), edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type, from_vertex, edge_ref);

  // "adding" new edge
  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), new_edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), new_edge_type, from_vertex, edge_ref);

  // edge type is not used while invalidating cache so we can only call it once
  transaction_.manyDeltasCache.Invalidate(from_vertex, new_edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(to_vertex, new_edge_type, EdgeDirection::IN);

  return EdgeAccessor(edge_ref, new_edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageManipulationError, void> InMemoryStorage::InMemoryAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp, bool is_main) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto could_replicate_all_sync_replicas = true;

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // TODO: duplicated transaction finalisation in md_deltas and deltas processing cases
  if (!transaction_.md_deltas.empty()) {
    // This is usually done by the MVCC, but it does not handle the metadata deltas
    transaction_.EnsureCommitTimestampExists();

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;

    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    commit_timestamp_.emplace(mem_storage->CommitTimestamp(desired_commit_timestamp));

    // Write transaction to WAL while holding the engine lock to make sure
    // that committed transactions are sorted by the commit timestamp in the
    // WAL files. We supply the new commit timestamp to the function so that
    // it knows what will be the final commit timestamp. The WAL must be
    // written before actually committing the transaction (before setting
    // the commit timestamp) so that no other transaction can see the
    // modifications before they are written to disk.
    // Replica can log only the write transaction received from Main
    // so the Wal files are consistent
    if (is_main || desired_commit_timestamp.has_value()) {
      could_replicate_all_sync_replicas =
          mem_storage->AppendToWalDataDefinition(transaction_, *commit_timestamp_);  // protected by engine_guard
      // TODO: release lock, and update all deltas to have a local copy of the commit timestamp
      transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);  // protected by engine_guard
      // Replica can only update the last commit timestamp with
      // the commits received from main.
      if (is_main || desired_commit_timestamp.has_value()) {
        // Update the last commit timestamp
        mem_storage->repl_storage_state_.last_commit_timestamp_.store(*commit_timestamp_);  // protected by engine_guard
      }
      // Release engine lock because we don't have to hold it anymore
      engine_guard.unlock();

      mem_storage->commit_log_->MarkFinished(start_timestamp);
    }
  } else if (transaction_.deltas.use().empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  } else {
    if (transaction_.constraint_verification_info.NeedsExistenceConstraintVerification()) {
      const auto vertices_to_update =
          transaction_.constraint_verification_info.GetVerticesForExistenceConstraintChecking();
      for (auto const *vertex : vertices_to_update) {
        // No need to take any locks here because we modified this vertex and no
        // one else can touch it until we commit.
        auto validation_result = storage_->constraints_.existence_constraints_->Validate(*vertex);
        if (validation_result) {
          Abort();
          DMG_ASSERT(!commit_timestamp_.has_value());
          return StorageManipulationError{*validation_result};
        }
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

      if (transaction_.constraint_verification_info.NeedsUniqueConstraintVerification()) {
        // Before committing and validating vertices against unique constraints,
        // we have to update unique constraints with the vertices that are going
        // to be validated/committed.
        const auto vertices_to_update =
            transaction_.constraint_verification_info.GetVerticesForUniqueConstraintChecking();

        for (auto const *vertex : vertices_to_update) {
          mem_unique_constraints->UpdateBeforeCommit(vertex, transaction_);
        }

        for (auto const *vertex : vertices_to_update) {
          // No need to take any locks here because we modified this vertex and no
          // one else can touch it until we commit.
          unique_constraint_violation = mem_unique_constraints->Validate(*vertex, transaction_, *commit_timestamp_);
          if (unique_constraint_violation) {
            break;
          }
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
        if (is_main || desired_commit_timestamp.has_value()) {
          could_replicate_all_sync_replicas =
              mem_storage->AppendToWalDataManipulation(transaction_, *commit_timestamp_);  // protected by engine_guard
        }

        // TODO: release lock, and update all deltas to have a local copy of the commit timestamp
        MG_ASSERT(transaction_.commit_timestamp != nullptr, "Invalid database state!");
        transaction_.commit_timestamp->store(*commit_timestamp_,
                                             std::memory_order_release);  // protected by engine_guard
        // Replica can only update the last commit timestamp with
        // the commits received from main.
        if (is_main || desired_commit_timestamp.has_value()) {
          // Update the last commit timestamp
          mem_storage->repl_storage_state_.last_commit_timestamp_.store(
              *commit_timestamp_);  // protected by engine_guard
        }
        // Release engine lock because we don't have to hold it anymore
        engine_guard.unlock();

        mem_storage->commit_log_->MarkFinished(start_timestamp);
      }
    }

    if (unique_constraint_violation) {
      Abort();
      DMG_ASSERT(commit_timestamp_.has_value());
      commit_timestamp_.reset();  // We have aborted, hence we have not committed
      return StorageManipulationError{*unique_constraint_violation};
    }
  }

  is_transaction_active_ = false;

  if (!could_replicate_all_sync_replicas) {
    return StorageManipulationError{ReplicationError{}};
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

  for (const auto &delta : transaction_.deltas.use()) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
        auto guard = std::unique_lock{vertex->lock};
        Delta *current = vertex->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
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
        auto guard = std::lock_guard{edge->lock};
        Delta *current = edge->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
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

      garbage_undo_buffers.emplace_back(mark_timestamp, std::move(transaction_.deltas),
                                        std::move(transaction_.commit_timestamp));
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

    if (!transaction_.deltas.use().empty()) {
      // Only hand over delta to be GC'ed if there was any deltas
      mem_storage->committed_transactions_.WithLock([&](auto &committed_transactions) {
        // using mark of 0 as GC will assign a mark_timestamp after unlinking
        committed_transactions.emplace_back(0, std::move(transaction_.deltas),
                                            std::move(transaction_.commit_timestamp));
      });
    }
    commit_timestamp_.reset();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateIndex(LabelId label) {
  MG_ASSERT(unique_guard_.owns_lock(), "Creating label index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory->indices_.label_index_.get());
  if (!mem_label_index->CreateIndex(label, in_memory->vertices_.access(), std::nullopt)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_create, label);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateIndex(
    LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Creating label-property index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory->indices_.label_property_index_.get());
  if (!mem_label_property_index->CreateIndex(label, property, in_memory->vertices_.access(), std::nullopt)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_create, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(LabelId label) {
  MG_ASSERT(unique_guard_.owns_lock(), "Dropping label index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory->indices_.label_index_.get());
  if (!mem_label_index->DropIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_drop, label);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(
    LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Dropping label-property index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory->indices_.label_property_index_.get());
  if (!mem_label_property_index->DropIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_drop, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  return {};
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void>
InMemoryStorage::InMemoryAccessor::CreateExistenceConstraint(LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Creating existence requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *existence_constraints = in_memory->constraints_.existence_constraints_.get();
  if (existence_constraints->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }
  if (auto violation =
          ExistenceConstraints::ValidateVerticesOnConstraint(in_memory->vertices_.access(), label, property);
      violation.has_value()) {
    return StorageExistenceConstraintDefinitionError{violation.value()};
  }
  existence_constraints->InsertConstraint(label, property);
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_create, label, property);
  return {};
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void>
InMemoryStorage::InMemoryAccessor::DropExistenceConstraint(LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Dropping existence constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *existence_constraints = in_memory->constraints_.existence_constraints_.get();
  if (!existence_constraints->DropConstraint(label, property)) {
    return StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_drop, label, property);
  return {};
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
InMemoryStorage::InMemoryAccessor::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) {
  MG_ASSERT(unique_guard_.owns_lock(), "Creating unique constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(in_memory->constraints_.unique_constraints_.get());
  auto ret = mem_unique_constraints->CreateConstraint(label, properties, in_memory->vertices_.access());
  if (ret.HasError()) {
    return StorageUniqueConstraintDefinitionError{ret.GetError()};
  }
  if (ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS) {
    return ret.GetValue();
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_create, label, properties);
  return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus InMemoryStorage::InMemoryAccessor::DropUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties) {
  MG_ASSERT(unique_guard_.owns_lock(), "Dropping unique constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(in_memory->constraints_.unique_constraints_.get());
  auto ret = mem_unique_constraints->DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_drop, label, properties);
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, View view) {
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get());
  return VerticesIterable(mem_label_index->Vertices(label, view, storage_, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, PropertyId property, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(
      mem_label_property_index->Vertices(label, property, std::nullopt, std::nullopt, view, storage_, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, PropertyId property,
                                                             const PropertyValue &value, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(mem_label_property_index->Vertices(label, property, utils::MakeBoundInclusive(value),
                                                             utils::MakeBoundInclusive(value), view, storage_,
                                                             &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(
    LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  return VerticesIterable(
      mem_label_property_index->Vertices(label, property, lower_bound, upper_bound, view, storage_, &transaction_));
}

Transaction InMemoryStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode, bool is_main) {
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
    if (is_main) {
      start_timestamp = timestamp_++;
    } else {
      start_timestamp = timestamp_;
    }
  }
  return {transaction_id, start_timestamp, isolation_level, storage_mode, false};
}

void InMemoryStorage::SetStorageMode(StorageMode new_storage_mode) {
  std::unique_lock main_guard{main_lock_};
  MG_ASSERT(
      (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL || storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) &&
      (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL ||
       new_storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL));
  if (storage_mode_ != new_storage_mode) {
    if (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      snapshot_runner_.Stop();
    } else if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
      snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval,
                           [this]() { this->create_snapshot_handler(); });
    }

    storage_mode_ = new_storage_mode;
    FreeMemory(std::move(main_guard));
  }
}

template <bool force>
void InMemoryStorage::CollectGarbage(std::unique_lock<utils::ResourceLock> main_guard) {
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
    DMG_ASSERT(main_guard.mutex() == std::addressof(main_lock_), "main_guard should be only for the main_lock_");
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

  // Deltas from previous GC runs or from aborts can be cleaned up here
  garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
    if constexpr (force) {
      // if force is set to true we can simply delete all the leftover undos because
      // no transaction is active
      garbage_undo_buffers.clear();
    } else {
      // garbage_undo_buffers is ordered, pop until we can't
      while (!garbage_undo_buffers.empty() &&
             garbage_undo_buffers.front().mark_timestamp_ <= oldest_active_start_timestamp) {
        garbage_undo_buffers.pop_front();
      }
    }
  });

  // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
  // list immediately, because we would have to repeatedly take
  // garbage_undo_buffers lock.
  std::list<GCDeltas> unlinked_undo_buffers{};

  // We will only free vertices deleted up until now in this GC cycle, and we
  // will do it after cleaning-up the indices. That way we are sure that all
  // vertices that appear in an index also exist in main storage.
  std::list<Gid> current_deleted_edges;
  std::list<Gid> current_deleted_vertices;
  deleted_vertices_->swap(current_deleted_vertices);
  deleted_edges_->swap(current_deleted_edges);

  auto const need_full_scan_vertices = gc_full_scan_vertices_delete_.exchange(false);
  auto const need_full_scan_edges = gc_full_scan_edges_delete_.exchange(false);

  // Short lock, to move to local variable. Hence allows other transactions to commit.
  auto linked_undo_buffers = std::list<GCDeltas>{};
  committed_transactions_.WithLock(
      [&](auto &committed_transactions) { committed_transactions.swap(linked_undo_buffers); });

  // Flag that will be used to determine whether the Index GC should be run. It
  // should be run when there were any items that were cleaned up (there were
  // updates between this run of the GC and the previous run of the GC). This
  // eliminates high CPU usage when the GC doesn't have to clean up anything.
  bool run_index_cleanup = !linked_undo_buffers.empty() || !garbage_undo_buffers_->empty() || need_full_scan_vertices ||
                           need_full_scan_edges;

  auto const end_linked_undo_buffers = linked_undo_buffers.end();
  for (auto linked_entry = linked_undo_buffers.begin(); linked_entry != end_linked_undo_buffers;) {
    auto const *const commit_timestamp_ptr = linked_entry->commit_timestamp_.get();
    auto const commit_timestamp = commit_timestamp_ptr->load(std::memory_order_acquire);

    // only process those that are no longer active
    if (commit_timestamp >= oldest_active_start_timestamp) {
      ++linked_entry;  // can not process, skip
      continue;        // must continue to next transaction, because committed_transactions_ was not ordered
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

    for (Delta &delta : linked_entry->deltas_.use()) {
      while (true) {
        auto prev = delta.prev.Get();
        switch (prev.type) {
          case PreviousPtr::Type::VERTEX: {
            Vertex *vertex = prev.vertex;
            auto vertex_guard = std::unique_lock{vertex->lock};
            if (vertex->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            vertex->delta = nullptr;
            if (vertex->deleted) {
              DMG_ASSERT(delta.action == memgraph::storage::Delta::Action::RECREATE_OBJECT);
              current_deleted_vertices.push_back(vertex->gid);
            }
            break;
          }
          case PreviousPtr::Type::EDGE: {
            Edge *edge = prev.edge;
            auto edge_guard = std::unique_lock{edge->lock};
            if (edge->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            edge->delta = nullptr;
            if (edge->deleted) {
              DMG_ASSERT(delta.action == memgraph::storage::Delta::Action::RECREATE_OBJECT);
              current_deleted_edges.push_back(edge->gid);
            }
            break;
          }
          case PreviousPtr::Type::DELTA: {
            //              kTransactionInitialId
            //                     │
            //                     ▼
            // ┌───────────────────┬─────────────┐
            // │     Committed     │ Uncommitted │
            // ├──────────┬────────┴─────────────┤
            // │ Inactive │      Active          │
            // └──────────┴──────────────────────┘
            //            ▲
            //            │
            //  oldest_active_start_timestamp

            if (prev.delta->timestamp == commit_timestamp_ptr) {
              // The delta that is newer than this one is also a delta from this
              // transaction. We skip the current delta and will remove it as a
              // part of the suffix later.
              break;
            }

            if (prev.delta->timestamp->load() < oldest_active_start_timestamp) {
              // If previous is from another inactive transaction, no need to
              // lock the edge/vertex, nothing will read this far or relink to
              // us directly
              break;
            }

            // Previous is either active (committed or uncommitted), we need to find
            // the parent object in order to be able to use its lock.
            auto parent = prev;
            while (parent.type == PreviousPtr::Type::DELTA) {
              parent = parent.delta->prev.Get();
            }

            auto const guard = std::invoke([&] {
              switch (parent.type) {
                case PreviousPtr::Type::VERTEX:
                  return std::unique_lock{parent.vertex->lock};
                case PreviousPtr::Type::EDGE:
                  return std::unique_lock{parent.edge->lock};
                case PreviousPtr::Type::DELTA:
                case PreviousPtr::Type::NULLPTR:
                  LOG_FATAL("Invalid database state!");
              }
            });
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

    // Now unlinked, move to unlinked_undo_buffers
    auto const to_move = linked_entry;
    ++linked_entry;  // advanced to next before we move the list node
    unlinked_undo_buffers.splice(unlinked_undo_buffers.end(), linked_undo_buffers, to_move);
  }

  if (!linked_undo_buffers.empty()) {
    // some were not able to be collected, add them back to committed_transactions_ for the next GC run
    committed_transactions_.WithLock([&linked_undo_buffers](auto &committed_transactions) {
      committed_transactions.splice(committed_transactions.begin(), std::move(linked_undo_buffers));
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
    uint64_t mark_timestamp = timestamp_;  // a timestamp no active transaction can currently have

    if (force or mark_timestamp == oldest_active_start_timestamp) {
      // if lucky, there are no active transactions, hence nothing looking at the deltas
      // remove them now
      unlinked_undo_buffers.clear();
    } else {
      // Take garbage_undo_buffers lock while holding the engine lock to make
      // sure that entries are sorted by mark timestamp in the list.
      garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
        // Release engine lock because we don't have to hold it anymore and
        // this could take a long time.
        guard.unlock();
        // correct the markers, and defer until next GC run
        for (auto &unlinked_undo_buffer : unlinked_undo_buffers) {
          unlinked_undo_buffer.mark_timestamp_ = mark_timestamp;
        }
        // ensure insert at end to preserve the order
        garbage_undo_buffers.splice(garbage_undo_buffers.end(), std::move(unlinked_undo_buffers));
      });
    }
  }

  {
    auto vertex_acc = vertices_.access();
    for (auto vertex : current_deleted_vertices) {
      MG_ASSERT(vertex_acc.remove(vertex), "Invalid database state!");
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
template void InMemoryStorage::CollectGarbage<true>(std::unique_lock<utils::ResourceLock>);
template void InMemoryStorage::CollectGarbage<false>(std::unique_lock<utils::ResourceLock>);

StorageInfo InMemoryStorage::GetBaseInfo(bool force_directory) {
  StorageInfo info{};
  info.vertex_count = vertices_.size();
  info.edge_count = edge_count_.load(std::memory_order_acquire);
  if (info.vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    info.average_degree = 2.0 * static_cast<double>(info.edge_count) / info.vertex_count;
  }
  info.memory_res = utils::GetMemoryRES();
  // Special case for the default database
  auto update_path = [&](const std::filesystem::path &dir) {
    if (!force_directory && std::filesystem::is_directory(dir) && dir.has_filename()) {
      const auto end = dir.end();
      auto it = end;
      --it;
      if (it != end) {
        --it;
        if (it != end && *it != "databases") {
          // Default DB points to the root (for back-compatibility); update to the "database" dir
          return dir / "databases" / dbms::kDefaultDB;
        }
      }
    }
    return dir;
  };
  info.disk_usage = utils::GetDirDiskUsage<false>(update_path(config_.durability.storage_directory));
  return info;
}

StorageInfo InMemoryStorage::GetInfo(bool force_directory) {
  StorageInfo info = GetBaseInfo(force_directory);
  {
    auto access = Access(std::nullopt);
    const auto &lbl = access->ListAllIndices();
    info.label_indices = lbl.label.size();
    info.label_property_indices = lbl.label_property.size();
    const auto &con = access->ListAllConstraints();
    info.existence_constraints = con.existence.size();
    info.unique_constraints = con.unique.size();
  }
  info.storage_mode = storage_mode_;
  info.isolation_level = isolation_level_;
  info.durability_snapshot_enabled =
      config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit;
  info.durability_wal_enabled =
      config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  return info;
}

bool InMemoryStorage::InitializeWalFile(memgraph::replication::ReplicationEpoch &epoch) {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, epoch.id(), config_.items, name_id_mapper_.get(), wal_seq_num_++,
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
  if (!InitializeWalFile(repl_storage_state_.epoch_)) {
    return true;
  }
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

  repl_storage_state_.InitializeTransaction(wal_file_->SequenceNumber(), this);

  auto append_deltas = [&](auto callback) {
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
          callback(*delta, parent, final_commit_timestamp);
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
    for (const auto &delta : transaction.deltas.use()) {
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
    for (const auto &delta : transaction.deltas.use()) {
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
    for (const auto &delta : transaction.deltas.use()) {
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
    for (const auto &delta : transaction.deltas.use()) {
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
    for (const auto &delta : transaction.deltas.use()) {
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
  };

  append_deltas([&](const Delta &delta, const auto &parent, uint64_t timestamp) {
    wal_file_->AppendDelta(delta, parent, timestamp);
    repl_storage_state_.AppendDelta(delta, parent, timestamp);
  });

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.replication_clients_.WithLock
  wal_file_->AppendTransactionEnd(final_commit_timestamp);
  FinalizeWalFile();

  return repl_storage_state_.FinalizeTransaction(final_commit_timestamp, this);
}

bool InMemoryStorage::AppendToWalDataDefinition(const Transaction &transaction, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile(repl_storage_state_.epoch_)) {
    return true;
  }

  repl_storage_state_.InitializeTransaction(wal_file_->SequenceNumber(), this);

  for (const auto &md_delta : transaction.md_deltas) {
    switch (md_delta.action) {
      case MetadataDelta::Action::LABEL_INDEX_CREATE: {
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_INDEX_CREATE, md_delta.label,
                                  final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_PROPERTY_INDEX_CREATE: {
        const auto &info = md_delta.label_property;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_CREATE, info.label,
                                  {info.property}, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_INDEX_DROP: {
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_INDEX_DROP, md_delta.label,
                                  final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_PROPERTY_INDEX_DROP: {
        const auto &info = md_delta.label_property;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_DROP, info.label,
                                  {info.property}, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_INDEX_STATS_SET: {
        const auto &info = md_delta.label_stats;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_INDEX_STATS_SET, info.label, info.stats,
                                  final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_INDEX_STATS_CLEAR: {
        const auto &info = md_delta.label_stats;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_INDEX_STATS_CLEAR, info.label,
                                  final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_PROPERTY_INDEX_STATS_SET: {
        const auto &info = md_delta.label_property_stats;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_SET, info.label,
                                  {info.property}, info.stats, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::LABEL_PROPERTY_INDEX_STATS_CLEAR: /* Special case we clear all label/property
                                                                       pairs with the defined label */
      {
        const auto &info = md_delta.label_stats;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::LABEL_PROPERTY_INDEX_STATS_CLEAR, info.label,
                                  final_commit_timestamp);
      } break;
      case MetadataDelta::Action::EXISTENCE_CONSTRAINT_CREATE: {
        const auto &info = md_delta.label_property;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_CREATE, info.label,
                                  {info.property}, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::EXISTENCE_CONSTRAINT_DROP: {
        const auto &info = md_delta.label_property;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::EXISTENCE_CONSTRAINT_DROP, info.label,
                                  {info.property}, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::UNIQUE_CONSTRAINT_CREATE: {
        const auto &info = md_delta.label_properties;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_CREATE, info.label,
                                  info.properties, final_commit_timestamp);
      } break;
      case MetadataDelta::Action::UNIQUE_CONSTRAINT_DROP: {
        const auto &info = md_delta.label_properties;
        AppendToWalDataDefinition(durability::StorageMetadataOperation::UNIQUE_CONSTRAINT_DROP, info.label,
                                  info.properties, final_commit_timestamp);
      } break;
    }
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  wal_file_->AppendTransactionEnd(final_commit_timestamp);
  FinalizeWalFile();

  return repl_storage_state_.FinalizeTransaction(final_commit_timestamp, this);
}

void InMemoryStorage::AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                                const std::set<PropertyId> &properties, LabelIndexStats stats,
                                                LabelPropertyIndexStats property_stats,
                                                uint64_t final_commit_timestamp) {
  wal_file_->AppendOperation(operation, label, properties, stats, property_stats, final_commit_timestamp);
  repl_storage_state_.AppendOperation(operation, label, properties, stats, property_stats, final_commit_timestamp);
}

void InMemoryStorage::AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                                const std::set<PropertyId> &properties,
                                                LabelPropertyIndexStats property_stats,
                                                uint64_t final_commit_timestamp) {
  return AppendToWalDataDefinition(operation, label, properties, {}, property_stats, final_commit_timestamp);
}

void InMemoryStorage::AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                                LabelIndexStats stats, uint64_t final_commit_timestamp) {
  return AppendToWalDataDefinition(operation, label, {}, stats, {}, final_commit_timestamp);
}

void InMemoryStorage::AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                                const std::set<PropertyId> &properties,
                                                uint64_t final_commit_timestamp) {
  return AppendToWalDataDefinition(operation, label, properties, {}, final_commit_timestamp);
}

void InMemoryStorage::AppendToWalDataDefinition(durability::StorageMetadataOperation operation, LabelId label,
                                                uint64_t final_commit_timestamp) {
  return AppendToWalDataDefinition(operation, label, {}, {}, final_commit_timestamp);
}

utils::BasicResult<InMemoryStorage::CreateSnapshotError> InMemoryStorage::CreateSnapshot() {
  auto const &epoch = repl_storage_state_.epoch_;
  auto snapshot_creator = [this, &epoch]() {
    utils::Timer timer;
    auto transaction = CreateTransaction(IsolationLevel::SNAPSHOT_ISOLATION, storage_mode_);
    durability::CreateSnapshot(this, &transaction, snapshot_directory_, wal_directory_, &vertices_, &edges_, uuid_,
                               epoch, repl_storage_state_.history, &file_retainer_);
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
      std::shared_lock storage_guard(main_lock_);
      if (storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
        snapshot_creator();
        return {};
      }
    } else {
      std::unique_lock main_guard{main_lock_};
      if (storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL) {
        snapshot_creator();
        return {};
      }
    }
    should_try_shared = !should_try_shared;
    max_num_tries--;
  }

  return CreateSnapshotError::ReachedMaxNumTries;
}

void InMemoryStorage::FreeMemory(std::unique_lock<utils::ResourceLock> main_guard) {
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

void InMemoryStorage::PrepareForNewEpoch() {
  std::unique_lock engine_guard{engine_lock_};
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }
  repl_storage_state_.TrackLatestHistory();
}

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

std::unique_ptr<Storage::Accessor> InMemoryStorage::Access(std::optional<IsolationLevel> override_isolation_level,
                                                           bool is_main) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{Storage::Accessor::shared_access, this,
                                                                override_isolation_level.value_or(isolation_level_),
                                                                storage_mode_, is_main});
}
std::unique_ptr<Storage::Accessor> InMemoryStorage::UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                                                 bool is_main) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{Storage::Accessor::unique_access, this,
                                                                override_isolation_level.value_or(isolation_level_),
                                                                storage_mode_, is_main});
}

void InMemoryStorage::CreateSnapshotHandler(
    std::function<utils::BasicResult<InMemoryStorage::CreateSnapshotError>()> cb) {
  create_snapshot_handler = [cb]() {
    if (auto maybe_error = cb(); maybe_error.HasError()) {
      switch (maybe_error.GetError()) {
        case CreateSnapshotError::DisabledForReplica:
          spdlog::warn(utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
          break;
        case CreateSnapshotError::ReachedMaxNumTries:
          spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
          break;
      }
    }
  };

  // Run the snapshot thread (if enabled)
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval,
                         [this]() { this->create_snapshot_handler(); });
  }
}
IndicesInfo InMemoryStorage::InMemoryAccessor::ListAllIndices() const {
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory->indices_.label_index_.get());
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory->indices_.label_property_index_.get());
  return {mem_label_index->ListIndices(), mem_label_property_index->ListIndices()};
}
ConstraintsInfo InMemoryStorage::InMemoryAccessor::ListAllConstraints() const {
  const auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  return {mem_storage->constraints_.existence_constraints_->ListConstraints(),
          mem_storage->constraints_.unique_constraints_->ListConstraints()};
}

void InMemoryStorage::InMemoryAccessor::SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) {
  SetIndexStatsForIndex(static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()), label, stats);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_stats_set, label, stats);
}

void InMemoryStorage::InMemoryAccessor::SetIndexStats(const storage::LabelId &label,
                                                      const storage::PropertyId &property,
                                                      const LabelPropertyIndexStats &stats) {
  SetIndexStatsForIndex(static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()),
                        std::make_pair(label, property), stats);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_stats_set, label, property, stats);
}

bool InMemoryStorage::InMemoryAccessor::DeleteLabelIndexStats(const storage::LabelId &label) {
  const auto res =
      DeleteIndexStatsForIndex<bool>(static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get()), label);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_stats_clear, label);
  return res;
}

std::vector<std::pair<LabelId, PropertyId>> InMemoryStorage::InMemoryAccessor::DeleteLabelPropertyIndexStats(
    const storage::LabelId &label) {
  const auto &res = DeleteIndexStatsForIndex<std::vector<std::pair<LabelId, PropertyId>>>(
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get()), label);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_stats_clear, label);
  return res;
}

}  // namespace memgraph::storage
