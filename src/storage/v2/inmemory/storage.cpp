// Copyright 2025 Memgraph Ltd.
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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <system_error>

#include "dbms/constants.hpp"
#include "flags/experimental.hpp"
#include "memory/global_memory_control.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/metadata_delta.hpp"
#include "storage/v2/replication/replication_transaction.hpp"
#include "storage/v2/schema_info_glue.hpp"
#include "utils/async_timer.hpp"
#include "utils/timer.hpp"

/// REPLICATION ///
#include "dbms/inmemory/replication_handlers.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/event_gauge.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/resource_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/stat.hpp"
#include "utils/temporal.hpp"
#include "utils/variant_helpers.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::metrics {
extern const Event PeakMemoryRes;
extern const Event GCLatency_us;
extern const Event GCSkiplistCleanupLatency_us;
}  // namespace memgraph::metrics

namespace memgraph::storage {
namespace {
constexpr auto ActionToStorageOperation(MetadataDelta::Action const action) -> durability::StorageMetadataOperation {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define add_case(E)              \
  case MetadataDelta::Action::E: \
    return durability::StorageMetadataOperation::E
  switch (action) {
    add_case(LABEL_INDEX_CREATE);
    add_case(LABEL_INDEX_STATS_SET);
    add_case(LABEL_INDEX_STATS_CLEAR);
    add_case(LABEL_INDEX_DROP);
    add_case(LABEL_PROPERTIES_INDEX_CREATE);
    add_case(LABEL_PROPERTIES_INDEX_STATS_SET);
    add_case(LABEL_PROPERTIES_INDEX_DROP);
    add_case(LABEL_PROPERTIES_INDEX_STATS_CLEAR);
    add_case(EDGE_INDEX_CREATE);
    add_case(EDGE_INDEX_DROP);
    add_case(EDGE_PROPERTY_INDEX_CREATE);
    add_case(EDGE_PROPERTY_INDEX_DROP);
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_CREATE);
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_DROP);
    add_case(TEXT_INDEX_CREATE);
    add_case(TEXT_EDGE_INDEX_CREATE);
    add_case(TEXT_INDEX_DROP);
    add_case(EXISTENCE_CONSTRAINT_CREATE);
    add_case(EXISTENCE_CONSTRAINT_DROP);
    add_case(UNIQUE_CONSTRAINT_CREATE);
    add_case(UNIQUE_CONSTRAINT_DROP);
    add_case(TYPE_CONSTRAINT_CREATE);
    add_case(TYPE_CONSTRAINT_DROP);
    add_case(ENUM_CREATE);
    add_case(ENUM_ALTER_ADD);
    add_case(ENUM_ALTER_UPDATE);
    add_case(POINT_INDEX_CREATE);
    add_case(POINT_INDEX_DROP);
    add_case(VECTOR_INDEX_CREATE);
    add_case(VECTOR_EDGE_INDEX_CREATE);
    add_case(VECTOR_INDEX_DROP);
    add_case(TTL_OPERATION);
    default:
      LOG_FATAL("Unknown MetadataDelta::Action");
  }
#undef add_case
}

auto FindEdges(const View view, EdgeTypeId edge_type, const VertexAccessor *from_vertex, VertexAccessor *to_vertex)
    -> Result<EdgesVertexAccessorResult> {
  auto use_out_edges = [](Vertex const *from_vertex, Vertex const *to_vertex) {
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

    // With the potentially cheaper side FindEdges
    const auto out_n = from_vertex->out_edges.size();
    const auto in_n = to_vertex->in_edges.size();
    return out_n <= in_n;
  };

  return use_out_edges(from_vertex->vertex_, to_vertex->vertex_) ? from_vertex->OutEdges(view, {edge_type}, to_vertex)
                                                                 : to_vertex->InEdges(view, {edge_type}, from_vertex);
}

class PeriodicSnapshotObserver : public memgraph::utils::Observer<memgraph::utils::SchedulerInterval> {
 public:
  explicit PeriodicSnapshotObserver(memgraph::utils::Scheduler &scheduler) : scheduler_{&scheduler} {}

  // String HAS to be a valid cron expr
  void Update(const memgraph::utils::SchedulerInterval &in) override {
    scheduler_->SetInterval(in);
    scheduler_->SpinOnce();
  }

 private:
  memgraph::utils::Scheduler *scheduler_;
};

};  // namespace

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

InMemoryStorage::InMemoryStorage(Config config, std::optional<free_mem_fn> free_mem_fn_override,
                                 PlanInvalidatorPtr invalidator,
                                 std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    : Storage(config, config.salient.storage_mode, std::move(invalidator), std::move(database_protector_factory)),
      recovery_{config.durability.storage_directory / durability::kSnapshotDirectory,
                config.durability.storage_directory / durability::kWalDirectory},
      lock_file_path_(config.durability.storage_directory / durability::kLockFile),
      snapshot_periodic_observer_(std::make_shared<PeriodicSnapshotObserver>(snapshot_runner_)),
      global_locker_(file_retainer_.AddLocker()) {
  MG_ASSERT(config.salient.storage_mode != StorageMode::ON_DISK_TRANSACTIONAL,
            "Invalid storage mode sent to InMemoryStorage constructor!");
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit || config_.durability.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(recovery_.snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(recovery_.wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(config_.durability.storage_directory);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_->Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    MG_ASSERT(lock_file_handle_->AcquireLock(),
              "Couldn't acquire lock on the storage directory {}"
              "!\nAnother Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              config_.durability.storage_directory);
  }

  if (config_.durability.recover_on_startup) {
    // Disable ttl until after recovery and role switch / write enabled
    ttl_.SetUserCheck([]() -> bool { return false; });
    // Recover data
    auto info = recovery_.RecoverData(
        uuid(), repl_storage_state_, &vertices_, &edges_, &edges_metadata_, &edge_count_, name_id_mapper_.get(),
        &indices_, &constraints_, config_, &wal_seq_num_, &enum_store_,
        config_.salient.items.enable_schema_info ? &schema_info_.Get() : nullptr,
        [this](Gid edge_gid) { return FindEdge(edge_gid); }, name(), &ttl_);
    if (info) {
      vertex_id_.store(info->next_vertex_id, std::memory_order_release);
      edge_id_.store(info->next_edge_id, std::memory_order_release);
      timestamp_ = std::max(timestamp_, info->next_timestamp);
      CommitTsInfo const new_info{.ldt_ = info->last_durable_timestamp,
                                  .num_committed_txns_ = info->num_committed_txns};
      repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
      spdlog::trace(
          "Recovering last durable timestamp {}. Timestamp recovered to {}. Num committed txns recovered to {}.",
          info->last_durable_timestamp, timestamp_, info->num_committed_txns);
    }
  } else if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
             config_.durability.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root = config_.durability.storage_directory / durability::kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(recovery_.snapshot_directory_, durability::kSnapshotDirectory, "snapshot"),
          std::make_tuple(recovery_.wal_directory_, durability::kWalDirectory, "WAL")}) {
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

  /// ###### From here onwards it is now safe to actually run async tasks ######

  if (free_mem_fn_override) {
    free_memory_func_ = *std::move(free_mem_fn_override);
  } else {
    free_memory_func_ = [this](std::unique_lock<utils::ResourceLock> main_guard, bool periodic) {
      CollectGarbage<true>(std::move(main_guard), periodic);

      // Indices
      static_cast<InMemoryLabelIndex *>(indices_.label_index_.get())->RunGC();
      static_cast<InMemoryLabelPropertyIndex *>(indices_.label_property_index_.get())->RunGC();
      static_cast<InMemoryEdgeTypeIndex *>(indices_.edge_type_index_.get())->RunGC();
      static_cast<InMemoryEdgeTypePropertyIndex *>(indices_.edge_type_property_index_.get())->RunGC();
      static_cast<InMemoryEdgePropertyIndex *>(indices_.edge_property_index_.get())->RunGC();

      // Constraints
      static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get())->RunGC();

      // SkipList is already threadsafe
      edges_metadata_.run_gc();
      vertices_.run_gc();
      edges_.run_gc();

      // Auto-indexer also has a skiplist
      async_indexer_.RunGC();

      // AsyncTimer resources are global, not particularly storage related, more query related
      // At some point in the future this should be scheduled by something else
      utils::AsyncTimer::GCRun();
    };
  }

  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    // TODO: move out of storage have one global gc_runner_
    gc_runner_.SetInterval(config_.gc.interval);
    gc_runner_.Run("Storage GC", [this] { this->FreeMemory({}, true); });
  }
  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }

  flags::run_time::SnapshotPeriodicAttach(snapshot_periodic_observer_);

  async_indexer_.Start(stop_source.get_token(), this);
}

InMemoryStorage::~InMemoryStorage() {
  flags::run_time::SnapshotPeriodicDetach(snapshot_periodic_observer_);

  stop_source.request_stop();

  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Stop();
  }
  {
    // Stop replication (Stop all clients or stop the REPLICA server)
    repl_storage_state_.Reset();
  }
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }
  snapshot_runner_.Stop();
  if (config_.durability.snapshot_on_exit && this->create_snapshot_handler) {
    create_snapshot_handler();
  }
  committed_transactions_.WithLock([](auto &transactions) { transactions.clear(); });
}

InMemoryStorage::InMemoryAccessor::InMemoryAccessor(SharedAccess tag, InMemoryStorage *storage,
                                                    IsolationLevel isolation_level, StorageMode storage_mode,
                                                    StorageAccessType rw_type,
                                                    std::optional<std::chrono::milliseconds> timeout)
    : Accessor(tag, storage, isolation_level, storage_mode, rw_type, timeout),
      config_(storage->config_.salient.items) {}
InMemoryStorage::InMemoryAccessor::InMemoryAccessor(auto tag, InMemoryStorage *storage, IsolationLevel isolation_level,
                                                    StorageMode storage_mode,
                                                    std::optional<std::chrono::milliseconds> timeout)
    : Accessor(tag, storage, isolation_level, storage_mode, timeout), config_(storage->config_.salient.items) {}

InMemoryStorage::InMemoryAccessor::InMemoryAccessor(InMemoryAccessor &&other) noexcept
    : Accessor(std::move(other)), config_(other.config_) {}

InMemoryStorage::InMemoryAccessor::~InMemoryAccessor() {
  if (is_transaction_active_) {
    InMemoryAccessor::Abort();
    // We didn't actually commit
    commit_timestamp_.reset();
  }

  InMemoryAccessor::FinalizeTransaction();
}

VertexAccessor InMemoryStorage::InMemoryAccessor::CreateVertex() {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto gid = mem_storage->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = mem_storage->vertices_.access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto schema_acc = SchemaInfoAccessor(storage_, &transaction_);
  auto [it, inserted] = acc.insert(Vertex{Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");

  if (delta) {
    delta->prev.Set(&*it);
  }
  if (schema_acc) {
    std::visit(utils::Overloaded{[&](SchemaInfo::VertexModifyingAccessor &acc) { acc.CreateVertex(&*it); },
                                 [](auto & /* unused */) { DMG_ASSERT(false, "Using the wrong accessor"); }},
               *schema_acc);
  }
  return {&*it, storage_, &transaction_};
}

std::optional<VertexAccessor> InMemoryStorage::InMemoryAccessor::CreateVertexEx(storage::Gid gid) {
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  atomic_fetch_max_explicit(&mem_storage->vertex_id_, gid.AsUint() + 1, std::memory_order_acq_rel);
  auto acc = mem_storage->vertices_.access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto schema_acc = SchemaInfoAccessor(storage_, &transaction_);
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  if (!inserted) {
    return std::nullopt;
  }
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  if (delta) {
    delta->prev.Set(&*it);
  }
  if (schema_acc) {
    std::visit(utils::Overloaded{[&](SchemaInfo::VertexModifyingAccessor &acc) { acc.CreateVertex(&*it); },
                                 [](auto & /* unused */) { DMG_ASSERT(false, "Using the wrong accessor"); }},
               *schema_acc);
  }
  return VertexAccessor{&*it, storage_, &transaction_};
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
      mem_storage->gc_full_scan_vertices_delete_.store(true, std::memory_order_release);
    }
  }};

  auto const inform_gc_edge_deletion = utils::OnScopeExit{[this, &deleted_edges = deleted_edges]() {
    if (!deleted_edges.empty() && transaction_.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
      mem_storage->gc_full_scan_edges_delete_.store(true, std::memory_order_release);
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

  // This has to be called before any object gets locked
  auto schema_acc = SchemaInfoAccessor(storage_, &transaction_);
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

  transaction_.async_index_helper_.Track(edge_type);

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_edge_types_.try_insert(edge_type);
  }
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto gid = storage::Gid::FromUint(mem_storage->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = mem_storage->edges_.access();
    // SchemaInfo handles edge creation via vertices; add collector here if that evert changes
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    if (delta) {
      delta->prev.Set(&*it);
    }
    if (config_.enable_edges_metadata) {
      auto acc = mem_storage->edges_metadata_.access();
      auto [_, inserted] = acc.insert(EdgeMetadata(gid, from->vertex_));
      MG_ASSERT(inserted, "The edge must be inserted here!");
    }
  }
  utils::AtomicMemoryBlock(
      [this, edge, from_vertex = from_vertex, edge_type = edge_type, to_vertex = to_vertex, &schema_acc]() {
        CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
        from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

        CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
        to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

        transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
        transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

        // Update indices if they exist.
        storage_->indices_.UpdateOnEdgeCreation(from_vertex, to_vertex, edge, edge_type, transaction_);

        // Increment edge count.
        storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

        if (schema_acc) {
          std::visit(utils::Overloaded{[&](SchemaInfo::VertexModifyingAccessor &acc) {
                                         acc.CreateEdge(from_vertex, to_vertex, edge_type);
                                       },
                                       [](auto & /* unused */) { DMG_ASSERT(false, "Using the wrong accessor"); }},
                     *schema_acc);
        }
      });

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

std::optional<EdgeAccessor> InMemoryStorage::InMemoryAccessor::FindEdge(Gid gid, const View view, EdgeTypeId edge_type,
                                                                        VertexAccessor *from_vertex,
                                                                        VertexAccessor *to_vertex) {
  auto res = FindEdges(view, edge_type, from_vertex, to_vertex);
  if (res.HasError()) return std::nullopt;  // TODO: use a Result type

  auto const it = std::invoke([this, gid, &res]() {
    auto const byGid = [gid](EdgeAccessor const &edge_accessor) { return edge_accessor.edge_.gid == gid; };
    auto const byEdgePtr = [gid](EdgeAccessor const &edge_accessor) { return edge_accessor.edge_.ptr->gid == gid; };
    if (config_.properties_on_edges) return std::ranges::find_if(res->edges, byEdgePtr);
    return std::ranges::find_if(res->edges, byGid);
  });

  if (it == res->edges.end()) return std::nullopt;  // TODO: use a Result type

  return *it;
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

  // This has to be called before any object gets locked
  auto schema_acc = SchemaInfoAccessor(storage_, &transaction_);
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

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_edge_types_.try_insert(edge_type);
  }

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  atomic_fetch_max_explicit(&mem_storage->edge_id_, gid.AsUint() + 1, std::memory_order_acq_rel);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = mem_storage->edges_.access();

    // SchemaInfo handles edge creation via vertices; add collector here if that evert changes
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    if (delta) {
      delta->prev.Set(&*it);
    }
    if (config_.enable_edges_metadata) {
      auto acc = mem_storage->edges_metadata_.access();
      auto [_, inserted] = acc.insert(EdgeMetadata(gid, from->vertex_));
      MG_ASSERT(inserted, "The edge must be inserted here!");
    }
  }
  utils::AtomicMemoryBlock(
      [this, edge, from_vertex = from_vertex, edge_type = edge_type, to_vertex = to_vertex, &schema_acc]() {
        CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
        from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

        CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
        to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

        transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
        transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

        // Update indices if they exist.
        storage_->indices_.UpdateOnEdgeCreation(from_vertex, to_vertex, edge, edge_type, transaction_);

        // Increment edge count.
        storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

        if (schema_acc) {
          std::visit(utils::Overloaded{[&](SchemaInfo::VertexModifyingAccessor &acc) {
                                         acc.CreateEdge(from_vertex, to_vertex, edge_type);
                                       },
                                       [](auto & /* unused */) { DMG_ASSERT(false, "Using the wrong accessor"); }},
                     *schema_acc);
        }
      });

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

void InMemoryStorage::UpdateEdgesMetadataOnModification(Edge *edge, Vertex *from_vertex) {
  auto edge_metadata_acc = edges_metadata_.access();
  auto edge_to_modify = edge_metadata_acc.find(edge->gid);
  if (edge_to_modify == edge_metadata_acc.end()) {
    throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
  }
  edge_to_modify->from_vertex = from_vertex;
}

std::optional<ConstraintViolation> InMemoryStorage::InMemoryAccessor::ExistenceConstraintsViolation() const {
  // ExistenceConstraints validation block
  auto const has_any_existence_constraints = !storage_->constraints_.existence_constraints_->empty();
  if (has_any_existence_constraints && transaction_.constraint_verification_info &&
      transaction_.constraint_verification_info->NeedsExistenceConstraintVerification()) {
    const auto vertices_to_update =
        transaction_.constraint_verification_info->GetVerticesForExistenceConstraintChecking();
    for (auto const *vertex : vertices_to_update) {
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      if (auto validation_result = storage_->constraints_.existence_constraints_->Validate(*vertex);
          validation_result.has_value()) {
        return validation_result;
      }
    }
  }
  return std::nullopt;
}

std::optional<ConstraintViolation> InMemoryStorage::InMemoryAccessor::UniqueConstraintsViolation() const {
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());

  auto const has_any_unique_constraints = !storage_->constraints_.unique_constraints_->empty();
  if (has_any_unique_constraints && transaction_.constraint_verification_info &&
      transaction_.constraint_verification_info->NeedsUniqueConstraintVerification()) {
    // Before committing and validating vertices against unique constraints,
    // we have to update unique constraints with the vertices that are going
    // to be validated/committed.
    const auto vertices_to_update = transaction_.constraint_verification_info->GetVerticesForUniqueConstraintChecking();

    for (auto const *vertex : vertices_to_update) {
      mem_unique_constraints->UpdateBeforeCommit(vertex, transaction_);
    }

    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

    // Hold accessor to prevent deletion of vertices while validating unique constraints. Otherwise, some previously
    // aborted txn could delete one of vertices being deleted.
    auto acc = mem_storage->vertices_.access();

    for (auto const *vertex : vertices_to_update) {
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      if (auto const maybe_unique_constraint_violation =
              mem_unique_constraints->Validate(*vertex, transaction_, *commit_timestamp_);
          maybe_unique_constraint_violation.has_value()) {
        return maybe_unique_constraint_violation;
      }
    }
  }
  return std::nullopt;
}

void InMemoryStorage::InMemoryAccessor::CheckForFastDiscardOfDeltas() {
  // while still holding engine lock and after durability + replication,
  // check if we can fast discard deltas (i.e. do not hand over to GC)
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  bool const no_older_transactions = mem_storage->commit_log_->OldestActive() == *commit_timestamp_;
  bool const no_newer_transactions = mem_storage->transaction_id_ == transaction_.transaction_id + 1;
  if (no_older_transactions && no_newer_transactions) [[unlikely]] {
    // STEP 0) Can only do fast discard if GC is not running
    //         We can't unlink our transactions deltas until all the older deltas in GC have been unlinked
    //         must do a try here, to avoid deadlock between transactions `engine_lock_` and the GC `gc_lock_`
    auto gc_guard = std::unique_lock{mem_storage->gc_lock_, std::defer_lock};
    if (gc_guard.try_lock()) {
      FastDiscardOfDeltas(std::move(gc_guard));
    }
  }
}

void InMemoryStorage::InMemoryAccessor::AbortAndResetCommitTs() {
  Abort();
  // We have aborted, need to release/cleanup commit_timestamp_ here
  DMG_ASSERT(commit_timestamp_.has_value());
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  mem_storage->commit_log_->MarkFinished(*commit_timestamp_);
  commit_timestamp_.reset();
}

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageManipulationError> InMemoryStorage::InMemoryAccessor::PrepareForCommitPhase(
    CommitArgs const commit_args) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // TODO: duplicated transaction finalization in md_deltas and deltas processing cases
  if (transaction_.deltas.empty() && transaction_.md_deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
    return {};
  }

  // This is usually done by the MVCC, but it does not handle the metadata deltas
  transaction_.EnsureCommitTimestampExists();

  // On REPLICA, user transactions shouldn't commit anything
  if (!commit_args.durability_allowed()) [[unlikely]] {
    Abort();
    // We have not started a commit timestamp no cleanup needed for that
    DMG_ASSERT(!commit_timestamp_.has_value());
    return StorageManipulationError{ReplicaShouldNotWriteError{}};
  }

  if (auto const maybe_violation = ExistenceConstraintsViolation(); maybe_violation.has_value()) {
    Abort();
    // We have not started a commit timestamp no cleanup needed for that
    DMG_ASSERT(!commit_timestamp_.has_value());
    return StorageManipulationError{*maybe_violation};
  }

  auto engine_guard = std::unique_lock{storage_->engine_lock_};
  commit_timestamp_.emplace(mem_storage->GetCommitTimestamp());

  // Unique constraints violated
  if (auto const maybe_unique_constraint_violation = UniqueConstraintsViolation();
      maybe_unique_constraint_violation.has_value()) {
    // Release engine lock because we don't have to hold it anymore
    engine_guard.unlock();
    AbortAndResetCommitTs();
    return StorageManipulationError{*maybe_unique_constraint_violation};
  }
  // Currently there are queries that write to some subsystem that are allowed on a replica
  // ex. analyze graph stats
  // There are probably others. We not to check all of them and figure out if they are allowed and what are
  // they even doing here...

  // Write transaction to WAL while holding the engine lock to make sure
  // that committed transactions are sorted by the commit timestamp in the
  // WAL files. We supply the new commit timestamp to the function so that
  // it knows what will be the final commit timestamp. The WAL must be
  // written before actually committing the transaction (before setting
  // the commit timestamp) so that no other transaction can see the
  // modifications before they are written to disk.
  // Replica can log only the write transaction received from main
  // so the wal files are consistent
  auto const durability_commit_timestamp = commit_args.durable_timestamp(*commit_timestamp_);

  // Specific case in which durability mode is != PERIODIC_SNAPSHOT_WITH_WAL
  if (!mem_storage->InitializeWalFile(mem_storage->repl_storage_state_.epoch_)) {
    FinalizeCommitPhase(durability_commit_timestamp);
    // No WAL file, hence no need to finalize it
    return {};
  }

  // If replica executes this, it will return immediately because it doesn't have any replicas registered (no
  // streams to obtain)
  auto replicating_txn =
      mem_storage->repl_storage_state_.StartPrepareCommitPhase(durability_commit_timestamp, mem_storage, commit_args);

  // If main executes this: Block until we receive votes from all replicas.
  // If replica executes this:,
  bool const repl_prepare_phase_status =
      HandleDurabilityAndReplicate(durability_commit_timestamp, replicating_txn, commit_args);

  // If replica executes this
  bool const replica_write_was_applied =
      commit_args.apply_if_replica_write([&](bool two_phase_commit, uint64_t /*desired_commit_timestamp*/) {
        // If SYNC and ASYNC replica executes this, commit immediately while holding the engine lock
        if (!two_phase_commit) {
          // WAL file is already finalized
          FinalizeCommitPhase(durability_commit_timestamp);
        }
      });
  if (replica_write_was_applied) {
    // If STRICT_SYNC replica with write txn executes this: return because the 2nd phase will be executed once we
    // receive FinalizeCommitRpc.
    return {};
  }

  auto res = commit_args.apply_if_main(
      [&](DatabaseProtector const &protector) -> utils::BasicResult<StorageManipulationError> {
        // From this point on, only main executes this
        // If there are no STRICT_SYNC replicas for the current txn
        if (!replicating_txn.ShouldRunTwoPC()) {
          // WAL file is already finalized
          FinalizeCommitPhase(durability_commit_timestamp);
          // Throw exception if we couldn't commit on one of SYNC replica
          if (!repl_prepare_phase_status) {
            return StorageManipulationError{SyncReplicationError{}};
          }
          return {};
        }

        // If we are here, it means we are the main executing the commit and there are some STRICT_SYNC replicas in the
        // cluster.
        if (repl_prepare_phase_status) {
          // All replicas voted yes, hence they want to commit the current transaction
          FinalizeCommitPhase(durability_commit_timestamp);
        }
        // We need to finalize WAL file after running FinalizeCommitPhase because we update there commit value in WAL

        if (mem_storage->wal_file_) {
          mem_storage->FinalizeWalFile();
        }
        // Send to all replicas they can finalize a transaction
        replicating_txn.FinalizeTransaction(repl_prepare_phase_status, mem_storage->uuid(), protector,
                                            durability_commit_timestamp);

        if (!repl_prepare_phase_status) {
          // Release engine lock because we don't have to hold it anymore for abort
          engine_guard.unlock();
          AbortAndResetCommitTs();

          return StorageManipulationError{StrictSyncReplicationError{}};
        }

        return {};
      });
  DMG_ASSERT(res, "The commit was not applied!");
  return *std::move(res);
}

void InMemoryStorage::InMemoryAccessor::FinalizeCommitPhase(uint64_t const durability_commit_timestamp) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  if (config_.enable_schema_info) {
    mem_storage->schema_info_.ProcessTransaction(transaction_.schema_diff_, transaction_.post_process_,
                                                 transaction_.start_timestamp, transaction_.transaction_id,
                                                 mem_storage->config_.salient.items.properties_on_edges);
  }

  // We only need to update commit flag from false->true if we are running 2PC. In all other situations, the default
  // is fine.
  if (commit_flag_wal_position_ != 0 && needs_wal_update_) {
    constexpr bool commit{true};
    mem_storage->wal_file_->UpdateCommitStatus(commit_flag_wal_position_, commit);
  }

  MG_ASSERT(transaction_.commit_timestamp != nullptr, "Invalid database state!");
  transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);

#ifndef NDEBUG
  auto const prev = mem_storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
  DMG_ASSERT(durability_commit_timestamp >= prev, "LDT not monotonically increasing");
#endif

  auto const update_func = [durability_commit_timestamp](CommitTsInfo const &old_ts_info) -> CommitTsInfo {
    return CommitTsInfo{.ldt_ = durability_commit_timestamp,
                        .num_committed_txns_ = old_ts_info.num_committed_txns_ + 1};
  };
  atomic_struct_update<CommitTsInfo>(mem_storage->repl_storage_state_.commit_ts_info_, update_func);

  // Install the new point index, if needed
  mem_storage->indices_.point_index_.InstallNewPointIndex(transaction_.point_index_change_collector_,
                                                          transaction_.point_index_ctx_);

  // Call other callbacks that publish/install upon commit
  transaction_.commit_callbacks_.RunAll(*commit_timestamp_);

  // Dispatch to another async work to create requested auto-indexes in their own transaction
  if (mem_storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    transaction_.async_index_helper_.DispatchRequests(mem_storage->async_indexer_);
  }
  // TODO: can and should this be moved earlier?
  mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  CheckForFastDiscardOfDeltas();
  memgraph::storage::TextIndex::ApplyTrackedChanges(transaction_, mem_storage->name_id_mapper_.get());
  memgraph::storage::TextEdgeIndex::ApplyTrackedChanges(transaction_, mem_storage->name_id_mapper_.get());
  is_transaction_active_ = false;
}

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageManipulationError, void> InMemoryStorage::InMemoryAccessor::PeriodicCommit(
    CommitArgs commit_args) {
  auto result = PrepareForCommitPhase(std::move(commit_args));

  const auto fatal_error =
      result.HasError() &&
      std::visit(
          [](const auto &e) {
            // All errors are handled at a higher level.
            // Replication errros are not fatal and should procede with finialize transaction
            return !std::is_same_v<std::remove_cvref_t<decltype(e)>, storage::SyncReplicationError>;
          },
          result.GetError());
  if (fatal_error) {
    return result;
  }

  FinalizeTransaction();

  auto original_start_timestamp = transaction_.original_start_timestamp.value_or(transaction_.start_timestamp);

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  auto new_transaction = mem_storage->CreateTransaction(transaction_.isolation_level, transaction_.storage_mode);
  transaction_.start_timestamp = new_transaction.start_timestamp;
  transaction_.transaction_id = new_transaction.transaction_id;
  transaction_.commit_timestamp.reset();
  transaction_.original_start_timestamp = original_start_timestamp;

  is_transaction_active_ = true;

  return result;
}

void InMemoryStorage::InMemoryAccessor::GCRapidDeltaCleanup(std::list<Gid> &current_deleted_edges,
                                                            std::list<Gid> &current_deleted_vertices,
                                                            IndexPerformanceTracker &impact_tracker) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  auto const unlink_remove_clear = [&](delta_container &deltas) {
    for (auto &delta : deltas) {
      impact_tracker.update(delta.action);
      auto prev = delta.prev.Get();
      switch (prev.type) {
        case PreviousPtr::Type::NULLPTR:
        case PreviousPtr::Type::DELTA:
          break;
        case PreviousPtr::Type::VERTEX: {
          // safe because no other txn can be reading this while we have engine lock
          auto &vertex = *prev.vertex;
          vertex.delta = nullptr;
          if (vertex.deleted) {
            DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
            current_deleted_vertices.push_back(vertex.gid);
          }
          break;
        }
        case PreviousPtr::Type::EDGE: {
          // safe because no other txn can be reading this while we have engine lock
          auto &edge = *prev.edge;
          edge.delta = nullptr;
          if (edge.deleted) {
            DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
            current_deleted_edges.push_back(edge.gid);
          }
          break;
        }
      }
    }

    // delete deltas
    deltas.clear();
  };

  // STEP 1) ensure everything in GC is gone

  // 1.a) old garbage_undo_buffers are safe to remove
  //      we are the only transaction, no one is reading those unlinked deltas
  mem_storage->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) { garbage_undo_buffers.clear(); });

  // 1.b.0) old committed_transactions_ need minimal unlinking + remove + clear
  //      must be done before this transactions delta unlinking
  auto linked_undo_buffers = std::list<GCDeltas>{};
  mem_storage->committed_transactions_.WithLock(
      [&](auto &committed_transactions) { committed_transactions.swap(linked_undo_buffers); });

  // 1.b.1) unlink, gathering the removals
  for (auto &gc_deltas : linked_undo_buffers) {
    unlink_remove_clear(gc_deltas.deltas_);
  }
  // 1.b.2) clear the list of deltas deques
  linked_undo_buffers.clear();

  // STEP 2) this transactions deltas also mininal unlinking + remove + clear
  unlink_remove_clear(transaction_.deltas);
}

void InMemoryStorage::InMemoryAccessor::FastDiscardOfDeltas(std::unique_lock<std::mutex> /*gc_guard*/) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // STEP 1 + STEP 2 - delta cleanup
  std::list<Gid> current_deleted_vertices;
  std::list<Gid> current_deleted_edges;
  auto impact_tracker = IndexPerformanceTracker{};
  GCRapidDeltaCleanup(current_deleted_edges, current_deleted_vertices, impact_tracker);

  // STEP 3) hand over the deleted vertices and edges to the GC
  if (!current_deleted_vertices.empty()) {
    mem_storage->deleted_vertices_.WithLock(
        [&](auto &deleted_vertices) { deleted_vertices.splice(deleted_vertices.end(), current_deleted_vertices); });
  }
  if (!current_deleted_edges.empty()) {
    mem_storage->deleted_edges_.WithLock(
        [&](auto &deleted_edges) { deleted_edges.splice(deleted_edges.end(), current_deleted_edges); });
  }

  // STEP 4) hint to GC that indices need cleanup for performance reasons
  if (impact_tracker.impacts_vertex_indexes()) {
    mem_storage->gc_index_cleanup_vertex_performance_.store(true, std::memory_order_release);
  }
  if (impact_tracker.impacts_edge_indexes()) {
    mem_storage->gc_index_cleanup_edge_performance_.store(true, std::memory_order_release);
  }
}

void InMemoryStorage::InMemoryAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // if we have no deltas then no need to do any undo work during Abort
  // note: this check also saves on unnecessary contention on `engine_lock_`
  if (!transaction_.deltas.empty()) {
    auto index_abort_processor = storage_->indices_.GetAbortProcessor(transaction_.active_indices_);

    auto const has_any_unique_constraints = !storage_->constraints_.unique_constraints_->empty();
    if (has_any_unique_constraints && transaction_.constraint_verification_info &&
        transaction_.constraint_verification_info->NeedsUniqueConstraintVerification()) {
      // Need to remove elements from constraints before handling of the deltas, so the elements match the correct
      // values
      auto vertices_to_check = transaction_.constraint_verification_info->GetVerticesForUniqueConstraintChecking();
      auto vertices_to_check_v = std::vector<Vertex const *>{vertices_to_check.begin(), vertices_to_check.end()};
      storage_->constraints_.AbortEntries(vertices_to_check_v, transaction_.start_timestamp);
    }

    // We collect vertices and edges we've created here and then splice them into
    // `deleted_vertices_` and `deleted_edges_` lists, instead of adding them one
    // by one and acquiring lock every time.
    std::vector<Gid> my_deleted_vertices;
    std::vector<Gid> my_deleted_edges;

    std::map<LabelPropKey, std::vector<Vertex *>> vector_label_property_cleanup;
    std::map<LabelPropKey, std::vector<std::pair<PropertyValue, Vertex *>>> vector_label_property_restore;
    std::map<EdgeTypePropKey,
             std::vector<std::pair<PropertyValue, std::tuple<Vertex *const, Vertex *const, Edge *const>>>>
        vector_edge_type_property_restore;  // No need to cleanup, because edge type can't be removed and when null
                                            // property is set, it's like removing the property from the index

    // TWO passes needed here
    // Abort will modify objects to restore state to how they were before this txn
    // The passes will find the head delta for each object and process the whole object,
    // To track which edge type indexes need cleaning up, we need the edge type which is held in vertices in/out edges
    // Hence need to first once to modify edges, so it can read vectices information intact.

    // Edges pass
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      switch (prev.type) {
        case PreviousPtr::Type::EDGE: {
          auto *edge = prev.edge;
          auto guard = std::lock_guard{edge->lock};
          Delta *current = edge->delta;
          while (current != nullptr &&
                 current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
            switch (current->action) {
              case Delta::Action::SET_PROPERTY: {
                DMG_ASSERT(mem_storage->config_.salient.items.properties_on_edges, "Invalid database state!");

                auto prop_id = current->property.key;
                auto from_vertex = current->property.out_vertex;

                const auto &vector_indexed_edge_types = index_abort_processor.vector_edge_.p2et.find(prop_id);
                auto vec_prop_is_interesting =
                    vector_indexed_edge_types != index_abort_processor.vector_edge_.p2et.end();

                auto processor_prop_is_interesting = index_abort_processor.IsInterestingEdgeProperty(prop_id);
                if (processor_prop_is_interesting || vec_prop_is_interesting) {
                  // TODO: MVCC collect out_edges (including ones deleted this txn)
                  //       from_vertex->out_edges would be missing any edge that was deleted during this transaction
                  //       ATM we don't handle that corner case. Setting a property on an edge that would then be
                  //       removed

                  if (processor_prop_is_interesting) {
                    for (auto const &[edge_type, to_vertex, edge_ref] : from_vertex->out_edges) {
                      if (edge_ref.ptr != edge) continue;
                      index_abort_processor.CollectOnPropertyChange(edge_type, prop_id, from_vertex, to_vertex, edge);
                    }
                  }

                  // Collect edge vector
                  if (vec_prop_is_interesting) {
                    // TODO: Fix out_edges will be missing the edge if it was deleted during this transaction
                    for (auto const &[edge_type, to_vertex, edge_ref] : from_vertex->out_edges) {
                      if (edge_ref.ptr != edge) continue;
                      // handle vector index -> we need to check if the edge type is indexed in the vector index
                      if (r::find(vector_indexed_edge_types->second, edge_type) !=
                          vector_indexed_edge_types->second.end()) {
                        // this edge type is indexed in the vector index
                        vector_edge_type_property_restore[EdgeTypePropKey{edge_type, prop_id}].emplace_back(
                            *current->property.value, std::make_tuple(from_vertex, to_vertex, edge));
                      }
                    }
                  }
                }

                edge->properties.SetProperty(prop_id, *current->property.value);

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
              case Delta::Action::SET_VECTOR_PROPERTY: {
                // Do nothing, vector index doesn't have transactional guarantees
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
        case PreviousPtr::Type::VERTEX:
        case PreviousPtr::Type::DELTA:
        // pointer probably couldn't be set because allocation failed
        case PreviousPtr::Type::NULLPTR:
          break;
      }
    }

    // Vertices pass
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      switch (prev.type) {
        case PreviousPtr::Type::VERTEX: {
          auto *vertex = prev.vertex;
          auto guard = std::unique_lock{vertex->lock};
          Delta *current = vertex->delta;

          auto remove_in_edges = absl::flat_hash_set<EdgeRef>{};
          auto remove_out_edges = absl::flat_hash_set<EdgeRef>{};

          while (current != nullptr &&
                 current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
            switch (current->action) {
              case Delta::Action::REMOVE_LABEL: {
                auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label.value);
                MG_ASSERT(it != vertex->labels.end(), "Invalid database state!");
                std::swap(*it, *vertex->labels.rbegin());
                vertex->labels.pop_back();

                index_abort_processor.CollectOnLabelRemoval(current->label.value, vertex);

                // we have to remove the vertex from the vector index if this label is indexed and vertex has
                // needed property
                const auto &vector_properties = index_abort_processor.vector_.l2p.find(current->label.value);
                if (vector_properties != index_abort_processor.vector_.l2p.end()) {
                  // label is in the vector index
                  for (const auto &property : vector_properties->second) {
                    if (vertex->properties.HasProperty(property)) {
                      // it has to be removed from the index
                      vector_label_property_cleanup[LabelPropKey{current->label.value, property}].emplace_back(vertex);
                    }
                  }
                }
                break;
              }
              case Delta::Action::ADD_LABEL: {
                auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label.value);
                MG_ASSERT(it == vertex->labels.end(), "Invalid database state!");
                vertex->labels.push_back(current->label.value);
                // we have to add the vertex to the vector index if this label is indexed and vertex has needed
                // property
                const auto &vector_properties = index_abort_processor.vector_.l2p.find(current->label.value);
                if (vector_properties != index_abort_processor.vector_.l2p.end()) {
                  // label is in the vector index
                  for (const auto &property : vector_properties->second) {
                    auto current_value = vertex->properties.GetProperty(property);
                    if (!current_value.IsNull()) {
                      // it has to be added to the index
                      vector_label_property_restore[LabelPropKey{current->label.value, property}].emplace_back(
                          std::move(current_value), vertex);
                    }
                  }
                }
                break;
              }
              case Delta::Action::SET_PROPERTY: {
                // For label index nothing
                // For property label index
                //  check if we care about the property, this will return all the labels and then get current property
                //  value
                index_abort_processor.CollectOnPropertyChange(current->property.key, vertex);

                const auto &vector_index_labels = index_abort_processor.vector_.p2l.find(current->property.key);
                const auto has_vector_index = vector_index_labels != index_abort_processor.vector_.p2l.end();
                if (has_vector_index) {
                  auto has_indexed_label = [&vector_index_labels](auto label) {
                    return std::binary_search(vector_index_labels->second.begin(), vector_index_labels->second.end(),
                                              label);
                  };
                  auto indexed_labels_on_vertex =
                      vertex->labels | rv::filter(has_indexed_label) | r::to<std::vector<LabelId>>();

                  for (const auto &label : indexed_labels_on_vertex) {
                    vector_label_property_restore[LabelPropKey{label, current->property.key}].emplace_back(
                        *current->property.value, vertex);
                  }
                }
                // Setting the correct value
                vertex->properties.SetProperty(current->property.key, *current->property.value);
                break;
              }
              case Delta::Action::ADD_IN_EDGE: {
                auto link =
                    std::tuple{current->vertex_edge.edge_type, current->vertex_edge.vertex, current->vertex_edge.edge};
                DMG_ASSERT(std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link) == vertex->in_edges.end(),
                           "Invalid database state!");
                vertex->in_edges.push_back(link);
                break;
              }
              case Delta::Action::ADD_OUT_EDGE: {
                auto link =
                    std::tuple{current->vertex_edge.edge_type, current->vertex_edge.vertex, current->vertex_edge.edge};
                DMG_ASSERT(
                    std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link) == vertex->out_edges.end(),
                    "Invalid database state!");
                vertex->out_edges.push_back(link);
                // Increment edge count. We only increment the count here because
                // the information in `ADD_IN_EDGE` and `Edge/RECREATE_OBJECT` is
                // redundant. Also, `Edge/RECREATE_OBJECT` isn't available when
                // edge properties are disabled.
                storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);
                break;
              }
              case Delta::Action::REMOVE_IN_EDGE: {
                // EdgeRef is unique
                remove_in_edges.insert(current->vertex_edge.edge);
                break;
              }
              case Delta::Action::REMOVE_OUT_EDGE: {
                // EdgeRef is unique
                remove_out_edges.insert(current->vertex_edge.edge);

                // Decrement edge count. We only decrement the count here because
                // the information in `REMOVE_IN_EDGE` and `Edge/DELETE_OBJECT` is
                // redundant. Also, `Edge/DELETE_OBJECT` isn't available when edge
                // properties are disabled.
                storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);

                // TODO: Change edge type index to work with EdgeRef rather than Edge *
                if (!mem_storage->config_.salient.items.properties_on_edges) break;

                auto const &[_, edge_type, to_vertex, edge] = current->vertex_edge;
                index_abort_processor.CollectOnEdgeRemoval(edge_type, vertex, to_vertex, edge.ptr);
                // TODO: ensure collector also processeses for edge_type+property index

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
              case Delta::Action::SET_VECTOR_PROPERTY: {
                // Do nothing, vector index doesn't have transactional guarantees
                break;
              }
            }
            current = current->next.load(std::memory_order_acquire);
          }

          // bulk remove in_edges
          if (!remove_in_edges.empty()) {
            auto mid = std::partition(vertex->in_edges.begin(), vertex->in_edges.end(), [&](auto const &edge_tuple) {
              return !remove_in_edges.contains(std::get<EdgeRef>(edge_tuple));
            });
            vertex->in_edges.erase(mid, vertex->in_edges.end());
            vertex->in_edges.shrink_to_fit();
          }

          // bulk remove out_edges
          if (!remove_out_edges.empty()) {
            auto mid = std::partition(vertex->out_edges.begin(), vertex->out_edges.end(), [&](auto const &edge_tuple) {
              return !remove_out_edges.contains(std::get<EdgeRef>(edge_tuple));
            });
            vertex->out_edges.erase(mid, vertex->out_edges.end());
            vertex->out_edges.shrink_to_fit();
          }

          vertex->delta = current;
          if (current != nullptr) {
            current->prev.Set(vertex);
          }

          break;
        }
        case PreviousPtr::Type::EDGE:
        case PreviousPtr::Type::DELTA:
        // pointer probably couldn't be set because allocation failed
        case PreviousPtr::Type::NULLPTR:
          break;
      }
    }

    {
      auto engine_guard = std::unique_lock(storage_->engine_lock_);
      uint64_t mark_timestamp = storage_->timestamp_;  // a timestamp no active transaction can currently have

      // Take garbage_undo_buffers lock while holding the engine lock to make
      // sure that entries are sorted by mark timestamp in the list.
      mem_storage->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
        // Release engine lock because we don't have to hold it anymore and
        // emplace back could take a long time.
        engine_guard.unlock();

        garbage_undo_buffers.emplace_back(mark_timestamp, std::move(transaction_.deltas),
                                          std::move(transaction_.commit_timestamp));
      });
    }

    /// We MUST unlink (aka. remove) entries in indexes and constraints
    /// before we unlink (aka. remove) vertices from storage
    /// this is because they point into vertices skip_list

    // Cleanup INDICES
    index_abort_processor.Process(storage_->indices_, transaction_.active_indices_, transaction_.start_timestamp);

    for (auto const &[label_prop, vertices] : vector_label_property_cleanup) {
      storage_->indices_.vector_index_.AbortEntries(label_prop, vertices);
    }
    for (auto const &[label_prop, prop_vertices] : vector_label_property_restore) {
      storage_->indices_.vector_index_.RestoreEntries(label_prop, prop_vertices);
    }
    for (auto const &[edge_type_prop, prop_edges] : vector_edge_type_property_restore) {
      storage_->indices_.vector_edge_index_.RestoreEntries(edge_type_prop, prop_edges);
    }

    // EDGES METADATA (has ptr to Vertices, must be before removing verticies)
    if (!my_deleted_edges.empty() && mem_storage->config_.salient.items.enable_edges_metadata) {
      auto edges_metadata_acc = mem_storage->edges_metadata_.access();
      for (auto gid : my_deleted_edges) {
        edges_metadata_acc.remove(gid);
      }
    }

    // VERTICES (has ptr to Edges, must be before removing edges)
    if (!my_deleted_vertices.empty()) {
      auto acc = mem_storage->vertices_.access();
      for (auto gid : my_deleted_vertices) {
        acc.remove(gid);
      }
    }

    // EDGES
    if (!my_deleted_edges.empty()) {
      auto edges_acc = mem_storage->edges_.access();
      for (auto gid : my_deleted_edges) {
        edges_acc.remove(gid);
      }
    }
  }

  mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
}

void InMemoryStorage::InMemoryAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
    mem_storage->commit_log_->MarkFinished(*commit_timestamp_);

    if (!transaction_.deltas.empty()) {
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

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateIndex(
    LabelId label, CheckCancelFunction cancel_check) {
  // UNIQUE access is also required by schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating label index requires a unique or read only access to the storage!");

  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get());
  if (!mem_label_index->RegisterIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionAlreadyExistsError{}};
  }
  DowngradeToReadIfValid();
  if (mem_label_index
          ->PopulateIndex(label, in_memory->vertices_.access(), std::nullopt, std::nullopt, &transaction_,
                          std::move(cancel_check))
          .HasError()) {
    return StorageIndexDefinitionError{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared

  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_label_index->PublishIndex(label, commit_timestamp); });

  transaction_.commit_callbacks_.Add(std::move(publisher));

  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_create, label);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

auto InMemoryStorage::InMemoryAccessor::CreateIndex(LabelId label, PropertiesPaths properties,
                                                    CheckCancelFunction cancel_check)
    -> utils::BasicResult<StorageIndexDefinitionError, void> {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating label-property index requires a unique or read only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  if (!mem_label_property_index->RegisterIndex(label, properties)) {
    return StorageIndexDefinitionError{IndexDefinitionAlreadyExistsError{}};
  }
  DowngradeToReadIfValid();
  if (mem_label_property_index
          ->PopulateIndex(label, properties, in_memory->vertices_.access(), std::nullopt, std::nullopt, &transaction_,
                          std::move(cancel_check))
          .HasError()) {
    return StorageIndexDefinitionError{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper([=](uint64_t commit_timestamp) {
    return mem_label_property_index->PublishIndex(label, properties, commit_timestamp);
  });
  transaction_.commit_callbacks_.Add(std::move(publisher));

  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_create, label, std::move(properties));
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

void InMemoryStorage::InMemoryAccessor::DowngradeToReadIfValid() {
  if (storage_guard_.owns_lock() && storage_guard_.type() == utils::SharedResourceLockGuard::Type::READ_ONLY) {
    storage_guard_.downgrade_to_read();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateIndex(
    EdgeTypeId edge_type, CheckCancelFunction cancel_check) {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Create index requires a unique or readonly access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(in_memory->indices_.edge_type_index_.get());
  if (!mem_edge_type_index->RegisterIndex(edge_type)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (mem_edge_type_index
          ->PopulateIndex(edge_type, in_memory->vertices_.access(), std::nullopt, &transaction_,
                          std::move(cancel_check))
          .HasError()) {
    return StorageIndexDefinitionError{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_edge_type_index->PublishIndex(edge_type, commit_timestamp); });
  transaction_.commit_callbacks_.Add(std::move(publisher));

  transaction_.md_deltas.emplace_back(MetadataDelta::edge_index_create, edge_type);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateIndex(
    EdgeTypeId edge_type, PropertyId property, CheckCancelFunction cancel_check) {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY, "Create index requires a unique access to the storage!");

  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return StorageIndexDefinitionError{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_type_property_index =
      static_cast<InMemoryEdgeTypePropertyIndex *>(in_memory->indices_.edge_type_property_index_.get());
  if (!mem_edge_type_property_index->RegisterIndex(edge_type, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (mem_edge_type_property_index
          ->PopulateIndex(edge_type, property, in_memory->vertices_.access(), std::nullopt, &transaction_,
                          std::move(cancel_check))
          .HasError()) {
    return StorageIndexDefinitionError{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper([=](uint64_t commit_timestamp) {
    return mem_edge_type_property_index->PublishIndex(edge_type, property, commit_timestamp);
  });
  transaction_.commit_callbacks_.Add(std::move(publisher));

  transaction_.md_deltas.emplace_back(MetadataDelta::edge_property_index_create, edge_type, property);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateGlobalEdgeIndex(
    PropertyId property, CheckCancelFunction cancel_check) {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Create index requires a unique or read-only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return StorageIndexDefinitionError{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_property_index =
      static_cast<InMemoryEdgePropertyIndex *>(in_memory->indices_.edge_property_index_.get());
  if (!mem_edge_property_index->RegisterIndex(property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (mem_edge_property_index
          ->PopulateIndex(property, in_memory->vertices_.access(), std::nullopt, &transaction_, std::move(cancel_check))
          .HasError()) {
    return StorageIndexDefinitionError{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_edge_property_index->PublishIndex(property, commit_timestamp); });
  transaction_.commit_callbacks_.Add(std::move(publisher));

  transaction_.md_deltas.emplace_back(MetadataDelta::global_edge_property_index_create, property);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(LabelId label) {
  MG_ASSERT(type() == UNIQUE || type() == READ,
            "Dropping label index requires a unique or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory->indices_.label_index_.get());

  // Done inside the wrapper to ensure plan cache invalidation is safe
  auto was_dropped = storage_->invalidator_->invalidate_now([&] { return mem_label_index->DropIndex(label); });
  if (!was_dropped) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_drop, label);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(
    LabelId label, std::vector<storage::PropertyPath> &&properties) {
  // Because of replication we still use UNIQUE ATM
  MG_ASSERT(type() == UNIQUE || type() == READ,
            "Dropping label-property index requires a unique or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory->indices_.label_property_index_.get());

  // Done inside the wrapper to ensure plan cache invalidation is safe
  auto was_dropped =
      storage_->invalidator_->invalidate_now([&] { return mem_label_property_index->DropIndex(label, properties); });
  if (!was_dropped) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_drop, label, std::move(properties));
  // We don't care if there is a replication error because on main node the change will go through

  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(
    EdgeTypeId edge_type) {
  // Because of replication we still use UNIQUE ATM
  MG_ASSERT(type() == UNIQUE || type() == READ,
            "Dropping edge-type index requires a unique or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(in_memory->indices_.edge_type_index_.get());
  // Done inside the wrapper to ensure plan cache invalidation is safe
  auto was_dropped = storage_->invalidator_->invalidate_now([&] { return mem_edge_type_index->DropIndex(edge_type); });
  if (!was_dropped) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::edge_index_drop, edge_type);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropIndex(
    EdgeTypeId edge_type, PropertyId property) {
  // Because of replication we still use UNIQUE ATM
  MG_ASSERT(type() == UNIQUE || type() == READ,
            "Dropping edge-type property index requires a unique or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to drop the index, no properties on edges
    return StorageIndexDefinitionError{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_type_property_index =
      static_cast<InMemoryEdgeTypePropertyIndex *>(in_memory->indices_.edge_type_property_index_.get());
  // Done inside the wrapper to ensure plan cache invalidation is safe
  auto was_dropped = storage_->invalidator_->invalidate_now(
      [&] { return mem_edge_type_property_index->DropIndex(edge_type, property); });
  if (!was_dropped) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::edge_property_index_drop, edge_type, property);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropGlobalEdgeIndex(
    PropertyId property) {
  MG_ASSERT(type() == UNIQUE || type() == READ, "Drop index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return StorageIndexDefinitionError{IndexDefinitionConfigError{}};
  }

  auto *mem_edge_property_index =
      static_cast<InMemoryEdgePropertyIndex *>(in_memory->indices_.edge_property_index_.get());
  auto was_dropped =
      storage_->invalidator_->invalidate_now([&] { return mem_edge_property_index->DropIndex(property); });
  if (!was_dropped) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  transaction_.md_deltas.emplace_back(MetadataDelta::global_edge_property_index_drop, property);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreatePointIndex(
    storage::LabelId label, storage::PropertyId property) {
  MG_ASSERT(type() == UNIQUE, "Creating point index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &point_index = in_memory->indices_.point_index_;
  if (!point_index.CreatePointIndex(label, property, in_memory->vertices_.access())) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::point_index_create, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActivePointIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropPointIndex(
    storage::LabelId label, storage::PropertyId property) {
  MG_ASSERT(type() == UNIQUE, "Dropping point index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &point_index = in_memory->indices_.point_index_;
  if (!point_index.DropPointIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::point_index_drop, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActivePointIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateVectorIndex(
    VectorIndexSpec spec) {
  MG_ASSERT(type() == UNIQUE, "Creating vector index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  auto vertices_acc = in_memory->vertices_.access();
  // We don't allow creating vector index on nodes with the same name as vector edge index
  if (vector_edge_index.IndexExists(spec.index_name) || !vector_index.CreateIndex(spec, vertices_acc)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_index_create, spec);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveVectorIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::DropVectorIndex(
    std::string_view index_name) {
  MG_ASSERT(type() == UNIQUE, "Dropping vector index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  if (vector_index.DropIndex(index_name)) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveVectorIndices);
  } else if (vector_edge_index.DropIndex(index_name)) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveVectorEdgeIndices);
  } else {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_index_drop, index_name);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

std::optional<std::vector<uint64_t>> InMemoryStorage::InMemoryAccessor::IsPropertyInVectorIndex(Vertex *vertex,
                                                                                                PropertyId property) {
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  return in_memory->indices_.vector_index_.IsPropertyInVectorIndex(vertex, property, in_memory->name_id_mapper_.get());
}

utils::BasicResult<StorageIndexDefinitionError, void> InMemoryStorage::InMemoryAccessor::CreateVectorEdgeIndex(
    VectorEdgeIndexSpec spec) {
  MG_ASSERT(type() == UNIQUE, "Creating vector edge index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  auto vertices_acc = in_memory->vertices_.access();
  // We don't allow creating vector edge index with the same name as vector index on nodes
  if (vector_index.IndexExists(spec.index_name) || !vector_edge_index.CreateIndex(spec, vertices_acc)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_edge_index_create, spec);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveVectorEdgeIndices);
  return {};
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void>
InMemoryStorage::InMemoryAccessor::CreateExistenceConstraint(LabelId label, PropertyId property) {
  MG_ASSERT(type() == UNIQUE, "Creating existence requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *existence_constraints = in_memory->constraints_.existence_constraints_.get();
  if (existence_constraints->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }
  if (auto violation = ExistenceConstraints::ValidateVerticesOnConstraint(in_memory->vertices_.access(), label,
                                                                          property, std::nullopt, std::nullopt);
      violation.has_value()) {
    return StorageExistenceConstraintDefinitionError{violation.value()};
  }
  existence_constraints->InsertConstraint(label, property);
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_create, label, property);
  return {};
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void>
InMemoryStorage::InMemoryAccessor::DropExistenceConstraint(LabelId label, PropertyId property) {
  MG_ASSERT(type() == UNIQUE, "Dropping existence constraint requires a unique access to the storage!");
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
  MG_ASSERT(type() == UNIQUE, "Creating unique constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(in_memory->constraints_.unique_constraints_.get());
  auto ret = mem_unique_constraints->CreateConstraint(label, properties, in_memory->vertices_.access(), std::nullopt);
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
  MG_ASSERT(type() == UNIQUE, "Dropping unique constraint requires a unique access to the storage!");
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

utils::BasicResult<StorageExistenceConstraintDefinitionError, void>
InMemoryStorage::InMemoryAccessor::CreateTypeConstraint(LabelId label, PropertyId property, TypeConstraintKind kind) {
  MG_ASSERT(type() == UNIQUE, "Creating IS TYPED constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *type_constraints = in_memory->constraints_.type_constraints_.get();
  if (type_constraints->ConstraintExists(label, property)) {
    return StorageTypeConstraintDefinitionError{ConstraintDefinitionError{}};
  }
  if (auto violation =
          TypeConstraints::ValidateVerticesOnConstraint(in_memory->vertices_.access(), label, property, kind);
      violation.has_value()) {
    return StorageTypeConstraintDefinitionError{violation.value()};
  }
  type_constraints->InsertConstraint(label, property, kind);
  transaction_.md_deltas.emplace_back(MetadataDelta::type_constraint_create, label, property, kind);
  return {};
}

utils::BasicResult<StorageTypeConstraintDroppingError, void> InMemoryStorage::InMemoryAccessor::DropTypeConstraint(
    LabelId label, PropertyId property, TypeConstraintKind kind) {
  MG_ASSERT(type() == UNIQUE, "Dropping IS TYPED constraint requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *type_constraints = in_memory->constraints_.type_constraints_.get();
  auto deleted_constraint = type_constraints->DropConstraint(label, property, kind);
  if (!deleted_constraint) {
    return StorageTypeConstraintDroppingError{ConstraintDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::type_constraint_drop, label, property, kind);
  return {};
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, View view) {
  auto *active_indices = static_cast<InMemoryLabelIndex::ActiveIndices *>(transaction_.active_indices_.label_.get());
  return VerticesIterable(active_indices->Vertices(label, view, storage_, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(
    LabelId label, std::span<storage::PropertyPath const> properties,
    std::span<storage::PropertyValueRange const> property_ranges, View view) {
  auto *active_indices =
      static_cast<InMemoryLabelPropertyIndex::ActiveIndices *>(transaction_.active_indices_.label_properties_.get());
  return VerticesIterable(active_indices->Vertices(label, properties, property_ranges, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, View view) {
  auto *active_indices =
      static_cast<InMemoryEdgeTypeIndex::ActiveIndices *>(transaction_.active_indices_.edge_type_.get());
  return EdgesIterable(active_indices->Edges(edge_type, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property, View view) {
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_.edge_type_properties_.get());
  return EdgesIterable(
      active_indices->Edges(edge_type, property, std::nullopt, std::nullopt, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property,
                                                       const PropertyValue &value, View view) {
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_.edge_type_properties_.get());
  return EdgesIterable(active_indices->Edges(edge_type, property, utils::MakeBoundInclusive(value),
                                             utils::MakeBoundInclusive(value), view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property,
                                                       const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                       const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                       View view) {
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_.edge_type_properties_.get());
  return EdgesIterable(
      active_indices->Edges(edge_type, property, lower_bound, upper_bound, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property, View view) {
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_.edge_property_.get());
  return EdgesIterable(
      mem_edge_property_active_indices->Edges(property, std::nullopt, std::nullopt, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property, const PropertyValue &value, View view) {
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_.edge_property_.get());
  return EdgesIterable(mem_edge_property_active_indices->Edges(
      property, utils::MakeBoundInclusive(value), utils::MakeBoundInclusive(value), view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property,
                                                       const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                       const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                       View view) {
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_.edge_property_.get());
  return EdgesIterable(
      mem_edge_property_active_indices->Edges(property, lower_bound, upper_bound, view, storage_, &transaction_));
}

std::optional<EdgeAccessor> InMemoryStorage::InMemoryAccessor::FindEdge(Gid gid, View view) {
  const auto maybe_edge_info = static_cast<InMemoryStorage *>(storage_)->FindEdge(gid);
  if (!maybe_edge_info) {
    return std::nullopt;
  }
  const auto &[edge_ref, edge_type, from, to] = *maybe_edge_info;
  return EdgeAccessor::Create(edge_ref, edge_type, from, to, storage_, &transaction_, view);
}

std::optional<EdgeAccessor> InMemoryStorage::InMemoryAccessor::FindEdge(Gid edge_gid, Gid from_vertex_gid, View view) {
  const auto maybe_edge_info = static_cast<InMemoryStorage *>(storage_)->FindEdge(edge_gid, from_vertex_gid);
  if (!maybe_edge_info) {
    return std::nullopt;
  }
  const auto &[edge_ref, edge_type, from, to] = *maybe_edge_info;
  return EdgeAccessor::Create(edge_ref, edge_type, from, to, storage_, &transaction_, view);
}

Transaction InMemoryStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) {
  // We acquire the transaction engine lock here because we access (and
  // modify) the transaction engine variables (`transaction_id` and
  // `timestamp`) below.
  uint64_t transaction_id = 0;
  uint64_t start_timestamp = 0;
  uint64_t last_durable_ts = 0;
  std::optional<PointIndexContext> point_index_context;
  std::optional<ActiveIndices> active_indices;
  {
    auto guard = std::lock_guard{engine_lock_};
    transaction_id = transaction_id_++;
    start_timestamp = timestamp_++;
    // IMPORTANT: this is retrieved while under the lock so that the index is consistant with the timestamp
    point_index_context = indices_.point_index_.CreatePointIndexContext();
    // Needed by snapshot to sync the durable and logical ts
    last_durable_ts = repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
    active_indices = GetActiveIndices();
  }

  auto async_index_helper = AsyncIndexHelper{config_, *active_indices, start_timestamp};

  DMG_ASSERT(point_index_context.has_value(), "Expected a value, even if got 0 point indexes");
  return {transaction_id,
          start_timestamp,
          isolation_level,
          storage_mode,
          false,
          !constraints_.empty(),
          *std::move(point_index_context),
          *std::move(active_indices),
          std::move(async_index_helper),
          last_durable_ts};
}

void InMemoryStorage::SetStorageMode(StorageMode new_storage_mode) {
  std::unique_lock main_guard{main_lock_};
  MG_ASSERT(
      (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL || storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) &&
      (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL ||
       new_storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL));
  if (storage_mode_ != new_storage_mode) {
    // Snapshot thread is already running, but setup periodic execution only if enabled
    if (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      // Ensure all pending work has been completed before changing to IN_MEMORY_ANALYTICAL
      async_indexer_.CompleteRemaining();
      snapshot_runner_.Pause();
    } else {
      // No need to resume async indexer, it is always running.
      // As IN_MEMORY_TRANSACTIONAL we will now start giving it new work
      snapshot_runner_.Resume();
    }
    storage_mode_ = new_storage_mode;
    FreeMemory(std::move(main_guard), false);
  }
}

template <bool aggressive = true>
void InMemoryStorage::CollectGarbage(std::unique_lock<utils::ResourceLock> main_guard, bool periodic) {
  // NOTE: You do not need to consider cleanup of deleted object that occurred in
  // different storage modes within the same CollectGarbage call. This is because
  // SetStorageMode will ensure CollectGarbage is called before any new transactions
  // with the new storage mode can start.

  // SetStorageMode will pass its unique_lock of main_lock_. We will use that lock,
  // as reacquiring the lock would cause deadlock. Otherwise, we need to get our own
  // lock.
  if (!main_guard.owns_lock()) {
    if constexpr (aggressive) {
      // We tried to be aggressive but we do not already have main lock continue as not aggressive
      // Perf note: Do not try to get unique lock if it was not already passed in. GC maybe expensive,
      // do not assume it is fast, unique lock will blocks all new storage transactions.
      CollectGarbage<false>({}, periodic);
      return;
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
    if (main_guard.owns_lock()) {
      main_guard.unlock();
    } else {
      main_lock_.unlock_shared();
    }
  }};

  // Only one gc run at a time
  auto gc_guard = std::unique_lock{gc_lock_, std::try_to_lock};
  if (!gc_guard.owns_lock()) {
    return;
  }

  // Diagnostic trace
  const utils::Timer timer;
  spdlog::trace("Storage GC on '{}' started [{}]", name(), periodic ? "periodic" : "forced");
  auto trace_on_exit = utils::OnScopeExit{[&] {
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed());
    memgraph::metrics::Measure(memgraph::metrics::GCLatency_us, elapsed.count());
    spdlog::trace("Storage GC on '{}' finished [{}]. Duration: {:.3f}s", name(), periodic ? "periodic" : "forced",
                  std::chrono::duration<double>(elapsed).count());
  }};

  // Garbage collection must be performed in two phases. In the first phase,
  // deltas that won't be applied by any transaction anymore are unlinked from
  // the version chains. They cannot be deleted immediately, because there
  // might be a transaction that still needs them to terminate the version
  // chain traversal. They are instead marked for deletion and will be deleted
  // in the second GC phase in this GC iteration or some of the following
  // ones.

  uint64_t oldest_active_start_timestamp = commit_log_->OldestActive();

  {
    auto guard = std::unique_lock{engine_lock_};
    uint64_t mark_timestamp = timestamp_;  // a timestamp no active transaction can currently have

    // Deltas from previous GC runs or from aborts can be cleaned up here
    garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
      guard.unlock();
      if (aggressive or mark_timestamp == oldest_active_start_timestamp) {
        // We know no transaction is active, it is safe to simply delete all the garbage undos
        // Nothing can be reading them
        garbage_undo_buffers.clear();
      } else {
        // garbage_undo_buffers is ordered, pop until we can't
        while (!garbage_undo_buffers.empty() &&
               garbage_undo_buffers.front().mark_timestamp_ <= oldest_active_start_timestamp) {
          garbage_undo_buffers.pop_front();
        }
      }
    });
  }

  // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
  // list immediately, because we would have to repeatedly take
  // garbage_undo_buffers lock.
  std::list<GCDeltas> unlinked_undo_buffers{};

  // We will only free vertices deleted up until now in this GC cycle, and we
  // will do it after cleaning-up the indices. That way we are sure that all
  // vertices that appear in an index also exist in main storage.
  std::list<Gid> current_deleted_edges{};
  std::list<Gid> current_deleted_vertices{};

  deleted_vertices_.WithLock([&](auto &deleted_vertices) { current_deleted_vertices.swap(deleted_vertices); });
  deleted_edges_.WithLock([&](auto &deleted_edges) { current_deleted_edges.swap(deleted_edges); });

  auto const need_full_scan_vertices = gc_full_scan_vertices_delete_.exchange(false, std::memory_order_acq_rel);
  auto const need_full_scan_edges = gc_full_scan_edges_delete_.exchange(false, std::memory_order_acq_rel);

  // Short lock, to move to local variable. Hence, allows other transactions to commit.
  auto linked_undo_buffers = std::list<GCDeltas>{};
  committed_transactions_.WithLock(
      [&](auto &committed_transactions) { committed_transactions.swap(linked_undo_buffers); });

  // This is to track if any of the unlinked deltas would have an impact on index performance, i.e. do they hint that
  // there are possible stale/duplicate entries that can be removed
  auto index_impact = IndexPerformanceTracker{};

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

    for (Delta &delta : linked_entry->deltas_) {
      index_impact.update(delta.action);
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
              DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
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
              DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
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

  // Index cleanup runs can be expensive, we want to avoid high CPU usage when the GC doesn't have to clean up any
  // indexes.
  // - Correctness: we need to remove entries from indexes to avoid dangling raw pointers
  // - Performance: we want to remove duplicate/stale entries to make the skip list as optimial as possible

  // On object deletion, theses indexes MUST be cleaned for functional correctness, their entries with raw pointers to
  // the actual objects need removing before the object is removed itself. Also moving from IN_MEMORY_ANALYTICAL to
  // IN_MEMORY_TRANSACTIONAL any object could have been deleted so also index cleanup is required for correctness.
  bool const index_cleanup_vertex_needed = need_full_scan_vertices || !current_deleted_vertices.empty();
  bool const index_cleanup_edge_needed = need_full_scan_edges || !current_deleted_edges.empty();

  // Used to determine whether the Index GC should be run for performance reasons (removing redundant entries). It
  // should be run when hinted by FastDiscardOfDeltas or by the deltas we processed this GC run.
  const utils::Timer skiplist_cleanup_timer;
  auto index_cleanup_vertex_performance =
      gc_index_cleanup_vertex_performance_.exchange(false, std::memory_order_acq_rel) ||
      index_impact.impacts_vertex_indexes();
  auto index_cleanup_edge_performance = gc_index_cleanup_edge_performance_.exchange(false, std::memory_order_acq_rel) ||
                                        index_impact.impacts_edge_indexes();

  // After unlinking deltas from vertices, we refresh the indices. That way
  // we're sure that none of the vertices from `current_deleted_vertices`
  // appears in an index, and we can safely remove the from the main storage
  // after the last currently active transaction is finished.
  // This operation is very expensive as it traverses through all of the items
  // in every index every time.
  if (auto token = stop_source.get_token(); !token.stop_requested()) {
    if (index_cleanup_vertex_needed || index_cleanup_vertex_performance) {
      indices_.RemoveObsoleteVertexEntries(oldest_active_start_timestamp, token);
      auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get());
      mem_unique_constraints->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
    }
    if (index_cleanup_edge_needed || index_cleanup_edge_performance) {
      indices_.RemoveObsoleteEdgeEntries(oldest_active_start_timestamp, token);
    }
  }
  memgraph::metrics::Measure(
      memgraph::metrics::GCSkiplistCleanupLatency_us,
      std::chrono::duration_cast<std::chrono::microseconds>(skiplist_cleanup_timer.Elapsed()).count());

  {
    auto guard = std::unique_lock{engine_lock_};
    uint64_t mark_timestamp = timestamp_;  // a timestamp no active transaction can currently have

    if (aggressive or mark_timestamp == oldest_active_start_timestamp) {
      guard.unlock();
      // if lucky, there are no active transactions, hence nothing looking at the deltas
      // remove them all now
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

  // EDGES METADATA (has ptr to Vertices, must be before removing vertices)
  if (!current_deleted_edges.empty() && config_.salient.items.enable_edges_metadata) {
    auto edge_metadata_acc = edges_metadata_.access();
    for (auto edge : current_deleted_edges) {
      MG_ASSERT(edge_metadata_acc.remove(edge), "Invalid database state!");
    }
  }

  // VERTICES (has ptr to Edges, must be before removing edges)
  if (!current_deleted_vertices.empty()) {
    auto vertex_acc = vertices_.access();
    for (auto vertex : current_deleted_vertices) {
      MG_ASSERT(vertex_acc.remove(vertex), "Invalid database state!");
    }
  }

  // EDGES
  if (!current_deleted_edges.empty()) {
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
    auto edge_metadata_acc = edges_metadata_.access();
    for (auto &edge : edge_acc) {
      // a deleted edge which as no deltas must have come from IN_MEMORY_ANALYTICAL deletion
      if (edge.delta == nullptr && edge.deleted) {
        edge_acc.remove(edge);
        edge_metadata_acc.remove(edge.gid);
      }
    }
  }
}

// tell the linker he can find the CollectGarbage definitions here
template void InMemoryStorage::CollectGarbage<true>(std::unique_lock<utils::ResourceLock> main_guard, bool periodic);
template void InMemoryStorage::CollectGarbage<false>(std::unique_lock<utils::ResourceLock> main_guard, bool periodic);

StorageInfo InMemoryStorage::GetBaseInfo() {
  StorageInfo info{};
  info.vertex_count = vertices_.size();
  info.edge_count = edge_count_.load(std::memory_order_acquire);
  if (info.vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    info.average_degree = 2.0 * static_cast<double>(info.edge_count) / info.vertex_count;
  }
  info.memory_res = utils::GetMemoryRES();
  memgraph::metrics::SetGaugeValue(memgraph::metrics::PeakMemoryRes, info.memory_res);
  info.peak_memory_res = memgraph::metrics::GetGaugeValue(memgraph::metrics::PeakMemoryRes);
  info.unreleased_delta_objects = memgraph::metrics::GetCounterValue(memgraph::metrics::UnreleasedDeltaObjects);

  // Special case for the default database
  auto update_path = [&](const std::filesystem::path &dir) {
#ifdef MG_ENTERPRISE
    if (config_.salient.name == dbms::kDefaultDB) {
      // Default DB points to the root (for back-compatibility); update to the "database" dir
      std::filesystem::path new_dir = dir / "databases" / dbms::kDefaultDB;
      if (std::filesystem::exists(new_dir) && std::filesystem::is_directory(new_dir)) {
        return new_dir;
      }
    }
#endif
    return dir;
  };
  info.disk_usage = utils::GetDirDiskUsage<false>(update_path(config_.durability.storage_directory));
  if (config_.salient.items.enable_schema_info) {
    const auto &[n_vertex, n_edge] = schema_info_.Size();
    info.schema_vertex_count = n_vertex;
    info.schema_edge_count = n_edge;
  } else {
    info.schema_vertex_count = 0;
    info.schema_edge_count = 0;
  }
  return info;
}

StorageInfo InMemoryStorage::GetInfo() {
  StorageInfo info = GetBaseInfo();
  {
    auto access = Access();  // TODO: override isolation level?
    const auto &lbl = access->ListAllIndices();
    info.label_indices = lbl.label.size();
    info.label_property_indices = lbl.label_properties.size();
    info.text_indices = lbl.text_indices.size();
    info.vector_indices = lbl.vector_indices_spec.size();
    const auto &con = access->ListAllConstraints();
    info.existence_constraints = con.existence.size();
    info.unique_constraints = con.unique.size();
  }
  info.storage_mode = storage_mode_;
  info.isolation_level = isolation_level_;
  info.durability_snapshot_enabled = snapshot_runner_.NextExecution() || config_.durability.snapshot_on_exit;
  info.durability_wal_enabled =
      config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  info.property_store_compression_enabled = config_.salient.items.property_store_compression_enabled;
  info.property_store_compression_level = config_.salient.property_store_compression_level;
  return info;
}

bool InMemoryStorage::InitializeWalFile(memgraph::replication::ReplicationEpoch &epoch) {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL) {
    return false;
  }

  if (!wal_file_) {
    wal_file_ =
        std::make_unique<durability::WalFile>(recovery_.wal_directory_, uuid(), epoch.id(), config_.salient.items,
                                              name_id_mapper_.get(), wal_seq_num_++, &file_retainer_);
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
    wal_file_.reset();
    wal_unsynced_transactions_ = 0;
  } else {
    // Try writing the internal buffer if possible, if not
    // the data should be written as soon as it's possible
    // (triggered by the new transaction commit, or some
    // reading thread EnabledFlushing)
    wal_file_->TryFlushing();
  }
}

bool InMemoryStorage::InMemoryAccessor::HandleDurabilityAndReplicate(uint64_t durability_commit_timestamp,
                                                                     TransactionReplication &replicating_txn,
                                                                     CommitArgs const &commit_args) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // If replica executes this:
  //   STRICT_SYNC: commit_immediately is false because such replica needs to commit only after receiving
  //   FinalizeCommitRpc SYNC/ASYNC:  commit_immediately is true because such replica needs to commit immediately
  // If main executes this:
  //   Any STRICT_SYNC replica registered -> need to run 2PC, don't commit immediately
  // else:
  //   All SYNC/ASYNC replicas -> commit immediately
  bool const two_phase_commit = commit_args.two_phase_commit(replicating_txn);

  // Both main and replica append txn start delta and remember the position in the WAL file in which this delta is
  // saved.
  commit_flag_wal_position_ = mem_storage->wal_file_->AppendTransactionStart(durability_commit_timestamp,
                                                                             !two_phase_commit, original_access_type_);
  // Send transaction start to replicas with the correct access type
  // It does not matter what we send in the `commit` argument as it always gets ignored
  // EXCEPT when loading from a WAL file which will use whatever the line above wrote
  replicating_txn.AppendTransactionStart(durability_commit_timestamp, !two_phase_commit, original_access_type_);
  // The WAL file needs to be updated only if we don't commit immediately.
  needs_wal_update_ = two_phase_commit;

  // IMPORTANT: In most transactions there can only be one, either data or metadata deltas.
  //            But since we introduced auto index creation, a data transaction can also introduce a metadata delta.
  //            For correctness on the REPLICA side we need to send the metadata deltas first in order to acquire a
  //            unique transaction to apply the index creation safely.
  auto const apply_encode = [&](durability::StorageMetadataOperation const op, auto &&encode_operation) {
    auto full_encode_operation = [&](durability::BaseEncoder &encoder) {
      EncodeOperationPreamble(encoder, op, durability_commit_timestamp);
      encode_operation(encoder);
    };

    full_encode_operation(mem_storage->wal_file_->encoder());
    mem_storage->wal_file_->UpdateStats(durability_commit_timestamp);
    replicating_txn.EncodeToReplicas(full_encode_operation);
  };

  // Handle metadata deltas
  for (const auto &md_delta : transaction_.md_deltas) {
    auto const op = ActionToStorageOperation(md_delta.action);
    switch (md_delta.action) {
      case MetadataDelta::Action::LABEL_INDEX_CREATE:
      case MetadataDelta::Action::LABEL_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabel(encoder, *mem_storage->name_id_mapper_, md_delta.label);
        });
        break;
      }
      case MetadataDelta::Action::LABEL_INDEX_STATS_CLEAR:
      case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_STATS_CLEAR: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabel(encoder, *mem_storage->name_id_mapper_, md_delta.label_stats.label);
        });
        break;
      }
      case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_STATS_SET: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabelPropertyStats(encoder, *mem_storage->name_id_mapper_, md_delta.label_property_stats.label,
                                   md_delta.label_property_stats.properties, md_delta.label_property_stats.stats);
        });
        break;
      }
      case MetadataDelta::Action::EDGE_INDEX_CREATE:
      case MetadataDelta::Action::EDGE_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEdgeTypeIndex(encoder, *mem_storage->name_id_mapper_, md_delta.edge_type);
        });
        break;
      }
      case MetadataDelta::Action::EDGE_PROPERTY_INDEX_CREATE:
      case MetadataDelta::Action::EDGE_PROPERTY_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEdgeTypePropertyIndex(encoder, *mem_storage->name_id_mapper_, md_delta.edge_type_property.edge_type,
                                      md_delta.edge_type_property.property);
        });
        break;
      }
      case MetadataDelta::Action::GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
      case MetadataDelta::Action::GLOBAL_EDGE_PROPERTY_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEdgePropertyIndex(encoder, *mem_storage->name_id_mapper_, md_delta.edge_property.property);
        });
        break;
      }
      case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_CREATE:
      case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabelProperties(encoder, *mem_storage->name_id_mapper_, md_delta.label_ordered_properties.label,
                                md_delta.label_ordered_properties.properties);
        });
        break;
      }
      case MetadataDelta::Action::EXISTENCE_CONSTRAINT_CREATE:
      case MetadataDelta::Action::EXISTENCE_CONSTRAINT_DROP:
      case MetadataDelta::Action::POINT_INDEX_CREATE:
      case MetadataDelta::Action::POINT_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabelProperty(encoder, *mem_storage->name_id_mapper_, md_delta.label_property.label,
                              md_delta.label_property.property);
        });
        break;
      }
      case MetadataDelta::Action::LABEL_INDEX_STATS_SET: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabelStats(encoder, *mem_storage->name_id_mapper_, md_delta.label_stats.label,
                           md_delta.label_stats.stats);
        });
        break;
      }
      case MetadataDelta::Action::TEXT_INDEX_CREATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeTextIndexSpec(encoder, *mem_storage->name_id_mapper_, md_delta.text_index);
        });
        break;
      }
      case MetadataDelta::Action::TEXT_EDGE_INDEX_CREATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeTextEdgeIndexSpec(encoder, *mem_storage->name_id_mapper_, md_delta.text_edge_index);
        });
        break;
      }
      case MetadataDelta::Action::TEXT_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) { EncodeIndexName(encoder, md_delta.index_name); });
        break;
      }
      case MetadataDelta::Action::VECTOR_INDEX_CREATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeVectorIndexSpec(encoder, *mem_storage->name_id_mapper_, md_delta.vector_index_spec);
        });
        break;
      }
      case MetadataDelta::Action::VECTOR_EDGE_INDEX_CREATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeVectorEdgeIndexSpec(encoder, *mem_storage->name_id_mapper_, md_delta.vector_edge_index_spec);
        });
        break;
      }
      case MetadataDelta::Action::VECTOR_INDEX_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) { EncodeIndexName(encoder, md_delta.index_name); });
        break;
      }
      case MetadataDelta::Action::UNIQUE_CONSTRAINT_CREATE:
      case MetadataDelta::Action::UNIQUE_CONSTRAINT_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeLabelProperties(encoder, *mem_storage->name_id_mapper_, md_delta.label_unordered_properties.label,
                                md_delta.label_unordered_properties.properties);
        });
        break;
      }
      case MetadataDelta::Action::TYPE_CONSTRAINT_CREATE:
      case MetadataDelta::Action::TYPE_CONSTRAINT_DROP: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeTypeConstraint(encoder, *mem_storage->name_id_mapper_, md_delta.label_property_type.label,
                               md_delta.label_property_type.property, md_delta.label_property_type.type);
        });
        break;
      }
      case MetadataDelta::Action::ENUM_CREATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEnumCreate(encoder, mem_storage->enum_store_, md_delta.enum_create_info.etype);
        });
        break;
      }
      case MetadataDelta::Action::ENUM_ALTER_ADD: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEnumAlterAdd(encoder, mem_storage->enum_store_, md_delta.enum_alter_add_info.value);
        });
        break;
      }
      case MetadataDelta::Action::ENUM_ALTER_UPDATE: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          EncodeEnumAlterUpdate(encoder, mem_storage->enum_store_, md_delta.enum_alter_update_info.value,
                                md_delta.enum_alter_update_info.old_value);
        });
        break;
      }
      case MetadataDelta::Action::TTL_OPERATION: {
        apply_encode(op, [&](durability::BaseEncoder &encoder) {
          durability::EncodeTtlOperation(encoder, md_delta.ttl_operation_info.operation_type,
                                         md_delta.ttl_operation_info.period, md_delta.ttl_operation_info.start_time,
                                         md_delta.ttl_operation_info.should_run_edge_ttl);
        });
        break;
      }
    }
  }
  // A single transaction will always be fully-contained in a single WAL file.
  auto current_commit_timestamp = transaction_.commit_timestamp->load(std::memory_order_acquire);
  auto append_deltas = [&](auto callback) {
    // Helper lambda that traverses the delta chain on order to find the first
    // delta that should be processed and then appends all discovered deltas.
    auto find_and_apply_deltas = [&](const auto *delta, auto *parent, auto filter) {
      while (true) {
        auto *older = delta->next.load(std::memory_order_acquire);
        if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
        delta = older;
      }
      while (true) {
        if (filter(delta->action)) {
          callback(*delta, parent, durability_commit_timestamp);
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
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
        switch (action) {
          case Delta::Action::DELETE_DESERIALIZED_OBJECT:
          case Delta::Action::DELETE_OBJECT:
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL:
          case Delta::Action::SET_VECTOR_PROPERTY:
            return true;

          case Delta::Action::RECREATE_OBJECT:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            return false;
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 2. Process all Vertex deltas and store all operations that create edges.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
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
          case Delta::Action::SET_VECTOR_PROPERTY:
            return false;
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 3. Process all Edge deltas and store all operations that modify edge data.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::EDGE) continue;
      find_and_apply_deltas(&delta, prev.edge, [](auto action) {
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
          case Delta::Action::SET_VECTOR_PROPERTY:
            return false;
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 4. Process all Vertex deltas and store all operations that delete edges.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
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
          case Delta::Action::SET_VECTOR_PROPERTY:
            return false;
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 5. Process all Vertex deltas and store all operations that delete vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
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
          case Delta::Action::SET_VECTOR_PROPERTY:
            return false;
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
  };

  // Handle MVCC deltas
  if (!transaction_.deltas.empty()) {
    append_deltas([&](const Delta &delta, auto *parent, uint64_t durability_commit_timestamp_arg) {
      mem_storage->wal_file_->AppendDelta(delta, parent, durability_commit_timestamp_arg, mem_storage);
      replicating_txn.AppendDelta(delta, parent, durability_commit_timestamp_arg, mem_storage);
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  mem_storage->wal_file_->AppendTransactionEnd(durability_commit_timestamp);

  // If main executes this and committing immediately we need to finalize wal file before sending deltas to replicas
  // If replica executes this and committing immediately, it is OK to finalize wal here
  if (!two_phase_commit) {
    mem_storage->FinalizeWalFile();
  }

  // Ships deltas to instances and waits for the reply
  // Returns only the status of SYNC and STRICT_SYNC replicas.
  return replicating_txn.ShipDeltas(durability_commit_timestamp, commit_args);
}

utils::BasicResult<InMemoryStorage::CreateSnapshotError, std::filesystem::path> InMemoryStorage::CreateSnapshot(
    memgraph::replication_coordination_glue::ReplicationRole replication_role, bool force) {
  using memgraph::replication_coordination_glue::ReplicationRole;
  if (replication_role == ReplicationRole::REPLICA) {
    return CreateSnapshotError::DisabledForReplica;
  }

  auto abort_reset = utils::OnScopeExit([this]() mutable {
    // Abort is a one shot, reset it to false every time
    abort_snapshot_.store(false, std::memory_order_release);
  });

  if (abort_snapshot_.load(std::memory_order_acquire)) {
    return CreateSnapshotError::AbortSnapshot;
  }

  // Make sure only one create snapshot is running at any moment
  auto expected = false;
  auto already_running = !snapshot_running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
  if (already_running) {
    return CreateSnapshotError::AlreadyRunning;
  }
  auto const clear_snapshot_running_on_exit =
      utils::OnScopeExit{[&] { snapshot_running_.store(false, std::memory_order_release); }};

  // This is to make sure SHOW SNAPSHOTS, CREATE SNAPSHOT, and some replication
  // stuff are mutually exclusive from each other
  auto const snapshot_guard = std::unique_lock(snapshot_lock_);

  auto accessor = std::invoke([&]() {
    if (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL) {
      // For analytical no other write txn can be in play
      return ReadOnlyAccess(IsolationLevel::SNAPSHOT_ISOLATION);  // Do we need snapshot isolation?
    }
    return Access(IsolationLevel::SNAPSHOT_ISOLATION);
  });

  utils::Timer timer;
  Transaction *transaction = accessor->GetTransaction();

  DMG_ASSERT(transaction->last_durable_ts_.has_value());
  auto const &epoch = repl_storage_state_.epoch_;
  auto const &epochHistory = repl_storage_state_.history;
  auto const &storage_uuid = uuid();

  // In memory analytical doesn't update last_durable_ts so digest isn't valid
  if (transaction->storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto current_digest = SnapshotDigest{epoch, epochHistory, storage_uuid, *transaction->last_durable_ts_};
    if (!force && last_snapshot_digest_ == current_digest) return CreateSnapshotError::NothingNewToWrite;
    last_snapshot_digest_ = std::move(current_digest);
  }

  // At the moment, the only way in which create snapshot can fail is if it got aborted
  const auto snapshot_path =
      durability::CreateSnapshot(this, transaction, recovery_.snapshot_directory_, recovery_.wal_directory_, &vertices_,
                                 &edges_, storage_uuid, epoch, epochHistory, &file_retainer_, &abort_snapshot_);
  if (!snapshot_path) {
    return CreateSnapshotError::AbortSnapshot;
  }

  memgraph::metrics::Measure(memgraph::metrics::SnapshotCreationLatency_us,
                             std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed()).count());

  return *snapshot_path;
}

// NOTE: Make sure this function is called while exclusively holding on to the main lock
utils::BasicResult<InMemoryStorage::RecoverSnapshotError> InMemoryStorage::RecoverSnapshot(
    std::filesystem::path path, bool force, memgraph::replication_coordination_glue::ReplicationRole replication_role) {
  using memgraph::replication_coordination_glue::ReplicationRole;
  if (replication_role == ReplicationRole::REPLICA) {
    return InMemoryStorage::RecoverSnapshotError::DisabledForReplica;
  }
  if (!std::filesystem::exists(path) || std::filesystem::is_directory(path)) {
    return InMemoryStorage::RecoverSnapshotError::MissingFile;
  }

  // Copy to local snapshot dir
  std::error_code ec{};
  const auto local_path = recovery_.snapshot_directory_ / path.filename();
  const bool file_in_local_dir = local_path == path;
  if (!file_in_local_dir) {
    std::filesystem::copy_file(path, local_path, ec);
    if (ec) {
      spdlog::warn("Failed to copy snapshot into local snapshots directory.");
      return InMemoryStorage::RecoverSnapshotError::CopyFailure;
    }
  }

  auto handler_error = [&]() {
    // If file was copied over, delete...
    if (!file_in_local_dir) file_retainer_.DeleteFile(local_path);
  };

  auto file_locker = file_retainer_.AddLocker();
  (void)file_locker.Access().AddPath(local_path);

  if (force) {
    Clear();
  } else {
    if (repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_ != kTimestampInitialId) {
      handler_error();
      return InMemoryStorage::RecoverSnapshotError::NonEmptyStorage;
    }
  }

  // When creating a snapshot, we first lock the snapshot, then create the accessor, so no need for the snapshot lock
  // GC could be running without the main lock, so lock it
  // Engine lock is needed because of PrepareForNewEpoch
  auto gc_lock = std::unique_lock{gc_lock_};
  auto engine_lock = std::unique_lock{engine_lock_};

  try {
    spdlog::debug("Recovering from a snapshot {}", local_path);
    auto recovered_snapshot = storage::durability::LoadSnapshot(
        local_path, &vertices_, &edges_, &edges_metadata_, &repl_storage_state_.history, name_id_mapper_.get(),
        &edge_count_, config_, &enum_store_, config_.salient.items.enable_schema_info ? &schema_info_.Get() : nullptr,
        &ttl_);
    spdlog::debug("Snapshot recovered successfully");
    // Instead of using the UUID from the snapshot, we will override the snapshot's UUID with our own
    // This snapshot creates a new state and cannot have any WALs associated with it at this point
    // If the storage's snapshot has been reused, the old version will be put in the .old directory
    spdlog::trace("Set epoch to {} for db {}", recovered_snapshot.snapshot_info.epoch_id, name());
    repl_storage_state_.epoch_.SetEpoch(std::move(recovered_snapshot.snapshot_info.epoch_id));
    const auto &recovery_info = recovered_snapshot.recovery_info;
    vertex_id_.store(recovery_info.next_vertex_id, std::memory_order_release);
    edge_id_.store(recovery_info.next_edge_id, std::memory_order_release);
    timestamp_ = std::max(timestamp_, recovery_info.next_timestamp);

    auto const update_func =
        [new_ldt = recovered_snapshot.snapshot_info.durable_timestamp](CommitTsInfo const &old_info) -> CommitTsInfo {
      return CommitTsInfo{.ldt_ = new_ldt, .num_committed_txns_ = old_info.num_committed_txns_};
    };
    atomic_struct_update<CommitTsInfo>(repl_storage_state_.commit_ts_info_, update_func);

    // We are the only active transaction, so mark everything up to the next timestamp
    if (timestamp_ > 0) commit_log_->MarkFinishedInRange(0, timestamp_ - 1);

    spdlog::trace("Recovering indices and constraints from snapshot.");
    storage::durability::RecoverIndicesStatsAndConstraints(
        &vertices_, name_id_mapper_.get(), &indices_, &constraints_, config_, recovery_info,
        recovered_snapshot.indices_constraints, config_.salient.items.properties_on_edges);

    spdlog::trace("Successfully recovered from snapshot {}", local_path);

    // Destroying current wal file
    wal_file_.reset();

    std::string old_dir = ".old_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    spdlog::trace("Moving old snapshots and WALs to {}", old_dir);
    std::error_code ec{};
    std::filesystem::create_directory(recovery_.snapshot_directory_ / old_dir, ec);
    if (ec) {
      spdlog::warn("Failed to create backup snapshot directory; snapshots directory should be cleaned manually.");
      return InMemoryStorage::RecoverSnapshotError::BackupFailure;
    }
    std::filesystem::create_directory(recovery_.wal_directory_ / old_dir, ec);
    if (ec) {
      spdlog::warn("Failed to create backup WAL directory; WAL directory should be cleaned manually.");
      return InMemoryStorage::RecoverSnapshotError::BackupFailure;
    }

    auto snapshot_files = durability::GetSnapshotFiles(recovery_.snapshot_directory_);
    for (const auto &[snapshot_path, snapshot_uuid, _2] : snapshot_files) {
      if (local_path != snapshot_path) {
        spdlog::trace("Moving snapshot file {}", snapshot_path);
        file_retainer_.RenameFile(snapshot_path, recovery_.snapshot_directory_ / old_dir / snapshot_path.filename());
      } else if (uuid() != snapshot_uuid) {
        // This is the recovered snapshot, but it has a different UUID than the current storage UUID
        if (file_in_local_dir) {
          // Used a snapshot for the local storage, back it up before updating the UUID
          std::filesystem::copy_file(snapshot_path, recovery_.snapshot_directory_ / old_dir / snapshot_path.filename(),
                                     ec);
          if (ec) {
            spdlog::warn(
                "Failed to copy snapshot file to backup directory; snapshots directory should be cleaned "
                "manually.");
            return InMemoryStorage::RecoverSnapshotError::BackupFailure;
          }
        }
        // Rewrite the UUID in the snapshot file
        durability::OverwriteSnapshotUUID(local_path, uuid());
      }
    }
    std::filesystem::remove(recovery_.snapshot_directory_ / old_dir, ec);  // remove dir if empty
    auto wal_files = storage::durability::GetWalFiles(recovery_.wal_directory_);

    for (const auto &wal_file : wal_files) {
      spdlog::trace("Moving WAL file {}", wal_file.path);
      file_retainer_.RenameFile(wal_file.path, recovery_.wal_directory_ / old_dir / wal_file.path.filename());
    }

    std::filesystem::remove(recovery_.wal_directory_ / old_dir, ec);  // remove dir if empty
  } catch (const storage::durability::RecoveryFailure &e) {
    handler_error();
    throw utils::BasicException("Couldn't recover from the snapshot because of: {}", e.what());
  }

  return {};
}

std::optional<SnapshotFileInfo> InMemoryStorage::ShowNextSnapshot() {
  auto lock = std::unique_lock{snapshot_lock_};
  auto next = snapshot_runner_.NextExecution();
  if (next) {
    return SnapshotFileInfo{recovery_.snapshot_directory_, 0, utils::LocalDateTime{*next}, 0};
  }
  return std::nullopt;
}

std::vector<SnapshotFileInfo> InMemoryStorage::ShowSnapshots() {
  auto lock = std::unique_lock{snapshot_lock_};

  std::vector<SnapshotFileInfo> res;
  auto file_locker = file_retainer_.AddLocker();
  auto locker_acc = file_locker.Access();
  (void)locker_acc.AddPath(recovery_.snapshot_directory_);
  auto dir_cleanup = utils::OnScopeExit{[&] { (void)locker_acc.RemovePath(recovery_.snapshot_directory_); }};

  // Add currently available snapshots
  auto snapshot_files = durability::GetSnapshotFiles(recovery_.snapshot_directory_ /*, std::string(storage_uuid())*/);
  std::error_code ec;
  for (const auto &[snapshot_path, _, start_timestamp] : snapshot_files) {
    // Hacky solution to covert between different clocks
    utils::LocalDateTime write_time_ldt{std::filesystem::last_write_time(snapshot_path, ec) -
                                        std::filesystem::file_time_type::clock::now() +
                                        std::chrono::system_clock::now()};
    if (ec) {
      spdlog::warn("Failed to read write time for {}", snapshot_path);
      write_time_ldt = utils::LocalDateTime{0};
    }
    size_t size = std::filesystem::file_size(snapshot_path, ec);
    if (ec) {
      spdlog::warn("Failed to read file size for {}", snapshot_path);
      size = 0;
    }
    res.emplace_back(snapshot_path, start_timestamp, write_time_ldt, size);
  }

  std::sort(res.begin(), res.end(),
            [](const auto &lhs, const auto &rhs) { return lhs.creation_time > rhs.creation_time; });

  return res;
}

void InMemoryStorage::FreeMemory(std::unique_lock<utils::ResourceLock> main_guard, bool periodic) {
  std::invoke(free_memory_func_, std::move(main_guard), periodic);
}

uint64_t InMemoryStorage::GetCommitTimestamp() { return timestamp_++; }

void InMemoryStorage::PrepareForNewEpoch() {
  std::unique_lock engine_guard{engine_lock_};
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }
  repl_storage_state_.SaveLatestHistory();
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

std::unique_ptr<Storage::Accessor> InMemoryStorage::Access(StorageAccessType rw_type,
                                                           std::optional<IsolationLevel> override_isolation_level,
                                                           std::optional<std::chrono::milliseconds> timeout) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{Storage::Accessor::shared_access, this,
                                                                override_isolation_level.value_or(isolation_level_),
                                                                storage_mode_, rw_type, timeout});
}
std::unique_ptr<Storage::Accessor> InMemoryStorage::UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                                                 std::optional<std::chrono::milliseconds> timeout) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{Storage::Accessor::unique_access, this,
                                                                override_isolation_level.value_or(isolation_level_),
                                                                storage_mode_, timeout});
}
std::unique_ptr<Storage::Accessor> InMemoryStorage::ReadOnlyAccess(
    std::optional<IsolationLevel> override_isolation_level, std::optional<std::chrono::milliseconds> timeout) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{Storage::Accessor::read_only_access, this,
                                                                override_isolation_level.value_or(isolation_level_),
                                                                storage_mode_, timeout});
}

void InMemoryStorage::CreateSnapshotHandler(
    std::function<utils::BasicResult<InMemoryStorage::CreateSnapshotError>()> cb) {
  create_snapshot_handler = [cb = std::move(cb)] {
    if (auto maybe_error = cb(); maybe_error.HasError()) {
      switch (maybe_error.GetError()) {
        case CreateSnapshotError::DisabledForReplica:
          spdlog::warn(utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
          break;
        case CreateSnapshotError::ReachedMaxNumTries:
          spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support.");
          break;
        case CreateSnapshotError::AbortSnapshot:
          spdlog::warn("Failed to create snapshot. The current snapshot needs to be aborted.");
          break;
        case CreateSnapshotError::AlreadyRunning:
          spdlog::info("Skipping snapshot creation. Another snapshot creation is already in progress.");
          break;
        case CreateSnapshotError::NothingNewToWrite:
          spdlog::info("Skipping snapshot creation. Nothing has been written since the last snapshot.");
          break;
      }
    }
  };

  // Start the snapshot thread in any case, paused if in analytical mode
  if (config_.salient.storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
    snapshot_runner_.Pause();
  }
  snapshot_runner_.SetInterval(config_.durability.snapshot_interval);
  snapshot_runner_.Run("Snapshot", [this, token = stop_source.get_token()]() {
    if (!token.stop_requested()) {
      this->create_snapshot_handler();
    }
  });
}

EdgeInfo ExtractEdgeInfo(Vertex *from_vertex, const Edge *edge_ptr) {
  for (const auto &out_edge : from_vertex->out_edges) {
    const auto [edge_type, other_vertex, edge_ref] = out_edge;
    if (edge_ref.ptr == edge_ptr) {
      return std::tuple(edge_ref, edge_type, from_vertex, other_vertex);
    }
  }
  return std::nullopt;
}

EdgeInfo InMemoryStorage::FindEdgeFromMetadata(Gid gid, const Edge *edge_ptr) {
  auto edge_metadata_acc = edges_metadata_.access();
  auto edge_metadata_it = edge_metadata_acc.find(gid);
  MG_ASSERT(edge_metadata_it != edge_metadata_acc.end(), "Invalid database state!");
  return ExtractEdgeInfo(edge_metadata_it->from_vertex, edge_ptr);
}

EdgeInfo InMemoryStorage::FindEdge(Gid gid) {
  auto edge_acc = edges_.access();
  auto edge_it = edge_acc.find(gid);
  if (edge_it == edge_acc.end()) {
    return std::nullopt;
  }

  auto *edge_ptr = &(*edge_it);

  auto vertices_acc = vertices_.access();
  if (config_.salient.items.enable_edges_metadata) {
    return FindEdgeFromMetadata(gid, edge_ptr);
  }

  for (auto &from_vertex : vertices_acc) {
    if (auto maybe_info = ExtractEdgeInfo(&from_vertex, edge_ptr)) {
      return maybe_info;
    }
  }
  return std::nullopt;
}

EdgeInfo InMemoryStorage::FindEdge(Gid edge_gid, Gid from_vertex_gid) {
  auto edge_acc = edges_.access();
  auto edge_it = edge_acc.find(edge_gid);
  if (edge_it == edge_acc.end()) {
    return std::nullopt;
  }

  auto *edge_ptr = &(*edge_it);

  auto vertices_acc = vertices_.access();
  if (config_.salient.items.enable_edges_metadata) {
    return FindEdgeFromMetadata(edge_gid, edge_ptr);
  }

  auto vertex_it = vertices_acc.find(from_vertex_gid);
  if (vertex_it == vertices_acc.end()) {
    throw utils::BasicException("Vertex with GID {} not found in the database", from_vertex_gid.AsUint());
  }
  return ExtractEdgeInfo(&(*vertex_it), edge_ptr);
}

void InMemoryStorage::Clear() {
  // NOTE: Make sure this function is called while exclusively holding on to the main lock
  // When creating a snapshot, we first lock the snapshot, then create the accessor
  // GC could be running without the main lock
  // Engine lock is needed because of PrepareForNewEpoch
  auto gc_lock = std::unique_lock{gc_lock_};
  auto engine_lock = std::unique_lock{engine_lock_};

  // Clear main memory
  vertices_.clear();
  vertices_.run_gc();
  vertex_id_.store(0, std::memory_order_release);

  edges_.clear();
  edges_.run_gc();
  edge_id_.store(0, std::memory_order_release);
  edge_count_.store(0, std::memory_order_release);

  timestamp_ = kTimestampInitialId;
  transaction_id_ = kTransactionInitialId;

  // Reset WALs
  wal_seq_num_ = 0;
  wal_file_.reset();
  wal_unsynced_transactions_ = 0;

  // Reset the commit log
  commit_log_.reset();
  commit_log_.emplace();

  // Drop any pending GC work (committed_transactions_ is holding on to old deltas)
  deleted_vertices_->clear();
  deleted_edges_->clear();
  garbage_undo_buffers_->clear();
  committed_transactions_->clear();

  // Clear incoming async index creation requests
  async_indexer_.Clear();

  // Clear indices, constraints and metadata
  indices_.DropGraphClearIndices();
  constraints_.DropGraphClearConstraints();
  edges_metadata_.clear();
  edges_metadata_.run_gc();
  stored_node_labels_.clear();
  stored_edge_types_.clear();

  // Reset helper classes
  enum_store_.clear();
  schema_info_.Clear();

  // Replication epoch and timestamp reset
  repl_storage_state_.epoch_.SetEpoch(std::string(utils::UUID{}));
  CommitTsInfo const new_info{.ldt_ = 0, .num_committed_txns_ = 0};
  repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
  repl_storage_state_.history.clear();

  last_snapshot_digest_ = std::nullopt;
}

bool InMemoryStorage::InMemoryAccessor::PointIndexExists(LabelId label, PropertyId property) const {
  return storage_->indices_.point_index_.PointIndexExists(label, property);
}

IndicesInfo InMemoryStorage::InMemoryAccessor::ListAllIndices() const {
  return {transaction_.active_indices_.label_->ListIndices(transaction_.start_timestamp),
          transaction_.active_indices_.label_properties_->ListIndices(transaction_.start_timestamp),
          transaction_.active_indices_.edge_type_->ListIndices(transaction_.start_timestamp),
          transaction_.active_indices_.edge_type_properties_->ListIndices(transaction_.start_timestamp),
          transaction_.active_indices_.edge_property_->ListIndices(transaction_.start_timestamp),
          storage_->indices_.text_index_.ListIndices(),
          storage_->indices_.text_edge_index_.ListIndices(),
          storage_->indices_.point_index_.ListIndices(),
          storage_->indices_.vector_index_.ListIndices(),
          storage_->indices_.vector_edge_index_.ListIndices()};
}
ConstraintsInfo InMemoryStorage::InMemoryAccessor::ListAllConstraints() const {
  const auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  return {mem_storage->constraints_.existence_constraints_->ListConstraints(),
          mem_storage->constraints_.unique_constraints_->ListConstraints(),
          mem_storage->constraints_.type_constraints_->ListConstraints()};
}

void InMemoryStorage::InMemoryAccessor::DropAllIndexes() {
  auto indices_info = ListAllIndices();

  static_cast<InMemoryStorage *>(storage_)->async_indexer_.Clear();

  for (const auto &label_id : indices_info.label) {
    [[maybe_unused]] auto maybe_error = DropIndex(label_id);
  }

  for (auto &[label_id, properties] : indices_info.label_properties) {
    [[maybe_unused]] auto maybe_error = DropIndex(label_id, std::move(properties));
  }

  for (const auto &edge_type_id : indices_info.edge_type) {
    [[maybe_unused]] auto maybe_error = DropIndex(edge_type_id);
  }

  for (const auto &[edge_type_id, property_id] : indices_info.edge_type_property) {
    [[maybe_unused]] auto maybe_error = DropIndex(edge_type_id, property_id);
  }

  for (const auto &property_id : indices_info.edge_property) {
    [[maybe_unused]] auto maybe_error = DropGlobalEdgeIndex(property_id);
  }

  for (const auto &[label_id, property_id] : indices_info.point_label_property) {
    [[maybe_unused]] auto maybe_error = DropPointIndex(label_id, property_id);
  }

  for (const auto &text_index_spec : indices_info.text_indices) {
    [[maybe_unused]] auto maybe_error = DropTextIndex(text_index_spec.index_name);
  }

  for (const auto &vector_index_spec : indices_info.vector_indices_spec) {
    [[maybe_unused]] auto maybe_error = DropVectorIndex(vector_index_spec.index_name);
  }

  for (const auto &vector_edge_index_spec : indices_info.vector_edge_indices_spec) {
    [[maybe_unused]] auto maybe_error = DropVectorIndex(vector_edge_index_spec.index_name);
  }
}

void InMemoryStorage::InMemoryAccessor::DropAllConstraints() {
  auto constraints_info = ListAllConstraints();

  for (const auto &[label_id, property_id] : constraints_info.existence) {
    [[maybe_unused]] auto maybe_error = DropExistenceConstraint(label_id, property_id);
  }

  for (const auto &[label_id, properties] : constraints_info.unique) {
    [[maybe_unused]] auto maybe_error = DropUniqueConstraint(label_id, properties);
  }

  for (const auto &[label_id, property_id, type] : constraints_info.type) {
    [[maybe_unused]] auto maybe_error = DropTypeConstraint(label_id, property_id, type);
  }
}

void InMemoryStorage::InMemoryAccessor::SetIndexStats(const storage::LabelId &label, const LabelIndexStats &stats) {
  static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get())->SetIndexStats(label, stats);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_stats_set, label, stats);
}

void InMemoryStorage::InMemoryAccessor::SetIndexStats(const storage::LabelId &label,
                                                      std::span<storage::PropertyPath const> properties,
                                                      const LabelPropertyIndexStats &stats) {
  static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get())
      ->SetIndexStats(label, properties, stats);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_stats_set, label,
                                      std::vector(properties.begin(), properties.end()), stats);
}

bool InMemoryStorage::InMemoryAccessor::DeleteLabelIndexStats(const storage::LabelId &label) {
  auto *in_mem_label_index = static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get());
  auto res = in_mem_label_index->DeleteIndexStats(label);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_stats_clear, label);
  return res;
}

std::vector<std::pair<LabelId, std::vector<PropertyPath>>>
InMemoryStorage::InMemoryAccessor::DeleteLabelPropertyIndexStats(const storage::LabelId &label) {
  auto *in_mem_label_prop_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  auto res = in_mem_label_prop_index->DeleteIndexStats(label);
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_stats_clear, label);
  return res;
}

void InMemoryStorage::InMemoryAccessor::DropGraph() {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // we take the control from the GC to clear any deltas
  auto gc_guard = std::unique_lock{mem_storage->gc_lock_};
  mem_storage->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) { garbage_undo_buffers.clear(); });
  mem_storage->committed_transactions_.WithLock([&](auto &committed_transactions) { committed_transactions.clear(); });

  mem_storage->async_indexer_.Clear();

  // also, we're the only transaction running, so we can safely remove the data as well
  mem_storage->indices_.DropGraphClearIndices();
  mem_storage->constraints_.DropGraphClearConstraints();

  if (mem_storage->config_.salient.items.enable_schema_info) mem_storage->schema_info_.Clear();

  mem_storage->vertices_.clear();
  mem_storage->edges_.clear();
  mem_storage->edge_count_.store(0, std::memory_order_release);

  memory::PurgeUnusedMemory();
}

auto InMemoryStorage::InMemoryAccessor::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                                      PropertyValue const &point_value,
                                                      PropertyValue const &boundary_value,
                                                      PointDistanceCondition condition) -> PointIterable {
  return transaction_.point_index_ctx_.PointVertices(label, property, crs, storage_, &transaction_, point_value,
                                                     boundary_value, condition);
}

std::vector<std::tuple<VertexAccessor, double, double>> InMemoryStorage::InMemoryAccessor::VectorIndexSearchOnNodes(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  std::vector<std::tuple<VertexAccessor, double, double>> result;

  // we have to take vertices accessor to be sure no vertex is deleted while we are searching
  auto acc = mem_storage->vertices_.access();
  const auto search_results = storage_->indices_.vector_index_.SearchNodes(index_name, number_of_results, vector);
  std::transform(search_results.begin(), search_results.end(), std::back_inserter(result), [&](const auto &item) {
    auto &[vertex, distance, score] = item;
    return std::make_tuple(VertexAccessor{vertex, storage_, &transaction_}, distance, score);
  });

  return result;
}

std::vector<std::tuple<EdgeAccessor, double, double>> InMemoryStorage::InMemoryAccessor::VectorIndexSearchOnEdges(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  std::vector<std::tuple<EdgeAccessor, double, double>> result;

  // we have to take edges accessor to be sure no edge is deleted while we are searching
  auto acc = mem_storage->edges_.access();
  auto edge_type_id = mem_storage->indices_.vector_edge_index_.GetEdgeTypeId(index_name);
  const auto search_results = storage_->indices_.vector_edge_index_.SearchEdges(index_name, number_of_results, vector);
  std::transform(search_results.begin(), search_results.end(), std::back_inserter(result), [&](const auto &item) {
    auto &[edge_tuple, distance, score] = item;
    auto &[from_vertex, to_vertex, edge] = edge_tuple;
    return std::make_tuple(EdgeAccessor{EdgeRef{edge}, edge_type_id, from_vertex, to_vertex, storage_, &transaction_},
                           distance, score);
  });

  return result;
}

std::vector<VectorIndexInfo> InMemoryStorage::InMemoryAccessor::ListAllVectorIndices() const {
  return storage_->indices_.vector_index_.ListVectorIndicesInfo();
}

std::vector<VectorEdgeIndexInfo> InMemoryStorage::InMemoryAccessor::ListAllVectorEdgeIndices() const {
  return storage_->indices_.vector_edge_index_.ListVectorIndicesInfo();
}

auto InMemoryStorage::InMemoryAccessor::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                                      PropertyValue const &bottom_left, PropertyValue const &top_right,
                                                      WithinBBoxCondition condition) -> PointIterable {
  return transaction_.point_index_ctx_.PointVertices(label, property, crs, storage_, &transaction_, bottom_left,
                                                     top_right, condition);
}

}  // namespace memgraph::storage
