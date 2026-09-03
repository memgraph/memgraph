// Copyright 2026 Memgraph Ltd.
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
#include <range/v3/all.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <system_error>
#include <unordered_set>
#include <utility>

#include "ctre.hpp"
#include "dbms/constants.hpp"
#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "memory/db_arena_fwd.hpp"
#include "memory/global_memory_control.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "replication_coordination_glue/role.hpp"
#include "requests/requests.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/commit_probe.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/edge_direction.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/inmemory/claimed_objects.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/vertex_property_index.hpp"
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
#include "utils/db_aware_allocator.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/resource_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/stat.hpp"
#include "utils/temporal.hpp"
#include "utils/variant_helpers.hpp"

import memgraph.utils.aws;

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {
namespace {

// Sub-directory holding durability files superseded by a new base state. Kept in sync with the name
// utils::GetFilesFromDir filters out, so archived files are invisible to every directory scan.
constexpr std::string_view kOldDurabilityDir = ".old";

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
    add_case(LABEL_PROPERTIES_INDEX_DROP);
    add_case(LABEL_PROPERTIES_INDEX_STATS_SET);
    add_case(LABEL_PROPERTIES_INDEX_STATS_CLEAR);
    add_case(EDGE_INDEX_CREATE);
    add_case(EDGE_INDEX_DROP);
    add_case(EDGE_PROPERTY_INDEX_CREATE);
    add_case(EDGE_PROPERTY_INDEX_DROP);
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_CREATE);
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_DROP);
    add_case(GLOBAL_VERTEX_PROPERTY_INDEX_CREATE);
    add_case(GLOBAL_VERTEX_PROPERTY_INDEX_DROP);
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
    add_case(DESCRIPTION_SET);
    add_case(DESCRIPTION_DELETE);
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

DeltaChainState ComputeDeltaChainState(bool has_blocker, WriteResult result) {
  if (has_blocker) return DeltaChainState::FORCED_SEQUENTIAL;
  if (result == WriteResult::NON_SEQUENTIAL) return DeltaChainState::NON_SEQUENTIAL;
  return DeltaChainState::SEQUENTIAL;
}

class PeriodicSnapshotObserver : public memgraph::utils::Observer<memgraph::utils::SchedulerInterval> {
 public:
  explicit PeriodicSnapshotObserver(memgraph::utils::Scheduler &scheduler) : scheduler_{&scheduler} {}

  // String HAS to be a valid cron expr
  void Update(const memgraph::utils::SchedulerInterval &in) override { scheduler_->SetIntervalAndWake(in); }

 private:
  memgraph::utils::Scheduler *scheduler_;
};

bool HasUncommittedNonSequentialDeltas(Vertex const *vertex, uint64_t skip_transaction_id) {
  DMG_ASSERT(vertex->lock.is_locked(), "HasUncommittedNonSequentialDeltas must be called with vertex lock held");
  Delta *delta = vertex->delta();
  while (delta != nullptr) {
    auto ts = delta->commit_info->timestamp.load(std::memory_order_acquire);

    if (ts == skip_transaction_id) {
      // don't include our own deltas they will soon been committed
    } else {
      if (IsDeltaNonSequential(*delta)) {
        if (ts >= kTransactionInitialId) {
          // found UncommittedNonSequential
          return true;
        }
        // skip committed ones
      } else {
        // Found a Sequential Delta, hence can't be a NonSequential block
        return false;
      }
    }
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

void UnlinkAndRemoveDeltas(delta_container &deltas, BatchedList<Edge *> &current_deleted_edges,
                           BatchedList<Gid> &current_deleted_vertices,
                           IndexArming::TransactionScope const &arming_scope) {
  for (auto &delta : deltas) {
    DMG_ASSERT(
        [&delta]() {
          Delta *next = delta.next.load(std::memory_order_acquire);
          if (next == nullptr) return true;
          auto next_ts = next->commit_info->timestamp.load(std::memory_order_acquire);
          return !(next_ts >= kTransactionInitialId && IsDeltaNonSequential(*next));
        }(),
        "downstream active non-sequential delta found during rapid cleanup");
    arming_scope.note(delta);
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::NULL_PTR:
      case PreviousPtr::Type::DELTA:
        break;
      case PreviousPtr::Type::VERTEX: {
        auto &vertex = *prev.vertex;
        vertex.SetDelta(nullptr);
        vertex.set_has_uncommitted_non_sequential_deltas(false);
        if (vertex.deleted()) {
          DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
          current_deleted_vertices.push_back(vertex.gid);
        }
        break;
      }
      case PreviousPtr::Type::EDGE: {
        auto &edge = *prev.edge;
        edge.SetDelta(nullptr);
        if (edge.deleted()) {
          DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
          current_deleted_edges.push_back(prev.edge);
        }
        break;
      }
    }
  }
}

/** When we have non-sequential deltas, we can no longer use the shortcut of
 * only processing deltas downstream from a "head" delta, i.e., one whose `prev`
 * is a vertex. Instead, we have to walk upstream, following `prev` pointers
 * until we find the vertex. Obviously, this can be costly with large delta
 * chains, so the cost is mitigated by:
 * - only finding the vertex when we need to find the upstream vertex from a
 *   non-sequential delta
 * - caching any intermediate subchain "heads" we find for this transaction. In
 *   practise, this massively reduces the amount of iterating needed to be done,
 *   as once we come across a delta we've seen before we can quickly work out to
 *   which vertex it belongs.
 */
class DeltaVertexCache {
 public:
  explicit DeltaVertexCache(uint64_t commit_timestamp) : commit_timestamp_(commit_timestamp) {}

  Vertex *GetVertexFromDelta(Delta const *delta) {
    auto prev = delta->prev.Get();
    if (prev.type == PreviousPtr::Type::VERTEX) return prev.vertex;

    auto const it = cache_.find(delta);
    if (it != cache_.cend()) return it->second;

    std::vector<Delta const *> discovered_subchain_heads{delta};

    auto const write_to_cache = [&](auto *vertex) {
      for (auto const *uncached : discovered_subchain_heads) cache_[uncached] = vertex;
    };

    delta = prev.delta;
    auto delta_ts = delta->commit_info->timestamp.load(std::memory_order_acquire);
    while (true) {
      auto current_prev = delta->prev.Get();
      if (current_prev.type == PreviousPtr::Type::VERTEX) {
        write_to_cache(current_prev.vertex);
        return current_prev.vertex;
      }

      DMG_ASSERT(current_prev.type == PreviousPtr::Type::DELTA, "Expected DELTA in vertex delta chain");

      auto const prev_ts = current_prev.delta->commit_info->timestamp.load(std::memory_order_acquire);
      // If the ts for the previous delta is different than this one's, we know
      // that they are from different transactions and so this delta is the
      // head of a non-sequential subchain.
      if (delta_ts != prev_ts) {
        if (delta_ts == commit_timestamp_) discovered_subchain_heads.push_back(delta);

        auto cached = cache_.find(current_prev.delta);
        if (cached != cache_.end()) {
          write_to_cache(cached->second);
          return cached->second;
        }
      }

      delta = current_prev.delta;
      delta_ts = prev_ts;
    }
  }

 private:
  uint64_t commit_timestamp_;
  std::unordered_map<Delta const *, Vertex *> cache_;
};

};  // namespace

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

InMemoryStorage::InMemoryStorage(Config config, std::optional<free_mem_fn> free_mem_fn_override,
                                 PlanInvalidatorPtr invalidator, metrics::DatabaseMetricHandles metric_handles,
                                 std::function<storage::DatabaseProtectorPtr()> database_protector_factory,
                                 memgraph::memory::ArenaPool *db_arena,
                                 utils::MemoryTracker *db_embedding_memory_tracker)
    : Storage(config, config.salient.storage_mode, std::move(invalidator), metric_handles, db_arena,
              db_embedding_memory_tracker, std::move(database_protector_factory)),
      db_arena_(db_arena),
      vertices_{},
      edges_{},
      edges_metadata_index_{(config.salient.items.properties_on_edges && config.salient.items.enable_edges_metadata)
                                ? std::optional<EdgeMetadataIndex>{std::in_place}
                                : std::nullopt},
      recovery_{.snapshot_directory_ = config.durability.storage_directory / durability::kSnapshotDirectory,
                .wal_directory_ = config.durability.storage_directory / durability::kWalDirectory},
      lock_file_path_(config.durability.storage_directory / durability::kLockFile),
      snapshot_periodic_observer_(std::make_shared<PeriodicSnapshotObserver>(snapshot_runner_)),
      global_locker_(file_retainer_.AddLocker()) {
  MG_ASSERT(config.salient.storage_mode != StorageMode::ON_DISK_TRANSACTIONAL,
            "Invalid storage mode sent to InMemoryStorage constructor!");
  if (config_.experimental_lockfree_read_snapshot) {
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) — make_unique<T[]> is the idiomatic heap array (cf. ring_buffer.hpp).
    snapshot_slots_ = std::make_unique<SnapshotSlot[]>(kSnapshotSlots);
  }
  MG_ASSERT(!config_.salient.items.storage_light_edge || config_.salient.items.properties_on_edges,
            "Light edges require properties on edges (--storage-light-edge implies "
            "--storage-properties-on-edges=true).");
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
    MG_ASSERT(lock_file_handle_->Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING),
              "Failed to open {}",
              lock_file_path_);
    MG_ASSERT(lock_file_handle_->AcquireLock(),
              "Couldn't acquire lock on the storage directory {}"
              "!\nAnother Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              config_.durability.storage_directory);
  }

  if (config_.durability.recover_on_startup) {
    // Disable TTL until after recovery and the role switch / write-enabled check.
    // LOAD-BEARING for hot/cold RESUME: a resumed tenant is rebuilt through this ctor
    // (recover_on_startup=true), so this deny-default (false) is what prevents TTL from
    // running under the permissive struct-default in the window between the TTL scheduler
    // starting (inside RecoverData) and on_resume_ rewiring the MAIN-only user check.
    // Do not move or remove this call.
    ttl_.SetUserCheck([]() -> bool { return false; });
    // Recover data
    utils::Timer const recovery_timer;
    // Exception-safety for light edges: if RecoverData throws after wiring one
    // or more pool-allocated light Edge* into vertex adjacency, the exception
    // escapes the InMemoryStorage constructor.  C++ runs member destructors but
    // never the ~InMemoryStorage body, so the ClearLightEdges() teardown in the
    // dtor would be skipped — orphaning every live light Edge* (the vertices_
    // SkipList dtor frees its Vertex nodes without touching the adjacency
    // Edge*).  Heavy edges are unaffected (freed by the edges_ SkipList dtor).
    // The catch below gates on the flag so the heavy path remains byte-identical
    // and delegates to ClearLightEdges() which is noexcept-safe (LightEdgePool::
    // Destroy is noexcept; deleted_edges_/graveyard are empty at recovery time
    // because the WAL edge-delete replay arm calls LightEdgePool::Destroy
    // directly without queuing).
    auto info = std::invoke([&] -> std::optional<durability::RecoveryInfo> {
      try {
        return recovery_.RecoverData(
            uuid(),
            repl_storage_state_,
            &vertices_,
            &edges_,
            edges_metadata_index_ ? &*edges_metadata_index_ : nullptr,
            &edge_count_,
            name_id_mapper_.get(),
            &indices_,
            &constraints_,
            config_,
            db_arena_,
            &wal_seq_num_,
            &enum_store_,
            config_.salient.items.enable_schema_info ? &schema_info_.Get() : nullptr,
            [this](Gid edge_gid) { return FindEdge(edge_gid); },
            name(),
            &ttl_,
            &description_store_);
      } catch (...) {
        // --storage-allow-recovery-failure: instead of crashing the process, bring this
        // database up empty and broken. RecoverData only reads durability files, so the
        // on-disk snapshot/WAL are left untouched for the operator to RECOVER SNAPSHOT
        // or restore the whole data directory from a backup. Any exception is treated as
        // a broken boot when the flag is on: data-driven corruption does not always
        // surface as RecoveryFailure (e.g. a flipped count byte yields std::length_error
        // or OutOfMemoryException from a reserve/loop bound). When the flag is off, every
        // exception propagates unchanged after freeing any pool-allocated light edges.
        if (!config_.durability.allow_recovery_failure) {
          if (config_.salient.items.storage_light_edge) {
            ClearLightEdges();
          }
          throw;
        }
        spdlog::warn("Database '{}' failed to recover; bringing it up in the broken state.", name());
        // Clear() harvests and frees the live light edges itself (gated on storage_light_edge),
        // so it is the single owner on this path; do not pre-free here or it double-frees.
        Clear();
        name_id_mapper_->Clear();
        description_store_.Clear();
        // Snapshot recovery may have already armed the storage-ttl background thread; stop it so a
        // broken database doesn't keep firing TTL jobs against cleared storage.
        ttl_.Disable();
        SetBroken(true);
        return std::nullopt;
      }
    });
    metric_handles_.snapshot_recovery_latency_seconds.Observe(
        std::chrono::duration<double>(recovery_timer.Elapsed()).count());
    if (info) {
      vertex_id_.store(info->next_vertex_id, std::memory_order_release);
      edge_id_.store(info->next_edge_id, std::memory_order_release);
      timestamp_ = std::max(timestamp_, info->next_timestamp);
      // EXPERIMENTAL (lock-free-read-snapshot): restore the read-snapshot watermark to the highest recovered
      // committed timestamp.  We derive it from the local MVCC counter (timestamp_ - 1 = highest committed ts
      // in this storage's own timestamp space) rather than from a durability-space field, so the watermark is
      // space-correct: readers compare it against local MVCC delta timestamps, which live in the same space.
      // Guard against underflow when the counter is still at its initial value.
      if (config_.experimental_lockfree_read_snapshot) {
        last_committed_mvcc_ts_.store(std::max(last_committed_mvcc_ts_.load(std::memory_order_relaxed),
                                               timestamp_ > kTimestampInitialId ? timestamp_ - 1 : kTimestampInitialId),
                                      std::memory_order_release);
      }
      CommitTsInfo const new_info{.ldt_ = info->last_durable_timestamp,
                                  .num_committed_txns_ = info->num_committed_txns};
      repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
      spdlog::trace(
          "Recovering last durable timestamp {}. Timestamp recovered to {}. Num committed txns recovered to {}.",
          info->last_durable_timestamp,
          timestamp_,
          info->num_committed_txns);
    }

    if (config_.track_label_counts) {
      auto label_counts_acc = label_counts_.Lock();
      for (auto const &vertex : vertices_.access()) {
        if (vertex.deleted()) continue;
        for (auto const label : vertex.labels) {
          ++(*label_counts_acc)[label];
        }
      }
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
        MG_ASSERT(
            !item_error_code, "Couldn't move {} file {} because of: {}", what, item.path(), item_error_code.message());
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
    free_memory_func_ = [this](utils::ResourceLockGuard main_guard, bool periodic) {
      CollectGarbage(std::move(main_guard), periodic);

      // Indices
      static_cast<InMemoryLabelIndex *>(indices_.label_index_.get())->RunGC();
      static_cast<InMemoryLabelPropertyIndex *>(indices_.label_property_index_.get())->RunGC();
      static_cast<InMemoryEdgeTypeIndex *>(indices_.edge_type_index_.get())->RunGC();
      static_cast<InMemoryEdgeTypePropertyIndex *>(indices_.edge_type_property_index_.get())->RunGC();
      static_cast<InMemoryEdgePropertyIndex *>(indices_.edge_property_index_.get())->RunGC();
      static_cast<InMemoryVertexPropertyIndex *>(indices_.vertex_property_index_.get())->RunGC();

      // Constraints
      static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get())->RunGC();

      // SkipList is already threadsafe
      if (edges_metadata_index_) {
        edges_metadata_index_->RunGc();
      }
      vertices_.run_gc();
      edges_.run_gc();
      DrainLightEdgeGraveyard();

      // Auto-indexer also has a skiplist
      async_indexer_.RunGC();

      // AsyncTimer resources are global, not particularly storage related, more query related
      // At some point in the future this should be scheduled by something else
      utils::AsyncTimer::GCRun();
    };
  }

  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }

  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    // TODO: move out of storage have one global gc_runner_
    gc_runner_.SetInterval(config_.gc.interval);
    gc_runner_.Run("Storage GC", [this] {
      const memory::DbArenaScope db_arena_scope{db_arena_};
      this->FreeMemory({}, true);
    });
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
  // Must stop all background tasks (async indexer, TTL) before finalizing WAL:
  // both commit transactions that write to wal_file_, so resetting wal_file_ while
  // they are still running causes a null dereference in HandleDurabilityAndReplicate.
  StopAllBackgroundTasks();
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }

  // On destruction, we want to stop snapshot creation unless snapshot_on_exit is set to true.
  // If snapshot on exit is set to true then create_snapshot_handler() will just skip snapshot creation because it
  // will figure out that there are no changes.
  if (!config_.durability.snapshot_on_exit) {
    if (snapshot_running_.load(std::memory_order_acquire)) {
      spdlog::info("snapshot aborting: storage is shutting down");
    }
    abort_snapshot_.store(true, std::memory_order_release);
  }

  snapshot_runner_.Stop();
  // A broken database must not write an exit snapshot over its untouched corrupt files.
  if (config_.durability.snapshot_on_exit && this->create_snapshot_handler && !IsBroken()) {
    create_snapshot_handler("exit");
  }
  // Leak fix: a deleted light edge whose delta chain was never GC-unlinked
  // (an older txn was active at delete time) is referenced ONLY by an un-GC'd
  // RECREATE_OBJECT delta in committed_transactions_/waiting_gc_deltas_. The
  // clear() below frees those deltas but NOT the pool-allocated Edge*, and
  // ClearLightEdges only drains {adjacency, graveyard, deleted_edges_} -> the
  // Edge* would leak. A forced CollectGarbage here would work but is NOT noexcept
  // (allocations, logging) and a throw in a dtor calls std::terminate. Instead
  // run a noexcept harvester that frees exactly those delta-chain-only light
  // edges (disjoint from the three sets ClearLightEdges drains, so no double
  // free). Gated: heavy Edge* live in edges_ and are freed by the skiplist.
  if (config_.salient.items.storage_light_edge) {
    HarvestDeltaChainOnlyLightEdges();
  }
  committed_transactions_.WithLock([](auto &transactions) { transactions.clear(); });
  // Free live light edges (pool-allocated Edge*) before teardown. Gated: heavy
  // edges live in edges_ and are freed by the skip-list, not here.
  if (config_.salient.items.storage_light_edge) {
    ClearLightEdges();
  }
}

void InMemoryStorage::UpdateLabelCount(LabelId const label, int64_t const change) {
  if (config_.track_label_counts) {
    auto label_counts_acc = label_counts_.Lock();
    auto &count = (*label_counts_acc)[label];
    count += change;
  }
}

InMemoryStorage::InMemoryAccessor::InMemoryAccessor(InMemoryStorage *storage,
                                                    std::optional<IsolationLevel> override_isolation_level,
                                                    utils::ResourceLockGuard guard)
    : Accessor(storage, override_isolation_level, std::move(guard)), config_(storage->config_.salient.items) {}

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

  if (!maybe_result) {
    return std::unexpected{maybe_result.error()};
  }

  auto value = maybe_result.value();

  if (!value) {
    return std::make_optional<ReturnType>();
  }

  auto &[deleted_vertices, deleted_edges] = *value;

  if (storage_->config_.track_label_counts) {
    for (auto const &vertex : deleted_vertices) {
      auto labels = vertex.Labels(View::NEW);
      if (labels) {
        for (auto const label : *labels) {
          storage_->UpdateLabelCount(label, -1);
        }
      }
    }
  }

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

      // A light edge is named only by the adjacency of its endpoints, and deleting an edge erases
      // it from both, so nothing can reach it afterwards to collect it: hand it over here instead.
      // A heavy edge needs no handover, its skip-list node is still there for the scan to find.
      if (config_.storage_light_edge) {
        // Collect the Edge* off-lock, then splice in O(1) under the SpinLock — the
        // critical section must not do O(batch) node allocation while locked.
        BatchedList<Edge *> light_edges;
        for (auto const &edge : deleted_edges) {
          light_edges.push_back(edge.edge_.ptr);
        }
        mem_storage->deleted_edges_.WithLock(
            [&](auto &storage_deleted_edges) { storage_deleted_edges.splice(light_edges); });
      }
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

std::optional<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdgeInternal(
    Vertex *from_vertex, Vertex *to_vertex, EdgeTypeId edge_type, DeltaChainState from_state, DeltaChainState to_state,
    storage::Gid gid, std::optional<SchemaInfo::ModifyingAccessor> &schema_acc,
    std::optional<utils::SkipListDb<Edge>::Accessor> &edge_acc) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    // SchemaInfo handles edge creation via vertices; add collector here if that ever changes
    Edge *edge_ptr = nullptr;
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    if (config_.storage_light_edge) {
      // Create throws on OOM (propagated to abort the txn), never returns null.
      edge_ptr = InMemoryStorage::LightEdgePool::Create(gid, delta);
    } else {
      edge_acc = mem_storage->edges_.access();
      auto [it, inserted] = edge_acc->insert(Edge(gid, delta));
      MG_ASSERT(inserted, "The edge must be inserted here!");
      MG_ASSERT(it != edge_acc->end(), "Invalid Edge accessor!");
      edge_ptr = &*it;
    }
    if (delta) {
      delta->prev.Set(edge_ptr);
    }
    edge = EdgeRef(edge_ptr);
    if (auto &idx = mem_storage->edges_metadata_index_) {
      idx->OnEdgeCreated(gid, from_vertex);
    }
  }

  utils::AtomicMemoryBlock([this,
                            edge,
                            from_vertex = from_vertex,
                            edge_type = edge_type,
                            to_vertex = to_vertex,
                            &schema_acc,
                            from_state,
                            to_state]() {
    CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge, from_state);
    from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

    CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge, to_state);
    to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

    transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
    transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

    // Update indices if they exist.
    Indices::UpdateOnEdgeCreation(from_vertex, to_vertex, edge, edge_type, transaction_);

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

Result<EdgeAccessor> InMemoryStorage::InMemoryAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                   EdgeTypeId edge_type) {
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  // It's important to destruct the accessor after we unlock the vertices to avoid expensive skip list gc while we hold
  // the locks
  std::optional<utils::SkipListDb<Edge>::Accessor> edge_acc;
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
  auto const from_result = PrepareForNonSequentialWrite(&transaction_, from_vertex, Delta::Action::ADD_OUT_EDGE);
  if (from_result == WriteResult::SERIALIZATION_ERROR) return std::unexpected{Error::SERIALIZATION_ERROR};
  if (from_vertex->deleted()) return std::unexpected{Error::DELETED_OBJECT};
  DeltaChainState const from_state =
      ComputeDeltaChainState(ShouldSetNonSequentialBlockerUpstreamFlag(&transaction_, from_vertex), from_result);

  // If to and from are the same we need to ensure to_result is the same as from_result
  DeltaChainState to_state = from_state;
  if (to_vertex != from_vertex) {
    WriteResult const to_result = PrepareForNonSequentialWrite(&transaction_, to_vertex, Delta::Action::ADD_IN_EDGE);
    if (to_result == WriteResult::SERIALIZATION_ERROR) return std::unexpected{Error::SERIALIZATION_ERROR};
    if (to_vertex->deleted()) return std::unexpected{Error::DELETED_OBJECT};
    to_state = ComputeDeltaChainState(ShouldSetNonSequentialBlockerUpstreamFlag(&transaction_, to_vertex), to_result);
  }

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_edge_types_.try_insert(edge_type);
  }
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  auto gid = storage::Gid::FromUint(mem_storage->edge_id_.fetch_add(1, std::memory_order_acq_rel));

  auto result = CreateEdgeInternal(from_vertex, to_vertex, edge_type, from_state, to_state, gid, schema_acc, edge_acc);
  MG_ASSERT(result.has_value(), "CreateEdgeInternal must not fail when called from CreateEdge");
  return *result;
}

std::optional<EdgeAccessor> InMemoryStorage::InMemoryAccessor::FindEdge(Gid gid, const View view, EdgeTypeId edge_type,
                                                                        VertexAccessor *from_vertex,
                                                                        VertexAccessor *to_vertex) {
  auto res = FindEdges(view, edge_type, from_vertex, to_vertex);
  if (!res) return std::nullopt;  // TODO: use a Result type

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

  // It's important to destruct the accessor after we unlock the vertices to avoid expensive skip list gc while we hold
  // the locks
  std::optional<utils::SkipListDb<Edge>::Accessor> edge_acc;
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

  auto const from_result = PrepareForNonSequentialWrite(&transaction_, from_vertex, Delta::Action::ADD_OUT_EDGE);
  if (from_result == WriteResult::SERIALIZATION_ERROR) return std::unexpected{Error::SERIALIZATION_ERROR};
  if (from_vertex->deleted()) return std::unexpected{Error::DELETED_OBJECT};
  DeltaChainState const from_state =
      ComputeDeltaChainState(ShouldSetNonSequentialBlockerUpstreamFlag(&transaction_, from_vertex), from_result);

  DeltaChainState to_state = from_state;
  if (to_vertex != from_vertex) {
    WriteResult const to_result = PrepareForNonSequentialWrite(&transaction_, to_vertex, Delta::Action::ADD_IN_EDGE);
    if (to_result == WriteResult::SERIALIZATION_ERROR) return std::unexpected{Error::SERIALIZATION_ERROR};
    if (to_vertex->deleted()) return std::unexpected{Error::DELETED_OBJECT};
    to_state = ComputeDeltaChainState(ShouldSetNonSequentialBlockerUpstreamFlag(&transaction_, to_vertex), to_result);
  }

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_edge_types_.try_insert(edge_type);
  }
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  atomic_fetch_max_explicit(&mem_storage->edge_id_, gid.AsUint() + 1, std::memory_order_acq_rel);

  auto result = CreateEdgeInternal(from_vertex, to_vertex, edge_type, from_state, to_state, gid, schema_acc, edge_acc);
  MG_ASSERT(result.has_value(), "CreateEdgeInternal must not fail when called from CreateEdgeEx");
  return *result;
}

std::expected<void, ConstraintViolation> InMemoryStorage::InMemoryAccessor::ExistenceConstraintsViolation() const {
  // ExistenceConstraints validation block
  auto const has_any_existence_constraints = !transaction_.active_constraints_->existence_->empty();
  if (has_any_existence_constraints && transaction_.constraint_verification_info &&
      transaction_.constraint_verification_info->NeedsExistenceConstraintVerification()) {
    auto validation_result = storage_->constraints_.existence_constraints_->Validate(
        transaction_.constraint_verification_info->GetVerticesForExistenceConstraintChecking());
    if (!validation_result.has_value()) {
      return std::unexpected{validation_result.error()};
    }
  }
  return {};
}

std::expected<void, ConstraintViolation> InMemoryStorage::InMemoryAccessor::UniqueConstraintsViolation() const {
  auto const has_any_unique_constraints = !transaction_.active_constraints_->unique_->empty();
  if (has_any_unique_constraints && transaction_.constraint_verification_info &&
      transaction_.constraint_verification_info->NeedsUniqueConstraintVerification()) {
    // Before committing and validating vertices against unique constraints,
    // we have to update unique constraints with the vertices that are going
    // to be validated/committed. Use ActiveConstraints which holds the snapshot.
    const auto vertices_to_update = transaction_.constraint_verification_info->GetVerticesForUniqueConstraintChecking();

    for (auto const *vertex : vertices_to_update) {
      transaction_.active_constraints_->unique_->UpdateBeforeCommit(vertex, transaction_);
    }

    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

    // Hold accessor to prevent deletion of vertices while validating unique constraints. Otherwise, some previously
    // aborted txn could delete one of vertices being deleted.
    auto acc = mem_storage->vertices_.access();

    // TODO: UpdateBeforeCommit + Validate could be done in one pass, also use the AbortProcessor
    //       pattern to gather, so we only require a single skip_list acccess
    auto *mem_unique_constraints =
        static_cast<InMemoryUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
    auto validation_result = mem_unique_constraints->Validate(vertices_to_update, transaction_, *commit_timestamp_);
    if (!validation_result.has_value()) {
      return std::unexpected{validation_result.error()};
    }
  }
  return {};
}

void InMemoryStorage::InMemoryAccessor::CheckForFastDiscardOfDeltas() {
  // while still holding engine lock and after durability + replication,
  // check if we can fast discard deltas (i.e. do not hand over to GC)
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // Invariant note (experimental_lockfree_read_snapshot path):
  //
  // Under the lockfree flag, engine_lock_ is released after the mint so that WAL + replication
  // run without blocking concurrent BEGINs.  A transaction that calls BEGIN inside this
  // mint->publish gap receives a start_timestamp that is ABOVE this commit's commit_timestamp_.
  // Such a "gap-BEGIN" transaction therefore does NOT lower commit_log_->OldestActive(), so
  // no_older_transactions can be true even while a gap-BEGIN reader is still live and can still
  // observe this transaction's deltas.
  //
  // The safety invariant therefore falls entirely on no_newer_transactions: that read is protected
  // by engine_lock_ (this function is called from FinalizeCommitPhase while the publish hold is
  // still active, and on the OFF path from PrepareForCommitPhase's engine_lock_ hold).
  // engine_lock_ serialises the transaction_id_ read against a concurrent GetCommitTimestamp()
  // (which increments transaction_id_).  If no_newer_transactions is true, no gap-BEGIN can exist
  // because the gap was closed before a new transaction_id_ was issued.
  //
  // Consequence: moving fast-discard outside of the engine_lock_ hold would make the
  // transaction_id_ read unsynchronised and could discard deltas that a concurrent gap-BEGIN
  // reader still needs -- use-after-free.
  //
  // Note: utils::SpinLock wraps pthread_spinlock_t and exposes no is_locked() / owner-tracking
  // API, so "caller holds engine_lock_" cannot be expressed as a DMG_ASSERT here; it is an
  // enforced caller contract, not a machine-checkable precondition.
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

void InMemoryStorage::InMemoryAccessor::AbortAndResetCommitTs(ProgressCallback const &on_progress) {
  Abort(on_progress);
  // We have aborted, need to release/cleanup commit_timestamp_ here
  DMG_ASSERT(commit_timestamp_);
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  mem_storage->commit_log_->MarkFinished(*commit_timestamp_);
  commit_timestamp_.reset();
}

// NOLINTNEXTLINE(google-default-arguments)
void InMemoryStorage::InMemoryAccessor::PublishIndexArming() {
  // Transactional arming is read off the deltas as the GC unlinks them. Analytical makes no deltas,
  // so its writes note what they touched on the transaction and it is handed over here instead.
  // Without this an analytical workload that only creates and updates never arms a sweep, and every
  // superseded index entry it writes stays until something is deleted.
  if (!transaction_.index_arming.arms_anything()) return;
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  mem_storage->pending_index_arming_.WithLock([this](IndexArming &pending) { pending |= transaction_.index_arming; });
}

std::expected<void, StorageManipulationError> InMemoryStorage::InMemoryAccessor::PrepareForCommitPhase(
    CommitArgs const commit_args, std::unique_lock<std::mutex> preheld_commit_lock) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.has_serialization_error, "Unable to commit due to serialization error.");

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  const bool lockfree = mem_storage->config_.experimental_lockfree_read_snapshot;

  PublishIndexArming();

  // TODO: duplicated transaction finalization in md_deltas and deltas processing cases
  if (transaction_.deltas.empty() && transaction_.md_deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
    return {};
  }

  // This is usually done by the MVCC, but it does not handle the metadata deltas
  transaction_.EnsureCommitInfoExists();

  // On REPLICA, user transactions shouldn't commit anything
  if (!commit_args.durability_allowed()) [[unlikely]] {
    Abort();
    // We have not started a commit timestamp no cleanup needed for that
    DMG_ASSERT(!commit_timestamp_.has_value());
    return std::unexpected{ReplicaShouldNotWriteError{}};
  }

  if (auto const validation_result = ExistenceConstraintsViolation(); !validation_result.has_value()) {
    Abort();
    // We have not started a commit timestamp no cleanup needed for that
    DMG_ASSERT(!commit_timestamp_.has_value());
    return std::unexpected{validation_result.error()};
  }

  // Serialize committers across mint->durability->publish for two reasons:
  //
  // 1. Watermark contiguity: mint-order must equal publish-order so the read-snapshot watermark
  //    advances without gaps.  Acquired BEFORE the mint.  Held for the whole function.
  //
  // 2. Unique-constraint correctness: UniqueConstraintsViolation() runs during the brief phase-1
  //    engine_lock_ hold (~line below) and must see every already-committed value.  That requires
  //    that no other committer can sit between its own mint and publish while the validation runs
  //    -- an unpublished committer's values are not yet visible through the normal MVCC read path,
  //    so they would be invisible to the validator and a duplicate could slip through.
  //    commit_mutex_ provides exactly this guarantee.  Releasing commit_mutex_ earlier (e.g. after
  //    the WAL append) would break unique-constraint validation even if watermark ordering were
  //    re-established through another mechanism.
  std::optional<std::unique_lock<std::mutex>> commit_serializer;
  if (lockfree) {
    if (preheld_commit_lock.owns_lock()) {
      // Caller (Interpreter::Commit, U4a) already acquired commit_mutex_ via try_lock.
      // Adopt the existing hold so we do not re-acquire (which would deadlock).
      commit_serializer.emplace(std::move(preheld_commit_lock));
    } else {
      // No pre-held guard (PeriodicCommit, replica paths, or OFF-path callers that
      // passed the default-constructed lock): acquire blocking — OFF-path behavior
      // is byte-identical to before this change.
      commit_serializer.emplace(mem_storage->commit_mutex_);
    }
  }

  auto engine_guard = std::unique_lock{storage_->engine_lock_};
  commit_timestamp_.emplace(mem_storage->GetCommitTimestamp());

  // Unique constraints violated
  if (auto const validation_result = UniqueConstraintsViolation(); !validation_result.has_value()) {
    // Release engine lock because we don't have to hold it anymore
    engine_guard.unlock();
    AbortAndResetCommitTs();
    return std::unexpected{validation_result.error()};
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

  // Release engine_lock so WAL + replication run lock-free (commit_mutex_ still held);
  // BEGIN can now mint a start_timestamp without waiting on the durability RTT.
  if (lockfree) {
    engine_guard.unlock();
    InvokeProbe(mem_storage->commit_probe_, &CommitProbe::after_mint);
  }

  // Specific case in which durability mode is != PERIODIC_SNAPSHOT_WITH_WAL
  if (!mem_storage->InitializeWalFile(mem_storage->repl_storage_state_.epoch_.id())) {
    FinalizeCommitPhase(durability_commit_timestamp, /*acquire_engine_lock=*/lockfree);
    // No WAL file, hence no need to finalize it
    return {};
  }

  // If replica executes this, it will return immediately because it doesn't have any replicas registered (no
  // streams to obtain)
  auto replicating_txn =
      mem_storage->repl_storage_state_.StartPrepareCommitPhase(durability_commit_timestamp, mem_storage, commit_args);

  // If main executes this: Block until we receive votes from all replicas.
  // If replica executes this:,
  if (lockfree) {
    InvokeProbe(mem_storage->commit_probe_, &CommitProbe::during_durability);
  }
  auto const repl_prepare_phase_ok =
      HandleDurabilityAndReplicate(durability_commit_timestamp, replicating_txn, commit_args);

  // If replica executes this
  bool const replica_write_was_applied =
      commit_args.apply_if_replica_write([&](bool two_phase_commit, uint64_t /*desired_commit_timestamp*/) {
        // If SYNC and ASYNC replica executes this, commit immediately while holding the engine lock
        if (!two_phase_commit) {
          // WAL file is already finalized
          FinalizeCommitPhase(durability_commit_timestamp, /*acquire_engine_lock=*/lockfree);
        }
      });
  if (replica_write_was_applied) {
    // If STRICT_SYNC replica with write txn executes this: return because the 2nd phase will be executed once we
    // receive FinalizeCommitRpc.
    return {};
  }

  auto res = commit_args.apply_if_main(
      [&](DatabaseProtector const &protector) -> std::expected<void, StorageManipulationError> {
        // From this point on, only main executes this
        // If there are no STRICT_SYNC replicas for the current txn
        if (!replicating_txn.ShouldRunTwoPC()) {
          // WAL file is already finalized
          FinalizeCommitPhase(durability_commit_timestamp, /*acquire_engine_lock=*/lockfree);

          auto failures = replicating_txn.CollectAllFailures();
          // update replicas' cached commit info to this txn's absolute committed-txn count
          replicating_txn.UpdateCommitTsInfo();

          if (!failures.empty()) {
            return std::unexpected{ReplicationError{.failures = std::move(failures), .transaction_committed = true}};
          }
          return {};
        }

        // If we are here, it means we are the main executing the commit and there are some STRICT_SYNC replicas in the
        // cluster.

        if (repl_prepare_phase_ok) {
          // All replicas voted yes, hence they want to commit the current transaction
          FinalizeCommitPhase(durability_commit_timestamp, /*acquire_engine_lock=*/lockfree);
        }
        // We need to finalize WAL file after running FinalizeCommitPhase because we update there commit value in WAL

        if (mem_storage->wal_file_) {
          mem_storage->FinalizeWalFile();
        }
        // Send to all replicas they can finalize a transaction
        replicating_txn.FinalizeTransaction(
            repl_prepare_phase_ok, mem_storage->uuid(), protector, durability_commit_timestamp);

        auto failures = replicating_txn.CollectAllFailures();
        // update replicas' cached commit info only if the txn was actually committed
        if (repl_prepare_phase_ok) {
          replicating_txn.UpdateCommitTsInfo();
        }

        if (!failures.empty()) {
          // Release engine lock because we don't have to hold it anymore for abort
          if (engine_guard.owns_lock()) engine_guard.unlock();
          AbortAndResetCommitTs();
          return std::unexpected{ReplicationError{.failures = std::move(failures), .transaction_committed = false}};
        }

        return {};
      });
  DMG_ASSERT(res, "The commit was not applied!");
  return *std::move(res);
}

void InMemoryStorage::InMemoryAccessor::FinalizeCommitPhase(uint64_t const durability_commit_timestamp,
                                                            bool const acquire_engine_lock) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  std::optional<std::unique_lock<utils::SpinLock>> pub_guard;
  if (acquire_engine_lock) {
    InvokeProbe(mem_storage->commit_probe_, &CommitProbe::before_publish);
    pub_guard.emplace(storage_->engine_lock_);
  }

  if (config_.enable_schema_info) {
    // Queue schema update instead of processing immediately. This ensures
    // schema updates are processed in commit timestamp order, solving a race
    // condition whereby a slow in-flight edge operation can be accidentally
    // read by the schema processing code in a label modification.
    std::lock_guard<std::mutex> const lock{mem_storage->schema_queue_mutex_};
    mem_storage->pending_schema_updates_.emplace(
        durability_commit_timestamp,
        SchemaUpdateData(std::move(transaction_.schema_diff_),
                         std::move(transaction_.post_process_),
                         transaction_.SchemaReconstructionBound(),
                         durability_commit_timestamp,
                         mem_storage->config_.salient.items.properties_on_edges));
  }

  // We only need to update commit flag from false->true if we are running 2PC. In all other situations, the default
  // is fine.
  if (wal_txn_positions_.commit_flag_wal_position_ != 0 && needs_wal_update_) {
    mem_storage->wal_file_->UpdateCommitStatus(wal_txn_positions_);
  }

  MG_ASSERT(transaction_.commit_info != nullptr, "Invalid database state!");
  transaction_.commit_info->timestamp.store(*commit_timestamp_, std::memory_order_release);

  // If the transaction had non-sequential deltas (or another transaction propagated
  // the flag to us), we should re-establish the `has_uncommitted_non_sequential_deltas`
  // flag on any vertices we've touched
  bool const needs_vertex_flag_cleanup = std::invoke([&] -> bool {
    auto guard = std::lock_guard{transaction_.commit_info->lock};
    auto prior_state = std::exchange(transaction_.commit_info->non_seq_propagation, NonSeqPropagationState::HANDLED);
    return transaction_.has_non_sequential_deltas || prior_state == NonSeqPropagationState::PENDING;
  });

  if (needs_vertex_flag_cleanup) {
    std::unordered_set<Vertex *> vertices_to_check;
    DeltaVertexCache delta_vertex_cache{transaction_.transaction_id};
    for (Delta const &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      if (prev.type == PreviousPtr::Type::VERTEX) {
        vertices_to_check.insert(prev.vertex);
      } else if (prev.type == PreviousPtr::Type::DELTA && IsDeltaNonSequential(delta)) {
        Vertex *vertex = delta_vertex_cache.GetVertexFromDelta(&delta);
        if (vertex != nullptr) {
          vertices_to_check.insert(vertex);
        }
      }
    }

    // NOLINTNEXTLINE(bugprone-nondeterministic-pointer-iteration-order)
    for (Vertex *vertex : vertices_to_check) {
      auto guard = std::unique_lock{vertex->lock};
      if (vertex->has_uncommitted_non_sequential_deltas()) {
        vertex->set_has_uncommitted_non_sequential_deltas(
            HasUncommittedNonSequentialDeltas(vertex, transaction_.transaction_id));
      }
    }
  }

#ifndef NDEBUG
  auto const prev = mem_storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
  DMG_ASSERT(durability_commit_timestamp >= prev, "LDT not monotonically increasing");
#endif

  auto const update_func = [durability_commit_timestamp](CommitTsInfo const &old_ts_info) -> CommitTsInfo {
    return CommitTsInfo{.ldt_ = durability_commit_timestamp,
                        .num_committed_txns_ = old_ts_info.num_committed_txns_ + 1};
  };
  // update main's cached info
  atomic_struct_update<CommitTsInfo>(mem_storage->repl_storage_state_.commit_ts_info_, update_func);

  // Install the new point index, if needed
  auto point_updater = mem_storage->indices_.MakeUpdater();
  mem_storage->indices_.point_index_.InstallNewPointIndex(
      transaction_.point_index_change_collector_, transaction_.point_index_ctx_, point_updater);

  // Drop abort callbacks before running publishers: from this point on we are
  // committing — partially or fully — state that must NOT be undone by a later
  // Abort(). If a commit_callback throws partway, the destructor's Abort() will
  // see an empty abort list rather than tearing down indices that were already
  // published.
  transaction_.abort_callbacks_.Clear();
  transaction_.commit_callbacks_.RunAll(*commit_timestamp_);

  // Dispatch to another async work to create requested auto-indexes in their own transaction
  if (transaction_.storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    transaction_.async_index_helper_.DispatchRequests(mem_storage->async_indexer_);
  }

  // Mark transaction as finished for commit ordering and MVCC visibility.
  // NOTE: Schema updates may still be queued in pending_schema_updates_ with raw pointers
  // to vertices. GC protects these by using last_processed_commit_ts_ as a safety horizon
  // (see CollectGarbage implementation).
  mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);

  if (config_.enable_schema_info) {
    mem_storage->ProcessPendingSchemaUpdates(durability_commit_timestamp);
  }

  CheckForFastDiscardOfDeltas();
  // Skip the virtual dispatch when the txn didn't touch any text/text-edge data
  // (the common case for the commit hot path).
  if (!transaction_.text_index_change_collector_.empty()) {
    transaction_.active_indices_->text_->ApplyTrackedChanges(transaction_, mem_storage->name_id_mapper_.get());
  }
  if (!transaction_.text_edge_index_change_collector_.empty()) {
    transaction_.active_indices_->text_edge_->ApplyTrackedChanges(transaction_, mem_storage->name_id_mapper_.get());
  }

  if (mem_storage->config_.experimental_lockfree_read_snapshot) {
    // Publish the watermark: readers that BEGIN after this see this commit. Ordered AFTER the
    // commit_info->timestamp store and MarkFinished, under the same publish engine_lock hold.
    mem_storage->last_committed_mvcc_ts_.store(*commit_timestamp_, std::memory_order_release);
    InvokeProbe(mem_storage->commit_probe_, &CommitProbe::after_publish);
  }
  is_transaction_active_ = false;
}

// NOLINTNEXTLINE(google-default-arguments)
std::expected<void, StorageManipulationError> InMemoryStorage::InMemoryAccessor::PeriodicCommit(
    CommitArgs commit_args) {
  auto result = PrepareForCommitPhase(std::move(commit_args));

  const auto fatal_error = !result && std::visit(
                                          [](const auto &e) {
                                            using E = std::remove_cvref_t<decltype(e)>;
                                            if constexpr (std::is_same_v<E, storage::ReplicationError>) {
                                              // Replication errors are fatal only if the transaction was aborted
                                              return !e.transaction_committed;
                                            }
                                            return true;  // all other errors are fatal
                                          },
                                          result.error());

  if (fatal_error) {
    // PrepareForCommitPhase aborted the transaction internally (e.g. constraint/serialization error).
    // The caller's cleanup path will call Abort() which handles is_transaction_active_=false gracefully.
    return result;
  }

  FinalizeTransaction();

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  auto new_transaction = mem_storage->CreateTransaction(transaction_.isolation_level, transaction_.storage_mode);
  transaction_.start_timestamp = new_transaction.start_timestamp;
  transaction_.transaction_id = new_transaction.transaction_id;
  // PERIODIC COMMIT advances the SI snapshot boundary too, so the next batch sees the batch just
  // committed above (and does not pin GC). Unconditional: with the experiment OFF, snapshot_ts equals
  // start_timestamp and is unused by the read path, so this copy is inert.
  transaction_.snapshot_ts = new_transaction.snapshot_ts;
  transaction_.commit_info.reset();
  // Do NOT touch `original_start_timestamp` — it must remain stable per-query
  // (procedures use it as a cache key across PERIODIC COMMIT).

  is_transaction_active_ = true;

  return result;
}

void InMemoryStorage::InMemoryAccessor::GCRapidDeltaCleanup(BatchedList<Edge *> &current_deleted_edges,
                                                            BatchedList<Gid> &current_deleted_vertices,
                                                            IndexArming &arming) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // STEP 1) ensure everything in GC is gone

  // 1.a) old garbage_undo_buffers are safe to remove
  //      we are the only transaction, no one is reading those unlinked deltas
  mem_storage->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) { garbage_undo_buffers.clear(); });

  // 1.b.0) old committed_transactions_ and waiting_gc_deltas_ need minimal unlinking + remove + clear
  //      must be done before this transactions delta unlinking
  auto linked_undo_buffers = std::list<GCDeltas, memory::DbAwareAllocator<GCDeltas>>{};
  mem_storage->committed_transactions_.WithLock(
      [&](auto &committed_transactions) { committed_transactions.swap(linked_undo_buffers); });
  mem_storage->waiting_gc_deltas_.WithLock(
      [&](auto &waiting_list) { linked_undo_buffers.splice(linked_undo_buffers.end(), waiting_list); });

  // 1.b.1) unlink, gathering the removals. These belong to other transactions, so each is read
  //        using its own record of what its property writes were on, not this transaction's.
  for (auto &gc_deltas : linked_undo_buffers) {
    auto const arming_scope = arming.for_deltas_of(gc_deltas.wrote_properties_on_);
    UnlinkAndRemoveDeltas(gc_deltas.deltas_, current_deleted_edges, current_deleted_vertices, arming_scope);
  }

  // STEP 2) this transaction's deltas
  auto const arming_scope = arming.for_deltas_of(transaction_.wrote_properties_on);
  UnlinkAndRemoveDeltas(transaction_.deltas, current_deleted_edges, current_deleted_vertices, arming_scope);

  // STEP 3) clear all deltas after unlinking is complete
  linked_undo_buffers.clear();
  transaction_.deltas.clear();
}

void InMemoryStorage::InMemoryAccessor::FastDiscardOfDeltas(std::unique_lock<std::mutex> /*gc_guard*/) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  BatchedList<Gid> current_deleted_vertices;
  BatchedList<Edge *> current_deleted_edges;
  auto arming = IndexArming{};

  // STEP 1 + STEP 2 - delta cleanup
  GCRapidDeltaCleanup(current_deleted_edges, current_deleted_vertices, arming);

  // STEP 3) hand over the deleted vertices and edges to the GC
  if (!current_deleted_vertices.empty()) {
    mem_storage->deleted_vertices_.WithLock(
        [&](auto &deleted_vertices) { deleted_vertices.splice(current_deleted_vertices); });
  }
  if (!current_deleted_edges.empty()) {
    // Both heavy and light edges defer to deleted_edges_ here; light edges are
    // pushed to the graveyard later, at GC-collection time.
    // O(1) splice under the SpinLock — never an O(batch) copy while locked.
    mem_storage->deleted_edges_.WithLock([&](auto &deleted_edges) { deleted_edges.splice(current_deleted_edges); });
  }

  // STEP 4) hint to GC that indices need cleanup for performance reasons. These deltas are gone
  // by the time a collection cycle runs, so this is the only place that can tell what they could
  // have invalidated.
  if (arming.arms_anything()) {
    mem_storage->pending_index_arming_.WithLock([&](IndexArming &pending) { pending |= arming; });
  }
}

void InMemoryStorage::InMemoryAccessor::Abort() { Abort({}); }

void InMemoryStorage::InMemoryAccessor::Abort(ProgressCallback const &on_progress) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");

  // An analytical abort undoes nothing, so its writes -- and whatever they left for a sweep to
  // collect -- are still there.
  PublishIndexArming();

  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  if (transaction_.commit_info != nullptr) {
    auto guard = std::lock_guard{transaction_.commit_info->lock};
    transaction_.commit_info->non_seq_propagation = NonSeqPropagationState::HANDLED;
  }

  // if we have no deltas then no need to do any undo work during Abort
  // note: this check also saves on unnecessary contention on `engine_lock_`
  if (!transaction_.deltas.empty()) {
    auto index_abort_processor = storage_->indices_.GetAbortProcessor(*transaction_.active_indices_);

    auto const has_any_unique_constraints = !transaction_.active_constraints_->unique_->empty();
    if (has_any_unique_constraints && transaction_.constraint_verification_info &&
        transaction_.constraint_verification_info->NeedsUniqueConstraintVerification()) {
      // Need to remove elements from constraints before handling of the deltas, so the elements match the correct
      // values. Use AbortProcessor pattern for efficient constraint-first iteration (one accessor per constraint).
      auto vertices_to_check = transaction_.constraint_verification_info->GetVerticesForUniqueConstraintChecking();
      auto abort_processor = transaction_.active_constraints_->unique_->GetAbortProcessor();
      for (auto const *vertex : vertices_to_check) {
        transaction_.active_constraints_->unique_->CollectForAbort(abort_processor, vertex);
      }
      transaction_.active_constraints_->unique_->AbortEntries(std::move(abort_processor.abortable_info_),
                                                              transaction_.start_timestamp);
    }

    // We collect vertices and edges we've deleted here into local vectors first,
    // then remove them directly from the skiplists and transfer ownership
    // to the GC in one locked batch, instead of acquiring the lock once per element.
    BatchedList<Gid> my_deleted_vertices;
    BatchedList<Edge *> my_deleted_edges;

    // TWO passes needed here
    // Abort will modify objects to restore state to how they were before this txn
    // The passes will find the head delta for each object and process the whole object,
    // To track which edge type indexes need cleaning up, we need the edge type which is held in vertices in/out edges
    // Hence need to first once to modify edges, so it can read vectices information intact.

    // Edges pass. Because edges cannot have non-sequential deltas, we needn't
    // concern ourselves with them here. We guarantee that any of our deltas
    // with an edge as the upstream object are a monolithic block of deltas
    // belonging to this transaction, and that these terminate in either a
    // nullptr or another monolithic block belonging to a committed but
    // uncollected transaction.
    for (const auto &delta : transaction_.deltas) {
      if (on_progress) on_progress();
      auto prev = delta.prev.Get();
      switch (prev.type) {
        case PreviousPtr::Type::EDGE: {
          auto *edge = prev.edge;
          auto guard = std::lock_guard{edge->lock};
          Delta *current = edge->delta();
          while (current != nullptr &&
                 current->commit_info->timestamp.load(std::memory_order_acquire) == transaction_.transaction_id) {
            switch (current->action) {
              case Delta::Action::SET_PROPERTY: {
                DMG_ASSERT(mem_storage->config_.salient.items.properties_on_edges, "Invalid database state!");

                auto prop_id = current->property.key;
                auto *from_vertex = current->property.out_vertex;

                index_abort_processor.CollectOnEdgePropertyChange(
                    prop_id, *current->property.value, from_vertex, edge, transaction_.deltas);

                edge->properties.SetProperty(prop_id, *current->property.value);

                break;
              }
              case Delta::Action::DELETE_DESERIALIZED_OBJECT:
              case Delta::Action::DELETE_OBJECT: {
                edge->SetDeleted(true);
                my_deleted_edges.push_back(edge);
                break;
              }
              case Delta::Action::RECREATE_OBJECT: {
                edge->SetDeleted(false);
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
          edge->SetDelta(current);
          if (current != nullptr) {
            current->prev.Set(edge);
          }

          break;
        }
        case PreviousPtr::Type::VERTEX:
        case PreviousPtr::Type::DELTA:
        // pointer probably couldn't be set because allocation failed
        case PreviousPtr::Type::NULL_PTR:
          break;
      }
    }

    // Vertices pass
    // Track if any deltas are downstream from another transaction's deltas.
    // We unlink deltas from vertices/edges where possible (VERTEX/EDGE prev),
    // but if any delta has `delta.prev` pointing to a delta from another transaction,
    // we must route all deltas through `waiting_gc_deltas_` since GC must wait
    // until the upstream transaction is also garbage collected.

    // Applies each undo delta in the chain, and optionally unlinks the deltas
    // from the vertex head.
    auto process_vertex_deltas = [&](Vertex *vertex, Delta *start, bool delta_chunk_attached_to_vertex) {
      auto remove_in_edges = absl::flat_hash_set<EdgeRef>{};
      auto remove_out_edges = absl::flat_hash_set<EdgeRef>{};

      Delta *current = start;
      while (current != nullptr &&
             current->commit_info->timestamp.load(std::memory_order_acquire) == transaction_.transaction_id) {
        switch (current->action) {
          case Delta::Action::REMOVE_LABEL: {
            auto it = r::find(vertex->labels, current->label.value);
            MG_ASSERT(it != vertex->labels.end(), "Invalid database state!");
            std::swap(*it, *vertex->labels.rbegin());
            vertex->labels.pop_back();

            storage_->UpdateLabelCount(current->label.value, -1);

            index_abort_processor.CollectOnLabelRemoval(current->label.value, vertex);
            break;
          }
          case Delta::Action::ADD_LABEL: {
            auto it = r::find(vertex->labels, current->label.value);
            MG_ASSERT(it == vertex->labels.end(), "Invalid database state!");
            vertex->labels.push_back(current->label.value);

            storage_->UpdateLabelCount(current->label.value, 1);
            index_abort_processor.CollectOnLabelAddition(current->label.value, vertex);
            break;
          }
          case Delta::Action::SET_PROPERTY: {
            // For label index nothing
            // For property label index
            //  check if we care about the property, this will return all the labels and then get current property
            //  value
            index_abort_processor.CollectOnPropertyChange(current->property.key, *current->property.value, vertex);
            // Setting the correct value
            vertex->properties.SetProperty(current->property.key, *current->property.value);
            break;
          }
          case Delta::Action::ADD_IN_EDGE: {
            auto link = std::tuple{
                current->vertex_edge.edge_type, current->vertex_edge.vertex.Get(), current->vertex_edge.edge};
            DMG_ASSERT(r::find(vertex->in_edges, link) == vertex->in_edges.end(), "Invalid database state!");
            vertex->in_edges.push_back(link);
            break;
          }
          case Delta::Action::ADD_OUT_EDGE: {
            auto link = std::tuple{
                current->vertex_edge.edge_type, current->vertex_edge.vertex.Get(), current->vertex_edge.edge};
            DMG_ASSERT(r::find(vertex->out_edges, link) == vertex->out_edges.end(), "Invalid database state!");
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

            if (!mem_storage->config_.salient.items.properties_on_edges) break;

            auto const &[_, edge_type, to_vertex, edge] = current->vertex_edge;
            index_abort_processor.CollectOnEdgeRemoval(edge_type, vertex, to_vertex.Get(), edge);
            // TODO: ensure collector also processeses for edge_type+property index

            break;
          }
          case Delta::Action::DELETE_DESERIALIZED_OBJECT:
          case Delta::Action::DELETE_OBJECT: {
            vertex->SetDeleted(true);
            my_deleted_vertices.push_back(vertex->gid);

            for (auto const label : vertex->labels) {
              storage_->UpdateLabelCount(label, -1);
            }
            break;
          }
          case Delta::Action::RECREATE_OBJECT: {
            vertex->SetDeleted(false);
            for (auto const label : vertex->labels) {
              storage_->UpdateLabelCount(label, 1);
            }
            break;
          }
        }
        current = current->next.load(std::memory_order_acquire);
      }

      // bulk remove in_edges
      if (!remove_in_edges.empty()) {
        auto mid = r::partition(vertex->in_edges, [&](auto const &edge_tuple) {
          return !remove_in_edges.contains(std::get<EdgeRef>(edge_tuple));
        });
        vertex->in_edges.erase(mid, vertex->in_edges.end());
        vertex->in_edges.shrink_to_fit();
      }

      // bulk remove out_edges
      if (!remove_out_edges.empty()) {
        auto mid = r::partition(vertex->out_edges, [&](auto const &edge_tuple) {
          return !remove_out_edges.contains(std::get<EdgeRef>(edge_tuple));
        });
        vertex->out_edges.erase(mid, vertex->out_edges.end());
        vertex->out_edges.shrink_to_fit();
      }

      if (delta_chunk_attached_to_vertex) {
        vertex->SetDelta(current);
        if (current) {
          current->prev.Set(vertex);
        }
      } else {
        // Surgical delta chain removal: we don't own the head, so our deltas are
        // non-sequential in the middle of the chain. We need to "snip out" our deltas
        // without disturbing the rest of the chain.
        // We handle four cases depending on what surrounds our delta subchain:
        auto prev = start->prev.Get();
        DMG_ASSERT(prev.type == PreviousPtr::Type::DELTA && prev.delta != nullptr);
        prev.delta->next.store(current, std::memory_order_release);
        if (current) {
          current->prev.Set(prev.delta);
        }
      }

      // NOTE TO COLIN: as we snip out aborted chunks of interleaved NonSeq, we may hit this many times
      //                it would do upto full walks of remaining delta chain
      //                maybe another justification for a sinlge lock + pass (caching wont help the walks)
      if (vertex->has_uncommitted_non_sequential_deltas()) {
        vertex->set_has_uncommitted_non_sequential_deltas(
            HasUncommittedNonSequentialDeltas(vertex, transaction_.transaction_id));
      }
    };

    DeltaVertexCache delta_vertex_cache{transaction_.transaction_id};

    for (Delta &delta : transaction_.deltas) {
      if (on_progress) on_progress();
      auto prev = delta.prev.Get();
      switch (prev.type) {
        case PreviousPtr::Type::VERTEX: {
          auto *vertex = prev.vertex;
          auto guard = std::unique_lock{vertex->lock};

          // Check if we're still at the head - another tx may have prepended
          bool const we_own_head = vertex->delta() == &delta;
          process_vertex_deltas(vertex, &delta, we_own_head);

          break;
        }
        case PreviousPtr::Type::DELTA: {
          // If prev delta belongs to another transaction, our deltas are downstream
          // and must wait in `waiting_gc_deltas_` until all contributor transactions
          // are finished.
          if (prev.delta->commit_info->timestamp.load(std::memory_order_acquire) != transaction_.transaction_id) {
            Vertex *vertex = delta_vertex_cache.GetVertexFromDelta(&delta);
            auto guard = std::unique_lock{vertex->lock};
            // Check if we're still at the head - another tx may have prepended
            bool const we_own_head = vertex->delta() == &delta;
            process_vertex_deltas(vertex, &delta, we_own_head);
          }
          break;
        }
        case PreviousPtr::Type::EDGE:
        // pointer probably couldn't be set because allocation failed
        case PreviousPtr::Type::NULL_PTR:
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

        garbage_undo_buffers.emplace_back(mark_timestamp,
                                          std::move(transaction_.deltas),
                                          std::move(transaction_.commit_info),
                                          transaction_.transaction_id,
                                          transaction_.wrote_properties_on);
      });
    }

    /// We MUST unlink (aka. remove) entries in indexes and constraints
    /// before we unlink (aka. remove) vertices from storage
    /// this is because they point into vertices skip_list

    // Cleanup INDICES
    index_abort_processor.Process(storage_->indices_,
                                  *transaction_.active_indices_,
                                  transaction_.start_timestamp,
                                  mem_storage->name_id_mapper_.get());
    // Handed to CollectGarbage for the same reason as the edges below: a removal on this thread
    // races a sweep or scan holding no pin on `vertices_`.
    if (!my_deleted_vertices.empty()) {
      mem_storage->deleted_vertices_.WithLock(
          [&](auto &deleted_vertices) { deleted_vertices.splice(my_deleted_vertices); });
    }

    // EDGES / LIGHT EDGES
    // Hand these to CollectGarbage rather than removing them here, so that the GC pass is the only
    // place an edge leaves storage. An inline removal runs on this client thread, unsynchronised
    // with a sweep or a scan that is mid-walk over an index entry naming the edge: the removal
    // tags the node with the newest accessor id, which nothing that started later holds back, so
    // it can be freed under a reader that had no chance to pin it first. Deferring means
    // every removal is ordered after the same pass's index cleanup, on the one thread that does
    // it. Light edges have no skiplist node and are only ever routed this way -- pushing them to
    // light_edge_graveyard_ here would snap the guard epoch before post-abort readers exist, and
    // the drain would free under them -- so both kinds now take the same path.
    // O(1) splice under the SpinLock -- never an O(batch) copy while locked.
    if (!my_deleted_edges.empty()) {
      mem_storage->deleted_edges_.WithLock([&](auto &deleted_edges) { deleted_edges.splice(my_deleted_edges); });
    }
  }

  transaction_.abort_callbacks_.RunAll();

  mem_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
}

void InMemoryStorage::InMemoryAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

    // Hand this transaction's deltas over to GC BEFORE marking the commit
    // timestamp finished. MarkFinished advances commit_log_->OldestActive(),
    // and another committing transaction's fast-discard precondition
    // (CheckForFastDiscardOfDeltas: `no_older_transactions`) treats an advanced
    // OldestActive as "I am the only transaction left, so every delta except my
    // own is already registered in committed_transactions_/waiting_gc_deltas_
    // and safe to free". If we marked finished first, there would be a window in
    // which this transaction is no longer active yet its deltas are still linked
    // in the version chains and not yet registered, so that fast-discard could
    // free a delta our still-linked deltas reference via `prev`, leaving a
    // dangling pointer for a later GC to dereference (use-after-free). The
    // registration lock release is sequenced-before MarkFinished, so any thread
    // that observes this txn as finished (through the commit_log SpinLock) also
    // observes the registration (through the committed_transactions_ SpinLock).
    if (!transaction_.deltas.empty()) {
      if (transaction_.has_non_sequential_deltas) {
        mem_storage->waiting_gc_deltas_.WithLock([&](auto &waiting_list) {
          waiting_list.emplace_back(InMemoryStorage::GCDeltas(0,
                                                              std::move(transaction_.deltas),
                                                              std::move(transaction_.commit_info),
                                                              transaction_.transaction_id,
                                                              transaction_.wrote_properties_on));
        });
      } else {
        mem_storage->committed_transactions_.WithLock([&](auto &committed_transactions) {
          committed_transactions.emplace_back(0,
                                              std::move(transaction_.deltas),
                                              std::move(transaction_.commit_info),
                                              transaction_.transaction_id,
                                              transaction_.wrote_properties_on);
        });
      }
    }

    mem_storage->commit_log_->MarkFinished(*commit_timestamp_);
    commit_timestamp_.reset();
  }
}

void InMemoryStorage::ProcessPendingSchemaUpdates(uint64_t up_to_commit_ts) {
  std::vector<SchemaUpdateData> to_process;
  uint64_t new_last_processed = 0;

  // Process in commit timestamp order, bounded by the `up_to_commit_ts`
  // timestamp
  {
    std::lock_guard<std::mutex> const lock{schema_queue_mutex_};

    auto it = pending_schema_updates_.upper_bound(last_processed_commit_ts_);
    while (it != pending_schema_updates_.end() && it->first <= up_to_commit_ts) {
      to_process.push_back(std::move(it->second));
      new_last_processed = it->first;
      it = pending_schema_updates_.erase(it);
    }

    if (!to_process.empty()) {
      last_processed_commit_ts_ = new_last_processed;
    }
  }

  for (auto &update : to_process) {
    schema_info_.ProcessTransaction(
        update.schema_diff, update.post_process, update.snapshot_bound, update.commit_ts, update.property_on_edges);
  }
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateIndex(
    LabelId label, CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating label index requires a unique or read only access to the storage!");

  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(storage_->indices_.label_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_label_index->RegisterIndex(label, updater)) {
    return std::unexpected{IndexDefinitionAlreadyExistsError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_label_index
           ->PopulateIndex(
               label, in_memory->vertices_.access(), std::nullopt, updater, {}, &transaction_, std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }

  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_label_index->PublishIndex(label, commit_timestamp); });

  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add(
      [mem_label_index, label, updater]() { (void)mem_label_index->DropIndex(label, updater); });

  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_create, label);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

auto InMemoryStorage::InMemoryAccessor::CreateIndex(LabelId label, PropertiesPaths properties, IndexOrder order,
                                                    CheckCancelFunction cancel_check)
    -> std::expected<void, StorageIndexDefinitionError> {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating label-property index requires a unique or read only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_label_property_index->RegisterIndex(label, properties, updater, order)) {
    return std::unexpected{IndexDefinitionAlreadyExistsError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_label_property_index
           ->PopulateIndex(label,
                           properties,
                           in_memory->vertices_.access(),
                           std::nullopt,
                           updater,
                           {},
                           order,
                           &transaction_,
                           std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper([=](uint64_t commit_timestamp) {
    return mem_label_property_index->PublishIndex(label, properties, commit_timestamp, order);
  });
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add([mem_label_property_index, label, properties, updater, order]() {
    (void)mem_label_property_index->DropIndex(label, properties, updater, order);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_create, label, std::move(properties), order);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

void InMemoryStorage::InMemoryAccessor::DowngradeToReadIfValid() {
  // Only transactional can let writers in for the population: they record deltas, so the populating
  // scan still reads its own snapshot, and their stale index entries wait for GC. Analytical writes
  // in place with no deltas to read past, and its index maintenance erases entries eagerly (see
  // ActiveIndices::UpdateOnRemoveLabel), which would race the scan that is still filling the same
  // skip list. Keep the READ_ONLY hold, which excludes writers and analytical GC alike.
  if (transaction_.storage_mode != StorageMode::IN_MEMORY_TRANSACTIONAL) return;
  if (guard_.owns_lock() && guard_.type() == utils::ResourceLockGuard::READ_ONLY) {
    guard_.downgrade_to_read();
  }
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateIndex(
    EdgeTypeId edge_type, CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Create index requires a unique or read only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(in_memory->indices_.edge_type_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_edge_type_index->RegisterIndex(edge_type, updater)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_edge_type_index
           ->PopulateIndex(
               edge_type, in_memory->vertices_.access(), updater, {}, &transaction_, std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_edge_type_index->PublishIndex(edge_type, commit_timestamp); });
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add(
      [mem_edge_type_index, edge_type, updater]() { (void)mem_edge_type_index->DropIndex(edge_type, updater); });

  transaction_.md_deltas.emplace_back(MetadataDelta::edge_index_create, edge_type);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateIndex(
    EdgeTypeId edge_type, PropertyId property, CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Create edge-type property index requires unique or read only access to the storage!");

  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return std::unexpected{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_type_property_index =
      static_cast<InMemoryEdgeTypePropertyIndex *>(in_memory->indices_.edge_type_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_edge_type_property_index->RegisterIndex(edge_type, property, updater)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_edge_type_property_index
           ->PopulateIndex(
               edge_type, property, in_memory->vertices_.access(), updater, {}, &transaction_, std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper([=](uint64_t commit_timestamp) {
    return mem_edge_type_property_index->PublishIndex(edge_type, property, commit_timestamp);
  });
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add([mem_edge_type_property_index, edge_type, property, updater]() {
    (void)mem_edge_type_property_index->DropIndex(edge_type, property, updater);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::edge_property_index_create, edge_type, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateGlobalEdgeIndex(
    PropertyId property, CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating global edge property index requires unique or read-only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return std::unexpected{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_property_index =
      static_cast<InMemoryEdgePropertyIndex *>(in_memory->indices_.edge_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_edge_property_index->RegisterIndex(property, updater)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_edge_property_index
           ->PopulateIndex(property, in_memory->vertices_.access(), updater, {}, &transaction_, std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }
  // Wrapper will make sure plan cache is cleared
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_edge_property_index->PublishIndex(property, commit_timestamp); });
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add(
      [mem_edge_property_index, property, updater]() { (void)mem_edge_property_index->DropIndex(property, updater); });

  transaction_.md_deltas.emplace_back(MetadataDelta::global_edge_property_index_create, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateGlobalVertexIndex(
    PropertyId property, CheckCancelFunction cancel_check) {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY,
            "Creating global vertex property index requires unique or read-only access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_vertex_property_index =
      static_cast<InMemoryVertexPropertyIndex *>(in_memory->indices_.vertex_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  if (!mem_vertex_property_index->RegisterIndex(property, updater)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  DowngradeToReadIfValid();
  if (!mem_vertex_property_index
           ->PopulateIndex(property,
                           in_memory->vertices_.access(),
                           std::nullopt,
                           updater,
                           {},
                           &transaction_,
                           std::move(cancel_check))
           .has_value()) {
    return std::unexpected{IndexDefinitionCancelationError{}};
  }
  auto publisher = storage_->invalidator_->invalidate_for_timestamp_wrapper(
      [=](uint64_t commit_timestamp) { return mem_vertex_property_index->PublishIndex(property, commit_timestamp); });
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add([mem_vertex_property_index, property, updater]() {
    (void)mem_vertex_property_index->DropIndex(property, updater);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::global_vertex_property_index_create, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropIndex(LabelId label) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping label index requires a unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory->indices_.label_index_.get());
  auto updater = storage_->indices_.MakeUpdater();

  // Done inside the wrapper to ensure plan cache invalidation is safe.
  // Capture the evicted entry so an aborted DROP (e.g. STRICT_SYNC commit failure)
  // can re-install it instead of leaving the index permanently gone on main.
  std::shared_ptr<InMemoryLabelIndex::IndividualIndex> evicted;
  storage_->invalidator_->invalidate_now([&] {
    evicted = mem_label_index->DropIndex(label, updater);
    return static_cast<bool>(evicted);
  });
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add([mem_label_index, label, updater, evicted]() mutable {
    mem_label_index->RestoreIndex(label, std::move(evicted), updater);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_drop, label);
  // We don't care if there is a replication error because on main node the change will go through
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropIndex(
    LabelId label, std::vector<storage::PropertyPath> &&properties, std::optional<IndexOrder> order) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping label-property index requires a unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory->indices_.label_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();

  LabelPropertyIndex::DropResult drop_result;
  std::optional<InMemoryLabelPropertyIndex::AscIndexPtrVariant> asc_evicted;
  std::optional<InMemoryLabelPropertyIndex::DescIndexPtrVariant> desc_evicted;
  std::optional<InMemoryLabelPropertyIndex::PropertiesIndicesStats> stats_evicted;
  storage_->invalidator_->invalidate_now([&] {
    auto captured = mem_label_property_index->DropIndex(label, properties, updater, order);
    drop_result = captured.result;
    asc_evicted = std::move(captured.asc_evicted);
    desc_evicted = std::move(captured.desc_evicted);
    stats_evicted = std::move(captured.stats_evicted);
    return static_cast<bool>(drop_result);
  });
  if (!drop_result) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add(
      [mem_label_property_index, label, properties, updater, asc_evicted, desc_evicted, stats_evicted]() mutable {
        mem_label_property_index->RestoreIndex(label,
                                               std::move(properties),
                                               std::move(asc_evicted),
                                               std::move(desc_evicted),
                                               std::move(stats_evicted),
                                               updater);
      });

  if (drop_result.dropped_asc) {
    transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_drop,
                                        label,
                                        std::vector<storage::PropertyPath>(properties),
                                        IndexOrder::ASC);
  }
  if (drop_result.dropped_desc) {
    transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_drop,
                                        label,
                                        std::vector<storage::PropertyPath>(properties),
                                        IndexOrder::DESC);
  }
  // We don't care if there is a replication error because on main node the change will go through

  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropIndex(EdgeTypeId edge_type) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping edge-type index requires a unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_edge_type_index = static_cast<InMemoryEdgeTypeIndex *>(in_memory->indices_.edge_type_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  // Done inside the wrapper to ensure plan cache invalidation is safe.
  std::shared_ptr<InMemoryEdgeTypeIndex::IndividualIndex> evicted;
  storage_->invalidator_->invalidate_now([&] {
    evicted = mem_edge_type_index->DropIndex(edge_type, updater);
    return static_cast<bool>(evicted);
  });
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add([mem_edge_type_index, edge_type, updater, evicted]() mutable {
    mem_edge_type_index->RestoreIndex(edge_type, std::move(evicted), updater);
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::edge_index_drop, edge_type);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropIndex(EdgeTypeId edge_type,
                                                                                              PropertyId property) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping edge-type property index requires a unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to drop the index, no properties on edges
    return std::unexpected{IndexDefinitionConfigError{}};
  }
  auto *mem_edge_type_property_index =
      static_cast<InMemoryEdgeTypePropertyIndex *>(in_memory->indices_.edge_type_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  // Done inside the wrapper to ensure plan cache invalidation is safe.
  std::shared_ptr<InMemoryEdgeTypePropertyIndex::IndividualIndex> evicted;
  storage_->invalidator_->invalidate_now([&] {
    evicted = mem_edge_type_property_index->DropIndex(edge_type, property, updater);
    return static_cast<bool>(evicted);
  });
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add([mem_edge_type_property_index, edge_type, property, updater, evicted]() mutable {
    mem_edge_type_property_index->RestoreIndex(edge_type, property, std::move(evicted), updater);
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::edge_property_index_drop, edge_type, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropGlobalEdgeIndex(
    PropertyId property) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping global edge property index requires unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  if (!in_memory->config_.salient.items.properties_on_edges) {
    // Not possible to create the index, no properties on edges
    return std::unexpected{IndexDefinitionConfigError{}};
  }

  auto *mem_edge_property_index =
      static_cast<InMemoryEdgePropertyIndex *>(in_memory->indices_.edge_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  std::shared_ptr<InMemoryEdgePropertyIndex::IndividualIndex> evicted;
  storage_->invalidator_->invalidate_now([&] {
    evicted = mem_edge_property_index->DropIndex(property, updater);
    return static_cast<bool>(evicted);
  });
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add([mem_edge_property_index, property, updater, evicted]() mutable {
    mem_edge_property_index->RestoreIndex(property, std::move(evicted), updater);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::global_edge_property_index_drop, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropGlobalVertexIndex(
    PropertyId property) {
  MG_ASSERT(type() == UNIQUE || type() == READ_ONLY || type() == READ,
            "Dropping global vertex property index requires unique, read-only or read access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_vertex_property_index =
      static_cast<InMemoryVertexPropertyIndex *>(in_memory->indices_.vertex_property_index_.get());
  auto updater = storage_->indices_.MakeUpdater();
  std::shared_ptr<InMemoryVertexPropertyIndex::IndividualIndex> evicted;
  storage_->invalidator_->invalidate_now([&] {
    evicted = mem_vertex_property_index->DropIndex(property, updater);
    return static_cast<bool>(evicted);
  });
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.abort_callbacks_.Add([mem_vertex_property_index, property, updater, evicted]() mutable {
    mem_vertex_property_index->RestoreIndex(property, std::move(evicted), updater);
  });

  transaction_.md_deltas.emplace_back(MetadataDelta::global_vertex_property_index_drop, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreatePointIndex(
    storage::LabelId label, storage::PropertyId property, ProgressCallback const &on_progress) {
  MG_ASSERT(type() == UNIQUE, "Creating point index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &point_index = in_memory->indices_.point_index_;
  if (!point_index.CreatePointIndex(label, property, in_memory->vertices_.access(), on_progress)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  // Defer publication to commit time so concurrent readers don't observe a
  // create that gets rolled back. Matches the constraint / vector-index paths.
  auto updater = in_memory->indices_.MakeUpdater();
  auto &metric_handles = in_memory->metric_handles_;
  transaction_.commit_callbacks_.Add([&point_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
    point_index.PublishActiveIndices(updater);
    metric_handles.active_point_indices.Increment();
  });
  transaction_.abort_callbacks_.Add(
      [&point_index, label, property]() { (void)point_index.DropPointIndex(label, property); });
  transaction_.md_deltas.emplace_back(MetadataDelta::point_index_create, label, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropPointIndex(
    storage::LabelId label, storage::PropertyId property) {
  MG_ASSERT(type() == UNIQUE, "Dropping point index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &point_index = in_memory->indices_.point_index_;
  auto evicted = point_index.DropPointIndex(label, property);
  if (!evicted) {
    return std::unexpected{IndexDefinitionError{}};
  }
  // Defer publication to commit time. See CreatePointIndex above.
  auto updater = in_memory->indices_.MakeUpdater();
  auto &metric_handles = in_memory->metric_handles_;
  transaction_.commit_callbacks_.Add([&point_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
    point_index.PublishActiveIndices(updater);
    metric_handles.active_point_indices.Decrement();
  });
  transaction_.abort_callbacks_.Add([&point_index, label, property, evicted = std::move(evicted)]() mutable {
    point_index.RestorePointIndex(label, property, std::move(evicted));
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::point_index_drop, label, property);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateVectorIndex(
    VectorIndexSpec spec, ProgressCallback const &on_progress) {
  MG_ASSERT(type() == UNIQUE, "Creating vector index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  auto vertices_acc = in_memory->vertices_.access();
  // We don't allow creating vector index on nodes with the same name as vector edge index
  if (vector_edge_index.IndexExists(spec.index_name) ||
      !vector_index.CreateIndex(
          spec, vertices_acc, &in_memory->indices_, in_memory->name_id_mapper_.get(), on_progress)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  // Defer publication to commit time so concurrent readers don't observe a
  // create that gets rolled back. Matches the constraint CREATE/DROP paths below.
  auto updater = in_memory->indices_.MakeUpdater();
  auto &metric_handles = in_memory->metric_handles_;
  transaction_.commit_callbacks_.Add([&vector_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
    vector_index.PublishActiveIndices(updater);
    metric_handles.active_vector_indices.Increment();
  });
  // DropIndex undoes both the owner install and the eager vertex property
  // rewrite (Vector -> VectorIndexId) CreateIndex did.
  auto *name_mapper = in_memory->name_id_mapper_.get();
  auto const name = spec.index_name;
  transaction_.abort_callbacks_.Add(
      [&vector_index, name_mapper, name]() { vector_index.DropIndex(name, name_mapper); });
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_index_create, spec);
  return {};
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::DropVectorIndex(
    std::string_view index_name, ProgressCallback const &on_progress) {
  MG_ASSERT(type() == UNIQUE, "Dropping vector index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  auto updater = in_memory->indices_.MakeUpdater();
  auto &metric_handles = in_memory->metric_handles_;
  if (auto vec_capture = vector_index.DropIndex(index_name, in_memory->name_id_mapper_.get(), on_progress)) {
    transaction_.commit_callbacks_.Add([&vector_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
      vector_index.PublishActiveIndices(updater);
      metric_handles.active_vector_indices.Decrement();
    });
    // RestoreIndex puts the IndexItem back (usearch state survives via the captured
    // shared_ptr) and re-rewrites the touched vertex properties.
    transaction_.abort_callbacks_.Add([&vector_index, capture = std::move(*vec_capture)]() mutable {
      vector_index.RestoreIndex(std::move(capture));
    });
  } else if (auto edge_capture =
                 vector_edge_index.DropIndex(index_name, in_memory->name_id_mapper_.get(), on_progress)) {
    transaction_.commit_callbacks_.Add([&vector_edge_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
      vector_edge_index.PublishActiveIndices(updater);
      metric_handles.active_vector_edge_indices.Decrement();
    });
    transaction_.abort_callbacks_.Add([&vector_edge_index, capture = std::move(*edge_capture)]() mutable {
      vector_edge_index.RestoreIndex(std::move(capture));
    });
  } else {
    return std::unexpected{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_index_drop, index_name);
  return {};
}

utils::small_vector<uint64_t> InMemoryStorage::InMemoryAccessor::GetVectorIndexIdsForVertex(Vertex *vertex,
                                                                                            PropertyId property) {
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  return in_memory->indices_.vector_index_.GetVectorIndexIdsForVertex(vertex, property);
}

utils::small_vector<float> InMemoryStorage::InMemoryAccessor::GetVectorFromVectorIndex(
    Vertex *vertex, std::string_view index_name) const {
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  return in_memory->indices_.vector_index_.GetVectorPropertyFromIndex(
      vertex, index_name, in_memory->name_id_mapper_.get());
}

std::expected<void, StorageIndexDefinitionError> InMemoryStorage::InMemoryAccessor::CreateVectorEdgeIndex(
    VectorEdgeIndexSpec spec, ProgressCallback const &on_progress) {
  MG_ASSERT(type() == UNIQUE, "Creating vector edge index requires a unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto &vector_index = in_memory->indices_.vector_index_;
  auto &vector_edge_index = in_memory->indices_.vector_edge_index_;
  auto vertices_acc = in_memory->vertices_.access();
  // We don't allow creating vector edge index with the same name as vector index on nodes
  if (vector_index.IndexExists(spec.index_name, in_memory->name_id_mapper_.get()) ||
      !vector_edge_index.CreateIndex(spec, vertices_acc, in_memory->name_id_mapper_.get(), on_progress)) {
    return std::unexpected{IndexDefinitionError{}};
  }
  // Defer publication to commit time. See CreateVectorIndex above.
  auto updater = in_memory->indices_.MakeUpdater();
  auto &metric_handles = in_memory->metric_handles_;
  auto *name_mapper = in_memory->name_id_mapper_.get();
  auto const edge_index_name = spec.index_name;
  transaction_.abort_callbacks_.Add([&vector_edge_index, name_mapper, edge_index_name]() {
    vector_edge_index.DropIndex(edge_index_name, name_mapper);
  });
  transaction_.commit_callbacks_.Add([&vector_edge_index, updater, &metric_handles](uint64_t /*commit_ts*/) {
    vector_edge_index.PublishActiveIndices(updater);
    metric_handles.active_vector_edge_indices.Increment();
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::vector_edge_index_create, spec);
  return {};
}

std::expected<void, StorageExistenceConstraintDefinitionError>
InMemoryStorage::InMemoryAccessor::CreateExistenceConstraint(LabelId label, PropertyId property,
                                                             CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Creating existence requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *existence_constraints = in_memory->constraints_.existence_constraints_.get();
  if (!existence_constraints->RegisterConstraint(label, property)) {
    return std::unexpected{StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}}};
  }
  try {
    if (auto validation_result = ExistenceConstraints::ValidateVerticesOnConstraint(
            in_memory->vertices_.access(), label, property, std::nullopt, {}, cancel_check);
        !validation_result.has_value()) {
      (void)existence_constraints->DropConstraint(label, property);
      return std::unexpected{StorageExistenceConstraintDefinitionError{validation_result.error()}};
    }
  } catch (const utils::OutOfMemoryException &) {
    (void)existence_constraints->DropConstraint(label, property);
    throw;
  } catch (const PopulateCancel &) {
    (void)existence_constraints->DropConstraint(label, property);
    return std::unexpected{StorageExistenceConstraintDefinitionError{ConstraintDefinitionCancelationError{}}};
  }
  // Defer publication to commit time for MVCC correctness
  auto updater = in_memory->constraints_.MakeUpdater();
  auto publisher = [existence_constraints, label, property, updater](uint64_t commit_ts) {
    existence_constraints->PublishConstraint(label, property, commit_ts);
    updater(existence_constraints->GetActiveConstraints());
  };
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add(
      [existence_constraints, label, property]() { (void)existence_constraints->DropConstraint(label, property); });
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_create, label, property);
  return {};
}

std::expected<void, StorageExistenceConstraintDroppingError> InMemoryStorage::InMemoryAccessor::DropExistenceConstraint(
    LabelId label, PropertyId property) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Dropping existence constraint requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *existence_constraints = in_memory->constraints_.existence_constraints_.get();
  auto evicted = existence_constraints->DropConstraint(label, property);
  if (!evicted) {
    return std::unexpected{StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}}};
  }
  // Defer publication to commit time so concurrent readers don't observe the
  // drop if the DDL transaction aborts. Matches the CREATE path above.
  auto updater = in_memory->constraints_.MakeUpdater();
  transaction_.commit_callbacks_.Add([existence_constraints, updater](uint64_t /*commit_ts*/) {
    updater(existence_constraints->GetActiveConstraints());
  });
  // Reinstall the evicted entry on abort so the constraint stays live.
  transaction_.abort_callbacks_.Add([existence_constraints, label, property, evicted = std::move(evicted)]() mutable {
    existence_constraints->RestoreConstraint(label, property, std::move(evicted));
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_drop, label, property);
  return {};
}

std::expected<UniqueConstraints::CreationStatus, StorageUniqueConstraintDefinitionError>
InMemoryStorage::InMemoryAccessor::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                                          CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Creating unique constraint requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(in_memory->constraints_.unique_constraints_.get());
  // CreateConstraint drops the constraint it installed before letting the cancellation out.
  auto ret =
      std::invoke([&]() -> std::expected<UniqueConstraints::CreationStatus, StorageUniqueConstraintDefinitionError> {
        try {
          auto created = mem_unique_constraints->CreateConstraint(
              label, properties, in_memory->vertices_.access(), std::nullopt, {}, cancel_check);
          if (!created) {
            return std::unexpected{StorageUniqueConstraintDefinitionError{created.error()}};
          }
          return created.value();
        } catch (const PopulateCancel &) {
          return std::unexpected{StorageUniqueConstraintDefinitionError{ConstraintDefinitionCancelationError{}}};
        }
      });
  if (!ret) {
    return std::unexpected{ret.error()};
  }
  if (ret.value() != UniqueConstraints::CreationStatus::SUCCESS) {
    return ret.value();
  }
  // Defer publication to commit time for MVCC correctness
  auto updater = in_memory->constraints_.MakeUpdater();
  auto publisher = [mem_unique_constraints, label, properties, updater](uint64_t commit_ts) {
    mem_unique_constraints->PublishConstraint(label, properties, commit_ts);
    updater(mem_unique_constraints->GetActiveConstraints());
  };
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add([mem_unique_constraints, label, properties]() {
    (void)mem_unique_constraints->DropConstraint(label, properties);
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_create, label, properties);
  return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus InMemoryStorage::InMemoryAccessor::DropUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Dropping unique constraint requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *mem_unique_constraints =
      static_cast<InMemoryUniqueConstraints *>(in_memory->constraints_.unique_constraints_.get());
  auto captured = mem_unique_constraints->DropConstraint(label, properties);
  if (captured.status != UniqueConstraints::DeletionStatus::SUCCESS) {
    return captured.status;
  }
  // Defer publication to commit time so concurrent readers don't observe the
  // drop if the DDL transaction aborts. Matches the CREATE path above.
  auto updater = in_memory->constraints_.MakeUpdater();
  // Hand the evicted constraint to GC rather than letting it die with these callbacks: freeing its skiplist is
  // O(constrained vertices), and on a replica this runs on the RPC handler thread its peer is waiting on. Only on
  // commit -- an abort restores the constraint instead, and then this callback never runs.
  transaction_.commit_callbacks_.Add(
      [mem_unique_constraints, updater, evicted = captured.evicted](uint64_t /*commit_ts*/) mutable {
        updater(mem_unique_constraints->GetActiveConstraints());
        mem_unique_constraints->RetireConstraint(std::move(evicted));
      });
  // Reinstall the evicted entry on abort so the constraint stays live.
  transaction_.abort_callbacks_.Add(
      [mem_unique_constraints, label, properties, evicted = std::move(captured.evicted)]() mutable {
        mem_unique_constraints->RestoreConstraint(label, properties, std::move(evicted));
      });
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_drop, label, properties);
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

std::expected<void, StorageExistenceConstraintDefinitionError> InMemoryStorage::InMemoryAccessor::CreateTypeConstraint(
    LabelId label, PropertyId property, TypeConstraintKind kind, CheckCancelFunction cancel_check) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Creating IS TYPED constraint requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *type_constraints = in_memory->constraints_.type_constraints_.get();
  if (!type_constraints->RegisterConstraint(label, property, kind)) {
    return std::unexpected{StorageTypeConstraintDefinitionError{ConstraintDefinitionError{}}};
  }
  try {
    if (auto validation_result = TypeConstraints::ValidateVerticesOnConstraint(
            in_memory->vertices_.access(), label, property, kind, {}, cancel_check);
        !validation_result.has_value()) {
      (void)type_constraints->DropConstraint(label, property, kind);
      return std::unexpected{StorageTypeConstraintDefinitionError{validation_result.error()}};
    }
  } catch (const utils::OutOfMemoryException &) {
    (void)type_constraints->DropConstraint(label, property, kind);
    throw;
  } catch (const PopulateCancel &) {
    (void)type_constraints->DropConstraint(label, property, kind);
    return std::unexpected{StorageTypeConstraintDefinitionError{ConstraintDefinitionCancelationError{}}};
  }
  // Defer publication to commit time for MVCC correctness
  auto updater = in_memory->constraints_.MakeUpdater();
  auto publisher = [type_constraints, label, property, kind, updater](uint64_t commit_ts) {
    type_constraints->PublishConstraint(label, property, kind, commit_ts);
    updater(type_constraints->GetActiveConstraints());
  };
  transaction_.commit_callbacks_.Add(std::move(publisher));
  transaction_.abort_callbacks_.Add(
      [type_constraints, label, property, kind]() { (void)type_constraints->DropConstraint(label, property, kind); });
  transaction_.md_deltas.emplace_back(MetadataDelta::type_constraint_create, label, property, kind);
  return {};
}

std::expected<void, StorageTypeConstraintDroppingError> InMemoryStorage::InMemoryAccessor::DropTypeConstraint(
    LabelId label, PropertyId property, TypeConstraintKind kind) {
  // UNIQUE access will be done only through schema.assert
  MG_ASSERT(type() == READ_ONLY || type() == UNIQUE,
            "Dropping IS TYPED constraint requires a read only or unique access to the storage!");
  auto *in_memory = static_cast<InMemoryStorage *>(storage_);
  auto *type_constraints = in_memory->constraints_.type_constraints_.get();
  auto evicted = type_constraints->DropConstraint(label, property, kind);
  if (!evicted) {
    return std::unexpected{StorageTypeConstraintDroppingError{ConstraintDefinitionError{}}};
  }
  // Defer publication to commit time so concurrent readers don't observe the
  // drop if the DDL transaction aborts. Matches the CREATE path above.
  auto updater = in_memory->constraints_.MakeUpdater();
  transaction_.commit_callbacks_.Add(
      [type_constraints, updater](uint64_t /*commit_ts*/) { updater(type_constraints->GetActiveConstraints()); });
  // Reinstall the evicted entry on abort so the constraint stays live.
  transaction_.abort_callbacks_.Add([type_constraints, label, property, evicted = std::move(evicted)]() mutable {
    type_constraints->RestoreConstraint(label, property, std::move(evicted));
  });
  transaction_.md_deltas.emplace_back(MetadataDelta::type_constraint_drop, label, property, kind);
  return {};
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(LabelId label, View view) {
  auto *active_indices = static_cast<InMemoryLabelIndex::ActiveIndices *>(transaction_.active_indices_->label_.get());
  return VerticesIterable(active_indices->Vertices(label, view, storage_, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(
    LabelId label, std::span<storage::PropertyPath const> properties,
    std::span<storage::PropertyValueRange const> property_ranges, View view, IndexOrder order) {
  auto *active_indices =
      static_cast<InMemoryLabelPropertyIndex::ActiveIndices *>(transaction_.active_indices_->label_properties_.get());
  return active_indices->Vertices(label, properties, property_ranges, view, storage_, &transaction_, order);
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(View view, size_t num_chunks) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  const auto max_gid = Gid::FromUint(mem_storage->vertex_id_.load(std::memory_order_acquire));
  return VerticesChunkedIterable(
      AllVerticesChunkedIterable(mem_storage->vertices_.access(), num_chunks, storage_, &transaction_, view, max_gid));
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(LabelId label, View view,
                                                                           size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryLabelIndex::ActiveIndices *>(transaction_.active_indices_->label_.get());
  return VerticesChunkedIterable(
      active_indices->ChunkedVertices(label, std::move(vertices_acc), view, storage_, &transaction_, num_chunks));
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(
    LabelId label, std::span<storage::PropertyPath const> properties,
    std::span<storage::PropertyValueRange const> property_ranges, View view, size_t num_chunks, IndexOrder order) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryLabelPropertyIndex::ActiveIndices *>(transaction_.active_indices_->label_properties_.get());
  return active_indices->ChunkedVertices(
      label, properties, property_ranges, std::move(vertices_acc), view, storage_, &transaction_, num_chunks, order);
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(PropertyId property, View view,
                                                                           size_t num_chunks) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesChunkedIterable(active_indices->ChunkedVertices(
      property, std::move(vertex_acc), std::nullopt, std::nullopt, view, storage_, &transaction_, num_chunks));
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(PropertyId property,
                                                                           const PropertyValue &value, View view,
                                                                           size_t num_chunks) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesChunkedIterable(active_indices->ChunkedVertices(property,
                                                                 std::move(vertex_acc),
                                                                 utils::MakeBoundInclusive(value),
                                                                 utils::MakeBoundInclusive(value),
                                                                 view,
                                                                 storage_,
                                                                 &transaction_,
                                                                 num_chunks));
}

VerticesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedVertices(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, size_t num_chunks) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesChunkedIterable(active_indices->ChunkedVertices(
      property, std::move(vertex_acc), lower_bound, upper_bound, view, storage_, &transaction_, num_chunks));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(PropertyId property, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesIterable(active_indices->Vertices(
      property, std::move(vertex_acc), std::nullopt, std::nullopt, view, storage_, &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(PropertyId property, PropertyValue const &value,
                                                             View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesIterable(active_indices->Vertices(property,
                                                   std::move(vertex_acc),
                                                   utils::MakeBoundInclusive(value),
                                                   utils::MakeBoundInclusive(value),
                                                   view,
                                                   storage_,
                                                   &transaction_));
}

VerticesIterable InMemoryStorage::InMemoryAccessor::Vertices(
    PropertyId property, std::optional<utils::Bound<PropertyValue>> const &lower_bound,
    std::optional<utils::Bound<PropertyValue>> const &upper_bound, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryVertexPropertyIndex::ActiveIndices *>(transaction_.active_indices_->vertex_property_.get());
  return VerticesIterable(active_indices->Vertices(
      property, std::move(vertex_acc), lower_bound, upper_bound, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryEdgeTypeIndex::ActiveIndices *>(transaction_.active_indices_->edge_type_.get());
  return EdgesIterable(active_indices->Edges(edge_type, std::move(vertex_acc), view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_->edge_type_properties_.get());
  return EdgesIterable(active_indices->Edges(
      edge_type, property, std::move(vertex_acc), std::nullopt, std::nullopt, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property,
                                                       const PropertyValue &value, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_->edge_type_properties_.get());
  return EdgesIterable(active_indices->Edges(edge_type,
                                             property,
                                             std::move(vertex_acc),
                                             utils::MakeBoundInclusive(value),
                                             utils::MakeBoundInclusive(value),
                                             view,
                                             storage_,
                                             &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(EdgeTypeId edge_type, PropertyId property,
                                                       const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                       const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                       View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_->edge_type_properties_.get());
  return EdgesIterable(active_indices->Edges(
      edge_type, property, std::move(vertex_acc), lower_bound, upper_bound, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesIterable(mem_edge_property_active_indices->Edges(
      property, std::move(vertex_acc), std::nullopt, std::nullopt, view, storage_, &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property, const PropertyValue &value, View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesIterable(mem_edge_property_active_indices->Edges(property,
                                                               std::move(vertex_acc),
                                                               utils::MakeBoundInclusive(value),
                                                               utils::MakeBoundInclusive(value),
                                                               view,
                                                               storage_,
                                                               &transaction_));
}

EdgesIterable InMemoryStorage::InMemoryAccessor::Edges(PropertyId property,
                                                       const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                       const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                       View view) {
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *mem_edge_property_active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesIterable(mem_edge_property_active_indices->Edges(
      property, std::move(vertex_acc), lower_bound, upper_bound, view, storage_, &transaction_));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(EdgeTypeId edge_type, View view,
                                                                     size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryEdgeTypeIndex::ActiveIndices *>(transaction_.active_indices_->edge_type_.get());
  return EdgesChunkedIterable(
      active_indices->ChunkedEdges(edge_type, std::move(vertices_acc), view, storage_, &transaction_, num_chunks));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(EdgeTypeId edge_type, PropertyId property,
                                                                     View view, size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_->edge_type_properties_.get());
  return EdgesChunkedIterable(active_indices->ChunkedEdges(edge_type,
                                                           property,
                                                           std::move(vertices_acc),
                                                           std::nullopt,
                                                           std::nullopt,
                                                           view,
                                                           storage_,
                                                           &transaction_,
                                                           num_chunks));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(
    EdgeTypeId edge_type, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices = static_cast<InMemoryEdgeTypePropertyIndex::ActiveIndices *>(
      transaction_.active_indices_->edge_type_properties_.get());
  return EdgesChunkedIterable(active_indices->ChunkedEdges(edge_type,
                                                           property,
                                                           std::move(vertices_acc),
                                                           lower_bound,
                                                           upper_bound,
                                                           view,
                                                           storage_,
                                                           &transaction_,
                                                           num_chunks));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(PropertyId property, View view,
                                                                     size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesChunkedIterable(active_indices->ChunkedEdges(
      property, std::move(vertices_acc), std::nullopt, std::nullopt, view, storage_, &transaction_, num_chunks));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(PropertyId property, const PropertyValue &value,
                                                                     View view, size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesChunkedIterable(active_indices->ChunkedEdges(property,
                                                           std::move(vertices_acc),
                                                           utils::MakeBoundInclusive(value),
                                                           utils::MakeBoundInclusive(value),
                                                           view,
                                                           storage_,
                                                           &transaction_,
                                                           num_chunks));
}

EdgesChunkedIterable InMemoryStorage::InMemoryAccessor::ChunkedEdges(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, size_t num_chunks) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage_)->vertices_.access();
  auto *active_indices =
      static_cast<InMemoryEdgePropertyIndex::ActiveIndices *>(transaction_.active_indices_->edge_property_.get());
  return EdgesChunkedIterable(active_indices->ChunkedEdges(
      property, std::move(vertices_acc), lower_bound, upper_bound, view, storage_, &transaction_, num_chunks));
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
  uint64_t snapshot_ts = 0;
  CommitTsInfo commit_ts_info;
  std::optional<PointIndexContext> point_index_context;
  ActiveIndicesPtr active_indices;
  ActiveConstraintsPtr active_constraints;
  {
    auto guard = std::lock_guard{engine_lock_};
    transaction_id = transaction_id_++;
    start_timestamp = timestamp_++;
    // Capture the SI snapshot boundary under the same engine_lock as the mint so it is consistent with
    // start_timestamp. When the lock-free-read-snapshot experiment is ON, SI reads use this frozen
    // last-published-commit watermark (snapshot_ts < start_timestamp, since the watermark holds an
    // earlier commit ts). When OFF it equals start_timestamp (legacy semantics; SI reads still key off
    // start_timestamp in mvcc.hpp, so this is inert).
    snapshot_ts = config_.experimental_lockfree_read_snapshot ? last_committed_mvcc_ts_.load(std::memory_order_acquire)
                                                              : start_timestamp;
    // Publish this SI txn's frozen snapshot_ts into the GC visibility ring so GC can recover min(active snapshot_ts).
    // RC/RU do not freeze a snapshot_ts and must not hold the GC floor down; skip them.
    if (config_.experimental_lockfree_read_snapshot && isolation_level == IsolationLevel::SNAPSHOT_ISOLATION) {
      // Invalidate-first message passing (writers serialized under engine_lock_; the GC reader is lock-free):
      // publish the empty sentinel into tag BEFORE overwriting snap, so a concurrent GC read can never pair
      // this slot's NEW snap with the PREVIOUS owner's tag. If the reader's acquire-load of snap observes the
      // new value, it synchronizes-with the release store below and therefore also observes tag == sentinel
      // (or the final tag), never the stale predecessor start_ts. tag last, as the commit point.
      auto &gc_slot = snapshot_slots_[start_timestamp % kSnapshotSlots];
      gc_slot.tag.store(std::numeric_limits<uint64_t>::max(), std::memory_order_relaxed);  // invalidate old owner
      gc_slot.snap.store(snapshot_ts, std::memory_order_release);                          // carries the invalidation
      gc_slot.tag.store(start_timestamp, std::memory_order_release);                       // publish, tag last
    }
    // IMPORTANT: this is retrieved while under the lock so that the index is consistant with the timestamp
    point_index_context = indices_.point_index_.CreatePointIndexContext();
    // Needed by snapshot to sync the durable and logical ts. Load ldt and num_committed_txns from the same atomic
    // so a snapshot taken from this txn writes a mutually consistent pair (a concurrent commit can't inflate the
    // count relative to the durable ts and produce a negative replication lag on recovering replicas).
    commit_ts_info = repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    active_indices = GetActiveIndices();
    active_constraints = GetActiveConstraints();
  }

  auto async_index_helper = AsyncIndexHelper{config_, *active_indices, start_timestamp};

  DMG_ASSERT(point_index_context.has_value(), "Expected a value, even if got 0 point indexes");
  auto transaction = Transaction{transaction_id,
                                 start_timestamp,
                                 isolation_level,
                                 storage_mode,
                                 false,
                                 *std::move(point_index_context),
                                 std::move(active_indices),
                                 std::move(active_constraints),
                                 std::move(async_index_helper),
                                 commit_ts_info.ldt_,
                                 commit_ts_info.num_committed_txns_,
                                 metric_handles_.unreleased_delta_objects};
  transaction.snapshot_ts = snapshot_ts;
  transaction.lockfree_snapshot =
      config_.experimental_lockfree_read_snapshot && isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
  return transaction;
}

void InMemoryStorage::SetStorageMode(StorageMode new_storage_mode) {
  // Drain before UNIQUE: worker holds AsyncIndexer::mutex_ while waiting on
  // main_lock_, so draining under UNIQUE would deadlock.
  if (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL && storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    spdlog::info("SetStorageMode: draining async indexer before transition to IN_MEMORY_ANALYTICAL");
    async_indexer_.CompleteRemaining();
    spdlog::info("SetStorageMode: async indexer drained");
  }

  auto unique_accessor = UniqueAccess();
  MG_ASSERT(
      (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL || storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) &&
      (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL ||
       new_storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL));
  if (storage_mode_ != new_storage_mode) {
    // Snapshot thread is already running, but setup periodic execution only if enabled
    if (new_storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      auto active_constraints = GetActiveConstraints();
      // Constraints violation require deltas so we can abort. Hence in analytical can not support any constraint
      if (active_constraints && !active_constraints->empty()) {
        throw utils::BasicException(
            "Constraints are not supported in analytical storage mode. Please drop them before "
            "changing storage mode to analytical or use transactional mode.");
      }
      // Anything enqueued in the gap between drain and UNIQUE can't be drained here without deadlock.
      if (!async_indexer_.IsIdle()) {
        throw utils::BasicException(
            "Cannot switch to IN_MEMORY_ANALYTICAL: an async index creation task (from CREATE INDEX "
            "or ENABLE TTL) was enqueued concurrently with the storage mode change. Wait for pending "
            "index creation to finish and retry.");
      }
      // Analytical writes are never appended to the WAL, so a replica attached across the switch would
      // silently miss the whole episode. Refuse and store the mode under the clients' own lock, which is
      // the same lock replica registration inserts under: "analytical with a live client" is then
      // unrepresentable rather than merely a narrow race.
      repl_storage_state_.replication_storage_clients_.WithLock([this](auto const &clients) {
        if (!clients.empty()) {
          throw utils::BasicException(
              "Cannot switch to IN_MEMORY_ANALYTICAL while {} replica(s) replicate from this database. Analytical "
              "writes are not replicated; unregister every replica first.",
              clients.size());
        }
        storage_mode_ = StorageMode::IN_MEMORY_ANALYTICAL;
      });

      // Finalize the WAL so the episode leaves a file-level signature: the pre-import file's [from, to]
      // range then ends before the switch-back snapshot's timestamp, which is how GetRecoverySteps
      // detects that no WAL can reproduce the imported data. Placed after every check that throws, so a
      // rejected switch has no side effect, and under engine_lock_ because GetRecoverySteps holds that
      // lock specifically to read wal_file_. Lock order main_lock_ -> engine_lock_ is respected, since
      // the UNIQUE hold above is on main_lock_.
      {
        std::unique_lock const engine_guard(engine_lock_);
        if (wal_file_) {
          wal_file_->FinalizeWal();
          wal_file_.reset();
          wal_unsynced_transactions_ = 0;
        }
      }
      snapshot_runner_.Pause();
    } else {
      // No need to resume async indexer, it is always running.
      // As IN_MEMORY_TRANSACTIONAL we will now start giving it new work.
      // The txn's last_durable_ts_ was captured at unique-acc construction
      // from the pre-analytical-mode ldt; analytical-mode writes don't bump
      // ldt, so without this the snapshot's durable_timestamp would lag its
      // contents and consumers would skip it as "not newer".
      auto *txn = unique_accessor->GetTransaction();
      txn->last_durable_ts_ = txn->start_timestamp;
      const auto snapshot_path = durability::CreateSnapshot(this,
                                                            txn,
                                                            recovery_.snapshot_directory_,
                                                            recovery_.wal_directory_,
                                                            &vertices_,
                                                            &edges_,
                                                            uuid(),
                                                            repl_storage_state_.epoch_.id(),
                                                            repl_storage_state_.history,
                                                            &file_retainer_,
                                                            &abort_snapshot_,
                                                            &snapshot_progress_,
                                                            "storage_mode_change");
      if (!snapshot_path) {
        // Analytical writes never reached a WAL, so this snapshot is the only durable record the episode
        // will ever have. Completing the switch without it would leave the data live in memory but absent
        // from durability, and the next recovery would silently drop it. Stay analytical instead: the
        // mode, the cached ldt and every durability file are still untouched at this point, so the user
        // can fix the cause and retry. CreateSnapshot has already removed whatever partial file it wrote.
        throw utils::BasicException(
            "Failed to create the snapshot required to leave IN_MEMORY_ANALYTICAL. The database is still in "
            "analytical mode and its data is unchanged; check the logs for the cause and retry.");
      }

      // Publish the snapshot's timestamp as the cached ldt. Analytical writes never reach
      // FinalizeCommitPhase, so without this main keeps advertising the pre-analytical ldt and a replica
      // registered afterwards is judged up to date by the heartbeat comparison -- recovery would never
      // run and the imported data would never reach it. num_committed_txns_ is deliberately left alone:
      // the snapshot records the same unchanged counter, so main and a recovering replica stay
      // consistent. Advance-only, since concurrent commits are barred by the UNIQUE main_lock_ hold but
      // the field is shared with the replication clients.
      atomic_struct_update<CommitTsInfo>(repl_storage_state_.commit_ts_info_,
                                         [ldt = *txn->last_durable_ts_](CommitTsInfo const &old_info) {
                                           return CommitTsInfo{.ldt_ = std::max(old_info.ldt_, ldt),
                                                               .num_committed_txns_ = old_info.num_committed_txns_};
                                         });

      // The switch-back snapshot is a new durability base, not an increment on the old one: an analytical
      // episode leaves a timestamp hole no WAL can fill, so nothing written before it can be chained onto
      // it. Archive the superseded files and restart the WAL numbering at 0 -- the two go together.
      // Wiping alone would break recovery, since a chain whose first file has a non-zero sequence number
      // is accepted only when some WAL predates the snapshot, and every such WAL is what was just wiped.
      // Restarting alone would be worse: the surviving files carry this same UUID, so the new numbers
      // would collide with theirs, and a duplicate sequence number is caught by no check anywhere.
      DMG_ASSERT(!wal_file_, "Analytical mode must not leave an open WAL file.");
      if (ArchiveSupersededDurabilityFiles(*snapshot_path)) {
        wal_seq_num_ = 0;
      } else {
        spdlog::warn(
            "Superseded WAL files could not be archived, so WAL sequence numbering continues from {} to stay "
            "collision-free.",
            wal_seq_num_);
      }
      snapshot_runner_.Resume();
      // Under the clients' lock for symmetry with the analytical store above, so replica registration's
      // in-lock read of storage_mode_ is serialized against every mode change, not just one direction.
      repl_storage_state_.replication_storage_clients_.WithLock(
          [this](auto const & /*clients*/) { storage_mode_ = StorageMode::IN_MEMORY_TRANSACTIONAL; });
    }
    // Hand off the same lock unique_accessor already holds; adopting main_lock_ into a second
    // lock would give the one hold two owners and release it twice.
    FreeMemory(unique_accessor->ReleaseGuard(), false);
  }
}

uint64_t InMemoryStorage::GcVisibilityHorizon(uint64_t raw_oldest_active, bool no_active_txns) {
  if (!config_.experimental_lockfree_read_snapshot) return raw_oldest_active;  // OFF: byte-identical
  auto const &gc_slot = snapshot_slots_[raw_oldest_active % kSnapshotSlots];
  uint64_t const snap = gc_slot.snap.load(std::memory_order_acquire);
  uint64_t const tag = gc_slot.tag.load(std::memory_order_acquire);
  if (tag == raw_oldest_active) {
    // The oldest active txn owns this slot: min(active snapshot_ts) == its snapshot. Advance the floor.
    uint64_t cur = gc_visibility_floor_.load(std::memory_order_acquire);
    while (snap > cur && !gc_visibility_floor_.compare_exchange_weak(
                             cur, snap, std::memory_order_release, std::memory_order_acquire)) {
    }
    return std::max(snap, cur);
  }
  // Tag mismatch: raw_oldest_active does not name an active txn's slot. If there are NO active transactions
  // there is nothing to protect -> reclaim fully (same as OFF). Otherwise a real older reader's slot was
  // recycled or not yet written; fall back to the monotone floor (<= min active snapshot_ts).
  // no_active_txns is `raw_oldest_active >= timestamp_` (the next-to-mint id), computed by the caller under
  // engine_lock_. We gate on that, NOT on `raw > last_committed`, which would false-positive on a leapfrogged
  // reader whose slot recycled while its snapshot stayed low (a leapfrogged reader has raw < timestamp_).
  if (no_active_txns) return raw_oldest_active;
  return gc_visibility_floor_.load(std::memory_order_acquire);
}

void InMemoryStorage::CollectGarbage(utils::ResourceLockGuard main_guard, bool periodic) {
  // NOTE: A single call need not handle objects deleted under a different storage mode: SetStorageMode
  // runs GC before any transaction in the new mode can start.

  using Guard = utils::ResourceLockGuard;
  auto const main_lock_guard = [&] -> Guard {
    // Adopt SetStorageMode's UNIQUE hold if it passed one; reacquiring would deadlock.
    if (main_guard.owns_lock()) {
      // Adopted in place of choosing a mode below, so it has to be exclusive: analytical GC is not
      // proven safe under a shared hold (see the WRITE choice further down).
      DMG_ASSERT(main_guard.mutex() == std::addressof(main_lock_) && main_guard.is_exclusive(),
                 "an adopted main_guard must be an exclusive hold on this storage's main_lock_");
      return std::move(main_guard);
    }

    // Aggressive GC escalates to UNIQUE (blocks new txns); otherwise a shared hold, so slow GC does
    // not block everyone.
    if (flags::run_time::GetStorageGcAggressive()) {
      auto unique_guard = Guard{main_lock_, Guard::UNIQUE, std::try_to_lock};
      if (unique_guard.owns_lock()) return unique_guard;
    }

    // Transactional GC only reads the COW indices/constraints, so READ suffices and stays compatible
    // with concurrent READ_ONLY DDL; analytical needs WRITE (GC vs snapshot not proven safe under
    // READ). storage_mode_ must be read under a shared hold -- it blocks SetStorageMode's UNIQUE, so
    // an unlocked read would both data-race the write (TSan) and risk acting on a stale mode (TOCTOU).
    auto read_guard = Guard{main_lock_, Guard::READ};
    if (storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) return read_guard;

    // Analytical. Release READ before requesting WRITE: acquiring a second shared hold while still
    // holding one deadlocks against a UNIQUE acquirer that registers as pending in between, because
    // lock_guard_condition<WRITE> is gated on unique_pending_ and that acquirer is in turn waiting
    // on the READ we would still be holding.
    read_guard.unlock();
    auto write_guard = Guard{main_lock_, Guard::WRITE};
    // The mode can flip in the gap, so re-read it under the hold we will actually use. Downgrading
    // is non-blocking, unlike the escalation above.
    if (storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) write_guard.downgrade_to_read();
    return write_guard;
  }();

  // Only one gc run at a time
  auto gc_guard = std::unique_lock{gc_lock_, std::try_to_lock};
  if (!gc_guard.owns_lock()) {
    return;
  }

  // Publish run-state for SHOW TRANSACTIONS; cleared on scope exit (see GcProgress).
  gc_progress_.Start(periodic, main_lock_guard.is_exclusive());
  const utils::OnScopeExit gc_run_state_reset{[&] { gc_progress_.Reset(); }};

  // Diagnostic trace
  const utils::Timer timer;
  spdlog::trace("Storage GC on '{}' started [{}]", name(), periodic ? "periodic" : "forced");
  auto trace_on_exit = utils::OnScopeExit{[&] {
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(timer.Elapsed());
    metric_handles_.gc_latency_seconds.Observe(std::chrono::duration<double>(elapsed).count());
    spdlog::trace("Storage GC on '{}' finished [{}]. Duration: {:.3f}s",
                  name(),
                  periodic ? "periodic" : "forced",
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

  // EXPERIMENTAL (lock-free-read-snapshot): the visibility ring is keyed by the ACTUAL oldest active
  // start_timestamp, so capture it before the schema-info fold below lowers oldest_active_start_timestamp.
  uint64_t const raw_oldest_active = oldest_active_start_timestamp;

  // Also consider unprocessed schema updates as a safety horizon.
  // `pending_schema_updates_` contains raw pointers to vertices (in SchemaInfoEdge.from/.to
  // and SchemaInfoPostProcess.vertex_cache). We cannot delete these vertices until their
  // schema updates have been processed.
  if (config_.salient.items.enable_schema_info) {
    std::lock_guard<std::mutex> const lock{schema_queue_mutex_};
    if (!pending_schema_updates_.empty()) {
      // Establish the earliest reconstruction boundary still queued: each pending update's deferred
      // reconstruction walks version chains down to `ts < snapshot_bound`, so no delta at or after
      // that boundary may be unlinked yet. snapshot_bound <= start_timestamp, so this is at least as
      // conservative as (and correctly aligned with) the boundary the reconstruction actually uses.
      uint64_t min_queued_bound = std::numeric_limits<uint64_t>::max();
      for (const auto &[commit_ts, update_data] : pending_schema_updates_) {
        min_queued_bound = std::min(min_queued_bound, update_data.snapshot_bound);
      }
      oldest_active_start_timestamp = std::min(min_queued_bound, oldest_active_start_timestamp);
    }
  }

  // EXPERIMENTAL (lock-free-read-snapshot): idle == "no active transactions" iff the oldest active start
  // timestamp has reached the next-to-mint counter timestamp_ (every issued id is finished). Read timestamp_
  // under engine_lock_ (as the fast-discard checks below already do). A leapfrogged reader has
  // raw_oldest_active < timestamp_, so this correctly protects it (unlike a `raw > last_committed` test).
  bool no_active_txns = false;
  if (config_.experimental_lockfree_read_snapshot) {
    auto const engine_guard = std::scoped_lock{engine_lock_};
    no_active_txns = raw_oldest_active >= timestamp_;
  }

  // Key the snapshot-based horizon on the RAW oldest active, then clamp to the (possibly lower) physical
  // horizon so pending schema-update deltas are still protected. OFF: min(raw, folded) == folded (byte-identical).
  uint64_t const visibility_horizon =
      std::min(GcVisibilityHorizon(raw_oldest_active, no_active_txns), oldest_active_start_timestamp);

  // When a transaction commits with non-sequential deltas, its deltas may be mixed with
  // deltas from other transactions in the same delta chains. We cannot immediately unlink
  // these deltas because:
  // 1. Other "contributor" transactions may still be running
  // 2. We need to ensure all contributors have committed/aborted before unlinking
  // 3. We must use the highest commit timestamp among all contributors as the safe
  //    unlinking horizon to prevent visibility violations
  //
  // This waiting room holds committed transactions with non-sequential deltas until all
  // their "contributors" (other transactions sharing the same delta chains) have finished.
  auto local_waiting = std::list<GCDeltas, memory::DbAwareAllocator<GCDeltas>>{};
  waiting_gc_deltas_.WithLock([&](auto &waiting_list) { local_waiting.swap(waiting_list); });

  {
    auto it = local_waiting.begin();
    while (it != local_waiting.end()) {
      bool all_contributors_committed = true;
      auto const our_commit_ts = it->commit_info_->timestamp.load(std::memory_order_acquire);

      uint64_t highest_commit_ts = our_commit_ts;
      std::unordered_set<const Delta *> visited;

      for (const auto &delta : it->deltas_) {
        if (!IsDeltaNonSequential(delta)) continue;
        if (visited.contains(&delta)) continue;

        auto *current = &delta;
        while (current != nullptr && !visited.contains(current)) {
          // early exit if we find a sequential delta
          if (!IsDeltaNonSequential(*current)) break;
          visited.insert(current);
          auto ts = current->commit_info->timestamp.load();

          if (ts >= kTransactionInitialId) {
            // Found an uncommitted NonSeq, we can't unlink these deltas
            all_contributors_committed = false;
            break;
          }

          // Track highest commit timestamp among all contributors. We can only
          // unlink when ALL contributors are inactive, so we must wait until
          // highest_commit_ts < visibility_horizon (the reclaim gate this feeds at the
          // `unlinkable_timestamp >= visibility_horizon` check below). Under the lock-free-read-snapshot
          // flag visibility_horizon is min(snapshot-based horizon, oldest_active_start_timestamp), so it
          // is at or below the old oldest_active_start_timestamp bound; OFF the two coincide.
          if (ts > highest_commit_ts) {
            highest_commit_ts = ts;
          }
          current = current->next.load(std::memory_order_acquire);
        }
        if (!all_contributors_committed) break;
      }

      // All contributors finished - safe to move to normal GC processing
      if (all_contributors_committed) {
        it->unlinkable_timestamp_ = highest_commit_ts;
        committed_transactions_.WithLock(
            [&](auto &committed_transactions) { committed_transactions.emplace_back(std::move(*it)); });
        it = local_waiting.erase(it);
      } else {
        ++it;
      }
    }
  }

  if (!local_waiting.empty()) {
    waiting_gc_deltas_.WithLock(
        [&](auto &waiting_list) { waiting_list.splice(waiting_list.begin(), std::move(local_waiting)); });
  }

  {
    auto guard = std::unique_lock{engine_lock_};
    uint64_t mark_timestamp = timestamp_;  // a timestamp no active transaction can currently have

    // Deltas from previous GC runs or from aborts can be cleaned up here
    garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
      guard.unlock();
      if (main_lock_guard.is_exclusive() || mark_timestamp == oldest_active_start_timestamp) {
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
  std::list<GCDeltas, memory::DbAwareAllocator<GCDeltas>> unlinked_undo_buffers{};

  // We will only free vertices deleted up until now in this GC cycle, and we
  // will do it after cleaning-up the indices. That way we are sure that all
  // vertices that appear in an index also exist in main storage.
  BatchedList<Edge *> current_deleted_edges{};
  BatchedList<Gid> current_deleted_vertices{};

  deleted_vertices_.WithLock([&](auto &deleted_vertices) { current_deleted_vertices.swap(deleted_vertices); });
  deleted_edges_.WithLock([&](auto &deleted_edges) { current_deleted_edges.swap(deleted_edges); });

  auto const need_full_scan_vertices = gc_full_scan_vertices_delete_.exchange(false, std::memory_order_acq_rel);
  auto const need_full_scan_edges = gc_full_scan_edges_delete_.exchange(false, std::memory_order_acq_rel);

  // Short lock, to move to local variable. Hence, allows other transactions to commit.
  auto linked_undo_buffers = std::list<GCDeltas, memory::DbAwareAllocator<GCDeltas>>{};
  committed_transactions_.WithLock(
      [&](auto &committed_transactions) { committed_transactions.swap(linked_undo_buffers); });

  // This is to track if any of the unlinked deltas would have an impact on index performance, i.e. do they hint that
  // there are possible stale/duplicate entries that can be removed
  auto &cycle_arming = cycle_index_arming_;
  cycle_arming.reset();

  auto const end_linked_undo_buffers = linked_undo_buffers.end();
  for (auto linked_entry = linked_undo_buffers.begin(); linked_entry != end_linked_undo_buffers;) {
    auto const *const commit_info_ptr = linked_entry->commit_info_.get();

    // Use unlinkable_timestamp to determine if safe to unlink
    auto const unlinkable_timestamp = linked_entry->unlinkable_timestamp_;

    // only process those that are no longer active
    if (unlinkable_timestamp >= visibility_horizon) {
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

    auto const arming_scope = cycle_arming.for_deltas_of(linked_entry->wrote_properties_on_);

    for (Delta &delta : linked_entry->deltas_) {
      arming_scope.note(delta);
      while (true) {
        auto prev = delta.prev.Get();
        switch (prev.type) {
          case PreviousPtr::Type::VERTEX: {
            Vertex *vertex = prev.vertex;
            auto vertex_guard = std::unique_lock{vertex->lock};
            if (vertex->delta() != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            vertex->SetDelta(nullptr);
            vertex->set_has_uncommitted_non_sequential_deltas(false);

            if (vertex->deleted()) {
              DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
              current_deleted_vertices.push_back(vertex->gid);
            }
            break;
          }
          case PreviousPtr::Type::EDGE: {
            Edge *edge = prev.edge;
            auto edge_guard = std::unique_lock{edge->lock};
            if (edge->delta() != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            edge->SetDelta(nullptr);
            if (edge->deleted()) {
              DMG_ASSERT(delta.action == Delta::Action::RECREATE_OBJECT);
              current_deleted_edges.push_back(edge);
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
            // EXPERIMENTAL (lock-free-read-snapshot): when the flag is ON the boundary used just below is
            // visibility_horizon (= min active snapshot_ts), which sits at or before this start-ts boundary.

            if (prev.delta->commit_info == commit_info_ptr) {
              // The delta that is newer than this one is also a delta from this
              // transaction. We skip the current delta and will remove it as a
              // part of the suffix later.
              break;
            }

            if (prev.delta->commit_info->timestamp.load() < visibility_horizon) {
              if (IsDeltaNonSequential(*prev.delta)) {
                // Non-sequential predecessor: readers follow next, so we must
                // null it to stop traversal into freed memory. We can skip the
                // lock because we know we are the only potential modifiers,
                // since:
                // - the predecessor delta is inactive
                // - prepends only happen at the chain head
                // - the GC is serialized via gc_lock_.
                // Safe for concurrent readers: all deltas beyond this point are
                // also inactive (guaranteed by waiting_gc_deltas_), so no
                // active transaction needs to read past here. Under the flag the
                // horizon is visibility_horizon (min active snapshot_ts), not a
                // start-ts, so "no active transaction needs to read past here"
                // holds against snapshot-based visibility as well.
                prev.delta->next.store(nullptr, std::memory_order_release);
              }
              break;
            }

            // Previous is active (committed or uncommitted). We need to find
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
                case PreviousPtr::Type::NULL_PTR:
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
          case PreviousPtr::Type::NULL_PTR: {
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
  auto &sweep_arming = claimed_index_arming_;
  sweep_arming.reset();
  pending_index_arming_.WithLock([&](IndexArming &pending) { std::swap(pending, sweep_arming); });
  sweep_arming |= cycle_arming;
  if (index_cleanup_vertex_needed) sweep_arming.arm_all_vertex_indexes();
  if (index_cleanup_edge_needed) sweep_arming.arm_all_edge_indexes();

  auto index_cleanup_vertex_performance = sweep_arming.arms_vertex_indexes();
  auto index_cleanup_edge_performance = sweep_arming.arms_edge_indexes();

  // After unlinking deltas from vertices, we refresh the indices. That way
  // we're sure that none of the vertices from `current_deleted_vertices`
  // appears in an index, and we can safely remove the from the main storage
  // after the last currently active transaction is finished.
  // This operation is very expensive as it traverses through all of the items
  // in every index every time.
  // Analytical deletes land in place, so what counts as collectable keeps growing while this pass
  // runs. Fix the set here, before the sweeps: an object deleted after a sweep has walked past its
  // index entries would otherwise be removed from storage by this same pass with those entries left
  // behind, naming freed memory. Anything deleted after this point waits for the next pass, which is
  // what the transactional path does by working from a list swapped out up front.
  auto analytical_deleted_vertices = std::vector<Vertex *>{};
  auto analytical_deleted_edges = std::vector<Edge *>{};
  // Reads under the object's lock: `deleted` shares its word with the delta pointer, and a
  // concurrent analytical delete writes both. Deciding collectability from an unsynchronised read
  // is a data race, and the answer decides whether this pass removes the object from storage.
  auto const is_collectable = [](auto const &object) {
    auto guard = std::shared_lock{object.lock};
    return object.delta() == nullptr && object.deleted();
  };

  // An object handed over by a transaction is collectable, so this scan would find it again. Both
  // sets are retired by this pass, and retiring an object twice removes it twice, so the scan takes
  // only what the handover has not already claimed. `current_deleted_*` is complete by now: the
  // swap and the delta unlinking above are the only things that add to it.
  auto const claimed_vertices = ClaimedObjects{current_deleted_vertices.elements()};
  auto const claimed_edges = ClaimedObjects{current_deleted_edges.elements()};

  if (need_full_scan_vertices) {
    auto vertex_acc = vertices_.access();
    for (auto &vertex : vertex_acc) {
      if (!claimed_vertices.contains(vertex.gid) && is_collectable(vertex)) {
        analytical_deleted_vertices.push_back(&vertex);
      }
    }
  }
  // Light edges are not scanned for. A deleted edge is erased from the adjacency of both its
  // endpoints as it is deleted, and adjacency is the only thing that names a light edge, so a
  // scan can no longer reach one. The delete hands them to `deleted_edges_` instead, which is why
  // they arrive here in `current_deleted_edges` like transactional ones.
  if (need_full_scan_edges && !config_.salient.items.storage_light_edge) {
    auto edge_acc = edges_.access();
    for (auto &edge : edge_acc) {
      if (!claimed_edges.contains(&edge) && is_collectable(edge)) analytical_deleted_edges.push_back(&edge);
    }
  }

  gc_progress_.SetPhase(GcPhase::INDEX_CLEANUP);
  if (auto token = stop_source.get_token(); !token.stop_requested()) {
    uint64_t swept = 0;
    if (index_cleanup_vertex_needed || index_cleanup_vertex_performance) {
      swept += indices_.RemoveObsoleteVertexEntries(this, visibility_horizon, token, sweep_arming);
      auto *mem_unique_constraints = static_cast<InMemoryUniqueConstraints *>(constraints_.unique_constraints_.get());
      swept += mem_unique_constraints->RemoveObsoleteEntries(this, visibility_horizon, token, sweep_arming);
    }
    if (index_cleanup_edge_needed || index_cleanup_edge_performance) {
      swept += indices_.RemoveObsoleteEdgeEntries(this, visibility_horizon, token, sweep_arming);
    }
    metric_handles_.gc_index_sweeps.Increment(static_cast<double>(swept));
  }
  {
    auto skiplist_elapsed = std::chrono::duration<double>(skiplist_cleanup_timer.Elapsed());
    metric_handles_.gc_skiplist_cleanup_latency_seconds.Observe(skiplist_elapsed.count());
  }

  gc_progress_.SetPhase(GcPhase::DELETE);

  {
    auto guard = std::unique_lock{engine_lock_};
    uint64_t mark_timestamp = timestamp_;  // a timestamp no active transaction can currently have

    if (main_lock_guard.is_exclusive() || mark_timestamp == oldest_active_start_timestamp) {
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
  // current_deleted_edges holds Edge* still owned by `edges_`; this single-threaded GC pass
  // frees them only in the remove loop below (MG_ASSERT'd), so reading ->gid here is safe.
  if (!current_deleted_edges.empty()) {
    if (auto &idx = edges_metadata_index_) {
      idx->OnEdgesDeleted(current_deleted_edges.elements() | std::ranges::views::transform(&Edge::gid));
    }
  }

  // VERTICES (has ptr to Edges, must be before removing edges)
  if (!current_deleted_vertices.empty()) {
    if (!indices_.vector_index_.Empty()) {
      auto vertex_acc = vertices_.access();
      auto const vertices_to_remove = current_deleted_vertices.elements() |
                                      std::ranges::views::transform([&vertex_acc](auto const gid) {
                                        auto it = vertex_acc.find(gid);
                                        DMG_ASSERT(it != vertex_acc.end(), "Invalid database state!");
                                        return &*it;
                                      }) |
                                      std::ranges::to<std::vector>();

      indices_.RemoveVerticesFromVectorIndices(vertices_to_remove);
    }

    // Remove edges from vector edge index BEFORE vertex skip-list removal.
    // edge_endpoints_ stores Vertex* — freeing vertices first would leave dangling pointers.
    if (!current_deleted_edges.empty() && !indices_.vector_edge_index_.Empty()) {
      // RemoveEdgesFromVectorEdgeIndices takes a contiguous std::span; current_deleted_edges
      // is a std::list (kept so the under-lock handover stays an O(1) splice), so materialize
      // a temp vector here. This runs off the deleted_edges_ lock, during GC.
      auto const edges_to_remove = current_deleted_edges.elements() | std::ranges::to<std::vector>();
      indices_.RemoveEdgesFromVectorEdgeIndices(edges_to_remove);
    }

    RetireVertices(current_deleted_vertices.elements());
  }

  // EDGES / LIGHT EDGES
  if (!current_deleted_edges.empty()) {
    if (config_.salient.items.storage_light_edge) {
      // Light edges are not skiplist nodes; route them to the graveyard for
      // deferred free. Clean the vector edge index first (unconditionally —
      // unlike the heavy arm below which only does so when no vertices were
      // deleted — because the light Edge* and their Vertex* outlive the push).
      if (!indices_.vector_edge_index_.Empty()) {
        // std::list -> contiguous temp vector for the std::span API (off-lock).
        auto const edges_to_remove = current_deleted_edges.elements() | std::ranges::to<std::vector>();
        indices_.RemoveEdgesFromVectorEdgeIndices(edges_to_remove);
      }
      RetireLightEdges(std::move(current_deleted_edges));
    } else {
      if (current_deleted_vertices.empty() && !indices_.vector_edge_index_.Empty()) {
        // std::list -> contiguous temp vector for the std::span API (off-lock; see above).
        auto const edges_to_remove = current_deleted_edges.elements() | std::ranges::to<std::vector>();
        indices_.RemoveEdgesFromVectorEdgeIndices(edges_to_remove);
      }

      RetireEdges(current_deleted_edges.elements() | std::ranges::views::transform(&Edge::gid));
    }
  }

  // EXPENSIVE full scan, is only run if an IN_MEMORY_ANALYTICAL transaction involved any deletions
  // TODO: implement a fast internal iteration inside the skip_list (to avoid unnecessary find_node calls),
  //  accessor.remove_if([](auto const & item){ return item.delta == nullptr && item.deleted;});
  //  alternatively, an auxiliary data structure within skip_list to track these, hence a full scan wouldn't be needed
  //  we will wait for evidence that this is needed before doing so.
  if (need_full_scan_vertices) {
    if (!indices_.vector_index_.Empty() && !analytical_deleted_vertices.empty()) {
      indices_.RemoveVerticesFromVectorIndices(analytical_deleted_vertices);
    }

    // Remove edges from vector edge index BEFORE vertex skip-list removal.
    if (!indices_.vector_edge_index_.Empty() && !analytical_deleted_edges.empty()) {
      indices_.RemoveEdgesFromVectorEdgeIndices(analytical_deleted_edges);
    }

    RetireVertices(analytical_deleted_vertices | std::ranges::views::transform(&Vertex::gid));
  }

  // EXPENSIVE full scan, is only run if an IN_MEMORY_ANALYTICAL transaction involved any deletions
  if (need_full_scan_edges) {
    if (!need_full_scan_vertices && !indices_.vector_edge_index_.Empty() && !analytical_deleted_edges.empty()) {
      indices_.RemoveEdgesFromVectorEdgeIndices(analytical_deleted_edges);
    }

    if (auto &idx = edges_metadata_index_) {
      for (auto *edge : analytical_deleted_edges) idx->OnEdgeDeleted(edge->gid);
    }
    RetireEdges(analytical_deleted_edges | std::ranges::views::transform(&Edge::gid));
  }
}

void InMemoryStorage::DrainLightEdgeGraveyard() {
  if (!config_.salient.items.storage_light_edge) return;
  // Light edges may be pinned by edge-index iterables (MakeEdgePin now returns a
  // LightEdgeIterableGuard that Acquires an epoch). A graveyard entry recorded
  // guard_epoch = CurrentEpoch() at delete time; we may only free its edges once
  // IsSafeToFree(guard_epoch) confirms every reader that existed before the delete
  // has Released. Swap the graveyard out under the lock, free the safe entries
  // lock-free, and splice the not-yet-safe survivors back for a later drain.
  //
  // Pushes happen ONLY at GC-collection time (CollectGarbage transactional arm and
  // the analytical arm) — commit (FastDiscard) and abort route deleted Edge*
  // through deleted_edges_ first, so guard_epoch is snapped AFTER index cleanup and
  // after all pre-existing readers are ordered.
  std::list<LightEdgeGraveyardEntry, memory::DbAwareAllocator<LightEdgeGraveyardEntry>> local_graveyard;
  light_edge_graveyard_.WithLock([&](auto &graveyard) { local_graveyard.swap(graveyard); });
  if (local_graveyard.empty()) return;

  // GC-collection time (CollectGarbage transactional arm OnEdgesDeleted, and the
  // analytical arm OnEdgeDeleted) BEFORE the Edge* was routed here. The graveyard
  // exists solely to defer the *memory free* until live edge-index iterables have
  // drained; it must NOT touch edges_metadata_index_ again, otherwise it would
  // double-remove the entry and trip the `acc.remove` assert in OnEdgeDeleted.
  // Snapshot the dead-prefix watermark once for the whole sweep: the dead prefix
  // is monotonic, so a single O(blocks) scan plus an O(1) check per entry beats
  // re-scanning the tracker for every entry. An entry not yet freeable under this
  // snapshot is simply spliced back and retried on the next drain, never freed early.
  const uint64_t watermark = light_edge_iterable_tracker_.DeadPrefixWatermark();
  for (auto it = local_graveyard.begin(); it != local_graveyard.end();) {
    if (utils::EpochTracker::IsSafeToFree(it->guard_epoch, watermark)) {
      for (auto *edge : it->edges.elements()) {
        InMemoryStorage::LightEdgePool::Destroy(edge);
      }
      it = local_graveyard.erase(it);
    } else {
      ++it;
    }
  }
  // Splice the not-yet-safe survivors back for a future drain (skip the lock
  // acquisition entirely when all entries were freed this pass).
  if (!local_graveyard.empty()) {
    light_edge_graveyard_.WithLock([&](auto &graveyard) { graveyard.splice(graveyard.end(), local_graveyard); });
  }
}

StorageInfo InMemoryStorage::GetBaseInfo() {
  StorageInfo info{};
  info.vertex_count = vertices_.size();
  info.edge_count = edge_count_.load(std::memory_order_acquire);
  if (info.vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    info.average_degree = 2.0 * static_cast<double>(info.edge_count) / info.vertex_count;
  }
  info.memory_res = utils::GetMemoryRES();
  info.peak_memory_res = metrics::Metrics().UpdateAndGetPeakMemoryRes(info.memory_res);
  info.unreleased_delta_objects = static_cast<uint64_t>(metric_handles_.unreleased_delta_objects.Value());

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
    auto access = Access(StorageAccessType::READ);  // TODO: override isolation level?
    const auto &lbl = access->ListAllIndices();
    info.label_indices = lbl.label.size();
    info.label_property_indices = lbl.label_properties.size();
    info.text_indices = lbl.text_indices.size();
    info.vector_indices = lbl.vector_indices_spec.size();
    const auto &con = access->ListAllConstraints();
    info.existence_constraints = con.existence.size();
    info.unique_constraints = con.unique.size();
    info.type_constraints = con.type.size();
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

bool InMemoryStorage::InitializeWalFile(std::string_view const epoch_id) {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL ||
      storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL) {
    return false;
  }

  if (!wal_file_) {
    wal_file_ = memory::MakeDbAwareUnique<durability::WalFile>(recovery_.wal_directory_,
                                                               uuid(),
                                                               epoch_id,
                                                               config_.salient.items,
                                                               name_id_mapper_.get(),
                                                               wal_seq_num_++,
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

bool InMemoryStorage::ArchiveSupersededDurabilityFiles(std::filesystem::path const &keep_snapshot) {
  auto const use_old_dir = FLAGS_storage_backup_dir_enabled;

  // A leftover .old from an earlier archival describes a state even older than the one being archived
  // now, so it is dropped rather than merged; keeping both would let the directory grow without bound.
  auto const prepare_old_dir = [](std::filesystem::path const &parent) -> bool {
    auto const target = parent / kOldDurabilityDir;
    std::error_code ec;
    std::filesystem::remove_all(target, ec);
    if (ec) {
      spdlog::warn("Failed to clear backup directory {}. Err: {}", target, ec.message());
      return false;
    }
    std::filesystem::create_directory(target, ec);
    if (ec) {
      spdlog::warn("Failed to create backup directory {}. Err: {}", target, ec.message());
      return false;
    }
    return true;
  };

  // Archival is best-effort: it is housekeeping, not part of making the new snapshot durable. A failure
  // degrades to "superseded files stay where they are", which the return value reports.
  auto const archive_dir = [&](std::filesystem::path const &dir, std::filesystem::path const *keep) {
    if (!utils::DirExists(dir)) return;  // durability off entirely; nothing was ever written here

    // With backups enabled, a backup directory that cannot be created means the files stay where they
    // are. Falling back to deleting them would turn a filesystem hiccup into unrecoverable data loss.
    std::optional<std::filesystem::path> backup_dir;
    if (use_old_dir) {
      if (!prepare_old_dir(dir)) return;
      backup_dir = dir / kOldDurabilityDir;
    }

    for (auto const &path : utils::GetFilesFromDir(dir)) {  // already skips the .old sub-directory
      if (keep && path.filename() == keep->filename()) continue;
      if (!backup_dir) {
        file_retainer_.DeleteFile(path);
        continue;
      }
      auto const new_path = *backup_dir / path.filename();
      spdlog::trace("Archiving durability file {} to {}", path, new_path);
      file_retainer_.RenameFile(path, new_path);
    }

    if (backup_dir) {
      std::error_code ec;
      std::filesystem::remove(*backup_dir, ec);  // no-op unless nothing needed archiving
    }
  };

  archive_dir(recovery_.snapshot_directory_, &keep_snapshot);
  archive_dir(recovery_.wal_directory_, nullptr);

  // Deletions and renames are deferred while a file is locked (SHOW SNAPSHOTS, a replica recovery), so
  // the directory is re-read rather than assumed empty.
  return !utils::DirExists(recovery_.wal_directory_) || utils::GetFilesFromDir(recovery_.wal_directory_).empty();
}

namespace {

// One MVCC delta resolved by the commit thread's traversal: the delta, the object it belongs to
// (exactly one of vertex/edge set), and the edge lookup data workers cannot pull from the transaction.
struct TxnDataCommand {
  Delta const *delta;
  Vertex *vertex;
  Edge *edge;
  Gid in_vertex_gid;
  EdgeTypeId edge_type_id;
};

// Everything a transaction writes, in encode order. Built once on the commit thread; the WAL worker
// and every replica worker encode from it concurrently, so it must outlive all of their tasks.
struct TxnCommands {
  std::vector<MetadataDelta const *> metadata;
  std::vector<TxnDataCommand> data;
};

void EncodeMetadataDelta(durability::BaseEncoder &encoder, MetadataDelta const &md_delta, Storage *mem_storage,
                         uint64_t durability_commit_timestamp) {
  auto const apply_encode = [&](durability::StorageMetadataOperation const op, auto &&encode_operation) {
    EncodeOperationPreamble(encoder, op, durability_commit_timestamp);
    encode_operation(encoder);
  };

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
        EncodeLabelPropertyStats(encoder,
                                 *mem_storage->name_id_mapper_,
                                 md_delta.label_property_stats.label,
                                 md_delta.label_property_stats.properties,
                                 md_delta.label_property_stats.stats);
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
        EncodeEdgeTypePropertyIndex(encoder,
                                    *mem_storage->name_id_mapper_,
                                    md_delta.edge_type_property.edge_type,
                                    md_delta.edge_type_property.property);
      });
      break;
    }
    case MetadataDelta::Action::GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
    case MetadataDelta::Action::GLOBAL_EDGE_PROPERTY_INDEX_DROP: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodePropertyIndex(encoder, *mem_storage->name_id_mapper_, md_delta.edge_property.property);
      });
      break;
    }
    case MetadataDelta::Action::GLOBAL_VERTEX_PROPERTY_INDEX_CREATE:
    case MetadataDelta::Action::GLOBAL_VERTEX_PROPERTY_INDEX_DROP: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodePropertyIndex(encoder, *mem_storage->name_id_mapper_, md_delta.vertex_property.property);
      });
      break;
    }
    case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_CREATE:
    case MetadataDelta::Action::LABEL_PROPERTIES_INDEX_DROP: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodeLabelProperties(encoder,
                              *mem_storage->name_id_mapper_,
                              md_delta.label_ordered_properties.label,
                              md_delta.label_ordered_properties.properties);
        encoder.WriteUint(static_cast<uint64_t>(md_delta.label_ordered_properties.order));
      });
      break;
    }
    case MetadataDelta::Action::EXISTENCE_CONSTRAINT_CREATE:
    case MetadataDelta::Action::EXISTENCE_CONSTRAINT_DROP:
    case MetadataDelta::Action::POINT_INDEX_CREATE:
    case MetadataDelta::Action::POINT_INDEX_DROP: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodeLabelProperty(
            encoder, *mem_storage->name_id_mapper_, md_delta.label_property.label, md_delta.label_property.property);
      });
      break;
    }
    case MetadataDelta::Action::LABEL_INDEX_STATS_SET: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodeLabelStats(
            encoder, *mem_storage->name_id_mapper_, md_delta.label_stats.label, md_delta.label_stats.stats);
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
        EncodeLabelProperties(encoder,
                              *mem_storage->name_id_mapper_,
                              md_delta.label_unordered_properties.label,
                              md_delta.label_unordered_properties.properties);
      });
      break;
    }
    case MetadataDelta::Action::TYPE_CONSTRAINT_CREATE:
    case MetadataDelta::Action::TYPE_CONSTRAINT_DROP: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        EncodeTypeConstraint(encoder,
                             *mem_storage->name_id_mapper_,
                             md_delta.label_property_type.label,
                             md_delta.label_property_type.property,
                             md_delta.label_property_type.type);
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
        EncodeEnumAlterUpdate(encoder,
                              mem_storage->enum_store_,
                              md_delta.enum_alter_update_info.value,
                              md_delta.enum_alter_update_info.old_value);
      });
      break;
    }
    case MetadataDelta::Action::TTL_OPERATION: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        durability::EncodeTtlOperation(encoder,
                                       md_delta.ttl_operation_info.operation_type,
                                       md_delta.ttl_operation_info.period,
                                       md_delta.ttl_operation_info.start_time,
                                       md_delta.ttl_operation_info.should_run_edge_ttl);
      });
      break;
    }
    case MetadataDelta::Action::DESCRIPTION_SET: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        durability::EncodeDescriptionSet(encoder,
                                         *mem_storage->name_id_mapper_,
                                         md_delta.description_op.kind,
                                         md_delta.description_op.labels,
                                         md_delta.description_op.edge_type,
                                         md_delta.description_op.property,
                                         md_delta.description_op.description,
                                         md_delta.description_op.from_labels,
                                         md_delta.description_op.to_labels,
                                         md_delta.description_op.value);
      });
      break;
    }
    case MetadataDelta::Action::DESCRIPTION_DELETE: {
      apply_encode(op, [&](durability::BaseEncoder &encoder) {
        durability::EncodeDescriptionDelete(encoder,
                                            *mem_storage->name_id_mapper_,
                                            md_delta.description_op.kind,
                                            md_delta.description_op.labels,
                                            md_delta.description_op.edge_type,
                                            md_delta.description_op.property,
                                            md_delta.description_op.from_labels,
                                            md_delta.description_op.to_labels,
                                            md_delta.description_op.value);
      });
      break;
    }
  }
}

}  // namespace

auto InMemoryStorage::InMemoryAccessor::HandleDurabilityAndReplicate(uint64_t durability_commit_timestamp,
                                                                     TransactionReplication &replicating_txn,
                                                                     CommitArgs const &commit_args) -> bool {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);

  // If replica executes this:
  //   STRICT_SYNC: commit_immediately is false because such replica needs to commit only after receiving
  //   FinalizeCommitRpc SYNC/ASYNC:  commit_immediately is true because such replica needs to commit immediately
  // If main executes this:
  //   Any STRICT_SYNC replica registered -> need to run 2PC, don't commit immediately
  // else:
  //   All SYNC/ASYNC replicas -> commit immediately
  bool const two_phase_commit = commit_args.two_phase_commit(replicating_txn);
  // The WAL file needs to be updated only if we don't commit immediately.
  needs_wal_update_ = two_phase_commit;

  // IMPORTANT: In most transactions there can only be one, either data or metadata deltas.
  //            But since we introduced auto index creation, a data transaction can also introduce a metadata delta.
  //            For correctness on the REPLICA side we need to send the metadata deltas first in order to acquire a
  //            unique transaction to apply the index creation safely.
  TxnCommands commands;
  commands.metadata.reserve(transaction_.md_deltas.size());
  for (auto const &md_delta : transaction_.md_deltas) {
    commands.metadata.push_back(&md_delta);
  }

  // A single transaction will always be fully-contained in a single WAL file.
  auto current_commit_timestamp = transaction_.commit_info->timestamp.load(std::memory_order_acquire);
  DeltaVertexCache vertex_cache(current_commit_timestamp);

  auto append_deltas = [&](auto callback) {
    // Helper lambda that traverses the delta chain to find the first delta
    // that should be processed and then applies the callback to all matching deltas.
    //
    // Template parameter TrackProcessedTails:
    //   - false: Simple traversal, no tracking (used for most passes)
    //   - true:  Track processed subchain tails to handle concurrent abort
    //            rewiring during pass 2 (edge creation)
    //
    // When TrackProcessedTails is true, concurrent aborts can rewire chains
    // while we traverse. If we hit a previously processed tail marker, it means
    // two subchains have merged because aborted deltas were snipped out.
    // Because aborting changes both `next` and `prev` pointers on the ends of
    // the two subchains, and this (as a compound operation) is *not* atomic, we
    // can possibly see additional deltas when following `prev` that are not
    // visible in the `next` pass. For example:
    // (vertex)->[A:Tx1]->B[:Tx1]-[C:Tx2]->[D:Tx1]->[E:Tx1]
    // If Tx2 aborts and sets B.next to D, there is a small window where:
    // B.next = D
    // D.prev = C
    // In this cases, following the `next` pointers would read A, B, D, E, and
    // the `prev` pointers would read `E`, D`, `C`, `B`, `A`.
    std::unordered_set<Delta const *> processed_subchain_heads;
    std::unordered_set<Delta const *> processed_subchain_tails;
    bool const should_track_nonseq_subchains{transaction_.has_non_sequential_deltas};
    constexpr auto kNoTracking = std::bool_constant<false>{};
    constexpr auto kTrackTails = std::bool_constant<true>{};
    auto find_and_apply_deltas =
        [&]<bool TrackProcessedTails>(
            std::bool_constant<TrackProcessedTails>, Delta const *head, auto *parent, auto filter) {
          CommitInfo const *const current_commit_info = head->commit_info;
          if constexpr (TrackProcessedTails) {
            processed_subchain_heads.insert(head);
          }
          auto const *current = head;
          while (true) {
            auto *older = current->next.load(std::memory_order_acquire);
            if (older == nullptr) break;
            if (older->commit_info != current_commit_info) break;
            if constexpr (TrackProcessedTails) {
              if (processed_subchain_heads.contains(older)) break;
            }
            current = older;
          }
          if constexpr (TrackProcessedTails) {
            // Concurrent aborts set `next` before `prev`. A delta may appear
            // as an entry point (its `prev` points to an aborted delta) even
            // though we already processed it via a rewired `next` path. If
            // this tail was already recorded, skip to avoid reprocessing.
            auto const [_, inserted] = processed_subchain_tails.insert(current);
            if (!inserted) return;
          }

          while (true) {
            if (current->commit_info == current_commit_info && filter(current->action)) {
              callback(*current, parent, durability_commit_timestamp);
            }
            if (current == head) break;

            auto prev = current->prev.Get();
            MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");
            if (prev.type != PreviousPtr::Type::DELTA) break;

            current = prev.delta;
          }
        };

    // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
    // don't traverse them in that order. That is because for each delta we need
    // information about the vertex or edge they belong to and that information
    // isn't stored in the deltas themselves. In order to find out information
    // about the corresponding vertex or edge it is necessary to traverse the
    // delta chain for each delta until a vertex or edge is encountered. This
    // operation is very expensive as the chain grows.
    // Instead, we traverse the deltas until we find a vertex or edge and traverse
    // their delta chains. This approach has a drawback because we lose the
    // correct order of the operations. Because of that, we need to traverse the
    // deltas several times and we have to manually ensure that the stored deltas
    // will be ordered correctly. The exception is if we have non-sequential
    // deltas. We can detect first (upstream) delta in an non-sequential delta
    // subchain by checking if the `prev` delta is from another transaction. If
    // this is the case, we can process the subchain as normal, but we *do*
    // need to look upstream to find the vertex at the head. To optimise this,
    // we use a `DeltaVertexCache` on subchain heads.

    // 1. Process all Vertex deltas and store all operations that create vertices
    // and modify vertex data.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");

      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(kNoTracking, &delta, prev.vertex, [](auto action) {
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
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 2. Process all Vertex deltas and store all operations that create edges.
    // Because this phase handles `REMOVE_OUT_EDGE`, we must accommodate
    // non-sequential chunks while abort can concurrently rewire prev/next.
    //
    // Example while committing Tx1:
    //   (v)-[A:Tx1]-[B:Tx1]-[C:Tx2]-[D:Tx1]-[E:Tx1]
    //
    // If Tx2 aborts, C is snipped and the chain can become:
    //   (v)-[A:Tx1]-[B:Tx1]-[D:Tx1]-[E:Tx1]
    //
    // i) A/B and D/E were two subchains but can become one merged subchain.
    //    Tail markers avoid re-processing already emitted depth when this happens.
    // ii) D (previously a non-sequential subchain head with prev=C) can stop
    //     looking like a head after rewiring (prev=B). The start condition also
    //     treats "prev is already a processed tail marker" as a valid start.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");

      bool const is_subchain_start =
          (prev.type == PreviousPtr::Type::VERTEX) ||
          (prev.type == PreviousPtr::Type::DELTA &&
           (prev.delta->commit_info != delta.commit_info ||
            (should_track_nonseq_subchains && processed_subchain_tails.contains(prev.delta))));
      if (!is_subchain_start) {
        continue;
      }

      Vertex *vertex = nullptr;
      if (prev.type == PreviousPtr::Type::VERTEX) {
        vertex = prev.vertex;
      } else {
        vertex = vertex_cache.GetVertexFromDelta(&delta);
      }
      auto const edge_create_filter = [](auto action) {
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
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      };
      if (should_track_nonseq_subchains) {
        find_and_apply_deltas(kTrackTails, &delta, vertex, edge_create_filter);
      } else {
        find_and_apply_deltas(kNoTracking, &delta, vertex, edge_create_filter);
      }
    }
    // 3. Process all Edge deltas and store all operations that modify edge data.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::EDGE) continue;
      find_and_apply_deltas(kNoTracking, &delta, prev.edge, [](auto action) {
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
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 4. Process all Vertex deltas and store all operations that delete edges.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");

      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(kNoTracking, &delta, prev.vertex, [](auto action) {
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
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
    // 5. Process all Vertex deltas and store all operations that delete vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULL_PTR, "Invalid pointer!");

      if (prev.type != PreviousPtr::Type::VERTEX) continue;
      find_and_apply_deltas(kNoTracking, &delta, prev.vertex, [](auto action) {
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
          default:
            LOG_FATAL("Unknown Delta Action");
        }
      });
    }
  };

  // Handle MVCC deltas
  if (!transaction_.deltas.empty()) {
    // Upper bound: append_deltas skips deltas whose action carries no durability record.
    commands.data.reserve(transaction_.deltas.size());
    append_deltas([&](const Delta &delta, auto *parent, uint64_t /*durability_commit_timestamp_arg*/) {
      if constexpr (std::is_same_v<decltype(parent), Edge *>) {
        // Connect the edge to the in-vertex and edge type for faster lookup.
        // NOTE: Invalid values will be sent in case the edge was created in this transaction.
        // In that case, we will cache the edge accessor in WalEdgeCreate, so no need for the overhead.
        auto edge_set_property_info = transaction_.GetEdgeSetPropertyInfo(static_cast<Edge *>(parent)->gid);
        commands.data.push_back({.delta = &delta,
                                 .vertex = nullptr,
                                 .edge = parent,
                                 .in_vertex_gid = edge_set_property_info.in_vertex_gid,
                                 .edge_type_id = edge_set_property_info.edge_type_id});
      } else {
        commands.data.push_back({.delta = &delta, .vertex = parent, .edge = nullptr});
      }
      commit_args.apply_cb_if_replica_write();
    });
  }

  // Every fused task borrows this frame (commands, streams), so if anything below throws before
  // ShipDeltas collected them, collect them here; on the normal path this is a no-op. Declared before
  // wal_promise on purpose: during unwind the promise must be destroyed first, so tasks blocked on
  // the gate observe the broken promise and finish before this guard joins them.
  auto const collect_workers = utils::OnScopeExit{[&]() noexcept { replicating_txn.DrainShipFutures(); }};

  // Durability gates the replicas' transaction ends: every fused task waits on this future between
  // encoding and shipping. If the WAL write below throws, the abandoned promise surfaces as a broken
  // promise in each fused task, whose containment drops the stream instead of shipping — no replica
  // can commit a transaction main did not make durable.
  std::promise<void> wal_promise;
  std::shared_future<void> const wal_result = wal_promise.get_future().share();

  // One fused task per streaming replica encodes concurrently with the WAL write below and the other
  // replicas, waits on the durability gate, and ships the transaction end. It does not matter what
  // gets sent in the `commit` argument of the transaction start as it always gets ignored EXCEPT when
  // loading from a WAL file, which uses what the WAL write below produces.
  replicating_txn.ScheduleEncodeAndShip(
      [mem_storage, &commands, durability_commit_timestamp, two_phase_commit, access_type = original_access_type_](
          ReplicaStream &stream) {
        const memory::DbArenaScope db_arena_scope{mem_storage->DbArenaPool()};
        stream.AppendTransactionStart(durability_commit_timestamp, !two_phase_commit, access_type);
        for (auto const *md_delta : commands.metadata) {
          auto encoder = stream.encoder();
          EncodeMetadataDelta(encoder, *md_delta, mem_storage, durability_commit_timestamp);
        }
        for (auto const &cmd : commands.data) {
          if (cmd.edge != nullptr) {
            stream.AppendDelta(
                *cmd.delta, cmd.edge, durability_commit_timestamp, mem_storage, cmd.in_vertex_gid, cmd.edge_type_id);
          } else {
            stream.AppendDelta(*cmd.delta, cmd.vertex, durability_commit_timestamp, mem_storage);
          }
        }
      },
      wal_result,
      durability_commit_timestamp,
      commit_args.replication_allowed() ? &commit_args.database_protector() : nullptr);

  // The WAL write runs inline: the commit thread would otherwise only sleep on the fused futures, and
  // it still has the just-traversed deltas hot in cache. WAL commit order follows from engine_lock_.
  {
    durability::WalTxnDataPos positions;
    // Append txn start delta and remember the position in the WAL file in which this delta is saved.
    positions.commit_flag_wal_position_ = mem_storage->wal_file_->AppendTransactionStart(
        durability_commit_timestamp, !two_phase_commit, original_access_type_);
    for (auto const *md_delta : commands.metadata) {
      EncodeMetadataDelta(mem_storage->wal_file_->encoder(), *md_delta, mem_storage, durability_commit_timestamp);
      mem_storage->wal_file_->UpdateStats(durability_commit_timestamp);
      commit_args.apply_cb_if_replica_write();
    }
    for (auto const &cmd : commands.data) {
      if (cmd.edge != nullptr) {
        mem_storage->wal_file_->AppendDelta(
            *cmd.delta, cmd.edge, durability_commit_timestamp, mem_storage, cmd.in_vertex_gid, cmd.edge_type_id);
      } else {
        mem_storage->wal_file_->AppendDelta(*cmd.delta, cmd.vertex, durability_commit_timestamp, mem_storage);
      }
      commit_args.apply_cb_if_replica_write();
    }
    // Add a delta that indicates that the transaction is fully written to the WAL
    auto const txn_end_positions = mem_storage->wal_file_->AppendTransactionEnd(durability_commit_timestamp);
    positions.crc_wal_pos_ = txn_end_positions.crc_wal_pos_;
    positions.stored_crc_ = txn_end_positions.stored_crc_;
    // When committing immediately the WAL file must be finalized before transaction ends ship to replicas.
    if (!two_phase_commit) {
      mem_storage->FinalizeWalFile();
    }
    wal_txn_positions_ = positions;
  }
  // Durability achieved: open the gate so the fused tasks may ship their transaction ends.
  wal_promise.set_value();

  // Collects every fused task, so no worker is left borrowing this frame, and folds their results
  // into the replication failures (the collect_workers guard backstops the unwind paths). Encoding
  // must finish before the commit timestamp is published: EncodeDelta reads live vertex and edge
  // state, which other transactions may overwrite in place the moment this transaction becomes
  // visible. A replica-side failure is contained inside its task (the replica drops to recovery):
  // main and the healthy replicas still commit, consistent with the WAL.
  bool const replicas_ok = replicating_txn.ShipDeltas(durability_commit_timestamp, commit_args);

  // Returns only the status of SYNC and STRICT_SYNC replicas.
  return replicas_ok;
}

std::expected<std::filesystem::path, InMemoryStorage::CreateSnapshotError> InMemoryStorage::CreateSnapshot(
    bool force, std::string_view trigger) {
  // A broken storage (failed recovery) is empty and must never overwrite the
  // operator's untouched corrupt durability files with an empty snapshot.
  if (IsBroken()) {
    return std::unexpected{CreateSnapshotError::AbortSnapshot};
  }

  auto abort_reset = utils::OnScopeExit([this]() mutable {
    // Abort is a one shot, reset it to false every time
    abort_snapshot_.store(false, std::memory_order_release);
  });

  if (abort_snapshot_.load(std::memory_order_acquire)) {
    return std::unexpected{CreateSnapshotError::AbortSnapshot};
  }

  // Make sure only one create snapshot is running at any moment
  auto expected = false;
  auto already_running = !snapshot_running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
  if (already_running) {
    return std::unexpected{CreateSnapshotError::AlreadyRunning};
  }
  snapshot_progress_.Start();
  auto const clear_snapshot_running_on_exit = utils::OnScopeExit{[&] {
    // Clear `running` first so readers stop trusting the fields before they are wiped.
    snapshot_running_.store(false, std::memory_order_release);
    snapshot_progress_.Reset();
  }};

  // This is to make sure SHOW SNAPSHOTS, CREATE SNAPSHOT, and some replication
  // stuff are mutually exclusive from each other
  auto const snapshot_guard = std::unique_lock(snapshot_lock_);

  auto accessor = std::invoke([&]() {
    if (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL) {
      // For analytical no other write txn can be in play
      return ReadOnlyAccess(IsolationLevel::SNAPSHOT_ISOLATION);  // Do we need snapshot isolation?
    }
    return Access(StorageAccessType::READ, IsolationLevel::SNAPSHOT_ISOLATION, std::nullopt);
  });

  utils::Timer timer;
  Transaction *transaction = accessor->GetTransaction();

  DMG_ASSERT(transaction->last_durable_ts_.has_value());
  auto const &epoch = repl_storage_state_.epoch_;
  auto const &epochHistory = repl_storage_state_.history;
  auto const &storage_uuid = uuid();

  SnapshotDigest current_digest;
  // In memory analytical doesn't update last_durable_ts so digest isn't valid
  if (transaction->storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    current_digest = SnapshotDigest{.epoch_ = epoch,
                                    .history_ = epochHistory,
                                    .storage_uuid_ = storage_uuid,
                                    .last_durable_ts_ = *transaction->last_durable_ts_};

    if (!force && last_snapshot_digest_ == current_digest)
      return std::unexpected{CreateSnapshotError::NothingNewToWrite};
  }

  // At the moment, the only way in which create snapshot can fail is if it got aborted
  const auto snapshot_path = durability::CreateSnapshot(this,
                                                        transaction,
                                                        recovery_.snapshot_directory_,
                                                        recovery_.wal_directory_,
                                                        &vertices_,
                                                        &edges_,
                                                        storage_uuid,
                                                        epoch.id(),
                                                        epochHistory,
                                                        &file_retainer_,
                                                        &abort_snapshot_,
                                                        &snapshot_progress_,
                                                        trigger);
  if (!snapshot_path) {
    return std::unexpected{CreateSnapshotError::AbortSnapshot};
  }

  // Update digest only after the file has been created. Only in transaction because digests are used only in
  // transactional mode
  if (transaction->storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    last_snapshot_digest_ = std::move(current_digest);
  }

  {
    auto snapshot_elapsed = std::chrono::duration<double>(timer.Elapsed());
    metric_handles_.snapshot_creation_latency_seconds.Observe(snapshot_elapsed.count());
  }

  return *snapshot_path;
}

// NOTE: Make sure this function is called while exclusively holding on to the main lock
std::expected<void, InMemoryStorage::RecoverSnapshotError> InMemoryStorage::RecoverSnapshot(
    std::filesystem::path uri, bool force, memgraph::replication_coordination_glue::ReplicationRole replication_role,
    std::optional<utils::S3Config> s3_config) {
  using memgraph::replication_coordination_glue::ReplicationRole;
  if (replication_role == ReplicationRole::REPLICA) {
    return std::unexpected{InMemoryStorage::RecoverSnapshotError::DisabledForReplica};
  }

  auto const uri_str = uri.string();
  const auto local_path = recovery_.snapshot_directory_ / uri.filename();
  const bool file_in_local_dir = local_path == uri;

  auto handler_error = [&]() {
    // If file was copied over, delete...
    if (!file_in_local_dir) file_retainer_.DeleteFile(local_path);
  };

  constexpr auto url_matcher = ctre::starts_with<"(https?|ftp)://">;
  constexpr auto s3_matcher = ctre::starts_with<"s3://">;

  if (url_matcher(uri_str)) {
    constexpr auto file_mode = "wbx";
    utils::FileUniquePtr file(std::fopen(local_path.string().data(), file_mode), &std::fclose);

    if (!requests::CreateAndDownloadFile(
            uri_str, std::move(file), memgraph::flags::run_time::GetFileDownloadConnTimeoutSec())) {
      // Delete the empty or partially written file
      handler_error();
      return std::unexpected{InMemoryStorage::RecoverSnapshotError::DownloadFailure};
    }

    spdlog::trace("Downloaded snapshot file from {} to {}", uri_str, local_path.string());

  } else if (s3_matcher(uri_str)) {
    DMG_ASSERT(s3_config.has_value(), "S3Config doesn't have a value");
    if (auto const res = s3_config->Validate(); res.has_value()) {
      switch (*res) {
        using enum utils::AwsValidationError;
        case AWS_REGION: {
          return std::unexpected{InMemoryStorage::RecoverSnapshotError::S3MissingAwsRegion};
        }
        case AWS_ACCESS_KEY: {
          return std::unexpected{InMemoryStorage::RecoverSnapshotError::S3MissingAwsAccessKey};
        }
        case AWS_SECRET_KEY: {
          return std::unexpected{InMemoryStorage::RecoverSnapshotError::S3MissingAwsSecretKey};
        }
        default: {
          std::unreachable();
        }
      }
    }
    if (auto const res = utils::GetS3Object(uri, *s3_config, local_path.string()); !res.has_value()) {
      spdlog::error(res.error().message);
      return std::unexpected{InMemoryStorage::RecoverSnapshotError::S3GetFailure};
    }

  } else {  // local filesystem path
    if (!std::filesystem::exists(uri) || std::filesystem::is_directory(uri)) {
      return std::unexpected{InMemoryStorage::RecoverSnapshotError::MissingFile};
    }

    // Copy to local snapshot dir
    std::error_code ec{};
    if (!file_in_local_dir) {
      std::filesystem::copy_file(uri, local_path, std::filesystem::copy_options::overwrite_existing, ec);
      if (ec) {
        spdlog::warn("Failed to copy snapshot into local snapshots directory. Err: {}", ec.message());
        return std::unexpected{InMemoryStorage::RecoverSnapshotError::CopyFailure};
      }
    }
  }

  auto file_locker = file_retainer_.AddLocker();
  (void)file_locker.Access().AddPath(local_path);

  if (force) {
    Clear();
  } else {
    if (repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_ != kTimestampInitialId) {
      handler_error();
      return std::unexpected{InMemoryStorage::RecoverSnapshotError::NonEmptyStorage};
    }
  }

  // When creating a snapshot, we first lock the snapshot, then create the accessor, so no need for the snapshot lock
  // GC could be running without the main lock, so lock it
  // Engine lock is needed because of PrepareForNewEpoch
  auto gc_lock = std::unique_lock{gc_lock_};
  auto engine_lock = std::unique_lock{engine_lock_};

  std::string loaded_snapshot_uuid;

  try {
    spdlog::debug("Recovering from a snapshot {}", local_path);
    auto recovered_snapshot =
        storage::durability::LoadSnapshot(local_path,
                                          &vertices_,
                                          &edges_,
                                          edges_metadata_index_ ? &*edges_metadata_index_ : nullptr,
                                          &repl_storage_state_.history,
                                          name_id_mapper_.get(),
                                          &edge_count_,
                                          config_,
                                          &enum_store_,
                                          config_.salient.items.enable_schema_info ? &schema_info_.Get() : nullptr,
                                          &ttl_,
                                          &description_store_);
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
    // EXPERIMENTAL (lock-free-read-snapshot): seed the watermark from the local MVCC counter
    // (timestamp_ - 1 = highest committed ts in this storage's own timestamp space).  Using the
    // local counter is space-correct: readers compare against local MVCC delta timestamps.
    // Guard against underflow when the counter is still at its initial value.
    if (config_.experimental_lockfree_read_snapshot) {
      last_committed_mvcc_ts_.store(std::max(last_committed_mvcc_ts_.load(std::memory_order_relaxed),
                                             timestamp_ > kTimestampInitialId ? timestamp_ - 1 : kTimestampInitialId),
                                    std::memory_order_release);
    }
    loaded_snapshot_uuid = recovered_snapshot.snapshot_info.uuid;

    auto const update_func = [new_ldt = recovered_snapshot.snapshot_info.durable_timestamp,
                              new_num_committed_txns = recovered_snapshot.snapshot_info.num_committed_txns](
                                 CommitTsInfo const & /*old_info*/) -> CommitTsInfo {
      return CommitTsInfo{.ldt_ = new_ldt, .num_committed_txns_ = new_num_committed_txns};
    };
    atomic_struct_update<CommitTsInfo>(repl_storage_state_.commit_ts_info_, update_func);

    // We are the only active transaction, so mark everything up to the next timestamp
    if (timestamp_ > 0) commit_log_->MarkFinishedInRange(0, timestamp_ - 1);

    spdlog::trace("Recovering derived state from snapshot.");
    storage::durability::RecoverDerivedState(&vertices_,
                                             &edges_,
                                             name_id_mapper_.get(),
                                             &indices_,
                                             &constraints_,
                                             config_,
                                             recovery_info,
                                             db_arena_,
                                             recovered_snapshot.indices_constraints,
                                             edges_metadata_index_ ? &*edges_metadata_index_ : nullptr,
                                             config_.salient.items.properties_on_edges);
    spdlog::trace("Successfully recovered from snapshot {}", local_path);

    // Destroying current wal file
    wal_file_.reset();

    // The tenant is now functionally healthy: the snapshot data is fully in memory and derived
    // state is rebuilt. Clear broken here rather than after the .old backup housekeeping below,
    // so a cosmetic filesystem failure (unable to archive superseded files, or overwrite the
    // UUID) does not leave a tenant that holds valid data query-locked with durability suppressed.
    SetBroken(false);

    auto const use_old_dir = FLAGS_storage_backup_dir_enabled;
    auto const &old_dir = kOldDurabilityDir;

    // Move all previous snapshots and WAL files to .old dir
    if (use_old_dir) {
      spdlog::trace("Moving old snapshots and WALs to {}", old_dir);
      std::error_code ec{};
      auto const snapshot_old_dir = recovery_.snapshot_directory_ / old_dir;
      // Clear old directory
      if (std::filesystem::exists(snapshot_old_dir)) {
        std::filesystem::remove_all(snapshot_old_dir);
      }
      // Recreate clean old directory
      std::filesystem::create_directory(snapshot_old_dir, ec);
      if (ec) {
        spdlog::warn(
            "Failed to create backup snapshot directory; snapshots directory should be cleaned manually. Err: {}",
            ec.message());
        handler_error();
        return std::unexpected{InMemoryStorage::RecoverSnapshotError::BackupFailure};
      }
      auto const wal_old_dir = recovery_.wal_directory_ / old_dir;
      // Clear old directory
      if (std::filesystem::exists(wal_old_dir)) {
        std::filesystem::remove_all(wal_old_dir);
      }
      // Recreate clean old directory
      std::filesystem::create_directory(wal_old_dir, ec);
      if (ec) {
        spdlog::warn("Failed to create backup WAL directory; WAL directory should be cleaned manually. Err: {}",
                     ec.message());
        handler_error();
        return std::unexpected{InMemoryStorage::RecoverSnapshotError::BackupFailure};
      }
    }

    // Move all snapshot files except the newest one to the old directory
    auto const snapshot_files = utils::GetFilesFromDir(recovery_.snapshot_directory_);
    for (const auto &snapshot_path : snapshot_files) {
      // Delete file if old dir won't be used anymore
      if (!use_old_dir) {
        file_retainer_.DeleteFile(snapshot_path);
        continue;
      }
      // Move to .old if enable_backup_dir is true
      auto const new_path = recovery_.snapshot_directory_ / old_dir / snapshot_path.filename();
      if (local_path != snapshot_path) {
        spdlog::trace("Moving snapshot file {} to {}", snapshot_path, new_path);
        file_retainer_.RenameFile(snapshot_path, new_path);
      } else if (file_in_local_dir) {
        spdlog::trace("Copying snapshot file {} to {}", snapshot_path, new_path);
        // Used a snapshot for the local storage, back it up
        std::error_code ec;
        std::filesystem::copy_file(snapshot_path, new_path, ec);
        if (ec) {
          spdlog::warn(
              "Failed to copy snapshot file to backup directory; snapshots directory should be cleaned "
              "manually. Err: {}",
              ec.message());
          handler_error();
          return std::unexpected{InMemoryStorage::RecoverSnapshotError::BackupFailure};
        }
      }
    }
    std::error_code ec;
    std::filesystem::remove(recovery_.snapshot_directory_ / old_dir, ec);  // remove dir if empty

    // Move or delete all WAL files to the old directory
    auto const wal_files = utils::GetFilesFromDir(recovery_.wal_directory_);
    for (const auto &wal_path : wal_files) {
      if (!use_old_dir) {
        file_retainer_.DeleteFile(wal_path);
      } else {
        auto const new_path = recovery_.wal_directory_ / old_dir / wal_path.filename();
        spdlog::trace("Moving WAL file {} to {}", wal_path, new_path);
        file_retainer_.RenameFile(wal_path, new_path);
      }
    }
    std::filesystem::remove(recovery_.wal_directory_ / old_dir, ec);  // remove dir if empty

    if (uuid() != loaded_snapshot_uuid) {
      // Rewrite the UUID in the snapshot file
      if (!durability::OverwriteSnapshotUUID(local_path, uuid())) {
        handler_error();
        return std::unexpected{InMemoryStorage::RecoverSnapshotError::FailedOverwritingUUID};
      }
    }
    // Generate new name for the snapshot file
    // Must be after moving to .old, otherwise you will move the file itself
    auto new_name = durability::MakeSnapshotName(recovered_snapshot.snapshot_info.durable_timestamp);
    file_retainer_.RenameFile(local_path, recovery_.snapshot_directory_ / new_name);
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
    return SnapshotFileInfo{.path = recovery_.snapshot_directory_,
                            .durable_timestamp = 0,
                            .creation_time = utils::LocalDateTime{*next},
                            .size = 0};
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
  auto const maybe_snapshot_files =
      durability::GetSnapshotFiles(recovery_.snapshot_directory_ /*, std::string(storage_uuid())*/);
  if (!maybe_snapshot_files.has_value()) {
    return res;
  }

  auto const &snapshot_files = *maybe_snapshot_files;
  std::error_code ec;
  for (const auto &snapshot_file : snapshot_files) {
    auto const &snapshot_path = snapshot_file.path;
    auto const &durable_timestamp = snapshot_file.durable_timestamp;
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
    res.emplace_back(snapshot_path, durable_timestamp, write_time_ldt, size);
  }

  std::ranges::sort(res, [](const auto &lhs, const auto &rhs) { return lhs.creation_time > rhs.creation_time; });

  return res;
}

void InMemoryStorage::FreeMemory(utils::ResourceLockGuard main_guard, bool periodic) {
  std::invoke(free_memory_func_, std::move(main_guard), periodic);
}

uint64_t InMemoryStorage::GetCommitTimestamp() { return timestamp_++; }

void InMemoryStorage::PrepareForNewEpoch() {
  // EXPERIMENTAL (lock-free-read-snapshot): take commit_mutex_ before engine_lock_ (committer order) so this
  // WAL reset cannot race a committer's WAL append under the flag.
  std::optional<std::unique_lock<std::mutex>> commit_serializer;
  if (config_.experimental_lockfree_read_snapshot) {
    commit_serializer.emplace(commit_mutex_);
  }
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
    if (!ret || !ret.value()) {
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
  DMG_ASSERT(rw_type != StorageAccessType::UNIQUE, "UNIQUE access must go through UniqueAccess()");
  return std::unique_ptr<InMemoryAccessor>(
      new InMemoryAccessor{this, override_isolation_level, AcquireGuardOrThrow(this, rw_type, timeout)});
}

std::unique_ptr<Storage::Accessor> InMemoryStorage::UniqueAccess(std::optional<IsolationLevel> override_isolation_level,
                                                                 std::optional<std::chrono::milliseconds> timeout) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{
      this, override_isolation_level, AcquireGuardOrThrow(this, StorageAccessType::UNIQUE, timeout)});
}

std::unique_ptr<Storage::Accessor> InMemoryStorage::ReadOnlyAccess(
    std::optional<IsolationLevel> override_isolation_level, std::optional<std::chrono::milliseconds> timeout) {
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{
      this, override_isolation_level, AcquireGuardOrThrow(this, StorageAccessType::READ_ONLY, timeout)});
}

std::unique_ptr<Storage::Accessor> InMemoryStorage::TryAccess(StorageAccessType rw_type,
                                                              std::optional<IsolationLevel> override_isolation_level) {
  utils::ResourceLockGuard guard{main_lock_, ToGuardType(rw_type), std::try_to_lock};
  if (!guard.owns_lock()) return nullptr;
  return std::unique_ptr<InMemoryAccessor>(new InMemoryAccessor{this, override_isolation_level, std::move(guard)});
}

void InMemoryStorage::CreateSnapshotHandler(
    std::function<std::expected<void, InMemoryStorage::CreateSnapshotError>(std::string_view)> cb) {
  create_snapshot_handler = [cb = std::move(cb)](std::string_view trigger) {
    if (auto maybe_error = cb(trigger); !maybe_error.has_value()) {
      switch (maybe_error.error()) {
        case CreateSnapshotError::ReachedMaxNumTries:
          spdlog::warn("snapshot failed: {}. Please contact support.",
                       CreateSnapshotErrorToString(maybe_error.error()));
          break;
        case CreateSnapshotError::AbortSnapshot:
          spdlog::warn("snapshot failed: {}", CreateSnapshotErrorToString(maybe_error.error()));
          break;
        case CreateSnapshotError::AlreadyRunning:
        case CreateSnapshotError::NothingNewToWrite:
          spdlog::info("snapshot skipped: {}", CreateSnapshotErrorToString(maybe_error.error()));
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
    const memory::DbArenaScope db_arena_scope{db_arena_};
    // Skip broken databases: they are empty and writing a snapshot would overwrite
    // the operator's untouched corrupt durability files. Skipped silently to avoid
    // per-tick log spam (CreateSnapshot also guards defensively).
    if (!token.stop_requested() && !IsBroken()) {
      this->create_snapshot_handler("periodic");
    }
  });
}

EdgeInfo ExtractEdgeInfo(Vertex *from_vertex, const Edge *edge_ptr) {
  std::shared_lock const guard{from_vertex->lock};
  for (const auto &out_edge : from_vertex->out_edges) {
    const auto [edge_type, other_vertex, edge_ref] = out_edge;
    if (edge_ref.ptr == edge_ptr) {
      return std::tuple(edge_ref, edge_type, from_vertex, other_vertex);
    }
  }
  return std::nullopt;
}

// Scan from_vertex->out_edges for the light edge with the given GID.
// Acquires from_vertex->lock (shared) for the scan, mirroring ExtractEdgeInfo.
namespace {
EdgeInfo ScanOutEdgesForGid(Vertex *from_vertex, Gid edge_gid) {
  std::shared_lock const guard{from_vertex->lock};
  for (const auto &[edge_type, to_vertex, edge_ref] : from_vertex->out_edges) {
    if (edge_ref.ptr->gid == edge_gid) {
      return std::tuple(edge_ref, edge_type, from_vertex, to_vertex);
    }
  }
  return std::nullopt;
}

// Plural, either-direction form of ScanOutEdgesForGid: resolves a whole set of GIDs in one pass over one
// vertex's adjacency rather than one pass each. An edge is linked from both of its endpoints, so a caller that
// knows both can scan whichever list is shorter — see CheaperScanSide. Found GIDs are erased from `wanted`, so
// whatever remains on return is the set this vertex does not have on this side. Kept separate from the singular
// form rather than sharing an implementation with it: that one is called per vertex by the full-scan light-edge
// path, which must not pay for a set allocation.
std::vector<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>> ScanEdgesForGids(Vertex *vertex,
                                                                                  EdgeDirection direction,
                                                                                  std::unordered_set<Gid> &wanted) {
  std::vector<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>> found;
  found.reserve(wanted.size());

  auto const scanning_out = direction == EdgeDirection::OUT;
  std::shared_lock const guard{vertex->lock};
  // The Vertex* in an adjacency entry is always the opposing endpoint, so which end `vertex` is depends on the
  // list being walked.
  for (const auto &[edge_type, opposing_vertex, edge_ref] : scanning_out ? vertex->out_edges : vertex->in_edges) {
    if (wanted.erase(edge_ref.ptr->gid) == 0) continue;
    found.emplace_back(
        edge_ref, edge_type, scanning_out ? vertex : opposing_vertex, scanning_out ? opposing_vertex : vertex);
    if (wanted.empty()) break;
  }
  return found;
}

// Which endpoint's adjacency is cheaper to scan for an edge between these two vertices. Sizes are read one lock
// at a time: only ever holding one means no lock-ordering rule is needed and a self-loop needs no special case,
// unlike FindEdges which holds both at once.
EdgeDirection CheaperScanSide(Vertex *from_vertex, Vertex *to_vertex) {
  auto const out_n = std::invoke([&] {
    std::shared_lock const guard{from_vertex->lock};
    return from_vertex->out_edges.size();
  });
  auto const in_n = std::invoke([&] {
    std::shared_lock const guard{to_vertex->lock};
    return to_vertex->in_edges.size();
  });
  return out_n <= in_n ? EdgeDirection::OUT : EdgeDirection::IN;
}
}  // namespace

EdgeInfo InMemoryStorage::FindHeavyEdge(Gid edge_gid) {
  auto edge_acc = edges_.access();
  auto edge_it = edge_acc.find(edge_gid);
  if (edge_it == edge_acc.end()) {
    return std::nullopt;
  }

  auto *edge_ptr = &(*edge_it);

  // Pin vertices_ for the duration of the ExtractEdgeInfo scan. The Vertex*
  // returned by the index points into this skip list; without the accessor,
  // GC could reclaim the node mid-scan.
  auto vertices_acc = vertices_.access();
  if (edges_metadata_index_) {
    return ExtractEdgeInfo(edges_metadata_index_->FromVertexOf(edge_gid), edge_ptr);
  }

  for (auto &from_vertex : vertices_acc) {
    if (auto maybe_info = ExtractEdgeInfo(&from_vertex, edge_ptr)) {
      return maybe_info;
    }
  }
  return std::nullopt;
}

EdgeInfo InMemoryStorage::FindLightEdgeFromMetadata(Gid edge_gid) {
  // Light edges are not in edges_; resolve from_vertex via the metadata index
  // (soft-miss: a deleted light edge has no entry) then rescan its out_edges.
  if (!edges_metadata_index_) return std::nullopt;
  // Pin vertices_ BEFORE calling TryFromVertexOf so that GC cannot reclaim the
  // Vertex node between the index lookup and the point we lock/scan it. This
  // mirrors FindHeavyEdge, which also pins before the index consultation.
  auto vertices_acc = vertices_.access();
  auto *from_vertex = edges_metadata_index_->TryFromVertexOf(edge_gid);
  if (from_vertex == nullptr) return std::nullopt;
  return ScanOutEdgesForGid(from_vertex, edge_gid);
}

EdgeInfo InMemoryStorage::FindLightEdgeByScan(Gid edge_gid) {
  // No metadata index: scan all vertices' out_edges for the gid.
  auto vertices_acc = vertices_.access();
  for (auto &from_vertex : vertices_acc) {
    if (auto maybe_info = ScanOutEdgesForGid(&from_vertex, edge_gid)) {
      return maybe_info;
    }
  }
  return std::nullopt;
}

EdgeInfo InMemoryStorage::FindEdge(Gid edge_gid) {
  if (config_.salient.items.storage_light_edge) {  // GATE
    if (edges_metadata_index_) return FindLightEdgeFromMetadata(edge_gid);
    return FindLightEdgeByScan(edge_gid);
  }
  return FindHeavyEdge(edge_gid);
}

EdgeInfo InMemoryStorage::FindEdge(Gid edge_gid, Gid from_vertex_gid) {
  if (config_.salient.items.storage_light_edge) {  // GATE — light edges not in edges_
    auto vertices_acc = vertices_.access();
    auto vertex_it = vertices_acc.find(from_vertex_gid);
    // Soft-miss on an absent from_vertex: the heavy arm (and the public
    // optional-returning FindEdge wrapper) return nullopt for a not-found edge,
    // and callers (e.g. the mgp C-API edge lookup) treat that as "no such edge"
    // rather than an error. A stale/deleted from_vertex_gid means the edge is
    // gone, so return nullopt instead of throwing across the query boundary.
    if (vertex_it == vertices_acc.end()) {
      return std::nullopt;
    }
    auto *from_vertex = &(*vertex_it);
    return ScanOutEdgesForGid(from_vertex, edge_gid);
  }
  auto edge_acc = edges_.access();
  auto edge_it = edge_acc.find(edge_gid);
  if (edge_it == edge_acc.end()) {
    return std::nullopt;
  }

  auto *edge_ptr = &(*edge_it);

  // Pin vertices_ for ExtractEdgeInfo (see FindEdge(Gid) above).
  auto vertices_acc = vertices_.access();
  if (edges_metadata_index_) {
    return ExtractEdgeInfo(edges_metadata_index_->FromVertexOf(edge_gid), edge_ptr);
  }

  auto vertex_it = vertices_acc.find(from_vertex_gid);
  if (vertex_it == vertices_acc.end()) {
    throw utils::BasicException("Vertex with GID {} not found in the database", from_vertex_gid.AsUint());
  }
  return ExtractEdgeInfo(&(*vertex_it), edge_ptr);
}

Result<size_t> InMemoryStorage::InMemoryAccessor::DeleteEdgesEx(std::span<EdgeDeleteSpec const> edges) {
  if (edges.empty()) return size_t{0};

  // Deliberately not routed through InMemoryStorage::FindEdge: that has to recover the edge type and the
  // to-vertex from adjacency (via ExtractEdgeInfo / ScanOutEdgesForGid) because its callers know only a GID,
  // which costs an O(degree) scan per edge. Every spec here already names both, so the only open question is
  // the EdgeRef — and only the light config needs to touch adjacency to answer it.
  std::vector<EdgeAccessor> accessors;
  accessors.reserve(edges.size());

  // Memoized because a batch is typically many edges around few vertices — a hub's whole fan-out resolves to
  // the same Vertex*, and without this each edge would repeat the skip-list lookup. Safe for the length of the
  // batch: vertices are only ever deleted after their edges, so nothing here can be removed mid-resolution.
  std::unordered_map<Gid, Vertex *> resolved_vertices;
  auto resolve_vertex = [&](Gid gid) -> Vertex * {
    auto [it, inserted] = resolved_vertices.try_emplace(gid, nullptr);
    if (inserted) {
      auto vertex = FindVertex(gid, View::NEW);
      it->second = vertex ? vertex->vertex_ : nullptr;
    }
    return it->second;
  };

  // A spec already names the edge type and both endpoints, so the configs differ only in how they answer the
  // one remaining question — which EdgeRef the GID denotes.
  auto emplace_from_spec = [&](EdgeDeleteSpec const &spec, EdgeRef edge_ref) {
    auto *from_vertex = resolve_vertex(spec.from_gid);
    auto *to_vertex = resolve_vertex(spec.to_gid);
    if (!from_vertex || !to_vertex) return false;
    accessors.emplace_back(edge_ref, spec.edge_type, from_vertex, to_vertex, storage_, &transaction_);
    return true;
  };

  // Held until after DetachDelete so an Edge* taken out of the skip list cannot be reclaimed underneath the
  // batch. Engaged only when there is a skip list to read, i.e. heavy edges.
  std::optional<utils::SkipListDb<Edge>::Accessor> edge_acc;

  if (!config_.properties_on_edges) {
    // No Edge object is ever allocated, so the EdgeRef carries the GID itself and there is nothing to look up.
    // Mirrors the same split in CreateEdgeInternal. Light edges cannot reach this arm: they imply properties.
    for (auto const &spec : edges) {
      if (!emplace_from_spec(spec, EdgeRef{spec.edge_gid})) return std::unexpected{Error::NONEXISTENT_OBJECT};
    }
  } else if (!config_.storage_light_edge) {
    // Heavy edges are in the edges_ skip list, keyed by GID.
    edge_acc = static_cast<InMemoryStorage *>(storage_)->edges_.access();
    for (auto const &spec : edges) {
      auto edge_it = edge_acc->find(spec.edge_gid);
      if (edge_it == edge_acc->end()) return std::unexpected{Error::NONEXISTENT_OBJECT};
      if (!emplace_from_spec(spec, EdgeRef{&*edge_it})) return std::unexpected{Error::NONEXISTENT_OBJECT};
    }
  } else {
    // Light edges are in no skip list, so adjacency is the only way to reach the Edge*.
    //
    // Each edge is linked from both endpoints, so it is resolved from whichever side is cheaper, and edges that
    // pick the same vertex are bucketed to share one scan. Both halves matter: the side choice is what makes
    // deleting a handful of a supernode's edges cost O(1) each instead of a full scan of the supernode, and the
    // bucketing is what makes deleting all of them cost one scan rather than one per edge.
    std::unordered_map<Vertex *, std::unordered_set<Gid>> wanted_by_out_vertex;
    std::unordered_map<Vertex *, std::unordered_set<Gid>> wanted_by_in_vertex;

    for (auto const &spec : edges) {
      auto *from_vertex = resolve_vertex(spec.from_gid);
      auto *to_vertex = resolve_vertex(spec.to_gid);
      if (!from_vertex || !to_vertex) return std::unexpected{Error::NONEXISTENT_OBJECT};

      if (CheaperScanSide(from_vertex, to_vertex) == EdgeDirection::OUT) {
        wanted_by_out_vertex[from_vertex].insert(spec.edge_gid);
      } else {
        wanted_by_in_vertex[to_vertex].insert(spec.edge_gid);
      }
    }

    // Returns false if this vertex turned out not to have some of the edges asked of it.
    auto drain_bucket = [&](Vertex *vertex, EdgeDirection direction, std::unordered_set<Gid> &wanted) {
      for (auto const &[edge_ref, edge_type, from_v, to_v] : ScanEdgesForGids(vertex, direction, wanted)) {
        accessors.emplace_back(edge_ref, edge_type, from_v, to_v, storage_, &transaction_);
      }
      return wanted.empty();
    };

    // The two maps partition the batch — the loop above put each edge in exactly one of them — so these are two
    // halves of a single pass over the edges, not one pass per direction. No edge is looked at twice.
    for (auto &[vertex, wanted] : wanted_by_out_vertex) {
      if (!drain_bucket(vertex, EdgeDirection::OUT, wanted)) return std::unexpected{Error::NONEXISTENT_OBJECT};
    }
    for (auto &[vertex, wanted] : wanted_by_in_vertex) {
      if (!drain_bucket(vertex, EdgeDirection::IN, wanted)) return std::unexpected{Error::NONEXISTENT_OBJECT};
    }
  }

  auto edge_ptrs = accessors | rv::transform([](EdgeAccessor &edge) { return &edge; }) | r::to_vector;
  auto res = DetachDelete({}, std::move(edge_ptrs), false);
  if (!res) return std::unexpected{res.error()};
  if (!*res) return size_t{0};
  return (*res)->second.size();
}

Edge *InMemoryStorage::LightEdgePool::Create(Gid gid, Delta *delta) {
  // Allocation failure is propagated, NOT swallowed: an over-limit allocation
  // throws utils::OutOfMemoryException (derived from BasicException, NOT
  // std::bad_alloc) and a genuine OOM throws std::bad_alloc. Both must reach the
  // query layer so the transaction aborts with an error — exactly as the heavy
  // path does when edges_.insert() allocates a skip-list node. Catching only
  // std::bad_alloc here (in a noexcept function) would let OutOfMemoryException
  // cross the noexcept boundary -> std::terminate, turning a recoverable
  // memory-limit hit into a server crash. Edge(Gid, Delta*) is nothrow (asserted
  // below), so only the allocation can throw; it does so before any edge is
  // constructed or linked, so propagation is exception-safe — the caller's delta
  // is unwound by the transaction abort, identical to the heavy OOM path.
  static_assert(std::is_nothrow_constructible_v<Edge, Gid, Delta *>,
                "Edge(Gid, Delta*) must be nothrow; otherwise a throwing "
                "construct_at would leak the allocation made just above it.");
  memory::DbAwareAllocator<Edge> alloc;
  Edge *edge_ptr = std::allocator_traits<decltype(alloc)>::allocate(alloc, 1);
  std::construct_at(edge_ptr, gid, delta);
  return edge_ptr;
}

void InMemoryStorage::LightEdgePool::Destroy(Edge *p) noexcept {
  if (p == nullptr) return;
  memory::DbAwareAllocator<Edge> alloc;
  std::destroy_at(p);
  std::allocator_traits<decltype(alloc)>::deallocate(alloc, p, 1);
}

void InMemoryStorage::HarvestDeltaChainOnlyLightEdges() noexcept {
  // Walk both containers under their respective locks. All background tasks are
  // stopped before this runs (single-threaded dtor path), so no concurrency on
  // the delta chains — atomic reads of prev/delta/deleted suffice; no edge lock
  // needed. The edge->delta()==&delta guard is the chain-head dedup used by the
  // GC loop (storage.cpp:3135) and UnlinkAndRemoveDeltas (:303-310): it ensures
  // each deleted light Edge* is freed EXACTLY ONCE across the whole walk.
  auto harvest = [](auto &transactions) noexcept {
    for (auto &entry : transactions) {
      for (Delta &delta : entry.deltas_) {
        auto prev = delta.prev.Get();
        if (prev.type != PreviousPtr::Type::EDGE) continue;
        Edge *edge = prev.edge;
        if (!edge->deleted()) continue;
        if (edge->delta() != &delta) continue;
        InMemoryStorage::LightEdgePool::Destroy(edge);
      }
    }
  };
  committed_transactions_.WithLock(harvest);
  waiting_gc_deltas_.WithLock(harvest);
}

void InMemoryStorage::ClearLightEdges(std::function<void()> const &on_progress) noexcept {
  // Free ONLY live light edges held in vertex adjacency. Each edge appears
  // exactly once across all out_edges (a self-loop has a single source-vertex
  // entry), so no deduplication is needed. Deleted light edges still queued in
  // the graveyard are freed by the loop below.
  auto vertex_acc = vertices_.access();
  uint64_t visited = 0;
  for (auto &vertex : vertex_acc) {
    for (auto const &[edge_type, to_vertex, edge_ref] : vertex.out_edges) {
      InMemoryStorage::LightEdgePool::Destroy(edge_ref.ptr);
    }
    // Mask first so the common path is an increment and a predicted-not-taken branch.
    if (((++visited & utils::kClearProgressMask) == 0) && on_progress) on_progress();
  }

  // Also free any deleted light edges still queued in the graveyard. Swap out
  // under lock, then free without holding the SpinLock (mirrors
  // DrainLightEdgeGraveyard's swap-out-then-free pattern).
  std::list<LightEdgeGraveyardEntry, memory::DbAwareAllocator<LightEdgeGraveyardEntry>> pending_graveyard;
  light_edge_graveyard_.WithLock([&](auto &graveyard) { pending_graveyard.swap(graveyard); });
  for (auto &entry : pending_graveyard) {
    for (auto *edge : entry.edges.elements()) {
      InMemoryStorage::LightEdgePool::Destroy(edge);
    }
  }

  // Free any deleted light edges still queued in deleted_edges_ that never
  // reached the graveyard (i.e. were deleted but GC has not yet collected them —
  // the commit (FastDiscard) / abort paths route them into deleted_edges_, and
  // the CollectGarbage swap later drains them into the graveyard). These
  // pool-allocated Edge* are owned by no skip-list, so without this drain they
  // leak at teardown/Clear/DropGraph. The three sets (live adjacency,
  // deleted_edges_, graveyard) are disjoint: CollectGarbage swaps deleted_edges_
  // empty BEFORE moving the edges into a graveyard entry, and an edge is removed
  // from vertex adjacency in the same delta-processing step that later routes its
  // Edge* into deleted_edges_ — so no Edge* freed here is double-freed by the
  // loops above. Heavy mode never reaches this function (all callers gate on
  // storage_light_edge); heavy deleted_edges_ Edge* live in the edges_ skip-list
  // and are freed by it, so they must NOT be freed as light edges.
  deleted_edges_.WithLock([&](auto &deleted_edges) {
    for (auto *edge : deleted_edges.elements()) {
      InMemoryStorage::LightEdgePool::Destroy(edge);
    }
    deleted_edges.clear();
  });
}

void InMemoryStorage::Clear(std::function<void()> const &on_progress) {
  // NOTE: Make sure this function is called while exclusively holding on to the main lock
  // When creating a snapshot, we first lock the snapshot, then create the accessor
  // GC could be running without the main lock
  // Engine lock is needed because of PrepareForNewEpoch
  auto gc_lock = std::unique_lock{gc_lock_};
  auto engine_lock = std::unique_lock{engine_lock_};

  // Reset schema tracking before vertices_.clear(); pending_schema_updates_
  // entries hold raw Vertex* and would dangle.
  {
    std::lock_guard<std::mutex> const schema_lock{schema_queue_mutex_};
    pending_schema_updates_.clear();
    last_processed_commit_ts_ = kTimestampInitialId;
  }
  schema_info_.Clear();
  // Leak fix (mirrors dtor ordering): a deleted light edge whose delta chain was
  // never GC-unlinked is referenced ONLY by a RECREATE_OBJECT delta in
  // committed_transactions_/waiting_gc_deltas_. The clear() below frees those
  // deltas but NOT the pool-allocated Edge*, and ClearLightEdges only drains
  // {adjacency, graveyard, deleted_edges_} -> the Edge* would leak. Harvest BEFORE
  // ClearLightEdges (and before committed_transactions_ is cleared) so the delta
  // chains are still reachable. Gated: heavy Edge* live in edges_ and are freed by
  // the skip-list, not here.
  if (config_.salient.items.storage_light_edge) {
    HarvestDeltaChainOnlyLightEdges();
  }
  // Free live light edges before clearing vertices (their adjacency lists are
  // the only handle to the pool-allocated Edge*).
  if (config_.salient.items.storage_light_edge) {
    ClearLightEdges(on_progress);
  }

  // Clear main memory
  vertices_.clear(on_progress);
  vertices_.run_gc();
  vertex_id_.store(0, std::memory_order_release);

  edges_.clear(on_progress);
  edges_.run_gc();
  edge_id_.store(0, std::memory_order_release);
  edge_count_.store(0, std::memory_order_release);

  timestamp_ = kTimestampInitialId;
  if (config_.experimental_lockfree_read_snapshot) {
    // Recovery via Clear() rewinds timestamp_; the read-snapshot watermark and GC visibility floor
    // must rewind with it, or a post-recovery commit at a low ts appears committed-before a reader's
    // stale-high snapshot_ts (SI phantom read) and the floor stalls. Recovery paths reseed the
    // watermark to the recovered durable ts afterward.
    last_committed_mvcc_ts_.store(kTimestampInitialId, std::memory_order_release);
    gc_visibility_floor_.store(kTimestampInitialId, std::memory_order_release);
  }
  transaction_id_ = kTransactionInitialId;

  // Reset WALs
  wal_seq_num_ = 0;
  wal_file_.reset();
  wal_unsynced_transactions_ = 0;

  // Reset the commit log
  commit_log_.reset();
  commit_log_.emplace();

  // Drop any pending GC work
  deleted_vertices_->clear();
  deleted_edges_->clear();
  garbage_undo_buffers_->clear();
  committed_transactions_->clear();
  waiting_gc_deltas_->clear();

  // Clear incoming async index creation requests
  async_indexer_.Clear();

  // Clear indices, constraints and metadata
  indices_.DropGraphClearIndices();
  constraints_.DropGraphClearConstraints();

  if (edges_metadata_index_) {
    edges_metadata_index_->Clear();
    edges_metadata_index_->RunGc();
  }
  stored_node_labels_.clear();
  stored_edge_types_.clear();

  // Reset helper classes
  enum_store_.clear();

  // Replication epoch and timestamp reset
  repl_storage_state_.epoch_.SetEpoch(std::string(utils::UUID{}));
  CommitTsInfo const new_info{.ldt_ = 0, .num_committed_txns_ = 0};
  repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
  repl_storage_state_.history.clear();

  last_snapshot_digest_ = std::nullopt;
}

bool InMemoryStorage::InMemoryAccessor::PointIndexExists(LabelId label, PropertyId property) const {
  return transaction_.active_indices_->point_->PointIndexExists(label, property);
}

IndicesInfo InMemoryStorage::InMemoryAccessor::ListAllIndices() const {
  return {
      .label = transaction_.active_indices_->label_->ListIndices(transaction_.start_timestamp),
      .label_properties = transaction_.active_indices_->label_properties_->ListIndices(transaction_.start_timestamp),
      .edge_type = transaction_.active_indices_->edge_type_->ListIndices(transaction_.start_timestamp),
      .edge_type_property =
          transaction_.active_indices_->edge_type_properties_->ListIndices(transaction_.start_timestamp),
      .edge_property = transaction_.active_indices_->edge_property_->ListIndices(transaction_.start_timestamp),
      .vertex_property = transaction_.active_indices_->vertex_property_->ListIndices(transaction_.start_timestamp),
      .text_indices = transaction_.active_indices_->text_->ListIndices(),
      .text_edge_indices = transaction_.active_indices_->text_edge_->ListIndices(),
      .point_label_property = transaction_.active_indices_->point_->ListIndices(),
      .vector_indices_spec = transaction_.active_indices_->vector_->ListIndices(),
      .vector_edge_indices_spec = transaction_.active_indices_->vector_edge_->ListIndices()};
}

ConstraintsInfo InMemoryStorage::InMemoryAccessor::ListAllConstraints() const {
  return {.existence = transaction_.active_constraints_->existence_->ListConstraints(transaction_.start_timestamp),
          .unique = transaction_.active_constraints_->unique_->ListConstraints(transaction_.start_timestamp),
          .type = transaction_.active_constraints_->type_->ListConstraints(transaction_.start_timestamp)};
}

void InMemoryStorage::InMemoryAccessor::DropAllIndexes() {
  auto indices_info = ListAllIndices();

  static_cast<InMemoryStorage *>(storage_)->async_indexer_.Clear();

  for (const auto &label_id : indices_info.label) {
    [[maybe_unused]] auto maybe_error = DropIndex(label_id);
  }

  for (auto &entry : indices_info.label_properties) {
    [[maybe_unused]] auto maybe_error = DropIndex(entry.label, std::move(entry.properties));
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

  for (const auto &property_id : indices_info.vertex_property) {
    [[maybe_unused]] auto maybe_error = DropGlobalVertexIndex(property_id);
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
  transaction_.md_deltas.emplace_back(
      MetadataDelta::label_property_index_stats_set, label, std::vector(properties.begin(), properties.end()), stats);
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
  // Free deleted light Edge* referenced only by un-GC'd RECREATE_OBJECT
  // deltas before clearing committed_transactions_/waiting_gc_deltas_ (which hold
  // those delta chains). Mirrors Clear()'s harvest call; ClearLightEdges below
  // only drains {adjacency, graveyard, deleted_edges_} — delta-chain-only edges
  // are disjoint from those three sets and would leak without this harvest.
  if (mem_storage->config_.salient.items.storage_light_edge) {
    mem_storage->HarvestDeltaChainOnlyLightEdges();
  }
  mem_storage->committed_transactions_.WithLock([&](auto &committed_transactions) { committed_transactions.clear(); });

  mem_storage->async_indexer_.Clear();

  // also, we're the only transaction running, so we can safely remove the data as well
  mem_storage->indices_.DropGraphClearIndices();
  mem_storage->constraints_.DropGraphClearConstraints();

  if (mem_storage->config_.salient.items.enable_schema_info) mem_storage->schema_info_.Clear();
  // Free live light edges before clearing vertices (analytical DROP GRAPH).
  if (mem_storage->config_.salient.items.storage_light_edge) {
    mem_storage->ClearLightEdges();
  }

  mem_storage->vertices_.clear();
  mem_storage->waiting_gc_deltas_->clear();
  mem_storage->edges_.clear();
  mem_storage->edge_count_.store(0, std::memory_order_release);
  mem_storage->description_store_.Clear();

  memory::PurgeUnusedMemory();
}

auto InMemoryStorage::InMemoryAccessor::PointVertices(LabelId label, PropertyId property, CoordinateReferenceSystem crs,
                                                      PropertyValue const &point_value,
                                                      PropertyValue const &boundary_value,
                                                      PointDistanceCondition condition) -> PointIterable {
  return transaction_.point_index_ctx_.PointVertices(
      label, property, crs, storage_, &transaction_, point_value, boundary_value, condition);
}

std::vector<std::tuple<VertexAccessor, double, double>> InMemoryStorage::InMemoryAccessor::VectorIndexSearchOnNodes(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  auto *mem_storage = static_cast<InMemoryStorage *>(storage_);
  std::vector<std::tuple<VertexAccessor, double, double>> result;

  // we have to take vertices accessor to be sure no vertex is deleted while we are searching
  auto acc = mem_storage->vertices_.access();
  const auto search_results = storage_->indices_.vector_index_.SearchNodes(
      index_name, number_of_results, vector, mem_storage->name_id_mapper_.get());
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
  const auto search_results = storage_->indices_.vector_edge_index_.SearchEdges(index_name, number_of_results, vector);
  std::transform(search_results.begin(), search_results.end(), std::back_inserter(result), [&](const auto &item) {
    const auto &[entry, distance, score] = item;
    return std::make_tuple(
        EdgeAccessor{EdgeRef{entry.edge}, entry.edge_type, entry.from_vertex, entry.to_vertex, storage_, &transaction_},
        distance,
        score);
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
  return transaction_.point_index_ctx_.PointVertices(
      label, property, crs, storage_, &transaction_, bottom_left, top_right, condition);
}

}  // namespace memgraph::storage
