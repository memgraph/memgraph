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

#pragma once

#include <atomic>
#include <exception>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/schema_info.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/skip_list.hpp"

#include "delta_container.hpp"
#include "storage/v2/async_indexer.hpp"
#include "storage/v2/constraint_verification_info.hpp"
#include "storage/v2/constraints/active_constraints.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/point_index_change_collector.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/metadata_delta.hpp"
#include "storage/v2/modified_edge.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "utils/pmr/list.hpp"

namespace rocksdb {
class Transaction;
}  // namespace rocksdb

namespace memgraph::storage {

struct CommitCallbacks {
  using func_t = std::function<void(uint64_t)>;

  void Add(func_t callback) { callbacks_.emplace_back(std::move(callback)); }

  void RunAll(uint64_t commit_timestamp) {
    for (auto &callback : callbacks_) {
      callback(commit_timestamp);
    }
    callbacks_.clear();
  }

  std::vector<std::function<void(uint64_t)>> callbacks_;
};

/// Mirror of CommitCallbacks. Each site that adds a commit_callback typically
/// adds a paired abort_callback that undoes the same eager mutation; commit
/// runs commit_callbacks and discards abort_callbacks, abort the reverse.
struct AbortCallbacks {
  using func_t = std::function<void()>;

  void Add(func_t callback) { callbacks_.emplace_back(std::move(callback)); }

  // noexcept: a throwing callback here will std::terminate. In practice this
  // is fine — abort runs with the soft memory tracker disabled (no
  // OutOfMemoryExceptionEnabler in scope), so OOM can't fire from these
  // callbacks, and a true system bad_alloc means we're going down anyway.
  void RunAll() noexcept {
    for (auto &callback : callbacks_) {
      callback();
    }
    callbacks_.clear();
  }

  void Clear() noexcept { callbacks_.clear(); }

  std::vector<func_t> callbacks_;
};

struct AsyncIndexHelper {
  AsyncIndexHelper() = default;
  AsyncIndexHelper(Config const &config, ActiveIndices const &active_indices, uint64_t start_timestamp);

  // Perf: keep this code inlinable
  void Track(LabelId label) {
    if (label_index_ && !label_index_->existing_.contains(label)) {
      label_index_->requested_.insert(label);
      // optimisation: this prevent redundant insertions requested_
      label_index_->existing_.insert(label);
    }
  }

  // Perf: keep this code inlinable
  void Track(EdgeTypeId edge_type) {
    if (edgetype_index_ && !edgetype_index_->existing_.contains(edge_type)) {
      edgetype_index_->requested_.insert(edge_type);
      // optimisation: this prevent redundant insertions requested_
      edgetype_index_->existing_.insert(edge_type);
    }
  }

  void DispatchRequests(AsyncIndexer &async_indexer);

 private:
  struct LabelIndexInfo {
    absl::flat_hash_set<LabelId> existing_;
    absl::flat_hash_set<LabelId> requested_;
  };

  struct EdgeTypeIndexInfo {
    absl::flat_hash_set<EdgeTypeId> existing_;
    absl::flat_hash_set<EdgeTypeId> requested_;
  };

  std::optional<LabelIndexInfo> label_index_ = std::nullopt;
  std::optional<EdgeTypeIndexInfo> edgetype_index_ = std::nullopt;
};

struct Transaction {
  Transaction(uint64_t transaction_id, uint64_t start_timestamp, IsolationLevel isolation_level,
              StorageMode storage_mode, bool edge_import_mode_active, PointIndexContext point_index_ctx,
              ActiveIndicesPtr active_indices, ActiveConstraintsPtr active_constraints,
              AsyncIndexHelper async_index_helper = {}, std::optional<uint64_t> last_durable_ts = std::nullopt,
              metrics::GaugeHandle unreleased_delta_gauge = {})
      : transaction_id(transaction_id),
        start_timestamp(start_timestamp),
        original_start_timestamp(start_timestamp),
        command_id(0),
        deltas(unreleased_delta_gauge),
        md_deltas(utils::NewDeleteResource()),
        has_serialization_error(false),
        isolation_level(isolation_level),
        storage_mode(storage_mode),
        edge_import_mode_active(edge_import_mode_active),
        constraint_verification_info{(active_constraints && !active_constraints->empty())
                                         ? std::optional<ConstraintVerificationInfo>{std::in_place}
                                         : std::nullopt},
        vertices_{(storage_mode == StorageMode::ON_DISK_TRANSACTIONAL)
                      ? std::optional<utils::SkipListDb<Vertex>>{std::in_place}
                      : std::nullopt},
        edges_{(storage_mode == StorageMode::ON_DISK_TRANSACTIONAL)
                   ? std::optional<utils::SkipListDb<Edge>>{std::in_place}
                   : std::nullopt},
        point_index_ctx_{std::move(point_index_ctx)},
        point_index_change_collector_{point_index_ctx_},
        query_memory_tracker_{},
        last_durable_ts_{last_durable_ts},
        active_indices_{std::move(active_indices)},
        active_constraints_{std::move(active_constraints)},
        async_index_helper_(std::move(async_index_helper)) {}

  Transaction(Transaction &&other) noexcept = default;

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() = default;

  bool IsDiskStorage() const { return storage_mode == StorageMode::ON_DISK_TRANSACTIONAL; }

  /// @throw std::bad_alloc if failed to create the `commit_info`
  void EnsureCommitInfoExists() {
    // LATENT (R16): a historical (read-only, InMemoryStorage::HistoricalAccess) accessor has no
    // type-level enforcement against mutation, so gate the write surface here instead of at every
    // individual accessor method. This is the single narrowest chokepoint EVERY delta creation
    // funnels through -- CreateDeleteObjectDelta (new vertex/edge), CreateAndLinkDelta (label/prop
    // modify), CreateDeleteDeserializedObjectDelta (disk/replica deserialize-delete), and the
    // metadata-delta commit path in PrepareForCommitPhase all call this first (mvcc.hpp / inmemory
    // storage.cpp). NOTE: pure metadata/schema-DDL mutations (CreateEnum/EnumAlter*/description/TTL
    // in storage.hpp) push directly to `md_deltas` WITHOUT going through this call and are NOT
    // covered here -- they already require `unique_guard_.owns_lock()` (a HistoricalAccess accessor
    // never holds unique_guard_), and full DDL rejection for branches is chunk 8's job.
    // Deliberately checks ONLY is_historical_, not is_recovery_replay_: a recovery-replay transaction
    // (CreateRecoveryReplayTransaction) is write-capable by design -- it replays real deltas during
    // ctor-time recovery -- and always has is_historical_ == false, so it never trips this assert.
    MG_ASSERT(!is_historical_, "Attempted a write through a historical (read-only) accessor");
    if (commit_info != nullptr) return;
    commit_info = std::make_unique<CommitInfo>(transaction_id);
  }

  bool AddModifiedEdge(Gid gid, ModifiedEdgeInfo modified_edge) {
    return modified_edges_.emplace(gid, modified_edge).second;
  }

  bool RemoveModifiedEdge(const Gid &gid) { return modified_edges_.erase(gid) > 0U; }

  struct EdgeSetPropertyInfo {
    Gid in_vertex_gid = kInvalidGid;
    EdgeTypeId edge_type_id = kInvalidEdgeTypeId;
  };

  void RecordEdgeSetPropertyInfo(Gid edge_gid, Gid in_vertex_gid, EdgeTypeId edge_type_id) {
    edge_set_property_info_[edge_gid] = EdgeSetPropertyInfo{in_vertex_gid, edge_type_id};
  }

  EdgeSetPropertyInfo GetEdgeSetPropertyInfo(Gid edge_gid) const {
    auto it = edge_set_property_info_.find(edge_gid);
    if (it == edge_set_property_info_.end()) return {};
    return it->second;
  }

  void UpdateOnChangeLabel(LabelId label, Vertex *vertex) {
    point_index_change_collector_.UpdateOnChangeLabel(label, vertex);
    manyDeltasCache.Invalidate(vertex, label);
  }

  void UpdateOnSetProperty(PropertyId property, const PropertyValue &old_value, const PropertyValue &new_value,
                           Vertex *vertex) {
    point_index_change_collector_.UpdateOnSetProperty(property, old_value, new_value, vertex);
    manyDeltasCache.Invalidate(vertex, property);
  }

  void UpdateOnVertexDelete(Vertex *vertex) {
    point_index_change_collector_.UpdateOnVertexDelete(vertex);
    manyDeltasCache.Invalidate(vertex);
  }

  void SetParallelExecution() { parallel_execution_ = true; }

  bool UseCache() const { return isolation_level == IsolationLevel::SNAPSHOT_ISOLATION && !parallel_execution_; }

  uint64_t transaction_id{};
  uint64_t start_timestamp{};
  // Set at construction; never reassigned. Stable across PeriodicCommit.
  uint64_t original_start_timestamp{};
  // True only for a read-only historical-timestamp accessor (graph versioning R16,
  // InMemoryStorage::HistoricalAccess): `start_timestamp` is a PAST fork_ts captured and retained
  // by a live RegisterForkPin (inmemory/storage.cpp:2873), not a freshly issued `timestamp_++` tick.
  // Because that exact numeric tick is later dispensed for real to whatever transaction is created
  // next (RegisterForkPin only peeks `timestamp_`, it does not consume it), finalization paths
  // (Abort / the empty-delta PrepareForCommitPhase fast path / FinalizeCommitPhase) must NEVER call
  // `commit_log_->MarkFinished(start_timestamp)` for such a transaction (R37): doing so would either
  // prematurely advance commit_log_->OldestActive() past a still-needed boundary, or double-mark the
  // same id once its real, later-dispensed owner also finishes. That finalization is owned exclusively
  // by whoever the tick is genuinely dispensed to (a real transaction) and, for GC-retention purposes,
  // by the fork pin itself (ReleaseForkPin, at DROP BRANCH) via the separate version_fork_pins_ set --
  // never by a transient per-query historical reader. Default false leaves every pre-existing
  // transaction/accessor unchanged.
  bool is_historical_{false};
  // Recovery-replay (branch-durability windowed recovery, graph versioning v1 S3a/S3c/S3d): flags a
  // Transaction created by InMemoryStorage::CreateRecoveryReplayTransaction. Unlike is_historical_,
  // this transaction IS write-capable -- it replays already-durable WAL entries during single-threaded
  // ctor-time recovery, so it genuinely creates deltas (CreateVertex/SetProperty/etc.) and therefore
  // must NOT trip EnsureCommitInfoExists()'s `MG_ASSERT(!is_historical_, ...)` (it doesn't: that assert
  // only checks is_historical_, which stays false for a replay transaction, by design).
  //
  // What it shares with is_historical_: `start_timestamp` is a HISTORICAL value (a past WAL commit
  // timestamp, `Ci - 1`), not a freshly issued `timestamp_++` tick. The commit_log_'s block arithmetic
  // (CommitLog::FindOrCreateBlock/MarkFinished) has no bounds check against `head_start_` -- calling
  // `commit_log_->MarkFinished(start_timestamp)` for an id BELOW head_start_ silently returns the
  // CURRENT head block and flips a bit at `head_start_ + (start_timestamp % kIdsInBlock)`, i.e. it
  // marks some unrelated, likely-future id as finished instead of (or in addition to not marking)
  // anything meaningful for start_timestamp -- corrupting commit_log_->OldestActive() and, downstream,
  // the GC/MVCC horizon. Recovery is single-threaded and reseeds the commit_log en-masse AFTER replay
  // (a blanket MarkFinishedInRange over the whole recovered window), so per-transaction commit_log
  // active-transaction tracking is meaningless (and actively harmful) DURING replay.
  //
  // Consequently, every finalize path that would otherwise call
  // `commit_log_->MarkFinished(start_timestamp)` (the empty-delta PrepareForCommitPhase fast path,
  // FinalizeCommitPhase, and Abort()) must skip that call when `is_recovery_replay_` is set -- exactly
  // like the is_historical_ guard already there, but WITHOUT taking the ReleaseForkPin branch (a replay
  // transaction owns no fork pin; recovery's own pin/seed bookkeeping is separate, see
  // CreateRecoveryReplayTransaction's doc-comment in storage.hpp). All OTHER finalize bookkeeping for a
  // replay transaction -- GC delta registration (waiting_gc_deltas_/committed_transactions_), index/
  // schema-update processing, `is_transaction_active_ = false`, LDT updates -- still runs normally: the
  // replayed deltas must remain visible and GC-tracked exactly like a normal commit's. Distinct from
  // is_historical_ (a read-only fork-pin reader that never writes); default false leaves every
  // pre-existing transaction/accessor unchanged.
  bool is_recovery_replay_{false};
  // The `Transaction` object is stack allocated, but the `commit_info`
  // must be heap allocated because `Delta`s have a pointer to it, and that
  // pointer must stay valid after the `Transaction` is moved into
  // `commited_transactions_` list for GC.
  std::unique_ptr<CommitInfo> commit_info{};
  uint64_t command_id{};

  delta_container deltas;
  utils::pmr::list<MetadataDelta> md_deltas;
  bool has_serialization_error{};
  bool has_non_sequential_deltas{};
  IsolationLevel isolation_level{};
  StorageMode storage_mode{};
  bool edge_import_mode_active{false};

  // A cache which is consistent to the current transaction_id + command_id.
  // Used to speedup getting info about a vertex when there is a long delta
  // chain involved in rebuilding that info.
  mutable VertexInfoCache manyDeltasCache{};
  mutable std::optional<ConstraintVerificationInfo> constraint_verification_info{};

  // Store modified edges GID mapped to changed Delta and serialized edge key
  // Only for disk storage
  ModifiedEdgesMap modified_edges_{};
  std::unordered_map<Gid, EdgeSetPropertyInfo> edge_set_property_info_{};
  rocksdb::Transaction *disk_transaction_{};
  /// Main storage
  std::optional<utils::SkipListDb<Vertex>> vertices_{};
  std::vector<std::unique_ptr<utils::SkipListDb<Vertex>>> index_storage_{};

  /// We need them because query context for indexed reading is cleared after the query is done not after the
  /// transaction is done
  std::vector<delta_container> index_deltas_storage_{};
  std::optional<utils::SkipListDb<Edge>> edges_{};
  std::map<std::string, std::pair<std::string, std::string>, std::less<>> edges_to_delete_{};
  std::map<std::string, std::string, std::less<>> vertices_to_delete_{};
  bool scanned_all_vertices_ = false;
  /// Hold point index relevant to this txn+command
  PointIndexContext point_index_ctx_;
  /// Tracks changes relevant to point index (used during Commit/AdvanceCommand)
  PointIndexChangeCollector point_index_change_collector_;
  /// Tracking schema changes done during the transaction
  LocalSchemaTracking schema_diff_;
  SchemaInfoPostProcess post_process_;

  /// Query memory tracker
  utils::QueryMemoryTracker query_memory_tracker_;

  /// Text index change tracking (batched apply on commit)
  TextIndexChangeCollector text_index_change_collector_;

  /// Text edge index change tracking (batched apply on commit)
  TextEdgeIndexChangeCollector text_edge_index_change_collector_;

  /// Pinned tantivy searcher cache for snapshot-consistent text index reads within this transaction
  mutable std::unique_ptr<TextSearchSession> text_search_session_;

  /// Last durable timestamp at the moment of transaction creation
  std::optional<uint64_t> last_durable_ts_;

  /// Concurrent safe indices that existed at the beginning of the transaction
  /// Used to insert new entries, and during planning to speed up scans
  ActiveIndicesPtr active_indices_;
  /// Concurrent safe constraints that existed at the beginning of the transaction
  /// Used for constraint validation during commit
  ActiveConstraintsPtr active_constraints_;
  CommitCallbacks commit_callbacks_;
  /// Rollback hooks for eager DDL owner-side mutations. Runs on Abort(),
  /// cleared on successful commit (see commit_callbacks_.RunAll site).
  AbortCallbacks abort_callbacks_;

  /// Auto indexing infomation gathering
  AsyncIndexHelper async_index_helper_;

  bool parallel_execution_{false};  // For now just disable for the whole query
};

inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.transaction_id == second.transaction_id;
}

inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.transaction_id < second.transaction_id;
}

inline bool operator==(const Transaction &first, const uint64_t &second) { return first.transaction_id == second; }

inline bool operator<(const Transaction &first, const uint64_t &second) { return first.transaction_id < second; }

}  // namespace memgraph::storage
