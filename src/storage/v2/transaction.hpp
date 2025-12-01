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

#pragma once

#include <atomic>
#include <memory>
#include <optional>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/schema_info.hpp"
#include "utils/memory.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/skip_list.hpp"

#include "delta_container.hpp"
#include "storage/v2/async_indexer.hpp"
#include "storage/v2/constraint_verification_info.hpp"
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

#include <rocksdb/utilities/transaction.h>

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
              StorageMode storage_mode, bool edge_import_mode_active, bool has_constraints,
              PointIndexContext point_index_ctx, ActiveIndices active_indices, AsyncIndexHelper async_index_helper = {},
              std::optional<uint64_t> last_durable_ts = std::nullopt)
      : transaction_id(transaction_id),
        start_timestamp(start_timestamp),
        command_id(0),
        md_deltas(utils::NewDeleteResource()),
        has_serialization_error(false),
        isolation_level(isolation_level),
        storage_mode(storage_mode),
        edge_import_mode_active(edge_import_mode_active),
        constraint_verification_info{(has_constraints) ? std::optional<ConstraintVerificationInfo>{std::in_place}
                                                       : std::nullopt},
        vertices_{(storage_mode == StorageMode::ON_DISK_TRANSACTIONAL)
                      ? std::optional<utils::SkipList<Vertex>>{std::in_place}
                      : std::nullopt},
        edges_{(storage_mode == StorageMode::ON_DISK_TRANSACTIONAL)
                   ? std::optional<utils::SkipList<Edge>>{std::in_place}
                   : std::nullopt},
        point_index_ctx_{std::move(point_index_ctx)},
        point_index_change_collector_{point_index_ctx_},
        last_durable_ts_{last_durable_ts},
        active_indices_{std::move(active_indices)},
        async_index_helper_(std::move(async_index_helper)) {}

  Transaction(Transaction &&other) noexcept = default;

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() = default;

  bool IsDiskStorage() const { return storage_mode == StorageMode::ON_DISK_TRANSACTIONAL; }

  /// @throw std::bad_alloc if failed to create the `commit_timestamp`
  void EnsureCommitTimestampExists() {
    if (commit_timestamp != nullptr) return;
    commit_timestamp = std::make_unique<std::atomic<uint64_t>>(transaction_id);
  }

  bool AddModifiedEdge(Gid gid, ModifiedEdgeInfo modified_edge) {
    return modified_edges_.emplace(gid, modified_edge).second;
  }

  bool RemoveModifiedEdge(const Gid &gid) { return modified_edges_.erase(gid) > 0U; }

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

  uint64_t transaction_id{};
  uint64_t start_timestamp{};
  std::optional<uint64_t> original_start_timestamp{};
  // The `Transaction` object is stack allocated, but the `commit_timestamp`
  // must be heap allocated because `Delta`s have a pointer to it, and that
  // pointer must stay valid after the `Transaction` is moved into
  // `commited_transactions_` list for GC.
  std::unique_ptr<std::atomic<uint64_t>> commit_timestamp{};
  uint64_t command_id{};

  delta_container deltas;
  utils::pmr::list<MetadataDelta> md_deltas;
  bool has_serialization_error{};
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
  rocksdb::Transaction *disk_transaction_{};
  /// Main storage
  std::optional<utils::SkipList<Vertex>> vertices_{};
  std::vector<std::unique_ptr<utils::SkipList<Vertex>>> index_storage_{};

  /// We need them because query context for indexed reading is cleared after the query is done not after the
  /// transaction is done
  std::vector<delta_container> index_deltas_storage_{};
  std::optional<utils::SkipList<Edge>> edges_{};
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
  utils::QueryMemoryTracker query_memory_tracker_{};

  /// Text index change tracking (batched apply on commit)
  TextIndexChangeCollector text_index_change_collector_;

  /// Text edge index change tracking (batched apply on commit)
  TextEdgeIndexChangeCollector text_edge_index_change_collector_;

  /// Last durable timestamp at the moment of transaction creation
  std::optional<uint64_t> last_durable_ts_;

  /// Concurrent safe indices that existed at the beginning of the transaction
  /// Used to insert new entries, and during planning to speed up scans
  ActiveIndices active_indices_;
  CommitCallbacks commit_callbacks_;

  /// Auto indexing infomation gathering
  AsyncIndexHelper async_index_helper_;
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
