// Copyright 2024 Memgraph Ltd.
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
#include <limits>
#include <memory>

#include "storage/v2/id_types.hpp"
#include "utils/memory.hpp"
#include "utils/skip_list.hpp"

#include "storage/v2/constraint_verification_info.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/metadata_delta.hpp"
#include "storage/v2/modified_edge.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "storage/v2/view.hpp"
#include "utils/bond.hpp"
#include "utils/pmr/list.hpp"

#include <rocksdb/utilities/transaction.h>

namespace memgraph::storage {

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

struct Transaction {
  Transaction(uint64_t transaction_id, uint64_t start_timestamp, IsolationLevel isolation_level,
              StorageMode storage_mode, bool edge_import_mode_active, bool has_constraints)
      : transaction_id(transaction_id),
        start_timestamp(start_timestamp),
        command_id(0),
        md_deltas(utils::NewDeleteResource()),
        must_abort(false),
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
                   : std::nullopt} {}

  Transaction(Transaction &&other) noexcept = default;

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = default;

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

  uint64_t transaction_id{};
  uint64_t start_timestamp{};
  // The `Transaction` object is stack allocated, but the `commit_timestamp`
  // must be heap allocated because `Delta`s have a pointer to it, and that
  // pointer must stay valid after the `Transaction` is moved into
  // `commited_transactions_` list for GC.
  std::unique_ptr<std::atomic<uint64_t>> commit_timestamp{};
  uint64_t command_id{};

  std::deque<Delta> deltas;
  utils::pmr::list<MetadataDelta> md_deltas;
  bool must_abort{};
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
  std::vector<std::list<Delta>> index_deltas_storage_{};
  std::optional<utils::SkipList<Edge>> edges_{};
  std::map<std::string, std::pair<std::string, std::string>> edges_to_delete_{};
  std::map<std::string, std::string> vertices_to_delete_{};
  bool scanned_all_vertices_ = false;
  std::set<LabelId> introduced_new_label_index_;
  std::set<EdgeTypeId> introduced_new_edge_type_index_;
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
