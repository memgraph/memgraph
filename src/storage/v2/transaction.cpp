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

#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

AutoIndexHelper::AutoIndexHelper(Config const &config, ActiveIndices const &active_indices, uint64_t start_timestamp) {
  if (config.salient.items.enable_label_index_auto_creation) {
    label_index_.emplace(AutoIndexHelper::LabelIndexInfo{
        .existing_ = active_indices.label_->ListIndices(start_timestamp) | ranges::to<absl::flat_hash_set>});
  }
  if (config.salient.items.enable_edge_type_index_auto_creation) {
    edgetype_index_.emplace(AutoIndexHelper::EdgeTypeIndexInfo{
        .existing_ = active_indices.edge_type_->ListIndices(start_timestamp) | ranges::to<absl::flat_hash_set>});
  }
}

void AutoIndexHelper::DispatchRequests(AutoIndexer &auto_indexer) {
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  if (label_index_ && !label_index_->requested_.empty()) {
    for (auto label : label_index_->requested_) {
      auto_indexer.Enqueue(label);
    }
  }
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  if (edgetype_index_ && !edgetype_index_->requested_.empty()) {
    for (auto edge_type : edgetype_index_->requested_) {
      auto_indexer.Enqueue(edge_type);
    }
  }
}
}  // namespace memgraph::storage
