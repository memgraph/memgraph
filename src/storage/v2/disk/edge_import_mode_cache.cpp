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

#include "storage/v2/disk//edge_import_mode_cache.hpp"
#include <algorithm>
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/algorithm.hpp"
#include "utils/disk_utils.hpp"

namespace memgraph::storage {

EdgeImportModeCache::EdgeImportModeCache(const Config &config)
    : in_memory_indices_(Indices(config, StorageMode::IN_MEMORY_TRANSACTIONAL)) {}

InMemoryLabelIndex::Iterable EdgeImportModeCache::Vertices(LabelId label, View view, Storage *storage,
                                                           Transaction *transaction) const {
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory_indices_.label_index_.get());
  return mem_label_index->Vertices(label, view, storage, transaction);
}

InMemoryLabelPropertyIndex::Iterable EdgeImportModeCache::Vertices(
    LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) const {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory_indices_.label_property_index_.get());
  return mem_label_property_index->Vertices(label, property, lower_bound, upper_bound, view, storage, transaction);
}

bool EdgeImportModeCache::CreateIndex(
    LabelId label, PropertyId property,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory_indices_.label_property_index_.get());
  bool res = mem_label_property_index->CreateIndex(label, property, vertices_.access(), parallel_exec_info);
  if (res) {
    scanned_label_properties_.insert({label, property});
  }
  return res;
}

bool EdgeImportModeCache::CreateIndex(
    LabelId label, const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory_indices_.label_index_.get());
  bool res = mem_label_index->CreateIndex(label, vertices_.access(), parallel_exec_info);
  if (res) {
    scanned_labels_.insert(label);
  }
  return res;
}

bool EdgeImportModeCache::VerticesWithLabelPropertyScanned(LabelId label, PropertyId property) const {
  return VerticesWithLabelScanned(label) || utils::Contains(scanned_label_properties_, std::make_pair(label, property));
}

bool EdgeImportModeCache::VerticesWithLabelScanned(LabelId label) const {
  return AllVerticesScanned() || utils::Contains(scanned_labels_, label);
}

bool EdgeImportModeCache::AllVerticesScanned() const { return scanned_all_vertices_; }

utils::SkipList<Vertex>::Accessor EdgeImportModeCache::AccessToVertices() { return vertices_.access(); }

utils::SkipList<Edge>::Accessor EdgeImportModeCache::AccessToEdges() { return edges_.access(); }

void EdgeImportModeCache::SetScannedAllVertices() { scanned_all_vertices_ = true; }

utils::Synchronized<std::list<Transaction>, utils::SpinLock> &EdgeImportModeCache::GetCommittedTransactions() {
  return committed_transactions_;
}

}  // namespace memgraph::storage
