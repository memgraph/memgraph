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

#include <rocksdb/options.h>

#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

DiskLabelIndex::DiskLabelIndex(Indices *indices, Constraints *constraints, const Config &config)
    : LabelIndex(indices, constraints, config) {
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(config.disk.label_index_directory);
  kvstore_->options_.create_if_missing = true;
  logging::AssertRocksDBStatus(
      rocksdb::DB::Open(kvstore_->options_, config.disk.label_index_directory, &kvstore_->db_));
}

bool DiskLabelIndex::CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices) {
  index_.emplace(label);
  rocksdb::WriteOptions wo;
  for (const auto &[key, value] : vertices) {
    kvstore_->db_->Put(wo, key, value);
  }
  return true;
}

std::unique_ptr<rocksdb::Iterator> DiskLabelIndex::CreateRocksDBIterator() {
  return std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(rocksdb::ReadOptions()));
}

/// TODO: andi if the vertex is already indexed, we should update the entry, not create a new one.
void DiskLabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  if (!IndexExists(label)) {
    return;
  }
  /// TODO: andi This change should be done at the commit time not before and under some lock, probably RocksDB lock.
  std::string key = utils::SerializeVertexForLabelIndex(label, vertex->labels, vertex->gid);
  std::string value = utils::SerializeProperties(vertex->properties);
  kvstore_->db_->Put(rocksdb::WriteOptions(), key, value);
}

/// TODO: andi Here will come Bloom filter deletion
bool DiskLabelIndex::DropIndex(LabelId label) { return index_.erase(label) > 0; }

bool DiskLabelIndex::IndexExists(LabelId label) const { return index_.find(label) != index_.end(); }

std::vector<LabelId> DiskLabelIndex::ListIndices() const { return {index_.begin(), index_.end()}; }

void DiskLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  throw utils::NotYetImplemented("DiskLabelIndex::RemoveObsoleteEntries");
}

uint64_t DiskLabelIndex::ApproximateVertexCount(LabelId /*label*/) const {
  /// TODO: andi figure out something smarter.
  return 10;
}

void DiskLabelIndex::Clear() { index_.clear(); }

void DiskLabelIndex::RunGC() {
  // throw utils::NotYetImplemented("DiskLabelIndex::RunGC");
}

}  // namespace memgraph::storage
