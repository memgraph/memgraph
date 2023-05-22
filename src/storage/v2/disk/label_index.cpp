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

#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb.hpp"

namespace memgraph::storage {

namespace {
constexpr const char *label_index_path = "rocksdb_label_index";
}  // namespace

DiskLabelIndex::DiskLabelIndex(Indices *indices, Constraints *constraints, Config::Items config)
    : LabelIndex(indices, constraints, config) {
  std::filesystem::path rocksdb_path = label_index_path;
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(rocksdb_path);
  kvstore_->options_.create_if_missing = true;
  //   // kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::DB::Open(kvstore_->options_, rocksdb_path, &kvstore_->db_));
}

void DiskLabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  auto it = index_.find(label);
  if (it == index_.end()) return;
}

bool DiskLabelIndex::CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices) {
  index_.emplace(label);
  for (const auto &[key, value] : vertices) {
    kvstore_->db_->Put(rocksdb::WriteOptions(), key, value);
  }
  return true;
}

std::vector<LabelId> DiskLabelIndex::ListIndices() const { return std::vector<LabelId>(index_.begin(), index_.end()); }

void DiskLabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  throw utils::NotYetImplemented("DiskLabelIndex::RemoveObsoleteEntries");
}

int64_t DiskLabelIndex::ApproximateVertexCount(LabelId /*label*/) const {
  /// TODO: andi figure out something smarter.
  return 10;
}

DiskLabelIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, nullptr, self_->config_),
      current_vertex_(nullptr) {}

DiskLabelIndex::Iterable::Iterator &DiskLabelIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  return *this;
}
DiskLabelIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label, View view,
                                   Transaction *transaction, Indices *indices, Constraints *constraints,
                                   Config::Items config)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      constraints_(constraints),
      config_(config) {}

void DiskLabelIndex::RunGC() {
  // throw utils::NotYetImplemented("DiskLabelIndex::RunGC");
}

}  // namespace memgraph::storage
