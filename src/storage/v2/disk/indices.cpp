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

// LABEL INDEX

#include "storage/v2/disk/indices.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"

namespace memgraph::storage {

namespace {
constexpr const char *label_index_path = "rocksdb_label_index";
constexpr const char *label_property_index_path = "rocksdb_label_property_index";

}  // namespace

// ----------------------------------------------------------------------------------------------
// LABEL_DISK_INDEX METHODS

LabelDiskIndex::LabelDiskIndex(DiskIndices *indices, Config config) : indices_(indices), config_(config) {
  std::filesystem::path rocksdb_path = label_index_path;
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(rocksdb_path);
  kvstore_->options_.create_if_missing = true;
  // kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::DB::Open(kvstore_->options_, rocksdb_path, &kvstore_->db_));
}

bool LabelDiskIndex::CreateIndex(
    LabelId label, const std::vector<std::tuple<std::string_view, std::string_view, uint64_t>> &vertices) {
  index_.emplace_back(label);
  // Serialize vertices with the same timestamp (latest commit), they have been serialized with at the main storage.
  for (const auto &[key, value, commit_ts] : vertices) {
    rocksdb::WriteOptions write_options;
    // rocksdb::Slice ts = Timestamp(commit_ts);
    // write_options.timestamp = &ts;
    logging::AssertRocksDBStatus(kvstore_->db_->Put(write_options, key, value));
  }
  return true;
}

bool LabelDiskIndex::DropIndex(LabelId label) { throw utils::NotYetImplemented("LabelIndex::DropIndex"); }

bool LabelDiskIndex::IndexExists(LabelId label) const { throw utils::NotYetImplemented("LabelIndex::IndexExists"); }

std::vector<LabelId> LabelDiskIndex::ListIndices() const { throw utils::NotYetImplemented("LabelIndex::ListIndices"); }

int64_t LabelDiskIndex::ApproximateVertexCount(LabelId label) const {
  throw utils::NotYetImplemented("LabelIndex::ApproximateVertexCount");
}

/// Clear all indexed vertices from the disk
void LabelDiskIndex::Clear() { throw utils::NotYetImplemented("LabelIndex::Clear"); }

/// TODO: Maybe we can remove completely interaction with garbage collector
void LabelDiskIndex::RunGC() { throw utils::NotYetImplemented("LabelIndex::RunGC"); }

// ----------------------------------------------------------------------------------------------
// LABEL_PROPERTY_DISK_INDEX METHODS

LabelPropertyDiskIndex::LabelPropertyDiskIndex(DiskIndices *indices, Config config)
    : indices_(indices), config_(config) {
  std::filesystem::path rocksdb_path = label_property_index_path;
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(rocksdb_path);
  kvstore_->options_.create_if_missing = true;
  // kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::DB::Open(kvstore_->options_, rocksdb_path, &kvstore_->db_));
}

bool LabelPropertyDiskIndex::CreateIndex(LabelId label, PropertyId property) {
  throw utils::NotYetImplemented("LabelPropertyIndex::CreateIndex");
}

bool LabelPropertyDiskIndex::DropIndex(LabelId label, PropertyId property) {
  throw utils::NotYetImplemented("LabelPropertyIndex::DropIndex");
}

bool LabelPropertyDiskIndex::IndexExists(LabelId label, PropertyId property) const {
  throw utils::NotYetImplemented("LabelPropertyIndex::IndexExists");
}

std::vector<std::pair<LabelId, PropertyId>> LabelPropertyDiskIndex::ListIndices() const {
  throw utils::NotYetImplemented("LabelPropertyIndex::ListIndices");
}

int64_t LabelPropertyDiskIndex::ApproximateVertexCount(LabelId label, PropertyId property) const {
  throw utils::NotYetImplemented("LabelPropertyIndex::ApproximateVertexCount");
}

/// Clear all indexed vertices from the disk
void LabelPropertyDiskIndex::Clear() { throw utils::NotYetImplemented("LabelPropertyIndex::Clear"); }

/// TODO: Maybe we can remove completely interaction with garbage collector
void LabelPropertyDiskIndex::RunGC() { throw utils::NotYetImplemented("LabelPropertyIndex::RunGC"); }

}  // namespace memgraph::storage
