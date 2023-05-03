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
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/rocksdb.hpp"
#include "utils/skip_list.hpp"
#include "utils/string.hpp"

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

AllDiskVerticesIterable LabelDiskIndex::Vertices(LabelId label, View view, Transaction *transaction) {
  /// TODO: (andi): How to solve issue with garbage collection of vertices?
  auto acc = vertices_.access();
  rocksdb::ReadOptions ro;
  rocksdb::Slice ts = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro));
  /// TODO: andi: No need to save all labels in the RocksDB, we can save only the first one and apply Bloom filter on
  /// it. Or do some kind of optimization.
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto &key = it->key().ToString();
    const auto vertex_parts = utils::Split(key, "|");
    if (const auto labels = utils::Split(vertex_parts[0], ",");
        // TODO: (andi): When you decouple SerializeIdType, modify this to_string call to use SerializeIdType.
        std::find(labels.begin(), labels.end(), std::to_string(label.AsUint())) != labels.end()) {
      auto gid = storage::Gid::FromUint(std::stoull(vertex_parts[1]));
      auto vertex_commit_ts = utils::ExtractTimestampFromDeserializedUserKey(key);
      auto delta = CreateDeleteDeserializedObjectDelta(transaction, vertex_commit_ts);
      spdlog::debug("Found vertex with gid {} and commit_ts {} in index", vertex_parts[1], vertex_commit_ts);
      auto [vertex_it, inserted] = acc.insert(DiskVertex{gid, delta});
      std::vector<LabelId> label_ids;
      if (!vertex_parts[0].empty()) {
        auto labels = utils::Split(vertex_parts[0], ",");
        std::transform(labels.begin(), labels.end(), std::back_inserter(label_ids),
                       [](const auto &label) { return storage::LabelId::FromUint(std::stoull(label)); });
      }
      vertex_it->labels = std::move(label_ids);
      /// TODO: (andi): Add support for deserialization vertices to indices.
      // vertex_tSetPropertyStore(it->value().ToStringView());

      /// if the vertex with the given gid doesn't exist on the disk, it must be inserted here.
      // MG_ASSERT(inserted, "The vertex must be inserted here!");
      // MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
    }
  }
  return {vertices_.access(), transaction, view, indices_, nullptr, config_};
}

/// TODO: andi: No need to save all labels in the RocksDB, we can save only the first one and apply Bloom filter on it.
bool LabelDiskIndex::CreateIndex(LabelId label,
                                 const std::vector<std::tuple<std::string, std::string, uint64_t>> &vertices) {
  index_.emplace_back(label);
  // Serialize vertices with the same timestamp (latest commit), they have been serialized with at the main storage.
  for (const auto &[key, value, commit_ts] : vertices) {
    rocksdb::WriteOptions write_options;
    rocksdb::Slice ts = utils::StringTimestamp(commit_ts);
    write_options.timestamp = &ts;
    logging::AssertRocksDBStatus(kvstore_->db_->Put(write_options, key, value));
  }
  return true;
}

bool LabelDiskIndex::DropIndex(LabelId label) { throw utils::NotYetImplemented("LabelIndex::DropIndex"); }

bool LabelDiskIndex::IndexExists(LabelId label) const {
  return std::find(index_.begin(), index_.end(), label) != index_.end();
}

std::vector<LabelId> LabelDiskIndex::ListIndices() const { throw utils::NotYetImplemented("LabelIndex::ListIndices"); }

int64_t LabelDiskIndex::ApproximateVertexCount(LabelId label) const { return 1; }

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
