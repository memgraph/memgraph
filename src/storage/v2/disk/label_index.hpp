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

#include <rocksdb/iterator.h>
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

using ParalellizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;
class DiskLabelIndex : public storage::LabelIndex {
 public:
  DiskLabelIndex(Indices *indices, Constraints *constraints, Config::Items config);

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) override;

  /// Key: INDEX_LABEL,OTHER_LABEL_1,OTHER_LABEL_2, ..|GID
  /// Value: VERTEX.PROPERTIES
  /// TODO: andi Whenever vertex is updated you should go to the disk if it is indexed.
  /// Optimize by using prefixed Bloom filters
  bool CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices);

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) override;

  bool IndexExists(LabelId label) const override;

  std::vector<LabelId> ListIndices() const override;

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) override;

  int64_t ApproximateVertexCount(LabelId label) const override;

  std::unique_ptr<rocksdb::Iterator> CreateRocksDBIterator() {
    return std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(rocksdb::ReadOptions()));
  }

  void Clear() override;

  void RunGC() override;

 private:
  std::unordered_set<LabelId> index_;
  Indices *indices_;  /// TODO: andi maybe you can remove a pointer to indices_
  Constraints *constraints_;
  Config config_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
