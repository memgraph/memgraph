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

#pragma once

#include <rocksdb/iterator.h>
#include <rocksdb/utilities/transaction.h>

#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {
class DiskLabelIndex : public storage::LabelIndex {
 public:
  explicit DiskLabelIndex(const Config &config);

  [[nodiscard]] bool CreateIndex(LabelId label, const std::vector<std::pair<std::string, std::string>> &vertices);

  std::unique_ptr<rocksdb::Transaction> CreateRocksDBTransaction() const;

  std::unique_ptr<rocksdb::Transaction> CreateAllReadingRocksDBTransaction() const;

  [[nodiscard]] bool SyncVertexToLabelIndexStorage(const Vertex &vertex, uint64_t commit_timestamp) const;

  [[nodiscard]] bool ClearDeletedVertex(std::string_view gid, uint64_t transaction_commit_timestamp) const;

  [[nodiscard]] bool DeleteVerticesWithRemovedIndexingLabel(uint64_t transaction_start_timestamp,
                                                            uint64_t transaction_commit_timestamp);
  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) override;

  /// Returns false if there was no index to drop
  bool DropIndex(LabelId label) override;

  bool IndexExists(LabelId label) const override;

  std::vector<LabelId> ListIndices() const override;

  uint64_t ApproximateVertexCount(LabelId label) const override;

  RocksDBStorage *GetRocksDBStorage() const;

  void LoadIndexInfo(const std::vector<std::string> &labels);

  std::unordered_set<LabelId> GetInfo() const;

 private:
  utils::Synchronized<std::map<uint64_t, std::map<Gid, std::vector<LabelId>>>> entries_for_deletion;
  std::unordered_set<LabelId> index_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

}  // namespace memgraph::storage
