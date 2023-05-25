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

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/transaction_db.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

/// Wraps RocksDB objects inside a struct. Vertex_chandle and edge_chandle are column family handles that may be
/// nullptr. In that case client should take care about them.
struct RocksDBStorage {
  /// TODO: (andi) Revisit special methods if this struct

  ~RocksDBStorage() {
    delete db_;
    db_ = nullptr;
    delete options_.comparator;
    options_.comparator = nullptr;
  }

  rocksdb::Options options_;
  rocksdb::TransactionDB *db_;
  rocksdb::ColumnFamilyHandle *vertex_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *edge_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *default_chandle = nullptr;

  uint64_t ApproximateVertexCount() const {
    uint64_t estimate_num_keys = 0;
    db_->GetIntProperty(vertex_chandle, "rocksdb.estimate-num-keys", &estimate_num_keys);
    return static_cast<int64_t>(estimate_num_keys);
  }
};

/// RocksDB comparator that compares keys with timestamps.
class ComparatorWithU64TsImpl : public rocksdb::Comparator {
 public:
  explicit ComparatorWithU64TsImpl();

  static const char *kClassName() { return "be"; }

  const char *Name() const override { return kClassName(); }

  void FindShortSuccessor(std::string *) const override {}
  void FindShortestSeparator(std::string *, const rocksdb::Slice &) const override {}

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override;

  using Comparator::CompareWithoutTimestamp;
  int CompareWithoutTimestamp(const rocksdb::Slice &a, bool a_has_ts, const rocksdb::Slice &b,
                              bool b_has_ts) const override;

  int CompareTimestamp(const rocksdb::Slice &ts1, const rocksdb::Slice &ts2) const override;

 private:
  // Extracts global id from user key. User key must be without timestamp.
  std::string_view ExtractGidFromUserKey(const rocksdb::Slice &key) const;

  const Comparator *cmp_without_ts_{nullptr};
};

}  // namespace memgraph::storage
