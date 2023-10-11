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

#include "storage/v2/edge_direction.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/modified_edge.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

/// TODO: this should be somehow more wrapped inside the storage class so from the software engineering perspective
/// it isn't great to have this here. But for now it is ok.
/// Wraps RocksDB objects inside a struct. Vertex_chandle and edge_chandle are column family handles that may be
/// nullptr. In that case client should take care about them.
struct RocksDBStorage {
  explicit RocksDBStorage() {}

  RocksDBStorage(const RocksDBStorage &) = delete;
  RocksDBStorage &operator=(const RocksDBStorage &) = delete;
  RocksDBStorage(RocksDBStorage &&) = delete;
  RocksDBStorage &operator=(RocksDBStorage &&) = delete;

  ~RocksDBStorage() {
    delete db_;
    db_ = nullptr;
    delete options_.comparator;
    options_.comparator = nullptr;
  }

  rocksdb::Options options_;
  rocksdb::TransactionDB *db_;
  /// TODO: (andi) Refactor this
  rocksdb::ColumnFamilyHandle *vertex_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *edge_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *default_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *out_edges_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *in_edges_chandle = nullptr;
};

/// RocksDB comparator that compares keys with timestamps.
class ComparatorWithU64TsImpl : public rocksdb::Comparator {
 public:
  explicit ComparatorWithU64TsImpl();

  static const char *kClassName() { return "be"; }

  const char *Name() const override { return kClassName(); }

  void FindShortSuccessor(std::string * /*key*/) const override {}
  void FindShortestSeparator(std::string * /*start*/, const rocksdb::Slice & /*limit*/) const override {}

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override;

  using Comparator::CompareWithoutTimestamp;
  int CompareWithoutTimestamp(const rocksdb::Slice &a, bool a_has_ts, const rocksdb::Slice &b,
                              bool b_has_ts) const override;

  int CompareTimestamp(const rocksdb::Slice &ts1, const rocksdb::Slice &ts2) const override;

 private:
  const Comparator *cmp_without_ts_{nullptr};
};

}  // namespace memgraph::storage
