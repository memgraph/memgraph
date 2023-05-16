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

#include <rocksdb/cache.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>

class TimestampCompactionFilter : public rocksdb::CompactionFilter {
 public:
  const char *Name() const override { return "TimestampCompactionFilter"; }

  /// Return true if the key-value pair should be removed from the database during compaction.
  /// Filters KV entries that are older than the specified timestamp.
  bool Filter(int /*level*/, const rocksdb::Slice & /*key*/, const rocksdb::Slice & /*existing_value*/,
              std::string * /*new_value*/, bool * /*value_changed*/) const override {
    return true;
  }
};
