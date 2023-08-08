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

#include "rocksdb/filter_policy.h"
#include "rocksdb/table/block_based/filter_policy_internal.h"

#include "storage/v2/edge.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::storage {

/// FilterBitsBuilder and FilterBitsReader are just forward declarations if I don't include filter_policy_internal.h,
/// I cannot inherit from them.

class EdgeFilterPolicy : public rocksdb::FilterPolicy {
 private:
  class EdgeFilterBitsReader : public rocksdb::FilterBitsReader {
   public:
    explicit EdgeFilterBitsReader(rocksdb::FilterBitsReader *orig_reader) : orig_reader_(orig_reader) {}

    EdgeFilterBitsReader(const EdgeFilterBitsReader &) = delete;
    EdgeFilterBitsReader &operator=(const EdgeFilterBitsReader &) = delete;
    EdgeFilterBitsReader(EdgeFilterBitsReader &&) = delete;
    EdgeFilterBitsReader &operator=(EdgeFilterBitsReader &&) = delete;
    ~EdgeFilterBitsReader() override {}

    bool MayMatch(const rocksdb::Slice &entry) override {
      spdlog::trace("Entry received in MayMatch: {}", entry.ToString());
      return true;
    }

   private:
    rocksdb::FilterBitsReader *orig_reader_;
  };

  class EdgeFilterBitsBuilder : public rocksdb::FilterBitsBuilder {
   public:
    explicit EdgeFilterBitsBuilder(rocksdb::FilterBitsBuilder *orig_builder) : orig_builder_(orig_builder) {}

    EdgeFilterBitsBuilder(const EdgeFilterBitsBuilder &) = delete;
    EdgeFilterBitsBuilder &operator=(const EdgeFilterBitsBuilder &) = delete;
    EdgeFilterBitsBuilder(EdgeFilterBitsBuilder &&) = delete;
    EdgeFilterBitsBuilder &operator=(EdgeFilterBitsBuilder &&) = delete;
    ~EdgeFilterBitsBuilder() override {}

    // Add a key (or prefix) to the filter. Typically, a builder will keep
    // a set of 64-bit key hashes and only build the filter in Finish
    // when the final number of keys is known. Keys are added in sorted order
    // and duplicated keys are possible, so typically, the builder will
    // only add this key if its hash is different from the most recently
    // added.
    void AddKey(const rocksdb::Slice &key) override;

    // Called by RocksDB before Finish to populate
    // TableProperties::num_filter_entries, so should represent the
    // number of unique keys (and/or prefixes) added, but does not have
    // to be exact. `return 0;` may be used to conspicuously indicate "unknown".
    size_t EstimateEntriesAdded() override { return orig_builder_->EstimateEntriesAdded(); }

    // Generate the filter using the keys that are added
    // The return value of this function would be the filter bits,
    // The ownership of actual data is set to buf
    rocksdb::Slice Finish(std::unique_ptr<const char[]> *buf) override { return orig_builder_->Finish(buf); }

    size_t ApproximateNumEntries(size_t bytes) override { return orig_builder_->ApproximateNumEntries(bytes); }

   private:
    rocksdb::FilterBitsBuilder *orig_builder_;
  };

 public:
  explicit EdgeFilterPolicy() {}

  EdgeFilterPolicy(const EdgeFilterPolicy &) = delete;
  EdgeFilterPolicy &operator=(const EdgeFilterPolicy &) = delete;
  EdgeFilterPolicy(EdgeFilterPolicy &&) = delete;
  EdgeFilterPolicy &operator=(EdgeFilterPolicy &&) = delete;
  ~EdgeFilterPolicy() override { delete orig_policy_; }

  rocksdb::FilterBitsBuilder *GetBuilderWithContext(const rocksdb::FilterBuildingContext &context) const override {
    return new EdgeFilterBitsBuilder(orig_policy_->GetBuilderWithContext(context));
  }

  rocksdb::FilterBitsReader *GetFilterBitsReader(const rocksdb::Slice &contents) const override {
    return new EdgeFilterBitsReader(orig_policy_->GetFilterBitsReader(contents));
  }

 private:
  const rocksdb::FilterPolicy *orig_policy_ = rocksdb::NewBloomFilterPolicy(10);
};

}  // namespace memgraph::storage
