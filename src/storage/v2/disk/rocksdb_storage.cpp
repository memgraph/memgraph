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

#include "rocksdb_storage.hpp"
#include "utils/rocksdb.hpp"

namespace memgraph::storage {

namespace {

inline rocksdb::Slice StripTimestampFromUserKey(const rocksdb::Slice &user_key, size_t ts_sz) {
  rocksdb::Slice ret = user_key;
  ret.remove_suffix(ts_sz);
  return ret;
}

inline rocksdb::Slice ExtractTimestampFromUserKey(const rocksdb::Slice &user_key) {
  assert(user_key.size() >= sizeof(uint64_t));
  return rocksdb::Slice(user_key.data() + user_key.size() - sizeof(uint64_t), sizeof(uint64_t));
}

}  // namespace

ComparatorWithU64TsImpl::ComparatorWithU64TsImpl()
    : Comparator(/*ts_sz=*/sizeof(uint64_t)), cmp_without_ts_(rocksdb::BytewiseComparator()) {
  assert(cmp_without_ts_->timestamp_size() == 0);
}

int ComparatorWithU64TsImpl::Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
  int ret = CompareWithoutTimestamp(a, b);
  if (ret != 0) {
    return ret;
  }
  // Compare timestamp.
  // For the same user key with different timestamps, larger (newer) timestamp
  // comes first.
  return -CompareTimestamp(ExtractTimestampFromUserKey(a), ExtractTimestampFromUserKey(b));
}

int ComparatorWithU64TsImpl::CompareWithoutTimestamp(const rocksdb::Slice &a, bool a_has_ts, const rocksdb::Slice &b,
                                                     bool b_has_ts) const {
  const size_t ts_sz = timestamp_size();
  assert(!a_has_ts || a.size() >= ts_sz);
  assert(!b_has_ts || b.size() >= ts_sz);
  rocksdb::Slice lhs = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
  rocksdb::Slice rhs = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
  return cmp_without_ts_->Compare(lhs, rhs);
}

int ComparatorWithU64TsImpl::CompareTimestamp(const rocksdb::Slice &ts1, const rocksdb::Slice &ts2) const {
  assert(ts1.size() == sizeof(uint64_t));
  assert(ts2.size() == sizeof(uint64_t));
  uint64_t lhs = utils::DecodeFixed64(ts1.data());
  uint64_t rhs = utils::DecodeFixed64(ts2.data());
  if (lhs < rhs) {
    return -1;
  }
  if (lhs > rhs) {
    return 1;
  }
  return 0;
}

}  // namespace memgraph::storage
