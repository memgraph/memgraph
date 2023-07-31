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
#include <rocksdb/slice.h>
#include <string_view>
#include "utils/algorithm.hpp"
#include "utils/disk_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

namespace {

inline rocksdb::Slice StripTimestampFromUserKey(const rocksdb::Slice &user_key, size_t ts_sz) {
  // spdlog::debug("StripTimestampFromUserKey: {}", user_key.ToString());
  rocksdb::Slice ret = user_key;
  ret.remove_suffix(ts_sz);
  return ret;
}

/// NOTE: Timestamp is encoded as last 8B in user key.
inline rocksdb::Slice ExtractTimestampFromUserKey(const rocksdb::Slice &user_key) {
  assert(user_key.size() >= sizeof(uint64_t));
  return {user_key.data() + user_key.size() - sizeof(uint64_t), sizeof(uint64_t)};
}

// Extracts global id from user key. User key must be without timestamp.
inline std::string_view ExtractGidFromUserKey(const rocksdb::Slice &key) {
  std::string_view keyStrView = key.ToStringView();
  // spdlog::debug("ExtractGidFromKey: {}", keyStrView);
  if (utils::Contains(keyStrView, '|')) {
    assert(key.size() >= 2);
    return keyStrView.substr(keyStrView.find_last_of('|') + 1);
  }
  return keyStrView;
}

}  // namespace

ComparatorWithU64TsImpl::ComparatorWithU64TsImpl()
    : Comparator(/*ts_sz=*/sizeof(uint64_t)), cmp_without_ts_(rocksdb::BytewiseComparator()) {
  assert(cmp_without_ts_->timestamp_size() == 0);
}

/// TODO: try to write somehow unit test for this
int ComparatorWithU64TsImpl::Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
  std::string a_str = a.ToString();
  std::string b_str = b.ToString();
  // spdlog::debug("Received a: {} b: {} in compare method", a_str, b_str);
  uint32_t num_separators_a = std::count(a_str.begin(), a_str.end(), '|');
  uint32_t num_separators_b = std::count(b_str.begin(), b_str.end(), '|');
  utils::RocksDBType a_key_type = utils::GetRocksDBKeyType(num_separators_a);
  utils::RocksDBType b_key_type = utils::GetRocksDBKeyType(num_separators_b);
  if (utils::ComparingVertexWithVertex(a_key_type, b_key_type) ||
      utils::ComparingEdgeWithEdge(a_key_type, b_key_type)) {
    int ret = CompareWithoutTimestamp(a, b);
    if (ret != 0) {
      return ret;
    }
    // Compare timestamp.
    // For the same user key with different timestamps, larger (newer) timestamp
    // comes first.
    return CompareTimestamp(ExtractTimestampFromUserKey(b), ExtractTimestampFromUserKey(a));
  }
  if (utils::ComparingEdgeWithGID(a_key_type, b_key_type)) {
    return CompareEdgeWithGidForPrefixSearch(a_str, b_str);
  }
  throw utils::BasicException(
      "Cannot handle specific compare use-case when one of the keys are neither vertex nor edge.");
}

int ComparatorWithU64TsImpl::CompareEdgeWithGidForPrefixSearch(const std::string_view edge,
                                                               const std::string_view source_vertex_gid) const {
  // spdlog::debug("Compare edge with gid for prefix search START");
  rocksdb::Slice stripped_src_vertex_gid = StripTimestampFromUserKey(source_vertex_gid, timestamp_size());
  // rocksdb::Slice stripped_src_vertex_gid = source_vertex_gid;
  std::string_view edge_source_vertex_gid = edge.substr(0, edge.find('|'));
  // spdlog::debug("Edge: {} Edge source vertex gid: {} Size: {} Source vertex gid: {} Size: {}", edge,
  //               edge_source_vertex_gid, edge_source_vertex_gid.size(), stripped_src_vertex_gid.ToString(),
  //               stripped_src_vertex_gid.ToString().size());
  int cmp_res = cmp_without_ts_->Compare(edge_source_vertex_gid, stripped_src_vertex_gid);
  // spdlog::debug("Compare result: {}", cmp_res);
  return cmp_res;
}

/// TODO: entered CompareWithoutTimestamp with GID. Handle something like in a compare function.
int ComparatorWithU64TsImpl::CompareWithoutTimestamp(const rocksdb::Slice &a, bool a_has_ts, const rocksdb::Slice &b,
                                                     bool b_has_ts) const {
  const size_t ts_sz = timestamp_size();
  // spdlog::debug("Timestamp size: {}", ts_sz);
  // spdlog::debug("a_has_ts: {} b_has_ts: {}", a_has_ts, b_has_ts);
  // spdlog::debug("a size: {} b size: {}", a.size(), b.size());
  assert(!a_has_ts || a.size() >= ts_sz);
  assert(!b_has_ts || b.size() >= ts_sz);
  // spdlog::debug("a key: {}", a.ToString());
  // spdlog::debug("b key: {}", b.ToString());
  rocksdb::Slice lhsUserKey = a_has_ts ? StripTimestampFromUserKey(a, ts_sz) : a;
  rocksdb::Slice rhsUserKey = b_has_ts ? StripTimestampFromUserKey(b, ts_sz) : b;
  // spdlog::debug("lhsUserKey: {}", lhsUserKey.ToString());
  // spdlog::debug("rhsUserKey: {}", rhsUserKey.ToString());
  rocksdb::Slice lhsGid = ExtractGidFromUserKey(lhsUserKey);
  rocksdb::Slice rhsGid = ExtractGidFromUserKey(rhsUserKey);
  // spdlog::debug("lhsGid: {}", lhsGid.ToString());
  // spdlog::debug("rhsGid: {}", rhsGid.ToString());
  return cmp_without_ts_->Compare(lhsGid, rhsGid);
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
