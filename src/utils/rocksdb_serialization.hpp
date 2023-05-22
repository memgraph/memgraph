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

#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <string>

#include <rocksdb/db.h>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::utils {

constexpr const char *outEdgeDirection = "0";
constexpr const char *inEdgeDirection = "1";

inline std::string PutIndexingLabelFirst(const std::string &indexing_label, const std::vector<std::string> &labels);

inline std::string SerializeIdType(const auto &id) { return std::to_string(id.AsUint()); }

inline auto DeserializeIdType(const std::string &str) { return storage::Gid::FromUint(std::stoull(str)); }

inline std::string SerializeTimestamp(const uint64_t ts) { return std::to_string(ts); }

inline std::string SerializeLabels(const std::vector<storage::LabelId> &labels) {
  if (labels.empty()) {
    return "";
  }
  std::string result = std::to_string(labels[0].AsUint());
  std::string ser_labels = std::accumulate(
      std::next(labels.begin()), labels.end(), result,
      [](const std::string &join, const auto &label_id) { return join + "," + std::to_string(label_id.AsUint()); });
  return ser_labels;
}

inline std::string SerializeProperties(storage::PropertyStore &properties) { return properties.StringBuffer(); }

/// Serialize vertex to string as a key in KV store
/// label1, label2 | GID | commit_timestamp
inline std::string SerializeVertex(const storage::Result<std::vector<storage::LabelId>> &labels, storage::Gid gid) {
  std::string result = labels.HasError() ? "" : utils::SerializeLabels(*labels) + "|";
  result += utils::SerializeIdType(gid);
  return result;
}

/// Serialize vertex to string as a key in KV store
/// label1, label2 | GID | commit_timestamp
inline std::string SerializeVertex(const storage::Vertex &vertex) {
  std::string result = utils::SerializeLabels(vertex.labels) + "|";
  result += utils::SerializeIdType(vertex.gid);
  return result;
}

inline std::string SerializeIndexedVertex(const std::string &indexing_label, const std::vector<std::string> &labels,
                                          const std::string &gid) {
  auto indexed_labels = PutIndexingLabelFirst(indexing_label, labels);
  return indexed_labels + "|" + gid;
}

inline std::string SerializeIndexedVertex(storage::LabelId label, const std::vector<storage::LabelId> &labels,
                                          storage::Gid gid) {
  std::vector<std::string> labels_str;
  std::transform(labels.begin(), labels.end(), std::back_inserter(labels_str),
                 [](storage::LabelId label_) { return SerializeIdType(label_); });
  return SerializeIndexedVertex(SerializeIdType(label), labels_str, utils::SerializeIdType(gid));
}

inline std::string PutIndexingLabelFirst(const std::string &indexing_label, const std::vector<std::string> &labels) {
  std::string result = indexing_label;
  for (const auto &label : labels) {
    if (label != indexing_label) {
      result += "," + label;
    }
  }
  return result;
}

/// Serialize edge as two KV entries
/// vertex_gid_1 | vertex_gid_2 | direction | edge_type | GID | commit_timestamp
inline std::pair<std::string, std::string> SerializeEdge(storage::EdgeAccessor *edge_acc) {
  // Serialized objects
  auto from_gid = utils::SerializeIdType(edge_acc->FromVertex().Gid());
  auto to_gid = utils::SerializeIdType(edge_acc->ToVertex().Gid());
  auto edge_type = utils::SerializeIdType(edge_acc->EdgeType());
  auto edge_gid = utils::SerializeIdType(edge_acc->Gid());
  // source->destination key
  std::string src_dest_key = from_gid + "|";
  src_dest_key += to_gid + "|";
  src_dest_key += outEdgeDirection;
  src_dest_key += "|" + edge_type + "|";
  src_dest_key += edge_gid;
  // destination->source key
  std::string dest_src_key = to_gid + "|";
  dest_src_key += from_gid + "|";
  dest_src_key += inEdgeDirection;
  dest_src_key += "|" + edge_type + "|";
  dest_src_key += edge_gid;
  return {src_dest_key, dest_src_key};
}

/// Serialize edge as two KV entries
/// vertex_gid_1 | vertex_gid_2 | direction | edge_type | GID | commit_timestamp
/// @tparam src_vertex_gid, dest_vertex_gid: Gid of the source and destination vertices
/// @tparam edge: Edge to be serialized
/// @tparam edge_type_id: EdgeTypeId of the edge
inline std::pair<std::string, std::string> SerializeEdge(storage::Gid src_vertex_gid, storage::Gid dest_vertex_gid,
                                                         storage::EdgeTypeId edge_type_id, const storage::Edge *edge) {
  // Serialized objects
  auto from_gid = utils::SerializeIdType(src_vertex_gid);
  auto to_gid = utils::SerializeIdType(dest_vertex_gid);
  auto edge_type = utils::SerializeIdType(edge_type_id);
  auto edge_gid = utils::SerializeIdType(edge->gid);
  // source->destination key
  std::string src_dest_key = from_gid + "|";
  src_dest_key += to_gid + "|";
  src_dest_key += outEdgeDirection;
  src_dest_key += "|" + edge_type + "|";
  src_dest_key += edge_gid;
  // destination->source key
  std::string dest_src_key = to_gid + "|";
  dest_src_key += from_gid + "|";
  dest_src_key += inEdgeDirection;
  dest_src_key += "|" + edge_type + "|";
  dest_src_key += edge_gid;
  return {src_dest_key, dest_src_key};
}

/// TODO: (andi): This can potentially be a problem on big-endian machines.
inline void PutFixed64(std::string *dst, uint64_t value) {
  dst->append(const_cast<const char *>(reinterpret_cast<char *>(&value)), sizeof(value));
}

inline uint64_t DecodeFixed64(const char *ptr) {
  // Load the raw bytes
  uint64_t result = 0;
  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
  return result;
}

inline std::string StringTimestamp(uint64_t ts) {
  std::string ret;
  PutFixed64(&ret, ts);
  return ret;
}

inline uint64_t ExtractTimestampFromDeserializedUserKey(const rocksdb::Slice &user_key) {
  return DecodeFixed64(user_key.data_ + user_key.size_);
}

}  // namespace memgraph::utils
