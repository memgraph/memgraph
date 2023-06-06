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
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

constexpr const char *outEdgeDirection = "0";
constexpr const char *inEdgeDirection = "1";

/// TODO: try to move this to hpp files so that we can follow jump on readings

inline std::string PutIndexingLabelFirst(const std::string &indexing_label, const std::vector<std::string> &labels);

inline std::string PutIndexingLabelAndPropertyFirst(const std::string &indexing_label,
                                                    const std::string &indexing_property,
                                                    const std::vector<std::string> &labels);

inline std::string SerializeIdType(const auto &id) { return std::to_string(id.AsUint()); }

inline auto DeserializeIdType(const std::string &str) { return storage::Gid::FromUint(std::stoull(str)); }

inline std::string SerializeTimestamp(const uint64_t ts) { return std::to_string(ts); }

inline std::string SerializeLabels(const std::vector<std::string> &labels) {
  if (labels.empty()) {
    return "";
  }
  std::string result = labels[0];
  std::string ser_labels =
      std::accumulate(std::next(labels.begin()), labels.end(), result,
                      [](const std::string &join, const auto &label_id) { return join + "," + label_id; });
  return ser_labels;
}

inline bool SerializedVertexHasLabels(const std::string &labels) { return !labels.empty(); }

template <typename Collection>
inline std::vector<std::string> TransformIDsToString(const Collection &labels) {
  std::vector<std::string> transformed_labels;
  std::transform(labels.begin(), labels.end(), std::back_inserter(transformed_labels),
                 [](const auto &label) { return SerializeIdType(label); });
  return transformed_labels;
}

inline std::vector<storage::LabelId> TransformFromStringLabels(const std::vector<std::string> &labels) {
  std::vector<storage::LabelId> transformed_labels;
  std::transform(labels.begin(), labels.end(), std::back_inserter(transformed_labels),
                 [](const auto &label) { return storage::LabelId::FromUint(std::stoull(label)); });
  return transformed_labels;
}

inline std::vector<storage::LabelId> DeserializeLabelsFromMainDiskStorage(const std::string &labels_str) {
  if (SerializedVertexHasLabels(labels_str)) {
    return TransformFromStringLabels(utils::Split(labels_str, ","));
  }
  return {};
}

inline std::string ExtractGidFromMainDiskStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  return key_vector[1];
}

inline std::string ExtractGidFromLabelIndexStorage(const std::string &key) {
  return ExtractGidFromMainDiskStorage(key);
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelIndexStorage(const std::string &value) {
  const auto value_splitted = utils::Split(value, "|");
  return TransformFromStringLabels(utils::Split(value_splitted[0], ","));
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelPropertyIndexStorage(const std::string &key) {
  std::vector<std::string> index_key = utils::Split(key, ",");
  auto property_it = index_key.begin() + 1;
  index_key.erase(property_it);
  return TransformFromStringLabels(index_key);
}

/// TODO: maybe it can be deleted
inline std::vector<storage::LabelId> DeserializeLabelsFromUniqueConstraintStorage(const std::string &key,
                                                                                  const std::string &value) {
  /// TODO: extract this to private method to get all labels except the constraint label from the value
  std::vector<std::string> value_vector = utils::Split(value, "|");
  std::vector<std::string> labels_str = utils::Split(value_vector[0], ",");
  auto labels = TransformFromStringLabels(labels_str);
  std::vector<std::string> key_vector = utils::Split(key, "|");
  std::vector<std::string> constraint_key = utils::Split(key_vector[0], ",");
  labels.emplace_back(storage::LabelId::FromUint(std::stoull(constraint_key[0])));
  return labels;
}

/// TODO: change to call ExtractGidFromMainDiskStorage
inline std::string ExtractGidFromUniqueConstraintStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  return key_vector[1];
}

/// TODO: andi Change that all method accept key-value named parameters to indicate that they are called from RocksDB
inline storage::LabelId DeserializeConstraintLabelFromUniqueConstraintStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  std::vector<std::string> constraint_key = utils::Split(key_vector[0], ",");
  /// TODO: andi Change this to deserialization method directly into the LabelId class
  return storage::LabelId::FromUint(std::stoull(constraint_key[0]));
}

inline storage::PropertyStore DeserializePropertiesFromUniqueConstraintStorage(const std::string &value) {
  std::vector<std::string> value_vector = utils::Split(value, "|");
  std::string properties_str = value_vector[1];
  return storage::PropertyStore::CreateFromBuffer(properties_str);
}

inline std::string SerializeProperties(const storage::PropertyStore &properties) { return properties.StringBuffer(); }

/// Serialize vertex to string as a key in KV store
/// label1, label2 | GID | commit_timestamp
inline std::string SerializeVertex(const storage::Result<std::vector<storage::LabelId>> &labels, storage::Gid gid) {
  std::string result = labels.HasError() ? "" : utils::SerializeLabels(TransformIDsToString(*labels)) + "|";
  result += utils::SerializeIdType(gid);
  return result;
}

/// Serialize vertex to string as a key in KV store.
/// label1, label2 | GID | commit_timestamp
/// TODO: is it now commit timestamp
inline std::string SerializeVertex(const storage::Vertex &vertex) {
  std::string result = utils::SerializeLabels(TransformIDsToString(vertex.labels)) + "|";
  result += utils::SerializeIdType(vertex.gid);
  return result;
}

/// Serialize vertex to string as a key in label index KV store.
/// target_label | label_1 | label_2 | ... | GID | commit_timestamp
/// TODO: is it now commit timestamp
inline std::string SerializeVertexAsKeyForLabelIndex(const std::string &indexing_label, const std::string &gid) {
  return indexing_label + "|" + gid;
}

inline std::string SerializeVertexAsKeyForLabelIndex(storage::LabelId label, storage::Gid gid) {
  return SerializeVertexAsKeyForLabelIndex(SerializeIdType(label), utils::SerializeIdType(gid));
}

/// TODO: this is same code as in SerializeVertexAsValueForUniqueConstraint
inline std::string SerializeVertexAsValueForLabelIndex(storage::LabelId indexing_label,
                                                       const std::vector<storage::LabelId> &vertex_labels,
                                                       const storage::PropertyStore &property_store) {
  std::vector<storage::LabelId> labels_without_target;
  std::copy_if(vertex_labels.begin(), vertex_labels.end(), std::back_inserter(labels_without_target),
               [&indexing_label](const auto &label) { return label != indexing_label; });
  std::string result = SerializeLabels(TransformIDsToString(vertex_labels)) + "|";
  return result + SerializeProperties(property_store);
}

inline std::string ExtractPropertiesFromLabelIndex(const std::string &value) {
  std::vector<std::string> value_vector = utils::Split(value, "|");
  return value_vector[1];
}

/// TODO: is this even used
inline std::string PutIndexingLabelFirst(const std::string &indexing_label, const std::vector<std::string> &labels) {
  std::string result = indexing_label;
  for (const auto &label : labels) {
    if (label != indexing_label) {
      result += "," + label;
    }
  }
  return result;
}

/// Serialize vertex to string as a key in label property index KV store.
/// target_label,target_property,label_1,label_2,... | GID | commit_timestamp
/// TODO: is it now commit timestamp
inline std::string SerializeVertexForLabelPropertyIndex(const std::string &indexing_label,
                                                        const std::string &indexing_property,
                                                        const std::vector<std::string> &vertex_labels,
                                                        const std::string &gid) {
  auto key_for_indexing = PutIndexingLabelAndPropertyFirst(indexing_label, indexing_property, vertex_labels);
  return key_for_indexing + "|" + gid;
}

/// TODO: andi Probably it is better to add delimiter between label,property and the rest of labels
/// TODO: reuse PutIndexingLabelAndPropertiesFirst
inline std::string PutIndexingLabelAndPropertyFirst(const std::string &indexing_label,
                                                    const std::string &indexing_property,
                                                    const std::vector<std::string> &vertex_labels) {
  std::string result = indexing_label + "," + indexing_property;
  for (const auto &label : vertex_labels) {
    if (label != indexing_label) {
      result += "," + label;
    }
  }
  return result;
}

inline std::string PutIndexingLabelAndPropertiesFirst(const std::string &target_label,
                                                      const std::vector<std::string> &target_properties) {
  std::string result = target_label;
  for (const auto &target_property : target_properties) {
    result += "," + target_property;
  }
  return result;
}

/// Serialize vertex to string as a key in unique constraint index KV store.
/// target_label, target_property_1, target_property_2, ... GID |
/// commit_timestamp
inline std::string SerializeVertexAsKeyForUniqueConstraint(const storage::LabelId &constraint_label,
                                                           const std::set<storage::PropertyId> &constraint_properties,
                                                           const std::string &gid) {
  auto key_for_indexing = PutIndexingLabelAndPropertiesFirst(SerializeIdType(constraint_label),
                                                             TransformIDsToString(constraint_properties));
  return key_for_indexing + "|" + gid;
}

inline std::string SerializeVertexAsValueForUniqueConstraint(const storage::LabelId &constraint_label,
                                                             const std::vector<storage::LabelId> &vertex_labels,
                                                             const storage::PropertyStore &property_store) {
  std::vector<storage::LabelId> labels_without_target;
  std::copy_if(vertex_labels.begin(), vertex_labels.end(), std::back_inserter(labels_without_target),
               [&constraint_label](const auto &label) { return label != constraint_label; });
  std::string result = SerializeLabels(TransformIDsToString(vertex_labels)) + "|";
  return result + SerializeProperties(property_store);
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
