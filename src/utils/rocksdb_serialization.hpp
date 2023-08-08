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

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {

static constexpr const char *outEdgeDirection = "0";
static constexpr const char *inEdgeDirection = "1";

/// TODO: try to move this to hpp files so that we can follow jump on readings

inline std::string SerializeIdType(const auto &id) { return std::to_string(id.AsUint()); }

inline bool SerializedVertexHasLabels(const std::string &labels) { return !labels.empty(); }

template <typename T>
concept WithSize = requires(const T value) {
  { value.size() } -> std::same_as<size_t>;
};

template <WithSize TCollection>
inline std::vector<std::string> TransformIDsToString(const TCollection &col) {
  std::vector<std::string> transformed_col;
  transformed_col.reserve(col.size());
  for (const auto &elem : col) {
    transformed_col.emplace_back(SerializeIdType(elem));
  }
  return transformed_col;
}

inline std::vector<storage::LabelId> TransformFromStringLabels(std::vector<std::string> &&labels) {
  std::vector<storage::LabelId> transformed_labels;
  transformed_labels.reserve(labels.size());
  for (const std::string &label : labels) {
    transformed_labels.emplace_back(storage::LabelId::FromUint(std::stoull(label)));
  }
  return transformed_labels;
}

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

inline std::string SerializeProperties(const storage::PropertyStore &properties) { return properties.StringBuffer(); }

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

inline std::string SerializeVertexAsValueForAuxiliaryStorages(storage::LabelId label_to_remove,
                                                              const std::vector<storage::LabelId> &vertex_labels,
                                                              const storage::PropertyStore &property_store) {
  std::vector<storage::LabelId> labels_without_target;
  labels_without_target.reserve(vertex_labels.size());
  for (const storage::LabelId &label : vertex_labels) {
    if (label != label_to_remove) {
      labels_without_target.emplace_back(label);
    }
  }
  std::string result = SerializeLabels(TransformIDsToString(labels_without_target)) + "|";
  return result + SerializeProperties(property_store);
}

inline std::string ExtractGidFromKey(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  return key_vector[1];
}

inline storage::PropertyStore DeserializePropertiesFromAuxiliaryStorages(const std::string &value) {
  std::vector<std::string> value_vector = utils::Split(value, "|");
  std::string properties_str = value_vector[1];
  return storage::PropertyStore::CreateFromBuffer(properties_str);
}

inline std::string SerializeVertex(const storage::Vertex &vertex) {
  std::string result = utils::SerializeLabels(TransformIDsToString(vertex.labels)) + "|";
  result += utils::SerializeIdType(vertex.gid);
  return result;
}

inline std::vector<storage::LabelId> DeserializeLabelsFromMainDiskStorage(const std::string &key) {
  std::string labels_str = key.substr(0, key.find('|'));
  if (SerializedVertexHasLabels(labels_str)) {
    return TransformFromStringLabels(utils::Split(labels_str, ","));
  }
  return {};
}

inline std::vector<std::string> ExtractLabelsFromMainDiskStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  std::string labels_str = key_vector[0];
  return utils::Split(labels_str, ",");
}

inline storage::PropertyStore DeserializePropertiesFromMainDiskStorage(const std::string_view value) {
  return storage::PropertyStore::CreateFromBuffer(value);
}

inline std::string ExtractGidFromMainDiskStorage(const std::string &key) { return ExtractGidFromKey(key); }

inline std::string ExtractGidFromUniqueConstraintStorage(const std::string &key) { return ExtractGidFromKey(key); }

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
  return SerializeVertexAsValueForAuxiliaryStorages(constraint_label, vertex_labels, property_store);
}

inline storage::LabelId DeserializeConstraintLabelFromUniqueConstraintStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  std::vector<std::string> constraint_key = utils::Split(key_vector[0], ",");
  /// TODO: andi Change this to deserialization method directly into the LabelId class
  return storage::LabelId::FromUint(std::stoull(constraint_key[0]));
}

inline storage::PropertyStore DeserializePropertiesFromUniqueConstraintStorage(const std::string &value) {
  return DeserializePropertiesFromAuxiliaryStorages(value);
}

inline std::string SerializeVertexAsKeyForLabelIndex(const std::string &indexing_label, const std::string &gid) {
  return indexing_label + "|" + gid;
}

inline std::string SerializeVertexAsKeyForLabelIndex(storage::LabelId label, storage::Gid gid) {
  return SerializeVertexAsKeyForLabelIndex(SerializeIdType(label), utils::SerializeIdType(gid));
}

inline std::string ExtractGidFromLabelIndexStorage(const std::string &key) { return ExtractGidFromKey(key); }

inline std::string SerializeVertexAsValueForLabelIndex(storage::LabelId indexing_label,
                                                       const std::vector<storage::LabelId> &vertex_labels,
                                                       const storage::PropertyStore &property_store) {
  return SerializeVertexAsValueForAuxiliaryStorages(indexing_label, vertex_labels, property_store);
}

inline std::vector<storage::LabelId> DeserializeLabelsFromIndexStorage(const std::string &value) {
  std::string labels = value.substr(0, value.find('|'));
  return TransformFromStringLabels(utils::Split(labels, ","));
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelIndexStorage(const std::string &value) {
  return DeserializeLabelsFromIndexStorage(value);
}

inline storage::PropertyStore DeserializePropertiesFromLabelIndexStorage(const std::string &value) {
  return DeserializePropertiesFromAuxiliaryStorages(value);
}

inline std::string SerializeVertexAsKeyForLabelPropertyIndex(const std::string &indexing_label,
                                                             const std::string &indexing_property,
                                                             const std::string &gid) {
  return indexing_label + "|" + indexing_property + "|" + gid;
}

inline std::string SerializeVertexAsKeyForLabelPropertyIndex(storage::LabelId label, storage::PropertyId property,
                                                             storage::Gid gid) {
  return SerializeVertexAsKeyForLabelPropertyIndex(SerializeIdType(label), SerializeIdType(property),
                                                   utils::SerializeIdType(gid));
}

inline std::string SerializeVertexAsValueForLabelPropertyIndex(storage::LabelId indexing_label,
                                                               const std::vector<storage::LabelId> &vertex_labels,
                                                               const storage::PropertyStore &property_store) {
  return SerializeVertexAsValueForAuxiliaryStorages(indexing_label, vertex_labels, property_store);
}

inline std::string ExtractGidFromLabelPropertyIndexStorage(const std::string &key) {
  std::vector<std::string> key_vector = utils::Split(key, "|");
  return key_vector[2];
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelPropertyIndexStorage(const std::string &value) {
  return DeserializeLabelsFromIndexStorage(value);
}

inline storage::PropertyStore DeserializePropertiesFromLabelPropertyIndexStorage(const std::string &value) {
  return DeserializePropertiesFromAuxiliaryStorages(value);
}

/// Serialize edge as two KV entries
/// vertex_gid_1 | vertex_gid_2 | direction | edge_type | GID | commit_timestamp
inline std::string SerializeEdge(storage::EdgeAccessor *edge_acc) {
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
  return src_dest_key;
}

/// Serialize edge as two KV entries
/// vertex_gid_1 | vertex_gid_2 | direction | edge_type | GID | commit_timestamp
/// @tparam src_vertex_gid, dest_vertex_gid: Gid of the source and destination vertices
/// @tparam edge: Edge to be serialized
/// @tparam edge_type_id: EdgeTypeId of the edge
inline std::string SerializeEdge(storage::Gid src_vertex_gid, storage::Gid dest_vertex_gid,
                                 storage::EdgeTypeId edge_type_id, const storage::EdgeRef edge_ref,
                                 bool properties_on_edges) {
  // Serialized objects
  auto from_gid = utils::SerializeIdType(src_vertex_gid);
  auto to_gid = utils::SerializeIdType(dest_vertex_gid);
  auto edge_type = utils::SerializeIdType(edge_type_id);
  std::string edge_gid;

  if (properties_on_edges) {
    edge_gid = utils::SerializeIdType(edge_ref.ptr->gid);
  } else {
    edge_gid = utils::SerializeIdType(edge_ref.gid);
  }

  // source->destination key
  std::string src_dest_key = from_gid + "|";
  src_dest_key += to_gid + "|";
  src_dest_key += outEdgeDirection;
  src_dest_key += "|" + edge_type + "|";
  src_dest_key += edge_gid;
  return src_dest_key;
}

/// TODO: (andi): This can potentially be a problem on big-endian machines.
inline void PutFixed64(std::string *dst, uint64_t value) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
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

}  // namespace memgraph::utils
