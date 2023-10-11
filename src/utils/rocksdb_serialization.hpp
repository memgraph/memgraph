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

#include <charconv>
#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <string>
#include <string_view>

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

struct StartEndPositions {
  size_t start;
  size_t end;

  size_t Size() const { return end - start; }
  bool Valid() const { return start != std::string::npos && start <= end; }
};

template <typename T>
inline std::string_view FindPartOfStringView(const std::string_view str, const char delim, T partNumber) {
  StartEndPositions startEndPos{0, 0};
  for (int i = 0; i < partNumber; ++i) {
    startEndPos.start = startEndPos.end;
    startEndPos.end = str.find(delim, startEndPos.start);
    if (i < partNumber - 1) {
      if (startEndPos.end == std::string::npos) {
        // We didn't find enough parts.
        startEndPos.start = std::string::npos;
        break;
      }
      ++startEndPos.end;
    }
  }
  return startEndPos.Valid() ? str.substr(startEndPos.start, startEndPos.Size()) : str;
}

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
    transformed_labels.emplace_back(storage::LabelId::FromString(label));
  }
  return transformed_labels;
}

/// TODO: (andi) Change to utils::Join call
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

inline storage::Gid ExtractSrcVertexGidFromEdgeValue(const std::string value) {
  const std::string_view src_vertex_gid_str = FindPartOfStringView(value, '|', 1);
  return storage::Gid::FromString(src_vertex_gid_str);
}

inline storage::Gid ExtractDstVertexGidFromEdgeValue(const std::string value) {
  const std::string_view dst_vertex_gid_str = FindPartOfStringView(value, '|', 2);
  return storage::Gid::FromString(dst_vertex_gid_str);
}

inline storage::EdgeTypeId ExtractEdgeTypeIdFromEdgeValue(const std::string_view value) {
  const std::string_view edge_type_str = FindPartOfStringView(value, '|', 3);
  return storage::EdgeTypeId::FromString(edge_type_str);
}

inline std::string_view GetPropertiesFromEdgeValue(const std::string_view value) {
  return FindPartOfStringView(value, '|', 4);
}

inline std::string SerializeEdgeAsValue(const std::string &src_vertex_gid, const std::string &dst_vertex_gid,
                                        const storage::EdgeTypeId &edge_type, const storage::Edge *edge = nullptr) {
  auto tmp = src_vertex_gid + "|" + dst_vertex_gid + "|" + SerializeIdType(edge_type) + "|";
  if (edge) {
    return tmp + utils::SerializeProperties(edge->properties);
  }
  return tmp;
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

inline std::string ExtractGidFromKey(const std::string &key) { return std::string(FindPartOfStringView(key, '|', 2)); }

inline storage::PropertyStore DeserializePropertiesFromAuxiliaryStorages(const std::string &value) {
  const std::string_view properties_str = FindPartOfStringView(value, '|', 2);
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
  return utils::Split(FindPartOfStringView(key, '|', 1), ",");
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
  const std::string_view firstPartKey = FindPartOfStringView(key, '|', 1);
  const std::string_view constraint_key = FindPartOfStringView(firstPartKey, ',', 1);
  /// TODO: andi Change this to deserialization method directly into the LabelId class
  uint64_t labelID = 0;
  const char *endOfConstraintKey = constraint_key.data() + constraint_key.size();
  auto [ptr, ec] = std::from_chars(constraint_key.data(), endOfConstraintKey, labelID);
  if (ec != std::errc() || ptr != endOfConstraintKey) {
    throw std::invalid_argument("Failed to deserialize label id from unique constraint storage");
  }
  return storage::LabelId::FromUint(labelID);
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

inline std::vector<storage::LabelId> DeserializeLabelsFromIndexStorage(const std::string &key,
                                                                       const std::string &value) {
  std::string labels_str{FindPartOfStringView(value, '|', 1)};
  std::vector<storage::LabelId> labels{TransformFromStringLabels(utils::Split(labels_str, ","))};
  std::string indexing_label = key.substr(0, key.find('|'));
  labels.emplace_back(storage::LabelId::FromString(indexing_label));
  return labels;
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelIndexStorage(const std::string &key,
                                                                            const std::string &value) {
  return DeserializeLabelsFromIndexStorage(key, value);
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
  return std::string(FindPartOfStringView(key, '|', 3));
}

inline std::vector<storage::LabelId> DeserializeLabelsFromLabelPropertyIndexStorage(const std::string &key,
                                                                                    const std::string &value) {
  return DeserializeLabelsFromIndexStorage(key, value);
}

inline storage::PropertyStore DeserializePropertiesFromLabelPropertyIndexStorage(const std::string &value) {
  return DeserializePropertiesFromAuxiliaryStorages(value);
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
