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

inline std::string SerializeLabels(const std::vector<std::string> &labels) { return utils::Join(labels, ","); }

inline std::string SerializeProperties(const storage::PropertyStore &properties) { return properties.StringBuffer(); }

inline std::string PutIndexingLabelAndPropertyFirst(const std::string &indexing_label,
                                                    const std::string &indexing_property,
                                                    const std::vector<std::string> &vertex_labels) {
  std::string result;
  result += indexing_label;
  result += ",";
  result += indexing_property;

  for (const auto &label : vertex_labels) {
    if (label != indexing_label) {
      result += ",";
      result += label;
    }
  }
  return result;
}

inline std::string PutIndexingLabelAndPropertiesFirst(const std::string &target_label,
                                                      const std::vector<std::string> &target_properties) {
  std::string result;
  result += target_label;
  for (const auto &target_property : target_properties) {
    result += ",";
    result += target_property;
  }
  return result;
}

inline storage::Gid ExtractSrcVertexGidFromEdgeValue(const std::string value) {
  return storage::Gid::FromString(FindPartOfStringView(value, '|', 1));
}

inline storage::Gid ExtractDstVertexGidFromEdgeValue(const std::string value) {
  return storage::Gid::FromString(FindPartOfStringView(value, '|', 2));
}

inline storage::EdgeTypeId ExtractEdgeTypeIdFromEdgeValue(const std::string_view value) {
  return storage::EdgeTypeId::FromString(FindPartOfStringView(value, '|', 3));
}

inline std::string_view GetPropertiesFromEdgeValue(const std::string_view value) {
  return FindPartOfStringView(value, '|', 4);
}

inline std::string SerializeEdgeAsValue(const std::string &src_vertex_gid, const std::string &dst_vertex_gid,
                                        const storage::EdgeTypeId &edge_type, const storage::Edge *edge = nullptr) {
  std::string edge_type_str = SerializeIdType(edge_type);
  std::string result;
  result.reserve(src_vertex_gid.size() + 3 + dst_vertex_gid.size() + edge_type_str.size());
  result += src_vertex_gid;
  result += "|";
  result += dst_vertex_gid;
  result += "|";
  result += edge_type_str;
  result += "|";
  if (edge) {
    return result + utils::SerializeProperties(edge->properties);
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

inline std::string ExtractGidFromKey(const std::string &key) { return std::string(FindPartOfStringView(key, '|', 2)); }

inline storage::PropertyStore DeserializePropertiesFromAuxiliaryStorages(const std::string &value) {
  return storage::PropertyStore::CreateFromBuffer(FindPartOfStringView(value, '|', 2));
}

inline std::string SerializeVertex(const storage::Vertex &vertex) {
  std::string result = utils::SerializeLabels(TransformIDsToString(vertex.labels)) + "|";
  return result + utils::SerializeIdType(vertex.gid);
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

inline std::string GetKeyForUniqueConstraintsDurability(storage::LabelId label,
                                                        const std::set<storage::PropertyId> &properties) {
  std::string entry;
  entry += utils::SerializeIdType(label);
  for (auto property : properties) {
    entry += ",";
    entry += utils::SerializeIdType(property);
  }
  return entry;
}

inline std::string SerializeVertexAsKeyForUniqueConstraint(const storage::LabelId &constraint_label,
                                                           const std::set<storage::PropertyId> &constraint_properties,
                                                           const std::string &gid) {
  auto key_for_indexing = PutIndexingLabelAndPropertiesFirst(SerializeIdType(constraint_label),
                                                             TransformIDsToString(constraint_properties));
  key_for_indexing += "|";
  key_for_indexing += gid;
  return key_for_indexing;
}

inline std::string SerializeVertexAsValueForUniqueConstraint(const storage::LabelId &constraint_label,
                                                             const std::vector<storage::LabelId> &vertex_labels,
                                                             const storage::PropertyStore &property_store) {
  return SerializeVertexAsValueForAuxiliaryStorages(constraint_label, vertex_labels, property_store);
}

inline storage::LabelId DeserializeConstraintLabelFromUniqueConstraintStorage(const std::string &key) {
  const std::string_view firstPartKey = FindPartOfStringView(key, '|', 1);
  const std::string_view constraint_key = FindPartOfStringView(firstPartKey, ',', 1);
  return storage::LabelId::FromString(constraint_key);
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
  std::string result;
  result += indexing_label;
  result += "|";
  result += indexing_property;
  result += "|";
  result += gid;
  return result;
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
