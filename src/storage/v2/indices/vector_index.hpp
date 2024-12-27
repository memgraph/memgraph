// Copyright 2024 Memgraph Ltd.
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

#include <cstddef>
#include <cstdint>
#include <json/json.hpp>
#include <string>

#include "query/frontend/ast/ast.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_plugins.hpp"

namespace memgraph::storage {

/// usearch does not provide a way to convert metric_kind_t to string, so we need to do it manually.
inline const std::unordered_map<unum::usearch::metric_kind_t, std::vector<std::string>> kMetricToStringMap = {
    {unum::usearch::metric_kind_t::l2sq_k, {"l2sq", "euclidean_sq"}},
    {unum::usearch::metric_kind_t::ip_k, {"ip", "inner", "dot"}},
    {unum::usearch::metric_kind_t::cos_k, {"cos", "angular"}},
    {unum::usearch::metric_kind_t::haversine_k, {"haversine"}},
    {unum::usearch::metric_kind_t::divergence_k, {"divergence"}},
    {unum::usearch::metric_kind_t::pearson_k, {"pearson"}},
    {unum::usearch::metric_kind_t::hamming_k, {"hamming"}},
    {unum::usearch::metric_kind_t::tanimoto_k, {"tanimoto"}},
    {unum::usearch::metric_kind_t::sorensen_k, {"sorensen"}}};

inline std::string SupportedMetricKindsToString() {
  std::string supported_metric_kinds;
  for (const auto &[_, names] : kMetricToStringMap) {
    for (const auto &name : names) {
      supported_metric_kinds += name;
      supported_metric_kinds += ", ";
    }
  }
  if (!supported_metric_kinds.empty()) {
    // Remove the last two characters (", ") from the string.
    supported_metric_kinds.pop_back();
    supported_metric_kinds.pop_back();
  }
  return supported_metric_kinds;
}

/// @struct VectorIndexConfigMap
/// @brief Represents the configuration options for a vector index.
///
/// This structure includes the metric name, the dimension of the vectors in the index,
/// the capacity of the index, and the resize coefficient for the index.
struct VectorIndexConfigMap {
  unum::usearch::metric_kind_t metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::uint16_t resize_coefficient;
};

/// @struct VectorIndexInfo
/// @brief Represents information about a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// the dimension of the vectors in the index, and the size of the index.
struct VectorIndexInfo {
  std::string index_name;
  LabelId label;
  PropertyId property;
  std::string metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::size_t size;
};

/// @struct VectorIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  // TODO(@DavIvek): Add scalar kind configuration options. This will be useful for memory usage.
  std::string index_name;
  LabelId label;
  PropertyId property;
  unum::usearch::metric_kind_t metric_kind;
  std::uint16_t dimension;
  std::size_t capacity;
  std::uint16_t resize_coefficient;  // TODO(@DavIvek): Revisit resizing options (maybe using float?)

  friend bool operator==(const VectorIndexSpec &, const VectorIndexSpec &) = default;
};

/// @class VectorIndex
/// @brief High-level interface for managing vector indexes.
///
/// The VectorIndex class supports creating new indexes, adding nodes to an index,
/// listing all indexes, and searching for nodes using a query vector.
/// Currently, vector index operates in READ_UNCOMMITTED isolation level. Database can
/// still operate in any other isolation level.
/// This class is thread-safe and uses the Pimpl (Pointer to Implementation) idiom
/// to hide implementation details.
class VectorIndex {
 public:
  struct IndexStats {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;
  };

  explicit VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  /// @brief Creates a new index based on the specified configuration.
  /// @param spec The specification for the index to be created.
  void CreateIndex(const std::shared_ptr<VectorIndexSpec> &spec);

  /// @brief Creates a new index based on the name, label, property and vertices.
  /// @param spec The specification for the index to be created.
  /// @return true if the index was created successfully, false otherwise.
  bool CreateIndex(const std::shared_ptr<VectorIndexSpec> &spec, utils::SkipList<Vertex>::Accessor vertices);

  /// @brief Drops an existing index.
  /// @param index_name The name of the index to be dropped.
  /// @return true if the index was dropped successfully, false otherwise.
  bool DropIndex(std::string_view index_name);

  /// @brief Drops all existing indexes.
  void Clear();

  /// @brief Updates the index when a label is added to a vertex.
  /// @param added_label The label that was added to the vertex.
  /// @param vertex_after_update The vertex after the label was added.
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update) const;

  /// @brief Updates the index when a label is removed from a vertex.
  /// @param removed_label The label that was removed from the vertex.
  /// @param vertex_before_update The vertex before the label was removed.
  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update) const;

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param property The property that was modified.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex) const;

  /// @brief Lists the info of all existing indexes.
  /// @return A vector of VectorIndexInfo objects representing the indexes.
  std::vector<VectorIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  /// @return A vector of specs representing vector indices configurations.
  std::vector<std::shared_ptr<VectorIndexSpec>> ListIndices() const;

  /// @brief Lists vector index specifications.
  /// @return A vector of VectorIndexSpec objects representing the index specifications.
  std::vector<VectorIndexSpec> ListIndexSpecs() const;

  /// @brief Returns number of vertices in the index.
  /// @param label_prop The label and property key for the index.
  /// @return The number of vertices in the index.
  std::optional<uint64_t> ApproximateVectorCount(LabelId label, PropertyId property) const;

  /// @brief Searches for nodes in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param start_timestamp The timestamp of transaction in which the search is performed.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of pairs containing the global ID (Gid) and the associated score (distance).
  std::vector<std::tuple<Vertex *, double, double>> Search(std::string_view index_name, uint64_t result_set_size,
                                                           const std::vector<float> &query_vector) const;

  /// @brief Aborts the entries that were inserted in the specified transaction.
  /// @param label The label of the vertices to be removed.
  /// @param vertices The vertices to be removed.
  void AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices) const;

  /// @brief Restores the entries that were removed in the specified transaction.
  /// @param label The label of the vertices to be restored.
  /// @param vertices The vertices to be restored.
  void RestoreEntries(const LabelPropKey &label_prop,
                      std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices) const;

  void RemoveObsoleteEntries(std::stop_token token) const;

  /// @brief Returns the index statistics.
  /// @return The index statistics.
  IndexStats Analysis() const;

  /// @brief Tries to insert a vertex into the index.
  /// @param vertex The vertex to be inserted.
  void TryInsertVertex(Vertex *vertex) const;

 private:
  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param commit_timestamp The commit timestamp for the operation.
  void UpdateVectorIndex(Vertex *vertex, const LabelPropKey &label_prop, const PropertyValue *value = nullptr) const;

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
