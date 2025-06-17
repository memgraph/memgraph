// Copyright 2025 Memgraph Ltd.
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
#include <string>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index_plugins.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {
struct VectorIndexOnEdgeTypeEntry {
  EdgeTypePropKey edge_type_prop_key;
  Edge *edge;
};

struct VectorIndexOnLabelEntry {
  LabelPropKey label_prop_key;
  Vertex *vertex;
};

struct VectorSearchResult {
  std::variant<Vertex *, Edge *> vertex_or_edge;
  double distance;
  double similarity;
};

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
  unum::usearch::scalar_kind_t scalar_kind;
};

/// @struct VectorIndexInfo
/// @brief Represents information about a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// the dimension of the vectors in the index, and the size of the index.
struct VectorIndexInfo {
  std::string index_name;
  std::variant<LabelId, EdgeTypeId> label_or_edge_type;
  PropertyId property;
  std::string metric;
  std::uint16_t dimension;
  std::size_t capacity;
  std::size_t size;
  std::string scalar_kind;
};

/// @struct VectorIndexSpec
/// @brief Represents a specification for creating a vector index in the system.
///
/// This structure includes the index name, the label and property on which the index is created,
/// and the configuration options for the index in the form of a JSON object.
struct VectorIndexSpec {
  std::string index_name;
  std::variant<LabelId, EdgeTypeId> label_or_edge_type;
  PropertyId property;
  unum::usearch::metric_kind_t metric_kind;
  std::uint16_t dimension;
  std::uint16_t resize_coefficient;
  std::size_t capacity;
  unum::usearch::scalar_kind_t scalar_kind;

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
    std::map<PropertyId, std::vector<LabelId>> p2l;  // TODO: fix later
  };

  explicit VectorIndex();
  ~VectorIndex();
  VectorIndex(const VectorIndex &) = delete;
  VectorIndex &operator=(const VectorIndex &) = delete;
  VectorIndex(VectorIndex &&) noexcept;
  VectorIndex &operator=(VectorIndex &&) noexcept;

  /// @brief Converts a metric kind to a string.
  /// @param metric The metric kind to be converted.
  /// @return The string representation of the metric kind.
  /// @throw query::VectorSearchException
  static const char *NameFromMetric(unum::usearch::metric_kind_t metric);

  /// @brief Converts a metric name to a metric kind.
  /// @param name The name of the metric to be converted.
  /// @return The metric kind corresponding to the name.
  /// @throw query::VectorSearchException
  static unum::usearch::metric_kind_t MetricFromName(std::string_view name);

  /// @brief Converts a scalar kind to a string.
  /// @param scalar The scalar kind to be converted.
  /// @return The string representation of the scalar kind.
  /// @throw query::VectorSearchException
  static const char *NameFromScalar(unum::usearch::scalar_kind_t scalar);

  /// @brief Converts a scalar name to a scalar kind.
  /// @param name The name of the scalar to be converted.
  /// @return The scalar kind corresponding to the name.
  /// @throw query::VectorSearchException
  static unum::usearch::scalar_kind_t ScalarFromName(std::string_view name);

  /// @brief Creates a new index based on the provided specification.
  /// @param spec The specification for the index to be created.
  /// @param snapshot_info
  /// @param vertices vertices from which to create vector index
  /// @return true if the index was created successfully, false otherwise.
  bool CreateIndex(const VectorIndexSpec &spec, utils::SkipList<Vertex>::Accessor &vertices,
                   std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// @brief Drops an existing index.
  /// @param index_name The name of the index to be dropped.
  /// @return true if the index was dropped successfully, false otherwise.
  bool DropIndex(std::string_view index_name);

  /// @brief Drops all existing indexes.
  void Clear();

  /// @brief Updates the index when a label is added to a vertex.
  /// @param added_label The label that was added to the vertex.
  /// @param vertex_after_update The vertex after the label was added.
  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update);

  /// @brief Updates the index when a label is removed from a vertex.
  /// @param removed_label The label that was removed from the vertex.
  /// @param vertex_before_update The vertex before the label was removed.
  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update);

  /// @brief Updates the index when a property is modified on a vertex.
  /// @param property The property that was modified.
  /// @param value The new value of the property.
  /// @param vertex The vertex on which the property was modified.
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex);

  /// @brief Lists the info of all existing indexes.
  /// @return A vector of VectorIndexInfo objects representing the indexes.
  std::vector<VectorIndexInfo> ListVectorIndicesInfo() const;

  /// @brief Lists the labels and properties that have vector indices.
  /// @return A vector of specs representing vector indices configurations.
  std::vector<VectorIndexSpec> ListIndices() const;

  /// @brief Returns number of vertices in the index.
  /// @param label The label of the vertices in the index.
  /// @param property The property of the vertices in the index.
  /// @return The number of vertices in the index.
  std::optional<uint64_t> ApproximateVectorCount(LabelId label, PropertyId property) const;

  /// @brief Searches for nodes in the specified index using a query vector.
  /// @param index_name The name of the index to search.
  /// @param result_set_size The number of results to return.
  /// @param query_vector The vector to be used for the search query.
  /// @return A vector of tuples containing the vertex, distance, and similarity of the search results.
  std::vector<VectorSearchResult> Search(std::string_view index_name, uint64_t result_set_size,
                                         const std::vector<float> &query_vector) const;

  /// @brief Aborts the entries that were inserted in the specified transaction.
  /// @param label_prop The label of the vertices to be removed.
  /// @param vertices The vertices to be removed.
  void AbortEntries(const LabelPropKey &label_prop, std::span<Vertex *const> vertices);

  /// @brief Restores the entries that were removed in the specified transaction.
  /// @param label_prop The label and property of the vertices to be restored.
  /// @param prop_vertices The vertices to be restored.
  void RestoreEntries(const LabelPropKey &label_prop,
                      std::span<std::pair<PropertyValue, Vertex *> const> prop_vertices);

  void RemoveObsoleteEntries(std::stop_token token) const;

  /// @brief Returns the index statistics.
  /// @return The index statistics.
  IndexStats Analysis() const;

  /// @brief Tries to insert a vertex into the index.
  /// @param vertex The vertex to be inserted.
  void TryInsertVertex(Vertex *vertex);

 private:
  /// @brief Adds a vertex to an existing index.
  /// @param vertex The vertex to be added.
  /// @param label_prop The label and property key for the index.
  /// @param value The value of the property.
  /// @throw query::VectorSearchException
  bool UpdateVectorIndex(std::variant<VectorIndexOnLabelEntry, VectorIndexOnEdgeTypeEntry> entry,
                         const PropertyValue *value = nullptr);

  struct Impl;
  std::unique_ptr<Impl> pimpl;
};

}  // namespace memgraph::storage
